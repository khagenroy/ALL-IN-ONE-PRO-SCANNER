"""
ALL IN ONE PRO - Section A (Liquidity Sweep + Order Block) scanner.

Ports the EXACT entry-trigger logic from Section A of
"ALL_IN_ONE_PRO_STOCK_COMMODITY_LIVE_NO_SCANNER_WITH_OB_ALERT.pine" (the
"PA Toolkit Dual Strategy" half) into Python, for scanning many symbols at
once rather than watching one chart. Section B (Keltner+SMC/RSI pattern) is
NOT included yet - Phase 2.

WHAT THIS DOES NOT PORT (deliberately)
----------------------------------------
The live script also tracks SL trailing, T1-T6 cascading targets, re-entry,
and position state for an OPEN trade. None of that applies here - a scanner
isn't holding a position, it's only asking "would Section A fire an entry
signal on the most recent closed bar for this symbol right now". Entry
price / stop-loss / qty ARE computed (same formulas as the Pine script)
purely as reference info to show alongside each signal, not because
anything here tracks or manages a trade.

TIMEFRAME
---------
Runs on 10-minute candles (confirmed with Khagen - matches how he actually
runs ALL IN ONE PRO on TradingView). Dhan's intraday API doesn't offer a
native 10-minute interval (only 1/5/15/25/60) - see build_10min_bars()
below for how 10-min candles are built from 5-minute data instead.

THIS HAS NOT BEEN RUN AGAINST THE LIVE DHAN API
-------------------------------------------------
Same disclosure as the EOD scanner: this sandbox cannot reach api.dhan.co,
so /charts/intraday's exact response shape is unverified here. Dry-run
against a handful of symbols first (TEST_SYMBOL_LIMIT) before trusting a
full scan.
"""

import numpy as np
import pandas as pd

# ============================================================================
# CONFIG - mirrors the Pine script's Section A input defaults exactly.
# ============================================================================
ZIGZAG_LEN = 9              # "ZigZag Length (Structure / CHoCH)"
LIQUIDITY_LEN = 30          # "Liquidity Length (Stop Hunt Zones)"
NUMBER_OB_SHOW = 2          # "Number of Order Blocks to Show"
COOLDOWN_BARS = 8           # "Minimum Bars Between Signals"

USE_VOL_FILTER = True       # "Require Volume Confirmation"
VOL_MA_LEN = 20
VOL_MULTIPLIER = 5.0        # live Pine default is 1.5 - raised to 5.0 for this
                            # scanner only (Khagen's explicit call: scanning
                            # 1000 symbols every 10 min throws too many
                            # signals at 1.5x). Does not change the live
                            # TradingView indicator, which still uses 1.5.
VOL_CLIMAX_LOOKBACK_BARS = 10  # scanner-only: don't require the 5x volume
                            # spike on the signal bar itself - a volume
                            # climax (rally exhaustion) often happens a few
                            # bars BEFORE price actually reverses/diverges.
                            # So the condition is "did a >=5x bar happen
                            # anywhere in the last 10 bars", not "is THIS
                            # bar >=5x". Khagen's explicit call, not in the
                            # live Pine script (which checks only the
                            # current bar).

BIG_CANDLE_FILTER_ON = True  # "Skip Oversized Signals on First Candle of Day"
BIG_CANDLE_ATR_MULT = 2.0    # "Max First-Candle Range (x ATR)"

SL_BUFFER_TICKS = 10        # tick-floor component of the SL buffer
ATR_BUFFER_MULT = 0.15      # ATR component of the SL buffer
ATR_LEN = 14

OB_TOUCH_ZONE_ATR_MULT = 0.5  # how close price must wick to an OB to count as a "touch"

T_RR = {"T1": 1.0, "T2": 2.0, "T3": 3.0, "T4": 4.0, "T5": 5.0, "T6": 6.0}


# ============================================================================
# BASIC INDICATOR HELPERS
# ============================================================================

def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    """Wilder's ATR - same smoothing ta.atr() uses in Pine."""
    n = len(close)
    tr = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
    atr = np.full(n, np.nan)
    alpha = 1.0 / length
    for i in range(n):
        if i < length - 1:
            continue
        if i == length - 1:
            atr[i] = tr[: i + 1].mean()
        else:
            atr[i] = atr[i - 1] + alpha * (tr[i] - atr[i - 1])
    return atr


def _pivot_high(high: np.ndarray, left: int, right: int) -> np.ndarray:
    """Mirrors ta.pivothigh(high, left, right): bar i-right is a confirmed
    pivot high (known only once `right` bars have closed after it) if it's
    the max of the window [i-right-left, i-right]. Returns an array aligned
    to the CURRENT bar index i where a pivot was just confirmed (NaN
    elsewhere) - same timing ta.pivothigh gives Pine (value available on
    the bar `right` bars after the actual pivot bar)."""
    n = len(high)
    out = np.full(n, np.nan)
    for i in range(left + right, n):
        pivot_idx = i - right
        window = high[pivot_idx - left: i + 1]
        if high[pivot_idx] == window.max():
            out[i] = high[pivot_idx]
    return out


def _pivot_low(low: np.ndarray, left: int, right: int) -> np.ndarray:
    n = len(low)
    out = np.full(n, np.nan)
    for i in range(left + right, n):
        pivot_idx = i - right
        window = low[pivot_idx - left: i + 1]
        if low[pivot_idx] == window.min():
            out[i] = low[pivot_idx]
    return out


# ============================================================================
# SECTION A STATE MACHINE - faithful bar-by-bar port of lines ~165-700 of
# the Pine script. Python objects (plain dicts) stand in for Pine's
# `orderblock`/`liquidity` user-defined types and arrays.
# ============================================================================

def compute_section_a(df: pd.DataFrame) -> dict:
    """df must have columns open/high/low/close/volume, indexed by
    timestamp, oldest first, on 10-MINUTE bars. Returns a dict describing
    the state as of the LAST (most recent closed) bar: whether a sweep or
    OB-mitigation signal just fired, or whether price is merely touching a
    live OB zone without having fired yet (the "OB Zone Watch" case)."""
    o = df["open"].to_numpy()
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    close = df["close"].to_numpy()
    volume = df["volume"].to_numpy()
    n = len(df)

    min_bars = max(ZIGZAG_LEN, LIQUIDITY_LEN) * 2 + VOL_MA_LEN + 10
    if n < min_bars:
        return None  # not enough 10-min history yet for this symbol

    atr = _atr(high, low, close, ATR_LEN)
    vol_ma = pd.Series(volume).rolling(VOL_MA_LEN, min_periods=VOL_MA_LEN).mean().to_numpy()

    # --- ZigZag / trend / CHoCH (lines 194-223) ---
    # to_up[i]/to_down[i] use ta.highest/ta.lowest over the trailing
    # ZIGZAG_LEN bars INCLUDING the current one, compared against the value
    # ZIGZAG_LEN bars back - exactly as written in Pine, not a symmetric
    # pivot (different from the EOD scanner's CHoCH, which uses a centered
    # pivot - this one is deliberately a separate, faithful port of a
    # DIFFERENT algorithm from a different script).
    trend = np.ones(n, dtype=int)  # var int trend = 1
    high_vals, low_vals = [], []   # highVal[]/lowVal[] arrays (confirmed swing prices)
    last_state = None              # lastState ('up'/'down'/None)
    choch_bull = np.zeros(n, dtype=bool)
    choch_bear = np.zeros(n, dtype=bool)

    for i in range(ZIGZAG_LEN, n):
        window_h = high[i - ZIGZAG_LEN: i + 1]
        window_l = low[i - ZIGZAG_LEN: i + 1]
        to_up = high[i - ZIGZAG_LEN] >= window_h.max()
        to_down = low[i - ZIGZAG_LEN] <= window_l.min()
        prev_trend = trend[i - 1]
        new_trend = -1 if (prev_trend == 1 and to_down) else (1 if (prev_trend == -1 and to_up) else prev_trend)
        trend[i] = new_trend

        if new_trend != prev_trend and new_trend == 1:
            high_vals.append(high[i - ZIGZAG_LEN])
        if new_trend != prev_trend and new_trend == -1:
            low_vals.append(low[i - ZIGZAG_LEN])

        if len(low_vals) > 1 and close[i] < low_vals[-1]:
            if last_state is None or last_state == "up":
                choch_bear[i] = True
            last_state = "down"
        if len(high_vals) > 1 and close[i] > high_vals[-1]:
            if last_state is None or last_state == "down":
                choch_bull[i] = True
            last_state = "up"

    # --- Order block formation + management (lines 225-401) ---
    # Each OB: {value, bar_start, broken, touch_alerted}. bullish = support
    # zones (formed on bullish CHoCH), bearish = resistance zones.
    bullish_ob, bearish_ob = [], []
    high_val_idx, low_val_idx = [], []  # confirmed pivot BAR INDEXES (for the dynamic lookback window)
    ob_mitigation_buy = np.zeros(n, dtype=bool)
    ob_mitigation_sell = np.zeros(n, dtype=bool)
    ob_zone_touched_buy = np.zeros(n, dtype=bool)
    ob_zone_touched_sell = np.zeros(n, dtype=bool)
    # Whether, on the LAST bar specifically, price is sitting inside a live
    # (not yet broken) OB zone without a fresh mitigation firing that same
    # bar - the "OB Zone Watch" case Khagen asked for.
    live_ob_zone_present = np.zeros(n, dtype=bool)

    for i in range(ZIGZAG_LEN, n):
        # track confirmed pivot bar indexes for the dynamic OB lookback window
        if i > 0 and trend[i] != trend[i - 1] and trend[i] == 1:
            high_val_idx.append(i - ZIGZAG_LEN)
        if i > 0 and trend[i] != trend[i - 1] and trend[i] == -1:
            low_val_idx.append(i - ZIGZAG_LEN)

        if choch_bull[i]:
            lookback = (i - high_val_idx[-1] - 1) if high_val_idx else 15
            lookback = max(lookback, 0)
            start = max(i - lookback, 0)
            seg_low = low[start: i + 1]
            min_idx_rel = int(np.argmin(seg_low))
            bar_start = start + min_idx_rel
            bullish_ob.append({"value": float(seg_low[min_idx_rel]), "bar_start": bar_start,
                                "broken": False, "touch_alerted": False})
            if len(bullish_ob) > 20:
                bullish_ob.pop(0)

        if choch_bear[i]:
            lookback = (i - low_val_idx[-1] - 1) if low_val_idx else 15
            lookback = max(lookback, 0)
            start = max(i - lookback, 0)
            seg_high = high[start: i + 1]
            max_idx_rel = int(np.argmax(seg_high))
            bar_start = start + max_idx_rel
            bearish_ob.append({"value": float(seg_high[max_idx_rel]), "bar_start": bar_start,
                                "broken": False, "touch_alerted": False})
            if len(bearish_ob) > 20:
                bearish_ob.pop(0)

        # --- bullish OB management (support zones - mitigation = BUY signal)
        counter = 0
        for ob in reversed(bullish_ob):
            if counter >= NUMBER_OB_SHOW:
                break
            if not ob["broken"] and not ob["touch_alerted"] and low[i] <= ob["value"] + (atr[i] * OB_TOUCH_ZONE_ATR_MULT):
                ob["touch_alerted"] = True
                ob_zone_touched_buy[i] = True
            if close[i] < ob["value"]:
                ob["broken"] = True  # invalidated (deleted from the chart in Pine; kept here, marked broken, just stops being "live")
            elif not ob["broken"] and low[i] <= ob["value"] + (atr[i] * OB_TOUCH_ZONE_ATR_MULT) and close[i] > ob["value"]:
                ob["broken"] = True
                ob_mitigation_buy[i] = True
            counter += 1
        bullish_ob = [ob for ob in bullish_ob if not (ob["broken"] and close[i] < ob["value"])]

        # --- bearish OB management (resistance zones - mitigation = SELL signal)
        counter = 0
        for ob in reversed(bearish_ob):
            if counter >= NUMBER_OB_SHOW:
                break
            if not ob["broken"] and not ob["touch_alerted"] and high[i] >= ob["value"] - (atr[i] * OB_TOUCH_ZONE_ATR_MULT):
                ob["touch_alerted"] = True
                ob_zone_touched_sell[i] = True
            if close[i] > ob["value"]:
                ob["broken"] = True
            elif not ob["broken"] and high[i] >= ob["value"] - (atr[i] * OB_TOUCH_ZONE_ATR_MULT) and close[i] < ob["value"]:
                ob["broken"] = True
                ob_mitigation_sell[i] = True
            counter += 1
        bearish_ob = [ob for ob in bearish_ob if not (ob["broken"] and close[i] > ob["value"])]

    # live_ob_zone_present on the LAST bar: any unbroken OB (within the
    # numberObShow visible window) still active, used below to report
    # "OB Zone Watch" when there's a live zone nearby but nothing fired.
    def _has_live_zone(obs):
        return any(not ob["broken"] for ob in obs[-NUMBER_OB_SHOW:])

    # --- Liquidity sweep engine (lines 266-336) ---
    ph = _pivot_high(high, LIQUIDITY_LEN, LIQUIDITY_LEN)
    pl = _pivot_low(low, LIQUIDITY_LEN, LIQUIDITY_LEN)
    bearish_liq, bullish_liq = [], []  # [{value, broken}]
    bearish_sweep = np.zeros(n, dtype=bool)
    bullish_sweep = np.zeros(n, dtype=bool)

    for i in range(n):
        if not np.isnan(ph[i]):
            bearish_liq.append({"value": float(ph[i]), "broken": False})
            if len(bearish_liq) > 7:
                bearish_liq.pop(0)
        if not np.isnan(pl[i]):
            bullish_liq.append({"value": float(pl[i]), "broken": False})
            if len(bullish_liq) > 7:
                bullish_liq.pop(0)

        for liq in bearish_liq:
            if not liq["broken"] and high[i] > liq["value"]:
                liq["broken"] = True
                if close[i] < liq["value"]:
                    bearish_sweep[i] = True
        for liq in bullish_liq:
            if not liq["broken"] and low[i] < liq["value"]:
                liq["broken"] = True
                if close[i] > liq["value"]:
                    bullish_sweep[i] = True

    # --- Volume filter, cooldown, big-candle filter, final signals (403-647) ---
    # Scanner-only climax-lookback version (see VOL_CLIMAX_LOOKBACK_BARS above):
    # a bar counts as volume-confirmed if a >=5x-average volume bar happened
    # ANYWHERE in the trailing N bars (including itself), not just on itself.
    if USE_VOL_FILTER:
        raw_vol_spike = np.where(np.isnan(vol_ma), False, volume >= vol_ma * VOL_MULTIPLIER)
        vol_condition = pd.Series(raw_vol_spike).rolling(
            VOL_CLIMAX_LOOKBACK_BARS, min_periods=1).max().to_numpy().astype(bool)
    else:
        vol_condition = np.ones(n, dtype=bool)

    # first-bar-of-day detection, from the timestamp index
    dates = df.index.date
    is_first_bar_of_day = np.zeros(n, dtype=bool)
    is_first_bar_of_day[0] = True
    for i in range(1, n):
        is_first_bar_of_day[i] = dates[i] != dates[i - 1]

    candle_too_big = BIG_CANDLE_FILTER_ON & is_first_bar_of_day & ((high - low) > (atr * BIG_CANDLE_ATR_MULT))

    last_signal_bar = -999
    sweep_buy = np.zeros(n, dtype=bool)
    sweep_sell = np.zeros(n, dtype=bool)
    ob_buy = np.zeros(n, dtype=bool)
    ob_sell = np.zeros(n, dtype=bool)
    for i in range(n):
        can_signal = (i - last_signal_bar) >= COOLDOWN_BARS
        vc = bool(vol_condition[i]) if not np.isnan(vol_ma[i]) else False
        sweep_buy[i] = bullish_sweep[i] and vc and can_signal and not candle_too_big[i]
        sweep_sell[i] = bearish_sweep[i] and vc and can_signal and not candle_too_big[i]
        ob_buy[i] = ob_mitigation_buy[i] and vc and can_signal and not candle_too_big[i]
        ob_sell[i] = ob_mitigation_sell[i] and vc and can_signal and not candle_too_big[i]
        if sweep_buy[i] or sweep_sell[i] or ob_buy[i] or ob_sell[i]:
            last_signal_bar = i

    i = n - 1  # most recent closed 10-min bar

    def _entry_sl_targets(side: str, entry_price: float, sl_price: float):
        risk_dist = (entry_price - sl_price) if side == "buy" else (sl_price - entry_price)
        if risk_dist <= 0:
            return None
        targets = {}
        for name, rr in T_RR.items():
            targets[name] = round(entry_price + risk_dist * rr, 2) if side == "buy" else round(entry_price - risk_dist * rr, 2)
        return {"entry": round(entry_price, 2), "sl": round(sl_price, 2), **targets}

    sl_buffer = max(SL_BUFFER_TICKS * 0.05, ATR_BUFFER_MULT * atr[i]) if not np.isnan(atr[i]) else 0.0
    # NOTE: SL_BUFFER_TICKS * syminfo.mintick in Pine - mintick isn't knowable
    # generically from OHLC data alone, so this uses 0.05 (NSE equity's
    # standard tick size) as a stand-in. Fine for stocks (the vast majority
    # of this universe); flagged here in case a non-standard-tick symbol
    # ever looks off.

    signal = None
    levels = None
    if sweep_buy[i]:
        signal = "SWEEP_BUY"
        levels = _entry_sl_targets("buy", high[i], low[i] - sl_buffer)
    elif sweep_sell[i]:
        signal = "SWEEP_SELL"
        levels = _entry_sl_targets("sell", low[i], high[i] + sl_buffer)
    elif ob_buy[i]:
        signal = "OB_BUY"
        levels = _entry_sl_targets("buy", high[i], low[i] - sl_buffer)
    elif ob_sell[i]:
        signal = "OB_SELL"
        levels = _entry_sl_targets("sell", low[i], high[i] + sl_buffer)

    ob_watch = None
    if signal is None:
        if ob_zone_touched_buy[i] or _has_live_zone(bullish_ob):
            ob_watch = "OB_ZONE_WATCH_BUY" if (ob_zone_touched_buy[i] or any(
                not ob["broken"] and low[i] <= ob["value"] + atr[i] * OB_TOUCH_ZONE_ATR_MULT
                for ob in bullish_ob[-NUMBER_OB_SHOW:])) else None
        if ob_watch is None and (ob_zone_touched_sell[i] or _has_live_zone(bearish_ob)):
            if ob_zone_touched_sell[i] or any(
                not ob["broken"] and high[i] >= ob["value"] - atr[i] * OB_TOUCH_ZONE_ATR_MULT
                for ob in bearish_ob[-NUMBER_OB_SHOW:]
            ):
                ob_watch = "OB_ZONE_WATCH_SELL"

    return {
        "timestamp": str(df.index[i]),
        "close": round(float(close[i]), 2),
        "signal": signal,
        "levels": levels,
        "ob_watch": ob_watch,
    }
