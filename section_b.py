"""
ALL IN ONE PRO - Section B ("UNI+PA+RSI+Pattern2 [STOCKS]") scanner.

Faithful port of Section B's entry-trigger logic (lines ~1251-2264 of the
Pine script) - two independent raw signals, combined:

  1. SSL/BSL sweep-and-reclaim: a swing low/high that formed outside a
     Keltner Channel band gets tracked as a liquidity level (SSL = sell-side
     liquidity below price, BSL = buy-side liquidity above price); a BUY
     fires when price wicks below the SSL level AND closes back above it
     (also still below the lower KC band at that moment) - the mirror image
     for SELL/BSL.
  2. RSI + candlestick pattern checklist: recent RSI oversold/overbought
     (last 3 bars) + a clean reversal candle (engulfing or hammer/shooting
     star, not a doji) + price at a local extreme (3-bar high/low).

Same deliberate exclusions as Section A: no SL-trailing/re-entry/position
tracking ported (not relevant to a scanner), and the "Force Signal [TEST
MODE]" input is left permanently off (it defaults off in the real script
too - this is a manual testing toggle, not part of the real strategy).

SCANNER-ONLY DEVIATION: the live Pine script's Section B has no volume
filter at all. One was added here anyway (20-bar volume MA x 5.0, same
multiplier Section A now uses) because scanning 1000 symbols every 10
minutes without one throws too many signals - Khagen's explicit call. This
does NOT change the live TradingView indicator.

Note: an RSI/MACD-style divergence calc exists in the Pine script
(u_bullishDiv/u_bearishDiv) but is NEVER actually used by longChecklistPass/
shortChecklistPass or any alert - confirmed by searching every reference to
it in the file. Left out here for the same reason: it's dead code in the
original, porting it would add a signal the real indicator doesn't use.
"""

import numpy as np
import pandas as pd

from all_in_one_scanner import (
    _atr, _pivot_high, _pivot_low,
    SL_BUFFER_TICKS, ATR_BUFFER_MULT, ATR_LEN,
    BIG_CANDLE_FILTER_ON, BIG_CANDLE_ATR_MULT, T_RR,
    VOL_MA_LEN, VOL_MULTIPLIER, VOL_CLIMAX_LOOKBACK_BARS,
)

# The live Pine script's Section B has NO volume filter at all. Added here
# anyway, scanner-only, for the same reason Section A's filter was raised to
# 5x: scanning 1000 symbols every 10 minutes throws too many signals without
# one (Khagen's explicit call). Uses the same climax-lookback shape as
# Section A: a bar is volume-confirmed if a >=5x-average volume bar happened
# anywhere in the trailing 10 bars (a climax), not necessarily on the signal
# bar itself - Khagen's point that a rally's volume climax tends to happen a
# few bars before price actually reverses/diverges.
USE_VOL_FILTER = True

KC_LENGTH = 20
KC_MULT = 2.0
SWING_LEFT_BARS = 5
SWING_RIGHT_BARS = 2

RSI_LENGTH = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
RSI_LOOKBACK = 3
SWING_LOOKBACK = 3
DOJI_BODY_RATIO = 0.12


def _rsi(close: np.ndarray, length: int) -> np.ndarray:
    delta = np.diff(close, prepend=close[0])
    gain = np.clip(delta, 0, None)
    loss = np.clip(-delta, 0, None)
    n = len(close)
    avg_gain = np.full(n, np.nan)
    avg_loss = np.full(n, np.nan)
    alpha = 1.0 / length
    for i in range(n):
        if i < length:
            continue
        if i == length:
            avg_gain[i] = gain[1:i + 1].mean()
            avg_loss[i] = loss[1:i + 1].mean()
        else:
            avg_gain[i] = avg_gain[i - 1] + alpha * (gain[i] - avg_gain[i - 1])
            avg_loss[i] = avg_loss[i - 1] + alpha * (loss[i] - avg_loss[i - 1])
    rs = avg_gain / np.where(avg_loss == 0, np.nan, avg_loss)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = np.where(avg_loss == 0, 100.0, rsi)
    return rsi


def compute_section_b(df: pd.DataFrame) -> dict:
    """df must have columns open/high/low/close/volume, indexed by
    timestamp, oldest first, on 10-minute bars (same input as
    all_in_one_scanner.compute_section_a). Returns the state as of the LAST
    (most recent closed) bar: a BUY/SELL signal (and which of the two
    mechanisms fired it), or None."""
    o = df["open"].to_numpy()
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    close = df["close"].to_numpy()
    volume = df["volume"].to_numpy()
    n = len(df)

    min_bars = max(KC_LENGTH, SWING_LEFT_BARS + SWING_RIGHT_BARS, RSI_LENGTH) * 2 + 10
    if n < min_bars:
        return None

    atr14 = _atr(high, low, close, ATR_LEN)

    # --- Keltner Channel (EMA basis, ATR(kcLength) range - a DIFFERENT ATR
    # length from the shared atr(14) used for the SL buffer / big-candle
    # filter, exactly as the Pine script keeps them separate) ---
    basis = pd.Series(close).ewm(span=KC_LENGTH, adjust=False, min_periods=KC_LENGTH).mean().to_numpy()
    range_ma = _atr(high, low, close, KC_LENGTH)
    upper_kc = basis + range_ma * KC_MULT
    lower_kc = basis - range_ma * KC_MULT

    swing_high = _pivot_high(high, SWING_LEFT_BARS, SWING_RIGHT_BARS)
    swing_low = _pivot_low(low, SWING_LEFT_BARS, SWING_RIGHT_BARS)

    # --- SSL/BSL box state machine (lines 1302-1353) ---
    bsl_price = None
    ssl_price = None
    trigger_buy_ssl = np.zeros(n, dtype=bool)
    trigger_sell_bsl = np.zeros(n, dtype=bool)

    for i in range(n):
        # set/refresh a BSL (resistance liquidity) level
        if not np.isnan(swing_high[i]) and high[i] > upper_kc[i]:
            bsl_price = swing_high[i]
        # set/refresh an SSL (support liquidity) level
        if not np.isnan(swing_low[i]) and low[i] < lower_kc[i]:
            ssl_price = swing_low[i]

        # check SSL trigger (buy) using whatever ssl_price holds NOW (same-bar set above is visible, matching Pine's top-to-bottom execution)
        if ssl_price is not None:
            if low[i] < ssl_price and close[i] > ssl_price and low[i] < lower_kc[i]:
                trigger_buy_ssl[i] = True
                ssl_price = None
            elif close[i] < ssl_price:
                ssl_price = None  # invalidated

        # check BSL trigger (sell)
        if bsl_price is not None:
            if high[i] > bsl_price and close[i] < bsl_price and high[i] > upper_kc[i]:
                trigger_sell_bsl[i] = True
                bsl_price = None
            elif close[i] > bsl_price:
                bsl_price = None

    # --- RSI + candlestick checklist (lines 1437-1490) ---
    rsi = _rsi(close, RSI_LENGTH)
    is_green = close > o
    is_red = close < o
    body_size = np.abs(close - o)
    candle_range = high - low
    is_doji = (candle_range > 0) & ((body_size / np.where(candle_range == 0, np.nan, candle_range)) < DOJI_BODY_RATIO)

    prev_close, prev_open = np.roll(close, 1), np.roll(o, 1)
    bullish_engulfing = is_green & (prev_close < prev_open) & (close >= prev_open) & (o <= prev_close)
    lower_shadow = np.minimum(close, o) - low
    is_hammer = (lower_shadow >= 2 * body_size) & ((high - np.maximum(close, o)) <= body_size * 0.5) & ~is_doji

    bearish_engulfing = is_red & (prev_close > prev_open) & (close <= prev_open) & (o >= prev_close)
    upper_shadow = high - np.maximum(close, o)
    is_shooting_star = (upper_shadow >= 2 * body_size) & ((np.minimum(close, o) - low) <= body_size * 0.5) & ~is_doji

    clean_bull_engulf = bullish_engulfing & ~is_doji
    clean_bear_engulf = bearish_engulfing & ~is_doji

    is_highest_high = high >= pd.Series(high).rolling(SWING_LOOKBACK, min_periods=1).max().to_numpy()
    is_lowest_low = low <= pd.Series(low).rolling(SWING_LOOKBACK, min_periods=1).min().to_numpy()

    recent_rsi_oversold = pd.Series(rsi).rolling(RSI_LOOKBACK, min_periods=1).min().to_numpy() < RSI_OVERSOLD
    recent_rsi_overbought = pd.Series(rsi).rolling(RSI_LOOKBACK, min_periods=1).max().to_numpy() > RSI_OVERBOUGHT

    short_checklist_pass = recent_rsi_overbought & is_red & (clean_bear_engulf | is_shooting_star) & ~is_doji & is_highest_high
    long_checklist_pass = recent_rsi_oversold & is_green & (clean_bull_engulf | is_hammer) & ~is_doji & is_lowest_low

    # --- shared big-candle filter (same formula/inputs Section A uses) ---
    dates = df.index.date
    is_first_bar_of_day = np.zeros(n, dtype=bool)
    is_first_bar_of_day[0] = True
    for i in range(1, n):
        is_first_bar_of_day[i] = dates[i] != dates[i - 1]
    candle_too_big = BIG_CANDLE_FILTER_ON & is_first_bar_of_day & ((high - low) > (atr14 * BIG_CANDLE_ATR_MULT))

    vol_ma = pd.Series(volume).rolling(VOL_MA_LEN, min_periods=VOL_MA_LEN).mean().to_numpy()
    if USE_VOL_FILTER:
        raw_vol_spike = np.where(np.isnan(vol_ma), False, volume >= vol_ma * VOL_MULTIPLIER)
        vol_condition = pd.Series(raw_vol_spike).rolling(
            VOL_CLIMAX_LOOKBACK_BARS, min_periods=1).max().to_numpy().astype(bool)
    else:
        vol_condition = np.ones(n, dtype=bool)

    raw_buy = (trigger_buy_ssl | long_checklist_pass) & ~candle_too_big & vol_condition
    raw_sell = (trigger_sell_bsl | short_checklist_pass) & ~candle_too_big & vol_condition

    i = n - 1  # most recent closed bar

    def _levels(side: str, entry_price: float, sl_price: float):
        risk = (entry_price - sl_price) if side == "buy" else (sl_price - entry_price)
        if risk <= 0 or np.isnan(risk):
            return None
        out = {"entry": round(float(entry_price), 2), "sl": round(float(sl_price), 2)}
        for name, rr in T_RR.items():
            out[name] = round(float(entry_price + risk * rr) if side == "buy" else float(entry_price - risk * rr), 2)
        return out

    sl_buffer = max(SL_BUFFER_TICKS * 0.05, ATR_BUFFER_MULT * atr14[i]) if not np.isnan(atr14[i]) else 0.0

    signal = None
    source = None
    levels = None
    if raw_buy[i]:
        signal = "BUY"
        source = "SSL_SWEEP" if trigger_buy_ssl[i] else "RSI_CHECKLIST"
        levels = _levels("buy", high[i], low[i] - sl_buffer)
    elif raw_sell[i]:
        signal = "SELL"
        source = "BSL_SWEEP" if trigger_sell_bsl[i] else "RSI_CHECKLIST"
        levels = _levels("sell", low[i], high[i] + sl_buffer)

    return {
        "timestamp": str(df.index[i]),
        "close": round(float(close[i]), 2),
        "signal": signal,
        "source": source,
        "levels": levels,
        "rsi": round(float(rsi[i]), 1) if not np.isnan(rsi[i]) else None,
    }
