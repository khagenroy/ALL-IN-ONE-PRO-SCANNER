"""
RSI FLUSH LIVE SCAN - faithful Python port of the TradingView indicator
"RSI Flush with Volume Filter + Break Confirm" (RSI_FLUSH_BREAK_CONFIRM_*.pine).

It rides on the bars the intraday scan already downloads (10m / 1H / 4H) - NO extra Dhan calls,
places NO orders, and cannot affect Section A / Section B (live_scanner wraps every call in
its own try/except). Results are shown on the same /scanner page, in the Section A area.

THE RULE (same as the Pine indicator, same defaults)
  Signal candle : RSI(14) was <= 30 (buy) / >= 70 (sell) on any of the previous 4 candles and the
                  current candle's RSI is back above 30 / below 70, AND volume >= 1.5 x its 20-bar
                  average, AND at least 8 candles since the last signal on that side.
  Confirmation  : a LATER candle (within 4 candles) trades through the signal candle's HIGH (buy) /
                  LOW (sell). Dropped if a candle CLOSES beyond the stop first. Anything still
                  pending at a new trading day is dropped.
  Entry         : the signal candle's high (buy) / low (sell).
  Stop          : lowest low (buy) / highest high (sell) of the last 5 candles incl. the signal candle.
  Targets       : T1..T6 = entry +/- 1R..6R.
  Cascade stop  : T1 -> SL to cost, T2 -> SL to T1, T3 -> SL to T2, T4 -> SL to T3, T5 -> SL to T4,
                  T6 = final exit. Stop is wick based and checked after any stage move on the same candle.

WHAT THE SCAN REPORTS (per symbol / timeframe, today's session only)
  PENDING  : signal candle formed, waiting for the break (entry = trigger level)
  ACTIVE   : break happened today and the trade is still open on the last closed candle
  STOPPED  : break happened today, trade already stopped out (SL HIT, or stopped after SL moved)
  T6 DONE  : break happened today, T6 reached

Everything is recomputed from the downloaded bars each cycle (stateless), so nothing is stored
between runs and a Render restart loses nothing.
"""

import os

import numpy as np
import pandas as pd

from section_b import _rsi  # same Wilder RSI the rest of the scanner uses (= Pine ta.rsi)

# --- defaults identical to the Pine indicator ---
RSI_LEN = 14
RSI_OB = 70.0
RSI_OS = 30.0
MAX_LOOKBACK = 4
COOLDOWN_BARS = 8
CONFIRM_EXPIRY = 4
USE_VOL_FILTER = True
VOL_MA_LEN = 20
VOL_MULT = 1.5
SL_LOOKBACK = 5
RR = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

# Which scan modes show it (the page for that mode gets the RSI Flush table). Default: intraday only.
ENABLED = os.environ.get("RSIFLUSH_SCAN", "true").strip().lower() == "true"
MODES = {m.strip() for m in os.environ.get("RSIFLUSH_SCAN_MODES", "intraday").split(",") if m.strip()}

WINDOW = 200            # bars replayed per symbol/timeframe (state never lasts more than ~13 bars, RSI uses the full series)
STAGE_TXT = ["SL original", "SL at cost", "SL at T1", "SL at T2", "SL at T3", "SL at T4"]


def _r(x):
    return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), 2)


def _ist_date(ts) -> str:
    return (pd.Timestamp(ts) + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d")


def _levels(side: str, entry: float, sl: float):
    risk = entry - sl if side == "BUY" else sl - entry
    if risk <= 0:
        risk = entry * 0.01
        sl = entry - risk if side == "BUY" else entry + risk
    sign = 1.0 if side == "BUY" else -1.0
    return sl, risk, [entry + sign * risk * k for k in RR]


def _step_trade(tr: dict, hi: float, lo: float):
    """One candle of the cascade engine, in the same order as the Pine script:
    stage move (from the stage held BEFORE this candle) -> wick stop check -> T6 check."""
    side = tr["side"]
    stage = tr["stage"]
    t = tr["t"]
    if side == "BUY":
        hit = stage <= 4 and hi >= t[stage]                 # stage 0 needs T1, stage 1 needs T2 ... stage 4 needs T5
        if hit:
            tr["stage"] = stage + 1
            tr["sl_now"] = tr["entry"] if stage == 0 else t[stage - 1]
        if lo <= tr["sl_now"]:
            tr["state"] = "STOPPED"
            return
        if hi >= t[5]:
            tr["state"] = "T6 DONE"
    else:
        hit = stage <= 4 and lo <= t[stage]
        if hit:
            tr["stage"] = stage + 1
            tr["sl_now"] = tr["entry"] if stage == 0 else t[stage - 1]
        if hi >= tr["sl_now"]:
            tr["state"] = "STOPPED"
            return
        if lo <= t[5]:
            tr["state"] = "T6 DONE"


def live_setups(df: pd.DataFrame, tf: str, sym: str) -> list:
    """Rows for TODAY's session (the session of the last closed bar). Never raises on short data - returns []."""
    if df is None or df.empty:
        return []
    n = len(df)
    if n < max(RSI_LEN, VOL_MA_LEN, SL_LOOKBACK) + MAX_LOOKBACK + 5:
        return []

    o = df["open"].to_numpy(dtype=float)
    hi = df["high"].to_numpy(dtype=float)
    lo = df["low"].to_numpy(dtype=float)
    cl = df["close"].to_numpy(dtype=float)
    vol = df["volume"].to_numpy(dtype=float)
    idx = df.index

    rsi = _rsi(cl, RSI_LEN)
    vol_ma = pd.Series(vol).rolling(VOL_MA_LEN, min_periods=VOL_MA_LEN).mean().to_numpy()
    days = np.array([_ist_date(t) for t in idx[-(WINDOW + 1):]])       # per-bar IST date for the replay window
    first = max(1, n - WINDOW)
    today = days[-1]

    last_buy = last_sell = -999
    pend = {"BUY": None, "SELL": None}      # {bar, level, sl}
    trades = []                             # every trade confirmed in the replay window
    rows = []

    for i in range(first, n):
        d_i = days[i - (n - len(days))]
        d_prev = days[i - 1 - (n - len(days))]
        new_day = d_i != d_prev
        if new_day:
            pend["BUY"] = pend["SELL"] = None
            for tr in trades:                           # intraday positions are squared off at the new day
                if tr["state"] == "ACTIVE":
                    tr["state"] = "CLOSED"

        # 0) existing trades: one candle of the cascade (a trade confirmed on THIS candle is stepped below)
        for tr in trades:
            if tr["state"] == "ACTIVE" and tr["conf_i"] < i:
                _step_trade(tr, hi[i], lo[i])

        # 1) pending trades armed on EARLIER candles
        for side in ("BUY", "SELL"):
            p = pend[side]
            if p is None or i <= p["bar"]:
                continue
            if i - p["bar"] > CONFIRM_EXPIRY:
                pend[side] = None
            elif side == "BUY" and cl[i] < p["sl"]:
                pend[side] = None
            elif side == "SELL" and cl[i] > p["sl"]:
                pend[side] = None
            elif (side == "BUY" and hi[i] > p["level"]) or (side == "SELL" and lo[i] < p["level"]):
                sl, risk, t = _levels(side, p["level"], p["sl"])
                tr = {"side": side, "sym": sym, "tf": tf, "entry": p["level"], "sl0": sl, "sl_now": sl, "risk": risk,
                      "t": t, "stage": 0, "state": "ACTIVE", "signal_i": p["bar"], "conf_i": i, "rsi": p["rsi"]}
                _step_trade(tr, hi[i], lo[i])         # the confirming candle itself can already reach T1 / the stop
                trades.append(tr)
                pend[side] = None

        # 2) arm new signal candles
        if i >= max(RSI_LEN, VOL_MA_LEN) and not np.isnan(rsi[i]):
            prev = rsi[max(0, i - MAX_LOOKBACK): i]
            prev = prev[~np.isnan(prev)]
            vol_ok = (not USE_VOL_FILTER) or (not np.isnan(vol_ma[i]) and vol[i] >= vol_ma[i] * VOL_MULT)
            if prev.size and vol_ok and i >= SL_LOOKBACK:
                buy_sig = prev.min() <= RSI_OS and rsi[i] > RSI_OS and (i - last_buy >= COOLDOWN_BARS)
                sell_sig = prev.max() >= RSI_OB and rsi[i] < RSI_OB and (i - last_sell >= COOLDOWN_BARS)
                if buy_sig:
                    last_buy = i
                    sl = lo[i - SL_LOOKBACK + 1: i + 1].min()
                    if sl >= hi[i]:
                        sl = hi[i] * 0.99
                    pend["BUY"] = {"bar": i, "level": hi[i], "sl": sl, "rsi": rsi[i]}
                if sell_sig:
                    last_sell = i
                    sl = hi[i - SL_LOOKBACK + 1: i + 1].max()
                    if sl <= lo[i]:
                        sl = lo[i] * 1.01
                    pend["SELL"] = {"bar": i, "level": lo[i], "sl": sl, "rsi": rsi[i]}

    def ts(i):
        return str(idx[i])

    # trades confirmed TODAY
    for tr in trades:
        if _ist_date(idx[tr["conf_i"]]) != today:
            continue
        t = tr["t"]
        state = tr["state"]
        if state == "STOPPED":
            detail = "SL hit" if tr["stage"] == 0 else f"stopped ({STAGE_TXT[tr['stage']]})"
        elif state == "T6 DONE":
            detail = "T6 reached"
        else:
            detail = STAGE_TXT[tr["stage"]]
        rows.append({
            "symbol": sym, "timeframe": tf, "side": tr["side"], "status": state, "detail": detail,
            "close": _r(cl[-1]), "entry": _r(tr["entry"]), "sl": _r(tr["sl0"]), "sl_now": _r(tr["sl_now"]),
            "T1": _r(t[0]), "T2": _r(t[1]), "T3": _r(t[2]), "T4": _r(t[3]), "T5": _r(t[4]), "T6": _r(t[5]),
            "risk_pct": _r(tr["risk"] / tr["entry"] * 100.0), "rsi": _r(tr["rsi"]),
            "signal_ts": ts(tr["signal_i"]), "confirm_ts": ts(tr["conf_i"]),
            "confirm_close": _r(cl[tr["conf_i"]]),
        })

    # still waiting for the break on the last closed candle
    for side, p in pend.items():
        if p is None or _ist_date(idx[p["bar"]]) != today:
            continue
        sl, risk, t = _levels(side, p["level"], p["sl"])
        left = CONFIRM_EXPIRY - (n - 1 - p["bar"])
        rows.append({
            "symbol": sym, "timeframe": tf, "side": side, "status": "PENDING",
            "detail": f"break {'above' if side == 'BUY' else 'below'} {_r(p['level'])} within {max(left, 0)} more candle(s)",
            "close": _r(cl[-1]), "entry": _r(p["level"]), "sl": _r(sl), "sl_now": _r(sl),
            "T1": _r(t[0]), "T2": _r(t[1]), "T3": _r(t[2]), "T4": _r(t[3]), "T5": _r(t[4]), "T6": _r(t[5]),
            "risk_pct": _r(risk / p["level"] * 100.0), "rsi": _r(p["rsi"]),
            "signal_ts": ts(p["bar"]), "confirm_ts": "", "confirm_close": None,
        })
    return rows
