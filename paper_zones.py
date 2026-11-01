"""
Live support / resistance zones for the volume-spurt pages (the "Support zone (10m)" and "Resistance zone (10m)" columns).

WHY THIS FILE EXISTS: the zone columns were always empty because spurt10_live asked all_in_one_scanner.compute_section_a()
for zones (with_zones=True), and that option was removed from all_in_one_scanner.py ("Remove OB Zone Watch and
support-resistance reporting"), so the call failed silently every time. This file rebuilds ONLY the order-block zones, with the
same rules as all_in_one_scanner.py / the ALL IN ONE PRO indicator (same constants, same ATR, same ZigZag + CHoCH + order block
logic). It does not touch all_in_one_scanner.py, the scanner signals or the volume-spurt calculation.

  bullish order block  = SUPPORT     zone [value, value + ATR]      (value = lowest low of the swing that ended in a bullish CHoCH)
  bearish order block  = RESISTANCE  zone [value - ATR, value]      (value = highest high of the swing that ended in a bearish CHoCH)
  only the newest NUMBER_OB_SHOW (2) zones of each side are live; a candle that CLOSES through a zone deletes it.
"""
import numpy as np

try:
    import all_in_one_scanner as _sa
    ZIGZAG_LEN = _sa.ZIGZAG_LEN
    NUMBER_OB_SHOW = _sa.NUMBER_OB_SHOW
    ATR_LEN = _sa.ATR_LEN
    _atr = _sa._atr
except Exception:                                   # standalone fallback with the same defaults
    ZIGZAG_LEN, NUMBER_OB_SHOW, ATR_LEN = 9, 2, 14

    def _atr(high, low, close, length):
        n = len(high)
        tr = np.empty(n)
        tr[0] = high[0] - low[0]
        for i in range(1, n):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
        out = np.full(n, np.nan)
        if n >= length:
            out[length - 1] = tr[:length].mean()
            for i in range(length, n):
                out[i] = (out[i - 1] * (length - 1) + tr[i]) / length
        return out


def _times(df):
    """Candle open times as float seconds, from the DataFrame's date index. None when the index is not a date index."""
    try:
        import pandas as pd
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 1:
            t = df.index.asi8.astype(float) / 1e9
            if np.all(np.isfinite(t)) and np.all(np.diff(t) > 0):
                return t
    except Exception:
        pass
    return None


def live_zones(df):
    """df = candles (open/high/low/close, date index), oldest first, last row = latest closed candle.
    Returns [{"type": "SUPPORT"|"RESISTANCE", "lo": float, "hi": float, "value": float}, ...]  ([] if not enough candles)."""
    try:
        high = df["high"].to_numpy(dtype=float)
        low = df["low"].to_numpy(dtype=float)
        close = df["close"].to_numpy(dtype=float)
        n = len(df)
        if n < ZIGZAG_LEN * 2 + ATR_LEN:
            return []
        atr = _atr(high, low, close, ATR_LEN)
        tt = _times(df)                              # candle open times (None = not available -> count candles)

        def _lookback(i, piv_list):
            """How many candles back the order block's high/low is searched. The indicator measures it in TIME
            ((time of previous candle - time of the pivot) / candle length), so weekends / overnight gaps count as if they were
            candles - this copies that, so the zones are the same as on the TradingView chart."""
            if not piv_list:
                return 15
            if tt is not None and i >= 1 and tt[i] - tt[i - 1] > 0:
                return max(int((tt[i - 1] - tt[piv_list[-1]]) / (tt[i] - tt[i - 1])), 0)
            return max(i - piv_list[-1] - 1, 0)      # fallback when the candle times are unknown (same as before)

        trend = np.ones(n, dtype=int)
        high_vals, low_vals = [], []
        last_state = None
        choch_bull = np.zeros(n, dtype=bool)
        choch_bear = np.zeros(n, dtype=bool)
        for i in range(ZIGZAG_LEN, n):
            to_up = high[i - ZIGZAG_LEN] >= high[i - ZIGZAG_LEN: i + 1].max()
            to_down = low[i - ZIGZAG_LEN] <= low[i - ZIGZAG_LEN: i + 1].min()
            prev = trend[i - 1]
            new = -1 if (prev == 1 and to_down) else (1 if (prev == -1 and to_up) else prev)
            trend[i] = new
            if new != prev and new == 1:
                high_vals.append(high[i - ZIGZAG_LEN])
            if new != prev and new == -1:
                low_vals.append(low[i - ZIGZAG_LEN])
            if len(low_vals) > 1 and close[i] < low_vals[-1]:
                if last_state is None or last_state == "up":
                    choch_bear[i] = True
                last_state = "down"
            if len(high_vals) > 1 and close[i] > high_vals[-1]:
                if last_state is None or last_state == "down":
                    choch_bull[i] = True
                last_state = "up"

        bull, bear = [], []                         # {"value", "atr"}
        high_idx, low_idx = [], []
        for i in range(ZIGZAG_LEN, n):
            if trend[i] != trend[i - 1] and trend[i] == 1:
                high_idx.append(i - ZIGZAG_LEN)
            if trend[i] != trend[i - 1] and trend[i] == -1:
                low_idx.append(i - ZIGZAG_LEN)
            if choch_bull[i]:
                lb = _lookback(i, high_idx)
                seg = low[max(i - lb, 0): i + 1]
                bull.append({"value": float(seg.min()), "atr": float(atr[i])})
                if len(bull) > 20:
                    bull.pop(0)
            if choch_bear[i]:
                lb = _lookback(i, low_idx)
                seg = high[max(i - lb, 0): i + 1]
                bear.append({"value": float(seg.max()), "atr": float(atr[i])})
                if len(bear) > 20:
                    bear.pop(0)
            # a close through the zone deletes it - only checked for the newest NUMBER_OB_SHOW, like the indicator
            keep = []
            for k, ob in enumerate(reversed(bull)):
                if k < NUMBER_OB_SHOW and close[i] < ob["value"]:
                    continue
                keep.append(ob)
            bull = list(reversed(keep))
            keep = []
            for k, ob in enumerate(reversed(bear)):
                if k < NUMBER_OB_SHOW and close[i] > ob["value"]:
                    continue
                keep.append(ob)
            bear = list(reversed(keep))

        out = []
        for ob in bull[-NUMBER_OB_SHOW:]:
            out.append({"type": "SUPPORT", "lo": round(ob["value"], 2), "hi": round(ob["value"] + ob["atr"], 2), "value": ob["value"]})
        for ob in bear[-NUMBER_OB_SHOW:]:
            out.append({"type": "RESISTANCE", "lo": round(ob["value"] - ob["atr"], 2), "hi": round(ob["value"], 2), "value": ob["value"]})
        return out
    except Exception:
        return []


def resistance_touch(df, start, end, tol_pct=0.5):
    """Did price touch a live RESISTANCE zone on any candle from index `start` to `end` (inclusive)?
    The zones are rebuilt as they stood on each of those candles, exactly like the indicator's "touched since the spurt".
    Returns the touched zone as 'lo-hi' text (the latest one), or None."""
    try:
        high = df["high"].to_numpy(dtype=float)
        low = df["low"].to_numpy(dtype=float)
        tol = tol_pct / 100.0
        found = None
        for k in range(max(start, 0), end + 1):
            for z in live_zones(df.iloc[:k + 1]):
                if z["type"] == "RESISTANCE" and low[k] <= z["hi"] * (1 + tol) and high[k] >= z["lo"] * (1 - tol):
                    found = f"{z['lo']:g}-{z['hi']:g}"
        return found
    except Exception:
        return None
