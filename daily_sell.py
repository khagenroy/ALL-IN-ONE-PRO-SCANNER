"""
DAILY SCAN - SELL side (ADDED 21 Nov 2026).  INTRADAY ONLY: no delivery short selling, so these are candidates for the next day's intraday SELL plan.
Mirror images of the BUY signals of daily_scan.py, on daily candles. Every function returns [(i, entry, sl, ...)] with entry = LOW of the signal candle (a stop entry BELOW the low) and sl above it.
  SPURT SELL            signals_scan._spurt_d side SELL (20x red volume spurt, CHoCH down, RSI under 40) + SMA rule (sma_rule ok_sell)
  SWEEP / OB MITIGATION SELL   all_in_one_scanner.compute_section_a on the daily candles, red candle, upper wick 70%+, volume 1.5x (same filters as the BUY side)
  BOX BREAKDOWN SELL    confirmed box breakout DOWN (spurt10_live._box_breakouts) that also closes below SMA 9, 21 and 60; SL = top of the box
  GAP DOWN RETRACE SELL gap DOWN day (open 4%+ below the day before) -> bounce of 38.2% of the leg -> first RED candle closing below the previous low; SL = highest high of the bounce
  RS CROSS DOWN + SPURT stock / NIFTY line crossed DOWN through its SMA 21 (this candle or the one before), red candle closing below SMA 9, 21 and 60 (crossing at least one from above), 5x volume
Room (sell side) = free distance from the entry DOWN to the nearest SUPPORT (zone or swing low), in R.
Never raises: any failure of one signal only drops that signal.
"""
import logging
import numpy as np
import pandas as pd

import swing_page as sw

log = logging.getLogger("daily_sell")
GAP_PCT, GAP_FIB, GAP_MAXDAYS = 4.0, 0.382, 20


def sup_levels(d, i, entry):
    """Every SUPPORT level below the entry as it was at candle i: [(price, kind)], nearest first (zones = spurt_zones SUPPORT, plus open 3/3 pivot lows no later close has broken)."""
    out = []
    try:
        import spurt_zones
        for z in (spurt_zones.live_zones(d.iloc[:i + 1]) or []):
            if z.get("type") == "SUPPORT" and isinstance(z.get("hi"), (int, float)) and 0 < z["hi"] < entry:
                out.append((float(z["hi"]), "zone"))
    except Exception as e:
        log.warning("daily sell: zones failed: %s", e)
    try:
        lo = d["low"].to_numpy(dtype=float)[:i + 1]
        cl = d["close"].to_numpy(dtype=float)[:i + 1]
        for k in range(max(3, len(lo) - 200), len(lo) - 3):
            if lo[k] < entry and lo[k] <= lo[k - 3:k].min() and lo[k] <= lo[k + 1:k + 4].min() and cl[k + 1:].min() >= lo[k]:
                out.append((float(lo[k]), "swing low"))
    except Exception as e:
        log.warning("daily sell: swing lows failed: %s", e)
    out.sort(key=lambda x: -x[0])
    return out


def room_text(entry, sl, sup):
    """(R of free room, short text) from the entry DOWN to the support."""
    r = sl - entry
    if sup is None or r <= 0:
        return None, "no support found below"
    x = (entry - sup) / r
    tail = "BLOCKED before T1" if x < 1 else "T1 free, T2 blocked" if x < 2 else "T1, T2 free, T6 blocked" if x < 6 else "all targets free"
    return round(x, 1), "%.1fR to support %s (%s)" % (x, ("%.2f" % sup).rstrip("0").rstrip("."), tail)


def levels_text(levels, n=4):
    return "  [" + " | ".join("%s %s" % (k, ("%.2f" % p).rstrip("0").rstrip(".")) for p, k in levels[:n]) + "]" if levels else "  [no levels found]"


def sell_levels(entry, sl):
    r = sl - entry
    return entry - r, entry - 2 * r, entry - 6 * r


def _mult(v):
    n = len(v)
    m = np.zeros(n)
    for i in range(sw.VOL_AVG, n):
        a = v[i - sw.VOL_AVG:i].mean()
        m[i] = v[i] / a if a > 0 else 0.0
    return m


def spurt_and_sweep_sell(d, win, htf=None):
    """[(i, name, entry, sl, volume x, note)] : SPURT SELL, SWEEP SELL, OB MITIGATION SELL. htf(i) -> text like "Daily, 1h" or None: SPURT SELL is kept only when the signal candle reached a RESISTANCE zone of a higher timeframe (Daily / 1h / 3h)."""
    import signals_scan as ss
    out = []
    n = len(d)
    o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
    m = _mult(v)
    mx = float(m[win].max()) if win.any() else 0.0
    if mx < min(getattr(ss, "SA_SWEEP_MULT", 3.0), sw.VOL_MULT):
        return out
    try:
        import sma_rule
        _b, ok_sell = sma_rule.ok_arrays(c)
    except Exception:
        ok_sell = np.ones(n, dtype=bool)
    try:
        import all_in_one_scanner as aio
        sa_sweep, sa_ob = getattr(ss, "SA_SWEEP_MULT", 3.0), getattr(ss, "SA_OB_MULT", 10.0)
        sa_wick, sa_vol = getattr(ss, "SA_WICK", 70.0), getattr(ss, "SA_VOL", 1.5)
        for k in (0, 1):
            mm = n - k
            if mm < 90 or not win[mm - 1]:
                continue
            a = aio.compute_section_a(d.iloc[:mm])
            if not a or not a.get("signal") or not a.get("levels") or a["signal"] not in ("SWEEP_SELL", "OB_SELL"):
                continue
            nm = "SWEEP SELL" if a["signal"] == "SWEEP_SELL" else "OB MITIGATION SELL"
            i = mm - 1
            if mx < (sa_sweep if nm.startswith("SWEEP") else sa_ob) or not ok_sell[i]:
                continue
            rng = h[i] - l[i]
            if rng <= 0 or c[i] >= o[i]:
                continue
            wick = (h[i] - max(o[i], c[i])) / rng * 100.0
            base = v[max(0, i - 50):i]
            if wick < sa_wick or not (v[i - 1] > 0 and v[i] / v[i - 1] >= sa_vol and len(base) >= 10 and base.mean() > 0 and v[i] / base.mean() >= sa_vol):
                continue
            lv = a["levels"]
            out.append((i, nm, float(l[i]), float(lv["sl"]), float(m[i]) or None, ""))
    except Exception as e:
        log.warning("daily sell: sweep/OB failed: %s", e)
    if mx >= ss.VS_MULT:
        try:
            rsi = ss._rsi(c)
            for i, side, sl in ss._spurt_d(o, h, l, c, v, win, rsi):
                if side == "SELL" and ok_sell[i] and sl > l[i]:
                    note = ""
                    if htf is not None:
                        t = htf(i)
                        if not t:
                            continue                                                   # not at a Daily / 1h / 3h resistance zone: no SPURT SELL
                        note = "at resistance zone: " + t
                    out.append((i, "SPURT SELL", float(l[i]), float(sl), float(m[i]) or None, note))
        except Exception as e:
            log.warning("daily sell: SPURT SELL failed: %s", e)
    return out


def box_breakdown_sell(d, win):
    """[(i, entry, sl, volume x, box candles)]"""
    import spurt10_live as sp
    c = d["close"].to_numpy(dtype=float)
    low = d["low"].to_numpy(dtype=float)
    s = [pd.Series(c).rolling(k).mean().to_numpy() for k in (9, 21, 60)]
    out = []
    for b in sp._box_breakouts(d):
        i = b["i"]
        if b["dir"] != "DOWN" or not win[i] or any(np.isnan(x[i]) or c[i] >= x[i] for x in s):
            continue
        out.append((i, float(low[i]), float(b["hi"]), b.get("vol_x"), b.get("n")))
    return out


def gap_down_retrace_sell(o, h, l, c, win):
    """[(i, entry, sl)]"""
    n = len(c)
    out = []
    for g in range(1, n):
        pc = c[g - 1]
        if not (pc > 0 and o[g] <= pc * (1 - GAP_PCT / 100.0)):
            continue
        leg_lo, bounced, pb_hi = l[g], False, None
        for j in range(g + 1, min(n, g + GAP_MAXDAYS + 1)):
            if c[j] >= pc:
                break
            if l[j] < leg_lo:
                leg_lo, bounced, pb_hi = l[j], False, None
                continue
            if h[j] >= leg_lo + GAP_FIB * (pc - leg_lo):
                bounced = True
                pb_hi = h[j] if pb_hi is None else max(pb_hi, h[j])
            if bounced and c[j] < o[j] and c[j] < l[j - 1]:
                if win[j]:
                    out.append((j, float(l[j]), float(pb_hi)))
                break
    return out


def rs_cross_down_sell(d, win, nifd):
    """[(i, entry, sl, volume x, text)]; nifd = {"dates": [...], "close": [...]} (daily_scan._NIFD)."""
    out = []
    if not sw.RS_ON or not nifd["dates"]:
        return out
    n = len(d)
    c, o, l, h, v = (d[k].to_numpy(dtype=float) for k in ("close", "open", "low", "high", "volume"))
    nm = dict(zip(nifd["dates"], nifd["close"]))
    nc = np.array([nm.get(pd.Timestamp(t).strftime("%Y-%m-%d"), np.nan) for t in d.index], dtype=float)
    sm = {k: sw._sma(c, k) for k in sw.SMAS}
    ratio = c / nc
    sma = pd.Series(ratio).rolling(sw.RS_SMA_LEN).mean().to_numpy()
    below = ratio < sma
    ok = ~np.isnan(ratio) & ~np.isnan(sma)
    for i in range(max(sw.RS_SMA_LEN + 1, sw.VOL_AVG), n):
        if not win[i]:
            continue
        if not (ok[i] and below[i] and c[i] < o[i] and h[i] > l[i]):
            continue
        if any(np.isnan(sm[k][i]) or np.isnan(sm[k][i - 1]) for k in sw.SMAS) or not all(c[i] < sm[k][i] for k in sw.SMAS):
            continue
        cr = [k for k in sw.SMAS if c[i - 1] >= sm[k][i - 1]]
        if not cr:
            continue
        a = v[i - sw.VOL_AVG:i].mean()
        if a <= 0 or v[i] / a < sw.RS_SPURT_MULT:
            continue
        if any(k >= 1 and ok[k] and ok[k - 1] and below[k] and not below[k - 1] for k in range(i, i - sw.RS_CROSS_WITHIN, -1)):
            out.append((i, float(l[i]), float(h[i]), float(v[i] / a), "RS down SMA21; crossed SMA " + " ".join(str(k) for k in cr)))
    return out
