"""
ALL IN ONE PRO SIGNALS SCAN  (feeds the page "spurt10_all_in_one_pro_signals" + one phone alert headed ALL IN ONE PRO)             ADDED 14 Nov 2026

Scans EVERY stock of the spurt scan (the 5-minute candles it has ALREADY downloaded - no extra Dhan call, no order) for the indicator's signals:
  VS SELL      spurt (>= 10x, green) -> FIRST red candle within 10 candles -> needs: resistance zone touched since the spurt, the 30-minute resistance zone
               touched since the spurt, RSI(14) peak since the spurt above 70.  Entry = low of the red candle, SL = highest high since the spurt, T1/T2 = 1x / 2x.
  VS BUY       spurt -> 2+ lower-low candles -> hold -> green higher-high candle with volume up (spurt_buy.buy_events).  SL = the hold low.
  SPURT BUY    spurt (>= 20x, green) earlier today + CHoCH UP + RSI(14) above 60.   SL = last swing LOW (ZigZag 9), T1/T2 = 1x / 2x.
  SPURT SELL   spurt (>= 20x, red) earlier today + CHoCH DOWN + RSI(14) below 40.   SL = last swing HIGH, T1/T2 = 1x / 2x.
(SWEEP and OB MITIGATION come from the Section A signals through spurt_link.py.)
Every rule is a bar-by-bar copy of the Pine code, with the same defaults. The scanner and TradingView read the same candles but may start from
different history, so compare the first few signals with the chart. Signals older than 20 minutes found on the first scan are listed without a phone alert.
Read-only. Any error is caught and logged. Needs no settings.
"""
import logging
import numpy as np
import pandas as pd

import spurt10_live as sp

log = logging.getLogger("signals_scan")
ON = True
VS_MULT, VS_AVG, VS_WAIT, VS_NEAR, VS_RSI_MIN = 10.0, 50, 10, 0.5, 70.0
SC_MULT, SC_AVG, ZZ, RSI_LEN, RSI_BUY, RSI_SELL = 20.0, 50, 9, 14, 60.0, 40.0
RR1, RR2 = 1.0, 2.0
ALERT_MINUTES = 20


def _rsi(c, n=RSI_LEN):
    """ta.rsi: Wilder (RMA) smoothing."""
    out = np.full(len(c), np.nan)
    if len(c) <= n:
        return out
    d = np.diff(c)
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    au, ad = up[:n].mean(), dn[:n].mean()
    out[n] = 100.0 if ad == 0 else 100.0 - 100.0 / (1.0 + au / ad)
    for i in range(n + 1, len(c)):
        au = (au * (n - 1) + up[i - 1]) / n
        ad = (ad * (n - 1) + dn[i - 1]) / n
        out[i] = 100.0 if ad == 0 else 100.0 - 100.0 / (1.0 + au / ad)
    return out


def _levels(entry, sl, side):
    r = abs(entry - sl)
    k = 1 if side == "BUY" else -1
    return entry + k * r * RR1, entry + k * r * RR2, entry + k * r * 6


def choch_with_swing(high, low, close, zz=ZZ):
    """spurt_choch.choch_events + the last swing low / high at that moment. Returns [(i, 'UP'|'DOWN', swing price)]."""
    n = len(close)
    trend, last_state = 1, None
    hv, lv, out = [], [], []
    for i in range(n):
        if i >= zz:
            to_up = high[i - zz] >= high[i - zz + 1: i + 1].max()
            to_dn = low[i - zz] <= low[i - zz + 1: i + 1].min()
            nt = -1 if (trend == 1 and to_dn) else 1 if (trend == -1 and to_up) else trend
            if nt != trend:
                (hv if nt == 1 else lv).append(high[i - zz] if nt == 1 else low[i - zz])
                trend = nt
        if len(lv) > 1 and close[i] < lv[-1]:
            if last_state is None or last_state == "up":
                out.append((i, "DOWN", hv[-1] if hv else float(high[i])))
            last_state = "down"
        if len(hv) > 1 and close[i] > hv[-1]:
            if last_state is None or last_state == "down":
                out.append((i, "UP", lv[-1] if lv else float(low[i])))
            last_state = "up"
    return out


def _spurt_d(o, h, l, c, v, today, rsi):
    """Section D. Returns [(i, 'BUY'|'SELL', sl)] for today's bars."""
    n = len(c)
    out = []
    mult = np.zeros(n)
    for i in range(SC_AVG, n):
        a = v[i - SC_AVG:i].mean()
        mult[i] = v[i] / a if a > 0 else 0.0
    up_bar = dn_bar = None
    ev = {i: (k, s) for i, k, s in choch_with_swing(h, l, c)}
    for i in range(n):
        if not today[i]:
            up_bar = dn_bar = None
            continue
        if mult[i] >= SC_MULT:
            if c[i] > o[i]:
                up_bar = i
            if c[i] < o[i]:
                dn_bar = i
        if i in ev:
            k, sw = ev[i]
            if k == "UP" and up_bar is not None and i >= up_bar and rsi[i] > RSI_BUY:
                out.append((i, "BUY", float(sw)))
            if k == "DOWN" and dn_bar is not None and i >= dn_bar and rsi[i] < RSI_SELL:
                out.append((i, "SELL", float(sw)))
    return out


def _vs_sell(o, h, l, c, v, today, rsi, ist, d):
    """Section V (VS SELL). Returns [(i, entry, sl)]."""
    import spurt_zones
    n = len(c)
    tol = VS_NEAR / 100.0
    # 30-minute candles (TradingView NSE: 09:15, 09:45 ...), a 30m candle counts once it is complete
    dfi = d.copy()
    dfi.index = pd.DatetimeIndex(ist)
    d30 = dfi.resample("30min", offset="15min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()

    def res_touch_5(i, lo_i, hi_i):
        for z in spurt_zones.live_zones(d.iloc[:i + 1]):
            if z["type"] == "RESISTANCE" and lo_i <= z["hi"] * (1 + tol) and hi_i >= z["lo"] * (1 - tol):
                return True
        return False

    def htf_touch(i, lo_i, hi_i):
        done = d30[d30.index + pd.Timedelta(minutes=30) <= ist[i]]
        zs = [z for z in spurt_zones.live_zones(done) if z["type"] == "RESISTANCE"]
        if not zs:
            return False
        z = min(zs, key=lambda q: q["lo"])
        return lo_i <= z["hi"] * (1 + tol) and hi_i >= z["lo"] * (1 - tol)

    out = []
    sBar = None
    hi_since = None
    res_since = htf_since = False
    rsi_peak = None
    for i in range(VS_AVG, n):
        if not today[i]:
            sBar = None
            continue
        a = v[i - VS_AVG:i].mean()
        mult = v[i] / a if a > 0 else 0.0
        spurt = mult >= VS_MULT and c[i] > o[i]
        if spurt:
            sBar = i
        active = sBar is not None and i > sBar and i - sBar <= VS_WAIT
        if spurt or active:
            tr = res_touch_5(i, l[i], h[i])
            th = htf_touch(i, l[i], h[i])
            if spurt:
                hi_since, res_since, htf_since, rsi_peak = float(h[i]), tr, th, float(rsi[i])
            else:
                hi_since = max(hi_since if hi_since is not None else h[i], float(h[i]))
                res_since = res_since or tr
                htf_since = htf_since or th
                rsi_peak = max(rsi_peak if rsi_peak is not None else rsi[i], float(rsi[i]))
        if active and c[i] < o[i]:                                     # the FIRST red candle: signal if everything is there, otherwise the setup is over
            if res_since and htf_since and rsi_peak is not None and rsi_peak > VS_RSI_MIN:
                out.append((i, float(l[i]), float(hi_since)))
            sBar = None
    return out


def update(rows, now_ist):
    """Called by spurt10_live.run_once (same place as spurt_buy). Never raises."""
    if not ON:
        return
    try:
        import live_scanner as ls
        import signals_page
        import spurt_buy
        today_s = now_ist.strftime("%Y-%m-%d")
        fresh = []
        for r in rows:
            sym = r["symbol"]
            if r.get("_df5") is None:
                continue
            try:
                d = ls.drop_forming_bars(r["_df5"], 5, r.get("_as_of"))
                if d is None or len(d) < 60:
                    continue
                o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
                ist = [(t + pd.Timedelta(minutes=330)) for t in d.index]
                today = np.array([t.strftime("%Y-%m-%d") == today_s for t in ist])
                if not today.any():
                    continue
                n0 = len(v)
                idx = [i for i in range(max(VS_AVG, 0), n0) if today[i]]
                if not any(v[i - VS_AVG:i].mean() > 0 and v[i] / v[i - VS_AVG:i].mean() >= VS_MULT for i in idx):
                    continue                                            # no 10x spurt today: none of the rules can fire
                rsi = _rsi(c)
                found = []                                              # (i, signal, side, entry, sl)
                for i, entry, sl in _vs_sell(o, h, l, c, v, today, rsi, ist, d):
                    found.append((i, "VS SELL", "SELL", entry, sl))
                for e in spurt_buy.buy_events(o, h, l, c, v, today, VS_MULT):
                    if float(e["sl"]) < float(c[e["i"]]):
                        found.append((e["i"], "VS BUY", "BUY", float(c[e["i"]]), float(e["sl"])))
                for i, side, sl in _spurt_d(o, h, l, c, v, today, rsi):
                    found.append((i, "SPURT BUY" if side == "BUY" else "SPURT SELL", side, float(c[i]), sl))
                for i, name, side, entry, sl in found:
                    if not today[i] or abs(entry - sl) <= 0:
                        continue
                    if (side == "BUY" and sl >= entry) or (side == "SELL" and sl <= entry):
                        continue
                    t1, t2, t6 = _levels(entry, sl, side)
                    cand = ist[i].strftime("%H:%M")
                    key = "%s|%s|%s|%s" % (today_s, sym, name, cand)
                    if signals_page.add(sym, name, side, entry, sl, t1, t2, t6, "", key, "scan"):
                        age = (now_ist.replace(tzinfo=None) - ist[i].to_pydatetime().replace(tzinfo=None)).total_seconds() / 60.0
                        if age <= ALERT_MINUTES:
                            fresh.append(dict(signal=name, side=side, symbol=sym, price=entry, sl=sl, t1=t1, t2=t2, t6=t6, candle=cand))
            except Exception as e:
                log.warning("signals_scan: %s failed: %s", sym, e)
        if fresh:
            signals_page.notify(fresh)
            log.info("signals_scan: new signals %s", [(x["symbol"], x["signal"]) for x in fresh])
    except Exception as e:
        log.warning("signals_scan step failed: %s", e)
