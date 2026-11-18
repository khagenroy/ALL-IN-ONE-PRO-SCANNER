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
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

import spurt10_live as sp

IST_TZ = ZoneInfo("Asia/Kolkata")

log = logging.getLogger("signals_scan")
ON = True
VS_MULT, VS_AVG, VS_WAIT, VS_NEAR, VS_RSI_MIN = 10.0, 50, 10, 0.5, 70.0
SC_MULT, SC_AVG, ZZ, RSI_LEN, RSI_BUY, RSI_SELL = 20.0, 50, 9, 14, 60.0, 40.0
RR1, RR2 = 1.0, 2.0
ALERT_MINUTES = 20                       # a signal found more than this many minutes after its candle CLOSED is listed without a phone alert


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


TFS = (5, 15)                    # CHANGED 17 Nov: every signal is looked for on BOTH timeframes, 5-minute and 15-minute; each signal carries its timeframe
SA_SWEEP_MULT, SA_OB_MULT = 3.0, 10.0      # Section A (SWEEP / OB MITIGATION) only on stocks that spurted this much today (same limits as spurt_link.py)
SA_WICK, SA_VOL = 70.0, 1.5                # rejection candle: wick of 70% or more, volume 1.5x the previous candle and 1.5x the 50-candle average (same as spurt_link.py)


def _mults(v, today):
    """volume multiple of every candle (vs the average of the 50 candles before it); 0 where it cannot be computed."""
    n = len(v)
    m = np.zeros(n)
    for i in range(VS_AVG, n):
        a = v[i - VS_AVG:i].mean()
        m[i] = v[i] / a if a > 0 else 0.0
    return m


def _htf30_touch(d, ist, i, side):
    """SWEEP / OB MITIGATION need the HIGHER timeframe too (same idea as VS SELL): candle i must touch the nearest 30-minute zone of the right side
    (SELL = the 30-minute RESISTANCE zone with the lowest bottom, BUY = the 30-minute SUPPORT zone with the highest top), with the same 0.5% tolerance.
    Only 30-minute candles that were already complete when candle i started are used. Never raises (False = not touched)."""
    try:
        import spurt_zones
        dfi = d.iloc[:i + 1].copy()
        dfi.index = pd.DatetimeIndex(list(ist)[:i + 1])
        d30 = dfi.resample("30min", offset="15min", label="left", closed="left").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        done = d30[d30.index + pd.Timedelta(minutes=30) <= ist[i]]
        want = "RESISTANCE" if side == "SELL" else "SUPPORT"
        zs = [z for z in spurt_zones.live_zones(done) if z["type"] == want]
        if not zs:
            return False
        z = min(zs, key=lambda q: q["lo"]) if side == "SELL" else max(zs, key=lambda q: q["hi"])
        tol = VS_NEAR / 100.0
        return float(d["low"].iloc[i]) <= z["hi"] * (1 + tol) and float(d["high"].iloc[i]) >= z["lo"] * (1 - tol)
    except Exception:
        return False


def _section_a(d, today, ok_buy, ok_sell, mx):
    """SWEEP / OB MITIGATION of the last two closed candles of this timeframe: Section A (all_in_one_scanner) + the spurt-stock rule + a REJECTION candle
    (wick >= SA_WICK % on the right side, right colour, volume >= SA_VOL x previous candle and x the 50-candle average). Returns [(i, name, side, entry, sl)]."""
    out = []
    try:
        import all_in_one_scanner as aio
        n = len(d)
        o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
        for k in (0, 1):
            m = n - k
            if m < 90 or not today[m - 1]:
                continue
            a = aio.compute_section_a(d.iloc[:m])
            if not a or not a.get("signal") or not a.get("levels"):
                continue
            nm, sd = {"SWEEP_BUY": ("SWEEP", "BUY"), "SWEEP_SELL": ("SWEEP", "SELL"), "OB_BUY": ("OB MITIGATION", "BUY"), "OB_SELL": ("OB MITIGATION", "SELL")}[a["signal"]]
            i = m - 1
            if mx < (SA_SWEEP_MULT if nm == "SWEEP" else SA_OB_MULT):
                continue
            if not (ok_buy[i] if sd == "BUY" else ok_sell[i]):
                continue
            rng = h[i] - l[i]
            if rng <= 0:
                continue
            wick = ((min(o[i], c[i]) - l[i]) if sd == "BUY" else (h[i] - max(o[i], c[i]))) / rng * 100.0
            if wick < SA_WICK or not ((c[i] > o[i]) if sd == "BUY" else (c[i] < o[i])):
                continue
            base = v[max(0, i - 50):i]
            if not (v[i - 1] > 0 and v[i] / v[i - 1] >= SA_VOL and len(base) >= 10 and base.mean() > 0 and v[i] / base.mean() >= SA_VOL):
                continue
            if not _htf30_touch(d, [t + pd.Timedelta(minutes=330) for t in d.index[:m]], i, sd):
                continue                                                  # the 30-minute zone of the right side was not touched
            lv = a["levels"]
            out.append((i, nm, sd, float(lv["entry"]), float(lv["sl"])))
    except Exception as e:
        log.warning("signals_scan: section A failed: %s", e)
    return out


def _frames(df5, as_of, ls):
    """{5: closed 5-minute candles, 15: closed 15-minute candles (merged from the same 5-minute download - no extra Dhan call)}."""
    out = {}
    d5 = ls.drop_forming_bars(df5, 5, as_of)
    if d5 is not None and len(d5) >= 60:
        out[5] = d5
        try:
            d15 = ls.drop_forming_bars(ls.build_merged_bars(df5, 3), 15, as_of)
            if d15 is not None and len(d15) >= 60:
                out[15] = d15
        except Exception as e:
            log.warning("signals_scan: 15-minute candles failed: %s", e)
    return out


_last = {"syms": [], "t": 0.0, "slot": -1, "started": False}
_run_lock = threading.Lock()


def update(rows, now_ist):
    """Called by spurt10_live.run_once (same place as spurt_buy), once per main cycle. Remembers the spurt stocks of this cycle for the 5-minute timer below. Never raises."""
    if not ON:
        return
    try:
        _last["syms"] = [r["symbol"] for r in rows if r.get("symbol")][:60]
        _last["t"] = time.time()
        if not _last["started"]:
            _last["started"] = True
            threading.Thread(target=_timer_loop, name="signals_scan_timer", daemon=True).start()
    except Exception as e:
        log.warning("signals_scan: timer start failed: %s", e)
    with _run_lock:
        _run(rows, now_ist)
        _follow_extra(now_ist, {r.get("symbol") for r in rows})


def _timer_loop():
    """ADDED 17 Nov: the six signals are looked for EVERY 5 MINUTES (about 1.5 minutes after each 5-minute candle closes), not only once per 10-minute spurt cycle.
    It uses the spurt stocks of the latest main cycle and downloads their 5-minute candles itself. A main cycle that ran in the last 3 minutes already did the job, so the timer skips that slot."""
    while True:
        try:
            time.sleep(15)
            now = datetime.now(IST_TZ).replace(tzinfo=None)
            m = now.hour * 60 + now.minute
            if m < 9 * 60 + 20 or m > 15 * 60 + 35 or now.weekday() >= 5:
                continue
            slot = m // 5
            if now.minute % 5 != 1 or now.second < 20 or slot == _last["slot"]:
                continue
            _last["slot"] = slot
            if time.time() - _last["t"] < 180 or not _last["syms"]:
                continue
            if not _run_lock.acquire(blocking=False):
                continue
            try:
                _timer_run(now)
            finally:
                _run_lock.release()
        except Exception as e:
            log.warning("signals_scan timer: %s", e)


def _timer_run(now_ist):
    import live_scanner as ls
    from concurrent.futures import ThreadPoolExecutor
    uni = dict(sp._load_universe())
    syms = [x for x in _last["syms"] if x in uni]
    as_of = pd.Timestamp(datetime.utcnow())

    def one(sym):
        try:
            return {"symbol": sym, "_df5": ls.fetch_intraday_history(uni[sym], "NSE_EQ", 5, getattr(sp, "HISTORY_DAYS", 5)), "_as_of": as_of}
        except Exception as e:
            log.warning("signals_scan timer: candles of %s failed: %s", sym, e)
            return None
    with ThreadPoolExecutor(max_workers=max(1, sp.WORKERS)) as ex:
        rows = [r for r in ex.map(one, syms) if r]
    _run(rows, now_ist)
    _follow_extra(now_ist, {r["symbol"] for r in rows})


def _follow_extra(now_ist, skip):
    """CORRECTION 18 Nov: the result (target / stop-loss) of a signal was only followed while its stock was still on the current spurt list, so most signals stayed
    OPEN for the whole day. Here every stock that still has an OPEN signal today, and is not on this cycle's list, gets its 5-minute candles and its signals followed.
    Only the follow-up (results) - it finds no new signals. Never raises."""
    try:
        import live_scanner as ls
        import signals_page
        from concurrent.futures import ThreadPoolExecutor
        uni = dict(sp._load_universe())
        syms = [x for x in signals_page.open_symbols() if x not in skip and x in uni]
        if not syms:
            return
        today_s = now_ist.strftime("%Y-%m-%d")
        day_over = now_ist.hour * 60 + now_ist.minute >= 15 * 60 + 30
        as_of = pd.Timestamp(datetime.utcnow())

        def one(sym):
            try:
                df5 = ls.fetch_intraday_history(uni[sym], "NSE_EQ", 5, getattr(sp, "HISTORY_DAYS", 5))
                d5 = ls.drop_forming_bars(df5, 5, as_of)
                if d5 is None or len(d5) == 0:
                    return
                ist5 = [(t + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M") for t in d5.index]
                bars = [(ist5[j], float(d5["open"].iloc[j]), float(d5["high"].iloc[j]), float(d5["low"].iloc[j]), float(d5["close"].iloc[j]))
                        for j in range(len(d5)) if ist5[j].startswith(today_s)]
                if bars:
                    signals_page.update_outcomes(sym, bars, day_over)
            except Exception as e:
                log.warning("signals_scan: follow-up of %s failed: %s", sym, e)
        with ThreadPoolExecutor(max_workers=max(1, min(4, sp.WORKERS))) as ex:
            list(ex.map(one, syms))
    except Exception as e:
        log.warning("signals_scan: extra follow-up failed: %s", e)


def _run(rows, now_ist):
    if not ON:
        return
    try:
        import live_scanner as ls
        import signals_page
        import spurt_buy
        today_s = now_ist.strftime("%Y-%m-%d")
        day_over = now_ist.hour * 60 + now_ist.minute >= 15 * 60 + 30
        fresh = []
        for r in rows:
            sym = r["symbol"]
            if r.get("_df5") is None:
                continue
            try:
                frames = _frames(r["_df5"], r.get("_as_of"), ls)
                if 5 not in frames:
                    continue
                d5 = frames[5]
                for tf in TFS:
                    d = frames.get(tf)
                    if d is None:
                        continue
                    o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
                    ist = [(t + pd.Timedelta(minutes=330)) for t in d.index]
                    today = np.array([t.strftime("%Y-%m-%d") == today_s for t in ist])
                    if not today.any():
                        continue
                    mult = _mults(v, today)
                    mx = float(mult[today].max()) if today.any() else 0.0
                    if mx < min(VS_MULT, SA_SWEEP_MULT):
                        continue                                            # no spurt today on this timeframe: none of the rules can fire
                    rsi = _rsi(c)
                    try:                                                    # SMA 9/21/60 distance rule (sma_rule.py), same as the indicator
                        import sma_rule
                        ok_buy, ok_sell = sma_rule.ok_arrays(c)
                    except Exception:
                        ok_buy = ok_sell = np.ones(len(c), dtype=bool)
                    found = []                                              # (i, signal, side, entry, sl)
                    for i, nm, sd, en, sl in _section_a(d, today, ok_buy, ok_sell, mx):
                        found.append((i, nm, sd, en, sl))
                    if mx >= VS_MULT:
                        for i, entry, sl in _vs_sell(o, h, l, c, v, today, rsi, ist, d):
                            if ok_sell[i]:
                                found.append((i, "VS SELL", "SELL", entry, sl))
                        for e in spurt_buy.buy_events(o, h, l, c, v, today, VS_MULT):
                            if float(e["sl"]) < float(c[e["i"]]):
                                found.append((e["i"], "VS BUY", "BUY", float(c[e["i"]]), float(e["sl"])))
                        for i, side, sl in _spurt_d(o, h, l, c, v, today, rsi):
                            if not (ok_buy[i] if side == "BUY" else ok_sell[i]):
                                continue
                            found.append((i, "SPURT BUY" if side == "BUY" else "SPURT SELL", side, float(c[i]), sl))
                    for i, name, side, entry, sl in found:
                        if not today[i] or abs(entry - sl) <= 0:
                            continue
                        if (side == "BUY" and sl >= entry) or (side == "SELL" and sl <= entry):
                            continue
                        t1, t2, t6 = _levels(entry, sl, side)
                        cand = ist[i].strftime("%H:%M")
                        key = "%s|%s|%s|%s|%dm" % (today_s, sym, name, cand, tf)
                        from_ts = (ist[i] + pd.Timedelta(minutes=tf)).strftime("%Y-%m-%d %H:%M")      # first 5-minute candle AFTER the signal candle closed
                        if signals_page.add(sym, name, side, entry, sl, t1, t2, t6, "", key, "scan", from_ts, tf):
                            age = (now_ist.replace(tzinfo=None) - (ist[i] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)).total_seconds() / 60.0
                            if age <= ALERT_MINUTES:
                                fresh.append(dict(signal=name, side=side, symbol=sym, price=entry, sl=sl, t1=t1, t2=t2, t6=t6, candle=cand, tf=tf))
                try:                                                        # follow every OPEN signal of this stock: which target / the stop-loss was touched
                    ist5 = [(t + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M") for t in d5.index]
                    bars = [(ist5[j], float(d5["open"].iloc[j]), float(d5["high"].iloc[j]), float(d5["low"].iloc[j]), float(d5["close"].iloc[j]))
                            for j in range(len(d5)) if ist5[j].startswith(today_s)]
                    if bars and signals_page.has_open(sym):
                        signals_page.update_outcomes(sym, bars, day_over)
                except Exception as e:
                    log.warning("signals_scan: follow-up of %s failed: %s", sym, e)
            except Exception as e:
                log.warning("signals_scan: %s failed: %s", sym, e)
        if fresh:
            signals_page.notify(fresh)
            log.info("signals_scan: new signals %s", [(x["symbol"], x["signal"], x["tf"]) for x in fresh])
    except Exception as e:
        log.warning("signals_scan step failed: %s", e)
