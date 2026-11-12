"""
DAILY CANDLE AT A DAILY ZONE - sweep / order-block hit / rejections      (page: /daily_zone   csv: /daily_zone.csv)      ADDED 12 Nov

Once a day (after the daily scan at 16:45 IST) every liquid stock's FINISHED daily candle is tested against its own daily
support / resistance zones (same ALL IN ONE PRO order-block zones as /spurt_daily, from spurt_zones.live_zones, built from the EARLIER days).
A stock is listed when its daily candle is AT a zone (candle range inside the zone or within ZONE_NEAR_PCT of it) AND at least one pattern is true:

  WICK     resistance: upper wick >= 70 % of the candle range and a RED candle, closed at / below the zone top.
           support   : lower wick >= 70 % and a GREEN candle, closed at / above the zone bottom.
  CLEAN    the same, but the opposite wick is at most 5 % and the day's volume is more than the previous day's (the 'clean rejection').
  ENGULF   resistance: bearish engulfing (red candle whose body covers the previous green body); support: bullish engulfing. Volume above the previous day.
  SWEEP    resistance: the day's high went above the highest high of the previous 20 daily candles and the close came back below that level.
           support   : the day's low went below the lowest low of the previous 20 candles and the close came back above it.
           (a simple daily version of the 'liquidity sweep'; the 10-minute Section A engine is not used on daily candles)
  OBH      order-block hit: the daily candle went into the zone (range overlaps it) and closed on the zone's own side
           (resistance: close at / below the zone top; support: close at / above the zone bottom).

It reads no extra data from Dhan: it uses the daily candles the 16:45 scan already downloaded (spurt_daily._LAST).
Read-only: no orders, no signals changed; any error is caught and logged. No env variables needed.
"""
import os
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import spurt10_live as sp
import spurt_daily as sd
import spurt_zones

log = logging.getLogger("daily_zone_rej")
IST = sp.IST

ON = True
NTFY_ALERT = True
WICK_PCT = 70.0
OPP_MAX_PCT = 5.0
SWEEP_LOOKBACK = 20
NEAR_PCT = sp.ZONE_NEAR_PCT          # 'at a zone' = inside it or within this % of it
MAX_LINES = 12
PATH = os.path.join(sd.RESULTS_DIR, "daily_zone.csv")
COLS = ["date", "symbol", "price", "day_chg_pct", "where", "zone", "patterns", "wick_pct", "vol_x", "vol_prev_x"]


def _eval(df, zones, close_prev_avg):
    """df: cleaned daily candles, last row = today. Returns a list of hit dicts (one per zone the candle is at)."""
    o, h, l, c, v = (float(df[k].iloc[-1]) for k in ("open", "high", "low", "close", "volume"))
    po, ph, pl, pc, pv = (float(df[k].iloc[-2]) for k in ("open", "high", "low", "close", "volume"))
    rng = h - l
    if rng <= 0:
        return []
    up = (h - max(o, c)) / rng * 100.0
    dn = (min(o, c) - l) / rng * 100.0
    prior = df.iloc[-1 - SWEEP_LOOKBACK:-1]
    out = []
    near = NEAR_PCT / 100.0
    for z in zones:
        lo, hi, kind = z["lo"], z["hi"], z["type"]
        pats = []
        if kind == "RESISTANCE":
            if not (h >= lo * (1 - near) and l <= hi):
                continue
            wick = up
            if up >= WICK_PCT and c < o and c <= hi:
                pats.append("WICK")
                if dn <= OPP_MAX_PCT and v > pv:
                    pats.append("CLEAN")
            if c < o and po < pc and o >= pc and c <= po and v > pv and h >= lo * (1 - near):
                pats.append("ENGULF")
            if len(prior) >= SWEEP_LOOKBACK:
                lvl = float(prior["high"].max())
                if h > lvl and c < lvl:
                    pats.append("SWEEP")
            if h >= lo and c <= hi:
                pats.append("OBH")
        else:
            if not (l <= hi * (1 + near) and h >= lo):
                continue
            wick = dn
            if dn >= WICK_PCT and c > o and c >= lo:
                pats.append("WICK")
                if up <= OPP_MAX_PCT and v > pv:
                    pats.append("CLEAN")
            if c > o and po > pc and o <= pc and c >= po and v > pv and l <= hi * (1 + near):
                pats.append("ENGULF")
            if len(prior) >= SWEEP_LOOKBACK:
                lvl = float(prior["low"].min())
                if l < lvl and c > lvl:
                    pats.append("SWEEP")
            if l <= hi and c >= lo:
                pats.append("OBH")
        if pats:
            out.append({"where": kind, "zone": f"{lo:g}-{hi:g}", "patterns": "+".join(pats), "wick_pct": round(wick, 1),
                        "vol_x": round(v / close_prev_avg, 2) if close_prev_avg else "", "vol_prev_x": round(v / pv, 2) if pv else ""})
    return out


OC = ["open", "high", "low", "close", "volume"]


def _clean(raw, today):
    """Cleaned daily candles (oldest first, last row = today) or None."""
    if raw is None or len(raw) < sd.MIN_HISTORY + 2:
        return None
    df = raw[OC + ["d"]].reset_index(drop=True)
    if df["d"].iloc[-1] != today:
        return None
    df[OC] = df[OC].astype(float)
    cl = df["close"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        chg = np.abs(cl[1:] / cl[:-1] - 1.0)
    jumps = np.where(np.isfinite(chg) & (chg > sd.SPLIT_JUMP))[0]
    if len(jumps):                                  # split / bonus: older candles are not comparable
        df = df.iloc[jumps[-1] + 1:].reset_index(drop=True)
    return df if len(df) >= sd.MIN_HISTORY else None


def _one(sym, raw, today, df=None, weak=None):
    try:
        oc = OC
        if df is None:
            df = _clean(raw, today)
        if df is None:
            return []
        price = float(df["close"].iloc[-1])
        earlier = df.iloc[:-1]
        avg = float(earlier["volume"].tail(sd.AVG_LEN).mean())
        if avg < sd.MIN_AVG_VOL or price < sp.MIN_PRICE:
            return []
        zdf = earlier[oc].copy()
        try:
            zdf.index = pd.DatetimeIndex(pd.to_datetime(list(earlier["d"])))
        except Exception:
            zdf = zdf.reset_index(drop=True)
        zs = [z for z in (spurt_zones.live_zones(zdf) or []) if (z["hi"] - z["lo"]) <= sd.MAX_ZONE_PCT * price]
        if not zs:
            return []
        if weak is not None:
            weak["zones"] = zs
        hits = _eval(df, zs, avg)
        day_chg = (price / float(df["close"].iloc[-2]) - 1.0) * 100.0
        for x in hits:
            x.update(symbol=sym, price=round(price, 2), day_chg_pct=round(day_chg, 2))
        return hits
    except Exception as e:
        log.warning(f"daily_zone {sym} failed: {e}")
        return []


# ------------------------------------------------------------------------------------------------ WEAK STOCK SCORE (ADDED 12 Nov)
WEAK_PATH = os.path.join(sd.RESULTS_DIR, "daily_weak.csv")
WEAK_COLS = ["date", "symbol", "price", "score", "flags", "rsi", "ret20_pct", "rel20_pct", "at_resistance", "day_chg_pct"]
WEAK_MIN = 4                 # listed from this score (0-7)
WEAK_REL = -5.0              # 20-day return at least this many points BELOW the market (median of all stocks)


def _rsi14(cl):
    if len(cl) < 16:
        return None
    d = np.diff(cl)
    up, dn = np.clip(d, 0, None), np.clip(-d, 0, None)
    au, ad = up[:14].mean(), dn[:14].mean()
    for i in range(14, len(d)):
        au = (au * 13 + up[i]) / 14
        ad = (ad * 13 + dn[i]) / 14
    return 100.0 if ad == 0 else 100.0 - 100.0 / (1.0 + au / ad)


def _swings(x, kind, k=3):
    """Indexes of swing highs / lows (k candles each side), last candle excluded from being a swing."""
    out = []
    for i in range(k, len(x) - k):
        w = x[i - k:i + k + 1]
        if (kind == "H" and x[i] == w.max()) or (kind == "L" and x[i] == w.min()):
            out.append(i)
    return out


def _weak_score(df, mkt20, zones):
    """df cleaned daily candles (last = today). Returns a row dict or None. 7 checks, 1 point each."""
    n = len(df)
    if n < 60:
        return None
    o, h, l, c, v = (df[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
    flags = []
    sh, sl = _swings(h, "H"), _swings(l, "L")
    if len(sh) >= 2 and len(sl) >= 2 and h[sh[-1]] < h[sh[-2]] and l[sl[-1]] < l[sl[-2]]:
        flags.append("STRUCT")                                       # 1 lower high + lower low
    ema = lambda a, L: pd.Series(a).ewm(span=L, adjust=False).mean().to_numpy()
    e20, e50 = ema(c, 20), ema(c, 50)
    if c[-1] < e20[-1] < e50[-1]:
        flags.append("TREND")                                        # 2 close below the 20 EMA, which is below the 50 EMA
    rsi = _rsi14(c)
    if rsi is not None and rsi < 50:
        flags.append("RSI")                                          # 3 daily RSI under 50
    res = [z for z in zones if z["type"] == "RESISTANCE"]
    rej = False
    for i in range(n - 5, n):                                        # 4 last 5 days: red candle, upper wick >= 60%, high reached a resistance zone
        rg = h[i] - l[i]
        if rg > 0 and c[i] < o[i] and (h[i] - max(o[i], c[i])) / rg >= 0.6 and any(h[i] >= z["lo"] * (1 - NEAR_PCT / 100.0) for z in res):
            rej = True
    if rej:
        flags.append("REJ")
    avg = float(np.mean(v[-60:-10])) if n >= 60 else 0.0
    heavy = 0
    for i in range(n - 10, n):                                       # 5 at least 2 heavy red days (volume 1.5x the average) closing in the lower 30% of the range
        rg = h[i] - l[i]
        if avg and rg > 0 and c[i] < o[i] and v[i] >= 1.5 * avg and (c[i] - l[i]) / rg <= 0.3:
            heavy += 1
    if heavy >= 2:
        flags.append("SELLVOL")
    r20 = (c[-1] / c[-21] - 1.0) * 100.0
    rel = r20 - mkt20
    if rel <= WEAK_REL:
        flags.append("REL")                                          # 6 20-day return well below the market
    fb = False
    for k in range(n - 10, n - 1):                                   # 7 failed breakout: closed above the earlier 20-day high within 10 days, now back below that level
        lvl = float(h[k - 20:k].max())
        if c[k] > lvl and c[-1] < lvl:
            fb = True
    if fb:
        flags.append("FAILBRK")
    return {"score": len(flags), "flags": "+".join(flags), "rsi": round(rsi, 1) if rsi is not None else "", "ret20_pct": round(r20, 1),
            "rel20_pct": round(rel, 1), "price": round(float(c[-1]), 2), "day_chg_pct": round((c[-1] / c[-2] - 1.0) * 100.0, 2),
            "at_resistance": any(h[-1] >= z["lo"] * (1 - NEAR_PCT / 100.0) and l[-1] <= z["hi"] for z in res)}


def run(today, names=None):
    """Called by spurt_daily.run_once after the daily scan. Uses the candles cached in sd._LAST. Never raises."""
    if not ON:
        return
    try:
        cache = dict(getattr(sd, "_LAST", {}) or {})
        rows, weak_rows = [], []
        dfs = {}
        for sym, raw in cache.items():
            if names is not None and sym not in names:
                continue
            d_ = _clean(raw, today)
            if d_ is not None and len(d_) >= 25:
                dfs[sym] = d_
        r20 = [float(d_["close"].iloc[-1] / d_["close"].iloc[-21] - 1.0) * 100.0 for d_ in dfs.values()]
        mkt20 = float(np.median(r20)) if r20 else 0.0           # the market = the median 20-day return of all liquid stocks (no extra data call)
        for sym, raw in cache.items():
            if sym not in dfs:
                continue
            wk = {}
            for x in _one(sym, raw, today, df=dfs[sym], weak=wk):
                x["date"] = today.strftime("%Y-%m-%d")
                rows.append(x)
            try:
                if wk.get("zones") is not None:
                    w = _weak_score(dfs[sym], mkt20, wk["zones"])
                    if w and w["score"] >= WEAK_MIN:
                        w.update(date=today.strftime("%Y-%m-%d"), symbol=sym)
                        weak_rows.append(w)
            except Exception as e:
                log.warning(f"weak score {sym} failed: {e}")
        order = {"SWEEP": 0, "CLEAN": 1, "ENGULF": 2, "WICK": 3, "OBH": 4}
        rows.sort(key=lambda r: (min(order.get(p, 9) for p in r["patterns"].split("+")), -len(r["patterns"].split("+")), r["symbol"]))
        today_s = today.strftime("%Y-%m-%d")
        df = pd.DataFrame(rows, columns=COLS)
        os.makedirs(sd.RESULTS_DIR, exist_ok=True)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"daily_zone csv unreadable, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        weak_rows.sort(key=lambda w: (-w["score"], not w["at_resistance"], w["symbol"]))
        wdf = pd.DataFrame(weak_rows, columns=WEAK_COLS)
        if os.path.exists(WEAK_PATH):
            try:
                old = pd.read_csv(WEAK_PATH)
                wdf = pd.concat([old[old["date"].astype(str) != today_s], wdf], ignore_index=True)
            except Exception as e:
                log.warning(f"daily_weak csv unreadable, writing today only: {e}")
        wdf.to_csv(WEAK_PATH + ".tmp", index=False)
        os.replace(WEAK_PATH + ".tmp", WEAK_PATH)
        _notify(rows)
        _notify_weak(weak_rows)
        log.info(f"Daily weak: {len(weak_rows)} stocks with a weakness score of {WEAK_MIN} or more (market 20-day median {mkt20:+.1f}%)")
        log.info(f"Daily zone: {len(rows)} stocks with a pattern at a daily zone ({len(cache)} candles checked)")
        try:
            sd._LAST.clear()
        except Exception:
            pass
    except Exception as e:
        log.warning(f"daily_zone run failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['where'][:3]} {r['symbol']}  Rs {r['price']:.2f}  {r['patterns']}  zone {r['zone']}" for r in rows[:MAX_LINES]]
        extra = f"\n+{len(rows) - MAX_LINES} more on /daily_zone" if len(rows) > MAX_LINES else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"Daily candle at daily zone ({len(rows)})", "Priority": "high", "Tags": "mag"})
    except Exception as e:
        log.warning(f"daily_zone ntfy failed: {e}")


def _notify_weak(weak_rows):
    """Evening phone alert: the weakest stocks = tomorrow's SELL watch-list (stocks at a daily resistance zone first)."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and weak_rows):
        return
    try:
        import requests
        lines = [f"{w['symbol']}  {w['score']}/7  Rs {w['price']:.2f}  RSI {w['rsi']}  {w['flags']}" + ("  AT RESISTANCE" if w["at_resistance"] else "") for w in weak_rows[:MAX_LINES]]
        extra = f"\n+{len(weak_rows) - MAX_LINES} more on /daily_zone" if len(weak_rows) > MAX_LINES else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"Weak stocks - tomorrow's SELL watch-list ({len(weak_rows)})", "Priority": "high", "Tags": "chart_with_downwards_trend"})
    except Exception as e:
        log.warning(f"daily_weak ntfy failed: {e}")


AT_RES = '<b style="color:#c62828">AT RESISTANCE</b>'


def _weak_html(with_gap=True):
    try:
        if not os.path.exists(WEAK_PATH):
            return ""
        df = pd.read_csv(WEAK_PATH).fillna("")
        if not len(df):
            return ""
        day = str(df["date"].iloc[-1])
        rows = df[df["date"].astype(str) == day].to_dict("records")
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td><td><b>{r['score']}/7</b></td><td>{str(r['flags']).replace('+', ' + ')}</td><td>{float(r['price']):.2f}</td>"
            f"<td>{r['rsi']}</td><td>{r['ret20_pct']}%</td><td>{r['rel20_pct']}%</td><td>{AT_RES if str(r['at_resistance']) == 'True' else '-'}</td></tr>"
            for r in rows) or '<tr><td colspan="8" style="text-align:center">No stock reached the score.</td></tr>'
        return (f'<h2 style="margin-top:{28 if with_gap else 6}px">Weak stocks on the daily chart &mdash; {day} ({len(rows)})</h2>'
                f'<div class="m">One point for each: <b>STRUCT</b> lower high + lower low &middot; <b>TREND</b> close below the 20 EMA, which is below the 50 EMA &middot; <b>RSI</b> daily RSI under 50 &middot; '
                f'<b>REJ</b> red candle with a long upper wick at a daily resistance zone in the last 5 days &middot; <b>SELLVOL</b> two or more heavy red days (1.5x volume) closing near the low in the last 10 days &middot; '
                f'<b>REL</b> 20-day return at least {abs(WEAK_REL):g} points below the market (median of all liquid stocks) &middot; <b>FAILBRK</b> broke out above the 20-day high within 10 days and is back below it. '
                f'Listed from {WEAK_MIN}/7; stocks at a resistance zone first. Information only. <a href="/daily_weak.csv" target="_blank" rel="noopener">CSV (all days)</a></div>'
                '<table><tr><th>Symbol</th><th>Score</th><th>Weak signs</th><th>Close</th><th>RSI</th><th>20-day</th><th>vs market</th><th>Now</th></tr>' + body + '</table>')
    except Exception as e:
        log.warning(f"daily_weak page failed: {e}")
        return ""


# ---------------------------------------------------------------------------------------- run it by hand (page link "Run now")
_job = {"running": False, "msg": "", "at": ""}


def _job_body():
    try:
        now = datetime.now(IST)
        today = now.date()
        uni = sp._load_universe()
        sid_of = dict(uni)
        try:
            names = sd._liquid(sd._fetch_quotes(uni))
            if len(names) < 50:
                names = [s_ for s_, _i in uni]
        except Exception:
            names = [s_ for s_, _i in uni]
        done = 0
        for sym in names:
            try:
                sd._daily_candles(sym, sid_of[sym], today)
            except Exception:
                pass
            done += 1
            _job["msg"] = f"reading daily candles {done}/{len(names)}"
        run(today, set(names))
        _job["msg"] = f"finished {datetime.now(IST).strftime('%H:%M')} IST"
    except Exception as e:
        _job["msg"] = f"failed: {e}"
        log.warning(f"daily_zone run-now failed: {e}")
    finally:
        _job["running"] = False


def start_now():
    """Starts the daily-candle scan in the background (the same job that normally runs after 16:45). Returns a short status text."""
    import threading
    if _job["running"]:
        return "Already running: " + _job["msg"]
    _job.update(running=True, msg="starting", at=datetime.now(IST).strftime("%H:%M"))
    threading.Thread(target=_job_body, daemon=True, name="daily-zone-now").start()
    return "Started. It reads every liquid stock's daily candles (about 5-10 minutes), then fills /daily_zone and /daily_weak."


def _runbar():
    st = (" &mdash; " + _job["msg"]) if (_job["running"] or _job["msg"]) else ""
    return f'<div class="m"><a href="/daily_zone/run" target="_blank" rel="noopener">Run the scan now</a> (about 5-10 minutes; refresh this page afterwards){st}</div>'


def _nav(cur):
    items = [("", "All patterns", "/daily_zone"), ("SWEEP", "Sweep", "/daily_zone?p=SWEEP"), ("OBH", "Order-block hit", "/daily_zone?p=OBH"),
             ("WICK", "Wick rejection", "/daily_zone?p=WICK"), ("CLEAN", "Clean rejection", "/daily_zone?p=CLEAN"), ("ENGULF", "Engulfing", "/daily_zone?p=ENGULF"),
             ("WEAK", "Weak stocks (sell list)", "/daily_weak")]
    return '<div style="margin:4px 0 10px">' + " ".join(
        f'<a href="{u}" style="display:inline-block;padding:5px 10px;margin:2px;border:1px solid #999;border-radius:4px;text-decoration:none;'
        f'{"background:#222;color:#fff" if k == cur else "color:#222;background:#fff"}">{t}</a>' for k, t, u in items) + "</div>"


def _head(title):
    return ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>{title}</title><style>body{{font-family:Arial,sans-serif;margin:12px;background:#fafafa}}'
            'table{border-collapse:collapse;width:100%;background:#fff}th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}'
            'th{background:#222;color:#fff;position:sticky;top:0}td:nth-child(1),td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')


def weak_page_html():
    """Separate page /daily_weak: the weak-stock (sell watch-list) table only."""
    try:
        return _head("Weak stocks - daily") + _nav("WEAK") + _runbar() + (_weak_html(False) or "<p>Nothing yet - the list is made once a day after 16:45 IST.</p>") + "</body></html>"
    except Exception as e:
        log.warning(f"daily_weak page failed: {e}")
        return "<p>The page could not be built - see the logs.</p>"


def page_html(pattern=""):
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Daily candle at daily zone</title><style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}'
            'table{border-collapse:collapse;width:100%;background:#fff}th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}'
            'th{background:#222;color:#fff;position:sticky;top:0}td:nth-child(1),td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        rows, day = [], ""
        if os.path.exists(PATH):
            df = pd.read_csv(PATH).fillna("")
            if len(df):
                day = str(df["date"].iloc[-1])
                rows = df[df["date"].astype(str) == day].to_dict("records")
        pattern = (pattern or "").upper()
        if pattern:
            rows = [r for r in rows if pattern in str(r["patterns"]).split("+")]

        def pc(v):
            v = float(v)
            return f'<span style="color:{"#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"}">{v:+.2f}%</span>'
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td><td style=\"color:{'#c62828' if r['where'] == 'RESISTANCE' else '#0a7d33'}\"><b>{r['where']}</b></td>"
            f"<td>{float(r['price']):.2f}</td><td>{pc(r['day_chg_pct'])}</td><td>{r['zone']}</td><td><b>{r['patterns'].replace('+', ' + ')}</b></td>"
            f"<td>{float(r['wick_pct']):.0f}%</td><td>{r['vol_x']}x</td><td>{r['vol_prev_x']}x</td></tr>" for r in rows) \
            or '<tr><td colspan="9" style="text-align:center">Nothing yet - the list is made once a day after 16:45 IST.</td></tr>'
        return (head + _nav(pattern) + _runbar() + f'<h2>Daily candle at a daily support / resistance zone{" - " + pattern if pattern else ""} &mdash; {day or "-"} ({len(rows)})</h2>'
                '<div class="m">Every liquid stock whose finished DAILY candle is at a daily zone and shows: <b>SWEEP</b> (took out the 20-day high/low and closed back), '
                '<b>OBH</b> (order-block hit: went into the zone and closed on its own side), <b>WICK</b> (wick 70% or more, right colour), <b>CLEAN</b> (wick 70% or more, almost no wick on the other side, volume above the previous day), '
                '<b>ENGULF</b> (engulfing candle with volume above the previous day). Resistance needs a red candle, support a green one. Made once a day after 16:45 IST. Information only. '
                '<a href="/daily_zone.csv" target="_blank" rel="noopener">CSV (all days)</a> &middot; <a href="/spurt_daily" target="_blank" rel="noopener">Daily spurt page</a></div>'
                '<table><tr><th>Symbol</th><th>Zone type</th><th>Close</th><th>Day %</th><th>Daily zone</th><th>Patterns</th><th>Wick</th><th>Volume vs 50-day</th><th>Volume vs prev day</th></tr>'
                + body + '</table></body></html>')
    except Exception as e:
        log.warning(f"daily_zone page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
