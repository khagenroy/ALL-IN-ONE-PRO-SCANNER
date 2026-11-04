"""
INVESTING SCAN  (page: /invest   csv: /invest.csv, /invest_history.csv)

Read-only. Places no orders and touches no signal logic. Runs once a day after the close (17:15 IST, Mon-Fri) on the daily candles the swing scan already
keeps in its cache (about 10 years per stock; stocks without a cache are downloaded, throttled by the scanner's shared request limiter).

Five checks per stock - each one point (score 0-5):

  1. RELATIVE STRENGTH   the stock's 3, 6 and 12-month returns are ranked against every other stock scanned (weights 40% / 20% / 40%, IBD style).
                         A point when the rank is at or above RS_MIN_RANK (80 = better than 80% of the stocks).
  2. WEEKLY / MONTHLY PULLBACK  an uptrend (weekly close above a rising 30-week average) whose price has come back to, or is within PULLBACK_PCT % above,
                         a SUPPORT zone made on the WEEKLY or MONTHLY candles (same order-block zones as the spurt pages, spurt_zones.py).
  3. WEEKLY VOLUME SPURT the last completed week's volume is at least WEEK_VOL_X times the average of the previous 50 weeks, the week closed green
                         and in the upper half of its range (accumulation).
  4. BASE BREAKOUT       the latest close is above the highest high of the previous BASE_DAYS trading days, the base was tight (range at most BASE_MAX_RANGE_PCT %),
                         volume at least BREAK_VOL_X times its 50-day average, and the close is within NEAR_HIGH_PCT % of the 52-week high.
  5. DEAL ACCUMULATION   net BUY value of NSE bulk + block deals in the last DEALS_DAYS days is at least DEALS_MIN_CR crore rupees.
                         (read from NSE's public historical bulk / block deal lists; if NSE does not answer, this check is skipped and the page says so)

A price scan finds candidates, not good businesses. Check the company before you buy. Information only.
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import live_scanner as ls
import spurt10_live as sp
import spurt_zones

log = logging.getLogger("invest")
IST = sp.IST

INVEST_ON = True
RUN_AT_MIN = 17 * 60 + 15
MIN_PRICE = 50.0
MIN_TURNOVER = 2e7              # average traded value of the last 20 days: Rs 2 crore
SPLIT_JUMP = 0.30               # a day-to-day close change above this = split / bonus (history is not adjusted): older candles are dropped
RS_MIN_RANK = 80.0
PULLBACK_PCT = 3.0
MAX_ZONE_PCT = 0.25
WEEK_VOL_X = 2.0
BASE_DAYS = 40
BASE_MAX_RANGE_PCT = 15.0
BREAK_VOL_X = 1.5
NEAR_HIGH_PCT = 5.0
DEALS_DAYS = 30
DEALS_MIN_CR = 5.0
MIN_ROWS = 260                  # daily candles needed (about a year)
LIST_MIN_SCORE = 1
ALERT_MIN_SCORE = 3
MAX_SCORE = 6
QUAL_ON = True                  # 6th check: company quality from Yahoo Finance, asked only for stocks that already have QUAL_FROM_SCORE+ price checks
QUAL_FROM_SCORE = 3
QUAL_MAX_STOCKS = 40
QUAL_MIN_ROE = 0.12             # return on equity 12 %
QUAL_MAX_PLEDGE = 10.0          # promoter shares pledged: above this % the stock fails Quality
QUAL_MAX_PROMOTER_FALL = 1.0    # promoter holding fell by more than this many points since the last quarter = fails Quality
DEBUG_PATH = os.path.join(sp.RESULTS_DIR, "invest_debug.json")      # first answers of the holding lists, to fix the parsing if NSE changes the format
QUAL_MAX_DE = 100.0             # Yahoo gives debt-to-equity in percent: 100 = debt equal to equity
ALERT_MAX_LINES = 10

RESULTS_DIR = sp.RESULTS_DIR
JSON_PATH = os.path.join(RESULTS_DIR, "invest.json")
CSV_PATH = os.path.join(RESULTS_DIR, "invest.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "invest_history.csv")
SCORES_PATH = os.path.join(RESULTS_DIR, "invest_scores.json")        # {symbol: score}, read by the spurt pages for the INV tag

COLS = ["date", "symbol", "price", "score", "rs_rank", "ret_3m", "ret_6m", "ret_12m", "uptrend", "pullback", "week_vol_x", "breakout", "break_vol_x",
        "high52_dist_pct", "deals_net_cr", "roe_pct", "debt_equity", "profit_growth_pct", "sales_growth_pct", "promoter_pct", "promoter_chg", "pledged_pct", "checks"]
_state = {"last_run_date": None, "running": False}


# ---------------------------------------------------------------- helpers
def _clean(df):
    """Daily candles (index = dates, columns open..volume): float, after the last split-like jump."""
    df = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
    cl = df["close"].to_numpy()
    if len(cl) > 1:
        with np.errstate(divide="ignore", invalid="ignore"):
            chg = np.abs(cl[1:] / cl[:-1] - 1.0)
        jumps = np.where(np.isfinite(chg) & (chg > SPLIT_JUMP))[0]
        if len(jumps):
            df = df.iloc[jumps[-1] + 1:]
    return df


def _resample(df, rule):
    g = df.resample(rule)
    out = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                        "close": g["close"].last(), "volume": g["volume"].sum()}).dropna()
    return out


def _ret(cl, n):
    if len(cl) <= n or cl[-n - 1] <= 0:
        return None
    return (cl[-1] / cl[-n - 1] - 1.0) * 100.0


def _support_near(zdf, price):
    """Nearest SUPPORT zone of the candles zdf with price inside it or within PULLBACK_PCT % above it -> 'lo-hi' or ''."""
    try:
        zs = [z for z in (spurt_zones.live_zones(zdf) or []) if z["type"] == "SUPPORT" and (z["hi"] - z["lo"]) <= MAX_ZONE_PCT * price]
        best = None
        for z in zs:
            if z["lo"] * 0.99 <= price <= z["hi"] * (1 + PULLBACK_PCT / 100.0):
                if best is None or z["hi"] > best["hi"]:
                    best = z
        return f"{best['lo']:g}-{best['hi']:g}" if best else ""
    except Exception:
        return ""


def _analyse(sym, df):
    """One stock -> dict of raw numbers, or None (not enough history / not liquid). Never raises."""
    try:
        d = _clean(df)
        if len(d) < MIN_ROWS:
            return None
        cl = d["close"].to_numpy()
        price = float(cl[-1])
        if price < MIN_PRICE or float((d["close"] * d["volume"]).tail(20).mean()) < MIN_TURNOVER:
            return None
        row = {"symbol": sym, "price": round(price, 2), "r3": _ret(cl, 63), "r6": _ret(cl, 126), "r12": _ret(cl, 252)}
        # 2: weekly uptrend + pullback to a weekly / monthly support zone
        w = _resample(d, "W-FRI")
        lastday = d.index[-1]
        if len(w) and w.index[-1].date() > lastday.date():          # the week is not finished: leave it out
            w = w.iloc[:-1]
        row["uptrend"] = False
        row["pullback"] = ""
        if len(w) >= 60:
            sma = w["close"].rolling(30).mean()
            up = bool(w["close"].iloc[-1] > sma.iloc[-1] and sma.iloc[-1] > sma.iloc[-5])
            row["uptrend"] = up
            if up:
                z = _support_near(w, price)
                if z:
                    row["pullback"] = "weekly " + z
                else:
                    m = _resample(d, "ME") if hasattr(pd.offsets, "MonthEnd") else _resample(d, "M")
                    if len(m) >= 40:
                        z = _support_near(m, price)
                        if z:
                            row["pullback"] = "monthly " + z
        # 3: weekly volume spurt
        row["week_vol_x"] = None
        if len(w) >= 55:
            base = float(w["volume"].iloc[-51:-1].mean())
            if base > 0:
                x = float(w["volume"].iloc[-1]) / base
                last = w.iloc[-1]
                rng = last["high"] - last["low"]
                green_top = last["close"] > last["open"] and rng > 0 and (last["close"] - last["low"]) / rng >= 0.5
                row["week_vol_x"] = round(x, 2) if green_top else None
                row["_wk_ok"] = bool(green_top and x >= WEEK_VOL_X)
        # 4: base breakout
        hi = d["high"].to_numpy()
        lo = d["low"].to_numpy()
        vol = d["volume"].to_numpy()
        prev_hi = hi[-BASE_DAYS - 1:-1].max()
        base_rng = (prev_hi - lo[-BASE_DAYS - 1:-1].min()) / price * 100.0
        avg50 = float(vol[-51:-1].mean()) if len(vol) > 51 else 0.0
        bx = float(vol[-1]) / avg50 if avg50 > 0 else 0.0
        h52 = float(hi[-252:].max())
        dist = (h52 / price - 1.0) * 100.0
        row["break_vol_x"] = round(bx, 2)
        row["high52_dist_pct"] = round(dist, 2)
        row["breakout"] = bool(price > prev_hi and base_rng <= BASE_MAX_RANGE_PCT and bx >= BREAK_VOL_X and dist <= NEAR_HIGH_PCT)
        return row
    except Exception as e:
        log.warning(f"Invest analysis failed for {sym}: {e}")
        return None


def _deals_net(now):
    """({symbol: net BUY value in rupees over DEALS_DAYS days}, status text). Fails open: ({}, reason)."""
    try:
        import requests
        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                             "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"})
        sess.get("https://www.nseindia.com/", timeout=12)
        fd, td = (now - timedelta(days=DEALS_DAYS)).strftime("%d-%m-%Y"), now.strftime("%d-%m-%Y")
        net, n = {}, 0
        for kind in ("bulk-deals", "block-deals"):
            resp = sess.get(f"https://www.nseindia.com/api/historical/{kind}?from={fd}&to={td}", timeout=25)
            if resp.status_code != 200:
                raise RuntimeError(f"NSE answered {resp.status_code} for {kind}")
            data = resp.json()
            data = data.get("data") if isinstance(data, dict) else data
            for a in data or []:
                sym = str(a.get("BD_SYMBOL") or a.get("symbol") or "").strip().upper()
                side = str(a.get("BD_BUY_SELL") or a.get("buySell") or "").strip().upper()
                try:
                    val = float(a.get("BD_QTY_TRD") or a.get("qty") or 0) * float(a.get("BD_TP_WATP") or a.get("watp") or 0)
                except Exception:
                    continue
                if not sym or not val or side not in ("BUY", "SELL"):
                    continue
                net[sym] = net.get(sym, 0.0) + (val if side == "BUY" else -val)
                n += 1
        if n == 0:
            return {}, "NSE deal lists were empty - the deal check was skipped."
        return net, f"{n} bulk / block deal lines read from NSE (last {DEALS_DAYS} days)."
    except Exception as e:
        return {}, f"NSE deal lists could not be read ({e}) - the deal check was skipped."


def _to_date(t):
    try:
        t = float(t)
        return pd.Timestamp(t, unit="s", tz="UTC").tz_convert("Asia/Kolkata").tz_localize(None).normalize()
    except Exception:
        return pd.Timestamp(t).tz_localize(None).normalize() if pd.Timestamp(t).tzinfo else pd.Timestamp(t).normalize()


def _norm(df):
    """Whatever the history loader returns -> candles indexed by date (oldest first), or None."""
    if df is None or not len(df):
        return None
    df = df.copy()
    for c in ("d", "date", "datetime", "timestamp", "time"):
        if c in df.columns:
            df.index = [_to_date(x) for x in df[c]]
            break
    else:
        df.index = pd.to_datetime(df.index)
        if getattr(df.index, "tz", None) is not None:
            df.index = df.index.tz_localize(None)
        df.index = df.index.normalize()
    df.columns = [str(c).lower() for c in df.columns]
    if not {"open", "high", "low", "close", "volume"} <= set(df.columns):
        return None
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def _history(sym, sid, today):
    """10-year daily candles: the scanner's disk cache first, a direct Dhan request if that fails. Never raises."""
    try:
        d = _norm(ls.fetch_daily_history_cached(sym, str(sid), "NSE_EQ"))
        if d is not None and len(d):
            return d
    except Exception:
        pass
    try:
        resp = ls._dhan_post("charts/historical", {"securityId": str(sid), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "expiryCode": 0, "oi": False,
                                                   "fromDate": (today - timedelta(days=3650)).strftime("%Y-%m-%d"), "toDate": (today + timedelta(days=1)).strftime("%Y-%m-%d")},
                             f"invest history {sym}")
        j = resp.json()
        if j and j.get("close"):
            return _norm(pd.DataFrame({k: j.get(k) for k in ("open", "high", "low", "close", "volume", "timestamp")}).dropna())
    except Exception:
        pass
    return None


def _quality(sym):
    """(passed True/False/None, roe %, debt/equity ratio, profit growth %, sales growth %) from Yahoo Finance. None = data not available. Never raises."""
    try:
        import yfinance as yf
        info = yf.Ticker(sym.replace("&", "&") + ".NS").info or {}
        roe, de = info.get("returnOnEquity"), info.get("debtToEquity")
        pg, sg = info.get("earningsGrowth"), info.get("revenueGrowth")
        vals = [roe, de, pg, sg]
        if sum(v is not None for v in vals) < 3:
            return None, None, None, None, None
        ok = ((pg is None or pg > 0) and (sg is None or sg > 0) and (de is None or de < QUAL_MAX_DE) and (roe is None or roe >= QUAL_MIN_ROE))
        r = lambda v, k=1.0, n=1: None if v is None else round(v * k, n)
        return bool(ok), r(roe, 100), r(de, 0.01, 2), r(pg, 100), r(sg, 100)
    except Exception as e:
        log.info(f"Quality data for {sym} not available: {e}")
        return None, None, None, None, None


def _num(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


def _nse_session():
    import requests
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                         "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"})
    sess.get("https://www.nseindia.com/", timeout=12)
    return sess


def _holding(sess, sym, dbg):
    """(promoter %, change vs the previous quarter in points, pledged % of promoter shares) from the SEBI shareholding / pledge filings that NSE lists. Any part may be None. Never raises."""
    prom = chg = pled = None
    try:
        r = sess.get("https://www.nseindia.com/api/corporate-share-holdings-master", params={"index": "equities", "symbol": sym}, timeout=20)
        data = r.json() if r.status_code == 200 else []
        data = data.get("data") if isinstance(data, dict) else data
        if data and len(dbg) < 3:
            dbg.append({"holdings": sym, "first": data[:2]})
        vals = [_num(x.get("pr_and_prgrp")) for x in (data or [])[:2] if isinstance(x, dict)]
        if vals and vals[0] is not None:
            prom = round(vals[0], 2)
            if len(vals) > 1 and vals[1] is not None:
                chg = round(vals[0] - vals[1], 2)
    except Exception as e:
        log.info(f"Holding data for {sym} not available: {e}")
    try:
        r = sess.get("https://www.nseindia.com/api/corporate-pledgedata", params={"index": "equities", "symbol": sym}, timeout=20)
        data = r.json() if r.status_code == 200 else []
        data = data.get("data") if isinstance(data, dict) else data
        if data and len(dbg) < 3:
            dbg.append({"pledge": sym, "first": data[:1]})
        if data and isinstance(data[0], dict):
            for k, v in data[0].items():
                kl = str(k).lower()
                if "pledg" in kl and ("perc" in kl or "%" in kl) and _num(v) is not None:
                    pled = round(_num(v), 2)
                    break
    except Exception as e:
        log.info(f"Pledge data for {sym} not available: {e}")
    return prom, chg, pled


# ---------------------------------------------------------------- scan
def run_once():
    t0 = time.monotonic()
    now = datetime.now(IST)
    uni = sp._load_universe()
    raw, failed = [], 0
    for sym, sid in uni:
        df = _history(sym, sid, now)
        if df is None:
            failed += 1
            continue
        r = _analyse(sym, df)
        del df
        if r:
            raw.append(r)
    if not raw:
        raise RuntimeError("no stock could be analysed")
    # 1: relative strength rank (against every stock scanned)
    sc = []
    for r in raw:
        parts = [(r["r3"], 0.4), (r["r6"], 0.2), (r["r12"], 0.4)]
        ok = [(v, w) for v, w in parts if v is not None]
        sc.append(sum(v * w for v, w in ok) / sum(w for _v, w in ok) if ok else np.nan)
    s = pd.Series(sc)
    ranks = s.rank(pct=True) * 100.0
    deals, deals_status = _deals_net(now)
    rows = []
    for r, rk in zip(raw, ranks):
        rs_ok = bool(pd.notna(rk) and rk >= RS_MIN_RANK)
        dn = deals.get(r["symbol"])
        deal_ok = bool(dn is not None and dn / 1e7 >= DEALS_MIN_CR)
        checks = []
        if rs_ok:
            checks.append("RS")
        if r["pullback"]:
            checks.append("PULLBACK")
        if r.get("_wk_ok"):
            checks.append("WEEKVOL")
        if r["breakout"]:
            checks.append("BREAKOUT")
        if deal_ok:
            checks.append("DEALS")
        if len(checks) < LIST_MIN_SCORE:
            continue
        f = lambda v: None if v is None else round(v, 1)
        rows.append({"date": now.strftime("%Y-%m-%d"), "symbol": r["symbol"], "price": r["price"], "score": len(checks), "rs_rank": (round(float(rk), 0) if pd.notna(rk) else None),
                     "ret_3m": f(r["r3"]), "ret_6m": f(r["r6"]), "ret_12m": f(r["r12"]), "uptrend": "YES" if r["uptrend"] else "", "pullback": r["pullback"],
                     "week_vol_x": r.get("week_vol_x") if r.get("_wk_ok") else None, "breakout": "YES" if r["breakout"] else "", "break_vol_x": r["break_vol_x"] if r["breakout"] else None,
                     "high52_dist_pct": r["high52_dist_pct"], "deals_net_cr": (round(dn / 1e7, 1) if dn is not None else None), "checks": " ".join(checks)})
    q_status = "Quality check is off."
    if QUAL_ON:
        try:
            import yfinance  # noqa: F401
            cand = sorted([r for r in rows if r["score"] >= QUAL_FROM_SCORE], key=lambda x: (-x["score"], -(x["rs_rank"] or 0)))[:QUAL_MAX_STOCKS]
            got = hgot = 0
            try:
                nse = _nse_session()
            except Exception as e:
                nse = None
                log.info(f"NSE session for holdings failed: {e}")
            dbg = []
            for r in cand:
                ok, roe, de, pg, sg = _quality(r["symbol"])
                r["roe_pct"], r["debt_equity"], r["profit_growth_pct"], r["sales_growth_pct"] = roe, de, pg, sg
                if nse is not None:
                    r["promoter_pct"], r["promoter_chg"], r["pledged_pct"] = _holding(nse, r["symbol"], dbg)
                    if r["promoter_pct"] is not None:
                        hgot += 1
                    if ok is not False and ((r["pledged_pct"] is not None and r["pledged_pct"] > QUAL_MAX_PLEDGE)
                                            or (r["promoter_chg"] is not None and r["promoter_chg"] < -QUAL_MAX_PROMOTER_FALL)):
                        ok = False
                    elif ok is None and r["promoter_pct"] is not None:
                        ok = None
                if ok is not None:
                    got += 1
                if ok:
                    r["checks"] += " QUALITY"
                    r["score"] += 1
                time.sleep(0.5)
            q_status = (f"Quality data read for {got} of {len(cand)} stocks with {QUAL_FROM_SCORE}+ price checks (Yahoo Finance for growth / ROE / debt; NSE shareholding filings for promoter holding: {hgot} read).")
            if dbg:
                try:
                    with open(DEBUG_PATH, "w") as f:
                        json.dump(dbg, f)
                except Exception:
                    pass
        except ImportError:
            q_status = "Quality check skipped: add 'yfinance' to requirements.txt."
    rows.sort(key=lambda x: (-x["score"], -(x["rs_rank"] or 0)))
    meta = {"date": now.strftime("%Y-%m-%d"), "run": now.strftime("%H:%M:%S IST"), "took_s": round(time.monotonic() - t0), "scanned": len(raw), "failed": failed,
            "listed": len(rows), "deals_status": deals_status}
    meta["deals_status"] += " " + q_status
    _write(rows, meta)
    _alert(rows, meta)
    return True


def _write(rows, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(JSON_PATH + ".tmp", "w") as f:
        json.dump({"meta": meta, "rows": rows}, f)
    os.replace(JSON_PATH + ".tmp", JSON_PATH)
    df = pd.DataFrame(rows, columns=COLS)
    df.to_csv(CSV_PATH + ".tmp", index=False)
    os.replace(CSV_PATH + ".tmp", CSV_PATH)
    try:
        if os.path.exists(HIST_PATH):
            old = pd.read_csv(HIST_PATH)
            df = pd.concat([old[old["date"].astype(str) != meta["date"]], df], ignore_index=True)
        df.to_csv(HIST_PATH + ".tmp", index=False)
        os.replace(HIST_PATH + ".tmp", HIST_PATH)
    except Exception as e:
        log.warning(f"Invest history write failed: {e}")
    with open(SCORES_PATH + ".tmp", "w") as f:
        json.dump({r["symbol"]: r["score"] for r in rows}, f)
    os.replace(SCORES_PATH + ".tmp", SCORES_PATH)


def _alert(rows, meta):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    top = [r for r in rows if r["score"] >= ALERT_MIN_SCORE]
    if not (sp.NTFY_ON and topic and top):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  Rs {r['price']:.2f}  score {r['score']}/{MAX_SCORE}  {r['checks']}" for r in top[:ALERT_MAX_LINES]]
        extra = f"\n+{len(top) - ALERT_MAX_LINES} more on /invest" if len(top) > ALERT_MAX_LINES else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"Investing scan: {len(top)} stock(s) with score {ALERT_MIN_SCORE}+ of {MAX_SCORE}", "Priority": "default", "Tags": "moneybag"})
    except Exception as e:
        log.warning(f"Invest ntfy failed: {e}")


# ---------------------------------------------------------------- page
def page_html():
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Investing scan</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}td:nth-child(2){text-align:left}'
            '.m{color:#555;font-size:13px;margin:6px 0 12px}th{position:sticky;top:0;z-index:5;box-shadow:0 1px 0 #555}</style></head><body>')
    try:
        if not os.path.exists(JSON_PATH):
            return head + "<h2>Investing scan</h2><p>No scan has run yet. It runs once a day after the market closes (17:15 IST, Mon-Fri).</p></body></html>"
        with open(JSON_PATH) as f:
            d = json.load(f)
        meta, rows = d["meta"], d["rows"]

        def pc(v):
            return "-" if v is None else f'<span style="color:{"#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"}">{v:+.1f}%</span>'

        def tick(ok):
            return '<b style="color:#0a7d33">&#10003;</b>' if ok else '<span style="color:#bbb">-</span>'

        body = "".join(
            f"<tr><td>{i}</td><td>{sp._tvl(r['symbol'])}</td><td>{r['price']:.2f}</td><td{' style=background:#ffe066' if r['score'] >= ALERT_MIN_SCORE else ''}><b>{r['score']}/{MAX_SCORE}</b></td>"
            f"<td>{tick('RS' in r['checks'].split())} {'' if r['rs_rank'] is None else int(r['rs_rank'])}</td><td>{pc(r['ret_3m'])}</td><td>{pc(r['ret_6m'])}</td><td>{pc(r['ret_12m'])}</td>"
            f"<td>{tick(bool(r['pullback']))} {r['pullback']}</td><td>{tick('WEEKVOL' in r['checks'].split())} {'' if r['week_vol_x'] is None else str(r['week_vol_x']) + 'x'}</td>"
            f"<td>{tick(bool(r['breakout']))} {'' if r['break_vol_x'] is None else str(r['break_vol_x']) + 'x'}</td><td>{r['high52_dist_pct']:.1f}%</td>"
            f"<td>{tick('DEALS' in r['checks'].split())} {'' if r['deals_net_cr'] is None else str(r['deals_net_cr']) + ' cr'}</td>"
            f"<td>{tick('QUALITY' in r['checks'].split())} {'' if r.get('roe_pct') is None else 'ROE ' + str(r['roe_pct']) + '%'}</td>"
            f"<td>{'' if r.get('debt_equity') is None else r['debt_equity']}</td><td>{pc(r.get('profit_growth_pct'))}</td><td>{pc(r.get('sales_growth_pct'))}</td>"
            f"<td>{'' if r.get('promoter_pct') is None else str(r['promoter_pct']) + '%'} {'' if r.get('promoter_chg') is None else pc(r['promoter_chg']).replace('%', ' pt')}</td>"
            f"<td>{'' if r.get('pledged_pct') is None else str(r['pledged_pct']) + '%'}</td></tr>"
            for i, r in enumerate(rows[:300], 1)) or '<tr><td colspan="19">No stock passed any check.</td></tr>'
        return (head + f'<h2>Investing scan &mdash; {meta["listed"]} stocks with at least one of {MAX_SCORE} checks</h2>'
                f'<div class="m">Scan of {meta["date"]} ({meta["run"]}, took {meta["took_s"]}s) &middot; {meta["scanned"]} liquid NSE stocks analysed on daily candles (price &ge; {MIN_PRICE:.0f}, traded &ge; Rs {MIN_TURNOVER / 1e7:g} crore a day) &middot; runs once a day after 17:15 IST. '
                f'<b>RS</b> = 3/6/12-month return ranked against all stocks (&ge; {RS_MIN_RANK:g}). <b>Pullback</b> = weekly uptrend, price at a weekly / monthly support zone (within {PULLBACK_PCT:g}%). '
                f'<b>Weekly volume</b> = last week &ge; {WEEK_VOL_X:g}x its 50-week average volume, green, closed in the upper half. <b>Breakout</b> = close above the {BASE_DAYS}-day high from a tight base, volume &ge; {BREAK_VOL_X:g}x, within {NEAR_HIGH_PCT:g}% of the 52-week high. '
                f'<b>Quality</b> (only for stocks with {QUAL_FROM_SCORE}+ other checks) = Yahoo Finance: profit and sales growing, debt/equity below {QUAL_MAX_DE / 100:g}, ROE &ge; {QUAL_MIN_ROE * 100:g}%, promoter holding not down more than {QUAL_MAX_PROMOTER_FALL:g} point, pledge below {QUAL_MAX_PLEDGE:g}%. <b>Deals</b> = net buying in NSE bulk + block deals over {DEALS_DAYS} days &ge; Rs {DEALS_MIN_CR:g} crore. {meta["deals_status"]} '
                'A scan of prices only: it finds candidates, not good businesses. Check the company before you buy. '
                '<a href="/invest.csv">CSV</a> &middot; <a href="/invest_history.csv">history CSV</a> &middot; <a href="/spurt_daily">Daily spurt</a> &middot; <a href="/scanner">Scanner</a></div>'
                '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Score</th><th>RS rank</th><th>3 m</th><th>6 m</th><th>12 m</th><th>Pullback to support</th>'
                '<th>Weekly volume</th><th>Breakout</th><th>Below 52-wk high</th><th>Deals (net buy)</th><th>Quality</th><th>Debt / equity</th><th>Profit growth</th><th>Sales growth</th><th>Promoter (chg)</th><th>Pledged</th></tr>' + body + '</table></div>' + sp._SORT_JS + '</body></html>')
    except Exception as e:
        log.warning(f"Invest page failed: {e}")
        return head + "<p>The investing page could not be built. Please refresh later.</p></body></html>"


# ---------------------------------------------------------------- loop
def _have_today(today_s):
    try:
        with open(JSON_PATH) as f:
            return json.load(f)["meta"]["date"] == today_s
    except Exception:
        return False


def loop():
    log.info("Investing scan autorun started - once a day after 17:15 IST (Mon-Fri); catch-up after a restart if today's scan is missing.")
    time.sleep(1200)                                      # stay out of the way of the start-up scans
    fails = 0
    while True:
        try:
            now = datetime.now(IST)
            today_s = now.strftime("%Y-%m-%d")
            due = INVEST_ON and now.weekday() < 5 and now.hour * 60 + now.minute >= RUN_AT_MIN
            if due and _state["last_run_date"] != today_s and not _have_today(today_s):
                try:
                    run_once()
                    _state["last_run_date"] = today_s
                    fails = 0
                except Exception as e:
                    fails += 1
                    log.error(f"Investing scan failed ({fails}): {e}")
                    if fails >= 3:
                        _state["last_run_date"] = today_s
                        fails = 0
                    else:
                        time.sleep(900)
            elif due:
                _state["last_run_date"] = today_s
        except Exception as e:
            log.error(f"Investing loop error: {e}")
        time.sleep(600)
