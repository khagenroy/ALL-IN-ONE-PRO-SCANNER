"""
Daily volume spurt + daily support / resistance zones (page /spurt_daily).

WHAT IT DOES
  Once a day after the market closes (16:45 IST, Mon-Fri) it looks at every liquid NSE stock and compares TODAY's full-day volume with the
  average of the last 50 daily volumes. Stocks at DAILY_MIN_MULT (2x) or more are listed, biggest multiple first, together with the
  support / resistance zones of the DAILY chart (same ALL IN ONE PRO order-block rules as the 10-minute page, from spurt_zones.live_zones,
  just on daily candles). A row turns green when the price is AT a daily support zone and red when it is AT a daily resistance zone.
  A phone alert (ntfy, same NTFY_TOPIC) is sent for stocks with a multiple of DAILY_ALERT_MULT (3x) or more that sit at a zone.

LIQUID = price >= 50, traded at least Rs 1 crore today, and (when Dhan sends a depth) a bid-ask spread of 0.25% or less - the same rule as
the 10-minute spurt page. ETFs are left out (the same universe as /spurt10).

IT DOES NOT TOUCH the 10-minute spurt scan, the SELL signals, any signal / entry / SL / target logic. Read-only: it only reads Dhan
quotes and daily candles and writes its own files (results/spurt_daily.json / .csv / _history.csv).

MEMORY: one stock at a time, only small numbers are kept; the daily candles of a stock are thrown away after it is scored. It never runs
at start-up - only once a day after 16:45 IST (a catch-up run, 15 min after a restart, only if today's scan is missing).
No env variables needed (defaults in the code).
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import live_scanner as ls
import spurt10_live as sp
import spurt_zones

log = logging.getLogger("spurt_daily")
IST = sp.IST

AUTORUN = os.environ.get("SPURT_DAILY_AUTORUN", "true").strip().lower() == "true"
AVG_LEN = 50                 # days in the volume average
MIN_HISTORY = 30             # a stock needs at least this many earlier daily candles
MIN_MULT = 2.0               # listed from this multiple of the 50-day average
ALERT_MULT = 3.0             # phone alert (when also at a zone) from this multiple
MIN_AVG_VOL = 1000           # ignore stocks whose 50-day average volume is below this many shares
MIN_TURNOVER = 1e7           # liquid: traded at least Rs 1 crore today
HIST_DAYS = 420              # calendar days of daily candles to read (about 280 trading days)
RUN_AT_MIN = 16 * 60 + 45    # 16:45 IST (the swing scan runs at 16:00, so this stays clear of it)
TOP_N = 100
ALERT_MAX_LINES = 12

RESULTS_DIR = sp.RESULTS_DIR
JSON_PATH = os.path.join(RESULTS_DIR, "spurt_daily.json")
CSV_PATH = os.path.join(RESULTS_DIR, "spurt_daily.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt_daily_history.csv")

_state = {"last_run_date": None, "running": False}


# ---------------------------------------------------------------- data
def _fetch_quotes(uni):
    """{symbol: dict(px, vol, open, high, low, spread)} via marketfeed/quote, 1000 ids per call."""
    by_id = {sid: sym for sym, sid in uni}
    ids = [int(s) for s in by_id]
    out = {}
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": chunk}, f"daily quote batch {i // 1000 + 1}")
        data = (resp.json().get("data") or {}).get("NSE_EQ") or {}
        for sid, q in data.items():
            sym = by_id.get(str(sid))
            if not sym or not isinstance(q, dict):
                continue
            try:
                px = float(q.get("last_price"))
                vol = float(q.get("volume"))
            except (TypeError, ValueError):
                continue
            oh = q.get("ohlc") or {}
            try:
                o, h, l = float(oh.get("open")), float(oh.get("high")), float(oh.get("low"))
            except (TypeError, ValueError):
                o = h = l = px
            dp = sp._parse_depth(q)
            out[sym] = {"px": px, "vol": vol, "open": o or px, "high": h or px, "low": l or px, "spread": (dp[0] if dp else None)}
        time.sleep(1.1)
    return out


def _liquid(quotes):
    names = []
    for sym, d in quotes.items():
        if d["px"] < sp.MIN_PRICE or d["px"] * d["vol"] < MIN_TURNOVER:
            continue
        if d["spread"] is not None and d["spread"] > sp.LIQ_SPREAD_THIN:
            continue
        names.append(sym)
    return names


def _to_date(ts):
    t = float(ts)
    if t < 1e9:                       # seconds counted from 1980-01-01 (Dhan's other timestamp style)
        t += 315513000
    return datetime.fromtimestamp(t, IST).date()


def _daily_candles(sym, sid, today):
    """Daily candles BEFORE today (oldest first) as a DataFrame, or None. Today's candle is built from the quote instead."""
    frm = (today - timedelta(days=HIST_DAYS)).strftime("%Y-%m-%d")
    j = None
    for to in ((today + timedelta(days=1)), today):
        try:
            resp = ls._dhan_post("charts/historical", {"securityId": str(sid), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY",
                                                       "expiryCode": 0, "oi": False, "fromDate": frm, "toDate": to.strftime("%Y-%m-%d")},
                                 f"daily candles {sym}")
            j = resp.json()
            if j and j.get("close"):
                break
        except Exception:
            j = None
    if not j or not j.get("close"):
        return None
    df = pd.DataFrame({k: j.get(k) for k in ("open", "high", "low", "close", "volume", "timestamp")})
    df = df.dropna()
    df["d"] = [_to_date(t) for t in df["timestamp"]]
    df = df[df["d"] < today].sort_values("d").reset_index(drop=True)      # drop today's candle if Dhan sent it
    return df[["open", "high", "low", "close", "volume"]].astype(float) if len(df) else None


def _score(sym, sid, q, today):
    df = _daily_candles(sym, sid, today)
    if df is None or len(df) < MIN_HISTORY:
        return None
    avg = float(df["volume"].tail(AVG_LEN).mean())
    if avg < MIN_AVG_VOL:
        return None
    mult = q["vol"] / avg
    if mult < MIN_MULT:
        return {"_skip": True}
    prev_close = float(df["close"].iloc[-1])
    day_chg = round((q["px"] - prev_close) / prev_close * 100.0, 2) if prev_close > 0 else None
    today_bar = pd.DataFrame([{"open": q["open"], "high": q["high"], "low": q["low"], "close": q["px"], "volume": q["vol"]}])
    zs = spurt_zones.live_zones(pd.concat([df, today_bar], ignore_index=True))
    row = {"symbol": sym, "price": round(q["px"], 2), "day_chg_pct": day_chg, "day_vol": int(q["vol"]), "avg_vol": int(avg),
           "multiple": round(mult, 2), "ohlc": (q["open"], q["high"], q["low"], q["px"])}
    row.update(sp._zone_fields(zs, q["px"], day_chg))
    row["zone_src"] = "Daily"
    return row


# ---------------------------------------------------------------- scan
def run_once():
    t0 = time.monotonic()
    now = datetime.now(IST)
    today = now.date()
    uni = sp._load_universe()
    sid_of = dict(uni)
    quotes = _fetch_quotes(uni)
    names = _liquid(quotes)
    if len(names) < 50:                                   # holiday / no data: do not overwrite anything
        log.info(f"Spurt daily: only {len(names)} liquid stocks have traded today - nothing to scan (market holiday?)")
        return False
    rows, failed, scanned = [], 0, 0
    for sym in names:
        try:
            r = _score(sym, sid_of[sym], quotes[sym], today)
        except Exception as e:
            failed += 1
            if failed <= 3:
                log.warning(f"Spurt daily: {sym} failed: {e}")
            if failed >= 40 and failed > scanned // 2:
                raise RuntimeError("too many daily-candle failures - Dhan problem?")
            continue
        scanned += 1
        if r and not r.get("_skip"):
            rows.append(r)
        time.sleep(0.15)
    rows.sort(key=lambda r: -r["multiple"])
    meta = {"run": now.strftime("%Y-%m-%d %H:%M:%S IST"), "date": today.strftime("%Y-%m-%d"), "took_s": round(time.monotonic() - t0),
            "universe": len(uni), "liquid": len(names), "scored": scanned, "failed": failed, "listed": len(rows),
            "min_mult": MIN_MULT, "alert_mult": ALERT_MULT, "avg_len": AVG_LEN}
    _write(rows, meta)
    try:
        _append_history(rows, meta)
    except Exception as e:
        log.warning(f"Spurt daily history failed: {e}")
    try:
        _send_alert(rows, meta)
    except Exception as e:
        log.warning(f"Spurt daily alert failed: {e}")
    log.info(f"Spurt daily: {len(rows)} stocks at {MIN_MULT:g}x or more, {len(names)} liquid stocks scanned in {meta['took_s']}s")
    return True


def _write(rows, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    flat = [{k: v for k, v in r.items() if k != "ohlc"} for r in rows]
    pd.DataFrame(flat).to_csv(CSV_PATH + ".tmp", index=False)
    os.replace(CSV_PATH + ".tmp", CSV_PATH)
    with open(JSON_PATH + ".tmp", "w") as f:
        json.dump({"meta": meta, "rows": rows}, f)
    os.replace(JSON_PATH + ".tmp", JSON_PATH)


def _append_history(rows, meta):
    if not rows:
        return
    cols = ["date", "symbol", "price", "day_chg_pct", "day_vol", "avg_vol", "multiple", "support", "support_dist_pct",
            "resistance", "resistance_dist_pct", "zone_view", "at_zone"]
    df = pd.DataFrame([{**{k: r.get(k) for k in cols if k != "date"}, "date": meta["date"]} for r in rows])[cols]
    if os.path.exists(HIST_PATH):
        old = pd.read_csv(HIST_PATH)
        old = old[old["date"] != meta["date"]]
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(HIST_PATH + ".tmp", index=False)
    os.replace(HIST_PATH + ".tmp", HIST_PATH)


def _send_alert(rows, meta):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (sp.NTFY_ON and topic):
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and r.get("at_zone") in ("SUPPORT", "RESISTANCE")]
    if not hits:
        return
    lines = []
    for r in hits[:ALERT_MAX_LINES]:
        zone = r.get("support") if r["at_zone"] == "SUPPORT" else r.get("resistance")
        lines.append(f"{r['at_zone']}  {r['symbol']}  Rs {r['price']:.2f}  daily spurt {r['multiple']:.1f}x  zone {zone}"
                     + (f"  day {r['day_chg_pct']:+.2f}%" if r.get("day_chg_pct") is not None else ""))
    extra = f"\n+{len(hits) - ALERT_MAX_LINES} more on /spurt_daily" if len(hits) > ALERT_MAX_LINES else ""
    import requests
    resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                         headers={"Title": f"Daily volume spurt at a daily zone ({len(hits)})", "Priority": "high", "Tags": "bar_chart"})
    if resp.status_code >= 300:
        log.warning(f"Spurt daily ntfy refused: {resp.status_code} {resp.text[:150]}")


# ---------------------------------------------------------------- page
_LEGEND = sp.LEGEND.split(' <span style="background:#f08d84;padding:2px 8px;border:1px solid #c9564c">Offers')[0]      # same colour key as /spurt10, without the Offers % entry

def page_html():
    """The /spurt_daily page, read from the last saved scan. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="300"><title>Daily Volume Spurt</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}td:nth-child(2){text-align:left}'
            '.m{color:#555;font-size:13px;margin:6px 0 12px}.w{overflow-x:auto}</style></head><body>')
    try:
        if not os.path.exists(JSON_PATH):
            return (head + "<h2>Daily Volume Spurt</h2><p>No daily scan has run yet. It runs once a day after the market closes (16:45 IST, Mon-Fri) "
                    "and shows the stocks whose volume today is 2x or more of their 50-day average, with daily support / resistance zones.</p></body></html>")
        with open(JSON_PATH) as f:
            d = json.load(f)
        meta, rows = d["meta"], d["rows"]

        def col(v):
            return "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"

        def pct(v):
            return "-" if v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'

        def ctype(r):
            v = r.get("day_chg_pct")
            oh = r.get("ohlc")
            svg = sp._candle_svg(*oh) if oh else ""
            if v is None:
                return "<td>-</td>"
            if v > 0:
                return '<td style="white-space:nowrap">' + svg + ' <b style="color:#089981">GREEN</b></td>'
            if v < 0:
                return '<td style="white-space:nowrap">' + svg + ' <b style="color:#F23645">RED</b></td>'
            return '<td style="white-space:nowrap">' + svg + ' DOJI</td>'

        def mstyle(r):
            return ' style="background:#ffe066"' if r["multiple"] >= meta.get("alert_mult", 3.0) else ""

        shown = rows[:TOP_N]
        body = "".join(
            f"<tr{sp._rowstyle(r.get('at_zone'))}><td>{i}</td><td>{sp._tvl(r['symbol'])}</td><td>{r['price']:.2f}</td><td>{pct(r.get('day_chg_pct'))}</td>{ctype(r)}"
            f"<td>{r['day_vol']:,}</td><td>{r['avg_vol']:,}</td>"
            f"<td{mstyle(r)}><b>{r['multiple']:.2f}x</b></td>"
            f"<td{sp._zbg(r.get('at_zone'), 'SUPPORT')}>{sp._zc(r, 'support')}</td><td{sp._zbg(r.get('at_zone'), 'RESISTANCE')}>{sp._zc(r, 'resistance')}</td>"
            f"<td>{sp._zv(r.get('zone_view'), r.get('zone_src'))}</td></tr>"
            for i, r in enumerate(shown, 1)) or '<tr><td colspan="11">No stock reached the daily spurt today.</td></tr>'
        return (head + f'<h2>Daily Volume Spurt &mdash; today\'s volume vs the {meta["avg_len"]}-day average volume</h2>'
                f'<div class="m">Scan of {meta["date"]}: {meta["run"]} (took {meta["took_s"]}s) &middot; {meta["liquid"]} liquid NSE stocks scanned '
                f'(price &ge; {sp.MIN_PRICE:.0f}, traded &ge; Rs {MIN_TURNOVER / 1e7:g} crore, thin spreads left out), {meta["listed"]} at {meta["min_mult"]:g}x or more '
                f'&middot; runs once a day after 16:45 IST &middot; phone alert at {meta["alert_mult"]:g}x or more when the price is AT a daily zone &middot; {_LEGEND} &middot; '
                f'zones = ALL IN ONE PRO order-block zones on the DAILY chart &middot; <a href="/spurt_daily.csv">CSV</a> &middot; <a href="/spurt_daily_history.csv">history CSV</a> &middot; '
                f'<a href="/spurt10">10-min volume spurt</a> &middot; <a href="/scanner">Scanner</a></div>'
                '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Day candle</th><th>Day volume</th>'
                f'<th>{meta["avg_len"]}-day avg vol</th><th>Multiple</th><th>Support zone (daily)</th><th>Resistance zone (daily)</th><th>Zone view</th></tr>{body}</table></div>'
                + sp._SORT_JS + "</body></html>")
    except Exception as e:
        log.warning(f"Spurt daily page failed: {e}")
        return head + "<p>The daily spurt page could not be built. Please refresh later.</p></body></html>"


# ---------------------------------------------------------------- loop
def _have_today(today_s):
    try:
        with open(JSON_PATH) as f:
            return json.load(f)["meta"].get("date") == today_s
    except Exception:
        return False


def loop():
    log.info("Spurt daily autorun started - once a day after 16:45 IST (Mon-Fri); catch-up 15 min after a restart if today's scan is missing.")
    time.sleep(900)                                       # stay out of the way of the start-up scans
    fails = 0
    while True:
        try:
            now = datetime.now(IST)
            today_s = now.strftime("%Y-%m-%d")
            due = now.weekday() < 5 and now.hour * 60 + now.minute >= RUN_AT_MIN
            if due and _state["last_run_date"] != today_s and not _have_today(today_s):
                try:
                    ok = run_once()
                    _state["last_run_date"] = today_s     # done for today (also when it was a holiday: nothing more to do)
                    fails = 0
                except Exception as e:
                    fails += 1
                    log.error(f"Spurt daily scan failed ({fails}): {e}")
                    if fails >= 3:
                        _state["last_run_date"] = today_s
                        fails = 0
                    else:
                        time.sleep(600)
            elif due:
                _state["last_run_date"] = today_s
        except Exception as e:
            log.error(f"Spurt daily loop error: {e}")
        time.sleep(300)
