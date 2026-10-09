"""
OPENING RANGE ALIGNMENT  (table at the bottom of /scanner)

For every signal on the page (Section A, Section B, RSI Flush) it checks the stock against its OPENING RANGE
(high / low of the first 15 minutes, 09:15-09:30 IST; ORB_MINUTES to change):

    BUY  signal, price above the range high  -> ALIGNED   (breakout up)
    SELL signal, price below the range low   -> ALIGNED   (breakout down)
    BUY  signal, price below the range low   -> AGAINST
    SELL signal, price above the range high  -> AGAINST
    anything inside the range                -> INSIDE
  The verdict uses the signal's own price (entry, else close). "Now" shows where the stock trades at the moment
  (ABOVE / INSIDE / BELOW the range) so you can see whether the breakout is still holding.
  Signals that formed before 09:30 are marked "range forming".

Read-only: no orders, no change to any signal logic. The opening range of a stock is downloaded ONCE per day
(one 5-min candle call per stock that has a signal); live prices come from one batched LTP call per refresh.

ENV (optional): ORB_AUTORUN=true  ORB_MINUTES=15  ORB_REFRESH_SECONDS=300
"""

import os
import json
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

import live_scanner as ls
from scrip_master import get_security_id_and_segment
import oi_zones_live as oz            # re-uses its reader of the three signal CSVs

log = logging.getLogger("orb")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("ORB_AUTORUN", "true").strip().lower() == "true"
ORB_MINUTES = int(os.environ.get("ORB_MINUTES", "15") or "15")
REFRESH_SECONDS = int(os.environ.get("ORB_REFRESH_SECONDS", "300") or "300")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
LIVE_PATH = os.path.join(RESULTS_DIR, "orb_live.json")
ORB_CSV = os.path.join(RESULTS_DIR, "orb.csv")

_or_cache = {"date": None, "rng": {}, "tries": {}}      # {symbol: (high, low) or None if unavailable}
_ids = {}


def _opening_range(sym):
    """(high, low) of today's first ORB_MINUTES minutes from 5-min candles, or None."""
    sid = _ids.get(sym)
    if sid is None:
        sid, seg = get_security_id_and_segment(sym)
        if not sid or seg != "NSE_EQ":
            return None
        _ids[sym] = sid
    df = ls.fetch_intraday_history(str(sid), "NSE_EQ", 5, 3)
    if df is None or df.empty:
        return None
    ist = df.index + pd.Timedelta(minutes=330)
    today = datetime.now(IST).date()
    df = df[[t.date() == today for t in ist]]
    ist = df.index + pd.Timedelta(minutes=330)
    mins = (ist.hour * 60 + ist.minute) - (9 * 60 + 15)
    part = df[(mins >= 0) & (mins < ORB_MINUTES)]
    need = ORB_MINUTES // 5
    if len(part) < need:
        return None
    return float(part["high"].max()), float(part["low"].min())


def _ltp(symbols):
    by_id = {}
    for s in symbols:
        sid = _ids.get(s)
        if sid:
            by_id[str(sid)] = s
    out = {}
    ids = [int(i) for i in by_id]
    for i in range(0, len(ids), 1000):
        resp = ls._dhan_post("marketfeed/ltp", {"NSE_EQ": ids[i:i + 1000]}, "ORB ltp")
        for sid, q in ((resp.json().get("data") or {}).get("NSE_EQ") or {}).items():
            try:
                out[by_id[str(sid)]] = float(q["last_price"])
            except Exception:
                pass
        time.sleep(1.1)
    return out


def _pos(price, hi, lo):
    return "ABOVE" if price > hi else "BELOW" if price < lo else "INSIDE"


def refresh():
    now = datetime.now(IST)
    today = now.strftime("%Y-%m-%d")
    if _or_cache["date"] != today:
        _or_cache.update(date=today, rng={}, tries={})
    sigs = oz._read_signals()
    syms = sorted({s["symbol"] for s in sigs})
    mins_now = now.hour * 60 + now.minute
    formed = mins_now >= 9 * 60 + 15 + ORB_MINUTES
    errors = 0
    if formed:
        for sym in syms:
            if _or_cache["rng"].get(sym) or _or_cache["tries"].get(sym, 0) >= 3:
                continue
            _or_cache["tries"][sym] = _or_cache["tries"].get(sym, 0) + 1     # a failed/incomplete range is retried, max 3x
            try:
                _or_cache["rng"][sym] = _opening_range(sym)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    log.warning(f"ORB: opening range failed for {sym}: {e}")
    px = {}
    if formed and syms:
        try:
            px = _ltp([s for s in syms if _or_cache["rng"].get(s)])
        except Exception as e:
            log.warning(f"ORB: LTP call failed: {e}")
    rows = []
    for s in sigs:
        r = _or_cache["rng"].get(s["symbol"]) if formed else None
        if not formed:
            rows.append({**s, "or_high": None, "or_low": None, "now": "", "verdict": "range forming", "now_px": None})
            continue
        if not r:
            rows.append({**s, "or_high": None, "or_low": None, "now": "", "verdict": "no range data", "now_px": None})
            continue
        hi, lo = r
        where = _pos(s["price"], hi, lo)
        if s["side"] == "BUY":
            verdict = "ALIGNED" if where == "ABOVE" else "AGAINST" if where == "BELOW" else "INSIDE"
        else:
            verdict = "ALIGNED" if where == "BELOW" else "AGAINST" if where == "ABOVE" else "INSIDE"
        p = px.get(s["symbol"])
        rows.append({**s, "or_high": round(hi, 2), "or_low": round(lo, 2), "now": _pos(p, hi, lo) if p else "",
                     "now_px": round(p, 2) if p else None, "verdict": verdict})
    # 2026-10-09 (Khagen): show only ALIGNED / AGAINST - INSIDE (neutral), "range forming" and "no range data" are not required
    rows = [x for x in rows if x["verdict"] in ("ALIGNED", "AGAINST")]
    order = {"ALIGNED": 0, "AGAINST": 1}
    rows.sort(key=lambda x: (order.get(x["verdict"], 3), x["symbol"]))
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(LIVE_PATH + ".tmp", "w") as f:
        json.dump({"meta": {"run": now.strftime("%Y-%m-%d %H:%M:%S IST"), "minutes": ORB_MINUTES, "errors": errors,
                            "symbols": len(syms)}, "rows": rows}, f)
    os.replace(LIVE_PATH + ".tmp", LIVE_PATH)
    try:
        pd.DataFrame(rows).to_csv(ORB_CSV, index=False)
    except Exception as e:
        log.warning(f"ORB: csv write failed: {e}")
    log.info(f"ORB refreshed: {len(rows)} signals, {len(syms)} stocks, {errors} errors")


def section_html():
    try:
        with open(LIVE_PATH) as f:
            d = json.load(f)
    except Exception:
        return ""
    m, rows = d["meta"], d["rows"]
    colr = {"ALIGNED": "#3fb950", "AGAINST": "#f85149"}
    head = ["Symbol", "Sec", "TF", "Signal", "Signal price", "Range high", "Range low", "Verdict", "Now"]
    body = "".join(
        "<tr>" + "".join(f"<td>{c}</td>" for c in [
            f"<b>{r['symbol']}</b>", r["section"], r["tf"], r["signal"], round(r["price"], 2),
            r["or_high"] if r["or_high"] is not None else "-", r["or_low"] if r["or_low"] is not None else "-",
            f'<b style="color:{colr.get(r["verdict"], "#9aa0a6")}">{r["verdict"]}</b>',
            (f"{r['now']} ({r['now_px']})" if r.get("now") else "-")]) + "</tr>"
        for r in rows)
    if not body:
        body = f'<tr><td colspan="{len(head)}" style="color:#9aa0a6">No ALIGNED or AGAINST signals in the last scan.</td></tr>'
    return (
        '<div style="margin-top:28px;font-family:Arial,sans-serif">'
        f'<h2>Our signals vs opening range ({m["minutes"]}-min, 09:15 start)</h2>'
        f'<div style="color:#9aa0a6;font-size:13px;margin:6px 0 12px">{len(rows)} signals &middot; run {m["run"]} &middot; '
        f'errors {m["errors"]} &middot; <a href="/orb.csv">CSV</a> &middot; ALIGNED = buy above the range high / sell below the range low</div>'
        '<div style="overflow-x:auto"><table><tr>'
        + "".join(f"<th>{h}</th>" for h in head)
        + "</tr>" + body + "</table></div></div>")


def loop():
    log.info(f"ORB alignment autorun started - every {REFRESH_SECONDS}s in market hours (opening range {ORB_MINUTES} min).")
    while True:
        try:
            now = datetime.now(IST)
            if now.weekday() < 5 and 9 * 60 + 16 <= now.hour * 60 + now.minute <= 15 * 60 + 40:
                refresh()
        except Exception as e:
            log.error(f"ORB cycle failed: {e}")
        time.sleep(REFRESH_SECONDS)
