"""
SIGNALS page of the scanner  (page: /spurt10_all_in_one_pro_signals, CSV: /spurt10_all_in_one_pro_signals.csv)                                          ADDED 14 Nov 2026

Shows the six ALL IN ONE PRO signals the moment TradingView sends them:
    SWEEP | OB MITIGATION | VS SELL | VS BUY | SPURT BUY | SPURT SELL      (entry, SL, T1, T2, quantity)
The DHAN bridge receives every TradingView alert, and forwards these six to POST /signals_in on this scanner (same token in both files).
Newest first, today only (?all=1 = last 500). Saved in results/signals.json (the Render disk), CSV in results/signals.csv. Places no order.
"""
import os
import json
import html
import threading
from datetime import datetime, timezone, timedelta

import spurt10_live as sp

TOKEN = "aiop-sig-7Kq2xM9vR4"      # must be the same as SIGNAL_TOKEN in the bridge's signals_feed.py
KEEP = 500
DUP_SECONDS = 90
NAMES = ("SWEEP", "OB MITIGATION", "VS SELL", "VS BUY", "SPURT BUY", "SPURT SELL")
COLOURS = {"SWEEP": "#ffb300", "OB MITIGATION": "#b388ff", "VS SELL": "#ff5252", "VS BUY": "#4caf50", "SPURT BUY": "#00e5ff", "SPURT SELL": "#ff4081"}
JSON_PATH = os.path.join(sp.RESULTS_DIR, "signals.json")
CSV_PATH = os.path.join(sp.RESULTS_DIR, "signals.csv")
IST = timezone(timedelta(hours=5, minutes=30))
_lock = threading.RLock()
_rows = None


def _load():
    global _rows
    if _rows is None:
        try:
            with open(JSON_PATH) as f:
                d = json.load(f)
            _rows = d[-KEEP:] if isinstance(d, list) else []
        except Exception:
            _rows = []
    return _rows


def _save():
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        with open(JSON_PATH + ".tmp", "w") as f:
            json.dump(_rows[-KEEP:], f)
        os.replace(JSON_PATH + ".tmp", JSON_PATH)
        with open(CSV_PATH + ".tmp", "w") as f:
            f.write("time,symbol,signal,side,entry,sl,t1,t2,t6,qty\n")
            for r in _rows:
                f.write(",".join(str(r.get(k, "")) for k in ("ts", "symbol", "signal", "side", "price", "sl", "t1", "t2", "t6", "qty")) + "\n")
        os.replace(CSV_PATH + ".tmp", CSV_PATH)
    except Exception:
        pass


def _num(v):
    try:
        v = float(v)
        return None if v != v else v
    except Exception:
        return None


def _f(v):
    return "-" if v is None else "%.2f" % v


def receive(d):
    """d = JSON from the bridge. Returns (ok, message)."""
    if not isinstance(d, dict) or d.get("token") != TOKEN:
        return False, "forbidden"
    name = str(d.get("signal", ""))
    if name not in NAMES:
        return False, "unknown signal"
    now = datetime.now(IST)
    row = {"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "sec": int(now.timestamp()), "symbol": str(d.get("symbol", "")), "signal": name,
           "side": str(d.get("side", "")).upper(), "price": _num(d.get("price")), "sl": _num(d.get("sl")), "t1": _num(d.get("t1")),
           "t2": _num(d.get("t2")), "t6": _num(d.get("t6")), "qty": d.get("qty")}
    with _lock:
        rows = _load()
        for r in reversed(rows[-30:]):
            if r.get("symbol") == row["symbol"] and r.get("signal") == name and r.get("price") == row["price"] and row["sec"] - r.get("sec", 0) < DUP_SECONDS:
                return True, "duplicate"
        rows.append(row)
        del rows[:-KEEP]
        _save()
    return True, "saved"


def add(symbol, signal, side, price, sl, t1, t2, t6=None, qty="", key="", source="scanner"):
    """Called by the scanner's own scans (signals_scan.py, spurt_link.py). Returns True when the row is NEW (False = already listed)."""
    now = datetime.now(IST)
    with _lock:
        rows = _load()
        for r in rows[-400:]:
            if key and r.get("key") == key:
                return False
        for r in reversed(rows[-60:]):          # the same signal on the same stock within 10 minutes (e.g. already sent by the bridge) is not listed twice
            if r.get("symbol") == symbol and r.get("signal") == signal and now.timestamp() - r.get("sec", 0) < 600:
                return False
        rows.append({"ts": now.strftime("%Y-%m-%d %H:%M:%S"), "sec": int(now.timestamp()), "symbol": symbol, "signal": signal, "side": str(side).upper(),
                     "price": _num(price), "sl": _num(sl), "t1": _num(t1), "t2": _num(t2), "t6": _num(t6), "qty": qty, "key": key, "src": source})
        del rows[:-KEEP]
        _save()
    return True


def notify(items):
    """One phone alert headed ALL IN ONE PRO, one block per new signal (name, side, stock, entry, SL, T1, T2, T6)."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    try:
        import requests
        for it in items[:8]:
            msg = "%s %s - %s\nEntry %s\nSL %s\nT1 %s\nT2 %s\nT6 %s\nSignal candle %s IST (scanner)" % (
                it["signal"], it["side"], it["symbol"], _f(_num(it["price"])), _f(_num(it["sl"])), _f(_num(it["t1"])), _f(_num(it["t2"])), _f(_num(it.get("t6"))), it.get("candle", ""))
            requests.post("https://ntfy.sh/" + topic, data=msg.encode("utf-8"), timeout=8, headers={"Title": "ALL IN ONE PRO", "Priority": "high", "Tags": "zap"})
        if len(items) > 8:
            requests.post("https://ntfy.sh/" + topic, data=("+%d more on the all in one pro signals page" % (len(items) - 8)).encode("utf-8"), timeout=8,
                          headers={"Title": "ALL IN ONE PRO", "Priority": "high"})
    except Exception:
        pass


def page_html(show_all=False):
    with _lock:
        rows = list(_load())
    if not show_all:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        rows = [r for r in rows if r.get("ts", "").startswith(today)]
    body = []
    for r in reversed(rows):
        c = COLOURS.get(r["signal"], "#9fb0c0")
        body.append("<tr><td>%s</td><td><b>%s</b></td><td style='color:%s;font-weight:700'>%s %s</td><td>%s</td><td style='color:#ff7070'>%s</td>"
                    "<td style='color:#7be08a'>%s</td><td style='color:#7be08a'>%s</td><td>%s</td></tr>" % (
                        html.escape(r["ts"][5:16]), html.escape(r["symbol"]), c, html.escape(r["signal"]), html.escape(r["side"]),
                        _f(r.get("price")), _f(r.get("sl")), _f(r.get("t1")), _f(r.get("t2")), html.escape(str(r.get("qty", "")))))
    if not body:
        body = ["<tr><td colspan='8' style='color:#9fb0c0'>No signal yet today. It fills the moment TradingView sends one.</td></tr>"]
    counts = {}
    for r in rows:
        counts[r["signal"]] = counts.get(r["signal"], 0) + 1
    chips = " ".join("<span style='border:1px solid %s;color:%s;border-radius:12px;padding:2px 9px;margin-right:6px'>%s %d</span>" % (
        COLOURS[n], COLOURS[n], n, counts.get(n, 0)) for n in NAMES)
    other = "<a href='/spurt10_all_in_one_pro_signals' target='_blank' rel='noopener'>today only</a>" if show_all else "<a href='/spurt10_all_in_one_pro_signals?all=1' target='_blank' rel='noopener'>last 500</a>"
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<meta http-equiv='refresh' content='30'><title>Signals</title><style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}"
            "table{border-collapse:collapse;width:100%%}th,td{padding:7px 10px;border-bottom:1px solid #243040;text-align:left;font-size:14px}th{color:#9fb0c0}"
            "a{color:#8ab4f8}</style></head><body><h2>ALL IN ONE PRO signals (%s)</h2><p>%s</p>"
            "<p style='color:#9fb0c0'>Every signal the indicator sent, whether or not the bridge traded it. Times are IST. Refreshes by itself every 30 seconds. "
            "%s &nbsp;|&nbsp; <a href='/spurt10_all_in_one_pro_signals.csv' target='_blank' rel='noopener'>CSV</a> &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<table><tr><th>Time</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>Qty</th></tr>%s</table></body></html>"
            ) % ("last 500" if show_all else "today", chips, other, "".join(body))
