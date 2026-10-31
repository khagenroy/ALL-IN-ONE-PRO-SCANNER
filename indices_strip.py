# -*- coding: utf-8 -*-
"""Indices strip: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX and INDIA VIX with the day's % change, shown at the top of every page.

One small background thread asks Dhan for the six index quotes every 20 seconds while the market is open (one call, far below Dhan's limit),
keeps the last values in memory and on the results disk (so the strip also shows the last close at night, at weekends and after a restart),
and serves them at /indices.json. Read-only. It uses the same DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN the scanner already has.
Add to app.py:   import indices_strip ; indices_strip.install(app)
"""
import os
import json
import time
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from flask import jsonify

log = logging.getLogger("indices")
IST = ZoneInfo("Asia/Kolkata")
BASE = os.environ.get("DHAN_BASE_URL", "https://api.dhan.co/v2").rstrip("/")
# (shown name, Dhan security id) - all in the IDX_I segment
INDICES = [("NIFTY", 13), ("BANKNIFTY", 25), ("FINNIFTY", 27), ("MIDCPNIFTY", 442), ("SENSEX", 51), ("INDIA VIX", 21)]
REFRESH_SECONDS = 20
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
FILE = os.path.join(RESULTS_DIR, "indices.json")
_data = {"asof": "", "rows": []}
_lock = threading.Lock()


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fetch():
    cid, tok = os.environ.get("DHAN_CLIENT_ID", ""), os.environ.get("DHAN_ACCESS_TOKEN", "")
    if not (cid and tok):
        return None
    proxy = os.environ.get("DHAN_PROXY_URL", "").strip()
    r = requests.post(f"{BASE}/marketfeed/quote", json={"IDX_I": [i for _n, i in INDICES]},
                      headers={"access-token": tok, "client-id": cid, "Content-Type": "application/json", "Accept": "application/json"},
                      timeout=10, proxies=({"https": proxy, "http": proxy} if proxy else None))
    if not r.ok:
        log.warning(f"indices fetch HTTP {r.status_code}: {r.text[:120]}")
        return None
    seg = ((r.json() or {}).get("data") or {}).get("IDX_I") or {}
    rows = []
    for name, sid in INDICES:
        q = seg.get(str(sid)) or {}
        last = _f(q.get("last_price"))
        if last is None:
            rows.append({"name": name, "price": None, "pct": None})
            continue
        prev = None
        nc = _f(q.get("net_change"))
        if nc is not None and last - nc > 0:
            prev = last - nc
        if prev is None:
            prev = _f((q.get("ohlc") or {}).get("close"))
        pct = round((last - prev) / prev * 100.0, 2) if prev else None
        rows.append({"name": name, "price": round(last, 2), "pct": pct})
    return rows


def _save():
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_data, f)
        os.replace(tmp, FILE)
    except Exception as e:
        log.warning(f"indices save failed: {e}")


def _market_open(now):
    return now.weekday() < 5 and 9 * 60 <= now.hour * 60 + now.minute <= 15 * 60 + 45


def _loop():
    try:
        with open(FILE, "r", encoding="utf-8") as f:
            _data.update(json.load(f))
    except Exception:
        pass
    last_try = 0.0
    while True:
        try:
            now = datetime.now(IST)
            stale = not _data["rows"] or all(r.get("price") is None for r in _data["rows"])
            # while the market is open every REFRESH_SECONDS; otherwise once at start and then every 30 min (keeps the last close fresh)
            gap = REFRESH_SECONDS if _market_open(now) else (30 if stale else 1800)
            if time.time() - last_try >= gap:
                last_try = time.time()
                rows = _fetch()
                if rows and any(r.get("price") is not None for r in rows):
                    with _lock:
                        _data.update(asof=now.strftime("%d-%b %H:%M:%S"), rows=rows)
                    _save()
        except Exception as e:
            log.warning(f"indices loop error: {e}")
        time.sleep(5)


def install(app):
    @app.route("/indices.json")
    def indices_json():
        with _lock:
            return jsonify(_data)

    if not os.environ.get("INDICES_OFF"):
        threading.Thread(target=_loop, daemon=True, name="indices").start()
        log.info("Indices strip started (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, INDIA VIX)")
