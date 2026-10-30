"""
Links the main scanner's Section A order-block MITIGATION signals (OB_BUY / OB_SELL) with volume-spurt stocks.

  A stock counts as a "spurt stock" for the day once it has reached SPURT_LINK_MULT (default 10) times its 50-candle volume
  average in the top-30 spurt table. When Section A then fires OB_BUY / OB_SELL on that stock, you get ONE phone alert
  (same NTFY_TOPIC as the other scanner alerts) and the signal is listed in a table on the /spurt10 page.

It is read-only: it only READS results/latest_signals.csv (the Section A signals the scanner already writes) and the spurt table,
sends a notification and writes results/spurt_link.csv. It does not change any signal, entry, SL, target or the volume-spurt
calculation, and any error is caught by the caller.

Defaults need no Render settings. Optional:
  SPURT_LINK=true            switch the whole link on/off
  SPURT_LINK_MULT=10         spurt multiple that makes a stock a "spurt stock"
  SPURT_LINK_SIGNALS=OB_BUY,OB_SELL   which Section A signals to link (add SWEEP_BUY,SWEEP_SELL if you ever want those too)
  SPURT_LINK_MAX_PER_DAY=40  safety cap on phone notifications per day
"""
import os
import csv
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

log = logging.getLogger("spurt_link")
IST = ZoneInfo("Asia/Kolkata")

ON = os.environ.get("SPURT_LINK", "true").strip().lower() == "true"
LINK_MULT = float(os.environ.get("SPURT_LINK_MULT", "10") or "10")
SIGNALS = {s.strip().upper() for s in (os.environ.get("SPURT_LINK_SIGNALS", "OB_BUY,OB_SELL") or "OB_BUY,OB_SELL").split(",") if s.strip()}
MAX_PER_DAY = int(os.environ.get("SPURT_LINK_MAX_PER_DAY", "40") or "40")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
SIGNALS_CSV = os.path.join(RESULTS_DIR, "latest_signals.csv")          # Section A signals written by the intraday scan
SPURT_HIST = os.path.join(RESULTS_DIR, "spurt10_history.csv")
LINK_CSV = os.path.join(RESULTS_DIR, "spurt_link.csv")
COLS = ["date", "alert_time", "symbol", "signal", "signal_candle", "price", "entry", "sl", "T1", "T2", "spurt_mult", "spurt_at"]

_lock = threading.RLock()
_st = {"date": None, "seen": {}, "done": set(), "count": 0}


def _today(now_ist):
    return now_ist.strftime("%Y-%m-%d")


def _roll(now_ist):
    """New day -> forget yesterday. After a restart the spurt stocks of today are re-read from the spurt history file."""
    today = _today(now_ist)
    if _st["date"] == today:
        return
    _st.update(date=today, seen={}, done=set(), count=0)
    try:
        if os.path.exists(SPURT_HIST):
            h = pd.read_csv(SPURT_HIST)
            d = h["date"].astype(str).str.replace(r"^(\d\d)-(\d\d)-(\d{4})$", r"\3-\2-\1", regex=True)
            h = h[(d == today) & (h["multiple"] >= LINK_MULT)]
            for r in h.sort_values("candle").itertuples():
                cur = _st["seen"].get(r.symbol)
                if cur is None:
                    _st["seen"][r.symbol] = {"mult": float(r.multiple), "at": str(r.candle)}
                elif float(r.multiple) > cur["mult"]:
                    cur["mult"] = float(r.multiple)
        if os.path.exists(LINK_CSV):                                   # signals already announced today (restart safety)
            l = pd.read_csv(LINK_CSV)
            for r in l[l["date"].astype(str) == today].itertuples():
                _st["done"].add((r.symbol, r.signal, str(r.signal_candle)))
            _st["count"] = int((l["date"].astype(str) == today).sum())
    except Exception as e:
        log.warning(f"spurt_link: could not preload today's state: {e}")


def note_spurts(top, now_ist):
    """Called after every spurt scan with the top-30 rows: remembers every stock at >= LINK_MULT today."""
    if not ON:
        return
    with _lock:
        _roll(now_ist)
        for r in top:
            if r.get("multiple", 0) >= LINK_MULT:
                cur = _st["seen"].get(r["symbol"])
                if cur is None:
                    _st["seen"][r["symbol"]] = {"mult": float(r["multiple"]), "at": str(r.get("candle", ""))}
                elif r["multiple"] > cur["mult"]:
                    cur["mult"] = float(r["multiple"])


def _f(v, nd=2):
    try:
        return round(float(v), nd)
    except Exception:
        return ""


def process(now_ist=None):
    """Looks at the latest Section A signals; every OB_BUY / OB_SELL on a stock that spurted >= LINK_MULT today (and was not
    announced yet) is notified and logged. Safe to call as often as you like (from the scan loop and the spurt loop)."""
    if not ON:
        return
    now_ist = now_ist or datetime.now(IST)
    with _lock:
        _roll(now_ist)
        if not _st["seen"] or not os.path.exists(SIGNALS_CSV):
            return
        today = _today(now_ist)
        try:
            sg = pd.read_csv(SIGNALS_CSV)
        except Exception as e:
            log.warning(f"spurt_link: cannot read {SIGNALS_CSV}: {e}")
            return
        if sg.empty or "signal" not in sg or "symbol" not in sg:
            return
        sg = sg[sg["signal"].astype(str).str.upper().isin(SIGNALS)]
        if "timeframe" in sg:
            sg = sg[sg["timeframe"].astype(str).str.lower().isin(["10m", "nan", ""])]
        if "timestamp" in sg:
            sg = sg[sg["timestamp"].astype(str).str.startswith(today)]
        new = []
        try:                                                    # ADDED: no OB alert for a stock with important news (spurt10_live.news_symbols)
            import spurt10_live as _s10
            _nws = _s10.news_symbols()
        except Exception:
            _nws = {}
        for r in sg.itertuples():
            if r.symbol in _nws:
                continue
            key = (r.symbol, str(r.signal).upper(), (str(getattr(r, "timestamp", ""))[11:16] or str(getattr(r, "timestamp", ""))))
            if r.symbol not in _st["seen"] or key in _st["done"]:
                continue
            sp = _st["seen"][r.symbol]
            new.append({"date": today, "alert_time": now_ist.strftime("%H:%M"), "symbol": r.symbol, "signal": str(r.signal).upper(),
                        "signal_candle": str(getattr(r, "timestamp", ""))[11:16] or str(getattr(r, "timestamp", "")),
                        "price": _f(getattr(r, "close", "")), "entry": _f(getattr(r, "entry", "")), "sl": _f(getattr(r, "sl", "")),
                        "T1": _f(getattr(r, "T1", "")), "T2": _f(getattr(r, "T2", "")),
                        "spurt_mult": round(sp["mult"], 1), "spurt_at": sp["at"]})
            _st["done"].add(key)
        if not new:
            return
        os.makedirs(RESULTS_DIR, exist_ok=True)
        fresh = not os.path.exists(LINK_CSV)
        with open(LINK_CSV, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLS)
            if fresh:
                w.writeheader()
            for n in new:
                w.writerow(n)
        if _st["count"] >= MAX_PER_DAY:
            return
        _notify(new)
        _st["count"] += 1


def _notify(new):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    lines = []
    for n in new[:12]:
        lvl = f"  entry {n['entry']:g}  SL {n['sl']:g}  T1 {n['T1']:g}" if n["entry"] != "" and n["sl"] != "" and n["T1"] != "" else ""
        lines.append(f"{n['signal']}  {n['symbol']}  Rs {n['price']}{lvl}  | spurt {n['spurt_mult']:g}x at {n['spurt_at']}")
    extra = f"\n+{len(new) - 12} more on the /spurt10 page" if len(new) > 12 else ""
    title = ("OB signal on spurt stock: " + ", ".join(sorted({n["signal"] for n in new})) + f" ({len(new)})")
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": title, "Priority": "high", "Tags": "zap"})
        if resp.status_code >= 300:
            log.warning(f"spurt_link ntfy refused: {resp.status_code} {resp.text[:150]}")
    except Exception as e:
        log.warning(f"spurt_link ntfy failed: {e}")


def section_html(now_ist=None):
    """Table for the /spurt10 page: today's OB mitigation signals on spurt stocks (newest first)."""
    now_ist = now_ist or datetime.now(IST)
    today = _today(now_ist)
    head = (f'<h3 style="margin:22px 0 4px">OB mitigation signals on spurt stocks (&ge; {LINK_MULT:g}x today)</h3>'
            f'<div class="m">Section A OB_BUY / OB_SELL (10-min) fired on a stock that has reached {LINK_MULT:g}x or more today. '
            f'A phone alert is sent for each. Green = OB_BUY, red = OB_SELL.</div>')
    try:
        if not os.path.exists(LINK_CSV):
            return head + '<div class="m">None yet today.</div>'
        l = pd.read_csv(LINK_CSV)
        l = l[l["date"].astype(str) == today]
        if l.empty:
            return head + '<div class="m">None yet today.</div>'
        rows = ""
        for r in l.iloc[::-1].itertuples():
            bg = "#d4f1dc" if str(r.signal).endswith("BUY") else "#fadad6"
            lv = lambda v: "-" if pd.isna(v) or v == "" else f"{float(v):g}"
            rows += (f'<tr style="background:{bg}"><td>{r.signal_candle}</td><td><b>{r.symbol}</b></td><td>{r.signal}</td><td>{lv(r.price)}</td>'
                     f'<td>{lv(r.entry)}</td><td>{lv(r.sl)}</td><td>{lv(r.T1)}</td><td>{lv(r.T2)}</td><td>{float(r.spurt_mult):g}x</td>'
                     f'<td>{r.spurt_at}</td><td>{r.alert_time}</td></tr>')
        return (head + '<div class="w"><table class="sp"><tr><th>Signal candle</th><th>Symbol</th><th>Signal</th><th>Price</th><th>Entry</th>'
                '<th>SL</th><th>T1</th><th>T2</th><th>Spurt (best)</th><th>Spurt at</th><th>Alert time</th></tr>' + rows + '</table></div>')
    except Exception as e:
        log.warning(f"spurt_link section failed: {e}")
        return ""
