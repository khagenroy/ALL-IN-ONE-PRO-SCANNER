"""
Links the main scanner's Section A signals - Strategy 2 order-block MITIGATION (OB_BUY / OB_SELL) and Strategy 1 liquidity SWEEP
(SWEEP_BUY / SWEEP_SELL) - with volume-spurt stocks.

  CHANGED 8 Nov: (1) the sweep signals are linked too; (2) a signal only counts when its 10-minute signal candle is a REJECTION candle:
  for a BUY signal the LOWER wick is REJ_MIN_PCT (70) % or more of the candle's high-low range, for a SELL signal the UPPER wick is.
  The signal candle is read from Dhan (one 5-minute download per new signal, merged to 10 minutes); the wick % is written to the CSV.

  A stock counts as a "spurt stock" for the day once it has reached SPURT_LINK_MULT (default 10, for OB_BUY / OB_SELL) or SPURT_SWEEP_MULT
  (default 3, for SWEEP_BUY / SWEEP_SELL - CHANGED 12 Nov) times its 50-candle volume average in the top-30 spurt table. When Section A then fires OB_BUY / OB_SELL on that stock, you get ONE phone alert
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
LINK_MULT = float(os.environ.get("SPURT_LINK_MULT", "10") or "10")        # OB_BUY / OB_SELL need a stock at this multiple today
SWEEP_MULT = float(os.environ.get("SPURT_SWEEP_MULT", "3") or "3")        # CHANGED 12 Nov: SWEEP_BUY / SWEEP_SELL need only 3x (was 10x)
MIN_MULT = min(LINK_MULT, SWEEP_MULT)                                      # stocks are remembered from this multiple; each signal then checks its own limit
_DEF_SIGNALS = "OB_BUY,OB_SELL,SWEEP_BUY,SWEEP_SELL"            # CHANGED 8 Nov: sweeps added
SIGNALS = {s.strip().upper() for s in (os.environ.get("SPURT_LINK_SIGNALS", _DEF_SIGNALS) or _DEF_SIGNALS).split(",") if s.strip()}
REJ_MIN_PCT = 70.0            # ADDED 8 Nov: the signal candle's rejection wick (lower for BUY, upper for SELL) must be this % of the candle range or more
VOL_MULT = 1.5                # ADDED 8 Nov: volume confirmation shown for every signal: signal candle volume vs the previous candle AND vs the 50-candle average
VOL_FILTER = True             # CHANGED 8 Nov: volume is COMPULSORY - a signal is only announced / listed when BOTH are >= VOL_MULT (False = only shown, with a tick when both pass)
WICK_RETRY_MAX = 4            # a signal whose candle cannot be read is tried this many times, then dropped
MAX_PER_DAY = int(os.environ.get("SPURT_LINK_MAX_PER_DAY", "40") or "40")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
SIGNALS_CSV = os.path.join(RESULTS_DIR, "latest_signals.csv")          # Section A signals written by the intraday scan
SPURT_HIST = os.path.join(RESULTS_DIR, "spurt10_history.csv")
LINK_CSV = os.path.join(RESULTS_DIR, "spurt_link.csv")
COLS = ["date", "alert_time", "symbol", "signal", "signal_candle", "price", "entry", "sl", "T1", "T2", "spurt_mult", "spurt_at", "wick_pct", "vol_prev_x", "vol_avg_x"]

_lock = threading.RLock()
_st = {"date": None, "seen": {}, "done": set(), "count": 0, "tries": {}}


def _today(now_ist):
    return now_ist.strftime("%Y-%m-%d")


def _roll(now_ist):
    """New day -> forget yesterday. After a restart the spurt stocks of today are re-read from the spurt history file."""
    today = _today(now_ist)
    if _st["date"] == today:
        return
    _st.update(date=today, seen={}, done=set(), count=0, tries={})
    try:
        if os.path.exists(SPURT_HIST):
            h = pd.read_csv(SPURT_HIST)
            d = h["date"].astype(str).str.replace(r"^(\d\d)-(\d\d)-(\d{4})$", r"\3-\2-\1", regex=True)
            h = h[(d == today) & (h["multiple"] >= MIN_MULT)]
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
            if r.get("multiple", 0) >= MIN_MULT:
                cur = _st["seen"].get(r["symbol"])
                if cur is None:
                    _st["seen"][r["symbol"]] = {"mult": float(r["multiple"]), "at": str(r.get("candle", ""))}
                elif r["multiple"] > cur["mult"]:
                    cur["mult"] = float(r["multiple"])


def _candle_info(sym, ts, is_buy):
    """Reads the 10-minute signal candle that opens at IST time `ts` ('YYYY-MM-DD HH:MM'). Returns {"wick": rejection wick % (lower for BUY, upper for SELL),
    "vprev": volume / previous candle's volume, "vavg": volume / average of the 50 candles before it}, or None when the candle cannot be read (retry later)."""
    try:
        import live_scanner as ls
        import spurt10_live as s10
        sid = dict(s10._load_universe()).get(sym)
        if sid is None:
            return None
        d10 = ls.build_merged_bars(ls.fetch_intraday_history(sid, "NSE_EQ", 5, 6), 2)
        if d10 is None or d10.empty:
            return None
        lab = (d10.index + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M")
        hit = [i for i, t in enumerate(lab) if t == ts[:16]]
        if not hit:
            return None
        k = hit[0]
        c = d10.iloc[k]
        o, h, l, cl, v = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]), float(c["volume"])
        wick = 0.0 if h - l <= 0 else round(((min(o, cl) - l) if is_buy else (h - max(o, cl))) / (h - l) * 100.0, 1)
        vol = d10["volume"].to_numpy(dtype=float)
        vprev = round(v / vol[k - 1], 2) if k >= 1 and vol[k - 1] > 0 else ""
        base = vol[max(0, k - 50):k]
        vavg = round(v / base.mean(), 2) if len(base) >= 10 and base.mean() > 0 else ""
        return {"wick": wick, "vprev": vprev, "vavg": vavg, "red": bool(cl < o), "green": bool(cl > o)}
    except Exception as e:
        log.warning(f"spurt_link: signal candle of {sym} could not be read: {e}")
        return None


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
            if sp["mult"] < (SWEEP_MULT if str(r.signal).upper().startswith("SWEEP") else LINK_MULT):
                continue                                     # this stock has not reached the multiple this kind of signal needs (not marked done: a bigger spurt later counts)
            is_buy = str(r.signal).upper().endswith("BUY")
            ci = _candle_info(r.symbol, str(getattr(r, "timestamp", "")), is_buy)
            wp = None if ci is None else ci["wick"]
            if wp is None:                                   # candle not readable now: try again at the next call, give up after WICK_RETRY_MAX
                _st["tries"][key] = _st["tries"].get(key, 0) + 1
                if _st["tries"][key] >= WICK_RETRY_MAX:
                    _st["done"].add(key)
                continue
            _st["done"].add(key)
            if wp < REJ_MIN_PCT or not (ci.get("green") if is_buy else ci.get("red")):        # not a rejection candle (wick below REJ_MIN_PCT, or wrong colour: BUY needs GREEN, SELL needs RED - CHANGED 8 Nov): not announced, not listed
                continue
            if VOL_FILTER and not (ci["vprev"] != "" and ci["vavg"] != "" and ci["vprev"] >= VOL_MULT and ci["vavg"] >= VOL_MULT):
                continue
            new.append({"date": today, "alert_time": now_ist.strftime("%H:%M"), "symbol": r.symbol, "signal": str(r.signal).upper(),
                        "signal_candle": str(getattr(r, "timestamp", ""))[11:16] or str(getattr(r, "timestamp", "")),
                        "price": _f(getattr(r, "close", "")), "entry": _f(getattr(r, "entry", "")), "sl": _f(getattr(r, "sl", "")),
                        "T1": _f(getattr(r, "T1", "")), "T2": _f(getattr(r, "T2", "")),
                        "spurt_mult": round(sp["mult"], 1), "spurt_at": sp["at"], "wick_pct": wp, "vol_prev_x": ci["vprev"], "vol_avg_x": ci["vavg"]})
        if not new:
            return
        os.makedirs(RESULTS_DIR, exist_ok=True)
        try:                                                    # old file without the wick_pct column: add it so the columns stay aligned
            if os.path.exists(LINK_CSV):
                _old = pd.read_csv(LINK_CSV)
                _miss = [c_ for c_ in COLS if c_ not in _old.columns]
                if _miss:
                    for c_ in _miss:
                        _old[c_] = ""
                    _old[COLS].to_csv(LINK_CSV, index=False)
        except Exception as e:
            log.warning(f"spurt_link: could not upgrade {LINK_CSV}: {e}")
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
        lines.append(f"{n['signal']}  {n['symbol']}  Rs {n['price']}{lvl}  | spurt {n['spurt_mult']:g}x at {n['spurt_at']}  | rejection wick {n['wick_pct']:g}%  | vol {n['vol_prev_x'] if n['vol_prev_x'] != '' else '-'}x prev, {n['vol_avg_x'] if n['vol_avg_x'] != '' else '-'}x avg")
    extra = f"\n+{len(new) - 12} more on the /spurt10 page" if len(new) > 12 else ""
    title = ("Section A signal on spurt stock (" + ("sweep" if all(str(n["signal"]).startswith("SWEEP") for n in new) else "OB" if all(str(n["signal"]).startswith("OB") for n in new) else "OB + sweep") + "): " + ", ".join(sorted({n["signal"] for n in new})) + f" ({len(new)})")
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": title, "Priority": "urgent", "Tags": "zap"})
        if resp.status_code >= 300:
            log.warning(f"spurt_link ntfy refused: {resp.status_code} {resp.text[:150]}")
    except Exception as e:
        log.warning(f"spurt_link ntfy failed: {e}")


def section_html(now_ist=None):
    """Table for the /spurt10 page: today's OB mitigation signals on spurt stocks (newest first)."""
    now_ist = now_ist or datetime.now(IST)
    today = _today(now_ist)
    head = (f'<h3 style="margin:22px 0 4px">OB mitigation signals on spurt stocks (&ge; {LINK_MULT:g}x today)</h3>'
            f'<div class="m">Section A Strategy 2 (10-min) OB_BUY / OB_SELL fired on a stock that has reached {LINK_MULT:g}x or more today (the sweep signals are on their own page, /spurt_sweep), '
            f'and only when the signal candle is a rejection candle (lower wick for BUY / upper wick for SELL of {REJ_MIN_PCT:g}% or more of the candle range; the candle must be GREEN for BUY and RED for SELL) that is also a RED candle (close below open). '
            f'{"The signal candle volume must also be " + format(VOL_MULT, "g") + "x or more of the previous candle AND of the 50-candle average. " if VOL_FILTER else ""}'
            f'A phone alert is sent for each. Green = BUY, red = SELL.</div>')
    try:
        if not os.path.exists(LINK_CSV):
            return head + '<div class="m">None yet today.</div>'
        l = pd.read_csv(LINK_CSV)
        l = l[(l["date"].astype(str) == today) & l["signal"].astype(str).str.upper().str.startswith("OB")]      # CHANGED 8 Nov: sweeps have their own page (/spurt_sweep)
        if l.empty:
            return head + '<div class="m">None yet today.</div>'
        rows = ""
        for r in l.iloc[::-1].itertuples():
            bg = "#d4f1dc" if str(r.signal).endswith("BUY") else "#fadad6"
            lv = lambda v: "-" if pd.isna(v) or v == "" else f"{float(v):g}"
            rows += (f'<tr style="background:{bg}"><td>{r.signal_candle}</td><td><b>{r.symbol}</b></td><td>{r.signal}</td><td>{lv(r.price)}</td>'
                     f'<td>{lv(r.entry)}</td><td>{lv(r.sl)}</td><td>{lv(r.T1)}</td><td>{lv(r.T2)}</td><td>{float(r.spurt_mult):g}x</td>'
                     f'<td>{r.spurt_at}</td><td>{lv(getattr(r, "wick_pct", ""))}{"" if lv(getattr(r, "wick_pct", "")) == "-" else "%"}</td><td>{r.alert_time}</td></tr>')
        return (head + '<div class="w"><table class="sp"><tr><th>Signal candle</th><th>Symbol</th><th>Signal</th><th>Price</th><th>Entry</th>'
                '<th>SL</th><th>T1</th><th>T2</th><th>Spurt (best)</th><th>Spurt at</th><th>Wick</th><th>Alert time</th></tr>' + rows + '</table></div>')
    except Exception as e:
        log.warning(f"spurt_link section failed: {e}")
        return ""


def sweep_page_html(now_ist=None):
    """Separate page /spurt_sweep: Section A Strategy 1 SWEEP_BUY / SWEEP_SELL on spurt stocks, today (newest first). Never raises."""
    now_ist = now_ist or datetime.now(IST)
    today = _today(now_ist)
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60">'
            '<title>Sweep signals on spurt stocks</title><style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff;position:sticky;top:0}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    intro = (f'<h2>Sweep signals on volume-spurt stocks (&ge; {SWEEP_MULT:g}x today)</h2><div class="m">Section A Strategy 1 (10-min): SWEEP_BUY / SWEEP_SELL fired on a stock that has reached {SWEEP_MULT:g}x or more today, '
             f'and only when the signal candle is a rejection candle (lower wick for BUY / upper wick for SELL of {REJ_MIN_PCT:g}% or more of the candle range; the candle must be GREEN for BUY and RED for SELL) that is also a RED candle (close below open). '
             f'<b>Volume</b> = the signal candle\'s volume as a multiple of the previous candle and of the 50-candle average; a tick means both are {VOL_MULT:g}x or more '
             f'({"only ticked signals are listed" if VOL_FILTER else "shown for information, the signal is listed either way"}). A phone alert is sent for each. Green = BUY, red = SELL. '
             f'<a href="/spurt_sweep.csv" target="_blank" rel="noopener">CSV</a> &middot; <a href="/spurt10" target="_blank" rel="noopener">Spurt10</a></div>')
    try:
        rows = ""
        if os.path.exists(LINK_CSV):
            l = pd.read_csv(LINK_CSV)
            l = l[(l["date"].astype(str) == today) & l["signal"].astype(str).str.upper().str.startswith("SWEEP")]
            lv = lambda v: "-" if pd.isna(v) or v == "" else f"{float(v):g}"
            for r in l.iloc[::-1].itertuples():
                bg = "#d4f1dc" if str(r.signal).endswith("BUY") else "#fadad6"
                vp, va = getattr(r, "vol_prev_x", ""), getattr(r, "vol_avg_x", "")
                ok = (not pd.isna(vp) and vp != "" and not pd.isna(va) and va != "" and float(vp) >= VOL_MULT and float(va) >= VOL_MULT)
                rows += (f'<tr style="background:{bg}"><td>{r.signal_candle}</td><td><b>{r.symbol}</b></td><td>{r.signal}</td><td>{lv(r.price)}</td><td>{lv(r.entry)}</td><td>{lv(r.sl)}</td>'
                         f'<td>{lv(r.T1)}</td><td>{lv(r.T2)}</td><td>{float(r.spurt_mult):g}x</td><td>{r.spurt_at}</td><td>{lv(getattr(r, "wick_pct", ""))}%</td>'
                         f'<td>{lv(vp)}x / {lv(va)}x {"&#10004;" if ok else ""}</td><td>{r.alert_time}</td></tr>')
        if not rows:
            rows = '<tr><td colspan="13" style="text-align:center">No sweep signal on a spurt stock yet today.</td></tr>'
        return (head + intro + '<table><tr><th>Signal candle</th><th>Symbol</th><th>Signal</th><th>Price</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th>'
                '<th>Spurt (best)</th><th>Spurt at</th><th>Wick</th><th>Volume (prev / avg)</th><th>Alert time</th></tr>' + rows + '</table></body></html>')
    except Exception as e:
        log.warning(f"spurt_link sweep page failed: {e}")
        return head + intro + "<p>The page could not be built - see the logs.</p></body></html>"
