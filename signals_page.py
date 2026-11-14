"""
ALL IN ONE PRO SIGNALS page of the scanner   page: /spurt10_all_in_one_pro_signals    CSV: /spurt10_all_in_one_pro_signals.csv                 ADDED 14 Nov 2026

The six signals - SWEEP | OB MITIGATION | VS SELL | VS BUY | SPURT BUY | SPURT SELL - found by the scanner on every spurt stock (signals_scan.py, spurt_link.py),
each with entry / SL / T1 / T2 / T6 and, as the day goes on, WHAT HAPPENED AFTERWARDS (target or stop-loss), so that after weeks / months you can see which signal works best.

RECORDS     one file per day: results/signals_days/YYYY-MM-DD.json (on the Render disk, never deleted). Every day can be downloaded as its own CSV
            (/spurt10_all_in_one_pro_signals.csv?date=2026-11-14) and all days together (no date). The page lists the days with a link each.
OUTCOME     follow(): the 5-minute candles after the signal are walked one by one with the SAME stop cascade the Dhan bridge uses: SL at the start (= -1R);
            when T1 is touched the SL moves to entry, T2 -> SL to T1, T3 -> T2 ... T6 = exit. T1..T6 = 1R..6R of (entry - SL). Within one candle the SL is checked
            first (cautious: the candle's order is unknown) and such a candle is marked "same candle". The result is written as
            reached (0-6 = highest target touched) / exit (SL, entry, T1 ... T6, OPEN, DAY END) / final R. OPEN = still running, DAY END = the day finished, closed at the last price.
            Only stocks that are in the spurt scan list are updated; the "updated" column shows when each line was last followed.
Places no order. Any error here is caught by the caller.
"""
import os
import json
import html
import glob
import threading
from datetime import datetime, timezone, timedelta

import spurt10_live as sp

TOKEN = "aiop-sig-7Kq2xM9vR4"      # must be the same as SIGNAL_TOKEN in the bridge's signals_feed.py
KEEP_VIEW = 500                    # rows shown by "last 500"
DUP_SECONDS = 600
NAMES = ("SWEEP", "OB MITIGATION", "VS SELL", "VS BUY", "SPURT BUY", "SPURT SELL")
COLOURS = {"SWEEP": "#ffb300", "OB MITIGATION": "#b388ff", "VS SELL": "#ff5252", "VS BUY": "#4caf50", "SPURT BUY": "#00e5ff", "SPURT SELL": "#ff4081"}
DAYS_DIR = os.path.join(sp.RESULTS_DIR, "signals_days")
IST = timezone(timedelta(hours=5, minutes=30))
CSV_COLS = ["date", "time", "symbol", "signal", "side", "entry", "sl", "t1", "t2", "t6", "qty", "source", "reached", "exit", "final_r", "same_candle",
            "t1_time", "t2_time", "t3_time", "t4_time", "t5_time", "t6_time", "exit_time", "last_price", "updated"]
_lock = threading.RLock()
_days = {}          # date -> rows (only days touched in this run are kept in memory)


def _now():
    return datetime.now(IST)


def _num(v):
    try:
        v = float(v)
        return None if v != v else v
    except Exception:
        return None


def _f(v):
    return "-" if v is None else "%.2f" % v


def _path(day):
    return os.path.join(DAYS_DIR, day + ".json")


def _rows(day):
    if day not in _days:
        try:
            with open(_path(day)) as f:
                d = json.load(f)
            _days[day] = d if isinstance(d, list) else []
        except Exception:
            _days[day] = []
        for k in [k for k in _days if k != day and k < _now().strftime("%Y-%m-%d")]:      # old days are not kept in memory
            if k != day:
                _days.pop(k, None)
    return _days[day]


def _save(day):
    try:
        os.makedirs(DAYS_DIR, exist_ok=True)
        with open(_path(day) + ".tmp", "w") as f:
            json.dump(_days.get(day, []), f)
        os.replace(_path(day) + ".tmp", _path(day))
    except Exception:
        pass


def _day_list():
    try:
        return sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(DAYS_DIR, "*.json")))
    except Exception:
        return []


# ------------------------------------------------------------------------------------------------------- adding signals
def _insert(row, key=""):
    day = row["date"]
    with _lock:
        rows = _rows(day)
        if key:
            for r in rows:
                if r.get("key") == key:
                    return False
        for r in reversed(rows[-60:]):          # same signal on the same stock within 10 minutes (e.g. also sent by the bridge) is listed once
            if r.get("symbol") == row["symbol"] and r.get("signal") == row["signal"] and abs(r.get("sec", 0) - row["sec"]) < DUP_SECONDS:
                return False
        row["key"] = key
        rows.append(row)
        _save(day)
    return True


def _mk(symbol, signal, side, price, sl, t1, t2, t6, qty, source, from_ts):
    now = _now()
    return {"date": now.strftime("%Y-%m-%d"), "ts": now.strftime("%Y-%m-%d %H:%M:%S"), "sec": int(now.timestamp()), "symbol": symbol, "signal": signal,
            "side": str(side).upper(), "price": _num(price), "sl": _num(sl), "t1": _num(t1), "t2": _num(t2), "t6": _num(t6), "qty": qty, "src": source,
            "from_ts": from_ts, "reached": None, "exit": "OPEN", "final_r": None, "same": False, "times": {}, "last": None, "upd": ""}


def add(symbol, signal, side, price, sl, t1, t2, t6=None, qty="", key="", source="scanner", from_ts=""):
    """Called by the scanner's own scans. from_ts = 'YYYY-MM-DD HH:MM' = open time of the first 5-minute candle to follow. True when NEW."""
    return _insert(_mk(symbol, signal, side, price, sl, t1, t2, t6, qty, source, from_ts), key)


def receive(d):
    """POST from the Dhan bridge. Returns (ok, message)."""
    if not isinstance(d, dict) or d.get("token") != TOKEN:
        return False, "forbidden"
    name = str(d.get("signal", ""))
    if name not in NAMES:
        return False, "unknown signal"
    now = _now()
    floor5 = now.replace(minute=now.minute - now.minute % 5, second=0, microsecond=0)
    ok = _insert(_mk(str(d.get("symbol", "")), name, d.get("side", ""), d.get("price"), d.get("sl"), d.get("t1"), d.get("t2"), d.get("t6"), d.get("qty", ""), "bridge",
                     floor5.strftime("%Y-%m-%d %H:%M")))
    return True, "saved" if ok else "duplicate"


# ------------------------------------------------------------------------------------------------------- outcome
def follow(side, entry, sl, bars, day_over=False):
    """bars = [(time text, open, high, low, close)] AFTER the signal. Returns dict(reached, exit, final_r, same, times, last)."""
    buy = side == "BUY"
    r = abs(entry - sl)
    if r <= 0:
        return None
    sg = 1 if buy else -1
    lv = [entry + sg * r * k for k in range(1, 7)]
    stop, stage, times, same = sl, 0, {}, False
    out = {"reached": 0, "exit": "OPEN", "final_r": None, "same": False, "times": {}, "last": None}
    for t, o, h, l, c in bars:
        hit_stop = (l <= stop) if buy else (h >= stop)
        nxt = lv[stage] if stage < 6 else None
        if hit_stop:
            if nxt is not None and ((h >= nxt) if buy else (l <= nxt)):
                same = True
            out.update(reached=stage, exit="SL" if stage == 0 else ("entry" if stage == 1 else "T%d" % (stage - 1)),
                       final_r=round((stop - entry) * sg / r, 2), same=same, times=dict(times, exit=t), last=c)
            return out
        while stage < 6 and ((h >= lv[stage]) if buy else (l <= lv[stage])):
            stage += 1
            times["t%d" % stage] = t
            stop = entry if stage == 1 else lv[stage - 2]
        if stage == 6:
            out.update(reached=6, exit="T6", final_r=6.0, same=same, times=dict(times, exit=t), last=c)
            return out
        out["last"] = c
    out.update(reached=stage, times=times, same=same)
    mark = ((out["last"] - entry) * sg / r) if out["last"] is not None else 0.0
    # open: the R the position would have if it were closed now (at the last price, or at the stop if that is better)
    out["final_r"] = round(max(mark, (stop - entry) * sg / r), 2)
    out["exit"] = "DAY END" if day_over else "OPEN"
    return out


def update_outcomes(symbol, bars, day_over=False):
    """bars = [(open time 'YYYY-MM-DD HH:MM', o, h, l, c)] oldest first (candles of today). Called by signals_scan for each scanned stock."""
    day = _now().strftime("%Y-%m-%d")
    changed = False
    with _lock:
        rows = _rows(day)
        for r in rows:
            if r.get("symbol") != symbol or r.get("exit") not in ("OPEN",) or not r.get("from_ts"):
                continue
            if None in (r.get("price"), r.get("sl")):
                continue
            after = [b for b in bars if b[0] >= r["from_ts"]]
            res = follow(r["side"], r["price"], r["sl"], after, day_over)
            if res is None:
                continue
            if (res["reached"], res["exit"], res["final_r"], res["last"]) != (r.get("reached"), r.get("exit"), r.get("final_r"), r.get("last")):
                changed = True
            r.update(reached=res["reached"], exit=res["exit"], final_r=res["final_r"], same=res["same"], times=res["times"], last=res["last"], upd=_now().strftime("%H:%M"))
        if changed:
            _save(day)
    return changed


def close_old_days():
    """A day that has ended is final: lines still OPEN become DAY END (closed at the last price followed)."""
    today = _now().strftime("%Y-%m-%d")
    with _lock:
        for day in _day_list():
            if day >= today:
                continue
            rows = _rows(day)
            ch = False
            for r in rows:
                if r.get("exit") == "OPEN":
                    r["exit"] = "DAY END"
                    ch = True
            if ch:
                _save(day)


# ------------------------------------------------------------------------------------------------------- CSV / page
def _csv_row(day, r):
    t = r.get("times") or {}
    vals = [day, (r.get("ts") or "")[11:16], r.get("symbol"), r.get("signal"), r.get("side"), r.get("price"), r.get("sl"), r.get("t1"), r.get("t2"), r.get("t6"), r.get("qty"),
            r.get("src"), r.get("reached"), r.get("exit"), r.get("final_r"), "yes" if r.get("same") else "", t.get("t1", ""), t.get("t2", ""), t.get("t3", ""), t.get("t4", ""),
            t.get("t5", ""), t.get("t6", ""), t.get("exit", ""), r.get("last"), r.get("upd")]
    return ",".join("" if v is None else str(v).replace(",", " ") for v in vals)


def csv_text(day=None):
    out = [",".join(CSV_COLS)]
    with _lock:
        for d in ([day] if day else _day_list()):
            for r in _rows(d) if d == _now().strftime("%Y-%m-%d") else _read_day(d):
                out.append(_csv_row(d, r))
    return "\n".join(out) + "\n"


def _read_day(day):
    try:
        with open(_path(day)) as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception:
        return []


def _result_text(r):
    if r.get("exit") == "OPEN" and not r.get("reached"):
        return "<span style='color:#9fb0c0'>OPEN</span>"
    reached = r.get("reached") or 0
    fr = r.get("final_r")
    col = "#7be08a" if (fr or 0) > 0 else "#ff7070" if (fr or 0) < 0 else "#e6edf3"
    ex = r.get("exit")
    lab = {"SL": "SL hit", "entry": "T1 then back to entry", "OPEN": "OPEN", "DAY END": "day end"}.get(ex, ("stopped at %s" % ex) if ex and ex.startswith("T") and ex != "T6" else "T6 hit")
    s = "reached T%d &middot; %s" % (reached, lab) if reached else lab
    if r.get("same"):
        s += " (same candle)"
    return "<span style='color:%s'>%s &middot; %s R</span>" % (col, s, "-" if fr is None else ("%+.2f" % fr))


def _stats(rows):
    st = {}
    for r in rows:
        s = st.setdefault(r["signal"], [0, 0, 0, 0, 0.0, 0])      # n, reached>=1, reached>=2, SL first, sumR, closed/with R
        s[0] += 1
        if (r.get("reached") or 0) >= 1:
            s[1] += 1
        if (r.get("reached") or 0) >= 2:
            s[2] += 1
        if r.get("exit") == "SL":
            s[3] += 1
        if r.get("final_r") is not None:
            s[4] += r["final_r"]
            s[5] += 1
    return st


def page_html(show_all=False):
    today = _now().strftime("%Y-%m-%d")
    with _lock:
        days = _day_list()
        if today not in days:
            days.append(today)
        if show_all:
            rows = []
            for d in sorted(days)[-10:]:
                rows += [(d, r) for r in (_rows(d) if d == today else _read_day(d))]
            rows = rows[-KEEP_VIEW:]
        else:
            rows = [(today, r) for r in _rows(today)]
        allrows = []
        for d in days:
            allrows += (_rows(d) if d == today else _read_day(d))
    body = []
    for d, r in reversed(rows):
        c = COLOURS.get(r["signal"], "#9fb0c0")
        body.append("<tr><td>%s</td><td><b>%s</b></td><td style='color:%s;font-weight:700'>%s %s</td><td>%s</td><td style='color:#ff7070'>%s</td>"
                    "<td style='color:#7be08a'>%s</td><td style='color:#7be08a'>%s</td><td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
                        html.escape((d[5:] + " " if show_all else "") + (r.get("ts") or "")[11:16]), html.escape(r["symbol"]), c, html.escape(r["signal"]), html.escape(r["side"]),
                        _f(r.get("price")), _f(r.get("sl")), _f(r.get("t1")), _f(r.get("t2")), _result_text(r), html.escape(r.get("upd") or "")))
    if not body:
        body = ["<tr><td colspan='9' style='color:#9fb0c0'>No signal yet today. It fills the moment the scanner finds one.</td></tr>"]
    counts = {}
    for _d, r in rows:
        counts[r["signal"]] = counts.get(r["signal"], 0) + 1
    chips = " ".join("<span style='border:1px solid %s;color:%s;border-radius:12px;padding:2px 9px;margin-right:6px'>%s %d</span>" % (
        COLOURS[n], COLOURS[n], n, counts.get(n, 0)) for n in NAMES)
    st = _stats(allrows)
    srows = ""
    for n in NAMES:
        if n in st:
            a = st[n]
            srows += ("<tr><td style='color:%s;font-weight:700'>%s</td><td>%d</td><td>%d%%</td><td>%d%%</td><td>%d%%</td><td>%+.2f R</td></tr>" % (
                COLOURS[n], n, a[0], round(a[1] * 100 / a[0]), round(a[2] * 100 / a[0]), round(a[3] * 100 / a[0]), (a[4] / a[5]) if a[5] else 0.0))
    perf = ("<h3 style='margin:18px 0 4px'>Which signal works best (all recorded days)</h3><table><tr><th>Signal</th><th>Signals</th><th>Reached T1</th><th>Reached T2</th><th>SL first</th><th>Average result</th></tr>"
            + srows + "</table>") if srows else ""
    dl = " &middot; ".join("<a href='/spurt10_all_in_one_pro_signals.csv?date=%s' target='_blank' rel='noopener'>%s</a>" % (d, d[5:]) for d in sorted(days, reverse=True)[:40])
    other = "<a href='/spurt10_all_in_one_pro_signals' target='_blank' rel='noopener'>today only</a>" if show_all else "<a href='/spurt10_all_in_one_pro_signals?all=1' target='_blank' rel='noopener'>last 500</a>"
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<meta http-equiv='refresh' content='30'><title>Signals</title><style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}"
            "table{border-collapse:collapse;width:100%%}th,td{padding:7px 10px;border-bottom:1px solid #243040;text-align:left;font-size:14px}th{color:#9fb0c0}"
            "a{color:#8ab4f8}</style></head><body><h2>ALL IN ONE PRO signals (%s)</h2><p>%s</p>"
            "<p style='color:#9fb0c0'>Signals found by the scanner on every spurt stock (same rules as the indicator), followed afterwards: which target or the SL was touched "
            "(stop moves like the bridge: T1 -> entry, T2 -> T1 ...). Times are IST. Refreshes every 30 seconds. %s &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<p style='color:#9fb0c0'>Download CSV: <a href='/spurt10_all_in_one_pro_signals.csv' target='_blank' rel='noopener'><b>all days</b></a> &middot; %s</p>"
            "<table><tr><th>Time</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>What happened</th><th>updated</th></tr>%s</table>%s</body></html>"
            ) % ("last 500" if show_all else "today", chips, other, dl, "".join(body), perf)


def notify(items):
    """ONE phone alert per scan cycle, headed ALL IN ONE PRO: a block per new signal (name, side, stock, entry, SL, T1, T2, T6).
    One message instead of one per signal keeps the phone-alert service from refusing them (HTTP 429); a refused message is tried again."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    blocks = []
    for it in items[:10]:
        blocks.append("%s %s - %s\nEntry %s | SL %s\nT1 %s | T2 %s | T6 %s\nSignal candle %s IST" % (
            it["signal"], it["side"], it["symbol"], _f(_num(it["price"])), _f(_num(it["sl"])),
            _f(_num(it["t1"])), _f(_num(it["t2"])), _f(_num(it.get("t6"))), it.get("candle", "")))
    if len(items) > 10:
        blocks.append("+%d more on the all in one pro signals page" % (len(items) - 10))
    body = "\n\n".join(blocks).encode("utf-8")
    title = "ALL IN ONE PRO" if len(items) == 1 else "ALL IN ONE PRO (%d signals)" % len(items)

    def _send():
        import time
        try:
            import requests
        except Exception:
            return
        for attempt in range(3):
            try:
                r = requests.post("https://ntfy.sh/" + topic, data=body, timeout=10, headers={"Title": title, "Priority": "high", "Tags": "zap"})
                if r.status_code < 300:
                    return
            except Exception:
                pass
            time.sleep(8 * (attempt + 1))

    threading.Thread(target=_send, daemon=True, name="signals-ntfy").start()
