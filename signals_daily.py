"""
ALL IN ONE PRO SIGNALS - DAILY timeframe          page: /spurt10_all_in_one_pro_signals_daily     CSV: /spurt10_all_in_one_pro_signals_daily.csv         ADDED 15 Nov 2026

The same six signals as the 5-minute page (SWEEP, OB MITIGATION, VS SELL, VS BUY, SPURT BUY, SPURT SELL), found on the DAILY candle.
It runs inside the daily volume-spurt scan (spurt_daily.py, once a day at 16:45 IST) with the daily candles that scan has ALREADY downloaded:
no extra Dhan call, no order, almost no memory. Only a signal formed on TODAY's daily candle is added (so a stock is listed once per signal).

HOW THE INTRADAY RULES WERE CARRIED OVER TO DAILY CANDLES  (read this - it is an adaptation, not a copy)
  The indicator's rules say "a spurt earlier TODAY ...". On a daily candle "today" is one candle, so here a spurt counts when it happened within the last
  SPURT_DAYS (15) daily candles, and the 10-candle wait of VS SELL / VS BUY means 10 daily candles. The volume multiples are the same (10x / 20x the 50-day average).
  VS SELL's 30-minute resistance check becomes a WEEKLY resistance check. Sweep / OB MITIGATION use the same Section A engine as the 10-minute scan on daily candles.
  Outcomes: each later daily candle is followed with the bridge-style cascade (SL -1R; T1 -> SL to entry ...; T1..T6 = 1R..6R), for up to 30 days, in results/signals_daily.json.
Read-only. Any error is caught by the caller.
"""
import os
import json
import html
import threading
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

import spurt10_live as sp
import signals_scan as ss
import signals_page as spg

IST = timezone(timedelta(hours=5, minutes=30))
SPURT_DAYS = 15
MIN_BARS = 90
FOLLOW_DAYS = 30
JSON_PATH = os.path.join(sp.RESULTS_DIR, "signals_daily.json")
_lock = threading.RLock()
_rows = None
_fresh = []
CSV_COLS = ["date", "symbol", "signal", "side", "entry", "sl", "t1", "t2", "t6", "reached", "exit", "final_r", "same_candle", "t1_date", "t2_date", "t3_date", "t4_date",
            "t5_date", "t6_date", "exit_date", "last_close", "updated"]
HTF_WEEK = ("W-MON", None, pd.Timedelta(days=7))


def _load():
    global _rows
    if _rows is None:
        try:
            with open(JSON_PATH) as f:
                _rows = json.load(f)
            if not isinstance(_rows, list):
                _rows = []
        except Exception:
            _rows = []
    return _rows


def _save():
    try:
        os.makedirs(os.path.dirname(JSON_PATH), exist_ok=True)
        with open(JSON_PATH + ".tmp", "w") as f:
            json.dump(_rows or [], f)
        os.replace(JSON_PATH + ".tmp", JSON_PATH)
    except Exception:
        pass


def begin():
    """Called at the start of a daily scan."""
    global _fresh
    with _lock:
        _load()
        _fresh = []


def _names(sig):
    return {"SWEEP_BUY": ("SWEEP", "BUY"), "SWEEP_SELL": ("SWEEP", "SELL"), "OB_BUY": ("OB MITIGATION", "BUY"), "OB_SELL": ("OB MITIGATION", "SELL")}.get(sig)


def _add(day, sym, name, side, entry, sl):
    if abs(entry - sl) <= 0 or (side == "BUY" and sl >= entry) or (side == "SELL" and sl <= entry):
        return
    t1, t2, t6 = ss._levels(entry, sl, side)
    with _lock:
        rows = _load()
        for r in rows:
            if r["date"] == day and r["symbol"] == sym and r["signal"] == name:
                return
        row = {"date": day, "symbol": sym, "signal": name, "side": side, "entry": round(entry, 2), "sl": round(sl, 2), "t1": round(t1, 2), "t2": round(t2, 2), "t6": round(t6, 2),
               "reached": 0, "exit": "OPEN", "final_r": 0.0, "same": False, "times": {}, "last": None, "upd": day}
        rows.append(row)
        _fresh.append(dict(signal=name, side=side, symbol=sym, price=entry, sl=sl, t1=t1, t2=t2, t6=t6, candle="daily " + day))


def _follow_open(sym, df, today_s):
    """Follow the earlier open signals of this stock with the daily candles after the signal day."""
    with _lock:
        rows = _load()
        for r in rows:
            if r["symbol"] != sym or r["exit"] != "OPEN" or r["date"] >= today_s:
                continue
            after = [(str(d), float(o), float(h), float(l), float(c)) for d, o, h, l, c in zip(df["d"], df["open"], df["high"], df["low"], df["close"]) if str(d) > r["date"]]
            res = spg.follow(r["side"], r["entry"], r["sl"], after, False)
            if res is None:
                continue
            r.update(reached=res["reached"], exit=res["exit"], final_r=res["final_r"], same=res["same"], times=res["times"], last=res["last"], upd=today_s)
            if r["exit"] == "OPEN" and (datetime.strptime(today_s, "%Y-%m-%d") - datetime.strptime(r["date"], "%Y-%m-%d")).days > FOLLOW_DAYS:
                r["exit"] = "30D END"


def scan_stock(sym, raw, today):
    """raw = the stock's daily candles (oldest first, columns open high low close volume d) as spurt_daily reads them. Never raises to the caller's loop (caller wraps it)."""
    today_s = today.strftime("%Y-%m-%d")
    cols = ["open", "high", "low", "close", "volume", "d"]
    df = raw[[c for c in cols if c in raw.columns]].copy()
    df = df[df["d"] <= today].reset_index(drop=True)
    num = ["open", "high", "low", "close", "volume"]
    df[num] = df[num].astype(float)
    cl = df["close"].to_numpy()
    if len(cl) > 1:                                         # split / bonus in the history (Dhan's daily history is not adjusted): use only candles after the last jump
        with np.errstate(divide="ignore", invalid="ignore"):
            chg = np.abs(cl[1:] / cl[:-1] - 1.0)
        jumps = np.where(np.isfinite(chg) & (chg > 0.30))[0]
        if len(jumps):
            df = df.iloc[jumps[-1] + 1:].reset_index(drop=True)
    _follow_open(sym, df, today_s)
    n = len(df)
    if n < MIN_BARS:
        return
    last_d = str(df["d"].iloc[-1])                          # the newest daily candle Dhan has (today's after the close; at worst a day or two old) = the signal day
    if last_d < (today - timedelta(days=4)).strftime("%Y-%m-%d"):
        return
    o, h, l, c, v = (df[k].to_numpy(dtype=float) for k in num)
    win = np.arange(n) >= n - SPURT_DAYS
    mult = np.zeros(n)
    for i in range(max(ss.VS_AVG, n - SPURT_DAYS), n):
        a = v[i - ss.VS_AVG:i].mean()
        mult[i] = v[i] / a if a > 0 else 0.0
    if mult[n - SPURT_DAYS:].max() < ss.VS_MULT:            # no 10x daily spurt within the last 15 candles: none of the six rules can fire
        return
    idx = pd.DatetimeIndex(pd.to_datetime(df["d"]))
    dfi = df[num].copy()
    dfi.index = idx
    rsi = ss._rsi(c)
    last = n - 1
    try:                                                    # SWEEP / OB MITIGATION (Section A engine on daily candles)
        import all_in_one_scanner as a1
        r = a1.compute_section_a(dfi)
        if r and r.get("signal") and r.get("levels"):
            nm = _names(r["signal"])
            if nm:
                _add(last_d, sym, nm[0], nm[1], float(r["levels"]["entry"]), float(r["levels"]["sl"]))
    except Exception:
        pass
    try:                                                    # VS SELL (weekly resistance instead of 30-minute)
        for i, entry, sl in ss._vs_sell(o, h, l, c, v, win, rsi, list(idx), dfi, htf=HTF_WEEK):
            if i == last:
                _add(last_d, sym, "VS SELL", "SELL", entry, sl)
    except Exception:
        pass
    try:                                                    # VS BUY
        import spurt_buy
        for e in spurt_buy.buy_events(o, h, l, c, v, win, ss.VS_MULT):
            if e["i"] == last and float(e["sl"]) < float(c[last]):
                _add(last_d, sym, "VS BUY", "BUY", float(c[last]), float(e["sl"]))
    except Exception:
        pass
    try:                                                    # SPURT BUY / SELL
        for i, side, sl in ss._spurt_d(o, h, l, c, v, win, rsi):
            if i == last:
                _add(last_d, sym, "SPURT BUY" if side == "BUY" else "SPURT SELL", side, float(c[last]), sl)
    except Exception:
        pass


def finish(today):
    """Called at the end of the daily scan: saves, one phone alert."""
    global _fresh
    with _lock:
        _save()
        items, _fresh = list(_fresh), []
    if items:
        spg.notify(items)


# ------------------------------------------------------------------------------------------------------- scan now (button on the page)
_run = {"on": False, "msg": "", "done": 0, "total": 0}


def run_now():
    """Scans the whole stock list on the newest daily candles right now (e.g. at the weekend on Friday's candles). Same rules as the 16:45 scan. Takes a few minutes,
    one stock at a time (small memory). Returns False when a run is already going."""
    with _lock:
        if _run["on"]:
            return False
        _run.update(on=True, msg="starting", done=0, total=0)

    def _job():
        import time
        try:
            import spurt_daily as sdl
            uni = sp._load_universe()
            today = datetime.now(IST).date()
            _run["total"] = len(uni)
            begin()
            found = 0
            for sym, sid in uni:
                try:
                    raw = sdl._daily_candles(sym, sid, today)
                    if raw is not None and len(raw):
                        last = raw.iloc[-1]
                        if float(last["close"]) >= sp.MIN_PRICE and float(last["close"]) * float(last["volume"]) >= sdl.MIN_TURNOVER:      # liquid, as the daily scan
                            scan_stock(sym, raw, today)
                except Exception:
                    pass
                sdl._LAST.pop(sym, None)                    # do not keep the candles in memory
                _run["done"] += 1
                _run["msg"] = "scanning %d / %d" % (_run["done"], _run["total"])
                time.sleep(0.15)
            finish(today)
            _run["msg"] = "finished %s IST - %d signals in the list" % (datetime.now(IST).strftime("%H:%M"), len(_load()))
        except Exception as e:
            _run["msg"] = "failed: %s" % e
        finally:
            _run["on"] = False

    threading.Thread(target=_job, daemon=True, name="signals-daily-now").start()
    return True


def run_status():
    return dict(_run)


# ------------------------------------------------------------------------------------------------------- CSV / page
def _csv_row(r):
    t = r.get("times") or {}
    vals = [r.get("date"), r.get("symbol"), r.get("signal"), r.get("side"), r.get("entry"), r.get("sl"), r.get("t1"), r.get("t2"), r.get("t6"), r.get("reached"), r.get("exit"),
            r.get("final_r"), "yes" if r.get("same") else "", t.get("t1", ""), t.get("t2", ""), t.get("t3", ""), t.get("t4", ""), t.get("t5", ""), t.get("t6", ""), t.get("exit", ""),
            r.get("last"), r.get("upd")]
    return ",".join("" if x is None else str(x).replace(",", " ") for x in vals)


def csv_text(day=None):
    with _lock:
        rows = [r for r in _load() if (not day or r["date"] == day)]
    return "\n".join([",".join(CSV_COLS)] + [_csv_row(r) for r in rows]) + "\n"


def page_html():
    with _lock:
        rows = list(_load())
    days = sorted({r["date"] for r in rows}, reverse=True)
    body = []
    for r in sorted(rows, key=lambda x: (x["date"], x["symbol"]), reverse=True)[:400]:
        c = spg.COLOURS.get(r["signal"], "#9fb0c0")
        body.append("<tr><td>%s</td><td><b>%s</b></td><td style='color:%s;font-weight:700'>%s %s</td><td>%s</td><td style='color:#ff7070'>%s</td><td style='color:#7be08a'>%s</td>"
                    "<td style='color:#7be08a'>%s</td><td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
                        r["date"][5:], html.escape(r["symbol"]), c, html.escape(r["signal"]), html.escape(r["side"]), spg._f(r.get("entry")), spg._f(r.get("sl")),
                        spg._f(r.get("t1")), spg._f(r.get("t2")), spg._result_text(r), html.escape(r.get("upd", "")[5:])))
    if not body:
        body = ["<tr><td colspan='9' style='color:#9fb0c0'>No daily signal yet. The scan runs once a day at 16:45 IST after the market closes.</td></tr>"]
    st = spg._stats(rows)
    srows = "".join("<tr><td style='color:%s;font-weight:700'>%s</td><td>%d</td><td>%d%%</td><td>%d%%</td><td>%d%%</td><td>%+.2f R</td></tr>" % (
        spg.COLOURS[n], n, st[n][0], round(st[n][1] * 100 / st[n][0]), round(st[n][2] * 100 / st[n][0]), round(st[n][3] * 100 / st[n][0]), (st[n][4] / st[n][5]) if st[n][5] else 0.0)
        for n in spg.NAMES if n in st)
    perf = ("<h3 style='margin:18px 0 4px'>Which signal works best (daily, all recorded days)</h3><table><tr><th>Signal</th><th>Signals</th><th>Reached T1</th><th>Reached T2</th>"
            "<th>SL first</th><th>Average result</th></tr>" + srows + "</table>") if srows else ""
    dl = " &middot; ".join("<a href='/spurt10_all_in_one_pro_signals_daily.csv?date=%s' target='_blank' rel='noopener'>%s</a>" % (d, d[5:]) for d in days[:40])
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Daily signals</title>"
            "<style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}table{border-collapse:collapse;width:100%%}th,td{padding:7px 10px;"
            "border-bottom:1px solid #243040;text-align:left;font-size:14px}th{color:#9fb0c0}a{color:#8ab4f8}</style></head><body><h2>ALL IN ONE PRO signals - DAILY candle</h2>"
            "<p style='color:#9fb0c0'>The six signals found on the daily candle (scan once a day at 16:45 IST). The intraday rules were adapted to daily candles: a 10x / 20x volume spurt within the last "
            "%d days replaces 'spurt earlier today'; VS SELL checks WEEKLY resistance. Followed afterwards with the bridge-style stop cascade for up to %d days. "
            "<a href='/spurt10_all_in_one_pro_signals' target='_blank' rel='noopener'>5-minute signals</a> &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<p><a href='/spurt10_all_in_one_pro_signals_daily/run' style='border:1px solid #8ab4f8;border-radius:6px;padding:5px 12px'>Scan now (newest daily candles)</a> &nbsp; <span style='color:#9fb0c0'>%s</span></p>"
            "<p style='color:#9fb0c0'>Download CSV: <a href='/spurt10_all_in_one_pro_signals_daily.csv' target='_blank' rel='noopener'><b>all days</b></a> &middot; %s</p>"
            "<table><tr><th>Day</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>What happened</th><th>updated</th></tr>%s</table>%s</body></html>"
            ) % (SPURT_DAYS, FOLLOW_DAYS, html.escape(_run["msg"] or "A scan takes a few minutes. Refresh this page to see the progress."), dl, "".join(body), perf)
