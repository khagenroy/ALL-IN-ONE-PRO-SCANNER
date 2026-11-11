"""
DAILY SPURT FOLLOW-UP, 30 TRADING DAYS  (page: /spurt_daily_follow   csv: /spurt_daily_follow.csv)             ADDED 11 Nov

Every stock that appears on /spurt_daily (volume 2x or more of its 50-day average) is followed for the next FOLLOW_DAYS (30) TRADING DAYS, so a
swing-trading rule can be studied from real data. For each (spurt day, stock) one line:

  * the spurt day: volume x, day change %, where the close sat inside the day's range (close_pos 100 = closed at the high), zone it was at;
  * gap on the next day, and the move of the CLOSE after 1, 2, 3, 5, 10, 15, 20 and 30 trading days (from the spurt-day close, + = price up);
  * best / worst move seen over the days followed (using highs / lows) and on which day;
  * zone result: for a spurt at RESISTANCE the first day a close came ABOVE the zone (breakout), for SUPPORT the first close BELOW it (breakdown),
    otherwise "held" (no close beyond the zone so far).

  * Runs once a day at the end of the daily scan (16:45 IST), reads each followed stock's daily candles ONCE (about 0.15 s per stock; a stock is
    dropped from the daily reading once its 30 days are complete). No orders, no alerts. Any error is caught and logged.
  * Dhan's daily history is not split-adjusted: if a stock has a day-to-day jump above SPLIT_JUMP (30%) after the spurt day, the follow stops there and the line is marked "split?".
  * Lines before 11 Nov exist only as far back as spurt_daily_history.csv goes (it started when the daily page was built).
"""
import os
import logging
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import spurt10_live as sp
import spurt_daily as sd

log = logging.getLogger("spurt_daily_follow")
IST = sp.IST

ON = True
FOLLOW_DAYS = 30
HORIZONS = (1, 2, 3, 5, 10, 15, 20, 30)
PATH = os.path.join(sd.RESULTS_DIR, "spurt_daily_follow.csv")
COLS = (["date", "symbol", "multiple", "day_chg_pct", "close_pos", "close", "at_zone", "zone", "gap_d1"]
        + [f"r{h}" for h in HORIZONS]
        + ["max_up_pct", "max_up_day", "max_down_pct", "max_down_day", "zone_result", "zone_day", "days_followed", "status", "note"])


def _zone_of(row):
    z = row.get("resistance") if row.get("at_zone") == "RESISTANCE" else row.get("support") if row.get("at_zone") == "SUPPORT" else ""
    return "" if z is None or (isinstance(z, float) and np.isnan(z)) else str(z)


def _follow_one(h, df):
    """One line for one history row h (dict) from the stock's daily candles df (oldest first, column 'd' = date). None when the spurt day is not in df."""
    d0 = pd.to_datetime(h["date"]).date()
    idx = None
    for i, d in enumerate(df["d"]):
        if d == d0:
            idx = i
            break
    if idx is None:
        return None
    o, hi, lo, cl = (df[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    c0 = cl[idx]
    if not c0 > 0:
        return None
    rng = hi[idx] - lo[idx]
    zone = _zone_of(h)
    zn = sd._zone_nums(zone) if zone else None
    out = {k: "" for k in COLS}
    out.update(date=h["date"], symbol=h["symbol"], multiple=h.get("multiple", ""), day_chg_pct=h.get("day_chg_pct", ""),
               close_pos=(round((c0 - lo[idx]) / rng * 100.0, 0) if rng > 0 else ""), close=round(float(c0), 2),
               at_zone=h.get("at_zone", "") if isinstance(h.get("at_zone", ""), str) else "", zone=zone)
    avail = min(FOLLOW_DAYS, len(df) - 1 - idx)
    note = ""
    for k in range(1, avail + 1):                       # stop at a split / bonus (a jump in the daily history)
        if abs(cl[idx + k] / cl[idx + k - 1] - 1.0) > sd.SPLIT_JUMP:
            avail, note = k - 1, "split?"
            break
    if avail >= 1:
        out["gap_d1"] = round((o[idx + 1] / c0 - 1.0) * 100.0, 2)
        for hz in HORIZONS:
            if hz <= avail:
                out[f"r{hz}"] = round((cl[idx + hz] / c0 - 1.0) * 100.0, 2)
        ups = hi[idx + 1: idx + avail + 1] / c0 - 1.0
        dns = lo[idx + 1: idx + avail + 1] / c0 - 1.0
        out["max_up_pct"], out["max_up_day"] = round(float(ups.max()) * 100.0, 2), int(ups.argmax()) + 1
        out["max_down_pct"], out["max_down_day"] = round(float(dns.min()) * 100.0, 2), int(dns.argmin()) + 1
        if zn:
            lo_z, hi_z = zn
            res = "held"
            zday = ""
            for k in range(1, avail + 1):
                if out["at_zone"] == "RESISTANCE" and cl[idx + k] > hi_z:
                    res, zday = "broke above", k
                    break
                if out["at_zone"] == "SUPPORT" and cl[idx + k] < lo_z:
                    res, zday = "broke below", k
                    break
            out["zone_result"], out["zone_day"] = res, zday
    out["days_followed"] = avail
    out["note"] = note
    out["status"] = "done" if (avail >= FOLLOW_DAYS or note) else "tracking"
    return out


def run(today):
    """Called at the end of spurt_daily.run_once. Never raises."""
    if not ON:
        return
    try:
        if not os.path.exists(sd.HIST_PATH):
            return
        hist = pd.read_csv(sd.HIST_PATH)
        hist = hist[hist["date"].astype(str) >= (today - timedelta(days=FOLLOW_DAYS * 2 + 14)).strftime("%Y-%m-%d")]      # 30 trading days = about 44 calendar days
        old = {}
        if os.path.exists(PATH):
            try:
                for r in pd.read_csv(PATH).to_dict("records"):
                    old[(str(r["date"]), r["symbol"])] = r
            except Exception as e:
                log.warning(f"spurt_daily_follow: old csv not read, starting again: {e}")
        todo = {}
        for h in hist.to_dict("records"):
            key = (str(h["date"]), h["symbol"])
            if old.get(key, {}).get("status") == "done":
                continue
            todo.setdefault(h["symbol"], []).append(h)
        sid_of = dict(sp._load_universe())
        new_rows, n_calls = {}, 0
        for sym, hs in todo.items():
            sid = sid_of.get(sym)
            if sid is None:
                continue
            df = sd._daily_candles(sym, sid, today)
            n_calls += 1
            time.sleep(0.15)
            if df is None:
                continue
            for h in hs:
                r = _follow_one(h, df)
                if r:
                    new_rows[(str(h["date"]), sym)] = r
        rows = {k: {c: v.get(c, "") for c in COLS} for k, v in old.items()}
        rows.update(new_rows)
        cutoff = (today - timedelta(days=FOLLOW_DAYS * 2 + 14)).strftime("%Y-%m-%d")
        out = [r for k, r in rows.items() if str(k[0]) >= cutoff or r.get("status") == "done"]       # finished lines stay for ever
        out.sort(key=lambda r: (str(r["date"]), float(r["multiple"] or 0)), reverse=True)
        os.makedirs(sd.RESULTS_DIR, exist_ok=True)
        pd.DataFrame(out, columns=COLS).to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        log.info(f"spurt_daily_follow: {len(out)} lines ({sum(1 for r in out if r['status'] == 'done')} complete), {n_calls} stocks read")
    except Exception as e:
        log.warning(f"spurt_daily_follow failed: {e}")


def _num(v):
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except Exception:
        return None


def page_html():
    """The /spurt_daily_follow page, read from the saved csv. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Daily Spurt Follow-up</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;background:#fff;margin-bottom:14px}'
            'th,td{border:1px solid #ddd;padding:5px 7px;text-align:right;font-size:13px}th{background:#222;color:#fff;position:sticky;top:0}'
            'td:nth-child(1),td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        if not os.path.exists(PATH):
            return (head + f"<h2>Daily spurt stocks &mdash; follow-up for {FOLLOW_DAYS} trading days</h2><p>No data yet. It is written once a day after the daily scan (16:45 IST, Mon-Fri), "
                    "from the days saved in the daily spurt history.</p></body></html>")
        df = pd.read_csv(PATH)
        pc = lambda v: "-" if _num(v) is None else f'<span style="color:{"#0a7d33" if _num(v) > 0 else "#c62828" if _num(v) < 0 else "#555"}">{_num(v):+.1f}%</span>'
        rcols = [f"r{h}" for h in HORIZONS]

        def summary(label, sub):
            if sub.empty:
                return ""
            cells = ""
            for c in rcols:
                s = pd.to_numeric(sub[c], errors="coerce").dropna()
                cells += (f"<td>{len(s)}</td><td>{pc(s.mean())}</td><td>{s.gt(0).mean() * 100:.0f}%</td>" if len(s) else "<td>0</td><td>-</td><td>-</td>")
            return f"<tr><td colspan='2' style='text-align:left'><b>{label}</b></td>{cells}</tr>"
        gr = df["day_chg_pct"].apply(lambda v: (_num(v) or 0) > 0)
        sm = "".join([summary("All spurts", df), summary("At RESISTANCE", df[df["at_zone"] == "RESISTANCE"]), summary("At SUPPORT", df[df["at_zone"] == "SUPPORT"]),
                      summary("No zone", df[df["at_zone"].isna() | (df["at_zone"] == "")]), summary("Spurt day GREEN", df[gr]), summary("Spurt day RED", df[~gr]),
                      summary("Closed in top 25% of the day", df[pd.to_numeric(df["close_pos"], errors="coerce") >= 75]),
                      summary("Closed in bottom 25% of the day", df[pd.to_numeric(df["close_pos"], errors="coerce") <= 25])])
        sh = "".join(f"<th colspan='3'>{h}d</th>" for h in HORIZONS)
        sh2 = "".join("<th>n</th><th>avg</th><th>up</th>" for _ in HORIZONS)
        body = ""
        for r in df.to_dict("records"):
            zr = r.get("zone_result") if isinstance(r.get("zone_result"), str) else ""
            zc = "#0a7d33" if zr == "broke above" else "#c62828" if zr == "broke below" else "#555"
            body += (f"<tr><td>{r['date']}</td><td>{sp._tvl(r['symbol'])}</td><td>{_num(r['multiple']) or 0:.1f}x</td><td>{pc(r['day_chg_pct'])}</td><td>{'-' if _num(r['close_pos']) is None else format(_num(r['close_pos']), '.0f') + '%'}</td>"
                     f"<td>{r['at_zone'] if isinstance(r['at_zone'], str) else '-'}</td><td>{r['zone'] if isinstance(r['zone'], str) else '-'}</td><td>{pc(r['gap_d1'])}</td>"
                     + "".join(f"<td>{pc(r[c])}</td>" for c in rcols)
                     + f"<td>{pc(r['max_up_pct'])} <small>d{'' if _num(r['max_up_day']) is None else int(_num(r['max_up_day']))}</small></td>"
                     f"<td>{pc(r['max_down_pct'])} <small>d{'' if _num(r['max_down_day']) is None else int(_num(r['max_down_day']))}</small></td>"
                     f"<td style=\"color:{zc}\">{zr or '-'}{'' if _num(r['zone_day']) is None else ' d' + str(int(_num(r['zone_day'])))}</td>"
                     f"<td>{'' if _num(r['days_followed']) is None else int(_num(r['days_followed']))}/{FOLLOW_DAYS}{' (split?)' if isinstance(r['note'], str) and r['note'] else ''}</td></tr>")
        return (head + f"<h2>Daily spurt stocks &mdash; follow-up for {FOLLOW_DAYS} trading days ({len(df)} lines, {int((df['status'] == 'done').sum())} complete)</h2>"
                '<div class="m">Every stock on /spurt_daily (volume 2x+ of its 50-day average) is followed for the next 30 trading days. Moves are from the SPURT-DAY CLOSE, + = price up; '
                '"gap" = next day open; "close in day" = where the close sat inside the spurt day range (100% = at the high); best / worst use the highs / lows (d = trading day). '
                'Zone result: at RESISTANCE = first close above the zone (broke above), at SUPPORT = first close below it (broke below), otherwise held. '
                'Updated once a day after 16:45 IST. Information only. <a href="/spurt_daily.csv">Today</a> &middot; <a href="/spurt_daily_follow.csv">CSV</a> &middot; '
                '<a href="/spurt_daily_history.csv">History CSV</a> &middot; <a href="/spurt_daily" target="_blank" rel="noopener">Daily spurt page</a></div>'
                "<h3>What happened after the spurt (n = lines with that many days done, avg = average move, up = share of lines that closed above the spurt-day close)</h3>"
                f"<table><tr><th colspan='2'></th>{sh}</tr><tr><th colspan='2'>Group</th>{sh2}</tr>{sm}</table>"
                "<h3>All lines</h3><table><tr><th>Spurt day</th><th>Symbol</th><th>Spurt</th><th>Day chg</th><th>Close in day</th><th>At zone</th><th>Zone</th><th>Gap d1</th>"
                + "".join(f"<th>{h}d</th>" for h in HORIZONS) + "<th>Best</th><th>Worst</th><th>Zone result</th><th>Days</th></tr>" + body + "</table></body></html>")
    except Exception as e:
        log.warning(f"spurt_daily_follow page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
