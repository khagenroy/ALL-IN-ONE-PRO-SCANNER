"""
SMA200 LIVE SETUPS - rides on the existing scans (no extra Dhan calls, places no orders).

The intraday scan (every 10 minutes during market hours) and the swing scan (daily after 16:00)
already download the 10m / 1H / 4H and 1D / 1W bars of every stock. After each stock's
Section A / B check, live_scanner hands the same bars to live_setups() here, which looks for
the SMA200 support / rejection rule (see sma200_study.py) on the last few CLOSED bars.
Anything it finds goes into one running list for the day: results/sma200_now.json,
shown at /sma200now (auto-refreshes) and downloadable at /sma200now.csv.

This can never affect Section A / B: live_scanner wraps every call here in its own try/except.
The rule is UNTESTED live - the backtest at /sma200 shows how it has done historically.
"""

import os
import json
import threading
from datetime import datetime

import pandas as pd

RECENT_BARS = {"10m": 3, "1H": 2, "4H": 2, "1D": 1, "1W": 1}   # last N closed bars checked each cycle
STRONG_VOL_X = 1.5          # "strong" = volume >= 1.5x its 20-bar average AND SMA slope >= 1x the timeframe's normal
STRONG_SLOPE_X = 1.0
TF_ORDER = {"10m": 0, "1H": 1, "4H": 2, "1D": 3, "1W": 4}

_LOCK = threading.Lock()


def live_setups(df: pd.DataFrame, tf: str, sym: str) -> list:
    """SMA200 setups on the last few closed bars of one symbol / timeframe. Never raises."""
    try:
        import sma200_study as st
        if tf not in st.TF_CFG:
            return []
        _, live = st.analyse(df, tf, sym, False, RECENT_BARS.get(tf, 1))
        return live
    except Exception:
        return []


def _paths(results_dir):
    return os.path.join(results_dir, "sma200_now.json"), os.path.join(results_dir, "sma200_now.csv"), os.path.join(results_dir, "sma200_now.html")


def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {"rows": {}}


def _key(r):
    return f"{r['symbol']}|{r['tf']}|{r['side']}|{r['bar_time']}"


def update(results_dir: str, mode: str, rows: list, ist_now: datetime, cycle_seconds: float = 0.0):
    """Merge this cycle's setups into the running list, drop earlier-day intraday rows, rewrite page + CSV."""
    os.makedirs(results_dir, exist_ok=True)
    jpath, cpath, hpath = _paths(results_dir)
    now_s = ist_now.strftime("%Y-%m-%d %H:%M:%S")
    today = ist_now.strftime("%Y-%m-%d")
    with _LOCK:
        store = _load(jpath)
        cur = store.get("rows", {})
        # earlier-day intraday rows are not "today" any more; daily/weekly rows stay until they scroll out (2 weeks)
        keep = {}
        for k, r in cur.items():
            if r["tf"] in ("10m", "1H", "4H"):
                if str(r["bar_time"])[:10] == today:
                    keep[k] = r
            else:
                age = (ist_now.date() - datetime.strptime(str(r["bar_time"])[:10], "%Y-%m-%d").date()).days
                if age <= 14:
                    keep[k] = r
        for r in rows:
            k = _key(r)
            if k in keep:
                keep[k].update({x: r[x] for x in ("close", "sma200", "dist_pct", "slope_pct", "slope_x", "vol_x", "stop", "risk_pct", "bars_ago")})
            else:
                r = dict(r)
                r["first_seen"] = now_s
                keep[k] = r
        meta = store.get("meta", {})
        meta[mode] = {"updated": now_s, "cycle_seconds": round(cycle_seconds), "found_this_cycle": len(rows)}
        store = {"rows": keep, "meta": meta}
        tmp = jpath + ".tmp"
        with open(tmp, "w") as f:
            json.dump(store, f, default=str)
        os.replace(tmp, jpath)
        df = pd.DataFrame(list(keep.values()))
        if not df.empty:
            df["strong"] = (df["vol_x"].fillna(0) >= STRONG_VOL_X) & (df["slope_x"].fillna(0) >= STRONG_SLOPE_X)
            df = df.sort_values("first_seen", ascending=False)
        tmp = cpath + ".tmp"
        df.to_csv(tmp, index=False)
        os.replace(tmp, cpath)
        tmp = hpath + ".tmp"
        with open(tmp, "w") as f:
            f.write(_render(df, meta, now_s))
        os.replace(tmp, hpath)


def _f(x, nd=2, suf=""):
    try:
        if x is None or pd.isna(x):
            return "-"
        return f"{float(x):.{nd}f}{suf}"
    except Exception:
        return "-"


def _render(df: pd.DataFrame, meta: dict, now_s: str) -> str:
    def table(d):
        if d.empty:
            return "<div class='empty'>Nothing yet.</div>"
        trs = ""
        for _, r in d.iterrows():
            strong = bool(r.get("strong"))
            trs += (f"<tr class='{'strong' if strong else ''}'><td class='sym'>{r['symbol']}{' &#9733;' if strong else ''}</td><td>{r['tf']}</td>"
                    f"<td class='{'pos' if r['side'] == 'LONG' else 'neg'}'>{r['side']}</td><td>{r['bar_time']}</td><td>{_f(r['close'])}</td>"
                    f"<td>{_f(r['sma200'])}</td><td>{_f(r['dist_pct'], 2, '%')}</td><td>{_f(r['slope_pct'], 2, '%')} ({_f(r['slope_x'], 1)}x)</td>"
                    f"<td>{_f(r['vol_x'], 1, 'x')}</td><td>{_f(r['stop'])}</td><td>{_f(r['risk_pct'], 2, '%')}</td><td>{str(r['first_seen'])[11:16]}</td></tr>")
        return ("<table><thead><tr><th>Symbol</th><th>TF</th><th>Side</th><th>Signal candle (IST)</th><th>Close</th><th>SMA200</th><th>Distance</th>"
                "<th>SMA slope</th><th>Volume vs 20-bar avg</th><th>Stop</th><th>Risk</th><th>Seen at</th></tr></thead><tbody>" + trs + "</tbody></table>")

    if df.empty:
        body = "<div class='empty'>No SMA200 setups found yet.</div>"
    else:
        intr = df[df["tf"].isin(["10m", "1H", "4H"])]
        swing = df[df["tf"].isin(["1D", "1W"])].copy()
        swing["_o"] = swing["tf"].map(TF_ORDER)
        swing = swing.sort_values(["bar_time"], ascending=False)
        body = (f"<h2>Intraday setups today ({len(intr)})</h2>{table(intr)}"
                f"<h2>Daily / weekly setups, last 2 weeks ({len(swing)})</h2>{table(swing)}")
    mt = " &middot; ".join(f"{k}: updated {v['updated']} ({v['found_this_cycle']} found, cycle {v['cycle_seconds']}s)" for k, v in meta.items())
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>SMA200 setups</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }} h2 {{ font-size: 16px; margin: 24px 0 8px; }}
  .nav {{ margin-bottom: 16px; font-size: 14px; }} .nav a {{ color: #58a6ff; text-decoration: none; margin-right: 18px; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 12px; line-height: 1.55; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 20px; }}
  th, td {{ padding: 7px 9px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; }} .sym {{ font-weight: 600; }}
  .pos {{ color: #3fb950; }} .neg {{ color: #f85149; }} .empty {{ color: #9aa0a6; padding: 16px 0; }}
  tr:hover {{ background: #161b22; }} tr.strong {{ background: #1b2a1f; }}
</style></head>
<body>
  <div class="nav"><a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/sma200">SMA200 backtest</a><a href="/sma200now.csv">Download CSV</a></div>
  <h1>SMA200 support / rejection - setups</h1>
  <div class="meta">
    Page time {now_s} IST (auto-refreshes every minute) &middot; {mt or 'no scan has finished yet'}<br>
    <b>LONG:</b> SMA200 rising, price above it, a candle dipped to the SMA200 zone and closed back above it (green). <b>SHORT:</b> SMA200 falling, price below it, a candle rallied to the zone and closed back below it (red).
    Only CLOSED candles are used; the last 3 (10m), 2 (1H/4H) or 1 (1D/1W) candles are checked every cycle, so a setup shows up even if the scan was mid-cycle when the candle closed.
    <b>&#9733; Strong</b> = volume at least 1.5x its 20-candle average and SMA slope at least 1x that timeframe's normal. The stop is beyond the signal candle (+0.1 ATR).
    <b>This rule is still being backtested</b> (see SMA200 backtest) - treat these as a watch list, not a recommendation. 1M is not possible (too little history); 4H appears only when Dhan history is long enough (about 230 bars needed).
  </div>
  {body}
</body></html>"""
