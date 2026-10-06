"""
TRENDLINE LIVE SCANNER - same trendlines as "Price Action Toolkit Lite [UAlgo]" (TradingView).
Rides on the existing scans (no extra Dhan calls, places no orders), exactly like sma200_live.py.

HOW THE PA TOOLKIT DRAWS ITS TRENDLINES (ported 1:1, "Trend Line Detection Sensitivity" = 20):
  * A pivot HIGH = a candle whose high is the highest of the 20 candles on each side (ta.pivothigh(high, 20, 20)).
    A pivot LOW  = the same for lows. A pivot only exists 20 candles AFTER it formed (that is when TradingView confirms it).
  * FALLING line (red) = through the LAST TWO pivot highs, drawn only if the later one is lower. Extended to the right.
  * RISING line (teal) = through the LAST TWO pivot lows, drawn only if the later one is higher. Extended to the right.
  * The line is redrawn every time a newer pivot appears.

WHAT THIS SCANNER FLAGS (on CLOSED candles only; the last 3 (10m), 2 (1H/4H) or 1 (1D/1W) candles are checked every cycle):
  * LONG  - candle CLOSES ABOVE the falling line (previous candle closed on or below it)       -> "Breaks above falling line"
  * SHORT - candle CLOSES BELOW the rising line (previous candle closed on or above it)        -> "Breaks below rising line"
  * LONG  - first touch of a rising line from above that holds (green candle, close above)    -> "Holds rising line (support)"
  * SHORT - first touch of a falling line from below that is rejected (red candle, close below) -> "Rejected at falling line (resistance)"
  * LONG  - same touch-and-hold on a falling line that price had already been ABOVE for 5 candles -> "Retest of broken falling line"
  * SHORT - same on a rising line price had already been BELOW for 5 candles                      -> "Retest of broken rising line"
  A touch = low (high) within 0.25 ATR of the line and not through it by more than 1 ATR, after 5 closes on the right side;
  first touch only (nothing similar in the previous 10 candles). Same touch rule as the SMA200 scanner.

This can never affect Section A / B: live_scanner wraps every call here in its own try/except.
The setups are UNTESTED - treat them as a watch list. Stop = beyond the signal candle (+0.1 ATR).
"""

import os
import json
import threading
from datetime import datetime

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

TREND_LEN = 20              # PA Toolkit "Trend Line Detection Sensitivity" (default 20)
ATR_LEN = 14
TOUCH_ATR = 0.25
PIERCE_ATR = 1.0
PRIOR_BARS = 5
COOLDOWN = 10
STOP_BUFFER_ATR = 0.1
VOL_LEN = 20
STRONG_VOL_X = 1.5
WINDOW = 700                # only the latest candles are needed to find the last two pivots
RECENT_BARS = {"10m": 3, "1H": 2, "4H": 2, "1D": 1, "1W": 1}
INTRADAY = ("10m", "1H", "4H")

_LOCK = threading.Lock()

EVENT_NAMES = {
    ("FALLING", "LONG", "BREAK"): "Breaks above falling line",
    ("RISING", "SHORT", "BREAK"): "Breaks below rising line",
    ("RISING", "LONG", "TOUCH"): "Holds rising line (support)",
    ("FALLING", "SHORT", "TOUCH"): "Rejected at falling line (resistance)",
    ("FALLING", "LONG", "TOUCH"): "Retest of broken falling line (support)",
    ("RISING", "SHORT", "TOUCH"): "Retest of broken rising line (resistance)",
}


# ---------------------------------------------------------------------------
# Trendline maths (port of the PA Toolkit trendline block)
# ---------------------------------------------------------------------------

def _atr(h, l, c, n):
    pc = np.concatenate(([c[0]], c[:-1]))
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    return pd.Series(tr).ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean().to_numpy()


def _pivots(arr: np.ndarray, L: int, high: bool) -> np.ndarray:
    """Bar indexes of pivots (ta.pivothigh / ta.pivotlow with L bars each side). Equal values: the FIRST of a flat top counts."""
    n = len(arr)
    if n < 2 * L + 1:
        return np.array([], dtype=int)
    w = sliding_window_view(arr, 2 * L + 1)          # window k is centred on bar k + L
    ctr, left, right = w[:, L], w[:, :L], w[:, L + 1:]
    if high:
        ok = (ctr > left.max(axis=1)) & (ctr >= right.max(axis=1))
    else:
        ok = (ctr < left.min(axis=1)) & (ctr <= right.min(axis=1))
    return np.nonzero(ok)[0] + L


def _line_at(piv: np.ndarray, vals: np.ndarray, t: int, L: int, falling: bool):
    """The line the toolkit shows on bar t: through the last two pivots CONFIRMED by bar t (pivot bar + L <= t).
    Falling line needs a negative slope, rising line a positive one. Returns (p1, v1, p2, v2, slope) or None."""
    k = int(np.searchsorted(piv, t - L, side="right"))
    if k < 2:
        return None
    p1, p2 = int(piv[k - 2]), int(piv[k - 1])
    v1, v2 = float(vals[p1]), float(vals[p2])
    slope = (v2 - v1) / (p2 - p1)
    if falling and not slope < 0:
        return None
    if (not falling) and not slope > 0:
        return None
    return p1, v1, p2, v2, slope


def _stamp(ts: pd.Timestamp, intraday: bool) -> str:
    if intraday:
        return (ts + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M")      # Dhan intraday stamps are UTC -> IST
    if ts.hour == 18 and ts.minute == 30:                                          # Dhan daily stamp = IST midnight shown as UTC
        ts = ts + pd.Timedelta(minutes=330)
    return ts.strftime("%Y-%m-%d")


def find_setups(df: pd.DataFrame, tf: str, sym: str, recent: int = 1) -> list:
    """Trendline setups on the last `recent` bars of df (df must hold CLOSED bars only)."""
    L = TREND_LEN
    if df is None or len(df) < 2 * L + 30:
        return []
    d = df.iloc[-WINDOW:]
    o = d["open"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    c = d["close"].to_numpy(float); v = d["volume"].to_numpy(float)
    n = len(d)
    atr = _atr(h, l, c, ATR_LEN)
    vma = pd.Series(v).rolling(VOL_LEN, min_periods=VOL_LEN).mean().shift(1).to_numpy()
    ph = _pivots(h, L, True)
    pl = _pivots(l, L, False)
    intraday = tf in INTRADAY
    times = d.index
    out = []
    first_t = max(n - recent, PRIOR_BARS + COOLDOWN + 1)

    for t in range(first_t, n):
        if not (np.isfinite(atr[t]) and atr[t] > 0):
            continue
        for kind, piv, vals, falling in (("FALLING", ph, h, True), ("RISING", pl, l, False)):
            ln = _line_at(piv, vals, t, L, falling)
            if ln is None:
                continue
            p1, v1, p2, v2, slope = ln

            def lv(j, p1=p1, v1=v1, slope=slope):
                return v1 + slope * (j - p1)

            def hold(j):          # candle j touched the line from above and closed back above it, green
                if not np.isfinite(atr[j]):
                    return False
                line = lv(j)
                if not (c[j] > o[j] and c[j] > line and l[j] <= line + TOUCH_ATR * atr[j] and l[j] >= line - PIERCE_ATR * atr[j]):
                    return False
                return all(c[j - k] > lv(j - k) for k in range(1, PRIOR_BARS + 1))

            def reject(j):        # candle j touched the line from below and closed back below it, red
                if not np.isfinite(atr[j]):
                    return False
                line = lv(j)
                if not (c[j] < o[j] and c[j] < line and h[j] >= line - TOUCH_ATR * atr[j] and h[j] <= line + PIERCE_ATR * atr[j]):
                    return False
                return all(c[j - k] < lv(j - k) for k in range(1, PRIOR_BARS + 1))

            events = []
            line_t = lv(t)
            if kind == "FALLING" and c[t] > line_t and c[t - 1] <= lv(t - 1):
                events.append(("LONG", "BREAK"))
            if kind == "RISING" and c[t] < line_t and c[t - 1] >= lv(t - 1):
                events.append(("SHORT", "BREAK"))
            if hold(t) and not any(hold(j) for j in range(t - COOLDOWN, t)):
                events.append(("LONG", "TOUCH"))
            if reject(t) and not any(reject(j) for j in range(t - COOLDOWN, t)):
                events.append(("SHORT", "TOUCH"))

            for side, ev in events:
                is_long = side == "LONG"
                stop = (l[t] - STOP_BUFFER_ATR * atr[t]) if is_long else (h[t] + STOP_BUFFER_ATR * atr[t])
                risk = (c[t] - stop) if is_long else (stop - c[t])
                out.append({
                    "tf": tf, "symbol": sym, "side": side, "event": EVENT_NAMES[(kind, side, ev)],
                    "line": kind.lower(), "bar_time": _stamp(times[t], intraday),
                    "close": round(float(c[t]), 2), "line_value": round(float(line_t), 2),
                    "dist_pct": round((float(c[t]) - float(line_t)) / float(line_t) * 100.0, 2) if line_t else None,
                    "p1_time": _stamp(times[p1], intraday), "p1_price": round(v1, 2),
                    "p2_time": _stamp(times[p2], intraday), "p2_price": round(v2, 2),
                    "line_age_bars": int(t - p2),
                    "vol_x": None if not (np.isfinite(vma[t]) and vma[t] > 0) else round(float(v[t] / vma[t]), 2),
                    "atr_pct": round(float(atr[t] / c[t] * 100.0), 2),
                    "stop": round(float(stop), 2),
                    "risk_pct": round(float(risk / c[t] * 100.0), 2) if risk > 0 else None,
                    "bars_ago": int(n - 1 - t),
                })
    return out


def live_setups(df: pd.DataFrame, tf: str, sym: str) -> list:
    """Trendline setups on the last few CLOSED bars of one symbol / timeframe. Never raises."""
    try:
        if tf not in RECENT_BARS:
            return []
        from sma200_live import _closed_only          # daily / weekly: ignore a candle that has not finished
        df = _closed_only(df, tf)
        if df is None or df.empty:
            return []
        return find_setups(df, tf, sym, RECENT_BARS[tf])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Running list for the day + page
# ---------------------------------------------------------------------------

def _paths(results_dir):
    return (os.path.join(results_dir, "trend_now.json"), os.path.join(results_dir, "trend_now.csv"),
            os.path.join(results_dir, "trend_now.html"))


def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {"rows": {}}


def _key(r):
    return f"{r['symbol']}|{r['tf']}|{r['side']}|{r['event']}|{r['bar_time']}"


def update(results_dir: str, mode: str, rows: list, ist_now: datetime, cycle_seconds: float = 0.0):
    """Merge this cycle's setups into the running list, drop earlier-day intraday rows, rewrite page + CSV."""
    os.makedirs(results_dir, exist_ok=True)
    jpath, cpath, hpath = _paths(results_dir)
    now_s = ist_now.strftime("%Y-%m-%d %H:%M:%S")
    today = ist_now.strftime("%Y-%m-%d")
    with _LOCK:
        store = _load(jpath)
        cur = store.get("rows", {})
        keep = {}
        for k, r in cur.items():
            if r["tf"] in INTRADAY:
                if str(r["bar_time"])[:10] == today:
                    keep[k] = r
            else:
                age = (ist_now.date() - datetime.strptime(str(r["bar_time"])[:10], "%Y-%m-%d").date()).days
                if age <= 14:
                    keep[k] = r
        for r in rows:
            if r["tf"] in INTRADAY and str(r["bar_time"])[:10] != today:
                continue                      # the first scans after 09:15 still hold yesterday's last candles
            k = _key(r)
            if k in keep:
                keep[k].update({x: r[x] for x in ("close", "line_value", "dist_pct", "vol_x", "stop", "risk_pct", "bars_ago")})
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
            df["strong"] = df["vol_x"].fillna(0) >= STRONG_VOL_X
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
                    f"<td class='{'pos' if r['side'] == 'LONG' else 'neg'}'>{r['side']}</td><td>{r['event']}</td><td>{r['bar_time']}</td>"
                    f"<td>{_f(r['close'])}</td><td>{_f(r['line_value'])}</td><td>{_f(r['dist_pct'], 2, '%')}</td>"
                    f"<td>{_f(r['p1_price'])} ({str(r['p1_time'])[5:]}) &rarr; {_f(r['p2_price'])} ({str(r['p2_time'])[5:]})</td>"
                    f"<td>{_f(r['vol_x'], 1, 'x')}</td><td>{_f(r['stop'])}</td><td>{_f(r['risk_pct'], 2, '%')}</td><td>{str(r['first_seen'])[11:16]}</td></tr>")
        return ("<table><thead><tr><th>Symbol</th><th>TF</th><th>Side</th><th>What happened</th><th>Signal candle (IST)</th><th>Close</th>"
                "<th>Line now</th><th>Distance</th><th>Line through pivots (price, date)</th><th>Volume vs 20-bar avg</th><th>Stop</th><th>Risk</th>"
                "<th>Seen at</th></tr></thead><tbody>" + trs + "</tbody></table>")

    if df.empty:
        body = "<div class='empty'>No trendline setups found yet.</div>"
    else:
        intr = df[df["tf"].isin(list(INTRADAY))]
        swing_all = df[df["tf"].isin(["1D", "1W"])].copy()
        if swing_all.empty:
            swing = swing_all
        else:
            latest = swing_all.groupby("tf")["bar_time"].transform("max")
            swing = swing_all[swing_all["bar_time"] == latest].sort_values(["tf", "vol_x"], ascending=[True, False])
        body = (f"<h2>Intraday setups today ({len(intr)})</h2>{table(intr)}"
                f"<h2>Daily / weekly setups on the latest candle ({len(swing)}; {len(swing_all) - len(swing)} older ones are in the CSV)</h2>{table(swing)}")
    mt = " &middot; ".join(f"{k}: updated {v['updated']} ({v['found_this_cycle']} found, cycle {v['cycle_seconds']}s)" for k, v in meta.items())
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>Trendline setups</title>
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
  <div class="nav"><a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/sma200now">SMA200 setups</a><a href="/trendnow.csv">Download CSV</a></div>
  <h1>Trendline setups (same trendlines as PA Toolkit Lite)</h1>
  <div class="meta">
    Page time {now_s} IST (auto-refreshes every minute) &middot; {mt or 'no scan has finished yet'}<br>
    <b>Trendlines:</b> falling line = through the last two pivot highs (later one lower); rising line = through the last two pivot lows (later one higher); pivot = highest / lowest of 20 candles each side, confirmed 20 candles later - the same as the PA Toolkit's red / teal lines at sensitivity 20.<br>
    <b>LONG:</b> candle closes above the falling line, or touches a line from above and holds (green). <b>SHORT:</b> candle closes below the rising line, or touches a line from below and is rejected (red).
    "Line through pivots" shows the two prices and dates the line passes through, so you can find the same line on your chart. Only CLOSED candles are used; the last 3 (10m), 2 (1H/4H) or 1 (1D/1W) candles are checked every cycle.
    <b>&#9733;</b> = volume at least 1.5x its 20-candle average. The stop is beyond the signal candle (+0.1 ATR). <b>These setups are untested</b> - use them as a watch list, not a recommendation.
  </div>
  {body}
</body></html>"""
