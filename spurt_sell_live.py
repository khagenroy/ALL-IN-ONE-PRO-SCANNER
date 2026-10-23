"""
SPURT SELL  (page: /spurt_sell   csv: /spurt_sell.csv)

Same rule as the TradingView indicator "VOLUME SPURT RED CANDLE SELL", on the 10-minute candles the volume-spurt
scanner already downloads (no extra Dhan calls):

  1. SPURT      a green 10-min candle whose volume >= SPURT_SELL_MULT (50) x the average volume of the previous
                SPURT_SELL_AVG_LEN (50) candles. The 09:15 opening candle counts too (set SPURT_SELL_SKIP_OPEN=true to ignore it).
  2. RED CANDLE within SPURT_SELL_WAIT (10) candles after the spurt, a SOLID red candle (body >= SPURT_SELL_BODY_PCT
                (60) % of its high-low range) whose volume is higher than the candle before it.
  3. SELL       entry = close of that red candle, stop loss = its high, targets = 1x and 2x the risk below the entry.
                One signal per spurt.

Read-only: it places no orders, is not connected to Section A / Section B / the bot, and any error here is caught
and logged without touching any other scan. Signals are logged to results/spurt_sell_history.csv.

Only stocks the volume-spurt scan scores in a cycle (the 150 busiest) can be checked in that cycle.

ENV (all optional)
  SPURT_SELL_ENABLED=true
  SPURT_SELL_MULT=50   SPURT_SELL_AVG_LEN=50   SPURT_SELL_WAIT=10   SPURT_SELL_BODY_PCT=60
  SPURT_SELL_SKIP_OPEN=false   (true = ignore the 09:15 candle as a spurt)
  SPURT_SELL_R1=1      SPURT_SELL_R2=2
  SPURT_SELL_NTFY_TOPIC=<a NEW ntfy topic>     phone alert goes ONLY to this topic (blank = no phone alert)
  SPURT_SELL_NTFY_MAX_PER_DAY=40
"""

import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("spurt_sell")
IST = ZoneInfo("Asia/Kolkata")


def _f(name, default):
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return float(default)


ENABLED = os.environ.get("SPURT_SELL_ENABLED", "true").strip().lower() == "true"
MULT = _f("SPURT_SELL_MULT", 50)
AVG_LEN = int(_f("SPURT_SELL_AVG_LEN", 50))
WAIT = int(_f("SPURT_SELL_WAIT", 10))
BODY_PCT = _f("SPURT_SELL_BODY_PCT", 60)
R1 = _f("SPURT_SELL_R1", 1)
R2 = _f("SPURT_SELL_R2", 2)
MAX_PER_DAY = int(_f("SPURT_SELL_NTFY_MAX_PER_DAY", 40))
SKIP_OPEN = os.environ.get("SPURT_SELL_SKIP_OPEN", "false").strip().lower() == "true"     # default: the 09:15 candle COUNTS as a spurt

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt_sell_history.csv")
COLS = ["date", "run", "candle", "symbol", "entry", "sl", "t1", "t2", "risk_pct", "spurt_candle", "spurt_mult",
        "candles_after_spurt", "red_vol_vs_prev", "day_chg_pct"]

_seen = {"date": None, "keys": set(), "count": 0}


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


# ---------------------------------------------------------------- the rule
def check(df, sym, day_chg=None):
    """df = 10-min candles, last row = latest CLOSED candle. Returns a signal dict or None. Never raises."""
    if not ENABLED:
        return None
    try:
        n = len(df)
        if n < AVG_LEN + 3:
            return None
        o = df["open"].to_numpy(dtype=float)
        h = df["high"].to_numpy(dtype=float)
        l = df["low"].to_numpy(dtype=float)
        c = df["close"].to_numpy(dtype=float)
        v = df["volume"].to_numpy(dtype=float)
        L = n - 1

        def red(i):
            rng = h[i] - l[i]
            return c[i] < o[i] and rng > 0 and (o[i] - c[i]) / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]

        if not red(L):
            return None
        sj, smult = None, None
        for j in range(L - 1, max(L - WAIT, AVG_LEN) - 1, -1):          # latest spurt first
            avg = v[j - AVG_LEN:j].mean()
            if avg <= 0:
                continue
            m = v[j] / avg
            t = _ist(df.index[j])
            if m >= MULT and c[j] > o[j] and not (SKIP_OPEN and t.hour == 9 and t.minute == 15):
                sj, smult = j, m
                break
        if sj is None:
            return None
        for m_ in range(sj + 1, L):                                      # one signal per spurt
            if red(m_):
                return None
        entry, sl = float(c[L]), float(h[L])
        risk = sl - entry
        if risk <= 0 or entry <= 0:
            return None
        return {
            "symbol": sym,
            "candle": _ist(df.index[L]).strftime("%H:%M"),
            "entry": round(entry, 2), "sl": round(sl, 2),
            "t1": round(entry - risk * R1, 2), "t2": round(entry - risk * R2, 2),
            "risk_pct": round(risk / entry * 100, 2),
            "spurt_candle": _ist(df.index[sj]).strftime("%H:%M"),
            "spurt_mult": round(float(smult), 1),
            "candles_after_spurt": int(L - sj),
            "red_vol_vs_prev": round(float(v[L] / v[L - 1]), 2) if v[L - 1] > 0 else None,
            "day_chg_pct": day_chg,
        }
    except Exception:
        return None


# ---------------------------------------------------------------- log + phone alert
def _load_seen(today):
    _seen.update(date=today, keys=set(), count=0)
    try:
        if os.path.exists(HIST_PATH):
            old = pd.read_csv(HIST_PATH, usecols=["date", "candle", "symbol"], dtype=str)
            for d, c_, s in old[old["date"] == today].itertuples(index=False):
                _seen["keys"].add((c_, s))
    except Exception as e:
        log.warning(f"SpurtSell: could not read history ({e})")


def _alert(new, now_ist):
    topic = os.environ.get("SPURT_SELL_NTFY_TOPIC", "").strip()
    if not topic or os.environ.get("SPURT_SELL_NTFY", "true").strip().lower() != "true":
        return
    if _seen["count"] >= MAX_PER_DAY:
        return
    lines = [f"{r['symbol']}  SELL {r['entry']:.2f}  SL {r['sl']:.2f}  T1 {r['t1']:.2f}  T2 {r['t2']:.2f}  (spurt {r['spurt_mult']:g}x at {r['spurt_candle']})"
             for r in new[:12]]
    extra = f"\n+{len(new) - 12} more on /spurt_sell" if len(new) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"SELL after volume spurt ({new[0]['candle']} candle closed)", "Priority": "high",
                                      "Tags": "chart_with_downwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"SpurtSell ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"SpurtSell ntfy failed: {e}")
        return
    _seen["count"] += 1
    log.info(f"SpurtSell alert sent for {len(new)} stock(s)")


def process(hits, now_ist):
    """Called once per volume-spurt cycle with every signal found. Logs the new ones and sends the phone alert. Never raises."""
    try:
        if not ENABLED or not hits:
            return
        today = now_ist.strftime("%Y-%m-%d")
        if _seen["date"] != today:
            _load_seen(today)
        new = []
        for r in sorted(hits, key=lambda r: -r["spurt_mult"]):
            k = (r["candle"], r["symbol"])
            if k in _seen["keys"]:
                continue
            _seen["keys"].add(k)
            new.append(r)
        if not new:
            return
        run = now_ist.strftime("%H:%M:%S")
        os.makedirs(RESULTS_DIR, exist_ok=True)
        new_file = not os.path.exists(HIST_PATH)
        pd.DataFrame([{"date": today, "run": run, **{c_: r.get(c_) for c_ in COLS[2:]}} for r in new], columns=COLS).to_csv(
            HIST_PATH, mode="a", header=new_file, index=False)
        log.info(f"SpurtSell: {len(new)} new SELL signal(s): {[r['symbol'] for r in new]}")
        _alert(new, now_ist)
    except Exception as e:
        log.warning(f"SpurtSell process failed: {e}")


# ---------------------------------------------------------------- page
_SORT_JS = """<script>
document.querySelectorAll('table.s th').forEach(function(th,i){th.style.cursor='pointer';th.title='click to sort';
th.addEventListener('click',function(){var t=th.closest('table'),b=t.tBodies[0],rows=Array.prototype.slice.call(b.rows),
asc=th.dataset.asc!=='1';th.dataset.asc=asc?'1':'0';
function val(r){var x=r.cells[i].innerText.replace(/[,%x]/g,'').trim();var n=parseFloat(x);return isNaN(n)?x.toLowerCase():n;}
rows.sort(function(a,b2){var p=val(a),q=val(b2);if(p<q)return asc?-1:1;if(p>q)return asc?1:-1;return 0;});
rows.forEach(function(r){b.appendChild(r);});});});
</script>"""


def page_html(date=None):
    import html as _h
    try:
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<meta http-equiv="refresh" content="60"><title>Spurt SELL</title>'
                '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;background:#fff}'
                'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}'
                'td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
        rule = (f"Rule: a green 10-min candle with volume &ge; {MULT:g}x the average of the previous {AVG_LEN} candles (09:15 candle {'ignored' if SKIP_OPEN else 'included'}), then within "
                f"{WAIT} candles a solid red candle (body &ge; {BODY_PCT:g}% of its range) with higher volume than the candle before it. "
                f"SELL at that red candle's close, stop loss = its high, targets = {R1:g}x and {R2:g}x the risk. One signal per spurt. "
                "Paper/information only - the scanner places no orders.")
        if not os.path.exists(HIST_PATH):
            return head + f'<h2>Spurt SELL</h2><div class="m">{rule}</div><p>No signal yet. It fills when a stock matches the rule during market hours.</p></body></html>'
        df = pd.read_csv(HIST_PATH)
        if df.empty:
            return head + f'<h2>Spurt SELL</h2><div class="m">{rule}</div><p>No signal yet.</p></body></html>'
        dates = sorted(df["date"].unique(), reverse=True)
        d = date if date in dates else dates[0]
        x = df[df["date"] == d].sort_values(["candle", "spurt_mult"], ascending=[False, False])
        col = lambda v: "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"
        pct = lambda v: "-" if v != v or v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'
        rows = "".join(
            f"<tr><td>{r.candle}</td><td><b>{_h.escape(str(r.symbol))}</b></td><td>{r.entry:.2f}</td><td style='color:#c62828'>{r.sl:.2f}</td>"
            f"<td style='color:#0a7d33'>{r.t1:.2f}</td><td style='color:#0a7d33'>{r.t2:.2f}</td><td>{r.risk_pct:.2f}%</td>"
            f"<td>{r.spurt_candle}</td><td><b>{r.spurt_mult:g}x</b></td><td>{int(r.candles_after_spurt)}</td><td>{pct(r.day_chg_pct)}</td></tr>"
            for r in x.itertuples())
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        return (head + f'<h2>Spurt SELL &mdash; {_h.escape(d)}</h2>'
                f'<form method="get" class="m">Day <select name="date">{opts}</select> <button>Show</button> &middot; {len(x)} signals &middot; '
                f'click any heading to sort &middot; <a href="/spurt_sell.csv">download CSV (all days)</a> &middot; <a href="/spurt10">live /spurt10</a></form>'
                f'<div class="m">{rule}</div>'
                '<table class="s"><tr><th>Signal candle</th><th>Symbol</th><th>SELL entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th>'
                '<th>Risk %</th><th>Spurt candle</th><th>Spurt multiple</th><th>Candles after spurt</th><th>Day %</th></tr>'
                f'{rows}</table>{_SORT_JS}</body></html>')
    except Exception as e:
        return f"<p>Spurt SELL page error: {_h.escape(str(e))}</p>"
