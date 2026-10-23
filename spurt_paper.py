"""
SPURT PAPER TRADING  (page: /spurt_paper   csv: /spurt_paper.csv)

Paper-trades every Spurt SELL signal (see spurt_sell_live.py / the TradingView indicator) with virtual money, so the
rule can be judged on real market data before any real order is placed. Read-only: it places NO order anywhere.

HOW A PAPER TRADE RUNS
  entry   SELL at the close of the red signal candle (the price on the /spurt_sell page).
  size    quantity = PAPER_RISK_RS / (stop loss - entry), at least 1 share, never more than PAPER_MAX_NOTIONAL rupees of stock.
  exits   followed on 5-minute candles until one of these happens:
            stop loss        (the red candle's high)           -> whole position out at the stop
            target 1 (1R)    half the quantity out at T1, the stop moves to the entry price (breakeven)
            target 2 (2R)    the remaining half out at T2
            back to entry    after T1, the remaining half out at the entry price (breakeven)
            15:10 IST        anything still open is closed at the last 5-minute close (intraday square-off)
  if the stop and a target are both touched inside the same 5-minute candle, the STOP is counted first (pessimistic).
  a gap through the stop fills at the candle's open (worse than the stop), like a real stop-market order.
  costs   PAPER_COST_PCT percent of the traded value (buy + sell) is deducted as brokerage/taxes.
  one open paper trade per stock at a time.

ENV (all optional)
  PAPER_ENABLED=true   PAPER_RISK_RS=2000   PAPER_MAX_NOTIONAL=500000   PAPER_COST_PCT=0.05   PAPER_SQUAREOFF=15:10
Data lives in results/paper_open.json and results/paper_trades.csv (wiped by a redeploy - download the CSV first).
"""

import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

log = logging.getLogger("spurt_paper")
IST = ZoneInfo("Asia/Kolkata")


def _f(name, default):
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return float(default)


ENABLED = os.environ.get("PAPER_ENABLED", "true").strip().lower() == "true"
RISK_RS = _f("PAPER_RISK_RS", 2000)
MAX_NOTIONAL = _f("PAPER_MAX_NOTIONAL", 500000)
COST_PCT = _f("PAPER_COST_PCT", 0.05)
SQ = os.environ.get("PAPER_SQUAREOFF", "15:10").strip()
try:
    SQ_MIN = int(SQ.split(":")[0]) * 60 + int(SQ.split(":")[1])
except Exception:
    SQ_MIN = 15 * 60 + 10

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
OPEN_PATH = os.path.join(RESULTS_DIR, "paper_open.json")
CSV_PATH = os.path.join(RESULTS_DIR, "paper_trades.csv")
COLS = ["date", "symbol", "signal_candle", "entry_time", "entry", "sl", "t1", "t2", "qty", "spurt_mult",
        "exit_reason", "t1_hit", "exit_time", "exit_price", "gross_pnl", "cost", "net_pnl", "r_multiple"]


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _load_open():
    try:
        if os.path.exists(OPEN_PATH):
            with open(OPEN_PATH) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Paper: could not read open trades ({e})")
    return []


def _save_open(rows):
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = OPEN_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rows, f)
        os.replace(tmp, OPEN_PATH)
    except Exception as e:
        log.warning(f"Paper: could not save open trades ({e})")


def _append_closed(row):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    new_file = not os.path.exists(CSV_PATH)
    pd.DataFrame([row], columns=COLS).to_csv(CSV_PATH, mode="a", header=new_file, index=False)


# ---------------------------------------------------------------- open
def open_trades(signals, now_ist):
    """Called by spurt_sell_live.process() with the NEW signals of a cycle. Never raises."""
    try:
        if not ENABLED or not signals:
            return
        today = now_ist.strftime("%Y-%m-%d")
        rows = _load_open()
        have = {r["symbol"] for r in rows}
        done_today = set()
        if os.path.exists(CSV_PATH):
            try:
                d = pd.read_csv(CSV_PATH, usecols=["date", "symbol", "signal_candle"], dtype=str)
                done_today = {(a, b) for a, b in d[d["date"] == today][["symbol", "signal_candle"]].itertuples(index=False)}
            except Exception:
                pass
        added = 0
        for s in signals:
            sym = s["symbol"]
            if sym in have or (sym, s["candle"]) in done_today:
                continue
            entry, sl = float(s["entry"]), float(s["sl"])
            risk = sl - entry
            if risk <= 0 or entry <= 0:
                continue
            qty = int(RISK_RS // risk)
            if MAX_NOTIONAL > 0:
                qty = min(qty, int(MAX_NOTIONAL // entry))
            qty = max(qty, 1)
            h, m = [int(x) for x in s["candle"].split(":")]
            close_min = h * 60 + m + 10                      # the signal candle is labelled by its start; it closes 10 min later
            rows.append({
                "date": today, "symbol": sym, "signal_candle": s["candle"], "entry_time": f"{close_min // 60:02d}:{close_min % 60:02d}",
                "entry_min": close_min, "entry": entry, "sl": sl, "sl_cur": sl, "t1": float(s["t1"]), "t2": float(s["t2"]),
                "qty": qty, "q_left": qty, "t1_hit": False, "realized": 0.0, "spurt_mult": s.get("spurt_mult"),
                "last_bar_min": close_min - 1, "last_price": entry,
            })
            have.add(sym)
            added += 1
        if added:
            _save_open(rows)
            log.info(f"Paper: opened {added} paper trade(s)")
    except Exception as e:
        log.warning(f"Paper open failed: {e}")


# ---------------------------------------------------------------- follow
def _close(t, reason, exit_price, exit_time, now_gross_extra=0.0):
    """t = open-trade dict with 'realized' = gross pnl already booked. Returns the closed CSV row."""
    qty, entry = t["qty"], t["entry"]
    left = t["q_left"]
    gross = t["realized"] + (entry - exit_price) * left
    avg_exit = entry - gross / qty if qty else exit_price
    turnover = entry * qty + avg_exit * qty
    cost = turnover * COST_PCT / 100.0
    net = gross - cost
    risk_total = (t["sl"] - entry) * qty
    return {
        "date": t["date"], "symbol": t["symbol"], "signal_candle": t["signal_candle"], "entry_time": t["entry_time"],
        "entry": round(entry, 2), "sl": round(t["sl"], 2), "t1": round(t["t1"], 2), "t2": round(t["t2"], 2), "qty": qty,
        "spurt_mult": t.get("spurt_mult"), "exit_reason": reason, "t1_hit": "yes" if t["t1_hit"] else "no",
        "exit_time": exit_time, "exit_price": round(avg_exit, 2), "gross_pnl": round(gross, 2), "cost": round(cost, 2),
        "net_pnl": round(net, 2), "r_multiple": round(net / risk_total, 2) if risk_total else None,
    }


def advance(t, bars):
    """Walk one open trade through 5-minute bars [(start_min, open, high, low, close), ...] in time order.
    Returns the closed-trade row if it finished, else None (t is updated in place)."""
    for (mn, o, h, l, c) in bars:
        if mn <= t["last_bar_min"]:
            continue
        hhmm = f"{(mn + 5) // 60:02d}:{(mn + 5) % 60:02d}"
        if mn >= SQ_MIN:
            break
        t["last_bar_min"] = mn
        t["last_price"] = c
        if not t["t1_hit"]:
            if h >= t["sl_cur"]:
                return _close(t, "STOP LOSS", max(t["sl_cur"], o), hhmm)
            if l <= t["t1"]:
                q1 = t["qty"] // 2
                fill = min(t["t1"], o)
                t["realized"] += (t["entry"] - fill) * q1
                t["q_left"] -= q1
                t["t1_hit"] = True
                t["sl_cur"] = t["entry"]
                if l <= t["t2"]:
                    return _close(t, "TARGET 2", min(t["t2"], o), hhmm)
        else:
            if h >= t["sl_cur"]:
                return _close(t, "BACK TO ENTRY (after T1)", max(t["sl_cur"], o), hhmm)
            if l <= t["t2"]:
                return _close(t, "TARGET 2", min(t["t2"], o), hhmm)
    return None


def update(now_ist, fetch5):
    """Called once per volume-spurt cycle. fetch5(symbol) -> DataFrame of CLOSED 5-minute candles (index UTC-naive, columns
    open/high/low/close) or None. Moves every open paper trade forward and closes the finished ones. Never raises."""
    try:
        if not ENABLED:
            return
        rows = _load_open()
        if not rows:
            return
        today = now_ist.strftime("%Y-%m-%d")
        keep = []
        mins_now = now_ist.hour * 60 + now_ist.minute
        for t in rows:
            try:
                if t["date"] != today:                       # left over from an earlier day (e.g. service was down at 15:10)
                    _append_closed(_close(t, "EOD (carried over)", t.get("last_price", t["entry"]), "15:10"))
                    continue
                df = fetch5(t["symbol"])
                bars = []
                if df is not None and len(df):
                    for ts, r in df.iterrows():
                        ti = _ist(ts)
                        if ti.strftime("%Y-%m-%d") != today:
                            continue
                        bars.append((ti.hour * 60 + ti.minute, float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])))
                    bars.sort()
                res = advance(t, bars)
                if res is None and mins_now >= SQ_MIN:
                    res = _close(t, "SQUARE-OFF 15:10", t["last_price"], SQ)
                if res is not None:
                    _append_closed(res)
                    log.info(f"Paper: {t['symbol']} closed {res['exit_reason']} net {res['net_pnl']}")
                else:
                    keep.append(t)
            except Exception as e:
                log.warning(f"Paper: could not update {t.get('symbol')}: {e}")
                keep.append(t)
        _save_open(keep)
    except Exception as e:
        log.warning(f"Paper update failed: {e}")


# ---------------------------------------------------------------- page
_SORT_JS = r"""<script>
document.querySelectorAll('table.sp').forEach(function(t){Array.prototype.forEach.call(t.rows[0].cells,function(th){
th.style.cursor='pointer';th.title='click to sort (high to low / low to high)';
th.addEventListener('click',function(){var i=th.cellIndex,b=t.tBodies[0],rows=Array.prototype.slice.call(t.rows,1),
asc=th.dataset.asc!=='1';th.dataset.asc=asc?'1':'0';
Array.prototype.forEach.call(t.rows[0].cells,function(c){c.textContent=c.textContent.replace(/ [▲▼]$/,'');});
th.textContent+=asc?' ▲':' ▼';
function val(r){var x=r.cells[i].innerText.trim();var m=x.match(/^(\d\d):(\d\d)$/);if(m)return parseInt(m[1])*60+parseInt(m[2]);
var n=parseFloat(x.replace(/[,%x+₹]/g,''));return isNaN(n)?x.toLowerCase():n;}
rows.sort(function(a,c){var p=val(a),q=val(c);if(p<q)return asc?-1:1;if(p>q)return asc?1:-1;return 0;});
rows.forEach(function(r){b.appendChild(r);});});});});
</script>"""


def _rs(v):
    c = "#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"
    return f'<span style="color:{c}">{"-" if v < 0 else "+" if v > 0 else ""}₹{abs(v):,.0f}</span>'


def page_html():
    import html as _h
    try:
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<meta http-equiv="refresh" content="120"><title>Spurt paper trading</title>'
                '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;background:#fff;margin-bottom:22px}'
                'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}'
                'td:nth-child(1),td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}'
                '.k{display:flex;flex-wrap:wrap;gap:10px;margin:10px 0 18px}.k div{background:#fff;border:1px solid #ddd;border-radius:8px;padding:10px 14px;min-width:120px}'
                '.k b{display:block;font-size:20px}.k small{color:#666}</style></head><body>')
        rule = (f"Virtual trades on every Spurt SELL signal: SELL at the red candle's close, risk ₹{RISK_RS:,.0f} per trade (size = risk / stop distance, "
                f"max ₹{MAX_NOTIONAL:,.0f} of stock), stop = red candle high, half out at T1 (1R) with the stop moved to entry, rest at T2 (2R) or back at entry, "
                f"everything closed at {SQ}. Stop is counted first if a stop and a target fall in the same 5-min candle. Costs {COST_PCT:g}% of traded value deducted. "
                "No real orders are ever placed.")
        title = '<h2>Spurt SELL &mdash; paper trading</h2>'
        links = ('<div class="m"><a href="/spurt_paper.csv">download trades CSV</a> &middot; <a href="/spurt_sell">Spurt SELL signals</a> &middot; '
                 '<a href="/spurt10">live /spurt10</a> &middot; auto-refresh every 2 min &middot; click any heading to sort</div>')
        op = _load_open()
        orows = ""
        for t in op:
            unreal = (t["entry"] - t.get("last_price", t["entry"])) * t["q_left"] + t["realized"]
            orows += (f"<tr><td>{t['date']}</td><td><b>{_h.escape(t['symbol'])}</b></td><td>{t['entry_time']}</td><td>{t['entry']:.2f}</td>"
                      f"<td style='color:#c62828'>{t['sl_cur']:.2f}</td><td>{t['t1']:.2f}</td><td>{t['t2']:.2f}</td><td>{t['qty']}</td>"
                      f"<td>{'T1 done' if t['t1_hit'] else 'waiting'}</td><td>{t.get('last_price', t['entry']):.2f}</td><td>{_rs(unreal)}</td></tr>")
        open_tbl = ""
        if orows:
            open_tbl = ('<h3>Open paper trades</h3><table class="sp"><tr><th>Day</th><th>Symbol</th><th>Entry time</th><th>Entry</th><th>Stop now</th><th>T1</th><th>T2</th>'
                        f'<th>Qty</th><th>Status</th><th>Last price</th><th>Open P&amp;L</th></tr>{orows}</table>')
        if not os.path.exists(CSV_PATH):
            return head + title + f'<div class="m">{rule}</div>{links}{open_tbl}<p>No closed paper trade yet. They appear after a Spurt SELL signal has hit its stop, a target, or the {SQ} square-off.</p>{_SORT_JS}</body></html>'
        df = pd.read_csv(CSV_PATH)
        if df.empty:
            return head + title + f'<div class="m">{rule}</div>{links}{open_tbl}<p>No closed paper trade yet.</p>{_SORT_JS}</body></html>'
        n = len(df)
        wins = int((df["net_pnl"] > 0).sum())
        tot = float(df["net_pnl"].sum())
        gp = float(df.loc[df["net_pnl"] > 0, "net_pnl"].sum())
        gl = float(-df.loc[df["net_pnl"] < 0, "net_pnl"].sum())
        pf = f"{gp / gl:.2f}" if gl > 0 else "-"
        avg_r = float(df["r_multiple"].mean())
        eq = df["net_pnl"].cumsum()
        dd = float((eq - eq.cummax()).min())
        kp = (f'<div class="k"><div><small>Closed trades</small><b>{n}</b></div><div><small>Win rate</small><b>{wins / n * 100:.0f}%</b></div>'
              f'<div><small>Net P&amp;L</small><b>{_rs(tot)}</b></div><div><small>Average R per trade</small><b>{avg_r:+.2f}R</b></div>'
              f'<div><small>Profit factor</small><b>{pf}</b></div><div><small>Worst drawdown</small><b>{_rs(dd)}</b></div>'
              f'<div><small>Best / worst trade</small><b>{_rs(float(df.net_pnl.max()))} / {_rs(float(df.net_pnl.min()))}</b></div></div>')
        byday = df.groupby("date").agg(trades=("symbol", "count"), wins=("net_pnl", lambda v: int((v > 0).sum())), net=("net_pnl", "sum"),
                                       r=("r_multiple", "sum")).sort_index(ascending=False)
        drows = "".join(f"<tr><td>{d}</td><td>{int(r.trades)}</td><td>{int(r.wins)}</td><td>{_rs(float(r.net))}</td><td>{r.r:+.2f}R</td></tr>" for d, r in byday.iterrows())
        day_tbl = f'<h3>By day</h3><table class="sp"><tr><th>Day</th><th>Trades</th><th>Winners</th><th>Net P&amp;L</th><th>Total R</th></tr>{drows}</table>'
        df = df.sort_values(["date", "exit_time"], ascending=[False, False])
        trows = "".join(
            f"<tr><td>{r.date}</td><td><b>{_h.escape(str(r.symbol))}</b></td><td>{r.entry_time}</td><td>{r.entry:.2f}</td><td>{r.sl:.2f}</td><td>{r.qty}</td>"
            f"<td>{r.t1_hit}</td><td>{_h.escape(str(r.exit_reason))}</td><td>{r.exit_time}</td><td>{r.exit_price:.2f}</td>"
            f"<td>{_rs(float(r.net_pnl))}</td><td>{float(r.r_multiple):+.2f}R</td><td>{float(r.spurt_mult):g}x</td></tr>" for r in df.itertuples())
        trade_tbl = ('<h3>Every closed trade (newest first)</h3><table class="sp"><tr><th>Day</th><th>Symbol</th><th>Entry time</th><th>Entry</th><th>Stop</th><th>Qty</th>'
                     '<th>T1 hit</th><th>Exit reason</th><th>Exit time</th><th>Avg exit</th><th>Net P&amp;L</th><th>R</th><th>Spurt</th></tr>' + trows + '</table>')
        return head + title + f'<div class="m">{rule}</div>{links}{kp}{open_tbl}{day_tbl}{trade_tbl}{_SORT_JS}</body></html>'
    except Exception as e:
        return f"<p>Paper trading page error: {_h.escape(str(e))}</p>"
