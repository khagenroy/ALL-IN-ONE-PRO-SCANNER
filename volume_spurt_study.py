"""
VOLUME-SPURT STRATEGY BACKTEST  (read-only research - places no orders, touches no signal logic)

IDEA
    A "volume spurt" = one 5-minute candle whose volume is far above NORMAL FOR
    THAT TIME OF DAY (the same 5-minute slot averaged over the previous 10
    trading days - this removes the usual heavy-open / quiet-midday pattern),
    on a candle that actually moved. The question is whether trading it pays
    after a realistic stop-loss, targets and costs, and under which rules.

WHAT IT TESTS (every stock in the universe, every trading day in the window)
    Signal bar  : volume >= M x its same-time-of-day 10-day average
                  AND |close-open| / open >= MIN_MOVE
                  AND the bar starts between 09:30 and 14:25 IST
    Direction   : FOLLOW  = trade in the direction of the spurt candle
                  FADE    = trade against it
    Entry       : OPEN of the NEXT 5-minute bar (no look-ahead)
    Stop-loss   : the signal candle's far extreme (FOLLOW long: its low; FOLLOW
                  short: its high; FADE short: its high; FADE long: its low).
                  Trades whose risk is under MIN_RISK_PCT or over MAX_RISK_PCT
                  of the entry price are skipped.
    Targets     : 1R, 2R, 3R (R = entry-to-stop distance), each tested alone,
                  full position out at the target.
    Exit        : target, stop, or the last bar of the day (15:25 bar close).
                  If a bar touches both stop and target the STOP is assumed
                  hit first (pessimistic).
    Costs       : COST_PCT of the price per round trip (default 0.05%), taken
                  off in R.
    One trade per stock per day per rule (the first qualifying signal).

    The grid: mode (follow/fade) x M (3,4,5,6,8,10) x MIN_MOVE (0.3,0.6,1.0%)
    x target (1R,2R,3R).  For every cell: trades, win rate, average net R
    (expectancy), profit factor, total R, and the average R in the first half
    vs the second half of the period (a rule that only works in one half is
    probably luck).

HONEST LIMITS
    ~40 trading days is one market regime, not proof. The universe is
    today's market-cap list. Fills are assumed at the next bar's open with
    only the flat cost - no market impact, no missed fills. Use it to find
    rules worth watching, then watch them live before trusting them.

OUTPUT (results/)
    volspurt.html         page (served at /volspurt)
    volspurt.json         stats
    volspurt_grid.csv     every rule x target with its stats
    volspurt_signals.csv  every signal with the R it would have made (slice it in Excel)

Run by hand:  python volume_spurt_study.py
              TEST_SYMBOL_LIMIT=20 python volume_spurt_study.py   (quick dry run)
"""

import os
import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_scanner as ls  # noqa: E402  (reuses its rate-limited Dhan fetch, universe and results folder)

log = logging.getLogger("volume_spurt_study")

HISTORY_DAYS = int(os.environ.get("VOLSPURT_HISTORY_DAYS", "60") or "60")   # calendar days of 5-min data
COST_PCT = float(os.environ.get("VOLSPURT_COST_PCT", "0.05") or "0.05")      # round-trip cost, % of price
MIN_PRICE = float(os.environ.get("VOLSPURT_MIN_PRICE", "20") or "20")
MIN_BASE_TURNOVER_CR = float(os.environ.get("VOLSPURT_MIN_BAR_TURNOVER_CR", "0.02") or "0.02")  # normal 5-min turnover floor
MIN_RISK_PCT = 0.2
MAX_RISK_PCT = 3.0
MIN_TRADES_TO_RANK = int(os.environ.get("VOLSPURT_MIN_TRADES", "150") or "150")

RVOL_LOOKBACK_DAYS = 10
RVOL_MIN_PRIOR_DAYS = 5
MIN_M = 3.0                       # lowest volume multiple stored; the grid filters upward from here
MIN_MOVE_STORE = 0.3              # lowest candle move (%) stored
GRID_M = (3.0, 4.0, 5.0, 6.0, 8.0, 10.0)
GRID_MOVE = (0.3, 0.6, 1.0)
GRID_K = (1, 2, 3)
MODES = ("follow", "fade")

IST_OFFSET_MIN = 330
SESSION_START_MIN = 9 * 60 + 15 - IST_OFFSET_MIN      # UTC minute-of-day of the 09:15 IST bar
SESSION_END_MIN = 15 * 60 + 30 - IST_OFFSET_MIN
N_SLOTS = (SESSION_END_MIN - SESSION_START_MIN) // 5   # 75
ENTRY_FIRST_START_MIN = 9 * 60 + 30 - IST_OFFSET_MIN   # signal bars start 09:30 IST ...
ENTRY_LAST_START_MIN = 14 * 60 + 25 - IST_OFFSET_MIN   # ... through the 14:25 IST bar
BUCKETS = (("09:30-11:00", 9 * 60 + 30, 11 * 60), ("11:00-13:30", 11 * 60, 13 * 60 + 30), ("13:30-14:30", 13 * 60 + 30, 14 * 60 + 30))


def _bucket_of(start_ist_min: int) -> str:
    for name, a, b in BUCKETS:
        if a <= start_ist_min < b:
            return name
    return "other"


# ---------------------------------------------------------------------------
# Trade simulation
# ---------------------------------------------------------------------------

def simulate_trade(is_long: bool, entry: float, stop: float, H: np.ndarray, L: np.ndarray, last_close: float, k: int):
    """Gross R of one trade entered at `entry` with stop `stop` and a target of
    k x R, walking the bars H/L (starting with the entry bar itself). Stop wins
    ties. Returns None if the stop is not on the loss side of the entry."""
    risk = (entry - stop) if is_long else (stop - entry)
    if risk <= 0:
        return None
    if is_long:
        target = entry + k * risk
        stop_hit = L <= stop
        tgt_hit = H >= target
    else:
        target = entry - k * risk
        stop_hit = H >= stop
        tgt_hit = L <= target
    inf = len(H) + 1
    s_idx = int(np.argmax(stop_hit)) if stop_hit.any() else inf
    t_idx = int(np.argmax(tgt_hit)) if tgt_hit.any() else inf
    if s_idx <= t_idx and s_idx < inf:
        return -1.0
    if t_idx < inf:
        return float(k)
    pnl = (last_close - entry) if is_long else (entry - last_close)
    return pnl / risk


# ---------------------------------------------------------------------------
# Per-symbol: find spurt candles and what each would have made
# ---------------------------------------------------------------------------

def find_signals(sym: str, df5: pd.DataFrame) -> pd.DataFrame:
    """All spurt candles (volume >= MIN_M x same-slot 10-day average, move >=
    MIN_MOVE_STORE%) for this symbol with the net R each would have produced
    in every mode / target. df5: UTC-naive DatetimeIndex, 5-minute OHLCV."""
    if df5 is None or df5.empty:
        return pd.DataFrame()
    mins_all = (df5.index.hour * 60 + df5.index.minute).to_numpy()
    df5 = df5[(mins_all >= SESSION_START_MIN) & (mins_all < SESSION_END_MIN)]
    if df5.empty:
        return pd.DataFrame()
    dates = np.array(list(df5.index.date))
    day_list = sorted(set(dates))
    if len(day_list) < RVOL_MIN_PRIOR_DAYS + 2:
        return pd.DataFrame()

    frames, vol_mat = {}, np.full((len(day_list), N_SLOTS), np.nan)
    for di, d in enumerate(day_list):
        f = df5[dates == d].sort_index()
        frames[d] = f
        slots = ((f.index.hour * 60 + f.index.minute).to_numpy() - SESSION_START_MIN) // 5
        vol_mat[di, slots] = f["volume"].to_numpy()

    out_rows = []
    cost = COST_PCT / 100.0
    for di in range(RVOL_MIN_PRIOR_DAYS, len(day_list)):
        d = day_list[di]
        f = frames[d]
        if len(f) < 20:
            continue
        window = vol_mat[max(0, di - RVOL_LOOKBACK_DAYS):di]
        cnt = np.sum(np.isfinite(window) & (window > 0), axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            base = np.where(cnt >= RVOL_MIN_PRIOR_DAYS, np.nanmean(np.where(window > 0, window, np.nan), axis=0), np.nan)

        idx = f.index
        m = (idx.hour * 60 + idx.minute).to_numpy()
        slots = (m - SESSION_START_MIN) // 5
        o = f["open"].to_numpy(); h = f["high"].to_numpy(); l = f["low"].to_numpy()
        c = f["close"].to_numpy(); v = f["volume"].to_numpy()
        n = len(f)
        last_close = float(c[-1])
        typ = (h + l + c) / 3.0
        cum_v = np.cumsum(v)
        vwap = np.where(cum_v > 0, np.cumsum(typ * v) / np.where(cum_v > 0, cum_v, 1), typ)
        prev_close = float(frames[day_list[di - 1]]["close"].iloc[-1])

        with np.errstate(invalid="ignore", divide="ignore"):
            rvol = v / base[slots]
            move = np.abs(c - o) / np.where(o > 0, o, np.nan) * 100.0
        base_turnover_cr = base[slots] * c / 1e7
        ok = (np.isfinite(rvol) & (rvol >= MIN_M) & np.isfinite(move) & (move >= MIN_MOVE_STORE)
              & (m >= ENTRY_FIRST_START_MIN) & (m <= ENTRY_LAST_START_MIN)
              & (c >= MIN_PRICE) & (base_turnover_cr >= MIN_BASE_TURNOVER_CR))
        for i in np.where(ok)[0]:
            if i + 1 >= n:
                continue
            up = bool(c[i] > o[i])
            entry = float(o[i + 1])
            if entry <= 0:
                continue
            Hf, Lf = h[i + 1:], l[i + 1:]
            row = {
                "date": str(d), "symbol": sym, "time_ist": f"{(m[i] + IST_OFFSET_MIN) // 60:02d}:{(m[i] + IST_OFFSET_MIN) % 60:02d}",
                "bucket": _bucket_of(int(m[i] + IST_OFFSET_MIN)), "rvol": round(float(rvol[i]), 2),
                "move_pct": round(float(move[i]), 2), "spurt": "UP" if up else "DOWN",
                "vwap_aligned": bool((c[i] > vwap[i]) == up),
                "day_pct_at_signal": round((float(c[i]) / prev_close - 1) * 100, 2),
                "entry": round(entry, 2),
            }
            for mode in MODES:
                is_long = up if mode == "follow" else (not up)
                stop = float(l[i]) if is_long else float(h[i])
                risk = (entry - stop) if is_long else (stop - entry)
                risk_pct = risk / entry * 100 if risk > 0 else None
                valid = risk_pct is not None and MIN_RISK_PCT <= risk_pct <= MAX_RISK_PCT
                row[f"{mode}_side"] = "LONG" if is_long else "SHORT"
                row[f"{mode}_risk_pct"] = None if risk_pct is None else round(risk_pct, 2)
                for k in GRID_K:
                    if not valid:
                        row[f"{mode}_R{k}"] = np.nan
                        continue
                    g = simulate_trade(is_long, entry, stop, Hf, Lf, last_close, k)
                    row[f"{mode}_R{k}"] = np.nan if g is None else round(g - (entry * cost) / risk, 3)
            out_rows.append(row)
    return pd.DataFrame(out_rows)


# ---------------------------------------------------------------------------
# Grid statistics
# ---------------------------------------------------------------------------

def _stats(r: pd.Series) -> dict:
    r = r.dropna()
    n = len(r)
    if n == 0:
        return {"trades": 0}
    pos = r[r > 0].sum(); neg = -r[r < 0].sum()
    return {"trades": int(n), "win_pct": round(float((r > 0).mean() * 100), 1), "avg_R": round(float(r.mean()), 3),
            "profit_factor": None if neg == 0 else round(float(pos / neg), 2), "total_R": round(float(r.sum()), 1)}


def select_trades(sig: pd.DataFrame, mode: str, m: float, mv: float, k: int) -> pd.DataFrame:
    """The trades one rule would take: spurt strength filters, valid risk, and
    only the FIRST such signal per stock per day."""
    col = f"{mode}_R{k}"
    s = sig[(sig["rvol"] >= m) & (sig["move_pct"] >= mv) & sig[col].notna()]
    if s.empty:
        return s
    return s.sort_values(["date", "symbol", "time_ist"]).groupby(["date", "symbol"], as_index=False).first()


def build_grid(sig: pd.DataFrame) -> pd.DataFrame:
    if sig.empty:
        return pd.DataFrame()
    days = sorted(sig["date"].unique())
    half = days[len(days) // 2] if days else None
    rows = []
    for mode in MODES:
        for m in GRID_M:
            for mv in GRID_MOVE:
                for k in GRID_K:
                    t = select_trades(sig, mode, m, mv, k)
                    if t.empty:
                        continue
                    col = f"{mode}_R{k}"
                    st = _stats(t[col])
                    a = t[t["date"] < half][col]; b = t[t["date"] >= half][col]
                    rows.append({
                        "mode": mode, "min_volume_x": m, "min_move_pct": mv, "target_R": k, **st,
                        "trades_per_day": round(st["trades"] / max(1, t["date"].nunique()), 1),
                        "avg_R_first_half": None if a.empty else round(float(a.mean()), 3),
                        "avg_R_second_half": None if b.empty else round(float(b.mean()), 3),
                    })
    return pd.DataFrame(rows)


def breakdown(sig: pd.DataFrame, mode: str, m: float, mv: float, k: int) -> list:
    """Stats of one rule split by time-of-day bucket and VWAP alignment."""
    t = select_trades(sig, mode, m, mv, k)
    col = f"{mode}_R{k}"
    out = []
    for name, _, _ in BUCKETS:
        g = t[t["bucket"] == name]
        if len(g):
            out.append({"slice": f"time {name}", **_stats(g[col])})
    for flag, label in ((True, "with VWAP side"), (False, "against VWAP side")):
        g = t[t["vwap_aligned"] == flag]
        if len(g):
            out.append({"slice": label, **_stats(g[col])})
    for spurt in ("UP", "DOWN"):
        g = t[t["spurt"] == spurt]
        if len(g):
            out.append({"slice": f"{spurt.lower()} spurt candle", **_stats(g[col])})
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _work(sym, security_id, segment):
    try:
        df5 = ls.fetch_intraday_history(security_id, segment, 5, HISTORY_DAYS)
        return {"sig": find_signals(sym, df5)}
    except Exception as e:
        return {"error": str(e)}


def run_study():
    if not ls.DHAN_CLIENT_ID or not ls.DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")
    symbols = ls._load_universe_symbols()
    log.info(f"[volspurt] backtesting {len(symbols)} symbols, {HISTORY_DAYS} calendar days of 5-min data...")
    jobs, errors = [], []
    for sym in symbols:
        sid, seg = ls.get_security_id_and_segment(sym)
        if not sid or seg != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        jobs.append((sym, sid, seg))

    parts, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=max(1, ls.SCAN_WORKERS)) as ex:
        futures = [(sym, ex.submit(_work, sym, sid, seg)) for sym, sid, seg in jobs]
        for n, (sym, fut) in enumerate(futures, 1):
            res = fut.result()
            if "error" in res:
                errors.append({"symbol": sym, "error": res["error"]})
                log.warning(f"{sym}: {res['error']}")
            elif not res["sig"].empty:
                parts.append(res["sig"])
            if n % 100 == 0:
                log.info(f"[volspurt] ...{n}/{len(futures)} done, {sum(len(p) for p in parts)} signals so far, "
                         f"{time.time() - t0:.0f}s elapsed")
    sig = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    grid = build_grid(sig)
    elapsed = time.time() - t0
    log.info(f"[volspurt] Done: {len(sig)} spurt signals, {len(grid)} rule cells, {len(errors)} errors, {elapsed:.0f}s")
    write_results(sig, grid, errors, len(symbols), elapsed)
    return sig, grid


def _ranked(grid: pd.DataFrame) -> pd.DataFrame:
    if grid.empty:
        return grid
    g = grid[grid["trades"] >= MIN_TRADES_TO_RANK]
    return g.sort_values("avg_R", ascending=False)


def write_results(sig, grid, errors, universe_size, elapsed):
    os.makedirs(ls.RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(ls.IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    ranked = _ranked(grid)
    top_breakdowns = []
    for _, r in ranked.head(3).iterrows():
        top_breakdowns.append({"rule": r.to_dict(), "slices": breakdown(sig, r["mode"], r["min_volume_x"], r["min_move_pct"], int(r["target_R"]))})
    payload = {
        "run_timestamp": run_ts, "universe_size": universe_size, "duration_sec": round(elapsed),
        "days": int(sig["date"].nunique()) if not sig.empty else 0,
        "first_day": None if sig.empty else str(sig["date"].min()), "last_day": None if sig.empty else str(sig["date"].max()),
        "signals": int(len(sig)), "error_count": len(errors), "errors": errors[:200],
        "settings": {"history_days": HISTORY_DAYS, "cost_pct": COST_PCT, "min_price": MIN_PRICE,
                     "min_trades_to_rank": MIN_TRADES_TO_RANK, "risk_pct_range": [MIN_RISK_PCT, MAX_RISK_PCT]},
        "top_breakdowns": top_breakdowns,
    }
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "volspurt.json"), json.dumps(payload, indent=2, default=str))
    ls._atomic_csv(grid if not grid.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "volspurt_grid.csv"))
    ls._atomic_csv(sig if not sig.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "volspurt_signals.csv"))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "volspurt.html"), render_html(payload, ranked, grid))
    log.info(f"[volspurt] Results written to {ls.RESULTS_DIR}/ (volspurt.html, .json, volspurt_grid.csv, volspurt_signals.csv)")


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def _f(x, nd=2, suffix=""):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{x:.{nd}f}{suffix}"


def _rcls(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return "pos" if x > 0 else "neg"


def render_html(payload: dict, ranked: pd.DataFrame, grid: pd.DataFrame) -> str:
    s = payload["settings"]

    def grid_rows(df, limit):
        trs = ""
        for _, r in df.head(limit).iterrows():
            stable = (r["avg_R_first_half"] is not None and r["avg_R_second_half"] is not None
                      and not pd.isna(r["avg_R_first_half"]) and not pd.isna(r["avg_R_second_half"])
                      and r["avg_R_first_half"] > 0 and r["avg_R_second_half"] > 0)
            trs += (f"<tr><td class='sym'>{r['mode'].upper()}</td><td>&ge; {r['min_volume_x']:g}x</td><td>&ge; {r['min_move_pct']:g}%</td>"
                    f"<td>{int(r['target_R'])}R</td><td>{int(r['trades'])}</td><td>{_f(r['trades_per_day'], 1)}</td>"
                    f"<td>{_f(r['win_pct'], 1, '%')}</td><td class='{_rcls(r['avg_R'])}'>{_f(r['avg_R'], 3)}</td>"
                    f"<td>{_f(r['profit_factor'])}</td><td class='{_rcls(r['total_R'])}'>{_f(r['total_R'], 1)}</td>"
                    f"<td class='{_rcls(r['avg_R_first_half'])}'>{_f(r['avg_R_first_half'], 3)}</td>"
                    f"<td class='{_rcls(r['avg_R_second_half'])}'>{_f(r['avg_R_second_half'], 3)}</td>"
                    f"<td>{'yes' if stable else 'no'}</td></tr>")
        return trs

    head = ("<thead><tr><th>Mode</th><th>Volume</th><th>Candle move</th><th>Target</th><th>Trades</th><th>Per day</th>"
            "<th>Win rate</th><th>Avg net R</th><th>Profit factor</th><th>Total R</th><th>Avg R 1st half</th><th>Avg R 2nd half</th>"
            "<th>Positive in both halves</th></tr></thead>")

    if ranked.empty:
        ranked_html = "<div class='empty'>No rule had enough trades yet.</div>"
        worst_html = ""
    else:
        ranked_html = f"<table>{head}<tbody>{grid_rows(ranked, 30)}</tbody></table>"
        worst = ranked.sort_values("avg_R", ascending=True)
        worst_html = f"<h2>Weakest rules (for contrast)</h2><table>{head}<tbody>{grid_rows(worst, 8)}</tbody></table>"

    bd_html = ""
    for b in payload.get("top_breakdowns", []):
        r = b["rule"]
        trs = "".join(f"<tr><td class='sym'>{x['slice']}</td><td>{x.get('trades', 0)}</td><td>{_f(x.get('win_pct'), 1, '%')}</td>"
                      f"<td class='{_rcls(x.get('avg_R'))}'>{_f(x.get('avg_R'), 3)}</td><td>{_f(x.get('profit_factor'))}</td>"
                      f"<td class='{_rcls(x.get('total_R'))}'>{_f(x.get('total_R'), 1)}</td></tr>" for x in b["slices"])
        bd_html += (f"<h2>Inside the rule: {r['mode'].upper()}, volume &ge; {r['min_volume_x']:g}x, move &ge; {r['min_move_pct']:g}%, "
                    f"{int(r['target_R'])}R target</h2><table><thead><tr><th>Slice</th><th>Trades</th><th>Win rate</th><th>Avg net R</th>"
                    f"<th>Profit factor</th><th>Total R</th></tr></thead><tbody>{trs}</tbody></table>")

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Volume spurt - backtest</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  h2 {{ font-size: 16px; margin: 28px 0 8px; }}
  .nav {{ margin-bottom: 16px; font-size: 14px; }}
  .nav a {{ color: #58a6ff; text-decoration: none; margin-right: 18px; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 12px; line-height: 1.55; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 20px; }}
  th, td {{ padding: 7px 9px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; }}
  .sym {{ font-weight: 600; }}
  .pos {{ color: #3fb950; }} .neg {{ color: #f85149; }}
  .empty {{ color: #9aa0a6; padding: 20px 0; }}
  tr:hover {{ background: #161b22; }}
</style>
</head>
<body>
  <div class="nav">
    <a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/gainers">Morning gainers</a>
    <a href="/volspurt/grid.csv">Grid CSV</a><a href="/volspurt/signals.csv">All signals CSV</a>
  </div>
  <h1>Volume-spurt strategy - backtest</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload['duration_sec']}s) &middot; {payload['universe_size']} stocks &middot;
    {payload['days']} trading days ({payload['first_day']} to {payload['last_day']}) &middot; {payload['signals']} spurt candles &middot; {payload['error_count']} errors<br>
    <b>Signal:</b> a 5-minute candle (starting 09:30-14:25 IST) with volume &ge; the stated multiple of that stock's average volume
    for the same time of day over the previous 10 days, and a body move &ge; the stated %.
    <b>FOLLOW</b> trades the candle's direction, <b>FADE</b> trades against it. Entry: next candle's open. SL: the signal candle's far end
    (skipped if risk is outside {s['risk_pct_range'][0]}-{s['risk_pct_range'][1]}% of price). Target: 1R / 2R / 3R. Exit: target, SL, or 15:25 close; stop wins ties.
    Cost: {s['cost_pct']}% per round trip, taken off in R. One trade per stock per day per rule.<br>
    <b>Avg net R</b> = average result per trade in units of the risk taken (positive = profitable after costs).
    Rules with at least {s['min_trades_to_rank']} trades are ranked. <b>Positive in both halves</b> = average R is above zero in both the first and the second half of the period.
    About {payload['days']} days is one market regime - treat the best rules as candidates to watch, not proof.
  </div>
  <h2>Best rules (by average net R)</h2>
  {ranked_html}
  {bd_html}
  {worst_html}
</body>
</html>"""


if __name__ == "__main__":
    run_study()
