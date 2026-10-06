"""
SPURT-PULLBACK BACKTEST  (read-only research - places no orders, touches no signal logic)

THE IDEA BEING TESTED
    "Something bigger is happening in a stock that shows a volume spurt in the
    first 45 minutes. At 10:00 I pick all such stocks as a watchlist, then trade
    the retracement: a stock that went up pulls back a bit and I buy the pullback;
    a stock that went down bounces a bit and I sell the bounce."

WATCHLIST (built at 10:00 IST, using only bars that have closed by then)
    Any 5-minute bar starting 09:15-09:55 IST with
        volume >= 3x that stock's average for the same time of day over the previous 10 days
        and candle body >= MIN_SPURT_MOVE% (default 0.5%).
    One setup per stock per day: the bar with the highest volume multiple.
    UP spurt candle   -> watch for a pullback to BUY.
    DOWN spurt candle -> watch for a bounce to SELL.
    The IMPULSE is measured from the day's extreme before/at the spurt candle
    (lowest low for UP, highest high for DOWN) to the furthest point reached by
    10:00. It must be at least MIN_IMPULSE_PCT of price (default 1.0%).

ENTRY (from 10:00 until 14:00 IST)
    The pullback level = furthest point reached so far, pulled back by R% of the
    impulse (R = 38.2%, 50%, 61.8%; each tested separately). Entry the first time
    price trades to that level (limit fill at the level, or at the bar's open if
    it gaps through). The "furthest point" keeps updating while price keeps
    running in the spurt direction, so the pullback is always off the latest extreme.
    If price breaks past the impulse start before a fill, the setup is dead.

STOP / TARGETS / EXIT
    Stop      : the impulse start (UP: below the day's low before the spurt; DOWN: above the day's high).
                Setups whose risk is outside 0.3-4% of price are skipped.
    Targets   : 1R, 2R, or "RETEST" = back to the extreme reached before the pullback.
    Exit      : target, stop, or the 15:25 candle's close. Stop wins ties. The entry candle
                is only checked for the stop (never for the target), which is pessimistic.
    Cost      : COST_PCT per round trip (default 0.05%) taken off in R.
    One trade per stock per day per rule.

FILTER TESTED ALONGSIDE
    VWAP side : entry only if the pullback is still on the right side of the day's VWAP
                (buy at/above VWAP, sell at/below VWAP) - i.e. the stock is still "strong".

HONEST LIMITS
    ~40 trading days is one market regime. Fills are assumed at the limit level with
    flat costs - no slippage, no missed fills. Use it to find rules worth watching live.

OUTPUT (results/)
    spurtpb.html         page (served at /spurtpb)
    spurtpb.json         stats
    spurtpb_grid.csv     every rule with its stats
    spurtpb_trades.csv   every simulated trade (slice it in Excel)

Run by hand:  python spurt_pullback_study.py
              TEST_SYMBOL_LIMIT=20 python spurt_pullback_study.py   (quick dry run)
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
import live_scanner as ls  # noqa: E402
import volume_spurt_study as vs  # noqa: E402  (reuses its same-time-of-day volume baseline constants and page helpers)

log = logging.getLogger("spurt_pullback_study")

HISTORY_DAYS = int(os.environ.get("SPURTPB_HISTORY_DAYS", "60") or "60")
COST_PCT = float(os.environ.get("SPURTPB_COST_PCT", "0.05") or "0.05")
MIN_PRICE = float(os.environ.get("SPURTPB_MIN_PRICE", "20") or "20")
MIN_SPURT_MOVE = float(os.environ.get("SPURTPB_MIN_SPURT_MOVE", "0.5") or "0.5")
MIN_IMPULSE_PCT = float(os.environ.get("SPURTPB_MIN_IMPULSE_PCT", "1.0") or "1.0")
MIN_TRADES_TO_RANK = int(os.environ.get("SPURTPB_MIN_TRADES", "150") or "150")
MIN_RISK_PCT, MAX_RISK_PCT = 0.3, 4.0
MIN_CUM_X = 1.0                    # lowest NSE-style multiple stored (volume so far today / avg full-day volume)
MIN_AVG_DAY_TURNOVER_CR = float(os.environ.get("SPURTPB_MIN_AVG_DAY_TURNOVER_CR", "0.5") or "0.5")

MIN_M = 3.0
RETRACES = (0.382, 0.5, 0.618)
GRID_M = {"NSE_CUMVOL": (1.0, 2.0, 3.0, 5.0), "CANDLE": (3.0, 5.0, 8.0)}
WATCH_LABEL = {"NSE_CUMVOL": "NSE-style (volume so far / 1-wk avg day volume)", "CANDLE": "single 5-min candle"}
TARGETS = ("1R", "2R", "RETEST")
SIDES = ("ALL", "LONG", "SHORT")

IST = 330
S0 = vs.SESSION_START_MIN
S1 = vs.SESSION_END_MIN
N_SLOTS = vs.N_SLOTS
WATCH_FIRST_IST = 9 * 60 + 15
WATCH_LAST_BAR_IST = 9 * 60 + 55       # last bar that has closed by 10:00
ENTRY_FROM_IST = 10 * 60
ENTRY_UNTIL_IST = 14 * 60


def _sim(entry, stop, target, Ha, Lb, last_close):
    """Walk bars (all in the 'long frame': Ha = favourable extreme, Lb = adverse
    extreme) from the entry bar on. The entry bar is only checked for the stop.
    Returns gross R."""
    risk = entry - stop
    if risk <= 0:
        return None
    H = Ha.copy()
    H[0] = -np.inf
    stop_hit = Lb <= stop
    tgt_hit = H >= target
    inf = len(H) + 1
    s_idx = int(np.argmax(stop_hit)) if stop_hit.any() else inf
    t_idx = int(np.argmax(tgt_hit)) if tgt_hit.any() else inf
    if s_idx <= t_idx and s_idx < inf:
        return -1.0
    if t_idx < inf:
        return (target - entry) / risk
    return (last_close - entry) / risk


def find_trades(sym: str, df5: pd.DataFrame) -> pd.DataFrame:
    if df5 is None or df5.empty:
        return pd.DataFrame()
    mins_all = (df5.index.hour * 60 + df5.index.minute).to_numpy()
    df5 = df5[(mins_all >= S0) & (mins_all < S1)]
    if df5.empty:
        return pd.DataFrame()
    dates = np.array(list(df5.index.date))
    day_list = sorted(set(dates))
    if len(day_list) < vs.RVOL_MIN_PRIOR_DAYS + 2:
        return pd.DataFrame()

    frames, vol_mat = {}, np.full((len(day_list), N_SLOTS), np.nan)
    for di, d in enumerate(day_list):
        f = df5[dates == d].sort_index()
        frames[d] = f
        slots = ((f.index.hour * 60 + f.index.minute).to_numpy() - S0) // 5
        vol_mat[di, slots] = f["volume"].to_numpy()

    rows = []
    cost = COST_PCT / 100.0
    for di in range(vs.RVOL_MIN_PRIOR_DAYS, len(day_list)):
        d = day_list[di]
        f = frames[d]
        if len(f) < 40:
            continue
        window = vol_mat[max(0, di - vs.RVOL_LOOKBACK_DAYS):di]
        cnt = np.sum(np.isfinite(window) & (window > 0), axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            base = np.where(cnt >= vs.RVOL_MIN_PRIOR_DAYS, np.nanmean(np.where(window > 0, window, np.nan), axis=0), np.nan)
        m = (f.index.hour * 60 + f.index.minute).to_numpy()
        ist = m + IST
        slots = (m - S0) // 5
        o = f["open"].to_numpy(); h = f["high"].to_numpy(); l = f["low"].to_numpy()
        c = f["close"].to_numpy(); v = f["volume"].to_numpy()
        n = len(f)
        with np.errstate(invalid="ignore", divide="ignore"):
            rvol = v / base[slots]
            move = np.abs(c - o) / np.where(o > 0, o, np.nan) * 100.0
        base_turnover_cr = base[slots] * c / 1e7
        post = np.where(ist >= ENTRY_FROM_IST)[0]
        if len(post) == 0:
            continue
        j0 = int(post[0])
        if j0 < 5:
            continue
        typ = (h + l + c) / 3.0
        cv = np.cumsum(v)
        vwap = np.where(cv > 0, np.cumsum(typ * v) / np.where(cv > 0, cv, 1), typ)
        prev_close = float(frames[day_list[di - 1]]["close"].iloc[-1])

        # ---- watchlist definitions -------------------------------------------------
        setups = []   # (watch_mode, multiple, up, anchor_index)
        # (1) NSE-style "volume spurt": volume traded so far today (to 10:00) as a multiple of the
        #     stock's average FULL-DAY volume over the previous 5 sessions ("1 WK AVG. VOLUME").
        prev_tot = np.nansum(np.where(np.isfinite(vol_mat[max(0, di - 5):di]), vol_mat[max(0, di - 5):di], 0.0), axis=1)
        prev_tot = prev_tot[prev_tot > 0]
        if len(prev_tot) >= 3:
            avg_day_vol = float(np.mean(prev_tot))
            px10 = float(c[j0 - 1])
            if avg_day_vol * px10 / 1e7 >= MIN_AVG_DAY_TURNOVER_CR and px10 >= MIN_PRICE:
                cum_x = float(cv[j0 - 1]) / avg_day_vol
                if cum_x >= MIN_CUM_X:
                    up_c = px10 > prev_close
                    setups.append(("NSE_CUMVOL", cum_x, up_c, None))
        # (2) single 5-minute candle spurt vs the same time of day (the earlier test)
        cand = (np.isfinite(rvol) & (rvol >= MIN_M) & np.isfinite(move) & (move >= MIN_SPURT_MOVE)
                & (ist >= WATCH_FIRST_IST) & (ist <= WATCH_LAST_BAR_IST) & (c >= MIN_PRICE)
                & (base_turnover_cr >= vs.MIN_BASE_TURNOVER_CR))
        if cand.any():
            i = int(np.argmax(np.where(cand, rvol, -np.inf)))
            if j0 > i:
                setups.append(("CANDLE", float(rvol[i]), bool(c[i] > o[i]), i))

        for watch_mode, mult, up, anchor in setups:
            a = h if up else -l              # favourable extreme (long frame; a DOWN spurt is negated so the logic reads "buy the pullback")
            b = l if up else -h              # adverse extreme
            op = o if up else -o
            cl = c if up else -c
            vwap_f = vwap if up else -vwap
            if anchor is None:
                lo_i = int(np.argmin(b[:j0])); hi_i = int(np.argmax(a[:j0]))
                if hi_i <= lo_i:
                    continue                  # the move did not run in the stock's direction (extreme made before the opposite extreme)
                L0 = float(b[lo_i]); Hrun = float(a[hi_i])
                spurt_time = "cum to 10:00"
            else:
                L0 = float(np.min(b[: anchor + 1]))      # impulse start (long frame)
                Hrun = float(np.max(a[anchor:j0]))        # furthest point reached by 10:00
                spurt_time = f"{(ist[anchor]) // 60:02d}:{(ist[anchor]) % 60:02d}"
            imp_pct0 = (Hrun - L0) / abs(Hrun) * 100.0
            if imp_pct0 < MIN_IMPULSE_PCT:
                continue
            filled = {r: False for r in RETRACES}
            last_close_f = float(cl[-1])
            for j in range(j0, n):
                if ist[j] > ENTRY_UNTIL_IST:
                    break
                if b[j] < L0:
                    break                                       # impulse start broken: setup dead
                for r in RETRACES:
                    if filled[r]:
                        continue
                    level = Hrun - r * (Hrun - L0)
                    if b[j] <= level:
                        filled[r] = True
                        entry = min(float(op[j]), level)
                        risk = entry - L0
                        if risk <= 0:
                            continue
                        risk_pct = risk / abs(entry) * 100.0
                        if not (MIN_RISK_PCT <= risk_pct <= MAX_RISK_PCT):
                            continue
                        Ha, Lb = a[j:], b[j:]
                        costR = (abs(entry) * cost) / risk
                        res = {}
                        for name, tgt in (("1R", entry + risk), ("2R", entry + 2 * risk), ("RETEST", Hrun)):
                            g = _sim(entry, L0, tgt, Ha, Lb, last_close_f)
                            res[name] = np.nan if g is None else round(g - costR, 3)
                        rows.append({
                            "date": str(d), "symbol": sym, "watch_mode": watch_mode, "side": "LONG" if up else "SHORT",
                            "mult": round(mult, 2), "spurt_time_ist": spurt_time,
                            "impulse_pct_at_10": round(imp_pct0, 2),
                            "retrace": r, "entry_time_ist": f"{(ist[j]) // 60:02d}:{(ist[j]) % 60:02d}",
                            "entry": round(abs(entry), 2), "risk_pct": round(risk_pct, 2),
                            "vwap_ok": bool(entry >= float(vwap_f[j])),
                            "R_1R": res["1R"], "R_2R": res["2R"], "R_RETEST": res["RETEST"],
                        })
                Hrun = max(Hrun, float(a[j]))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _select(tr: pd.DataFrame, watch: str, side: str, m: float, r: float, vwap_only: bool, tgt: str) -> pd.DataFrame:
    col = f"R_{tgt}"
    t = tr[(tr["watch_mode"] == watch) & (tr["mult"] >= m) & (tr["retrace"] == r) & tr[col].notna()]
    if side != "ALL":
        t = t[t["side"] == side]
    if vwap_only:
        t = t[t["vwap_ok"]]
    return t


def build_grid(tr: pd.DataFrame) -> pd.DataFrame:
    if tr.empty:
        return pd.DataFrame()
    days = sorted(tr["date"].unique())
    half = days[len(days) // 2]
    rows = []
    for watch, ms in GRID_M.items():
      for side in SIDES:
        for m in ms:
            for r in RETRACES:
                for vw in (False, True):
                    for tgt in TARGETS:
                        t = _select(tr, watch, side, m, r, vw, tgt)
                        if t.empty:
                            continue
                        col = f"R_{tgt}"
                        st = vs._stats(t[col])
                        a = t[t["date"] < half][col]; b = t[t["date"] >= half][col]
                        rows.append({
                            "watchlist": watch, "side": side, "min_volume_x": m, "pullback_pct_of_move": round(r * 100, 1),
                            "vwap_filter": "yes" if vw else "no", "target": tgt, **st,
                            "trades_per_day": round(st["trades"] / max(1, t["date"].nunique()), 1),
                            "avg_R_first_half": None if a.empty else round(float(a.mean()), 3),
                            "avg_R_second_half": None if b.empty else round(float(b.mean()), 3),
                        })
    return pd.DataFrame(rows)


def _ranked(grid: pd.DataFrame) -> pd.DataFrame:
    if grid.empty:
        return grid
    g = grid[grid["trades"] >= MIN_TRADES_TO_RANK]
    return g.sort_values("avg_R", ascending=False)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _work(sym, security_id, segment):
    try:
        df5 = ls.fetch_intraday_history(security_id, segment, 5, HISTORY_DAYS)
        return {"tr": find_trades(sym, df5)}
    except Exception as e:
        return {"error": str(e)}


def run_study():
    if not ls.DHAN_CLIENT_ID or not ls.DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")
    symbols = ls._load_universe_symbols()
    log.info(f"[spurtpb] backtesting {len(symbols)} symbols, {HISTORY_DAYS} calendar days of 5-min data...")
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
            elif not res["tr"].empty:
                parts.append(res["tr"])
            if n % 100 == 0:
                log.info(f"[spurtpb] ...{n}/{len(futures)} done, {sum(len(p) for p in parts)} setups so far, {time.time() - t0:.0f}s elapsed")
    tr = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    grid = build_grid(tr)
    elapsed = time.time() - t0
    log.info(f"[spurtpb] Done: {len(tr)} simulated entries, {len(grid)} rule cells, {len(errors)} errors, {elapsed:.0f}s")
    write_results(tr, grid, errors, len(symbols), elapsed)
    return tr, grid


def write_results(tr, grid, errors, universe_size, elapsed):
    os.makedirs(ls.RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(ls.IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    ranked = _ranked(grid)
    payload = {
        "run_timestamp": run_ts, "universe_size": universe_size, "duration_sec": round(elapsed),
        "days": int(tr["date"].nunique()) if not tr.empty else 0,
        "first_day": None if tr.empty else str(tr["date"].min()), "last_day": None if tr.empty else str(tr["date"].max()),
        "entries": int(len(tr)), "error_count": len(errors), "errors": errors[:200],
        "settings": {"history_days": HISTORY_DAYS, "cost_pct": COST_PCT, "min_price": MIN_PRICE,
                     "min_spurt_move": MIN_SPURT_MOVE, "min_impulse_pct": MIN_IMPULSE_PCT,
                     "min_trades_to_rank": MIN_TRADES_TO_RANK, "risk_pct_range": [MIN_RISK_PCT, MAX_RISK_PCT]},
    }
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "spurtpb.json"), json.dumps(payload, indent=2, default=str))
    ls._atomic_csv(grid if not grid.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "spurtpb_grid.csv"))
    ls._atomic_csv(tr if not tr.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "spurtpb_trades.csv"))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "spurtpb.html"), render_html(payload, ranked, grid))
    log.info(f"[spurtpb] Results written to {ls.RESULTS_DIR}/ (spurtpb.html, .json, spurtpb_grid.csv, spurtpb_trades.csv)")


def render_html(payload: dict, ranked: pd.DataFrame, grid: pd.DataFrame) -> str:
    s = payload["settings"]
    f, rc = vs._f, vs._rcls

    def rows(df, limit):
        out = ""
        for _, r in df.head(limit).iterrows():
            a, b = r["avg_R_first_half"], r["avg_R_second_half"]
            stable = a is not None and b is not None and not pd.isna(a) and not pd.isna(b) and a > 0 and b > 0
            out += (f"<tr><td>{'NSE-style' if r['watchlist'] == 'NSE_CUMVOL' else '5-min candle'}</td><td class='sym'>{r['side']}</td><td>&ge; {r['min_volume_x']:g}x</td><td>{r['pullback_pct_of_move']:g}%</td>"
                    f"<td>{r['vwap_filter']}</td><td>{r['target']}</td><td>{int(r['trades'])}</td><td>{f(r['trades_per_day'], 1)}</td>"
                    f"<td>{f(r['win_pct'], 1, '%')}</td><td class='{rc(r['avg_R'])}'>{f(r['avg_R'], 3)}</td><td>{f(r['profit_factor'])}</td>"
                    f"<td class='{rc(r['total_R'])}'>{f(r['total_R'], 1)}</td><td class='{rc(a)}'>{f(a, 3)}</td><td class='{rc(b)}'>{f(b, 3)}</td>"
                    f"<td>{'yes' if stable else 'no'}</td></tr>")
        return out

    head = ("<thead><tr><th>Watchlist</th><th>Side</th><th>Volume</th><th>Pullback</th><th>VWAP filter</th><th>Target</th><th>Trades</th><th>Per day</th>"
            "<th>Win rate</th><th>Avg net R</th><th>Profit factor</th><th>Total R</th><th>Avg R 1st half</th><th>Avg R 2nd half</th>"
            "<th>Positive in both halves</th></tr></thead>")
    if ranked.empty:
        best, worst = "<div class='empty'>No rule had enough trades yet.</div>", ""
    else:
        best = f"<table>{head}<tbody>{rows(ranked, 30)}</tbody></table>"
        worst = f"<h2>Weakest rules (for contrast)</h2><table>{head}<tbody>{rows(ranked.sort_values('avg_R'), 8)}</tbody></table>"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spurt pullback - backtest</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }} h2 {{ font-size: 16px; margin: 28px 0 8px; }}
  .nav {{ margin-bottom: 16px; font-size: 14px; }} .nav a {{ color: #58a6ff; text-decoration: none; margin-right: 18px; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 12px; line-height: 1.55; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 20px; }}
  th, td {{ padding: 7px 9px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; }} .sym {{ font-weight: 600; }}
  .pos {{ color: #3fb950; }} .neg {{ color: #f85149; }} .empty {{ color: #9aa0a6; padding: 20px 0; }} tr:hover {{ background: #161b22; }}
</style></head>
<body>
  <div class="nav"><a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/gainers">Morning gainers</a><a href="/volspurt">Volume spurt</a>
    <a href="/spurtpb/grid.csv">Grid CSV</a><a href="/spurtpb/trades.csv">All trades CSV</a></div>
  <h1>Volume spurt, then trade the pullback - backtest</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload['duration_sec']}s) &middot; {payload['universe_size']} stocks &middot;
    {payload['days']} trading days ({payload['first_day']} to {payload['last_day']}) &middot; {payload['entries']} simulated entries &middot; {payload['error_count']} errors<br>
    <b>Watchlist at 10:00, two definitions.</b> <b>NSE-style</b> (the one in NSE's "Volume Spurts" report): volume traded so far today (09:15-10:00) &ge; the stated multiple of the stock's
    average FULL-DAY volume over the previous 5 sessions; stock up vs yesterday's close = LONG setup, down = SHORT setup. <b>5-min candle</b>: one candle between 09:15 and 10:00 with volume &ge; the stated
    multiple of that stock's average for the same time of day (previous 10 days) and a body &ge; {s['min_spurt_move']}%. Both need a move of at least {s['min_impulse_pct']}% from the day's extreme to the furthest point by 10:00.
    LONG = buy the pullback of an up-move; SHORT = sell the bounce of a down-move.<br>
    <b>Entry (10:00-14:00):</b> first time price pulls back the stated % of that move (limit fill). <b>SL:</b> the move's start (skipped if risk is outside
    {s['risk_pct_range'][0]}-{s['risk_pct_range'][1]}% of price). <b>Target:</b> 1R, 2R, or RETEST (back to the extreme before the pullback). <b>Exit:</b> target, SL, or 15:25 close; stop wins ties.
    <b>VWAP filter:</b> entry only while the pullback is still on the strong side of VWAP. Cost {s['cost_pct']}% per round trip, taken off in R.
    One trade per stock per day per rule.<br>
    <b>Avg net R</b> = average result per trade in units of the risk taken (positive = profitable after costs). Rules with at least {s['min_trades_to_rank']} trades are ranked.
    <b>Positive in both halves</b> = average R is above zero in both the first and second half of the period. A short period is one market regime - candidates to watch, not proof.
  </div>
  <h2>Best rules (by average net R)</h2>
  {best}
  {worst}
</body></html>"""


if __name__ == "__main__":
    run_study()
