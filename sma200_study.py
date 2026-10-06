"""
SMA200 SUPPORT / REJECTION - BACKTEST + SETUP SCANNER  (read-only: places no orders, touches no signal logic)

THE IDEA
    LONG  ("support")  : price is above a rising SMA200, pulls back to it, and holds (bullish close above it).
    SHORT ("rejection"): price is below a falling SMA200, rallies up to it, and is rejected (bearish close below it).
    Volume support     : the touch candle's volume is a multiple of the previous-20-bar average volume.

EXACT RULES (every timeframe uses the same rules, scaled by that timeframe's own ATR(14))
    LONG  signal bar t:
        - SMA200(t) is rising: SMA200(t) > SMA200(t - slope_bars)   (optionally by a minimum amount, see grid)
        - the 5 closes before t were all above SMA200
        - the bar's LOW reaches the SMA zone: low <= SMA200 + 0.25 x ATR, but not through it: low >= SMA200 - 1.0 x ATR
        - the bar CLOSES above SMA200 and closes above its open (bullish)
    SHORT signal: the exact mirror (falling SMA, 5 closes below, HIGH reaches the SMA zone from below,
        not through it by more than 1 ATR, closes below SMA200 and below its open).
    One signal per side per 10 bars per stock (first touch only; repeated touches are skipped).

TRADE (the backtest)
    Entry  : OPEN of the next bar (no look-ahead).
    Stop   : long -> signal bar low - 0.1 ATR ; short -> signal bar high + 0.1 ATR.
             Skipped if the risk is under 0.3 ATR or over 3 ATR.
    Target : 1R, 2R or 3R (tested separately). Exit: target, stop, or after the hold limit
             (1D 20 bars, 1W 12, 1H 30, 10m 40) at that bar's close. A bar touching both stop and target = STOP.
    Cost   : round trip, taken off in R (1D/1W 0.15%, 1H/10m 0.05%; env SMA200_COST_DAILY / SMA200_COST_INTRADAY).

WHAT IT REPORTS
    Backtest grid per timeframe (1D, 1W, 1H, 10m) x side x volume multiple (none, 1x, 1.5x, 2x) x SMA slope strength x target:
    trades, win rate, average net R, profit factor, total R, first-half vs second-half average R.
    "Setups now": every stock whose LAST CLOSED bar meets the rules, on 10m, 1H, 4H, 1D, 1W (1M is impossible:
    10 years of data holds only ~120 monthly bars, so no monthly SMA200). 4H has too little history to backtest.

HONEST LIMITS
    Daily/weekly use ~10 years of history; 1H ~5 months; 10m ~2 months, so the intraday results are thin.
    Shorting on daily/weekly needs futures (cash-delivery shorting is not possible) - read the SHORT rows as "for F&O stocks".
    The universe is today's market-cap list (survivorship). Use the results to find rules worth watching live.

OUTPUT (results/)
    sma200.html (page /sma200) · sma200.json · sma200_grid.csv · sma200_trades.csv · sma200_live.csv

Run by hand:  python sma200_study.py        (TEST_SYMBOL_LIMIT=20 python sma200_study.py for a quick dry run)
"""

import os
import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_scanner as ls  # noqa: E402
import volume_spurt_study as vs  # noqa: E402  (page helpers + _stats)

log = logging.getLogger("sma200_study")

SMA_LEN = 200
ATR_LEN = 14
VOL_LEN = 20
TOUCH_ATR = 0.25
PIERCE_ATR = 1.0
STOP_BUFFER_ATR = 0.1
MIN_RISK_ATR, MAX_RISK_ATR = 0.3, 3.0
PRIOR_BARS = 5
COOLDOWN_BARS = 10
MIN_TRADES_TO_RANK = int(os.environ.get("SMA200_MIN_TRADES", "150") or "150")
COST_DAILY = float(os.environ.get("SMA200_COST_DAILY", "0.15") or "0.15")
COST_INTRADAY = float(os.environ.get("SMA200_COST_INTRADAY", "0.05") or "0.05")
INTRADAY_HISTORY_DAYS = int(os.environ.get("SMA200_INTRADAY_HISTORY_DAYS", "60") or "60")

# per-timeframe: bars used to measure the SMA slope, "normal" slope (% of SMA over those bars), hold limit, cost
TF_CFG = {
    "1D":  {"slope_bars": 20, "slope_base": 1.0, "hold": 20, "cost": COST_DAILY},
    "1W":  {"slope_bars": 13, "slope_base": 3.0, "hold": 12, "cost": COST_DAILY},
    "1H":  {"slope_bars": 20, "slope_base": 0.30, "hold": 30, "cost": COST_INTRADAY},
    "10m": {"slope_bars": 20, "slope_base": 0.10, "hold": 40, "cost": COST_INTRADAY},
    "4H":  {"slope_bars": 20, "slope_base": 0.60, "hold": 20, "cost": COST_INTRADAY},
}
BACKTEST_TFS = ("1D", "1W", "1H", "10m")
LIVE_TFS = ("10m", "1H", "4H", "1D", "1W")
GRID_VOL = (0.0, 1.0, 1.5, 2.0)          # 0 = no volume requirement
GRID_SLOPE = (0.0, 1.0, 2.0)             # x the timeframe's "normal" slope; 0 = merely rising / falling
TARGET_K = (1, 2, 3)


def _atr(h, l, c, n):
    pc = np.concatenate(([c[0]], c[:-1]))
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    return pd.Series(tr).ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean().to_numpy()


def _sim(is_long, entry, stop, H, L, last_close, k):
    """Gross R of a trade with a k x R target walking bars H/L from the entry bar on. Stop wins ties."""
    risk = (entry - stop) if is_long else (stop - entry)
    if risk <= 0 or len(H) == 0:
        return None
    if is_long:
        tgt = entry + k * risk
        stop_hit, tgt_hit = L <= stop, H >= tgt
    else:
        tgt = entry - k * risk
        stop_hit, tgt_hit = H >= stop, L <= tgt
    inf = len(H) + 1
    s_idx = int(np.argmax(stop_hit)) if stop_hit.any() else inf
    t_idx = int(np.argmax(tgt_hit)) if tgt_hit.any() else inf
    if s_idx <= t_idx and s_idx < inf:
        return -1.0
    if t_idx < inf:
        return float(k)
    pnl = (last_close - entry) if is_long else (entry - last_close)
    return pnl / risk


def analyse(df: pd.DataFrame, tf: str, sym: str, backtest: bool, recent: int = 1):
    """Returns (trades list of dicts, live list of dicts) for one symbol / timeframe.
    `live` = signals on the last `recent` closed bars (default: only the very last bar)."""
    cfg = TF_CFG[tf]
    lb = cfg["slope_bars"]
    if df is None or len(df) < SMA_LEN + lb + PRIOR_BARS + 5:
        return [], []
    o = df["open"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float); v = df["volume"].to_numpy(float)
    n = len(df)
    sma = pd.Series(c).rolling(SMA_LEN, min_periods=SMA_LEN).mean().to_numpy()
    atr = _atr(h, l, c, ATR_LEN)
    vma = pd.Series(v).rolling(VOL_LEN, min_periods=VOL_LEN).mean().shift(1).to_numpy()
    with np.errstate(invalid="ignore", divide="ignore"):
        vol_x = np.where(vma > 0, v / vma, np.nan)
        sma_lb = np.concatenate((np.full(lb, np.nan), sma[:-lb]))
        slope_pct = (sma - sma_lb) / sma_lb * 100.0
    above = (c > sma).astype(float); below = (c < sma).astype(float)
    prior_above = pd.Series(above).shift(1).rolling(PRIOR_BARS).sum().to_numpy() == PRIOR_BARS
    prior_below = pd.Series(below).shift(1).rolling(PRIOR_BARS).sum().to_numpy() == PRIOR_BARS
    valid = np.isfinite(sma) & np.isfinite(atr) & np.isfinite(slope_pct) & (atr > 0)
    with np.errstate(invalid="ignore"):
        long_base = (valid & (slope_pct > 0) & prior_above & (c > sma) & (c > o)
                     & (l <= sma + TOUCH_ATR * atr) & (l >= sma - PIERCE_ATR * atr))
        short_base = (valid & (slope_pct < 0) & prior_below & (c < sma) & (c < o)
                      & (h >= sma - TOUCH_ATR * atr) & (h <= sma + PIERCE_ATR * atr))
    intraday = tf in ("10m", "1H", "4H")
    cost = cfg["cost"] / 100.0
    idx_time = df.index

    def stamp(t):
        ts = idx_time[t]
        if intraday:
            ts = ts + pd.Timedelta(minutes=330)        # Dhan intraday stamps are UTC -> show IST
            return ts.strftime("%Y-%m-%d %H:%M")
        if ts.hour == 18 and ts.minute == 30:          # Dhan daily stamp = IST midnight shown as UTC -> real trading date is +5:30
            ts = ts + pd.Timedelta(minutes=330)
        return ts.strftime("%Y-%m-%d")

    trades, live = [], []
    for side, base in (("LONG", long_base), ("SHORT", short_base)):
        is_long = side == "LONG"
        last_kept = -10 ** 9
        for t in np.where(base)[0]:
            if t - last_kept < COOLDOWN_BARS:
                continue
            last_kept = t
            sign = 1.0 if is_long else -1.0
            row = {
                "tf": tf, "symbol": sym, "side": side, "bar_time": stamp(t), "close": round(float(c[t]), 2),
                "sma200": round(float(sma[t]), 2), "dist_pct": round((float(c[t]) - float(sma[t])) / float(sma[t]) * 100, 2),
                "slope_pct": round(float(slope_pct[t]) * sign, 3),
                "slope_x": round(float(slope_pct[t]) * sign / cfg["slope_base"], 2),
                # same slope as an angle: 1x "normal" slope = 45 degrees, 2x = 63, 3x = 72, flat = 0 (chart-zoom independent)
                "slope_deg": round(float(np.degrees(np.arctan(float(slope_pct[t]) * sign / cfg["slope_base"]))), 1),
                "vol_x": None if not np.isfinite(vol_x[t]) else round(float(vol_x[t]), 2),
                "atr_pct": round(float(atr[t]) / float(c[t]) * 100, 2),
            }
            stop = (l[t] - STOP_BUFFER_ATR * atr[t]) if is_long else (h[t] + STOP_BUFFER_ATR * atr[t])
            if t >= n - recent:
                risk = abs(float(c[t]) - float(stop))
                row["stop"] = round(float(stop), 2)
                row["risk_pct"] = round(risk / float(c[t]) * 100, 2)
                row["bars_ago"] = int(n - 1 - t)
                live.append(row)
                continue
            if not backtest:
                continue
            entry = float(o[t + 1])
            risk = (entry - stop) if is_long else (stop - entry)
            if risk <= 0 or risk < MIN_RISK_ATR * atr[t] or risk > MAX_RISK_ATR * atr[t]:
                continue
            e = min(n, t + 1 + cfg["hold"])
            H, L = h[t + 1:e], l[t + 1:e]
            last_close = float(c[e - 1])
            costR = (entry * cost) / risk
            row["entry"] = round(entry, 2)
            row["risk_pct"] = round(risk / entry * 100, 2)
            for k in TARGET_K:
                g = _sim(is_long, entry, float(stop), H, L, last_close, k)
                row[f"R{k}"] = np.nan if g is None else round(g - costR, 3)
            trades.append(row)
    return trades, live


# ---------------------------------------------------------------------------
# Data per symbol
# ---------------------------------------------------------------------------

def _weekly_complete(dfd: pd.DataFrame) -> pd.DataFrame:
    wk = ls.build_weekly_bars(dfd)
    if wk.empty or dfd.empty:
        return wk
    if wk.index[-1].date() > dfd.index[-1].date():      # the week has not finished yet
        wk = wk.iloc[:-1]
    return wk


def _work(sym, security_id, segment):
    out_trades, out_live, err = [], [], []
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    try:
        dfd = ls.fetch_daily_history_cached(sym, security_id, segment)
        if dfd is not None and not dfd.empty:
            t, lv = analyse(dfd, "1D", sym, True); out_trades += t; out_live += lv
            t, lv = analyse(_weekly_complete(dfd), "1W", sym, True); out_trades += t; out_live += lv
    except Exception as e:
        err.append(f"daily: {e}")
    try:
        df60 = ls.fetch_60min_history(sym, security_id, segment)
        if df60 is not None and not df60.empty:
            h1 = ls.drop_forming_bars(df60, 60, as_of)
            t, lv = analyse(h1, "1H", sym, True); out_trades += t; out_live += lv
            h4 = ls.drop_forming_bars(ls.build_merged_bars(df60, 4), 240, as_of)
            t, lv = analyse(h4, "4H", sym, False); out_trades += t; out_live += lv
    except Exception as e:
        err.append(f"60min: {e}")
    try:
        df5 = ls.fetch_intraday_history(security_id, segment, 5, INTRADAY_HISTORY_DAYS)
        if df5 is not None and not df5.empty:
            m10 = ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of)
            t, lv = analyse(m10, "10m", sym, True); out_trades += t; out_live += lv
    except Exception as e:
        err.append(f"5min: {e}")
    return {"trades": out_trades, "live": out_live, "error": "; ".join(err) if err else None}


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _select(tr: pd.DataFrame, tf: str, side: str, vmin: float, smin: float, k: int) -> pd.DataFrame:
    col = f"R{k}"
    t = tr[(tr["tf"] == tf) & (tr["side"] == side) & tr[col].notna()]
    if vmin > 0:
        t = t[t["vol_x"].notna() & (t["vol_x"] >= vmin)]
    t = t[t["slope_x"] >= smin] if smin > 0 else t
    return t


def build_grid(tr: pd.DataFrame) -> pd.DataFrame:
    if tr.empty:
        return pd.DataFrame()
    rows = []
    for tf in BACKTEST_TFS:
        d = tr[tr["tf"] == tf]
        if d.empty:
            continue
        days = sorted(d["bar_time"].str[:10].unique())
        half = days[len(days) // 2]
        for side in ("LONG", "SHORT"):
            for vmin in GRID_VOL:
                for smin in GRID_SLOPE:
                    for k in TARGET_K:
                        t = _select(tr, tf, side, vmin, smin, k)
                        if t.empty:
                            continue
                        col = f"R{k}"
                        st = vs._stats(t[col])
                        dates = t["bar_time"].str[:10]
                        a = t[dates < half][col]; b = t[dates >= half][col]
                        yrs = t.groupby(dates.str[:4])[col].agg(["mean", "count"])
                        yrs = yrs[yrs["count"] >= 10]
                        rows.append({
                            "timeframe": tf, "side": side, "min_volume_x": vmin, "min_slope_x": smin, "target_R": k, **st,
                            "avg_R_first_half": None if a.empty else round(float(a.mean()), 3),
                            "avg_R_second_half": None if b.empty else round(float(b.mean()), 3),
                            "years_positive": f"{int((yrs['mean'] > 0).sum())}/{len(yrs)}" if tf in ("1D", "1W") else "",
                        })
    return pd.DataFrame(rows)


def _ranked(grid: pd.DataFrame) -> pd.DataFrame:
    if grid.empty:
        return grid
    return grid[grid["trades"] >= MIN_TRADES_TO_RANK].sort_values("avg_R", ascending=False)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_study():
    if not ls.DHAN_CLIENT_ID or not ls.DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")
    symbols = ls._load_universe_symbols()
    log.info(f"[sma200] analysing {len(symbols)} symbols on {', '.join(LIVE_TFS)} ...")
    jobs, errors = [], []
    for sym in symbols:
        sid, seg = ls.get_security_id_and_segment(sym)
        if not sid or seg != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        jobs.append((sym, sid, seg))
    trades, live, t0 = [], [], time.time()
    with ThreadPoolExecutor(max_workers=max(1, ls.SCAN_WORKERS)) as ex:
        futures = [(sym, ex.submit(_work, sym, sid, seg)) for sym, sid, seg in jobs]
        for n, (sym, fut) in enumerate(futures, 1):
            res = fut.result()
            trades += res["trades"]; live += res["live"]
            if res["error"]:
                errors.append({"symbol": sym, "error": res["error"]})
                log.warning(f"{sym}: {res['error']}")
            if n % 100 == 0:
                log.info(f"[sma200] ...{n}/{len(futures)} done, {len(trades)} backtest signals, {len(live)} setups now, {time.time() - t0:.0f}s elapsed")
    tr = pd.DataFrame(trades)
    lv = pd.DataFrame(live)
    grid = build_grid(tr)
    elapsed = time.time() - t0
    log.info(f"[sma200] Done: {len(tr)} historical signals, {len(grid)} rule cells, {len(lv)} setups now, {len(errors)} errors, {elapsed:.0f}s")
    write_results(tr, lv, grid, errors, len(symbols), elapsed)
    return tr, grid


def write_results(tr, lv, grid, errors, universe_size, elapsed):
    os.makedirs(ls.RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(ls.IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    ranked = _ranked(grid)
    counts = {} if tr.empty else {tf: int((tr["tf"] == tf).sum()) for tf in BACKTEST_TFS}
    payload = {
        "run_timestamp": run_ts, "universe_size": universe_size, "duration_sec": round(elapsed),
        "historical_signals_by_tf": counts, "setups_now": 0 if lv.empty else int(len(lv)),
        "error_count": len(errors), "errors": errors[:200],
        "settings": {"min_trades_to_rank": MIN_TRADES_TO_RANK, "cost_daily_pct": COST_DAILY, "cost_intraday_pct": COST_INTRADAY,
                     "intraday_history_days": INTRADAY_HISTORY_DAYS},
    }
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "sma200.json"), json.dumps(payload, indent=2, default=str))
    ls._atomic_csv(grid if not grid.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "sma200_grid.csv"))
    ls._atomic_csv(tr if not tr.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "sma200_trades.csv"))
    ls._atomic_csv(lv if not lv.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "sma200_live.csv"))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "sma200.html"), render_html(payload, ranked, grid, lv))
    log.info(f"[sma200] Results written to {ls.RESULTS_DIR}/ (sma200.html, .json, sma200_grid.csv, sma200_trades.csv, sma200_live.csv)")


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def render_html(payload: dict, ranked: pd.DataFrame, grid: pd.DataFrame, lv: pd.DataFrame) -> str:
    f, rc = vs._f, vs._rcls
    s = payload["settings"]

    def grid_rows(df, limit):
        out = ""
        for _, r in df.head(limit).iterrows():
            a, b = r["avg_R_first_half"], r["avg_R_second_half"]
            stable = a is not None and b is not None and not pd.isna(a) and not pd.isna(b) and a > 0 and b > 0
            out += (f"<tr><td class='sym'>{r['timeframe']}</td><td>{r['side']}</td><td>{'any' if r['min_volume_x'] == 0 else '&ge; ' + format(r['min_volume_x'], 'g') + 'x'}</td>"
                    f"<td>{'rising/falling' if r['min_slope_x'] == 0 else '&ge; ' + format(r['min_slope_x'], 'g') + 'x normal'}</td><td>{int(r['target_R'])}R</td>"
                    f"<td>{int(r['trades'])}</td><td>{f(r['win_pct'], 1, '%')}</td><td class='{rc(r['avg_R'])}'>{f(r['avg_R'], 3)}</td>"
                    f"<td>{f(r['profit_factor'])}</td><td class='{rc(r['total_R'])}'>{f(r['total_R'], 1)}</td>"
                    f"<td class='{rc(a)}'>{f(a, 3)}</td><td class='{rc(b)}'>{f(b, 3)}</td><td>{r['years_positive']}</td><td>{'yes' if stable else 'no'}</td></tr>")
        return out

    head = ("<thead><tr><th>Timeframe</th><th>Side</th><th>Volume</th><th>SMA slope</th><th>Target</th><th>Trades</th><th>Win rate</th>"
            "<th>Avg net R</th><th>Profit factor</th><th>Total R</th><th>Avg R 1st half</th><th>Avg R 2nd half</th><th>Years positive</th><th>Positive in both halves</th></tr></thead>")
    if ranked.empty:
        best, per_tf = "<div class='empty'>No rule had enough trades yet.</div>", ""
    else:
        best = f"<table>{head}<tbody>{grid_rows(ranked, 25)}</tbody></table>"
        per_tf = ""
        for tf in BACKTEST_TFS:
            g = ranked[ranked["timeframe"] == tf]
            if not g.empty:
                per_tf += f"<h2>Best rules on {tf}</h2><table>{head}<tbody>{grid_rows(g, 6)}</tbody></table>"

    live_html = "<div class='empty'>No stock's last closed bar meets the rules right now.</div>"
    if lv is not None and not lv.empty:
        d = lv.copy()
        d["_o"] = d["tf"].map({"1W": 0, "1D": 1, "4H": 2, "1H": 3, "10m": 4})
        d = d.sort_values(["_o", "vol_x"], ascending=[True, False], na_position="last")
        trs = ""
        for _, r in d.iterrows():
            trs += (f"<tr><td class='sym'>{r['symbol']}</td><td>{r['tf']}</td><td class='{'pos' if r['side'] == 'LONG' else 'neg'}'>{r['side']}</td>"
                    f"<td>{r['bar_time']}</td><td>{f(r['close'])}</td><td>{f(r['sma200'])}</td><td>{f(r['dist_pct'], 2, '%')}</td>"
                    f"<td>{f(r['slope_pct'], 2, '%')} ({f(r['slope_x'], 1)}x, {f(r['slope_deg'], 0, '&deg;')})</td><td>{f(r['vol_x'], 1, 'x')}</td><td>{f(r['stop'])}</td><td>{f(r['risk_pct'], 2, '%')}</td></tr>")
        live_html = ("<table><thead><tr><th>Symbol</th><th>Timeframe</th><th>Side</th><th>Signal bar</th><th>Close</th><th>SMA200</th><th>Distance</th>"
                     "<th>SMA slope</th><th>Volume vs 20-bar avg</th><th>Stop</th><th>Risk</th></tr></thead><tbody>" + trs + "</tbody></table>")

    counts = ", ".join(f"{k}: {v}" for k, v in payload["historical_signals_by_tf"].items())
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>SMA200 support / rejection</title>
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
  <div class="nav"><a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/gainers">Morning gainers</a><a href="/volspurt">Volume spurt</a><a href="/spurtpb">Spurt pullback</a>
    <a href="/sma200/live.csv">Setups CSV</a><a href="/sma200/grid.csv">Rules CSV</a><a href="/sma200/trades.csv">All historical signals CSV</a></div>
  <h1>SMA200 support / rejection - backtest and setups</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload['duration_sec']}s) &middot; {payload['universe_size']} stocks &middot; {payload['setups_now']} setups right now &middot;
    historical signals: {counts} &middot; {payload['error_count']} errors<br>
    <b>LONG (support):</b> rising SMA200, price above it, a bar dips to the SMA200 zone and closes back above it on a bullish candle.
    <b>SHORT (rejection):</b> falling SMA200, price below it, a bar rallies to the SMA200 zone and closes back below it on a bearish candle.
    Touch zone = within 0.25 ATR of the SMA (no more than 1 ATR through it). One signal per side per 10 bars per stock.
    <b>Volume</b> = the touch bar's volume as a multiple of the previous 20 bars' average.<br>
    <b>Backtest trade:</b> entry next bar's open; stop beyond the touch bar's far end (+0.1 ATR); target 1R / 2R / 3R; exit at target, stop, or after the hold limit
    (1D 20 bars, 1W 12, 1H 30, 10m 40); stop wins ties; cost {s['cost_daily_pct']}% round trip on 1D/1W, {s['cost_intraday_pct']}% on 1H/10m, taken off in R.
    <b>Avg net R</b> = average result per trade in units of the risk taken. Rules with at least {s['min_trades_to_rank']} trades are ranked.
    <b>Positive in both halves</b> and <b>Years positive</b> are quick checks against luck.<br>
    Monthly SMA200 is impossible (only ~120 monthly bars exist). 4H shows setups only (too little history to backtest). Daily/weekly SHORTs need futures.
    1H covers ~5 months and 10m ~2 months, so their results are thin.
  </div>
  <h2>Setups right now (last closed bar of each timeframe)</h2>
  {live_html}
  <h2>Best rules overall (by average net R)</h2>
  {best}
  {per_tf}
</body></html>"""


if __name__ == "__main__":
    run_study()
