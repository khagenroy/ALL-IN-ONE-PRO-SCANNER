"""
CONFLUENCE STUDY - which combination of "leading" clues + "lagging" setup actually pays?   (page /best)

WHAT IT TESTS
    Every historical SMA200 support/rejection setup (sma200_study.py) and every trendline setup (trendline_live.py, the PA Toolkit's
    trendlines: a break, or a touch that holds / is rejected) on 1D, 1W, 1H and 10m for the whole stock universe.
    Each setup is traded exactly like the SMA200 backtest: entry at the next candle's open, stop beyond the signal candle (+0.1 ATR),
    risk 0.3-3 ATR, target 1R / 2R / 3R, exit after the hold limit (1D 20 bars, 1W 12, 1H 30, 10m 40), stop wins ties, round-trip cost
    taken off in R (0.15% on 1D/1W, 0.05% on 1H/10m).

    The clues (see confluence.py, all measured BEFORE the signal candle closes except the candle's own volume):
        V  volume now        >= 1.5x or 2.5x the previous-20-candle average
        B  volume build-up   the 3 candles before already averaged >= 1.2x that average
        S  squeeze           the last 10 candles' range <= 0.8x (or 0.6x) the 50 candles before them
        H  higher timeframe  the bigger trend agrees (previous day's daily SMA200 for 10m/1H, previous week's weekly SMA200 for 1D)
        C  confluence        an SMA200 setup and a trendline setup on the same side on the same candle (or the one before, intraday)

WHAT IT REPORTS
    1. "What each clue is worth": average result with the clue vs without it, per timeframe and setup type, split into two halves of
       the history. A clue that helps in BOTH halves is real evidence; one that helps in only one is probably luck.
    2. Rules (setup type x clues x target) that were positive in both halves, with enough trades. Written to
       results/confluence_rules.json; the live page /bestnow tags setups that match one of them.

HONEST LIMITS
    ~5000 rule cells are tested, so a few will look good by chance - trust the clue table and rules that are stable AND have neighbours
    that also work. The universe is today's market-cap list (survivorship). 1H covers ~5 months and 10m ~2 months, so intraday is thin.
    Open interest, delivery % and relative strength are not available from the downloaded bars and are not tested.

OUTPUT (results/)
    confluence.html (page /best) · confluence.json · confluence_grid.csv · confluence_effects.csv · confluence_rules.json · confluence_trades.csv

Run by hand:  python confluence_study.py        (TEST_SYMBOL_LIMIT=20 python confluence_study.py for a quick dry run)
"""

import os
import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_scanner as ls  # noqa: E402
import volume_spurt_study as vs  # noqa: E402  (page helpers)
import sma200_study as st  # noqa: E402  (SMA200 events, TF settings, trade simulator)
import trendline_live as tl  # noqa: E402  (trendline events)
import confluence as cf  # noqa: E402

log = logging.getLogger("confluence_study")

MIN_TRADES_TO_RANK = int(os.environ.get("CONFL_MIN_TRADES", "150") or "150")
MIN_CELL_TRADES = 30
INTRADAY_HISTORY_DAYS = int(os.environ.get("CONFL_INTRADAY_HISTORY_DAYS", "60") or "60")
BACKTEST_TFS = ("1D", "1W", "1H", "10m")
TARGET_K = (1, 2, 3)
FAMS = ("SMA200", "TREND_BREAK", "TREND_TOUCH")
FAM_NAME = {"SMA200": "SMA200 support/rejection", "TREND_BREAK": "trendline break", "TREND_TOUCH": "trendline touch/retest"}
G_VOL = (0.0, 1.5, 2.5)
G_SQ = (None, 0.8, 0.6)
G_BUILD = (None, 1.2)
G_HTF = (False, True)
G_CONFL = (False, True)
MAX_RULES = 40


# ---------------------------------------------------------------------------
# Events of one symbol / timeframe
# ---------------------------------------------------------------------------

def _row(sym, tf, side, fam, event, bar_time, vol_x, vol_build, squeeze, tabs, confl, risk_pct, dist_atr, rs):
    return {"symbol": sym, "tf": tf, "side": side, "family": fam, "event": event, "bar_time": bar_time,
            "vol_x": vol_x, "vol_build": vol_build, "squeeze": squeeze,
            "htf_ok": cf.htf_lookup(tabs, tf, bar_time, side), "confl": bool(confl),
            "dist_atr": dist_atr, "risk_pct": risk_pct, **rs}


def _events(df, tf, sym, tabs):
    n = 0 if df is None else len(df)
    if n < 260:
        return []
    o = df["open"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float); c = df["close"].to_numpy(float)
    atr = st._atr(h, l, c, st.ATR_LEN)
    cfg = st.TF_CFG[tf]
    cost = cfg["cost"] / 100.0
    L = cf.CONFL_LOOKBACK.get(tf, 0)
    out = []

    # every SMA200 / trendline event position (before any risk filter) - used for the "same candle" confluence flag
    _, sma_all = st.analyse(df, tf, sym, False, recent=n)
    sma_t = {"LONG": set(), "SHORT": set()}
    for r in sma_all:
        sma_t[r["side"]].add(n - 1 - r["bars_ago"])
    tev = tl.find_setups(df, tf, sym, recent=n, window=n, keep_t=True)
    tr_t = {"LONG": set(), "SHORT": set()}
    for r in tev:
        tr_t[r["side"]].add(r["_t"])

    def near(sets, side, t):
        return any((t - i) in sets[side] for i in range(L + 1))

    def trade(side, t, stop):
        if t >= n - 1:
            return None
        is_long = side == "LONG"
        entry = float(o[t + 1])
        risk = (entry - stop) if is_long else (stop - entry)
        if risk <= 0 or risk < st.MIN_RISK_ATR * atr[t] or risk > st.MAX_RISK_ATR * atr[t]:
            return None
        e = min(n, t + 1 + cfg["hold"])
        H, Lw = h[t + 1:e], l[t + 1:e]
        last_close = float(c[e - 1])
        costR = entry * cost / risk
        res = {}
        for k in TARGET_K:
            g = st._sim(is_long, entry, float(stop), H, Lw, last_close, k)
            res[f"R{k}"] = np.nan if g is None else round(g - costR, 3)
        return res, round(risk / entry * 100, 2)

    # SMA200 events (already simulated by sma200_study, same conventions)
    sma_tr, _ = st.analyse(df, tf, sym, True, 1)
    for r in sma_tr:
        t = r["_t"]
        out.append(_row(sym, tf, r["side"], "SMA200", "SMA200", r["bar_time"], r["vol_x"], r.get("vol_build"), r.get("squeeze"), tabs,
                        near(tr_t, r["side"], t), r["risk_pct"], np.nan, {k: r.get(k, np.nan) for k in ("R1", "R2", "R3")}))
    # trendline events
    for r in tev:
        t = r["_t"]
        stop = (l[t] - st.STOP_BUFFER_ATR * atr[t]) if r["side"] == "LONG" else (h[t] + st.STOP_BUFFER_ATR * atr[t])
        tt = trade(r["side"], t, float(stop))
        if tt is None:
            continue
        res, risk_pct = tt
        atr_pct = r["atr_pct"] or np.nan
        out.append(_row(sym, tf, r["side"], cf.family_of(r), r["event"], r["bar_time"], r["vol_x"], r.get("vol_build"), r.get("squeeze"), tabs,
                        near(sma_t, r["side"], t), risk_pct, abs(r["dist_pct"]) / atr_pct if r.get("dist_pct") is not None and atr_pct == atr_pct and atr_pct else np.nan, res))
    return out


def _work(sym, security_id, segment):
    out, err = [], []
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    tabs = {"daily": None, "weekly": None}
    try:
        dfd = ls.fetch_daily_history_cached(sym, security_id, segment)
        if dfd is not None and not dfd.empty:
            tabs = cf.htf_tables(dfd)
            out += _events(dfd, "1D", sym, tabs)
            out += _events(st._weekly_complete(dfd), "1W", sym, tabs)
    except Exception as e:
        err.append(f"daily: {e}")
    try:
        df60 = ls.fetch_60min_history(sym, security_id, segment)
        if df60 is not None and not df60.empty:
            out += _events(ls.drop_forming_bars(df60, 60, as_of), "1H", sym, tabs)
    except Exception as e:
        err.append(f"60min: {e}")
    try:
        df5 = ls.fetch_intraday_history(security_id, segment, 5, INTRADAY_HISTORY_DAYS)
        if df5 is not None and not df5.empty:
            out += _events(ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of), "10m", sym, tabs)
    except Exception as e:
        err.append(f"5min: {e}")
    return {"rows": out, "error": "; ".join(err) if err else None}


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _st(x: np.ndarray) -> dict:
    x = x[~np.isnan(x)]
    n = len(x)
    if n == 0:
        return {"trades": 0}
    pos, neg = x[x > 0].sum(), -x[x < 0].sum()
    return {"trades": int(n), "win_pct": round(float((x > 0).mean() * 100), 1), "avg_R": round(float(x.mean()), 3),
            "profit_factor": None if neg == 0 else round(float(pos / neg), 2), "total_R": round(float(x.sum()), 1)}


def _half(tf_df: pd.DataFrame) -> str:
    days = sorted(tf_df["bar_time"].str[:10].unique())
    return days[len(days) // 2]


def build_grid(tr: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for tf in BACKTEST_TFS:
        d_tf = tr[tr["tf"] == tf]
        if d_tf.empty:
            continue
        half = _half(d_tf)
        for side in ("LONG", "SHORT"):
            for fam in FAMS:
                d = d_tf[(d_tf["side"] == side) & (d_tf["family"] == fam)]
                if len(d) < MIN_CELL_TRADES:
                    continue
                with np.errstate(invalid="ignore"):
                    vx = d["vol_x"].to_numpy(float); vb = d["vol_build"].to_numpy(float); sq = d["squeeze"].to_numpy(float)
                    hk = d["htf_ok"].to_numpy(float) == 1.0
                    cn = d["confl"].to_numpy(bool)
                    first = (d["bar_time"].str[:10] < half).to_numpy()
                    R = {k: d[f"R{k}"].to_numpy(float) for k in TARGET_K}
                    for vmin in G_VOL:
                        mv = (vx >= vmin) if vmin > 0 else np.ones(len(d), bool)
                        for smax in G_SQ:
                            ms = (sq <= smax) if smax is not None else np.ones(len(d), bool)
                            for bmin in G_BUILD:
                                mb = (vb >= bmin) if bmin is not None else np.ones(len(d), bool)
                                for htf in G_HTF:
                                    mh = hk if htf else np.ones(len(d), bool)
                                    for cn_req in G_CONFL:
                                        mc = cn if cn_req else np.ones(len(d), bool)
                                        m = mv & ms & mb & mh & mc
                                        if m.sum() < MIN_CELL_TRADES:
                                            continue
                                        for k in TARGET_K:
                                            s = _st(R[k][m])
                                            if s["trades"] < MIN_CELL_TRADES:
                                                continue
                                            a = R[k][m & first]; b = R[k][m & ~first]
                                            a = a[~np.isnan(a)]; b = b[~np.isnan(b)]
                                            rows.append({"timeframe": tf, "side": side, "family": fam, "min_volume_x": vmin,
                                                         "max_squeeze": smax, "min_build": bmin, "htf_agrees": htf, "confluence": cn_req,
                                                         "target_R": k, **s,
                                                         "avg_R_first_half": round(float(a.mean()), 3) if len(a) else None,
                                                         "avg_R_second_half": round(float(b.mean()), 3) if len(b) else None})
    return pd.DataFrame(rows)


def build_effects(tr: pd.DataFrame) -> pd.DataFrame:
    """What each clue is worth: average 2R-target result with the clue vs without, per timeframe x setup type."""
    rows = []
    clues = [
        ("Volume now >= 1.5x", lambda d: (d["vol_x"] >= 1.5, d["vol_x"].notna())),
        ("Volume now >= 2.5x", lambda d: (d["vol_x"] >= 2.5, d["vol_x"].notna())),
        ("Volume build-up >= 1.2x", lambda d: (d["vol_build"] >= 1.2, d["vol_build"].notna())),
        ("Squeeze <= 0.8", lambda d: (d["squeeze"] <= 0.8, d["squeeze"].notna())),
        ("Squeeze <= 0.6", lambda d: (d["squeeze"] <= 0.6, d["squeeze"].notna())),
        ("Higher timeframe agrees", lambda d: (d["htf_ok"] == 1.0, d["htf_ok"].notna())),
        ("SMA200 + trendline together", lambda d: (d["confl"].astype(bool), pd.Series(True, index=d.index))),
    ]
    for tf in BACKTEST_TFS:
        d_tf = tr[tr["tf"] == tf]
        if d_tf.empty:
            continue
        half = _half(d_tf)
        for fam in FAMS + ("ALL",):
            d = d_tf if fam == "ALL" else d_tf[d_tf["family"] == fam]
            d = d[d["R2"].notna()]
            if len(d) < 2 * MIN_CELL_TRADES:
                continue
            first = d["bar_time"].str[:10] < half
            for name, fn in clues:
                on_m, known = fn(d)
                on_m = on_m & known
                off_m = (~on_m) & known
                if on_m.sum() < MIN_CELL_TRADES or off_m.sum() < MIN_CELL_TRADES:
                    continue
                def avg(mask):
                    x = d.loc[mask, "R2"]
                    return round(float(x.mean()), 3) if len(x) else None
                a_on, a_off = avg(on_m), avg(off_m)
                d1 = (avg(on_m & first), avg(off_m & first)); d2 = (avg(on_m & ~first), avg(off_m & ~first))
                diff1 = None if None in d1 else round(d1[0] - d1[1], 3)
                diff2 = None if None in d2 else round(d2[0] - d2[1], 3)
                rows.append({"timeframe": tf, "setup": fam, "clue": name, "trades_with": int(on_m.sum()), "avg_R_with": a_on,
                             "trades_without": int(off_m.sum()), "avg_R_without": a_off,
                             "difference": None if a_on is None or a_off is None else round(a_on - a_off, 3),
                             "difference_first_half": diff1, "difference_second_half": diff2,
                             "helps_in_both_halves": bool(diff1 is not None and diff2 is not None and diff1 > 0 and diff2 > 0)})
    return pd.DataFrame(rows)


def _conds_text(r) -> str:
    c = []
    if r["min_volume_x"]:
        c.append(f"volume >= {r['min_volume_x']:g}x")
    if r["min_build"] is not None and not pd.isna(r["min_build"]):
        c.append(f"volume build-up >= {r['min_build']:g}x")
    if r["max_squeeze"] is not None and not pd.isna(r["max_squeeze"]):
        c.append(f"squeeze <= {r['max_squeeze']:g}")
    if r["htf_agrees"]:
        c.append("higher timeframe agrees")
    if r["confluence"]:
        c.append("SMA200 + trendline together")
    return ", ".join(c) if c else "no extra clue"


def pick_rules(grid: pd.DataFrame, tr: pd.DataFrame) -> list:
    """Rules positive in BOTH halves with enough trades (and, on 1D/1W, positive in at least half of the years)."""
    if grid.empty:
        return []
    g = grid[(grid["trades"] >= MIN_TRADES_TO_RANK) & (grid["avg_R"] > 0) & (grid["avg_R_first_half"] > 0) & (grid["avg_R_second_half"] > 0)]
    g = g[g["profit_factor"].fillna(0) >= 1.1].sort_values("avg_R", ascending=False)
    rules = []
    for _, r in g.iterrows():
        if r["timeframe"] in ("1D", "1W"):          # also check calendar years
            d = tr[(tr["tf"] == r["timeframe"]) & (tr["side"] == r["side"]) & (tr["family"] == r["family"])]
            with np.errstate(invalid="ignore"):
                m = np.ones(len(d), bool)
                if r["min_volume_x"]:
                    m &= d["vol_x"].to_numpy(float) >= r["min_volume_x"]
                if r["max_squeeze"] is not None and not pd.isna(r["max_squeeze"]):
                    m &= d["squeeze"].to_numpy(float) <= r["max_squeeze"]
                if r["min_build"] is not None and not pd.isna(r["min_build"]):
                    m &= d["vol_build"].to_numpy(float) >= r["min_build"]
                if r["htf_agrees"]:
                    m &= d["htf_ok"].to_numpy(float) == 1.0
                if r["confluence"]:
                    m &= d["confl"].to_numpy(bool)
            x = d[m]
            yrs = x.groupby(x["bar_time"].str[:4])[f"R{int(r['target_R'])}"].agg(["mean", "count"])
            yrs = yrs[yrs["count"] >= 10]
            if len(yrs) >= 3 and (yrs["mean"] > 0).mean() < 0.5:
                continue
        label = (f"{r['timeframe']} {r['side']} {FAM_NAME[r['family']]} | {_conds_text(r)} | {int(r['target_R'])}R target: "
                 f"{r['avg_R']:+.2f}R average over {int(r['trades'])} trades (win {r['win_pct']:.0f}%)")
        rules.append({"tf": r["timeframe"], "side": r["side"], "family": r["family"], "min_vol": float(r["min_volume_x"]),
                      "max_squeeze": None if pd.isna(r["max_squeeze"]) else float(r["max_squeeze"]),
                      "min_build": None if pd.isna(r["min_build"]) else float(r["min_build"]),
                      "htf": bool(r["htf_agrees"]), "confl": bool(r["confluence"]), "target": int(r["target_R"]),
                      "trades": int(r["trades"]), "win_pct": float(r["win_pct"]), "avg_R": float(r["avg_R"]),
                      "avg_R_first_half": float(r["avg_R_first_half"]), "avg_R_second_half": float(r["avg_R_second_half"]),
                      "label": label})
        if len(rules) >= MAX_RULES:
            break
    return rules


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_study():
    if not ls.DHAN_CLIENT_ID or not ls.DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")
    symbols = ls._load_universe_symbols()
    log.info(f"[confluence] analysing {len(symbols)} symbols on {', '.join(BACKTEST_TFS)} ...")
    jobs, errors = [], []
    for sym in symbols:
        sid, seg = ls.get_security_id_and_segment(sym)
        if not sid or seg != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        jobs.append((sym, sid, seg))
    rows, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=max(1, ls.SCAN_WORKERS)) as ex:
        futures = [(sym, ex.submit(_work, sym, sid, seg)) for sym, sid, seg in jobs]
        for n, (sym, fut) in enumerate(futures, 1):
            res = fut.result()
            rows += res["rows"]
            if res["error"]:
                errors.append({"symbol": sym, "error": res["error"]})
                log.warning(f"{sym}: {res['error']}")
            if n % 100 == 0:
                log.info(f"[confluence] ...{n}/{len(futures)} done, {len(rows)} setups so far, {time.time() - t0:.0f}s elapsed")
    tr = pd.DataFrame(rows)
    return finish(tr, errors, len(symbols), time.time() - t0)


def finish(tr: pd.DataFrame, errors: list, universe_size: int, elapsed: float):
    grid = build_grid(tr) if not tr.empty else pd.DataFrame()
    effects = build_effects(tr) if not tr.empty else pd.DataFrame()
    rules = pick_rules(grid, tr) if not tr.empty else []
    log.info(f"[confluence] Done: {len(tr)} historical setups, {len(grid)} rule cells, {len(rules)} validated rules, {len(errors)} errors, {elapsed:.0f}s")
    write_results(tr, grid, effects, rules, errors, universe_size, elapsed)
    return tr, grid, effects, rules


def write_results(tr, grid, effects, rules, errors, universe_size, elapsed):
    os.makedirs(ls.RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(ls.IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    counts = {} if tr.empty else {f"{tf} {fam}": int(((tr["tf"] == tf) & (tr["family"] == fam)).sum()) for tf in BACKTEST_TFS for fam in FAMS}
    payload = {"run_timestamp": run_ts, "universe_size": universe_size, "duration_sec": round(elapsed), "setups_by_type": counts,
               "rule_cells": 0 if grid.empty else int(len(grid)), "validated_rules": len(rules), "error_count": len(errors), "errors": errors[:200],
               "settings": {"min_trades_to_rank": MIN_TRADES_TO_RANK, "cost_daily_pct": st.COST_DAILY, "cost_intraday_pct": st.COST_INTRADAY,
                            "intraday_history_days": INTRADAY_HISTORY_DAYS}}
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "confluence.json"), json.dumps(payload, indent=2, default=str))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "confluence_rules.json"),
                     json.dumps({"generated": run_ts, "min_trades": MIN_TRADES_TO_RANK, "rules": rules}, indent=1, default=str))
    ls._atomic_csv(grid if not grid.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "confluence_grid.csv"))
    ls._atomic_csv(effects if not effects.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "confluence_effects.csv"))
    ls._atomic_csv(tr.drop(columns=[c for c in ("_t",) if c in tr.columns]) if not tr.empty else pd.DataFrame(),
                   os.path.join(ls.RESULTS_DIR, "confluence_trades.csv"))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "confluence.html"), render_html(payload, grid, effects, rules))
    log.info(f"[confluence] Results written to {ls.RESULTS_DIR}/ (confluence.html, .json, confluence_rules.json, _grid.csv, _effects.csv, _trades.csv)")


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def render_html(payload: dict, grid: pd.DataFrame, effects: pd.DataFrame, rules: list) -> str:
    f, rc = vs._f, vs._rcls
    s = payload["settings"]

    eff_html = "<div class='empty'>Not enough setups to measure the clues yet.</div>"
    if effects is not None and not effects.empty:
        e = effects[effects["setup"] == "ALL"].copy()
        order = {"1D": 0, "1W": 1, "1H": 2, "10m": 3}
        e["_o"] = e["timeframe"].map(order)
        e = e.sort_values(["_o", "difference"], ascending=[True, False])
        trs = ""
        for _, r in e.iterrows():
            trs += (f"<tr><td class='sym'>{r['timeframe']}</td><td>{r['clue']}</td><td>{int(r['trades_with'])}</td><td class='{rc(r['avg_R_with'])}'>{f(r['avg_R_with'], 3)}</td>"
                    f"<td>{int(r['trades_without'])}</td><td class='{rc(r['avg_R_without'])}'>{f(r['avg_R_without'], 3)}</td>"
                    f"<td class='{rc(r['difference'])}'>{f(r['difference'], 3)}</td><td class='{rc(r['difference_first_half'])}'>{f(r['difference_first_half'], 3)}</td>"
                    f"<td class='{rc(r['difference_second_half'])}'>{f(r['difference_second_half'], 3)}</td><td>{'yes' if r['helps_in_both_halves'] else 'no'}</td></tr>")
        eff_html = ("<table><thead><tr><th>Timeframe</th><th>Clue</th><th>Trades with</th><th>Avg R with</th><th>Trades without</th><th>Avg R without</th>"
                    "<th>Difference</th><th>Diff. 1st half</th><th>Diff. 2nd half</th><th>Helps in both halves</th></tr></thead><tbody>" + trs + "</tbody></table>")

    if not rules:
        rules_html = "<div class='empty'>No rule was positive in both halves with enough trades. That is an honest result: the clues did not make these setups reliably profitable.</div>"
    else:
        trs = ""
        for r in rules[:30]:
            trs += (f"<tr><td class='sym'>{r['tf']}</td><td class='{'pos' if r['side'] == 'LONG' else 'neg'}'>{r['side']}</td><td>{FAM_NAME[r['family']]}</td>"
                    f"<td>{r['label'].split(' | ')[1]}</td><td>{r['target']}R</td><td>{r['trades']}</td><td>{f(r['win_pct'], 1, '%')}</td>"
                    f"<td class='{rc(r['avg_R'])}'>{f(r['avg_R'], 3)}</td><td class='{rc(r['avg_R_first_half'])}'>{f(r['avg_R_first_half'], 3)}</td>"
                    f"<td class='{rc(r['avg_R_second_half'])}'>{f(r['avg_R_second_half'], 3)}</td></tr>")
        rules_html = ("<table><thead><tr><th>Timeframe</th><th>Side</th><th>Setup</th><th>Clues required</th><th>Target</th><th>Trades</th><th>Win rate</th>"
                      "<th>Avg net R</th><th>Avg R 1st half</th><th>Avg R 2nd half</th></tr></thead><tbody>" + trs + "</tbody></table>")

    counts = ", ".join(f"{k}: {v}" for k, v in payload["setups_by_type"].items() if v)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Best setups - what the clues are worth</title>
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
  <div class="nav"><a href="/bestnow">Best setups now</a><a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a><a href="/sma200">SMA200 backtest</a>
    <a href="/best/effects.csv">Clue table CSV</a><a href="/best/grid.csv">All rules CSV</a><a href="/best/trades.csv">All historical setups CSV</a></div>
  <h1>Best setups - what each clue is actually worth</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload['duration_sec']}s) &middot; {payload['universe_size']} stocks &middot; setups tested: {counts or 'none'} &middot; {payload['error_count']} errors<br>
    Every SMA200 and trendline setup in the history was traded the same way (next open, stop beyond the signal candle, 2R target in the clue table; hold limit 1D 20 bars,
    1W 12, 1H 30, 10m 40; stop wins ties; cost {s['cost_daily_pct']}% on 1D/1W and {s['cost_intraday_pct']}% on 1H/10m taken off in R). <b>Avg R</b> = average result per trade in units of the risk taken.<br>
    <b>Clues:</b> volume now (signal candle vs previous 20), volume build-up (the 3 candles before), squeeze (recent candle ranges vs the 50 before), higher timeframe agrees
    (previous day's daily SMA200 for 10m/1H, previous week's weekly SMA200 for 1D), SMA200 + trendline setup together. A clue is only real evidence if it helps in <b>both halves</b> of the history.
    About {payload['rule_cells']} rule combinations were tested, so a few will look good by luck - prefer rules with many trades whose neighbours also work. Intraday history is short (1H ~5 months, 10m ~2 months).
    Not tested (no data in the downloaded bars): open interest, delivery %, relative strength.
  </div>
  <h2>What each clue is worth (all setups, 2R target)</h2>
  {eff_html}
  <h2>Rules that were positive in both halves ({len(rules)}; these are what /bestnow tags as tested)</h2>
  {rules_html}
</body></html>"""


if __name__ == "__main__":
    run_study()
