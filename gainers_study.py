"""
MORNING-GAINERS STRENGTH STUDY  (read-only - places no orders, touches no signal logic)

QUESTION IT ANSWERS
    "Which stocks are in the top-gainers list in the morning, and do they KEEP
     their strength through the rest of the day? How does volume relate to
     that?"

HOW
    Dhan serves full 5-minute candles (with volume) for any past day, so
    nothing has to be tracked live. After the close this study pulls ~30
    calendar days of 5-minute candles per symbol (one request per symbol), and
    for EVERY trading day in that window it:

      1. Takes a snapshot at each PICK TIME (default 09:30 and 10:00 IST):
         % change vs the previous day's close, for every stock.
      2. Keeps the TOP N gainers at that moment (default 20), after dropping
         penny / illiquid names (min price, min traded value so far).
      3. Follows each pick through the rest of that day and measures how it
         behaved: where it closed, how far it ran / dipped after the pick,
         whether it stayed above VWAP, where it closed inside the day's range,
         what volume did afterwards, and how heavy the morning volume was vs
         its own recent average (RVOL).
      4. Gives each pick a plain verdict:
            STRONG    closed at/above its pick price, above VWAP, and spent
                      >=70% of the post-pick bars above VWAP
            HELD      closed above VWAP and kept at least half of its gain
            FADED     still green at the close but gave back most of the gain
                      (or closed below VWAP)
            REVERSED  closed at/below the previous close (gain fully lost)

    Because the data is rebuilt from Dhan's history each run, nothing depends
    on files surviving a redeploy.

OUTPUT (results/)
    gainers.html          page (served at /gainers)
    gainers.json          machine-readable copy of the stats
    gainers_picks.csv     every pick, every day, with all the metrics
    gainers_latest.csv    only the most recent day's picks
    gainers_summary.csv   per stock across days: how often picked, how often it held

Run by hand:   python gainers_study.py          (full study)
               TEST_SYMBOL_LIMIT=20 python gainers_study.py   (quick dry run)
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

log = logging.getLogger("gainers_study")

# ---- settings (all overridable from Render env vars) -----------------------
TOP_N = int(os.environ.get("GAINER_TOP_N", "20") or "20")
PICK_TIMES = [t.strip() for t in os.environ.get("GAINER_PICK_TIMES", "09:30,10:00").split(",") if t.strip()]
HISTORY_DAYS = int(os.environ.get("GAINER_HISTORY_DAYS", "30") or "30")           # calendar days of 5-min data
MIN_PRICE = float(os.environ.get("GAINER_MIN_PRICE", "20") or "20")               # skip penny stocks
MIN_PICK_TURNOVER_CR = float(os.environ.get("GAINER_MIN_TURNOVER_CR", "0.5") or "0.5")  # Rs crore traded by the pick time
RVOL_LOOKBACK_DAYS = 10
RVOL_MIN_PRIOR_DAYS = 3

# Dhan timestamps are UTC; NSE 09:15 IST = 03:45 UTC. Work in "minute of day, UTC".
IST_OFFSET_MIN = 330
SESSION_START_MIN = 9 * 60 + 15 - IST_OFFSET_MIN     # first 5-min bar starts 09:15 IST
SESSION_END_MIN = 15 * 60 + 30 - IST_OFFSET_MIN      # bars start before 15:30 IST
LAST_BAR_START_MIN = SESSION_END_MIN - 5             # 15:25 IST bar
CHECKPOINTS_IST = (11, 12, 13, 14)                   # hourly "where is it now" columns

STRONG_MIN_ABOVE_VWAP_PCT = 70.0
HELD_MIN_RETENTION = 0.5


def _ist_hhmm_to_min(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m) - IST_OFFSET_MIN


def _fmt_ist(min_utc: int) -> str:
    t = min_utc + IST_OFFSET_MIN
    return f"{t // 60:02d}:{t % 60:02d}"


def _verdict(pct_pick, pct_close, close_vs_vwap_pct, pct_above_vwap, close_price, pick_price) -> str:
    if pct_close <= 0:
        return "REVERSED"
    if close_price >= pick_price and close_vs_vwap_pct > 0 and pct_above_vwap >= STRONG_MIN_ABOVE_VWAP_PCT:
        return "STRONG"
    if close_vs_vwap_pct > 0 and pct_pick > 0 and (pct_close / pct_pick) >= HELD_MIN_RETENTION:
        return "HELD"
    return "FADED"


# ---------------------------------------------------------------------------
# Per-symbol analysis (pure function of a 5-min DataFrame - easy to test)
# ---------------------------------------------------------------------------

def analyse_symbol(sym: str, df5: pd.DataFrame) -> list:
    """One row per (trading day, pick time) for this stock, with its
    behaviour after the pick. df5: UTC-naive DatetimeIndex, open/high/low/
    close/volume, 5-minute bars. The first day in the data is only used as
    the 'previous close' for the second."""
    if df5 is None or df5.empty:
        return []
    mins_all = (df5.index.hour * 60 + df5.index.minute).to_numpy()
    keep = (mins_all >= SESSION_START_MIN) & (mins_all < SESSION_END_MIN)
    df5 = df5[keep]
    if df5.empty:
        return []
    dates = np.array([d for d in df5.index.date])
    frames = {}
    for d in sorted(set(dates)):
        frames[d] = df5[dates == d].sort_index()
    day_list = sorted(frames)
    if len(day_list) < 2:
        return []

    pick_mins = {t: _ist_hhmm_to_min(t) for t in PICK_TIMES}

    # volume traded before each pick time, per day (for RVOL)
    vol_till = {t: {} for t in PICK_TIMES}
    for d in day_list:
        f = frames[d]
        m = (f.index.hour * 60 + f.index.minute).to_numpy()
        for t, tm in pick_mins.items():
            vol_till[t][d] = float(f["volume"].to_numpy()[m < tm].sum())

    rows = []
    for k in range(1, len(day_list)):
        d = day_list[k]
        prev_close = float(frames[day_list[k - 1]]["close"].iloc[-1])
        if prev_close <= 0:
            continue
        f = frames[d]
        m = (f.index.hour * 60 + f.index.minute).to_numpy()
        o = f["open"].to_numpy(); h = f["high"].to_numpy(); l = f["low"].to_numpy()
        c = f["close"].to_numpy(); v = f["volume"].to_numpy()
        complete = bool(m[-1] >= LAST_BAR_START_MIN)

        typ = (h + l + c) / 3.0
        cum_v = np.cumsum(v)
        vwap = np.where(cum_v > 0, np.cumsum(typ * v) / np.where(cum_v > 0, cum_v, 1), typ)

        day_high = float(h.max()); day_low = float(l.min())
        close_price = float(c[-1])
        pct_close = (close_price / prev_close - 1) * 100
        rng = day_high - day_low
        close_in_range = (close_price - day_low) / rng if rng > 0 else None
        high_time = _fmt_ist(int(m[int(np.argmax(h))]))
        turnover_cr = float((c * v).sum()) / 1e7

        checkpoints = {}
        for hh in CHECKPOINTS_IST:
            cm = hh * 60 - IST_OFFSET_MIN
            sel = m < cm
            if sel.any() and (m[-1] + 5) >= cm:
                checkpoints[f"pct_{hh}00"] = round((float(c[sel][-1]) / prev_close - 1) * 100, 2)
            else:
                checkpoints[f"pct_{hh}00"] = None

        for t, tm in pick_mins.items():
            before = m < tm
            after = ~before
            if before.sum() < 2 or not after.any():
                continue
            pick_price = float(c[before][-1])
            pct_pick = (pick_price / prev_close - 1) * 100
            pick_turnover_cr = float((c[before] * v[before]).sum()) / 1e7

            prior = [vol_till[t][dd] for dd in day_list[max(0, k - RVOL_LOOKBACK_DAYS):k] if vol_till[t][dd] > 0]
            rvol = (vol_till[t][d] / float(np.mean(prior))) if (len(prior) >= RVOL_MIN_PRIOR_DAYS and vol_till[t][d] > 0) else None

            high_after = (float(h[after].max()) / pick_price - 1) * 100
            dip_after = (float(l[after].min()) / pick_price - 1) * 100
            vwap_close = float(vwap[-1])
            close_vs_vwap = (close_price / vwap_close - 1) * 100 if vwap_close > 0 else 0.0
            pct_above_vwap = float((c[after] > vwap[after]).mean()) * 100

            vb = v[before]; va = v[after]
            vol_ratio = (float(va.mean()) / float(vb.mean())) if vb.mean() > 0 else None
            up_vol_pct = (float(va[c[after] >= o[after]].sum()) / float(va.sum()) * 100) if va.sum() > 0 else None

            rows.append({
                "date": str(d), "pick_time": t, "symbol": sym,
                "prev_close": round(prev_close, 2),
                "open_gap_pct": round((float(o[0]) / prev_close - 1) * 100, 2),
                "pick_price": round(pick_price, 2), "pct_at_pick": round(pct_pick, 2),
                "rvol_at_pick": None if rvol is None else round(rvol, 2),
                "pick_turnover_cr": round(pick_turnover_cr, 2),
                **checkpoints,
                "pct_at_close": round(pct_close, 2),
                "retention": round(pct_close / pct_pick, 2) if pct_pick > 0 else None,
                "high_after_pick_pct": round(high_after, 2), "worst_dip_after_pick_pct": round(dip_after, 2),
                "day_high_pct": round((day_high / prev_close - 1) * 100, 2), "high_time_ist": high_time,
                "close_vs_vwap_pct": round(close_vs_vwap, 2), "bars_above_vwap_after_pick_pct": round(pct_above_vwap, 1),
                "close_in_day_range": None if close_in_range is None else round(close_in_range, 2),
                "volume_after_vs_before": None if vol_ratio is None else round(vol_ratio, 2),
                "up_volume_after_pct": None if up_vol_pct is None else round(up_vol_pct, 1),
                "day_turnover_cr": round(turnover_cr, 2),
                "complete_day": complete,
                "verdict": _verdict(pct_pick, pct_close, close_vs_vwap, pct_above_vwap, close_price, pick_price),
            })
    return rows


# ---------------------------------------------------------------------------
# Ranking + statistics
# ---------------------------------------------------------------------------

def pick_top_gainers(all_rows: list, top_n: int = None) -> pd.DataFrame:
    """For each (date, pick_time): keep liquid, non-penny stocks, rank by
    % change at the pick and take the top N."""
    top_n = top_n or TOP_N
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df = df[(df["pick_price"] >= MIN_PRICE) & (df["pick_turnover_cr"] >= MIN_PICK_TURNOVER_CR) & (df["pct_at_pick"] > 0)]
    if df.empty:
        return df
    df = df.sort_values(["date", "pick_time", "pct_at_pick"], ascending=[True, True, False])
    df["rank"] = df.groupby(["date", "pick_time"]).cumcount() + 1
    return df[df["rank"] <= top_n].reset_index(drop=True)


def _bucket_rvol(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return "<1.5x" if x < 1.5 else ("1.5-3x" if x < 3 else ">=3x")


def _verdict_stats(g: pd.DataFrame) -> dict:
    n = len(g)
    if n == 0:
        return {"picks": 0}
    vc = g["verdict"].value_counts()
    pct = lambda name: round(float(vc.get(name, 0)) / n * 100, 1)
    return {
        "picks": int(n),
        "strong_pct": pct("STRONG"), "held_pct": pct("HELD"), "faded_pct": pct("FADED"), "reversed_pct": pct("REVERSED"),
        "avg_pct_at_pick": round(float(g["pct_at_pick"].mean()), 2),
        "avg_pct_at_close": round(float(g["pct_at_close"].mean()), 2),
        "median_retention": None if g["retention"].dropna().empty else round(float(g["retention"].median()), 2),
        "avg_high_after_pick_pct": round(float(g["high_after_pick_pct"].mean()), 2),
        "avg_worst_dip_after_pick_pct": round(float(g["worst_dip_after_pick_pct"].mean()), 2),
    }


def compute_stats(picks: pd.DataFrame) -> dict:
    """Overall and by-RVOL verdict breakdowns, over COMPLETE days only."""
    out = {"overall": {}, "by_rvol": {}, "days": 0}
    if picks.empty:
        return out
    done = picks[picks["complete_day"]]
    out["days"] = int(done["date"].nunique())
    for t in PICK_TIMES:
        g = done[done["pick_time"] == t]
        out["overall"][t] = _verdict_stats(g)
        buckets = {}
        gb = g.assign(rvol_bucket=g["rvol_at_pick"].map(_bucket_rvol))
        for b in ("<1.5x", "1.5-3x", ">=3x", "n/a"):
            sub = gb[gb["rvol_bucket"] == b]
            if len(sub):
                buckets[b] = _verdict_stats(sub)
        out["by_rvol"][t] = buckets
    return out


def symbol_summary(picks: pd.DataFrame) -> pd.DataFrame:
    if picks.empty:
        return pd.DataFrame()
    done = picks[picks["complete_day"]]
    if done.empty:
        return pd.DataFrame()
    rows = []
    for sym, g in done.groupby("symbol"):
        n = len(g)
        vc = g["verdict"].value_counts()
        rows.append({
            "symbol": sym, "times_in_top_gainers": n, "distinct_days": g["date"].nunique(),
            "strong": int(vc.get("STRONG", 0)), "held": int(vc.get("HELD", 0)),
            "faded": int(vc.get("FADED", 0)), "reversed": int(vc.get("REVERSED", 0)),
            "hold_rate_pct": round((vc.get("STRONG", 0) + vc.get("HELD", 0)) / n * 100, 1),
            "avg_pct_at_pick": round(float(g["pct_at_pick"].mean()), 2),
            "avg_pct_at_close": round(float(g["pct_at_close"].mean()), 2),
            "avg_rvol_at_pick": None if g["rvol_at_pick"].dropna().empty else round(float(g["rvol_at_pick"].mean()), 2),
        })
    return pd.DataFrame(rows).sort_values(["times_in_top_gainers", "hold_rate_pct"], ascending=[False, False]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _work(sym, security_id, segment):
    try:
        df5 = ls.fetch_intraday_history(security_id, segment, 5, HISTORY_DAYS)
        return {"rows": analyse_symbol(sym, df5)}
    except Exception as e:
        return {"error": str(e)}


def run_study():
    if not ls.DHAN_CLIENT_ID or not ls.DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")
    symbols = ls._load_universe_symbols()
    log.info(f"[gainers] studying {len(symbols)} symbols, {HISTORY_DAYS} calendar days of 5-min data, "
             f"picks at {PICK_TIMES} IST, top {TOP_N}...")
    jobs, errors = [], []
    for sym in symbols:
        sid, seg = ls.get_security_id_and_segment(sym)
        if not sid or seg != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        jobs.append((sym, sid, seg))

    all_rows, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=max(1, ls.SCAN_WORKERS)) as ex:
        futures = [(sym, ex.submit(_work, sym, sid, seg)) for sym, sid, seg in jobs]
        for n, (sym, fut) in enumerate(futures, 1):
            res = fut.result()
            if "error" in res:
                errors.append({"symbol": sym, "error": res["error"]})
                log.warning(f"{sym}: {res['error']}")
            else:
                all_rows.extend(res["rows"])
            if n % 100 == 0:
                log.info(f"[gainers] ...{n}/{len(futures)} done, {time.time() - t0:.0f}s elapsed")

    picks = pick_top_gainers(all_rows)
    stats = compute_stats(picks)
    summary = symbol_summary(picks)
    elapsed = time.time() - t0
    log.info(f"[gainers] Done: {len(all_rows)} stock-days analysed, {len(picks)} top-gainer picks over "
             f"{stats.get('days', 0)} complete days, {len(errors)} errors, {elapsed:.0f}s")
    write_results(picks, stats, summary, errors, len(symbols), elapsed)
    return picks, stats


def write_results(picks, stats, summary, errors, universe_size, elapsed):
    os.makedirs(ls.RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(ls.IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    latest_date = None if picks.empty else str(picks["date"].max())
    latest = picks[picks["date"] == latest_date] if latest_date else picks
    payload = {
        "run_timestamp": run_ts, "universe_size": universe_size, "duration_sec": round(elapsed),
        "settings": {"top_n": TOP_N, "pick_times": PICK_TIMES, "history_days": HISTORY_DAYS,
                     "min_price": MIN_PRICE, "min_pick_turnover_cr": MIN_PICK_TURNOVER_CR},
        "stats": stats, "latest_date": latest_date, "error_count": len(errors), "errors": errors[:200],
    }
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "gainers.json"), json.dumps(payload, indent=2, default=str))
    ls._atomic_csv(picks if not picks.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "gainers_picks.csv"))
    ls._atomic_csv(latest if not latest.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "gainers_latest.csv"))
    ls._atomic_csv(summary if not summary.empty else pd.DataFrame(), os.path.join(ls.RESULTS_DIR, "gainers_summary.csv"))
    ls._atomic_write(os.path.join(ls.RESULTS_DIR, "gainers.html"), render_html(payload, latest, summary))
    log.info(f"[gainers] Results written to {ls.RESULTS_DIR}/ (gainers.html, .json, gainers_picks.csv, "
             f"gainers_latest.csv, gainers_summary.csv)")


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def _cls(v):
    return {"STRONG": "strong", "HELD": "held", "FADED": "faded", "REVERSED": "rev"}.get(v, "")


def _num(x, nd=2, suffix=""):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{x:.{nd}f}{suffix}"


def render_html(payload: dict, latest: pd.DataFrame, summary: pd.DataFrame) -> str:
    stats = payload["stats"]
    s = payload["settings"]

    def verdict_table(title, block):
        if not block:
            return ""
        rows = ""
        for label, st in block.items():
            if not st or st.get("picks", 0) == 0:
                continue
            rows += (f"<tr><td class='sym'>{label}</td><td>{st['picks']}</td>"
                     f"<td class='strong'>{st['strong_pct']}%</td><td class='held'>{st['held_pct']}%</td>"
                     f"<td class='faded'>{st['faded_pct']}%</td><td class='rev'>{st['reversed_pct']}%</td>"
                     f"<td>{_num(st['avg_pct_at_pick'], 2, '%')}</td><td>{_num(st['avg_pct_at_close'], 2, '%')}</td>"
                     f"<td>{_num(st['median_retention'], 2, 'x')}</td>"
                     f"<td>{_num(st['avg_high_after_pick_pct'], 2, '%')}</td><td>{_num(st['avg_worst_dip_after_pick_pct'], 2, '%')}</td></tr>")
        if not rows:
            return ""
        return (f"<h2>{title}</h2><table><thead><tr><th></th><th>Picks</th><th>STRONG</th><th>HELD</th><th>FADED</th>"
                f"<th>REVERSED</th><th>Avg % at pick</th><th>Avg % at close</th><th>Median retention</th>"
                f"<th>Avg further high</th><th>Avg worst dip</th></tr></thead><tbody>{rows}</tbody></table>")

    overall_html = verdict_table("Do morning gainers keep their strength? (all complete days)",
                                 {f"Pick at {t} IST": st for t, st in stats.get("overall", {}).items()})
    rvol_html = ""
    for t, buckets in stats.get("by_rvol", {}).items():
        rvol_html += verdict_table(f"By morning volume (RVOL = volume till {t} vs its own recent average) - pick at {t} IST",
                                   {f"RVOL {b}": st for b, st in buckets.items()})
    if not overall_html:
        overall_html = "<div class='empty'>Not enough completed days of data yet.</div>"

    latest_html = ""
    if latest is not None and not latest.empty:
        for t in PICK_TIMES:
            g = latest[latest["pick_time"] == t].sort_values("rank")
            if g.empty:
                continue
            trs = ""
            for _, r in g.iterrows():
                trs += (f"<tr><td>{int(r['rank'])}</td><td class='sym'>{r['symbol']}</td><td>{_num(r['pct_at_pick'], 2, '%')}</td>"
                        f"<td>{_num(r['rvol_at_pick'], 1, 'x')}</td><td>{_num(r.get('pct_1100'), 2, '%')}</td>"
                        f"<td>{_num(r.get('pct_1200'), 2, '%')}</td><td>{_num(r.get('pct_1300'), 2, '%')}</td>"
                        f"<td>{_num(r.get('pct_1400'), 2, '%')}</td><td>{_num(r['pct_at_close'], 2, '%')}</td>"
                        f"<td>{_num(r['high_after_pick_pct'], 2, '%')}</td><td>{_num(r['worst_dip_after_pick_pct'], 2, '%')}</td>"
                        f"<td>{_num(r['close_vs_vwap_pct'], 2, '%')}</td><td>{_num(r['volume_after_vs_before'], 2, 'x')}</td>"
                        f"<td class='{_cls(r['verdict'])} verdict'>{r['verdict']}{'' if r['complete_day'] else ' (day not over)'}</td></tr>")
            latest_html += (f"<h2>{payload['latest_date']} - top {s['top_n']} at {t} IST and how they behaved</h2>"
                            "<table><thead><tr><th>#</th><th>Symbol</th><th>% at pick</th><th>RVOL</th><th>11:00</th><th>12:00</th>"
                            "<th>13:00</th><th>14:00</th><th>Close</th><th>Further high</th><th>Worst dip</th><th>Close vs VWAP</th>"
                            f"<th>Vol after/before</th><th>Verdict</th></tr></thead><tbody>{trs}</tbody></table>")

    repeat_html = ""
    if summary is not None and not summary.empty:
        trs = ""
        for _, r in summary.head(40).iterrows():
            trs += (f"<tr><td class='sym'>{r['symbol']}</td><td>{int(r['times_in_top_gainers'])}</td><td>{int(r['strong'])}</td>"
                    f"<td>{int(r['held'])}</td><td>{int(r['faded'])}</td><td>{int(r['reversed'])}</td>"
                    f"<td>{_num(r['hold_rate_pct'], 1, '%')}</td><td>{_num(r['avg_pct_at_pick'], 2, '%')}</td>"
                    f"<td>{_num(r['avg_pct_at_close'], 2, '%')}</td><td>{_num(r['avg_rvol_at_pick'], 1, 'x')}</td></tr>")
        repeat_html = ("<h2>Stocks that keep showing up (and how often they hold)</h2><table><thead><tr><th>Symbol</th><th>Times picked</th>"
                       "<th>STRONG</th><th>HELD</th><th>FADED</th><th>REVERSED</th><th>Hold rate</th><th>Avg % at pick</th>"
                       f"<th>Avg % at close</th><th>Avg RVOL</th></tr></thead><tbody>{trs}</tbody></table>")

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Morning Gainers - strength study</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  h2 {{ font-size: 16px; margin: 28px 0 8px; }}
  .nav {{ margin-bottom: 16px; font-size: 14px; }}
  .nav a {{ color: #58a6ff; text-decoration: none; margin-right: 18px; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 12px; line-height: 1.5; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 20px; }}
  th, td {{ padding: 7px 9px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; }}
  .sym {{ font-weight: 600; }}
  .verdict {{ font-weight: 700; }}
  .strong {{ color: #3fb950; }} .held {{ color: #8fd19e; }} .faded {{ color: #d29922; }} .rev {{ color: #f85149; }}
  .empty {{ color: #9aa0a6; padding: 20px 0; }}
  tr:hover {{ background: #161b22; }}
</style>
</head>
<body>
  <div class="nav">
    <a href="/scanner">Intraday scan</a><a href="/swing">Swing scan</a>
    <a href="/gainers/picks.csv">All picks CSV</a><a href="/gainers/latest.csv">Latest day CSV</a><a href="/gainers/summary.csv">Per-stock CSV</a>
  </div>
  <h1>Morning gainers - do they keep their strength?</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload['duration_sec']}s) &middot; {payload['universe_size']} stocks &middot;
    {stats.get('days', 0)} complete trading days &middot; top {s['top_n']} by % change vs previous close at {', '.join(s['pick_times'])} IST
    (price &ge; {s['min_price']:g}, &ge; Rs {s['min_pick_turnover_cr']:g} cr traded by the pick time) &middot; {payload['error_count']} errors<br>
    <b class="strong">STRONG</b> closed at/above its pick price, above VWAP, and &ge;70% of the later bars above VWAP &middot;
    <b class="held">HELD</b> closed above VWAP and kept &ge;half its gain &middot;
    <b class="faded">FADED</b> still green but gave most of it back &middot;
    <b class="rev">REVERSED</b> closed at/below the previous close.<br>
    Retention = % at close &divide; % at pick. Further high / worst dip are measured from the pick price. RVOL = volume traded till the pick time &divide; its own average for the same window over the previous 10 days.
  </div>
  {overall_html}
  {rvol_html}
  {latest_html}
  {repeat_html}
</body>
</html>"""


if __name__ == "__main__":
    run_study()
