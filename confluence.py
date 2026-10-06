"""
CONFLUENCE - the "leading + lagging" checklist shared by the SMA200 and trendline setups, the backtest
(confluence_study.py) and the live "best setups" page (/bestnow).

A SETUP (SMA200 support/rejection, or a trendline break / touch) is a LAGGING signal: it fires after price has moved.
The checklist adds clues that tend to show up BEFORE a move, all measured on closed candles with no look-ahead:

  V  volume now        - the signal candle's volume >= 1.5x the average of the previous 20 candles
  B  volume build-up   - the 3 candles BEFORE the signal already averaged >= 1.2x that 20-candle average (interest was building)
  S  squeeze           - the last 10 candles' average range <= 0.8x the 50 candles before them (price was coiling)
  H  higher timeframe  - the bigger trend agrees: for 10m / 1H the previous day's daily close is on the right side of a rising
                         (falling) daily SMA200; for 1D the previous week's weekly candle is on the right side of a rising
                         (falling) weekly SMA200. (Weekly setups have no higher timeframe here.)
  C  confluence        - an SMA200 setup AND a trendline setup on the same side on this candle (or the one before, intraday)

Score = how many of the five are true (0-5). Whether the score actually matters is NOT assumed: confluence_study.py backtests
every combination, and only rules that were positive in both halves of the history are written to results/confluence_rules.json.
Live rows that match such a rule are tagged with its tested result.

Not included (not available from the data this scanner already downloads): open interest, delivery %, options data, relative strength.
"""

import os
import json
import threading

import numpy as np
import pandas as pd

VOL_LEN = 20
BUILD_BARS = 3
SQ_RECENT = 10
SQ_BASE = 50

T_VOL = 1.5
T_BUILD = 1.2
T_SQUEEZE = 0.8

CONFL_LOOKBACK = {"10m": 1, "1H": 1, "4H": 1, "1D": 0, "1W": 0}      # bars before the signal bar that still count
HTF_KIND = {"10m": "daily", "1H": "daily", "4H": "daily", "1D": "weekly", "1W": None}
HTF_SMA = 200
HTF_SLOPE_BARS = {"daily": 20, "weekly": 13}

_LOCK = threading.Lock()
_HTF_CACHE = {}          # symbol -> (day, {"daily": table, "weekly": table})
_RULES_CACHE = {"mtime": None, "rules": []}


# ---------------------------------------------------------------------------
# Per-bar features (only data BEFORE the signal bar is used for the baselines)
# ---------------------------------------------------------------------------

def series_features(o, h, l, c, v):
    """Returns (vol_x, vol_build, squeeze) arrays aligned to every bar t.
    vol_x      = v[t] / mean(v[t-20 .. t-1])
    vol_build  = mean(v[t-3 .. t-1]) / mean(v[t-20 .. t-1])
    squeeze    = mean(TR[t-10 .. t-1]) / mean(TR[t-60 .. t-11])      (< 1 = candles shrinking = coiling)"""
    vs = pd.Series(np.asarray(v, float))
    vma = vs.rolling(VOL_LEN, min_periods=VOL_LEN).mean().shift(1)
    b3 = vs.rolling(BUILD_BARS, min_periods=BUILD_BARS).mean().shift(1)
    h = np.asarray(h, float); l = np.asarray(l, float); c = np.asarray(c, float)
    pc = np.concatenate(([c[0]], c[:-1]))
    tr = pd.Series(np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc))))
    recent = tr.rolling(SQ_RECENT, min_periods=SQ_RECENT).mean().shift(1)
    base = tr.rolling(SQ_BASE, min_periods=SQ_BASE).mean().shift(SQ_RECENT + 1)
    with np.errstate(invalid="ignore", divide="ignore"):
        vol_x = np.where(vma.to_numpy() > 0, vs.to_numpy() / vma.to_numpy(), np.nan)
        vol_build = np.where(vma.to_numpy() > 0, b3.to_numpy() / vma.to_numpy(), np.nan)
        squeeze = np.where(base.to_numpy() > 0, recent.to_numpy() / base.to_numpy(), np.nan)
    return vol_x, vol_build, squeeze


# ---------------------------------------------------------------------------
# Higher-timeframe agreement
# ---------------------------------------------------------------------------

def real_dates(idx) -> pd.DatetimeIndex:
    """Dhan daily stamps sit at 18:30 UTC of the previous day (= IST midnight); +5:30 gives the real trading date."""
    idx = pd.DatetimeIndex(idx)
    shifted = (idx.hour == 18) & (idx.minute == 30)
    return pd.DatetimeIndex(np.where(shifted, idx + pd.Timedelta(minutes=330), idx)).normalize()


def _table(close: pd.Series, slope_bars: int):
    sma = close.rolling(HTF_SMA, min_periods=HTF_SMA).mean()
    slope = sma - sma.shift(slope_bars)
    valid = sma.notna() & slope.notna()
    long_ok = np.where(valid, ((close > sma) & (slope > 0)).astype(float), np.nan)
    short_ok = np.where(valid, ((close < sma) & (slope < 0)).astype(float), np.nan)
    return {"dates": close.index.values.astype("datetime64[D]"), "long": long_ok, "short": short_ok}


def htf_tables(df_daily: pd.DataFrame):
    """{'daily': table, 'weekly': table} from a daily OHLC frame (Dhan stamps). Either may be None."""
    out = {"daily": None, "weekly": None}
    try:
        if df_daily is None or df_daily.empty:
            return out
        d = df_daily.copy()
        d.index = real_dates(d.index)
        d = d[~d.index.duplicated(keep="last")].sort_index()
        out["daily"] = _table(d["close"].astype(float), HTF_SLOPE_BARS["daily"])
        wk = d["close"].astype(float).resample("W-FRI").last().dropna()
        out["weekly"] = _table(wk, HTF_SLOPE_BARS["weekly"])
    except Exception:
        pass
    return out


def htf_lookup(tables: dict, tf: str, date_str: str, side: str):
    """1.0 / 0.0 / nan: does the bigger trend agree with `side`, using only periods that ENDED BEFORE date_str."""
    kind = HTF_KIND.get(tf)
    if not kind or not tables or tables.get(kind) is None:
        return np.nan
    tb = tables[kind]
    i = int(np.searchsorted(tb["dates"], np.datetime64(str(date_str)[:10], "D"), side="left")) - 1
    if i < 0:
        return np.nan
    return float(tb["long" if side == "LONG" else "short"][i])


def live_htf(sym: str, tf: str, date_str: str, side: str):
    """Same as htf_lookup for a live row, reading the daily history the swing scan cached on disk (no Dhan call)."""
    try:
        if not HTF_KIND.get(tf):
            return np.nan
        import live_scanner as ls
        from datetime import datetime
        today = datetime.now(ls.IST).strftime("%Y-%m-%d")
        with _LOCK:
            hit = _HTF_CACHE.get(sym)
        if hit is None or hit[0] != today:
            path = os.path.join(ls.DAILY_CACHE_DIR, f"{sym}.csv")
            tabs = {"daily": None, "weekly": None}
            if os.path.exists(path):
                tabs = htf_tables(pd.read_csv(path, index_col=0, parse_dates=True))
            with _LOCK:
                _HTF_CACHE[sym] = (today, tabs)
            hit = (today, tabs)
        return htf_lookup(hit[1], tf, date_str, side)
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# Score + validated rules
# ---------------------------------------------------------------------------

def family_of(row: dict) -> str:
    ev = str(row.get("event", ""))
    if not ev:
        return "SMA200"
    return "TREND_BREAK" if "Breaks" in ev else "TREND_TOUCH"


def _num(x):
    try:
        x = float(x)
        return x if np.isfinite(x) else None
    except Exception:
        return None


def checks(row: dict) -> dict:
    vx, vb, sq = _num(row.get("vol_x")), _num(row.get("vol_build")), _num(row.get("squeeze"))
    return {
        "V": vx is not None and vx >= T_VOL,
        "B": vb is not None and vb >= T_BUILD,
        "S": sq is not None and sq <= T_SQUEEZE,
        "H": _num(row.get("htf_ok")) == 1.0,
        "C": bool(row.get("confl")) is True,
    }


def score_row(row: dict):
    ck = checks(row)
    return sum(ck.values()), "".join(k for k, ok in ck.items() if ok)


def load_rules(results_dir: str):
    path = os.path.join(results_dir, "confluence_rules.json")
    try:
        m = os.path.getmtime(path)
    except Exception:
        return []
    with _LOCK:
        if _RULES_CACHE["mtime"] == m:
            return _RULES_CACHE["rules"]
    try:
        with open(path) as f:
            rules = json.load(f).get("rules", [])
    except Exception:
        rules = []
    with _LOCK:
        _RULES_CACHE.update({"mtime": m, "rules": rules})
    return rules


def rule_matches(rule: dict, row: dict, fam: str) -> bool:
    if rule.get("tf") != row.get("tf") or rule.get("side") != row.get("side") or rule.get("family") != fam:
        return False
    vx, vb, sq = _num(row.get("vol_x")), _num(row.get("vol_build")), _num(row.get("squeeze"))
    if rule.get("min_vol") and not (vx is not None and vx >= rule["min_vol"]):
        return False
    if rule.get("max_squeeze") is not None and not (sq is not None and sq <= rule["max_squeeze"]):
        return False
    if rule.get("min_build") is not None and not (vb is not None and vb >= rule["min_build"]):
        return False
    if rule.get("htf") and _num(row.get("htf_ok")) != 1.0:
        return False
    if rule.get("confl") and not bool(row.get("confl")):
        return False
    return True


def enrich_live(sym: str, sma_rows: list, trend_rows: list, results_dir: str = None):
    """Adds htf_ok, confl, score, score_tags and (if it matches a validated rule) rule / rule_avg_R / rule_trades to this
    symbol's live rows, in place. Never raises."""
    try:
        rules = load_rules(results_dir) if results_dir else []
        allrows = [(r, "SMA200") for r in sma_rows] + [(r, family_of(r)) for r in trend_rows]
        for r, fam in allrows:
            r["htf_ok"] = live_htf(sym, r["tf"], str(r["bar_time"]), r["side"])
            r["htf_ok"] = None if r["htf_ok"] != r["htf_ok"] else r["htf_ok"]        # nan -> None
        for r, fam in allrows:
            L = CONFL_LOOKBACK.get(r["tf"], 0)
            other = trend_rows if fam == "SMA200" else sma_rows
            r["confl"] = any(o["tf"] == r["tf"] and o["side"] == r["side"] and r["bars_ago"] <= o["bars_ago"] <= r["bars_ago"] + L
                             for o in other)
            r["score"], r["score_tags"] = score_row(r)
            best = None
            for rule in rules:
                if rule_matches(rule, r, fam) and (best is None or rule["avg_R"] > best["avg_R"]):
                    best = rule
            if best:
                r["rule"] = best["label"]
                r["rule_avg_R"] = best["avg_R"]
                r["rule_trades"] = best["trades"]
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Live pages: score cell + the combined "best setups now" page
# ---------------------------------------------------------------------------

def _fmt(x, nd=2, suf=""):
    v = _num(x)
    return "-" if v is None else f"{v:.{nd}f}{suf}"


def score_cell(r) -> str:
    """HTML for a score column: '3/5 V S H' (tooltip explains the letters)."""
    sc = _num(r.get("score"))
    if sc is None:
        return "-"
    tags = " ".join(str(r.get("score_tags") or ""))
    return (f"<span title='V volume now, B volume build-up, S squeeze, H higher timeframe agrees, C SMA200 + trendline together'>"
            f"<b>{int(sc)}/5</b> {tags}</span>")


def rule_cell(r) -> str:
    """HTML for the tested-rule column."""
    lab = r.get("rule")
    if not isinstance(lab, str) or not lab:
        return "-"
    return (f"<span class='pos' title=\"{lab}\">&#10003; {_fmt(r.get('rule_avg_R'), 2)}R over {int(_num(r.get('rule_trades')) or 0)} trades</span>")


BEST_FIELDS = ("tf", "symbol", "side", "family", "setup", "bar_time", "close", "score", "score_tags", "rule", "rule_avg_R", "rule_trades",
               "vol_x", "vol_build", "squeeze", "htf_ok", "confl", "stop", "risk_pct", "first_seen")


def update_best(results_dir: str, ist_now):
    """Combined page of the BEST setups right now: SMA200 + trendline setups with at least 3 of the 5 clues, or that match a
    rule validated by confluence_study.py. Reads the two running lists the live modules already maintain. Never raises."""
    try:
        rows = []
        for fn, famfn in (("sma200_now.json", lambda r: "SMA200"), ("trend_now.json", family_of)):
            try:
                with open(os.path.join(results_dir, fn)) as f:
                    store = json.load(f)
            except Exception:
                continue
            for r in store.get("rows", {}).values():
                r = dict(r)
                r["family"] = famfn(r)
                r["setup"] = ("SMA200 support" if r["side"] == "LONG" else "SMA200 rejection") if r["family"] == "SMA200" else r.get("event", "")
                rows.append(r)
        df = pd.DataFrame(rows)
        for col in BEST_FIELDS:
            if col not in df.columns:
                df[col] = None
        now_s = ist_now.strftime("%Y-%m-%d %H:%M:%S")
        today = ist_now.strftime("%Y-%m-%d")
        if not df.empty:
            df["score"] = pd.to_numeric(df["score"], errors="coerce")
            has_rule = df["rule"].apply(lambda x: isinstance(x, str) and bool(x))
            keep = df[(df["score"].fillna(0) >= 3) | has_rule].copy()
            keep["_hr"] = keep["rule"].apply(lambda x: isinstance(x, str) and bool(x)).astype(int)
            keep["_ra"] = pd.to_numeric(keep["rule_avg_R"], errors="coerce").fillna(-9)
            keep["_vx"] = pd.to_numeric(keep["vol_x"], errors="coerce").fillna(0)
            keep = keep.sort_values(["_hr", "_ra", "score", "_vx"], ascending=False)
        else:
            keep = df
        n_rules = len(load_rules(results_dir))
        cpath = os.path.join(results_dir, "best_now.csv")
        tmp = cpath + ".tmp"
        (keep[[c for c in BEST_FIELDS if c in keep.columns]] if not keep.empty else pd.DataFrame()).to_csv(tmp, index=False)
        os.replace(tmp, cpath)
        hpath = os.path.join(results_dir, "best_now.html")
        tmp = hpath + ".tmp"
        with open(tmp, "w") as f:
            f.write(_render_best(keep, now_s, today, n_rules, len(df)))
        os.replace(tmp, hpath)
    except Exception:
        pass


def _render_best(keep: pd.DataFrame, now_s: str, today: str, n_rules: int, n_all: int) -> str:
    def table(d):
        if d.empty:
            return "<div class='empty'>Nothing yet.</div>"
        trs = ""
        for _, r in d.iterrows():
            htf = _num(r.get("htf_ok"))
            trs += (f"<tr class='{'strong' if isinstance(r.get('rule'), str) and r.get('rule') else ''}'><td class='sym'>{r['symbol']}</td><td>{r['tf']}</td>"
                    f"<td class='{'pos' if r['side'] == 'LONG' else 'neg'}'>{r['side']}</td><td>{r['setup']}</td><td>{r['bar_time']}</td><td>{_fmt(r['close'])}</td>"
                    f"<td>{score_cell(r)}</td><td>{rule_cell(r)}</td>"
                    f"<td>{_fmt(r['vol_x'], 1, 'x')}</td><td>{_fmt(r['vol_build'], 1, 'x')}</td><td>{_fmt(r['squeeze'], 2)}</td>"
                    f"<td>{'agrees' if htf == 1.0 else ('against' if htf == 0.0 else '-')}</td><td>{_fmt(r['stop'])}</td><td>{_fmt(r['risk_pct'], 2, '%')}</td>"
                    f"<td>{str(r['first_seen'])[11:16]}</td></tr>")
        return ("<table><thead><tr><th>Symbol</th><th>TF</th><th>Side</th><th>Setup</th><th>Signal candle (IST)</th><th>Close</th><th>Clues (score)</th>"
                "<th>Tested rule</th><th>Volume now</th><th>Volume build-up</th><th>Squeeze</th><th>Higher TF</th><th>Stop</th><th>Risk</th><th>Seen at</th></tr></thead><tbody>"
                + trs + "</tbody></table>")

    if keep.empty:
        body = "<div class='empty'>No setup with 3 or more clues (or matching a tested rule) right now.</div>"
    else:
        intr = keep[keep["tf"].isin(["10m", "1H", "4H"]) & keep["bar_time"].astype(str).str.startswith(today)]
        sw_all = keep[keep["tf"].isin(["1D", "1W"])]
        if sw_all.empty:
            sw = sw_all
        else:
            latest = sw_all.groupby("tf")["bar_time"].transform("max")
            sw = sw_all[sw_all["bar_time"] == latest]
        body = f"<h2>Intraday, today ({len(intr)})</h2>{table(intr)}<h2>Daily / weekly, latest candle ({len(sw)})</h2>{table(sw)}"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>Best setups now</title>
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
  <div class="nav"><a href="/best">What the clues are worth (backtest)</a><a href="/sma200now">All SMA200 setups</a><a href="/trendnow">All trendline setups</a><a href="/scanner">Intraday scan</a><a href="/bestnow.csv">Download CSV</a></div>
  <h1>Best setups now</h1>
  <div class="meta">
    Page time {now_s} IST (refreshes every minute) &middot; {n_all} SMA200 + trendline setups seen, shown here only if they have <b>3 or more of 5 clues</b> or match a <b>tested rule</b> ({n_rules} tested rules available).<br>
    <b>Clues:</b> V volume now &ge; 1.5x the previous 20 candles &middot; B volume build-up (the 3 candles before averaged &ge; 1.2x) &middot; S squeeze (recent candle ranges &le; 0.8x the 50 before) &middot;
    H higher timeframe agrees (10m/1H: previous day's daily SMA200 on the right side and slope; 1D: previous week's weekly SMA200) &middot; C an SMA200 setup and a trendline setup together.<br>
    <b>&#10003; Tested rule</b> = the setup matches a combination that was positive in <b>both halves</b> of the history (average net R and number of trades shown; hover for the full rule). Green rows have one.
    Without a tick, the score is only a checklist - whether it pays is decided by the backtest on <a href="/best" style="color:#58a6ff">/best</a>, which runs every evening. A tick is evidence, not a guarantee.
    Stop = beyond the signal candle (+0.1 ATR). Only closed candles are used.
  </div>
  {body}
</body></html>"""
