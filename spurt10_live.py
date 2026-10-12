"""
VOLUME SPURT 10-MIN SCANNER  (page: /spurt10   csv: /spurt10.csv)

Every 10 minutes (just after each 10-minute candle closes) it ranks the whole NSE cash market by

        volume of the LATEST CLOSED 10-min candle  /  average volume of the PREVIOUS 50 ten-minute candles
        (= the "Volume Multiple vs SMA" idea, with a 50-candle volume moving average)

and shows the TOP 30. Stocks priced below MIN_PRICE (50) are left out.

HOW IT COVERS ALL ~2,700 NSE STOCKS WITHOUT HAMMERING DHAN
  Step 1  3 batched quote calls (marketfeed/quote, up to 1000 stocks each) give every stock's last price and
          today's total volume. Price < 50 is dropped. Comparing today's volume with the previous cycle's
          snapshot shows how much traded in the last 10 minutes for EVERY stock.
  Step 2  A shortlist of the SHORTLIST (default 150) busiest stocks is picked from that.
  Step 3  Only the shortlist has its 5-min candles downloaded (merged to 10-min, closed candles only) so the
          exact ratio vs the 50-candle volume average can be computed and the top 30 ranked.

It is read-only: places no orders, is not connected to Section A / Section B / the bot / RSI Flush, and any
error here is caught and logged without touching the other scans. It uses the same rate limiter as the main scan.

ENV (all optional)
  SPURT10_AUTORUN=true      SPURT10_TOP_N=30        SPURT10_MIN_PRICE=50      SPURT10_SMA_LEN=50
  SPURT10_SHORTLIST=150     SPURT10_MIN_SMA_VOL=1000  (ignore stocks whose 50-candle average is below this many shares)
  SPURT10_WORKERS=4
  SPURT10_ALERT_MULT=3.0    phone alert (ntfy) when a stock's latest 10-min candle volume is >= this many times the 50-candle average
  SPURT10_NTFY=true         switch the phone alert on/off (uses the same NTFY_TOPIC as the other scanner alerts)
  SPURT10_NTFY_MAX_PER_DAY=40   safety cap on phone notifications per day
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import live_scanner as ls
from scrip_master import get_all_nse_equity_symbols, get_security_id_and_segment

log = logging.getLogger("spurt10")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("SPURT10_AUTORUN", "true").strip().lower() == "true"
TOP_N = int(os.environ.get("SPURT10_TOP_N", "30") or "30")
MIN_PRICE = float(os.environ.get("SPURT10_MIN_PRICE", "50") or "50")
SMA_LEN = int(os.environ.get("SPURT10_SMA_LEN", "50") or "50")
SHORTLIST = int(os.environ.get("SPURT10_SHORTLIST", "150") or "150")
MIN_SMA_VOL = float(os.environ.get("SPURT10_MIN_SMA_VOL", "1000") or "1000")
WORKERS = int(os.environ.get("SPURT10_WORKERS", "4") or "4")
ALERT_MULT = float(os.environ.get("SPURT10_ALERT_MULT", "3.0") or "3.0")
NTFY_ON = os.environ.get("SPURT10_NTFY", "true").strip().lower() == "true"
NTFY_MAX_PER_DAY = int(os.environ.get("SPURT10_NTFY_MAX_PER_DAY", "40") or "40")
HISTORY_DAYS = 8            # calendar days of 5-min candles: 50 ten-minute candles need ~3 sessions

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
JSON_PATH = os.path.join(RESULTS_DIR, "spurt10.json")
CSV_PATH = os.path.join(RESULTS_DIR, "spurt10.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt10_history.csv")
HISTORY_ON = os.environ.get("SPURT10_HISTORY", "true").strip().lower() == "true"       # ADDED: full-day log of every cycle
HISTORY_MIN_MULT = float(os.environ.get("SPURT10_HISTORY_MIN_MULT", "0") or "0")      # 0 = keep every row passed in (the top 30)
HTML_PATH = os.path.join(RESULTS_DIR, "spurt10.html")

_universe = None            # list of (symbol, security_id)
_snap = {"date": None, "ts": None, "vol": {}}      # previous cycle's day-volume snapshot


# ---------------------------------------------------------------- universe + quotes
import re
_ETF_SYMBOL = re.compile(r"(BEES|ETF|NIFTY|SENSEX|LIQUID|NASDAQ|MON100|MAFANG|MOM100)")
_ETF_NAME = re.compile(r"(ETF|EXCHANGE TRADED|INDEX FUND|LIQUID|BEES|\bFOF\b|MUTUAL FUND)")
_excluded = []


def _etf_symbols():
    """Symbols to leave out so only real company stocks remain. Uses Dhan's own instrument-type column when it
    has one (ES = equity share) plus a name check for ETFs / index / liquid funds."""
    import scrip_master as sm
    sm._ensure_fresh_cache()
    df = pd.read_csv(sm.CACHE_FILE, dtype=str)
    df = df[(df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & (df["SEM_SERIES"].str.upper() == "EQ")].copy()
    sym = df["SEM_TRADING_SYMBOL"].astype(str).str.strip().str.upper()
    bad = sym.str.contains(_ETF_SYMBOL)
    for col in ("SEM_CUSTOM_SYMBOL", "SEM_SYMBOL_NAME"):
        if col in df.columns:
            bad |= df[col].fillna("").astype(str).str.upper().str.contains(_ETF_NAME)
    if "SEM_EXCH_INSTRUMENT_TYPE" in df.columns:
        t = df["SEM_EXCH_INSTRUMENT_TYPE"].fillna("").astype(str).str.upper().str.strip()
        if (t == "ES").sum() > 500:                       # column is populated: anything not an equity share goes
            bad |= (t != "ES")
    if "SEM_INSTRUMENT_NAME" in df.columns:
        bad |= df["SEM_INSTRUMENT_NAME"].fillna("").astype(str).str.upper().str.contains("ETF|MUTUAL")
    return set(sym[bad])


def _load_universe():
    global _universe, _excluded
    if _universe is None:
        try:
            skip = _etf_symbols()
        except Exception as e:
            log.warning(f"Spurt10: ETF filter could not read the instrument list ({e}) - using the symbol-name check only")
            skip = set()
        out = []
        for sym in get_all_nse_equity_symbols():
            if sym in skip or _ETF_SYMBOL.search(sym):
                _excluded.append(sym)
                continue
            try:
                sid, seg = get_security_id_and_segment(sym)
                if sid and seg == "NSE_EQ":
                    out.append((sym, str(sid)))
            except Exception:
                continue
        _universe = out
        log.info(f"Spurt10 universe: {len(out)} NSE stocks; {len(_excluded)} ETFs/funds left out, e.g. {_excluded[:15]}")
    return _universe


def _fetch_quotes(uni):
    """{symbol: (last_price, day_volume)} via marketfeed/quote, 1000 ids per call, ~1 call/s."""
    by_id = {sid: sym for sym, sid in uni}
    ids = [int(s) for s in by_id]
    out, bad = {}, 0
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": chunk}, f"quote batch {i // 1000 + 1}")
        data = (resp.json().get("data") or {}).get("NSE_EQ") or {}
        for sid, q in data.items():
            sym = by_id.get(str(sid))
            if not sym or not isinstance(q, dict):
                continue
            try:
                px = float(q.get("last_price"))
                vol = float(q.get("volume"))
            except (TypeError, ValueError):
                bad += 1
                continue
            out[sym] = (px, vol)
        time.sleep(1.1)
    if not out:
        raise RuntimeError("marketfeed/quote returned no usable rows (price/volume fields missing?)")
    if bad:
        log.warning(f"Spurt10: {bad} quote rows had no price/volume")
    return out


def _shortlist(quotes, now_ist):
    """Pick the busiest stocks of the last 10 min (or of the day, on the first cycle)."""
    today = now_ist.strftime("%Y-%m-%d")
    if _snap["date"] != today:
        _snap.update(date=today, ts=None, vol={})
    elapsed_bars = max(1.0, ((now_ist.hour * 60 + now_ist.minute) - (9 * 60 + 15)) / 10.0)
    cand = []
    for sym, (px, vol) in quotes.items():
        if px < MIN_PRICE or vol <= 0:
            continue
        prev = _snap["vol"].get(sym)
        if prev is not None and vol >= prev:
            delta = vol - prev
            avg10 = max(prev / max(elapsed_bars - 1.0, 1.0), 1.0)
            cand.append((sym, delta / avg10, delta * px))          # spurt vs today's pace, rupee turnover of the window
        else:
            cand.append((sym, 0.0, vol * px / elapsed_bars))      # no snapshot yet: average turnover per 10 min
    have_snap = bool(_snap["vol"])
    pick = []
    if have_snap:
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[1])[:SHORTLIST]]
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[2])[:max(SHORTLIST // 3, 30)]]
    else:
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[2])[:SHORTLIST]]
    _snap["vol"] = {s: v for s, (p, v) in quotes.items()}
    _snap["ts"] = now_ist.isoformat()
    seen, out = set(), []
    for s in pick:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out, len(cand)


# ---------------------------------------------------------------- candle maths
def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _score_symbol(sym, sid):
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, HISTORY_DAYS)
    df = ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of)
    if df is None or len(df) < SMA_LEN + 1:
        return None
    vol = df["volume"].to_numpy(dtype=float)
    base = vol[-(SMA_LEN + 1):-1].mean()                     # previous 50 candles (current one excluded)
    if base < MIN_SMA_VOL:
        return None
    last = df.iloc[-1]
    day = df.index.map(lambda t: _ist(t).date())
    today_mask = day == day[-1]
    prev_close = df["close"][~today_mask].iloc[-1] if (~today_mask).any() else np.nan
    close = float(last["close"])
    return {
        "_zones": _secA_zones(df),
        "symbol": sym,
        "price": round(close, 2),
        "day_chg_pct": round((close / prev_close - 1) * 100, 2) if prev_close == prev_close else None,
        "candle": _ist(df.index[-1]).strftime("%H:%M"),
        "candle_chg_pct": round((close / float(last["open"]) - 1) * 100, 2) if last["open"] else None,
        "candle_vol": int(vol[-1]),
        "sma_vol": int(round(base)),
        "multiple": round(float(vol[-1] / base), 2),
        "day_vol": int(vol[today_mask].sum()),
    }


# ---------------------------------------------------------------- phone alert
_alerted = {"date": None, "keys": set(), "count": 0}


def _send_alerts(rows, now_ist):
    """ADDED 2026-10-09: one phone notification per cycle listing every stock whose latest closed 10-min candle volume is
    >= ALERT_MULT x its 50-candle average. A stock is announced once per candle (never twice for the same candle), and at most
    NTFY_MAX_PER_DAY notifications a day. Read-only - it only sends a notification."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ON and topic):
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _alerted["date"] != today:
        _alerted.update(date=today, keys=set(), count=0)
    if _alerted["count"] >= NTFY_MAX_PER_DAY:
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and (r["symbol"], r["candle"]) not in _alerted["keys"]]
    if not hits:
        return
    hits.sort(key=lambda r: -r["multiple"])
    lines = []
    for r in hits[:12]:
        chg = r.get("candle_chg_pct")
        lines.append(f"{r['symbol']}  {r['multiple']:.1f}x  Rs {r['price']:.2f}" + (f"  candle {chg:+.2f}%" if chg is not None else ""))
    extra = f"\n+{len(hits) - 12} more on the scanner page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Volume spurt >= {ALERT_MULT:g}x ({hits[0]['candle']} candle)", "Priority": "high",
                                      "Tags": "chart_with_upwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt10 ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Spurt10 ntfy failed: {e}")
        return
    for r in hits:
        _alerted["keys"].add((r["symbol"], r["candle"]))
    _alerted["count"] += 1
    log.info(f"Spurt10 alert sent for {len(hits)} stock(s) at >= {ALERT_MULT:g}x")


# ---------------------------------------------------------------- run + output
def run_once():
    t0 = time.monotonic()
    now_ist = datetime.now(IST)
    uni = _load_universe()
    quotes = _fetch_quotes(uni)
    names, n_pass = _shortlist(quotes, now_ist)
    sid_of = dict(uni)
    rows, failed = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_score_symbol, s, sid_of[s]) for s in names]
        for f in futs:
            try:
                r = f.result()
                if r:
                    rows.append(r)
            except Exception as e:
                failed += 1
                if failed <= 3:
                    log.warning(f"Spurt10 candle fetch failed: {e}")
    rows.sort(key=lambda r: -r["multiple"])
    top = rows[:TOP_N]
    top_ids = {id(x) for x in top}
    for r in rows:                                                  # ADDED: Section A support / resistance zones of each stock
        zs = r.pop("_zones", None)
        if id(r) in top_ids:
            r.update(_zone_fields(zs, r["price"], r.get("candle_chg_pct")))
    try:
        _send_alerts(rows, now_ist)
    except Exception as e:
        log.warning(f"Spurt10 alert step failed: {e}")
    meta = {
        "run": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"), "took_s": round(time.monotonic() - t0),
        "universe": len(uni), "quoted": len(quotes), "price_ok": n_pass, "shortlisted": len(names),
        "scored": len(rows), "failed": failed, "etf_excluded": len(_excluded), "min_price": MIN_PRICE, "sma_len": SMA_LEN, "alert_mult": ALERT_MULT,
    }
    _write(top, meta)
    _append_history(top, now_ist)      # top 30 of the cycle only
    log.info(f"Spurt10 done: {meta}")
    return top, meta


ZONE_NEAR_PCT = float(os.environ.get("SPURT10_ZONE_NEAR_PCT", "0.5") or "0.5")      # "at" a zone = inside it or within this % of it
ZONE_AGAINST_PCT = 1.0                                                                # opposite zone this close ahead = AGAINST


def _secA_zones(df):
    """Live order-block zones from Section A on this stock's 10-min candles: the same zones the ALL IN ONE PRO indicator draws
    (bullish OB = support, bearish OB = resistance). Runs on candles already downloaded - no extra Dhan call."""
    try:
        import all_in_one_scanner as sec_a
        r = sec_a.compute_section_a(df, with_zones=True)
        return (r or {}).get("zones") or []
    except Exception:
        return []


def _zone_fields(zs, price, candle_chg):
    """Nearest support below / resistance above the price and whether the spurt candle's direction agrees with it."""
    out = {"support": "", "support_dist_pct": None, "resistance": "", "resistance_dist_pct": None, "zone_view": "no zone", "zone_src": "", "at_zone": ""}
    try:
        if not zs or not price:
            return out
        out["zone_src"] = "Section A 10m"
        near, ag = ZONE_NEAR_PCT / 100.0, ZONE_AGAINST_PCT / 100.0
        sup = [z for z in zs if z["type"] == "SUPPORT"]
        res = [z for z in zs if z["type"] == "RESISTANCE"]
        zsup = max(sup, key=lambda z: z["hi"]) if sup else None          # nearest support (highest one)
        zres = min(res, key=lambda z: z["lo"]) if res else None          # nearest resistance (lowest one)
        if zsup:
            out["support"] = f"{zsup['lo']:g}-{zsup['hi']:g}"
            out["support_dist_pct"] = round(max(price - zsup["hi"], 0) / price * 100, 2)
        if zres:
            out["resistance"] = f"{zres['lo']:g}-{zres['hi']:g}"
            out["resistance_dist_pct"] = round(max(zres["lo"] - price, 0) / price * 100, 2)
        at_s = zsup is not None and zsup["lo"] * (1 - near) <= price <= zsup["hi"] * (1 + near)
        at_r = zres is not None and zres["lo"] * (1 - near) <= price <= zres["hi"] * (1 + near)
        out["zone_view"] = "between zones" if (zsup or zres) else "no zone"
        if at_s and at_r:                                                 # inside both (very narrow range): the nearer one
            at_s, at_r = (out["support_dist_pct"] <= out["resistance_dist_pct"]), (out["support_dist_pct"] > out["resistance_dist_pct"])
        out["at_zone"] = "SUPPORT" if at_s else "RESISTANCE" if at_r else ""
        if candle_chg is not None and abs(candle_chg) >= 0.05:
            up = candle_chg > 0
            if (up and at_s) or (not up and at_r):
                out["zone_view"] = "ALIGNED"                              # green at support / red at resistance
            elif (up and zres and (at_r or 0 <= (zres["lo"] - price) / price <= ag)) or \
                 (not up and zsup and (at_s or 0 <= (price - zsup["hi"]) / price <= ag)):
                out["zone_view"] = "AGAINST"                              # green into resistance / red onto support
            elif at_s:
                out["zone_view"] = "at support"
            elif at_r:
                out["zone_view"] = "at resistance"
        else:
            out["zone_view"] = "at support" if at_s else "at resistance" if at_r else out["zone_view"]
    except Exception:
        pass
    return out


_hist_seen = {"date": None, "keys": set()}
HIST_COLS = ["date", "run", "candle", "symbol", "price", "day_chg_pct", "candle_chg_pct", "candle_vol", "sma_vol", "multiple", "day_vol",
             "support", "support_dist_pct", "resistance", "resistance_dist_pct", "zone_view", "zone_src", "at_zone"]


def _append_history(rows, now_ist):
    """ADDED: append the top-30 stocks of this cycle to results/spurt10_history.csv, one row per
    (date, 10-min candle, symbol). Read-only logging: never raises, never touches the scan itself."""
    if not HISTORY_ON or not rows:
        return
    try:
        today = now_ist.strftime("%Y-%m-%d")
        if _hist_seen["date"] != today:
            _hist_seen.update(date=today, keys=set())
            if os.path.exists(HIST_PATH):                      # restart in the middle of the day: do not write the same candle twice
                old = pd.read_csv(HIST_PATH, usecols=["date", "candle", "symbol"], dtype=str)
                for d, c, sy in old[old["date"] == today].itertuples(index=False):
                    _hist_seen["keys"].add((c, sy))
        run = now_ist.strftime("%H:%M:%S")
        out = []
        for r in rows:
            k = (r["candle"], r["symbol"])
            if k in _hist_seen["keys"] or r["multiple"] < HISTORY_MIN_MULT:
                continue
            _hist_seen["keys"].add(k)
            out.append({"date": today, "run": run, **{c: r.get(c) for c in HIST_COLS[2:]}})
        if not out:
            return
        os.makedirs(RESULTS_DIR, exist_ok=True)
        new_file = not os.path.exists(HIST_PATH)
        if not new_file:                                   # file written by an older version (fewer columns): upgrade it in place
            with open(HIST_PATH) as fh:
                first = fh.readline().strip().split(",")
            if first != HIST_COLS:
                pd.read_csv(HIST_PATH).reindex(columns=HIST_COLS).to_csv(HIST_PATH + ".tmp", index=False)
                os.replace(HIST_PATH + ".tmp", HIST_PATH)
        pd.DataFrame(out, columns=HIST_COLS).sort_values(["candle", "multiple"], ascending=[True, False]).to_csv(
            HIST_PATH, mode="a", header=new_file, index=False)
    except Exception as e:
        log.warning(f"Spurt10 history write failed: {e}")


def _write(top, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    pd.DataFrame(top).to_csv(CSV_PATH + ".tmp", index=False)
    os.replace(CSV_PATH + ".tmp", CSV_PATH)
    with open(JSON_PATH, "w") as f:
        json.dump({"meta": meta, "rows": top}, f)
    html = _html(top, meta)
    with open(HTML_PATH + ".tmp", "w") as f:
        f.write(html)
    os.replace(HTML_PATH + ".tmp", HTML_PATH)


ROW_BG = {"SUPPORT": "#d4f1dc", "RESISTANCE": "#fadad6"}          # row colour: price is AT a support zone (green) / resistance zone (red)
LEGEND = ('<span style="background:#d4f1dc;padding:2px 8px;border:1px solid #9ccfae">green row = price AT a support zone</span> '
          '<span style="background:#fadad6;padding:2px 8px;border:1px solid #e0a39c">red row = price AT a resistance zone</span> '
          '<span style="background:#ffe066;padding:2px 8px;border:1px solid #d9b800">yellow multiple = phone-alert strength</span>')


def _rowstyle(at):
    return f' style="background:{ROW_BG[at]}"' if at in ROW_BG else ""


def _zc(r, k):
    z = r.get(k) or ""
    d = r.get(k + "_dist_pct")
    if not z:
        return "-"
    return f"{z}<br><small>{'inside/at' if (d or 0) == 0 else f'{d:.2f}% away'}</small>"


def _zh(z, d):
    if not isinstance(z, str) or not z:
        return "-"
    return f"{z}<br><small>{'inside/at' if (d or 0) == 0 else f'{d:.2f}% away'}</small>"


def _zv(v, src=None):
    c = {"ALIGNED": "#0a7d33", "AGAINST": "#c62828", "at support": "#0a7d33", "at resistance": "#c62828"}.get(v or "", "#777")
    lab = f'<br><small>{src} zones</small>' if isinstance(src, str) and src else ""
    return f'<span style="color:{c}"><b>{v or "-"}</b></span>{lab}'


def _html(top, meta):
    def col(v):
        return "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"

    def pct(v):
        return "-" if v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'

    def hl(r):
        return _rowstyle(r.get("at_zone"))

    def mc(r):
        return ' style="background:#ffe066"' if r["multiple"] >= meta.get("alert_mult", 3.0) else ""

    body = "".join(
        f"<tr{hl(r)}><td>{i}</td><td><b>{r['symbol']}</b></td><td>{r['price']:.2f}</td><td>{pct(r['day_chg_pct'])}</td>"
        f"<td>{r['candle']}</td><td>{pct(r['candle_chg_pct'])}</td><td>{r['candle_vol']:,}</td>"
        f"<td>{r['sma_vol']:,}</td><td{mc(r)}><b>{r['multiple']:.2f}x</b></td><td>{r['day_vol']:,}</td>"
        f"<td>{_zc(r, 'support')}</td><td>{_zc(r, 'resistance')}</td><td>{_zv(r.get('zone_view'), r.get('zone_src'))}</td></tr>"
        for i, r in enumerate(top, 1)
    ) or '<tr><td colspan="13">No stocks qualified in this cycle.</td></tr>'
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60"><title>Volume Spurt 10-min</title>
<style>body{{font-family:Arial,sans-serif;margin:12px;background:#fafafa}}table{{border-collapse:collapse;width:100%;background:#fff}}
th,td{{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}}th{{background:#222;color:#fff}}td:nth-child(2){{text-align:left}}
.m{{color:#555;font-size:13px;margin:6px 0 12px}}.w{{overflow-x:auto}}</style></head><body>
<h2>Volume Spurt &mdash; latest 10-min candle vs {meta['sma_len']}-candle volume average (top {len(top)})</h2>
<div class="m">Run: {meta['run']} (took {meta['took_s']}s) &middot; {meta['universe']} NSE stocks (ETFs left out: {meta.get('etf_excluded', 0)}), {meta['price_ok']} priced &ge; {meta['min_price']:.0f},
{meta['shortlisted']} shortlisted, {meta['scored']} scored, {meta['failed']} errors &middot; refreshes every 10 min &middot; {LEGEND} &middot; phone alert at {meta.get('alert_mult', 3.0):g}x or more &middot; zones = ALL IN ONE PRO order-block zones on the 10-min chart (same as the indicator on a 10-min chart) &middot;
<a href="/spurt10.csv">CSV</a> (this cycle) &middot; <a href="/spurt10_history">full-day history page</a> &middot; <a href="/spurt10_history.csv">history CSV</a> &middot; <a href="/scanner">Scanner</a></div>
<div class="w"><table><tr><th>#</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Candle</th><th>Candle %</th>
<th>Candle volume</th><th>{meta['sma_len']}-candle avg vol</th><th>Multiple</th><th>Day volume</th><th>Support zone (10m)</th><th>Resistance zone (10m)</th><th>Zone view</th></tr>{body}</table></div>
</body></html>"""


def history_html(date=None, min_mult=0.0, sym=None, only=None):
    """ADDED: /spurt10_history page - every logged top-30 cycle of one day (default: latest day), with a day picker,
    a minimum-multiple filter and a per-stock summary. Reads results/spurt10_history.csv; never raises."""
    import html as _h
    try:
        if not os.path.exists(HIST_PATH):
            return "<p>No history yet. It starts filling at the first cycle after 09:25 IST on a trading day.</p>"
        df = pd.read_csv(HIST_PATH)
        if df.empty:
            return "<p>No history yet.</p>"
        dates = sorted(df["date"].unique(), reverse=True)
        d = date if date in dates else dates[0]
        for c_ in ("support", "support_dist_pct", "resistance", "resistance_dist_pct", "zone_view", "zone_src", "at_zone"):
            if c_ not in df.columns:                           # rows logged before zones were added
                df[c_] = np.nan
        x = df[df["date"] == d].copy()
        x = x[x["multiple"] >= min_mult]
        if sym:
            x = x[x["symbol"].str.upper() == sym.upper()]
        x["at_zone"] = x["at_zone"].fillna("")
        if only == "zone":
            x = x[x["at_zone"] != ""]
        col = lambda v: "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"
        pct = lambda v: "-" if v != v or v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'
        summ = (x.groupby("symbol").agg(times=("candle", "count"), atz=("at_zone", lambda v: int((v != "").sum())), first=("candle", "min"), last=("candle", "max"),
                                        best=("multiple", "max"), price=("price", "last"), day=("day_chg_pct", "last"))
                .sort_values(["times", "best"], ascending=False))
        srows = "".join(f"<tr><td><b>{_h.escape(str(i))}</b></td><td>{int(r.times)}</td><td>{int(r.atz)}</td><td>{r['first']}</td><td>{r['last']}</td>"
                        f"<td>{r.best:.2f}x</td><td>{r.price:.2f}</td><td>{pct(r.day)}</td></tr>" for i, r in summ.iterrows())
        x = x.sort_values(["candle", "multiple"], ascending=[False, False])
        rows = "".join(f"<tr{_rowstyle(r.at_zone)}><td>{r.candle}</td><td><b>{_h.escape(str(r.symbol))}</b></td><td>{r.price:.2f}</td><td>{pct(r.day_chg_pct)}</td>"
                       f"<td>{pct(r.candle_chg_pct)}</td><td>{int(r.candle_vol):,}</td><td>{int(r.sma_vol):,}</td><td><b>{r.multiple:.2f}x</b></td><td>{int(r.day_vol):,}</td>"
                       f"<td>{_zh(r.support, r.support_dist_pct)}</td><td>{_zh(r.resistance, r.resistance_dist_pct)}</td><td>{_zv(None if r.zone_view != r.zone_view else r.zone_view, None if r.zone_src != r.zone_src else r.zone_src)}</td></tr>"
                       for r in x.itertuples())
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Volume Spurt history</title>
<style>body{{font-family:Arial,sans-serif;margin:12px;background:#fafafa}}table{{border-collapse:collapse;background:#fff;margin-bottom:24px}}
th,td{{border:1px solid #ddd;padding:5px 8px;text-align:right;font-size:14px}}th{{background:#222;color:#fff}}td:nth-child(2){{text-align:left}}.m{{color:#555;font-size:13px;margin:6px 0 12px}}</style></head><body>
<h2>Volume Spurt history &mdash; {_h.escape(d)}</h2>
<form method="get" class="m">Day <select name="date">{opts}</select> &nbsp; min multiple <input name="min" value="{min_mult:g}" size="4"> &nbsp; stock <input name="sym" value="{_h.escape(sym or '')}" size="10"> <label><input type="checkbox" name="only" value="zone"{" checked" if only == "zone" else ""}> only rows at a zone</label> <button>Show</button>
 &middot; {len(x)} rows, {len(summ)} stocks &middot; <a href="/spurt10_history.csv">download CSV (all days)</a> &middot; <a href="/spurt10">live /spurt10</a></form>
<div class="m">{LEGEND}</div>
<h3>Stocks of the day (how many cycles each was in the top 30)</h3>
<table><tr><th>Stock</th><th>Times in top 30</th><th>Times at a zone</th><th>First</th><th>Last</th><th>Best multiple</th><th>Last price</th><th>Day %</th></tr>{srows}</table>
<h3>Every cycle (newest first)</h3>
<table><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Candle %</th><th>Candle vol</th><th>50-candle avg</th><th>Multiple</th><th>Day volume</th><th>Support zone (10m)</th><th>Resistance zone (10m)</th><th>Zone view</th></tr>{rows}</table>
</body></html>"""
    except Exception as e:
        return f"<p>History page error: {_h.escape(str(e))}</p>"


def section_html():
    """Table block for the /scanner page (read from the last saved result; '' if none yet)."""
    try:
        with open(JSON_PATH) as f:
            d = json.load(f)
        full = _html(d["rows"], d["meta"])
        a = full.index("<h2>")
        b = full.index("</body>")
        return '<div style="margin-top:28px">' + full[a:b] + "</div>"
    except Exception:
        return ""


# ---------------------------------------------------------------- loop
def _seconds_to_next_run(now):
    nxt = now.replace(second=20, microsecond=0)
    nxt += timedelta(minutes=(10 - now.minute % 10) % 10)
    if nxt <= now:
        nxt += timedelta(minutes=10)
    return max((nxt - now).total_seconds(), 1)


def _in_session(now):
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return 9 * 60 + 25 <= mins <= 15 * 60 + 40


def loop():
    log.info("Spurt10 autorun started - every 10 min at candle close (09:25-15:40 IST, Mon-Fri).")
    while True:
        try:
            time.sleep(_seconds_to_next_run(datetime.now(IST)))
            if _in_session(datetime.now(IST)):
                run_once()
        except Exception as e:
            log.error(f"Spurt10 cycle failed: {e}")
            time.sleep(30)
