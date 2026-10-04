"""
Orchestration for the ALL IN ONE PRO live scanner - MULTI-TIMEFRAME version
(Khagen's request, 2026-10-04 night): runs BOTH Section A and Section B
across SIX timeframes (10m / 1H / 4H / Daily / Weekly / Monthly) for every
symbol in the universe, same general shape as dhan-bridge/eod_scanner.py but
intraday-aware and run on a schedule during market hours.

THIS HAS NOT BEEN RUN AGAINST THE LIVE DHAN API - same disclosure as the
other two scanners in this project: this sandbox cannot reach api.dhan.co,
so /charts/intraday's and /charts/historical's exact response shapes and
per-request date-range caps are unverified here. Dry-run with
TEST_SYMBOL_LIMIT set to a small number first, and if the 60-minute fetch
below (85 calendar days per request) errors or silently truncates, Dhan's
real per-request cap is tighter than assumed - split it into multiple
requests and concatenate.

USAGE
-----
    python live_scanner.py                       # full scan, writes results/
    TEST_SYMBOL_LIMIT=5 python live_scanner.py    # quick dry run

Needs DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN env vars (same Dhan Data API
credentials dhan-bridge already uses - read-only scope is enough).

TIMEFRAME ARCHITECTURE
-----------------------
  10m  - built from a 5-minute intraday pull (merged pairs), ~20 days window.
  1H   - fetched NATIVELY from Dhan (interval=60 IS one of Dhan's supported
         intraday intervals, unlike 10-min), ~85 days window.
  4H   - built by merging groups of 4 of that same 1H pull (no extra call).
  1D   - fetched from /charts/historical, CACHED TO DISK AND REUSED FOR THE
         REST OF THE CALENDAR DAY (see fetch_daily_history_cached). Pulling
         ~7 years of daily history for 1000 symbols on every 10-minute cycle
         would multiply Dhan API load for data that's 99.9% unchanged
         between cycles - the daily candle only moves intraday, and only
         its own last bar. One real daily fetch per symbol per day is enough;
         each 10-min cycle just re-reads the cached CSV (near-instant).
  1W/1M - resampled from that same cached daily history, no extra calls.

  Net result: the SAME number of live Dhan calls per 10-minute cycle as the
  original 10-min-only scanner used to make per symbol (one 5-min pull), PLUS
  one 60-min pull (new) - the heavy daily pull only happens once a day.

Each of the 6 timeframes is independently long enough to let
all_in_one_scanner/section_b's own min-bar checks decide whether that
timeframe has enough history yet for THIS symbol (a recent listing may
simply show nothing on Weekly/Monthly for a while - not a bug).
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from scrip_master import get_security_id_and_segment  # noqa: E402
import all_in_one_scanner as sec_a  # noqa: E402
import section_b as sec_b  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("live_scanner")

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
DHAN_BASE_URL = "https://api.dhan.co/v2"
DHAN_PROXY_URL = os.environ.get("DHAN_PROXY_URL", "").strip()
DHAN_PROXIES = {"https": DHAN_PROXY_URL, "http": DHAN_PROXY_URL} if DHAN_PROXY_URL else None

REQUESTS_PER_SECOND = 4
SECONDS_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND
HTTP_TIMEOUT = 20
MAX_RETRIES = 3

# --- 5-min pull (builds 10m) ---
INTRADAY_5MIN_INTERVAL = 5
INTRADAY_5MIN_HISTORY_DAYS = 20   # plenty for 10-min's warmup needs

# --- 60-min pull (builds 1H natively, 4H by merging groups of 4) ---
INTRADAY_60MIN_INTERVAL = 60
# 4H needs ~240 confirmed 1H bars both sides of liquidity_len=30 -> roughly
# 38 trading days of 1H bars; 85 calendar days gives comfortable margin
# while staying (assumed) under Dhan's per-request intraday cap - UNVERIFIED,
# see module docstring.
INTRADAY_60MIN_HISTORY_DAYS = 85

# --- Daily pull (builds D, and W/M by resampling) - cached, see below ---
# compute_section_a's REAL minimum is max(ZIGZAG_LEN, LIQUIDITY_LEN)*2 +
# VOL_MA_LEN + 10 = max(9,30)*2+20+10 = 90 bars on whatever timeframe it's
# given - including Monthly, i.e. 90 MONTHLY bars needed, not 60 (confirmed
# empirically 2026-10-04: a 2555-day/~7-year pull produced only 84 monthly
# bars after resampling and compute_section_a returned None for every
# symbol on 1M - 6 bars short of the real 90-bar floor). 3650 days (~10
# years, ~120 monthly bars) gives real margin above that. Confirmed Dhan's
# /charts/historical does NOT silently truncate a ~7-year request (returned
# the full requested range), so a 10-year request is expected to behave the
# same way - but re-verify with the same cache-file-length check if this
# ever gets bumped further.
DAILY_HISTORY_DAYS = 3650  # ~10 years
DAILY_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache", "daily")

TEST_SYMBOL_LIMIT = int(os.environ.get("TEST_SYMBOL_LIMIT", "0") or "0")
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", "1000") or "1000")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
MARKET_CAP_CSV = os.path.join(os.path.dirname(__file__), "market_cap_universe.csv")

# Ordered (label, min-dataframe-needed) - drives both the fetch/build step
# and the iteration order used everywhere results are collected/rendered.
TIMEFRAMES = ["10m", "1H", "4H", "1D", "1W", "1M"]


def dhan_headers():
    return {"access-token": DHAN_ACCESS_TOKEN, "client-id": DHAN_CLIENT_ID, "Content-Type": "application/json"}


# ============================================================================
# FETCH - intraday (5-min and 60-min, same endpoint/shape, different interval)
# ============================================================================

def _parse_intraday(resp_json: dict) -> pd.DataFrame:
    """Same defensive key-matching approach as eod_scanner.py's
    _parse_historical() - Dhan's SDK uses slightly different field spellings
    across versions, and this is the one piece unverified against a live
    response from this sandbox."""
    key_map = {
        "open": ["open"], "high": ["high"], "low": ["low"], "close": ["close"],
        "volume": ["volume"], "timestamp": ["timestamp", "start_Time", "startTime"],
    }
    data = {}
    for col, candidates in key_map.items():
        for c in candidates:
            if c in resp_json:
                data[col] = resp_json[c]
                break
    missing = [c for c in ("open", "high", "low", "close", "volume", "timestamp") if c not in data]
    if missing:
        raise ValueError(
            f"Unexpected /charts/intraday response shape - missing {missing}. "
            f"Actual top-level keys: {list(resp_json.keys())}. Adjust key_map in "
            "_parse_intraday() to match the real field names shown here."
        )
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    if df["timestamp"].isna().all():
        df["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    df = df.set_index("timestamp").sort_index()
    return df[["open", "high", "low", "close", "volume"]].astype(float)


def fetch_intraday_history(security_id: str, exchange_segment: str, interval: int, history_days: int) -> pd.DataFrame:
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=history_days)).strftime("%Y-%m-%d")
    payload = {
        "securityId": security_id, "exchangeSegment": exchange_segment, "instrument": "EQUITY",
        "interval": interval, "fromDate": from_date, "toDate": to_date,
    }
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{DHAN_BASE_URL}/charts/intraday",
                headers=dhan_headers(), json=payload, timeout=HTTP_TIMEOUT, proxies=DHAN_PROXIES,
            )
            if resp.status_code == 429:
                wait = 2 * attempt
                log.warning(f"Rate-limited on {security_id} (interval={interval}), waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return _parse_intraday(resp.json())
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch intraday(interval={interval}) history for security_id={security_id}: {last_err}")


# ============================================================================
# FETCH - daily (/charts/historical), disk-cached once per calendar day
# ============================================================================

def _parse_historical(resp_json: dict) -> pd.DataFrame:
    key_map = {
        "open": ["open"], "high": ["high"], "low": ["low"], "close": ["close"],
        "volume": ["volume"], "timestamp": ["timestamp", "start_Time", "startTime"],
    }
    data = {}
    for col, candidates in key_map.items():
        for c in candidates:
            if c in resp_json:
                data[col] = resp_json[c]
                break
    missing = [c for c in ("open", "high", "low", "close", "volume", "timestamp") if c not in data]
    if missing:
        raise ValueError(
            f"Unexpected /charts/historical response shape - missing {missing}. "
            f"Actual top-level keys: {list(resp_json.keys())}. Adjust key_map in "
            "_parse_historical() to match the real field names shown here."
        )
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    if df["timestamp"].isna().all():
        df["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    df = df.set_index("timestamp").sort_index()
    return df[["open", "high", "low", "close", "volume"]].astype(float)


def fetch_daily_history(security_id: str, exchange_segment: str) -> pd.DataFrame:
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=DAILY_HISTORY_DAYS)).strftime("%Y-%m-%d")
    payload = {
        "securityId": security_id, "exchangeSegment": exchange_segment, "instrument": "EQUITY",
        "expiryCode": 0, "oi": False, "fromDate": from_date, "toDate": to_date,
    }
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{DHAN_BASE_URL}/charts/historical",
                headers=dhan_headers(), json=payload, timeout=HTTP_TIMEOUT, proxies=DHAN_PROXIES,
            )
            if resp.status_code == 429:
                wait = 2 * attempt
                log.warning(f"Rate-limited on {security_id} (daily), waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return _parse_historical(resp.json())
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch daily history for security_id={security_id}: {last_err}")


def fetch_daily_history_cached(symbol: str, security_id: str, exchange_segment: str) -> pd.DataFrame:
    """Re-fetches the ~7-year daily history at most once per calendar day
    per symbol, keyed off the cache file's own mtime - everything else
    (every 10-min cycle during the same trading day) reads the cached CSV."""
    os.makedirs(DAILY_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(DAILY_CACHE_DIR, f"{symbol}.csv")
    today_str = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(cache_path):
        mtime_str = datetime.fromtimestamp(os.path.getmtime(cache_path)).strftime("%Y-%m-%d")
        if mtime_str == today_str:
            try:
                cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if not cached.empty:
                    return cached
            except Exception as e:
                log.warning(f"{symbol}: daily cache unreadable ({e}), refetching")
    df = fetch_daily_history(security_id, exchange_segment)
    try:
        df.to_csv(cache_path)
    except Exception as e:
        log.warning(f"{symbol}: could not write daily cache: {e}")
    return df


# ============================================================================
# BAR BUILDERS
# ============================================================================

def build_merged_bars(df: pd.DataFrame, group_size: int) -> pd.DataFrame:
    """Merges consecutive GROUPS of `group_size` bars into one, grouped
    separately within each trading day so a day boundary never merges bars
    from different sessions (10m = pairs of 5-min bars; 4H = groups of 4
    hourly bars). NSE's session length isn't an exact multiple of most group
    sizes, so the last bar of a day may be a short group - same artifact a
    real chart at that timeframe shows, not a bug here."""
    if df.empty:
        return df
    day = df.index.date
    day_change = pd.Series(day).ne(pd.Series(day).shift()).to_numpy()
    day_group_id = day_change.cumsum()
    pos_in_day = pd.Series(range(len(df))).groupby(day_group_id).cumcount().to_numpy()
    pair_id = day_group_id * 100000 + (pos_in_day // group_size)
    g = df.groupby(pair_id)
    out = pd.DataFrame({
        "open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
        "close": g["close"].last(), "volume": g["volume"].sum(),
    })
    out.index = g.apply(lambda x: x.index[0])
    return out.sort_index()


def build_weekly_bars(df_daily: pd.DataFrame) -> pd.DataFrame:
    if df_daily.empty:
        return df_daily
    out = pd.DataFrame({
        "open": df_daily["open"].resample("W-FRI").first(),
        "high": df_daily["high"].resample("W-FRI").max(),
        "low": df_daily["low"].resample("W-FRI").min(),
        "close": df_daily["close"].resample("W-FRI").last(),
        "volume": df_daily["volume"].resample("W-FRI").sum(),
    }).dropna(subset=["open"])
    return out.sort_index()


def build_monthly_bars(df_daily: pd.DataFrame) -> pd.DataFrame:
    if df_daily.empty:
        return df_daily
    out = pd.DataFrame({
        "open": df_daily["open"].resample("ME").first(),
        "high": df_daily["high"].resample("ME").max(),
        "low": df_daily["low"].resample("ME").min(),
        "close": df_daily["close"].resample("ME").last(),
        "volume": df_daily["volume"].resample("ME").sum(),
    }).dropna(subset=["open"])
    return out.sort_index()


def build_all_timeframes(security_id: str, exchange_segment: str, symbol: str) -> dict:
    """Returns {timeframe_label: dataframe} for all 6 timeframes, using the
    minimum number of Dhan calls described in the module docstring."""
    df5 = fetch_intraday_history(security_id, exchange_segment, INTRADAY_5MIN_INTERVAL, INTRADAY_5MIN_HISTORY_DAYS)
    df10 = build_merged_bars(df5, 2)

    df60 = fetch_intraday_history(security_id, exchange_segment, INTRADAY_60MIN_INTERVAL, INTRADAY_60MIN_HISTORY_DAYS)
    df4h = build_merged_bars(df60, 4)

    dfd = fetch_daily_history_cached(symbol, security_id, exchange_segment)
    dfw = build_weekly_bars(dfd)
    dfm = build_monthly_bars(dfd)

    return {"10m": df10, "1H": df60, "4H": df4h, "1D": dfd, "1W": dfw, "1M": dfm}


# ============================================================================
# UNIVERSE
# ============================================================================

def _load_universe_symbols():
    if not os.path.exists(MARKET_CAP_CSV):
        raise RuntimeError(f"{MARKET_CAP_CSV} not found - this scanner needs the same market-cap-ranked "
                            "symbol list as the EOD scanner (copy it over).")
    df = pd.read_csv(MARKET_CAP_CSV, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df = df[df["SERIES"].astype(str).str.strip().str.upper() == "EQ"]
    df = df.sort_values("MARKET CAPITAL (₹ Crores)", ascending=False)
    symbols = df["SYMBOL"].drop_duplicates().tolist()
    if MAX_SYMBOLS > 0:
        symbols = symbols[:MAX_SYMBOLS]
    if TEST_SYMBOL_LIMIT > 0:
        symbols = symbols[:TEST_SYMBOL_LIMIT]
    return symbols


# ============================================================================
# ORCHESTRATION
# ============================================================================

def run_scan():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log.error("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API. Aborting.")
        sys.exit(1)

    symbols = _load_universe_symbols()
    log.info(f"Live-scanning {len(symbols)} NSE equity symbols across {TIMEFRAMES} (Section A + Section B)...")

    signals, signals_b, watch, errors = [], [], [], []
    scanned = 0
    t_start = time.time()

    for sym in symbols:
        security_id, segment = get_security_id_and_segment(sym)
        if not security_id or segment != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        try:
            tf_frames = build_all_timeframes(security_id, segment, sym)
            scanned += 1

            for tf in TIMEFRAMES:
                df = tf_frames.get(tf)
                if df is None or df.empty:
                    continue

                result = sec_a.compute_section_a(df)
                result_b = sec_b.compute_section_b(df)

                if result:
                    result["symbol"] = sym
                    result["timeframe"] = tf
                    if result["signal"]:
                        signals.append(result)
                    elif result["ob_watch"] or result["support_zone"] or result["resistance_zone"]:
                        watch.append(result)

                if result_b and result_b["signal"]:
                    result_b["symbol"] = sym
                    result_b["timeframe"] = tf
                    signals_b.append(result_b)
        except Exception as e:
            errors.append({"symbol": sym, "error": str(e)})
            log.warning(f"{sym}: {e}")

        time.sleep(SECONDS_BETWEEN_REQUESTS)
        if scanned % 100 == 0 and scanned > 0:
            elapsed = time.time() - t_start
            log.info(f"...{scanned}/{len(symbols)} scanned, {len(signals)} Section A signals, "
                     f"{len(signals_b)} Section B signals, {len(watch)} ob-watch so far, {elapsed:.0f}s elapsed")

    elapsed = time.time() - t_start
    log.info(f"Done: {scanned} scanned, {len(signals)} Section A signals, {len(signals_b)} Section B signals, "
             f"{len(watch)} ob-watch, {len(errors)} errors, {elapsed:.0f}s total")

    write_results(signals, signals_b, watch, errors, scanned, len(symbols))
    return signals, signals_b, watch


def write_results(signals, signals_b, watch, errors, scanned, universe_size):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "run_timestamp": run_ts,
        "universe_size": universe_size,
        "scanned": scanned,
        "timeframes": TIMEFRAMES,
        "signal_count": len(signals),
        "signal_b_count": len(signals_b),
        "watch_count": len(watch),
        "error_count": len(errors),
        "signals": signals,
        "signals_b": signals_b,
        "watch": watch,
        "errors": errors,
    }
    with open(os.path.join(RESULTS_DIR, "latest.json"), "w") as f:
        json.dump(payload, f, indent=2)

    def _zone_cols(r, prefix):
        z = r.get(prefix)
        return {f"{prefix}_low": z["low"], f"{prefix}_high": z["high"]} if z else {f"{prefix}_low": None, f"{prefix}_high": None}

    sig_rows = [{"symbol": r["symbol"], "timeframe": r["timeframe"], "signal": r["signal"], "close": r["close"],
                 "timestamp": r["timestamp"], **(r["levels"] or {}),
                 **_zone_cols(r, "support_zone"), **_zone_cols(r, "resistance_zone")} for r in signals]
    pd.DataFrame(sig_rows).to_csv(os.path.join(RESULTS_DIR, "latest_signals.csv"), index=False)

    sig_b_rows = [{"symbol": r["symbol"], "timeframe": r["timeframe"], "signal": r["signal"], "source": r["source"],
                   "close": r["close"], "rsi": r.get("rsi"), "timestamp": r["timestamp"], **(r["levels"] or {})}
                  for r in signals_b]
    pd.DataFrame(sig_b_rows).to_csv(os.path.join(RESULTS_DIR, "latest_signals_b.csv"), index=False)

    watch_rows = [{"symbol": r["symbol"], "timeframe": r["timeframe"], "ob_watch": r["ob_watch"], "close": r["close"],
                   "timestamp": r["timestamp"], **_zone_cols(r, "support_zone"), **_zone_cols(r, "resistance_zone")}
                  for r in watch]
    pd.DataFrame(watch_rows).to_csv(os.path.join(RESULTS_DIR, "latest_watch.csv"), index=False)

    html = render_html(payload)
    with open(os.path.join(RESULTS_DIR, "latest.html"), "w") as f:
        f.write(html)

    log.info(f"Results written to {RESULTS_DIR}/ (latest.html, latest.json, latest_signals.csv, "
             f"latest_signals_b.csv, latest_watch.csv)")


def _zone_txt(z):
    return f"{z['low']} - {z['high']}" if z else "-"


def render_html(payload: dict) -> str:
    sig_rows_html = ""
    for r in payload["signals"]:
        cls = "buy" if "BUY" in r["signal"] else "sell"
        lv = r["levels"] or {}
        sig_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="tf">{r['timeframe']}</td>
          <td class="{cls} verdict">{r['signal']}</td>
          <td>{r['close']}</td>
          <td>{lv.get('entry','-')}</td>
          <td>{lv.get('sl','-')}</td>
          <td>{lv.get('T1','-')}</td>
          <td>{lv.get('T2','-')}</td>
          <td>{lv.get('T3','-')}</td>
          <td>{_zone_txt(r.get('support_zone'))}</td>
          <td>{_zone_txt(r.get('resistance_zone'))}</td>
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload["signals"]:
        sig_rows_html = '<tr><td colspan="12" class="empty">No Section A signals this run.</td></tr>'

    watch_rows_html = ""
    for r in payload["watch"]:
        status = r["ob_watch"] or "OB_ZONE_LIVE"
        cls = "buy" if "BUY" in status else "sell" if "SELL" in status else ""
        watch_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="tf">{r['timeframe']}</td>
          <td class="{cls} verdict">{status}</td>
          <td>{r['close']}</td>
          <td>{_zone_txt(r.get('support_zone'))}</td>
          <td>{_zone_txt(r.get('resistance_zone'))}</td>
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload["watch"]:
        watch_rows_html = '<tr><td colspan="7" class="empty">No live OB zones this run.</td></tr>'

    sig_b_rows_html = ""
    for r in payload.get("signals_b", []):
        cls = "buy" if r["signal"] == "BUY" else "sell"
        lv = r["levels"] or {}
        sig_b_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="tf">{r['timeframe']}</td>
          <td class="{cls} verdict">{r['signal']}</td>
          <td>{r['source']}</td>
          <td>{r['close']}</td>
          <td>{lv.get('entry','-')}</td>
          <td>{lv.get('sl','-')}</td>
          <td>{lv.get('T1','-')}</td>
          <td>{lv.get('T2','-')}</td>
          <td>{r.get('rsi','-')}</td>
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload.get("signals_b"):
        sig_b_rows_html = '<tr><td colspan="11" class="empty">No Section B signals this run.</td></tr>'

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>ALL IN ONE PRO Live Scanner - MTF</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 24px 0 4px; }}
  h1:first-of-type {{ margin-top: 0; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 20px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 32px; }}
  th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; position: sticky; top: 0; }}
  .sym {{ font-weight: 600; }}
  .tf {{ color: #58a6ff; font-weight: 600; }}
  .verdict {{ font-weight: 700; }}
  .buy {{ color: #3fb950; }}
  .sell {{ color: #f85149; }}
  .empty {{ text-align: center; color: #9aa0a6; padding: 40px; }}
  tr:hover {{ background: #161b22; }}
</style>
</head>
<body>
  <h1>ALL IN ONE PRO - Live Signals (Section A: Sweep + Order Block) - All Timeframes</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} &middot;
    Scanned {payload['scanned']}/{payload['universe_size']} symbols &middot;
    Timeframes: {', '.join(payload['timeframes'])} &middot;
    {payload['signal_count']} signals &middot;
    {payload['watch_count']} OB zones live &middot;
    {payload['error_count']} errors &middot;
    10m/1H refresh every cycle, Daily/Weekly/Monthly cached once per day
  </div>
  <table>
    <thead><tr><th>Symbol</th><th>TF</th><th>Signal</th><th>Close</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>T3</th><th>Support Zone</th><th>Resistance Zone</th><th>Bar Time</th></tr></thead>
    <tbody>{sig_rows_html}</tbody>
  </table>

  <h1>OB Zone Watch (live zones, any timeframe)</h1>
  <div class="meta">The nearest still-unbroken order block zone on each side, per symbol/timeframe - a reference level, not a signal.</div>
  <table>
    <thead><tr><th>Symbol</th><th>TF</th><th>Status</th><th>Close</th><th>Support Zone</th><th>Resistance Zone</th><th>Bar Time</th></tr></thead>
    <tbody>{watch_rows_html}</tbody>
  </table>

  <h1>ALL IN ONE PRO - Live Signals (Section B: Keltner/SMC + RSI Pattern) - All Timeframes</h1>
  <div class="meta">
    {payload.get('signal_b_count', 0)} signals &middot; "source" is SSL_SWEEP / BSL_SWEEP (liquidity reclaim)
    or RSI_CHECKLIST (RSI + candlestick pattern).
  </div>
  <table>
    <thead><tr><th>Symbol</th><th>TF</th><th>Signal</th><th>Source</th><th>Close</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>RSI</th><th>Bar Time</th></tr></thead>
    <tbody>{sig_b_rows_html}</tbody>
  </table>
</body>
</html>"""


if __name__ == "__main__":
    run_scan()
