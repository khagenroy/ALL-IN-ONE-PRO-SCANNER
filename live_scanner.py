"""
Orchestration for the ALL IN ONE PRO (Section A) live scanner: fetches
10-minute candles for every symbol in the universe, runs
all_in_one_scanner.compute_section_a() on each, and writes results - same
general shape as dhan-bridge/eod_scanner.py, but intraday and run on a
schedule during market hours instead of once after close.

THIS HAS NOT BEEN RUN AGAINST THE LIVE DHAN API - same disclosure as the
other two scanners in this project: this sandbox cannot reach api.dhan.co,
so /charts/intraday's exact response shape is unverified here. Dry-run with
TEST_SYMBOL_LIMIT set to a small number first.

USAGE
-----
    python live_scanner.py                  # full scan, writes results/
    TEST_SYMBOL_LIMIT=5 python live_scanner.py   # quick dry run

Needs DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN env vars (same Dhan Data API
credentials dhan-bridge already uses - read-only scope is enough).
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

# Dhan's /charts/intraday supports only 1/5/15/25/60-minute intervals - no
# native 10-minute option, so we pull 5-minute bars and merge consecutive
# pairs into 10-minute bars ourselves (build_10min_bars() below).
INTRADAY_INTERVAL = 5
REQUESTS_PER_SECOND = 4
SECONDS_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND
HTTP_TIMEOUT = 20
MAX_RETRIES = 3

# At 10-min resolution the toolkit's longest lookback (liquidity_len=30,
# confirmed both sides -> needs ~65+ bars warmup) needs only a handful of
# trading days. 20 calendar days gives comfortable margin, well inside
# Dhan's 90-day-per-request cap for intraday data.
HISTORY_DAYS = 20

TEST_SYMBOL_LIMIT = int(os.environ.get("TEST_SYMBOL_LIMIT", "0") or "0")
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", "1000") or "1000")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
MARKET_CAP_CSV = os.path.join(os.path.dirname(__file__), "market_cap_universe.csv")


def dhan_headers():
    return {"access-token": DHAN_ACCESS_TOKEN, "client-id": DHAN_CLIENT_ID, "Content-Type": "application/json"}


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


def fetch_5min_history(security_id: str, exchange_segment: str) -> pd.DataFrame:
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    payload = {
        "securityId": security_id,
        "exchangeSegment": exchange_segment,
        "instrument": "EQUITY",
        "interval": INTRADAY_INTERVAL,
        "fromDate": from_date,
        "toDate": to_date,
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
                log.warning(f"Rate-limited on {security_id}, waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return _parse_intraday(resp.json())
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch intraday history for security_id={security_id}: {last_err}")


def build_10min_bars(df5: pd.DataFrame) -> pd.DataFrame:
    """Merges consecutive PAIRS of 5-minute bars into 10-minute bars,
    grouped separately within each trading day so a day boundary never
    merges two bars from different sessions together. NSE's session (9:15-
    15:30, 375 minutes) isn't an exact multiple of 10, so the very last bar
    of each day may be a lone 5-minute bar - same artifact a real 10-minute
    chart shows for the same reason, not a bug here."""
    if df5.empty:
        return df5
    day = df5.index.date
    # position within each day's own sequence of 5-min bars
    day_change = pd.Series(day).ne(pd.Series(day).shift()).to_numpy()
    day_group_id = day_change.cumsum()
    pos_in_day = pd.Series(range(len(df5))).groupby(day_group_id).cumcount().to_numpy()
    pair_id = day_group_id * 100000 + (pos_in_day // 2)  # unique id per (day, pair)

    g = df5.groupby(pair_id)
    out = pd.DataFrame({
        "open": g["open"].first(),
        "high": g["high"].max(),
        "low": g["low"].min(),
        "close": g["close"].last(),
        "volume": g["volume"].sum(),
    })
    out.index = g.apply(lambda x: x.index[0])
    return out.sort_index()


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


def run_scan():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        log.error("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API. Aborting.")
        sys.exit(1)

    symbols = _load_universe_symbols()
    log.info(f"Live-scanning {len(symbols)} NSE equity symbols on 10-min candles (Section A + Section B)...")

    signals, signals_b, watch, errors = [], [], [], []
    scanned = 0
    t_start = time.time()

    for sym in symbols:
        security_id, segment = get_security_id_and_segment(sym)
        if not security_id or segment != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        try:
            df5 = fetch_5min_history(security_id, segment)
            df10 = build_10min_bars(df5)

            result = sec_a.compute_section_a(df10)
            result_b = sec_b.compute_section_b(df10)
            scanned += 1

            if result:
                result["symbol"] = sym
                if result["signal"]:
                    signals.append(result)
                elif result["ob_watch"]:
                    watch.append(result)

            if result_b and result_b["signal"]:
                result_b["symbol"] = sym
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

    sig_rows = [{"symbol": r["symbol"], "signal": r["signal"], "close": r["close"], "timestamp": r["timestamp"],
                 **(r["levels"] or {})} for r in signals]
    pd.DataFrame(sig_rows).to_csv(os.path.join(RESULTS_DIR, "latest_signals.csv"), index=False)

    sig_b_rows = [{"symbol": r["symbol"], "signal": r["signal"], "source": r["source"], "close": r["close"],
                   "rsi": r.get("rsi"), "timestamp": r["timestamp"], **(r["levels"] or {})} for r in signals_b]
    pd.DataFrame(sig_b_rows).to_csv(os.path.join(RESULTS_DIR, "latest_signals_b.csv"), index=False)

    watch_rows = [{"symbol": r["symbol"], "ob_watch": r["ob_watch"], "close": r["close"], "timestamp": r["timestamp"]}
                  for r in watch]
    pd.DataFrame(watch_rows).to_csv(os.path.join(RESULTS_DIR, "latest_watch.csv"), index=False)

    html = render_html(payload)
    with open(os.path.join(RESULTS_DIR, "latest.html"), "w") as f:
        f.write(html)

    log.info(f"Results written to {RESULTS_DIR}/ (latest.html, latest.json, latest_signals.csv, "
             f"latest_signals_b.csv, latest_watch.csv)")


def render_html(payload: dict) -> str:
    sig_rows_html = ""
    for r in payload["signals"]:
        cls = "buy" if "BUY" in r["signal"] else "sell"
        lv = r["levels"] or {}
        sig_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="{cls} verdict">{r['signal']}</td>
          <td>{r['close']}</td>
          <td>{lv.get('entry','-')}</td>
          <td>{lv.get('sl','-')}</td>
          <td>{lv.get('T1','-')}</td>
          <td>{lv.get('T2','-')}</td>
          <td>{lv.get('T3','-')}</td>
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload["signals"]:
        sig_rows_html = '<tr><td colspan="9" class="empty">No Section A signals this run.</td></tr>'

    watch_rows_html = ""
    for r in payload["watch"]:
        cls = "buy" if "BUY" in r["ob_watch"] else "sell"
        watch_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="{cls} verdict">{r['ob_watch']}</td>
          <td>{r['close']}</td>
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload["watch"]:
        watch_rows_html = '<tr><td colspan="4" class="empty">No live OB zones being watched this run.</td></tr>'

    sig_b_rows_html = ""
    for r in payload.get("signals_b", []):
        cls = "buy" if r["signal"] == "BUY" else "sell"
        lv = r["levels"] or {}
        sig_b_rows_html += f"""
        <tr>
          <td class="sym">{r['symbol']}</td>
          <td class="{cls} verdict">{r['signal']}</td>
          <td>{r['source']}</td>
          <td>{r['close']}</td>
          <td>{lv.get('entry','-')}</td>
          <td>{lv.get('sl','-')}</td>
          <td>{lv.get('T1','-')}</td>
          <td>{lv.get('T2','-')}</td>
          <td>{lv.get('T3','-')}</td>
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
<title>ALL IN ONE PRO Live Scanner</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 24px 0 4px; }}
  h1:first-of-type {{ margin-top: 0; }}
  .meta {{ color: #9aa0a6; font-size: 13px; margin-bottom: 20px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 32px; }}
  th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #262b36; }}
  th {{ background: #161b22; color: #9aa0a6; font-weight: 600; position: sticky; top: 0; }}
  .sym {{ font-weight: 600; }}
  .verdict {{ font-weight: 700; }}
  .buy {{ color: #3fb950; }}
  .sell {{ color: #f85149; }}
  .empty {{ text-align: center; color: #9aa0a6; padding: 40px; }}
  tr:hover {{ background: #161b22; }}
</style>
</head>
<body>
  <h1>ALL IN ONE PRO - Live Signals (Section A: Sweep + Order Block)</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} &middot;
    Scanned {payload['scanned']}/{payload['universe_size']} symbols &middot;
    {payload['signal_count']} signals &middot;
    {payload['watch_count']} OB zones watched &middot;
    {payload['error_count']} errors &middot;
    10-min candles, updates every 10 min during market hours
  </div>
  <table>
    <thead><tr><th>Symbol</th><th>Signal</th><th>Close</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>T3</th><th>Bar Time</th></tr></thead>
    <tbody>{sig_rows_html}</tbody>
  </table>

  <h1>OB Zone Watch (touched, not yet confirmed)</h1>
  <div class="meta">Price is sitting inside a live order-block zone but hasn't closed back through to confirm mitigation yet.</div>
  <table>
    <thead><tr><th>Symbol</th><th>Status</th><th>Close</th><th>Bar Time</th></tr></thead>
    <tbody>{watch_rows_html}</tbody>
  </table>

  <h1>ALL IN ONE PRO - Live Signals (Section B: Keltner/SMC + RSI Pattern)</h1>
  <div class="meta">
    {payload.get('signal_b_count', 0)} signals &middot; "source" is SSL_SWEEP / BSL_SWEEP (liquidity reclaim)
    or RSI_CHECKLIST (RSI + candlestick pattern).
  </div>
  <table>
    <thead><tr><th>Symbol</th><th>Signal</th><th>Source</th><th>Close</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>T3</th><th>RSI</th><th>Bar Time</th></tr></thead>
    <tbody>{sig_b_rows_html}</tbody>
  </table>
</body>
</html>"""


if __name__ == "__main__":
    run_scan()
