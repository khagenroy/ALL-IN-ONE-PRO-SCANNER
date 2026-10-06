"""
Orchestration for the ALL IN ONE PRO live scanner - TWO SCANS, one codebase
(Khagen's request, 2026-10-05): runs BOTH Section A and Section B, split by
how often their data actually changes.

  INTRADAY scan  (10m / 1H / 4H)   - re-run every 10 minutes during market
                                     hours. Page: /scanner
  SWING scan     (1D / 1W / 1M)    - run ONCE A DAY after the close (and once
                                     to seed on a fresh deploy). Page: /swing

Why split: daily/weekly/monthly signals only change when a daily candle
closes, so re-scanning them every 10 minutes wasted Dhan calls and stretched
a full scan to ~27 min (2026-10-05 live run: 999 symbols, 1614s). The
intraday scan now needs ~2 Dhan calls per symbol per cycle, throttled by one
shared request-rate limiter, so a cycle fits inside 10 minutes.

VERIFIED AGAINST THE LIVE DHAN API: 2026-10-04 (999/1000 symbols, all six
timeframes) and 2026-10-05 (full run after the split of failure handling).
The intraday/swing split itself (this revision) was tested against mocked
Dhan responses only - first live run is the real test. Known live findings:
  - compute_section_a needs 90 bars on WHATEVER timeframe it is given
    (max(9,30)*2 + 20 + 10), so Monthly needs a ~10-year daily pull and 4H
    needs ~165 days of 60-min history.
  - 2026-10-05: Dhan's /charts/historical started returning 400 DH-905
    ("Missing required fields, bad values for parameters") for ~17% of
    symbols (167/999, e.g. RELIANCE, INFY, SUNPHARMA, LICI) on the IDENTICAL
    request that works for others (TCS, HDFCBANK, SBIN) and worked for these
    same symbols the day before. Cause unknown / on Dhan's side. Handling:
    use that symbol's older cached daily history if any, otherwise skip it in
    the swing scan (counted as "no daily data", not an error). Intraday is
    unaffected - it never calls the daily endpoint.

USAGE
-----
    python live_scanner.py                  # intraday scan (default)
    python live_scanner.py swing            # swing scan
    python live_scanner.py both             # intraday, then swing
    TEST_SYMBOL_LIMIT=10 python live_scanner.py swing     # quick dry run

Needs DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN env vars (same Dhan Data API
credentials dhan-bridge already uses - read-only scope is enough).

DON'T run a manual full scan from the Shell during market hours while the
service autorun is going: it is a separate process with its own rate
limiter, so the two would add up against Dhan's request limit.

DATA ARCHITECTURE
-----------------
  10m  - 5-minute pull (20 days, 1 call), merged in pairs.
  1H   - 60-minute pull: RECENT chunk (last 85 days, 1 call per cycle) plus an
         OLDER chunk (165..80 days back, cached to disk, re-fetched only when
         the cache is >3 days old - it is historical and does not change).
  4H   - groups of 4 of that same 60-minute data (no extra calls).
  1D   - /charts/historical, ~10 years, cached to disk once per calendar day.
  1W/1M- resampled from that same cached daily history.

REQUEST RATE: one process-wide, SELF-TUNING limiter is applied before every
Dhan call. MAX_REQUESTS_PER_SECOND (default 4) is only the starting rate: each
429 from Dhan slows it down (floor 1.5/s) and a long run of successes speeds it
back up, so it finds Dhan's real limit by itself (a fixed 4/s drew 429s on the
daily endpoint on 2026-10-05). SCAN_WORKERS threads overlap network latency.
"""

import os
import sys
import time
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from scrip_master import get_security_id_and_segment  # noqa: E402
import all_in_one_scanner as sec_a  # noqa: E402
import section_b as sec_b  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("live_scanner")

IST = ZoneInfo("Asia/Kolkata")

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
DHAN_BASE_URL = "https://api.dhan.co/v2"
DHAN_PROXY_URL = os.environ.get("DHAN_PROXY_URL", "").strip()
DHAN_PROXIES = {"https": DHAN_PROXY_URL, "http": DHAN_PROXY_URL} if DHAN_PROXY_URL else None

MAX_REQUESTS_PER_SECOND = float(os.environ.get("MAX_REQUESTS_PER_SECOND", "4") or "4")
SCAN_WORKERS = int(os.environ.get("SCAN_WORKERS", "4") or "4")
HTTP_TIMEOUT = 20
MAX_RETRIES = 3

# --- 5-min pull (builds 10m) ---
INTRADAY_5MIN_INTERVAL = 5
INTRADAY_5MIN_HISTORY_DAYS = 20   # plenty for 10-min's warmup needs

# --- 60-min pull (builds 1H natively, 4H by merging groups of 4) ---
# compute_section_a's REAL minimum is 90 bars on whatever timeframe it is
# given (see module docstring), 4H included. One 85-day request gave only
# ~85-90 four-hour bars (right on the floor, ~23% of symbols fell short on
# 2026-10-04), so the window is two chunks: RECENT (fetched every cycle) and
# OLDER (cached - see fetch_60min_history). The two overlap by ~5 days; the
# duplicates are dropped, keeping the recent copy. Each chunk is <=85 days
# (Dhan's per-request cap on /charts/intraday is assumed ~90 days, UNVERIFIED).
INTRADAY_60MIN_INTERVAL = 60
H60_RECENT_DAYS = 85
H60_OLD_FROM_DAYS = 165
H60_OLD_TO_DAYS = 80
H60_OLD_REFRESH_DAYS = 3          # re-fetch the older chunk only when cache is older than this
H60_OLD_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache", "h60_old")

# --- Daily pull (builds D, and W/M by resampling) - cached once per day ---
# 90 MONTHLY bars needed => ~10 years (confirmed 2026-10-04: 7 years gave 84).
DAILY_HISTORY_DAYS = 3650  # ~10 years
DAILY_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache", "daily")

TEST_SYMBOL_LIMIT = int(os.environ.get("TEST_SYMBOL_LIMIT", "0") or "0")
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", "1000") or "1000")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
MARKET_CAP_CSV = os.path.join(os.path.dirname(__file__), "market_cap_universe.csv")

INTRADAY_TFS = ["10m", "1H", "4H"]
SWING_TFS = ["1D", "1W", "1M"]

MODES = {
    "intraday": {
        "timeframes": INTRADAY_TFS,
        "prefix": "latest",
        "title": "Intraday (10m / 1H / 4H)",
        "page": "/scanner",
        "note": "Re-scanned every 10 minutes during market hours (9:15-15:30 IST)",
        "other_page": "/swing",
        "other_title": "Swing / Positional (1D / 1W / 1M)",
        "csv_a": "/scanner/signals.csv",
        "csv_b": "/scanner/signals_b.csv",
    },
    "swing": {
        "timeframes": SWING_TFS,
        "prefix": "latest_swing",
        "title": "Swing / Positional (1D / 1W / 1M)",
        "page": "/swing",
        "note": "Scanned once a day after the close (around 16:00 IST)",
        "other_page": "/scanner",
        "other_title": "Intraday (10m / 1H / 4H)",
        "csv_a": "/swing/signals.csv",
        "csv_b": "/swing/signals_b.csv",
    },
}


class DailyDataUnavailable(Exception):
    """Dhan's daily history could not be fetched and no cached copy exists."""


def dhan_headers():
    return {"access-token": DHAN_ACCESS_TOKEN, "client-id": DHAN_CLIENT_ID, "Content-Type": "application/json"}


# ============================================================================
# REQUEST-RATE LIMITER (shared by every Dhan call in this process)
# ============================================================================

class _RateLimiter:
    """Spaces request START times at least `interval` seconds apart across all
    threads. SELF-TUNING: every 429 from Dhan widens the interval (x1.5, down
    to a floor of MIN_REQUESTS_PER_SECOND) and pauses all threads briefly;
    a long run of successes narrows it back toward the configured rate. So
    MAX_REQUESTS_PER_SECOND is only the starting point - the limiter finds
    Dhan's real limit itself (2026-10-05: a fixed 4/s drew 429s on the daily
    endpoint, so the true limit is lower than that, or shared with other
    traffic on the same Dhan account)."""

    RECOVER_AFTER_OK = 60      # consecutive successes before speeding back up
    SLOW_FACTOR = 1.5
    FAST_FACTOR = 1.15
    BURST_WINDOW = 2.0         # seconds: 429s closer together than this count as one burst

    def __init__(self, rps: float, min_rps: float = 1.5):
        self.base_interval = 1.0 / max(rps, 0.1)
        self.max_interval = 1.0 / max(min(min_rps, rps), 0.1)
        self.interval = self.base_interval
        self._lock = threading.Lock()
        self._next = 0.0
        self._ok_streak = 0
        self._last_throttle = -1e9

    def wait(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        delay = start - now
        if delay > 0:
            time.sleep(delay)

    def throttle(self):
        """Called on every 429. Widens the interval ONCE per burst: several
        requests in flight at the same moment all get their 429 together, and
        counting each one separately would crash the rate straight to the floor."""
        with self._lock:
            now = time.monotonic()
            self._ok_streak = 0
            if now - self._last_throttle >= self.BURST_WINDOW:
                new = min(self.interval * self.SLOW_FACTOR, self.max_interval)
                if new > self.interval:
                    log.warning(f"Dhan rate limit hit - slowing request rate to {1.0 / new:.1f}/s")
                self.interval = new
                self._last_throttle = now
            # brief global pause so in-flight threads don't keep hammering
            self._next = max(self._next, now + self.interval * 2)

    def success(self):
        with self._lock:
            self._ok_streak += 1
            if self._ok_streak >= self.RECOVER_AFTER_OK and self.interval > self.base_interval:
                self.interval = max(self.base_interval, self.interval / self.FAST_FACTOR)
                self._ok_streak = 0


_limiter = _RateLimiter(MAX_REQUESTS_PER_SECOND)


# ============================================================================
# SHARED DHAN POST (rate limiting + 429 backoff + retries)
# ============================================================================

MAX_429_RETRIES = 6   # 429s don't use up normal retry attempts; waits 2,4,6,... s


def _dhan_post(path: str, payload: dict, label: str):
    """POST to Dhan through the shared limiter. Returns the 2xx Response.
    Raises RuntimeError: "400 from Dhan: ..." for a 400 (deterministic -
    never retried), "rate-limited (429) ..." if Dhan keeps returning 429,
    otherwise the last error after MAX_RETRIES attempts."""
    last_err = None
    rl_hits = 0
    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            _limiter.wait()
            resp = requests.post(
                f"{DHAN_BASE_URL}/{path}",
                headers=dhan_headers(), json=payload, timeout=HTTP_TIMEOUT, proxies=DHAN_PROXIES,
            )
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
            continue
        if resp.status_code == 429:
            rl_hits += 1
            _limiter.throttle()
            if rl_hits <= MAX_429_RETRIES:
                log.warning(f"Rate-limited on {label}, waiting {2 * rl_hits}s (429 #{rl_hits}/{MAX_429_RETRIES})")
                time.sleep(2 * rl_hits)
                attempt -= 1          # a 429 is not a failed attempt
                continue
            raise RuntimeError(f"rate-limited (429) {rl_hits} times on {label}")
        if resp.status_code == 400:
            raise RuntimeError(f"400 from Dhan: {resp.text[:150]}")
        if resp.status_code >= 400:
            last_err = f"HTTP {resp.status_code}"
            time.sleep(1.5 * attempt)
            continue
        _limiter.success()
        return resp
    raise RuntimeError(f"{last_err}")


# ============================================================================
# FETCH - intraday (5-min and 60-min, same endpoint/shape, different interval)
# ============================================================================

def _parse_intraday(resp_json: dict) -> pd.DataFrame:
    """Same defensive key-matching approach as eod_scanner.py's
    _parse_historical() - Dhan's SDK uses slightly different field spellings
    across versions."""
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


def _fetch_intraday_chunk(security_id: str, exchange_segment: str, interval: int, from_date: str, to_date: str) -> pd.DataFrame:
    payload = {
        "securityId": security_id, "exchangeSegment": exchange_segment, "instrument": "EQUITY",
        "interval": interval, "fromDate": from_date, "toDate": to_date,
    }
    try:
        resp = _dhan_post("charts/intraday", payload, f"{security_id} (interval={interval})")
        return _parse_intraday(resp.json())
    except RuntimeError as e:
        raise RuntimeError(f"Failed to fetch intraday(interval={interval}) chunk {from_date}..{to_date} "
                           f"for security_id={security_id}: {e}")


def fetch_intraday_history(security_id: str, exchange_segment: str, interval: int, history_days: int) -> pd.DataFrame:
    """Single-request fetch (used for the 5-min/10m pull - 20 days, well
    under any plausible per-request cap)."""
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=history_days)).strftime("%Y-%m-%d")
    return _fetch_intraday_chunk(security_id, exchange_segment, interval, from_date, to_date)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def _load_h60_old(symbol: str, security_id: str, exchange_segment: str, now: datetime):
    """The OLDER 60-min chunk (H60_OLD_FROM_DAYS..H60_OLD_TO_DAYS days back).
    It is historical data that does not change, so it is cached to disk and
    re-fetched only when the cache is older than H60_OLD_REFRESH_DAYS (the
    ~5-day overlap with the recent chunk keeps the seam gap-free until then).
    Returns None if nothing is available (4H then has fewer bars)."""
    os.makedirs(H60_OLD_CACHE_DIR, exist_ok=True)
    path = os.path.join(H60_OLD_CACHE_DIR, f"{symbol}.csv")
    if os.path.exists(path):
        age_days = (time.time() - os.path.getmtime(path)) / 86400.0
        if age_days < H60_OLD_REFRESH_DAYS:
            try:
                cached = pd.read_csv(path, index_col=0, parse_dates=True)
                if not cached.empty:
                    return cached
            except Exception as e:
                log.warning(f"{symbol}: 60-min cache unreadable ({e}), refetching")
    fmt = "%Y-%m-%d"
    try:
        old = _fetch_intraday_chunk(
            security_id, exchange_segment, INTRADAY_60MIN_INTERVAL,
            (now - timedelta(days=H60_OLD_FROM_DAYS)).strftime(fmt),
            (now - timedelta(days=H60_OLD_TO_DAYS)).strftime(fmt),
        )
        if not old.empty:
            try:
                old.to_csv(path)
            except Exception as e:
                log.warning(f"{symbol}: could not write 60-min cache: {e}")
            return old
    except Exception as e:
        log.warning(f"{symbol}: older 60-min chunk fetch failed ({e})")
    if os.path.exists(path):  # stale is better than nothing
        try:
            stale = pd.read_csv(path, index_col=0, parse_dates=True)
            if not stale.empty:
                return stale
        except Exception:
            pass
    return None


def fetch_60min_history(symbol: str, security_id: str, exchange_segment: str) -> pd.DataFrame:
    """RECENT chunk (live, every cycle: 1 call) + OLDER chunk (cached), stitched
    and de-duplicated keeping the recent copy of any overlapping bar."""
    now = datetime.now()
    fmt = "%Y-%m-%d"
    recent = _fetch_intraday_chunk(
        security_id, exchange_segment, INTRADAY_60MIN_INTERVAL,
        (now - timedelta(days=H60_RECENT_DAYS)).strftime(fmt), now.strftime(fmt),
    )
    old = _load_h60_old(symbol, security_id, exchange_segment, now)
    parts = [d for d in (old, recent) if d is not None and not d.empty]
    if not parts:
        return _empty_frame()
    out = pd.concat(parts).sort_index(kind="stable")
    return out[~out.index.duplicated(keep="last")]


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
    try:
        # A 400 here is deterministic (2026-10-05: DH-905 for ~17% of symbols on
        # a request identical to ones that succeed) - _dhan_post never retries it.
        resp = _dhan_post("charts/historical", payload, f"{security_id} (daily)")
        return _parse_historical(resp.json())
    except RuntimeError as e:
        raise RuntimeError(f"Failed to fetch daily history for security_id={security_id}: {e}")


def fetch_daily_history_cached(symbol: str, security_id: str, exchange_segment: str) -> pd.DataFrame:
    """Re-fetches the ~10-year daily history at most once per calendar day
    per symbol, keyed off the cache file's own mtime. If Dhan refuses the
    refresh, falls back to ANY older cached copy; raises if there is none."""
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
    try:
        df = fetch_daily_history(security_id, exchange_segment)
    except Exception as e:
        if os.path.exists(cache_path):
            try:
                stale = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if not stale.empty:
                    log.warning(f"{symbol}: daily refresh failed ({e}); using stale cached history")
                    return stale
            except Exception:
                pass
        raise
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


SESSION_END_UTC_HOUR = 10          # NSE closes 15:30 IST = 10:00 UTC (Dhan timestamps are UTC)
BAR_CLOSE_GRACE_SECONDS = 5        # let Dhan finalise a bar a few seconds after it closes


def drop_forming_bars(df: pd.DataFrame, bar_minutes: int, as_of: pd.Timestamp) -> pd.DataFrame:
    """Keeps only CLOSED bars. The Pine script only fires on a confirmed bar
    (barstate.isconfirmed), so the scanner must not evaluate a candle that is
    still forming - its close/high/low keep changing and a signal on it can
    vanish by the time the bar closes. A bar is closed once its end time has
    passed; the last bar of a session ends at 15:30 IST even if shorter than
    the timeframe (e.g. the 15:15 hourly bar)."""
    if df.empty:
        return df
    idx = df.index
    ends = (idx + pd.Timedelta(minutes=bar_minutes)).to_numpy()
    session_end = (idx.normalize() + pd.Timedelta(hours=SESSION_END_UTC_HOUR)).to_numpy()
    ends = pd.DatetimeIndex(np.minimum(ends, session_end))
    closed = (ends + pd.Timedelta(seconds=BAR_CLOSE_GRACE_SECONDS)) <= as_of
    return df[closed]


def build_intraday_timeframes(security_id: str, exchange_segment: str, symbol: str) -> dict:
    """{'10m','1H','4H'} - ~2 Dhan calls per symbol per cycle (5-min pull +
    recent 60-min chunk; the older 60-min chunk comes from the disk cache).
    Only CLOSED bars are returned (see drop_forming_bars)."""
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))  # taken BEFORE the fetch
    df5 = fetch_intraday_history(security_id, exchange_segment, INTRADAY_5MIN_INTERVAL, INTRADAY_5MIN_HISTORY_DAYS)
    df60 = fetch_60min_history(symbol, security_id, exchange_segment)
    return {
        "10m": drop_forming_bars(build_merged_bars(df5, 2), 10, as_of),
        "1H": drop_forming_bars(df60, 60, as_of),
        "4H": drop_forming_bars(build_merged_bars(df60, 4), 240, as_of),
    }


def build_swing_timeframes(security_id: str, exchange_segment: str, symbol: str) -> dict:
    """{'1D','1W','1M'} from the cached daily history. Raises
    DailyDataUnavailable if Dhan refuses it and there is no cached copy."""
    try:
        dfd = fetch_daily_history_cached(symbol, security_id, exchange_segment)
    except Exception as e:
        raise DailyDataUnavailable(str(e))
    return {"1D": dfd, "1W": build_weekly_bars(dfd), "1M": build_monthly_bars(dfd)}


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

def _scan_one_symbol(sym: str, security_id: str, segment: str, mode: str) -> dict:
    """Runs in a worker thread. Always returns a dict - never raises."""
    try:
        build = build_intraday_timeframes if mode == "intraday" else build_swing_timeframes
        frames = build(security_id, segment, sym)
    except DailyDataUnavailable as e:
        return {"no_data": str(e)}
    except Exception as e:
        return {"error": str(e)}

    out_a, out_b = [], []
    try:
        for tf in MODES[mode]["timeframes"]:
            df = frames.get(tf)
            if df is None or df.empty:
                continue
            result = sec_a.compute_section_a(df)
            result_b = sec_b.compute_section_b(df)
            if result and result["signal"]:
                result["symbol"] = sym
                result["timeframe"] = tf
                out_a.append(result)
            if result_b and result_b["signal"]:
                result_b["symbol"] = sym
                result_b["timeframe"] = tf
                out_b.append(result_b)
    except Exception as e:
        return {"error": str(e)}

    # SMA200 support / rejection setups on the SAME bars (no extra Dhan calls). Separate from
    # Section A / B and fully isolated: any failure here is swallowed and never affects the signals above.
    out_sma = []
    try:
        import sma200_live
        for tf in MODES[mode]["timeframes"]:
            df = frames.get(tf)
            if df is not None and not df.empty:
                out_sma.extend(sma200_live.live_setups(df, tf, sym))
    except Exception:
        out_sma = []

    # Trendline setups (same trendlines as the PA Toolkit) on the SAME bars - also fully isolated from Section A / B.
    out_trend = []
    try:
        import trendline_live
        for tf in MODES[mode]["timeframes"]:
            df = frames.get(tf)
            if df is not None and not df.empty:
                out_trend.extend(trendline_live.live_setups(df, tf, sym))
    except Exception:
        out_trend = []
    return {"signals": out_a, "signals_b": out_b, "sma200": out_sma, "trend": out_trend}


def run_scan(mode: str = "intraday"):
    if mode not in MODES:
        raise ValueError(f"unknown scan mode {mode!r} - expected one of {list(MODES)}")
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set - cannot call Dhan's Data API.")

    cfg = MODES[mode]
    symbols = _load_universe_symbols()
    log.info(f"[{mode}] scanning {len(symbols)} NSE equity symbols across {cfg['timeframes']} "
             f"(Section A + Section B), {SCAN_WORKERS} workers, starting at {MAX_REQUESTS_PER_SECOND:g} req/s (self-tuning)...")

    # Resolve security ids up front, single-threaded (the scrip master loads
    # lazily and is not safe to initialise from several threads at once).
    jobs, errors = [], []
    for sym in symbols:
        security_id, segment = get_security_id_and_segment(sym)
        if not security_id or segment != "NSE_EQ":
            errors.append({"symbol": sym, "error": "no NSE_EQ security_id found"})
            continue
        jobs.append((sym, security_id, segment))

    signals, signals_b, no_data = [], [], []
    sma_rows = []
    trend_rows = []
    scanned = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = [(sym, ex.submit(_scan_one_symbol, sym, sid, seg, mode)) for sym, sid, seg in jobs]
        for n, (sym, fut) in enumerate(futures, 1):
            try:
                res = fut.result()
            except Exception as e:  # defensive - _scan_one_symbol should not raise
                res = {"error": str(e)}
            if "error" in res:
                errors.append({"symbol": sym, "error": res["error"]})
                log.warning(f"{sym}: {res['error']}")
            elif "no_data" in res:
                no_data.append(sym)
                log.warning(f"{sym}: no daily data ({res['no_data']}); skipped in the swing scan")
            else:
                scanned += 1
                signals.extend(res["signals"])
                signals_b.extend(res["signals_b"])
                sma_rows.extend(res.get("sma200", []))
                trend_rows.extend(res.get("trend", []))
            if n % 100 == 0:
                log.info(f"[{mode}] ...{n}/{len(futures)} done, {len(signals)} Section A, "
                         f"{len(signals_b)} Section B signals so far, {time.time() - t_start:.0f}s elapsed")

    elapsed = time.time() - t_start
    log.info(f"[{mode}] Done: {scanned} scanned, {len(signals)} Section A signals, {len(signals_b)} Section B signals, "
             f"{len(no_data)} no daily data, {len(errors)} errors, {elapsed:.0f}s total")

    write_results(mode, signals, signals_b, errors, no_data, scanned, len(symbols), elapsed)
    try:
        import sma200_live
        sma200_live.update(RESULTS_DIR, mode, sma_rows, datetime.now(IST), elapsed)
        log.info(f"[{mode}] SMA200 setups: {len(sma_rows)} found this cycle (page /sma200now)")
    except Exception as e:
        log.warning(f"[{mode}] SMA200 setup list not updated: {e}")
    try:
        import trendline_live
        trendline_live.update(RESULTS_DIR, mode, trend_rows, datetime.now(IST), elapsed)
        log.info(f"[{mode}] Trendline setups: {len(trend_rows)} found this cycle (page /trendnow)")
    except Exception as e:
        log.warning(f"[{mode}] Trendline setup list not updated: {e}")
    return signals, signals_b


def _atomic_write(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def _atomic_csv(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def write_results(mode, signals, signals_b, errors, no_data, scanned, universe_size, elapsed=0.0):
    cfg = MODES[mode]
    prefix = cfg["prefix"]
    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S") + " IST"
    payload = {
        "mode": mode,
        "run_timestamp": run_ts,
        "universe_size": universe_size,
        "scanned": scanned,
        "timeframes": cfg["timeframes"],
        "signal_count": len(signals),
        "signal_b_count": len(signals_b),
        "no_data_count": len(no_data),
        "error_count": len(errors),
        "duration_sec": round(elapsed),
        "signals": signals,
        "signals_b": signals_b,
        "no_data": no_data,
        "errors": errors,
    }
    _atomic_write(os.path.join(RESULTS_DIR, f"{prefix}.json"), json.dumps(payload, indent=2, default=str))

    sig_rows = [{"symbol": r["symbol"], "timeframe": r["timeframe"], "signal": r["signal"], "close": r["close"],
                 "timestamp": r["timestamp"], **(r["levels"] or {})} for r in signals]
    _atomic_csv(pd.DataFrame(sig_rows), os.path.join(RESULTS_DIR, f"{prefix}_signals.csv"))

    sig_b_rows = [{"symbol": r["symbol"], "timeframe": r["timeframe"], "signal": r["signal"], "source": r["source"],
                   "close": r["close"], "rsi": r.get("rsi"), "timestamp": r["timestamp"], **(r["levels"] or {})}
                  for r in signals_b]
    _atomic_csv(pd.DataFrame(sig_b_rows), os.path.join(RESULTS_DIR, f"{prefix}_signals_b.csv"))

    _atomic_write(os.path.join(RESULTS_DIR, f"{prefix}.html"), render_html(payload))

    log.info(f"[{mode}] Results written to {RESULTS_DIR}/ ({prefix}.html, {prefix}.json, "
             f"{prefix}_signals.csv, {prefix}_signals_b.csv)")


def render_html(payload: dict) -> str:
    cfg = MODES[payload.get("mode", "intraday")]

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
          <td>{r['timestamp']}</td>
        </tr>"""
    if not payload["signals"]:
        sig_rows_html = '<tr><td colspan="10" class="empty">No Section A signals this run.</td></tr>'

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

    no_data_txt = ""
    if payload.get("no_data_count"):
        no_data_txt = f" &middot; {payload['no_data_count']} symbols skipped (Dhan returned no daily data)"

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>ALL IN ONE PRO - {cfg['title']}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }}
  h1 {{ font-size: 20px; margin: 24px 0 4px; }}
  h1:first-of-type {{ margin-top: 0; }}
  .nav {{ margin-bottom: 16px; font-size: 14px; }}
  .nav a {{ color: #58a6ff; text-decoration: none; margin-right: 18px; }}
  .nav .here {{ color: #e6e6e6; font-weight: 700; margin-right: 18px; }}
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
  <div class="nav">
    <span class="here">{cfg['title']}</span>
    <a href="{cfg['other_page']}">{cfg['other_title']}</a>
    <a href="{cfg['csv_a']}">Section A CSV</a>
    <a href="{cfg['csv_b']}">Section B CSV</a>
  </div>
  <h1>ALL IN ONE PRO - Section A (Sweep + Order Block) - {cfg['title']}</h1>
  <div class="meta">
    Run: {payload['run_timestamp']} (took {payload.get('duration_sec', 0)}s) &middot;
    Scanned {payload['scanned']}/{payload['universe_size']} symbols &middot;
    Timeframes: {', '.join(payload['timeframes'])} &middot;
    {payload['signal_count']} signals &middot;
    {payload['error_count']} errors{no_data_txt}<br>
    {cfg['note']}
  </div>
  <table>
    <thead><tr><th>Symbol</th><th>TF</th><th>Signal</th><th>Close</th><th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>T3</th><th>Bar Time</th></tr></thead>
    <tbody>{sig_rows_html}</tbody>
  </table>

  <h1>ALL IN ONE PRO - Section B (Keltner/SMC + RSI Pattern) - {cfg['title']}</h1>
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
    arg = (sys.argv[1] if len(sys.argv) > 1 else "intraday").strip().lower()
    if arg not in ("intraday", "swing", "both"):
        print("usage: python live_scanner.py [intraday|swing|both]")
        sys.exit(2)
    try:
        for m in (["intraday", "swing"] if arg == "both" else [arg]):
            run_scan(m)
    except RuntimeError as e:
        log.error(str(e))
        sys.exit(1)
