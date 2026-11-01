"""
Small Dhan helper for the FNO-SCANNER service (read-only: quotes, candles, scrip master, phone alerts). Nothing here places an order.
Needs the same Render env values as the stock scanner: DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN (and DHAN_PROXY_URL only if you use one), NTFY_TOPIC.
"""
import os
import time
import threading
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests

log = logging.getLogger("fno")

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
DHAN_BASE_URL = "https://api.dhan.co/v2"
DHAN_PROXY_URL = os.environ.get("DHAN_PROXY_URL", "").strip()
PROXIES = {"https": DHAN_PROXY_URL, "http": DHAN_PROXY_URL} if DHAN_PROXY_URL else None
RPS = 0.7                   # PAPER-TRADE: gentle on purpose - at most about 1 Dhan call per 1.4 s for all students together, so the real bridge and scanners keep their share of the limit
MIN_RPS = 0.4
TIMEOUT = 20
RETRIES = 3
MAX_429 = 6
COOLDOWN_SECONDS = 20       # after MAX_429 quick back-offs, the whole scanner pauses this long ...
COOLDOWN_ROUNDS = 2         # ... and retries, up to this many times, before giving up on one contract


class _Limiter:
    """Spaces request starts apart across all threads; widens the gap on 429 and narrows it again after a long run of successes."""

    def __init__(self, rps, min_rps):
        self.base = 1.0 / rps
        self.max = 1.0 / min_rps
        self.interval = self.base
        self._lock = threading.Lock()
        self._next = 0.0
        self._ok = 0
        self._last = -1e9

    def wait(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            time.sleep(start - now)

    def throttle(self):
        with self._lock:
            now = time.monotonic()
            self._ok = 0
            if now - self._last >= 2.0:
                self.interval = min(self.interval * 1.5, self.max)
                self._last = now
            self._next = max(self._next, now + self.interval * 2)

    def cooldown(self, seconds):
        """Every thread waits: nothing new is sent for this many seconds (Dhan's shared limit needs time to recover)."""
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)

    def success(self):
        with self._lock:
            self._ok += 1
            if self._ok >= 60 and self.interval > self.base:
                self.interval = max(self.base, self.interval / 1.15)
                self._ok = 0


_lim = _Limiter(RPS, MIN_RPS)


def headers():
    return {"access-token": DHAN_ACCESS_TOKEN, "client-id": DHAN_CLIENT_ID, "Content-Type": "application/json"}


def post(path, payload, label=""):
    """POST to Dhan (rate limited, 429 back-off, retries). Returns the Response. Raises RuntimeError with a plain message."""
    if not DHAN_ACCESS_TOKEN or not DHAN_CLIENT_ID:
        raise RuntimeError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are not set on this Render service")
    last, hits, attempt, rounds = None, 0, 0, 0
    while attempt < RETRIES:
        attempt += 1
        try:
            _lim.wait()
            r = requests.post(f"{DHAN_BASE_URL}/{path}", headers=headers(), json=payload, timeout=TIMEOUT, proxies=PROXIES)
        except Exception as e:
            last = e
            time.sleep(1.5 * attempt)
            continue
        if r.status_code == 429:
            hits += 1
            _lim.throttle()
            if hits <= MAX_429:
                time.sleep(2 * hits)
                attempt -= 1
                continue
            if rounds < COOLDOWN_ROUNDS:        # still limited: pause the whole scanner for a while, then try this call again
                rounds += 1
                hits = 0
                _lim.cooldown(COOLDOWN_SECONDS)
                time.sleep(COOLDOWN_SECONDS)
                attempt -= 1
                continue
            raise RuntimeError(f"rate-limited (429) on {label}")
        if r.status_code == 400:
            raise RuntimeError(f"400 from Dhan on {label}: {r.text[:150]}")
        if r.status_code in (401, 403):
            raise RuntimeError(f"HTTP {r.status_code} on {label} (access token expired or not allowed?)")
        if r.status_code >= 400:
            last = f"HTTP {r.status_code}"
            time.sleep(1.5 * attempt)
            continue
        _lim.success()
        return r
    raise RuntimeError(f"{last} on {label}")


# ------------------------------------------------------------------ candles
def parse_intraday(j):
    """Dhan intraday json -> DataFrame (UTC index; open/high/low/close/volume and 'oi' when Dhan sends open_interest)."""
    if not isinstance(j, dict) or not j.get("close"):
        raise ValueError("empty candle answer")
    d = {k: j.get(k) for k in ("open", "high", "low", "close", "volume")}
    ts = j.get("timestamp") or j.get("start_Time") or j.get("startTime")
    if ts is None:
        raise ValueError(f"no timestamp in candle answer (keys: {list(j.keys())})")
    d["timestamp"] = ts
    oi = j.get("open_interest") or j.get("oi")
    if isinstance(oi, list) and len(oi) == len(ts):
        d["oi"] = oi
    df = pd.DataFrame(d)
    t = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    if t.isna().all():
        t = pd.to_datetime(df["timestamp"], errors="coerce")
    df["timestamp"] = t
    df = df.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df.astype(float)


def merge_bars(df, group):
    """Merges `group` consecutive bars inside each day into one (5-min -> 10 / 15-min). OI = the last OI of the group."""
    if df is None or df.empty:
        return df
    day = pd.Series(df.index.date)
    gid = day.ne(day.shift()).to_numpy().cumsum()
    pos = pd.Series(range(len(df))).groupby(gid).cumcount().to_numpy()
    key = gid * 100000 + pos // group
    g = df.groupby(key)
    out = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                        "close": g["close"].last(), "volume": g["volume"].sum()})
    if "oi" in df.columns:
        out["oi"] = g["oi"].last()
    out.index = g.apply(lambda x: x.index[0])
    return out.sort_index()


def drop_forming(df, minutes, as_of, session_end_utc_hour=None):
    """Keeps only closed bars (a bar is closed once its end time + 5 s has passed)."""
    if df is None or df.empty:
        return df
    idx = df.index
    ends = (idx + pd.Timedelta(minutes=minutes)).to_numpy()
    if session_end_utc_hour is not None:
        ends = np.minimum(ends, (idx.normalize() + pd.Timedelta(hours=session_end_utc_hour)).to_numpy())
    return df[(pd.DatetimeIndex(ends) + pd.Timedelta(seconds=5)) <= as_of]


def candles(sid, seg, instrument, days, with_oi=True):
    """5-minute candles of any instrument for the last `days` days. Returns a DataFrame; raises RuntimeError on failure."""
    now = datetime.now()
    payload = {"securityId": str(sid), "exchangeSegment": seg, "instrument": instrument, "interval": 5,
               "fromDate": (now - pd.Timedelta(days=days)).strftime("%Y-%m-%d"), "toDate": now.strftime("%Y-%m-%d")}
    if with_oi and instrument not in ("EQUITY", "INDEX"):
        payload["oi"] = True
    r = post("charts/intraday", payload, f"{instrument} {sid}")
    try:
        return parse_intraday(r.json())
    except ValueError as e:
        raise RuntimeError(str(e))


def utc_now():
    return pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))


# ------------------------------------------------------------------ phone alert
def ntfy(title, body, priority="high", tags="chart_with_upwards_trend"):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return False
    for attempt in range(4):
        try:
            r = requests.post(f"https://ntfy.sh/{topic}", data=body.encode("utf-8"), timeout=8,
                              headers={"Title": title, "Priority": priority, "Tags": tags})
            if r.status_code < 300:
                return True
            log.warning(f"ntfy refused: {r.status_code} {r.text[:100]}")
            return False
        except Exception as e:
            log.warning(f"ntfy failed (try {attempt + 1}/4): {e}")
            time.sleep(30 if attempt < 3 else 0)
    return False
