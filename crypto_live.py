"""
CRYPTO PAGE (Delta Exchange)   page: /crypto     BTCUSD, ETHUSD, SOLUSD, XAUTUSD (gold), SLVONUSD (silver)   perpetual futures, 30-minute candles

Read-only. Places no orders and is not connected to the stock scans, the bot or the bridges. Any error here is caught and logged.
Data comes from Delta Exchange's PUBLIC API (no key): candles and open-interest candles. Nothing is read from Dhan.
It reuses the scanner's own signal functions on the crypto candles - the signal logic itself is NOT changed:
    Section A  = all_in_one_scanner.compute_section_a        Section B = section_b.compute_section_b
    RSI Flush  = rsi_flush_live.live_setups
run on 30-minute candles (main) and on 1H / 4H.

THE PAGE (one dark page, refreshes every 60 s) - same layout as the commodity page
  1. Volume spurt        latest closed 30-min candle volume / average of the previous 50 candles (CSV /crypto_spurt.csv). Phone alert at >= 3x.
  2. Our signals vs OI zones   Section A / B / RSI Flush signals of the last 24 h, only ALIGNED / AGAINST the OI zone (CSV /crypto_signals.csv)
  3. OI support / resistance zones   perpetual OI buildup zones (daily OI, completed UTC days only) + "at a zone now" (CSV /crypto_oizones.csv)
  4. Section A, 5. RSI Flush, 6. Section B   every signal of the last 24 h with its verdict (CSV /crypto_signals_all.csv)

Crypto runs 24x7, so it runs every 30 minutes, every day. A "day" is the UTC day (05:30 IST to 05:30 IST) - Delta's daily candle.
XAUTUSD (gold token) and SLVONUSD (silver ETF token) are tokenised-asset perpetuals: SLVONUSD in particular trades thin, so its volume
multiples can jump on small volume.

ENV (all optional)
  CRYPTO_AUTORUN=true   CRYPTO_SYMBOLS=BTCUSD,ETHUSD,SOLUSD,XAUTUSD,SLVONUSD   CRYPTO_TFS=30m,1H,4H   CRYPTO_SPURT_TF=30m
  CRYPTO_ALERT_MULT=3.0   CRYPTO_NTFY=true   CRYPTO_NTFY_MAX_PER_DAY=40   CRYPTO_SMA_LEN=50   CRYPTO_ZONE_DAYS=120
  CRYPTO_NEAR_ATR_MULT=0.25  CRYPTO_NEAR_MIN=0.3  CRYPTO_NEAR_MAX=3.0  CRYPTO_AGAINST_RATIO=2.0   zone buffer scaled to each coin's daily range
  CRYPTO_NEAR_PCT_<SYMBOL>=0.8   force one coin's at-zone buffer (percent), e.g. CRYPTO_NEAR_PCT_BTCUSD
  DELTA_BASE_URL=https://api.india.delta.exchange   (same variable the Delta bridge uses; the global site is https://api.delta.exchange)
  CRYPTO_OI_FALLBACK=binance   if Delta has no OI history, use Binance futures daily OI for BTC/ETH/SOL (may be blocked from some servers); "off" to disable
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("crypto")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("CRYPTO_AUTORUN", "true").strip().lower() == "true"
SYMBOLS = [s.strip().upper() for s in os.environ.get("CRYPTO_SYMBOLS", "BTCUSD,ETHUSD,SOLUSD,XAUTUSD,SLVONUSD").split(",") if s.strip()]
TFS = [t.strip() for t in os.environ.get("CRYPTO_TFS", "30m,1H,4H").split(",") if t.strip() in ("15m", "30m", "1H", "4H")] or ["30m"]
SPURT_TF = os.environ.get("CRYPTO_SPURT_TF", "30m").strip()
SMA_LEN = int(os.environ.get("CRYPTO_SMA_LEN", "50") or "50")
ALERT_MULT = float(os.environ.get("CRYPTO_ALERT_MULT", "3.0") or "3.0")
NTFY_ON = os.environ.get("CRYPTO_NTFY", "true").strip().lower() == "true"
NTFY_MAX_PER_DAY = int(os.environ.get("CRYPTO_NTFY_MAX_PER_DAY", "40") or "40")
ZONE_DAYS = int(os.environ.get("CRYPTO_ZONE_DAYS", "120") or "120")
BAR_GRACE_S = 5
KEEP_HOURS = 24
# Zone buffer per coin, scaled to its own volatility (14-day average daily range as % of price):
#   "at the zone" distance = NEAR_ATR_MULT x daily range %, kept between NEAR_MIN and NEAR_MAX (in %); AGAINST distance = AGAINST_RATIO x that.
# 0.25 x a typical stock's 2% daily range = the 0.5% the stock page uses. A fixed value can be forced per coin: CRYPTO_NEAR_PCT_BTCUSD=0.8
NEAR_ATR_MULT = float(os.environ.get("CRYPTO_NEAR_ATR_MULT", "0.25") or "0.25")
NEAR_MIN = float(os.environ.get("CRYPTO_NEAR_MIN", "0.3") or "0.3")
NEAR_MAX = float(os.environ.get("CRYPTO_NEAR_MAX", "3.0") or "3.0")
AGAINST_RATIO = float(os.environ.get("CRYPTO_AGAINST_RATIO", "2.0") or "2.0")
DEFAULT_NEAR, DEFAULT_AGAINST = 1.0, 2.0             # percent, used only if a coin has no volatility figure yet


DELTA_BASE = os.environ.get("DELTA_BASE_URL", "https://api.india.delta.exchange").strip().rstrip("/")
OI_FALLBACK = os.environ.get("CRYPTO_OI_FALLBACK", "binance").strip().lower()
BINANCE_MAP = {"BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT", "SOLUSD": "SOLUSDT"}

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
LIVE_JSON = os.path.join(RESULTS_DIR, "crypto_live.json")
SPURT_CSV = os.path.join(RESULTS_DIR, "crypto_spurt.csv")
ZONES_JSON = os.path.join(RESULTS_DIR, "crypto_zones.json")
ZONES_CSV = os.path.join(RESULTS_DIR, "crypto_oizones.csv")
SIGLOG_JSON = os.path.join(RESULTS_DIR, "crypto_signals_log.json")
SIGS_CSV = os.path.join(RESULTS_DIR, "crypto_signals.csv")
SIGS_ALL_CSV = os.path.join(RESULTS_DIR, "crypto_signals_all.csv")

_ids = {}                 # symbol -> Delta symbol that is live (checked against /v2/products)
_alerted = {"date": None, "keys": set(), "count": 0}


# ======================================================================================= ids + candles (Delta public API)
_http = {"s": None}


def _get(path, params, what):
    """GET on Delta's public API with a short retry on rate limit / server errors. Returns the parsed JSON."""
    import requests
    if _http["s"] is None:
        _http["s"] = requests.Session()
    last = None
    for attempt in range(3):
        try:
            r = _http["s"].get(DELTA_BASE + path, params=params, timeout=15, headers={"Accept": "application/json", "User-Agent": "crypto-scanner"})
            if r.status_code in (429, 500, 502, 503, 504):
                last = f"HTTP {r.status_code}"
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"{what}: HTTP {r.status_code} {r.text[:120]}")
            j = r.json()
            if isinstance(j, dict) and j.get("success") is False:
                raise RuntimeError(f"{what}: {str(j.get('error'))[:120]}")
            time.sleep(0.12)
            return j
        except (requests.RequestException, ValueError) as e:
            last = str(e)[:100]
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"{what}: {last}")


def _resolve_ids():
    """Check each symbol exists on Delta (cached). Returns the list of symbols that were not found."""
    missing = []
    for s in SYMBOLS:
        if s in _ids:
            continue
        try:
            j = _get(f"/v2/products/{s}", None, f"product {s}")
            res = j.get("result") or {}
            if res.get("symbol") == s and str(res.get("state", "live")).lower() in ("live", "operational", ""):
                _ids[s] = s
            else:
                missing.append(s)
        except Exception as e:
            log.warning(f"Crypto: could not look up {s}: {e}")
            missing.append(s)
    if missing:
        log.warning(f"Crypto: {missing} not found on Delta - skipped")
    return missing


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _buffers(sym, zd):
    """(near, against) as fractions for this coin: a manual CRYPTO_NEAR_PCT_<SYMBOL> if set, else scaled to its daily range."""
    manual = os.environ.get(f"CRYPTO_NEAR_PCT_{sym}", "").strip()
    v = ((zd or {}).get("vol") or {}).get(sym) or {}
    try:
        near_pct = float(manual) if manual else float(v.get("near_pct", DEFAULT_NEAR))
    except ValueError:
        near_pct = float(v.get("near_pct", DEFAULT_NEAR))
    ag_pct = near_pct * AGAINST_RATIO if (manual or v) else DEFAULT_AGAINST
    return near_pct / 100.0, ag_pct / 100.0


def _is_near(price, z, near):
    return z["lo"] * (1 - near) <= price <= z["hi"] * (1 + near)


def _judge(side, price, zs, near, against):
    """Same rule as oi_zones_live.judge, with this coin's own buffers."""
    sup = [z for z in zs if z["type"] == "SUPPORT"]
    res = [z for z in zs if z["type"] == "RESISTANCE"]
    if side == "BUY":
        for z in sup:
            if _is_near(price, z, near):
                return "ALIGNED", f"at OI support {z['lo']}-{z['hi']}"
        for z in res:
            if z["lo"] * (1 - near) <= price <= z["hi"] or 0 <= (z["lo"] - price) / price <= against:
                return "AGAINST", f"OI resistance {z['lo']}-{z['hi']} just above"
    else:
        for z in res:
            if _is_near(price, z, near):
                return "ALIGNED", f"at OI resistance {z['lo']}-{z['hi']}"
        for z in sup:
            if z["lo"] <= price <= z["hi"] * (1 + near) or 0 <= (price - z["hi"]) / price <= against:
                return "AGAINST", f"OI support {z['lo']}-{z['hi']} just below"
    return "neutral", ""


def _range_pct(d):
    """14-day average true range of the completed daily futures candles, as % of the last close."""
    h, l, c = d["high"].to_numpy(float), d["low"].to_numpy(float), d["close"].to_numpy(float)
    if len(c) < 16:
        return None
    pc = c[:-1]
    tr = np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - pc), abs(l[1:] - pc)))
    atr = float(tr[-14:].mean())
    return atr / float(c[-1]) * 100.0 if c[-1] > 0 else None


RES = {"15m": ("15m", 15), "30m": ("30m", 30), "1H": ("1h", 60), "4H": ("4h", 240)}
NBARS = {"15m": 2500, "30m": 2000, "1H": 2000, "4H": 1000}   # history per timeframe (30m ~42 days, 1H ~83 days, 4H ~166 days)


def _candles(symbol, res, minutes, n, end_ts=None):
    """Last n candles of `symbol` (any Delta symbol incl. OI:BTCUSD), fetched in chunks of <= 1900 (Delta caps a call at 2000)."""
    end_ts = int(end_ts or time.time())
    rows = []
    left = n
    while left > 0:
        k = min(left, 1900)
        start = end_ts - k * minutes * 60
        j = _get("/v2/history/candles", {"resolution": res, "symbol": symbol, "start": start, "end": end_ts}, f"candles {symbol} {res}")
        r = j.get("result") or []
        rows += r
        if not r:
            break
        end_ts = start - 1
        left -= k
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    d = pd.DataFrame(rows)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d.get(c), errors="coerce")
    d["timestamp"] = pd.to_datetime(d["time"], unit="s", errors="coerce")
    d = d.dropna(subset=["timestamp", "close"]).set_index("timestamp").sort_index(kind="stable")
    d = d[~d.index.duplicated(keep="last")]
    return d[["open", "high", "low", "close", "volume"]].astype(float)


def _closed(df, minutes, as_of):
    """Closed candles only: a candle is closed once its start + length has passed (crypto has no session end)."""
    if df is None or df.empty:
        return df
    ends = df.index + pd.Timedelta(minutes=minutes)
    return df[(ends + pd.Timedelta(seconds=BAR_GRACE_S)) <= as_of]


def _frames(sym, sid):
    """{'30m','1H','4H'} closed candles of the perpetual (only the timeframes in CRYPTO_TFS, plus the spurt timeframe)."""
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    out = {}
    for tf, (res, mins) in RES.items():
        if tf in TFS or tf == SPURT_TF:
            out[tf] = _closed(_candles(sid, res, mins, NBARS[tf]), mins, as_of)
    return out


# ======================================================================================= volume spurt
def _stale(df):
    """True when the newest closed candle is more than 75 minutes old (no trading going on)."""
    if df is None or df.empty:
        return True
    return (datetime.now(timezone.utc).replace(tzinfo=None) - df.index[-1].to_pydatetime()) > timedelta(minutes=75)


def _score(sym, df):
    if df is None or len(df) < SMA_LEN + 1:
        return None
    vol = df["volume"].to_numpy(dtype=float)
    base = vol[-(SMA_LEN + 1):-1].mean()
    if not base > 0:
        return None
    last = df.iloc[-1]
    day = np.array([t.date() for t in df.index])   # crypto day = UTC day (Delta's daily candle)
    today_mask = day == day[-1]
    prev_close = df["close"][~today_mask].iloc[-1] if (~today_mask).any() else np.nan
    close, opn = float(last["close"]), float(last["open"])
    return {
        "symbol": sym, "price": round(close, 2),
        "day_chg_pct": round(float((close / prev_close - 1) * 100), 2) if prev_close == prev_close else None,
        "candle": _ist(df.index[-1]).strftime("%H:%M"),
        "candle_chg_pct": round((close / opn - 1) * 100, 2) if opn else None,
        "candle_type": "GREEN" if close > opn else "RED" if close < opn else "DOJI",
        "candle_vol": int(vol[-1]), "sma_vol": int(round(base)), "multiple": round(float(vol[-1] / base), 2),
        "day_vol": int(vol[today_mask].sum()),
    }


def _alert(rows, now):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ON and topic):
        return
    today = now.strftime("%Y-%m-%d")
    if _alerted["date"] != today:
        _alerted.update(date=today, keys=set(), count=0)
    if _alerted["count"] >= NTFY_MAX_PER_DAY:
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and (r["symbol"], r["candle"]) not in _alerted["keys"]]
    if not hits:
        return
    hits.sort(key=lambda r: -r["multiple"])
    lines = [f"{r['symbol']}  {r['multiple']:.1f}x  {r['price']:.2f}  {r['candle_type']}"
             + (f"  candle {r['candle_chg_pct']:+.2f}%" if r.get("candle_chg_pct") is not None else "") for r in hits]
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                             headers={"Title": f"Crypto volume spurt >= {ALERT_MULT:g}x ({hits[0]['candle']} candle)", "Priority": "high", "Tags": "chart_with_upwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Crypto ntfy refused: {resp.status_code}")
            return
    except Exception as e:
        log.warning(f"Crypto ntfy failed: {e}")
        return
    for r in hits:
        _alerted["keys"].add((r["symbol"], r["candle"]))
    _alerted["count"] += 1


# ======================================================================================= signals (the scanner's own functions)
def _side_of(sig):
    s = str(sig or "").upper()
    return "BUY" if "BUY" in s else "SELL" if "SELL" in s else ""


def _signals_for(sym, frames):
    """Section A / Section B / RSI Flush on each timeframe, using the scanner's own functions unchanged."""
    import all_in_one_scanner as sec_a
    import section_b as sec_b
    rows = []
    for tf in TFS:
        df = frames.get(tf)
        if df is None or df.empty:
            continue
        try:
            a = sec_a.compute_section_a(df)
            if a and a.get("signal"):
                lv = a.get("levels") or {}
                rows.append({"symbol": sym, "section": "A", "tf": tf, "signal": a["signal"], "side": _side_of(a["signal"]),
                             "price": lv.get("entry", a["close"]), "sl": lv.get("sl"), "t1": lv.get("T1", lv.get("t1")), "t2": lv.get("T2", lv.get("t2")),
                             "bar": _ist(a["timestamp"]).strftime("%Y-%m-%d %H:%M"), "status": "", "key": f"{sym}|A|{tf}|{a['signal']}|{a['timestamp']}"})
        except Exception as e:
            log.warning(f"Crypto Section A failed for {sym} {tf}: {e}")
        try:
            b = sec_b.compute_section_b(df)
            if b and b.get("signal"):
                lv = b.get("levels") or {}
                rows.append({"symbol": sym, "section": "B", "tf": tf, "signal": f"{b['signal']} ({b.get('source', '')})", "side": _side_of(b["signal"]),
                             "price": lv.get("entry", b["close"]), "sl": lv.get("sl"), "t1": lv.get("T1", lv.get("t1")), "t2": lv.get("T2", lv.get("t2")),
                             "bar": _ist(b["timestamp"]).strftime("%Y-%m-%d %H:%M"), "status": "", "key": f"{sym}|B|{tf}|{b['signal']}|{b['timestamp']}"})
        except Exception as e:
            log.warning(f"Crypto Section B failed for {sym} {tf}: {e}")
        try:
            import rsi_flush_live
            for r in rsi_flush_live.live_setups(df, tf, sym):
                rows.append({"symbol": sym, "section": "RSI", "tf": tf, "signal": f"RSI_FLUSH_{r['side']}", "side": r["side"],
                             "price": r["entry"], "sl": r["sl"], "t1": r.get("T1"), "t2": r.get("T2"),
                             "bar": _ist(r["signal_ts"]).strftime("%Y-%m-%d %H:%M"), "status": r["status"],
                             "key": f"{sym}|RSI|{tf}|{r['side']}|{r['signal_ts']}"})
        except Exception as e:
            log.warning(f"Crypto RSI Flush failed for {sym} {tf}: {e}")
    return rows


def _update_log(new_rows, now):
    """Keep the last 24 h of signals: a signal is stored once (first time seen); an RSI Flush row's status is refreshed each cycle."""
    try:
        with open(SIGLOG_JSON) as f:
            logd = json.load(f)
    except Exception:
        logd = {}
    stamp = now.isoformat()
    for r in new_rows:
        old = logd.get(r["key"])
        if old:
            if r["section"] == "RSI":
                old["status"] = r["status"]
                old["price"], old["sl"] = r["price"], r["sl"]
        else:
            logd[r["key"]] = {**r, "seen": stamp}
    cut = (now - timedelta(hours=KEEP_HOURS)).isoformat()
    logd = {k: v for k, v in logd.items() if v.get("seen", stamp) >= cut}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(SIGLOG_JSON + ".tmp", "w") as f:
        json.dump(logd, f)
    os.replace(SIGLOG_JSON + ".tmp", SIGLOG_JSON)
    return list(logd.values())


# ======================================================================================= OI zones (daily futures OI)
def _oi_from_delta(sym):
    """Daily OHLC + open interest from Delta: price candles joined with the OI candles (symbol OI:<SYMBOL>, close = open interest)."""
    n = ZONE_DAYS + 5
    px = _candles(sym, "1d", 1440, n)
    oic = _candles("OI:" + sym, "1d", 1440, n)
    if px.empty or oic.empty:
        return None
    d = px[["open", "high", "low", "close", "volume"]].copy()
    d["oi"] = oic["close"].reindex(d.index)
    d = d[d["oi"] > 0]
    return d


def _oi_from_binance(sym, px):
    """Fallback: Binance USD-M futures daily open interest (BTC / ETH / SOL), joined to Delta's daily price candles by UTC date."""
    import requests
    b = BINANCE_MAP.get(sym)
    if not b or OI_FALLBACK != "binance" or px is None or px.empty:
        return None
    r = requests.get("https://fapi.binance.com/futures/data/openInterestHist", params={"symbol": b, "period": "1d", "limit": 500}, timeout=15)
    r.raise_for_status()
    h = pd.DataFrame(r.json())
    if h.empty:
        return None
    h["day"] = (pd.to_datetime(h["timestamp"], unit="ms") - pd.Timedelta(milliseconds=1)).dt.normalize()   # snapshot at day end -> that day
    oi = h.set_index("day")["sumOpenInterest"].astype(float)
    d = px.copy()
    d["oi"] = oi.reindex(d.index)
    return d[d["oi"] > 0]


def _fetch_daily_oi(sym):
    """(daily df with 'oi', source). Completed UTC days only. None if there is not enough history."""
    src = "Delta"
    d = None
    try:
        d = _oi_from_delta(sym)
    except Exception as e:
        log.info(f"Crypto {sym}: Delta OI candles not available ({str(e)[:100]})")
    if d is None or len(d) < 15:
        try:
            px = _candles(sym, "1d", 1440, ZONE_DAYS + 5)
            d = _oi_from_binance(sym, px)
            src = "Binance (fallback)"
        except Exception as e:
            log.info(f"Crypto {sym}: Binance OI fallback failed ({str(e)[:100]})")
            d = None
    if d is None:
        return None, src
    today = datetime.now(timezone.utc).date()
    d = d[[ts.date() != today for ts in d.index]]            # today's UTC daily bar is still forming
    return (d if len(d) >= 15 else None), src


def build_zones():
    import oi_zones_live as oz
    _resolve_ids()
    zones, errors, notes, vol, srcs = {}, 0, [], {}, {}
    for s in SYMBOLS:
        sid = _ids.get(s)
        if not sid:
            errors += 1
            notes.append(f"{s}: not found on Delta")
            continue
        try:
            d, src = _fetch_daily_oi(s)
            srcs[s] = src
            if d is None:
                errors += 1
                notes.append(f"{s}: no OI rows")
                continue
            rp = _range_pct(d)
            if rp:
                np_ = min(max(NEAR_ATR_MULT * rp, NEAR_MIN), NEAR_MAX)
                vol[s] = {"atr_pct": round(rp, 2), "near_pct": round(np_, 2), "against_pct": round(np_ * AGAINST_RATIO, 2)}
            z = oz.zones_from_df(d)
            if z:
                zones[s] = z
            else:
                notes.append(f"{s}: no zone")
        except Exception as e:
            errors += 1
            notes.append(f"{s}: {str(e)[:80]}")
    obj = {"built": datetime.now(IST).isoformat(), "stocks": zones, "vol": vol, "oi_source": srcs, "errors": errors, "universe": len(SYMBOLS), "note": " | ".join(notes)}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(ZONES_JSON + ".tmp", "w") as f:
        json.dump(obj, f)
    os.replace(ZONES_JSON + ".tmp", ZONES_JSON)
    log.info(f"Crypto OI zones built: {len(zones)} of {len(SYMBOLS)} coins have zones, {errors} problems {obj['note']}")
    return obj


def load_zones():
    try:
        with open(ZONES_JSON) as f:
            return json.load(f)
    except Exception:
        return None


def _zones_due(now):
    """Rebuild when missing, older than 20 h, or once after 00:05 UTC (05:35 IST) so the finished UTC day is included."""
    z = load_zones()
    if not z:
        return True
    try:
        built = datetime.fromisoformat(z["built"])
    except Exception:
        return True
    if now - built > timedelta(hours=20):
        return True
    nu = datetime.now(timezone.utc)
    boundary = nu.replace(hour=0, minute=5, second=0, microsecond=0)
    if nu < boundary:
        boundary -= timedelta(days=1)
    return built < boundary


# ======================================================================================= one cycle
def run_once():
    now = datetime.now(IST)
    if _zones_due(now):
        try:
            build_zones()
        except Exception as e:
            log.warning(f"Crypto zones build failed: {e}")
    missing = _resolve_ids()
    zd = load_zones() or {}
    zones = zd.get("stocks") or {}
    spurt, errors, cur_sigs, last_px, idle = [], 0, [], {}, []
    for s in SYMBOLS:
        sid = _ids.get(s)
        if not sid:
            continue
        try:
            fr = _frames(s, sid)
            r = _score(s, fr.get(SPURT_TF))
            if r and _stale(fr.get(SPURT_TF)):
                idle.append(s)                                  # market closed (e.g. gold / silver tokens at the weekend): no fresh candle
                r = None
            if r:
                spurt.append(r)
                last_px[s] = r["price"]
            cur_sigs += _signals_for(s, fr)
        except Exception as e:
            errors += 1
            log.warning(f"Crypto data failed for {s}: {e}")
    spurt.sort(key=lambda r: -r["multiple"])
    try:
        _alert(spurt, now)
    except Exception as e:
        log.warning(f"Crypto alert step failed: {e}")
    all_sigs = _update_log(cur_sigs, now)

    near = []
    for s, zs in zones.items():
        px = last_px.get(s)
        if px is None:
            continue
        best = None
        nr, _ag = _buffers(s, zd)
        for z in zs:
            if _is_near(px, z, nr):
                dist = (px - z["hi"]) / px * 100 if z["type"] == "SUPPORT" else (px - z["lo"]) / px * 100
                if best is None or abs(dist) < abs(best[1]):
                    best = (z, dist)
        if best:
            z, dist = best
            near.append({"symbol": s, "price": px, "zone": z["type"], "zone_lo": z["lo"], "zone_hi": z["hi"], "dist_pct": round(dist, 2),
                         "oi_added_pct": z["oi_added_pct"], "zone_date": z["date"]})

    # alignment of every signal of the last 24 h with the OI zones (same judge as the stock page)
    for r in all_sigs:
        if r["section"] == "RSI" and r.get("status") not in ("PENDING", "ACTIVE"):
            r["verdict"], r["why"] = "closed", ""
            continue
        nr, ag = _buffers(r["symbol"], zd)
        v, why = _judge(r["side"], float(r["price"]), zones.get(r["symbol"], []), nr, ag) if r.get("price") is not None and r["side"] else ("neutral", "")
        r["verdict"], r["why"] = v, why
    all_sigs.sort(key=lambda r: r["bar"], reverse=True)

    meta = {"run": now.strftime("%Y-%m-%d %H:%M:%S IST"), "symbols": SYMBOLS, "missing": missing, "idle": idle, "oi_source": zd.get("oi_source", {}), "scored": len(spurt), "errors": errors,
            "alert_mult": ALERT_MULT, "sma_len": SMA_LEN, "tfs": TFS, "spurt_tf": SPURT_TF,
            "zones_built": (zd.get("built") or "")[:16].replace("T", " "), "zones_note": zd.get("note", ""),
            "zones_count": len(zones), "zones_universe": zd.get("universe", len(SYMBOLS))}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(LIVE_JSON + ".tmp", "w") as f:
        json.dump({"meta": meta, "spurt": spurt, "near": near, "signals": all_sigs}, f, default=str)
    os.replace(LIVE_JSON + ".tmp", LIVE_JSON)
    pd.DataFrame(spurt).to_csv(SPURT_CSV, index=False)
    pd.DataFrame([{"symbol": s, **z} for s, zs in zones.items() for z in zs]).to_csv(ZONES_CSV, index=False)
    cols = ["bar", "symbol", "section", "tf", "signal", "side", "price", "sl", "t1", "t2", "status", "verdict", "why"]
    pd.DataFrame(all_sigs, columns=cols).to_csv(SIGS_ALL_CSV, index=False)
    pd.DataFrame([r for r in all_sigs if r["verdict"] in ("ALIGNED", "AGAINST")], columns=cols).to_csv(SIGS_CSV, index=False)
    log.info(f"Crypto cycle: {len(spurt)} scored, {len(cur_sigs)} signals now ({len(all_sigs)} in 24h), {len(near)} at a zone, {errors} errors")


# ======================================================================================= the page
def _load():
    try:
        with open(LIVE_JSON) as f:
            return json.load(f)
    except Exception:
        return None


def page_html():
    d = _load()
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>Crypto scanner</title>'
            '<style>body{background:#0e1117;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:24px}'
            'table{border-collapse:collapse;width:100%;font-size:13px;margin-bottom:8px}th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #262b36}'
            'th{background:#161b22;color:#9aa0a6;font-weight:600}tr:hover{background:#161b22}a{color:#58a6ff}.m{color:#9aa0a6;font-size:13px;margin:6px 0 12px}'
            'h2{margin:30px 0 4px}h3{margin:18px 0 6px}</style></head><body>')
    if not d:
        return (head + "<h2>Crypto scanner (Delta Exchange)</h2><p>No cycle has run yet. It runs every 30 minutes, 24x7.</p></body></html>")
    m = d["meta"]
    G, R, M = "#3fb950", "#f85149", "#9aa0a6"

    def tbl(cols, rows, empty):
        if not rows:
            return f'<p style="color:{M}">{empty}</p>'
        return ('<div style="overflow-x:auto"><table><tr>' + "".join(f"<th>{c}</th>" for c in cols) + "</tr>"
                + "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows) + "</table></div>")

    def pct(v):
        return "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'

    def side_c(s):
        return f'<b style="color:{G if s == "BUY" else R}">{s}</b>'

    def verdict_c(v):
        return f'<b style="color:{G if v == "ALIGNED" else R if v == "AGAINST" else M}">{v}</b>'

    miss = f' &middot; <span style="color:{R}">not found on Delta: {", ".join(m["missing"])}</span>' if m.get("missing") else ""
    idle = f' &middot; <span style="color:{M}">no fresh candle (market closed): {", ".join(m["idle"])}</span>' if m.get("idle") else ""
    out = [head, "<h1>Crypto scanner (Delta perpetuals)</h1>",
           f'<div class="m">Run {m["run"]} &middot; {", ".join(m["symbols"])} (perpetuals) &middot; timeframes {", ".join(m["tfs"])} &middot; errors {m["errors"]}{miss}{idle} &middot; '
           f'<a href="/scanner">Stock scanner</a> &middot; <a href="/spurt10">Stock volume spurt</a></div>']

    # 1 volume spurt
    sp = [[f"<b>{x['symbol']}</b>", f"{x['price']:.2f}", pct(x["day_chg_pct"]), x["candle"], pct(x["candle_chg_pct"]),
           f'<b style="color:{G if x["candle_type"] == "GREEN" else R if x["candle_type"] == "RED" else M}">{x["candle_type"]}</b>',
           f"{x['candle_vol']:,}", f"{x['sma_vol']:,}", f'<b style="color:{"#d29922" if x["multiple"] >= m["alert_mult"] else "#e6e6e6"}">{x["multiple"]:.2f}x</b>', f"{x['day_vol']:,}"]
          for x in d["spurt"]]
    out.append(f'<h2>1. Volume spurt &mdash; latest {m["spurt_tf"]} candle vs {m["sma_len"]}-candle average</h2>'
               f'<div class="m">amber = {m["alert_mult"]:g}x or more (phone alert) &middot; <a href="/crypto_spurt.csv">CSV</a></div>'
               + tbl(["Symbol", "Price", "Day %", "Candle", "Candle %", "Candle type", "Candle volume", f"{m['sma_len']}-candle avg vol", "Multiple", "Day volume"], sp,
                     "No coin candles scored yet (needs 50+ closed candles)."))

    sigs = d["signals"]
    al = [x for x in sigs if x["verdict"] in ("ALIGNED", "AGAINST")]
    # 2 signals vs OI zones
    out.append(f'<h2>2. Our signals vs OI zones ({len(al)}) &middot; <a href="/crypto_signals.csv" style="font-size:13px;font-weight:400">CSV</a></h2>'
               f'<div class="m">Section A / B / RSI Flush signals of the last 24 h; ALIGNED = at an OI zone that supports the trade, AGAINST = at the opposite zone</div>'
               + tbl(["Bar (IST)", "Symbol", "Sec", "TF", "Signal", "Price", "Verdict", "Why"],
                     [[x["bar"], f"<b>{x['symbol']}</b>", x["section"], x["tf"], x["signal"], x["price"], verdict_c(x["verdict"]), x["why"]] for x in al],
                     "No ALIGNED or AGAINST signals in the last 24 h."))

    # 3 OI zones
    zd = load_zones() or {}
    zc = lambda t: G if t == "SUPPORT" else R
    near = [[f"<b>{x['symbol']}</b>", x["price"], f'<span style="color:{zc(x["zone"])}">{x["zone"]}</span>', f"{x['zone_lo']} - {x['zone_hi']}",
             f"{x['dist_pct']:+.2f}%", f"{x['oi_added_pct']}%", x["zone_date"]] for x in d["near"]]
    allz = [[f"<b>{s}</b>", f'<span style="color:{zc(z["type"])}">{z["type"]}</span>', f"{z['lo']} - {z['hi']}", f"{z['oi_added_pct']}%", z["date"]]
            for s, zs in (zd.get("stocks") or {}).items() for z in zs]
    out.append(f'<h2>3. OI support / resistance zones (perpetual OI buildup)</h2>'
               f'<div class="m">Zones built {m["zones_built"] or "-"} IST &middot; {m["zones_count"]} of {m["zones_universe"]} coins have zones &middot; <a href="/crypto_oizones.csv">CSV</a>'
               + (f'<br>OI source: {", ".join(f"{k} {v}" for k, v in (m.get("oi_source") or {}).items())}' if m.get("oi_source") else "")
               + (f'<br><i>{m["zones_note"]}</i>' if m.get("zones_note") else "") + "</div>"
               f"<h3>Zone buffer per coin</h3>"
               + tbl(["Symbol", "Daily range (14-day avg)", "At-zone buffer", "AGAINST distance", "Source"],
                     [[f"<b>{s_}</b>", (f"{(zd.get('vol') or {}).get(s_, {}).get('atr_pct', '-')}%"), f"{_buffers(s_, zd)[0] * 100:.2f}%", f"{_buffers(s_, zd)[1] * 100:.2f}%",
                       "manual (CRYPTO_NEAR_PCT_" + s_ + ")" if os.environ.get(f"CRYPTO_NEAR_PCT_{s_}", "").strip() else ("volatility" if (zd.get("vol") or {}).get(s_) else "default")]
                      for s_ in m["symbols"]], "")
               + f"<h3>Coins at an OI zone now ({len(near)})</h3>"
               + tbl(["Symbol", "Price", "Zone", "Zone range", "Dist from edge", "OI added", "Zone day"], near, "None right now.")
               + f"<h3>All zones ({len(allz)})</h3>" + tbl(["Symbol", "Zone", "Zone range", "OI added", "Zone day"], allz, "No zones built yet."))

    # 4-6 the three signal sections
    def sec_table(code):
        rows = [x for x in sigs if x["section"] == code]
        return rows, tbl(["Bar (IST)", "Symbol", "TF", "Signal", "Side", "Entry", "SL", "T1", "T2"] + (["Status"] if code == "RSI" else []) + ["OI verdict"],
                         [[x["bar"], f"<b>{x['symbol']}</b>", x["tf"], x["signal"], side_c(x["side"]), x["price"], x["sl"], x.get("t1") or "-", x.get("t2") or "-"]
                          + ([x.get("status", "")] if code == "RSI" else [])
                          + [verdict_c(x["verdict"]) if x["verdict"] in ("ALIGNED", "AGAINST") else f'<span style="color:{M}">{x["verdict"]}</span>'] for x in rows],
                         "No signals in the last 24 h.")

    for n, code, title in ((4, "A", "Section A (Sweep + Order Block)"), (5, "RSI", "RSI Flush"), (6, "B", "Section B (Keltner/SMC + RSI Pattern)")):
        rows, t = sec_table(code)
        out.append(f'<h2>{n}. {title} ({len(rows)}) &middot; last 24 h</h2>' + t)
    out.append(f'<div class="m" style="margin-top:20px"><a href="/crypto_signals_all.csv">CSV of all signals</a> &middot; Signals are computed by the same functions as the stock scanner on 30m / 1H / 4H perpetual candles. Day = UTC day.</div>')
    out.append("</body></html>")
    return "".join(out)


# ======================================================================================= loop
def _seconds_to_next_run(now):
    nxt = now.replace(second=30, microsecond=0)
    nxt += timedelta(minutes=(30 - now.minute % 30) % 30)
    if nxt <= now:
        nxt += timedelta(minutes=30)
    return max((nxt - now).total_seconds(), 1)


def loop():
    log.info(f"Crypto autorun started for {SYMBOLS} ({', '.join(TFS)}) - every 30 min at candle close, 24x7.")
    time.sleep(20)                                   # let the web server come up first, then give the page its first cycle at once
    try:
        run_once()
    except Exception as e:
        log.error(f"Crypto first cycle failed: {e}")
    while True:
        try:
            time.sleep(_seconds_to_next_run(datetime.now(IST)))
            run_once()
        except Exception as e:
            log.error(f"Crypto cycle failed: {e}")
            time.sleep(30)
