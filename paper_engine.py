"""
PAPER-TRADE engine - practice trading with virtual money at LIVE Dhan prices. Places no real orders, ever.
Each approved user has a virtual account (START_CASH). Orders: market or limit, buy or sell (short), always with a stop-loss and up to two targets.
 - Two products, like a real broker:
     INTRADAY   (MIS): margin = value / LEVERAGE is blocked, buy or short, everything squared off at 15:20 IST.
     POSITIONAL (CNC / delivery): the full value is blocked (no leverage), BUY only (no delivery shorting on NSE),
                carried overnight with no square-off; the SL and targets keep working every market day.
 - T1 with a T2 set: half the quantity is booked at T1 and the SL moves to the entry price; the rest runs to T2 or the SL.
 - Costs: a flat % per side of the turnover is charged (an estimate of brokerage + taxes), higher for delivery, so results are not flattered.
 - SL / targets / limit fills are only checked while the market is open (09:15 - 15:30 IST, Monday to Friday).
All data is kept in one json file on the Render disk (/var/data/paper when a disk is attached).
"""
import os
import json
import time
import threading
import logging
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

import paper_dhan as dl
import pandas as pd
import numpy as np

log = logging.getLogger("paper")
IST = ZoneInfo("Asia/Kolkata")

START_CASH = 2_500_000
LEVERAGE = 5                        # intraday (MIS) margin multiple
COST_PCT = 0.03                     # intraday, per side, % of turnover (estimate of brokerage + STT + charges)
COST_PCT_CNC = 0.12                 # positional / delivery, per side (delivery STT is much higher)
OPEN_MIN = 9 * 60 + 15
LAST_ENTRY_MIN = 15 * 60 + 15       # no new orders after this
SQUAREOFF_MIN = 15 * 60 + 20
CLOSE_MIN = 15 * 60 + 30
TICK_SECONDS = 5
MAX_TRADES_KEPT = 5000
PRICE_CACHE_S = 2.0
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

INTRADAY = "INTRADAY"
POSITIONAL = "POSITIONAL"


def _product(v):
    """Order product from the form. Anything that is not clearly positional/delivery is intraday (the old default)."""
    v = (v or "").strip().upper()
    return POSITIONAL if v in ("POSITIONAL", "CNC", "DELIVERY") else INTRADAY


def _lev(x):
    """Margin multiple of a position / order (old records without a product are intraday)."""
    return 1 if x.get("product") == POSITIONAL else LEVERAGE


def _plabel(p):
    return "Positional" if p == POSITIONAL else "Intraday"


def _data_dir():
    for d in ("/var/data",):
        try:
            if os.path.isdir(d) and os.access(d, os.W_OK):
                p = os.path.join(d, "paper")
                os.makedirs(p, exist_ok=True)
                return p
        except Exception:
            pass
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(p, exist_ok=True)
    return p


DATA = _data_dir()
DB_FILE = os.path.join(DATA, "paper_db.json")
SYM_FILE = os.path.join(DATA, "symbols.json")
_LOCK = threading.RLock()
_db = {"users": {}}


def now_ist():
    return datetime.now(IST)


def _mins(t):
    return t.hour * 60 + t.minute


def market_open(t=None):
    t = t or now_ist()
    return t.weekday() < 5 and OPEN_MIN <= _mins(t) < CLOSE_MIN


def can_enter(t=None):
    t = t or now_ist()
    return t.weekday() < 5 and OPEN_MIN <= _mins(t) < LAST_ENTRY_MIN


# ------------------------------------------------------------------ storage
def _load():
    global _db
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            _db = json.load(f)
        _db.setdefault("users", {})
    except FileNotFoundError:
        _db = {"users": {}}
    except Exception as e:
        log.error(f"paper db load failed: {e}")
        try:
            os.replace(DB_FILE, DB_FILE + ".broken")
        except Exception:
            pass
        _db = {"users": {}}


def _save():
    try:
        tmp = DB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_db, f)
        os.replace(tmp, DB_FILE)
    except Exception as e:
        log.error(f"paper db save failed: {e}")


_load()


def _new_acct():
    return {"cash": float(START_CASH), "start": float(START_CASH), "positions": [], "pending": [], "trades": [],
            "resets": 0, "joined": now_ist().strftime("%Y-%m-%d %H:%M"), "seq": 0}


def acct(user):
    with _LOCK:
        a = _db["users"].get(user)
        if a is None:
            a = _db["users"][user] = _new_acct()
            _save()
        return a


def _note(a, text):
    a.setdefault("notes", []).append({"t": now_ist().strftime("%d %b %H:%M"), "m": text})
    a["notes"] = a["notes"][-8:]


def _nid(a, prefix):
    a["seq"] += 1
    return f"{prefix}{a['seq']}"


# ------------------------------------------------------------------ symbols (NSE equities from Dhan's scrip master)
_sym = {"map": {}, "loaded": 0.0, "loading": False, "err": ""}


def _sym_load_file():
    try:
        with open(SYM_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        _sym["map"] = {k: tuple(v) for k, v in d["map"].items()}
        _sym["loaded"] = d.get("ts", 0)
    except Exception:
        pass


def _sym_download():
    import pandas as pd
    cols = ["SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID", "SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL", "SEM_SERIES"]
    out = {}
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tmp:
        with requests.get(SCRIP_URL, timeout=120, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1 << 20):
                tmp.write(chunk)
        tmp.flush()
        for df in pd.read_csv(tmp.name, usecols=lambda c: c in cols, dtype=str, chunksize=40000):
            df = df.apply(lambda s: s.str.strip() if s.dtype == object else s)
            eq = df[(df["SEM_EXM_EXCH_ID"] == "NSE") & (df["SEM_SEGMENT"] == "E") & (df["SEM_SERIES"] == "EQ")]
            for sy, sid, nm in zip(eq["SEM_TRADING_SYMBOL"], eq["SEM_SMST_SECURITY_ID"], eq["SEM_CUSTOM_SYMBOL"].fillna("")):
                out[sy] = (str(sid), nm)
    if len(out) < 500:
        raise RuntimeError(f"scrip master looked wrong ({len(out)} NSE equities)")
    return out


def sym_refresh(force=False):
    """Loads the NSE equity list (from the disk copy, downloading a fresh one when it is older than a day)."""
    with _LOCK:
        if _sym["loading"]:
            return
        if not _sym["map"]:
            _sym_load_file()
        if _sym["map"] and not force and time.time() - _sym["loaded"] < 86400:
            return
        _sym["loading"] = True
    try:
        m = _sym_download()
        with _LOCK:
            _sym["map"], _sym["loaded"], _sym["err"] = m, time.time(), ""
        tmp = SYM_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ts": _sym["loaded"], "map": m}, f)
        os.replace(tmp, SYM_FILE)
        log.info(f"paper: {len(m)} NSE equities loaded")
    except Exception as e:
        _sym["err"] = str(e)
        log.warning(f"paper: symbol list download failed: {e}")
    finally:
        _sym["loading"] = False


def sym_search(q, n=15):
    q = (q or "").strip().upper()
    if not q:
        return []
    m = _sym["map"]
    starts = [s for s in m if s.startswith(q)]
    rest = [s for s in m if q in s and not s.startswith(q)]
    starts.sort(key=lambda s: (len(s), s))
    rest.sort(key=lambda s: (len(s), s))
    return [{"symbol": s, "name": m[s][1]} for s in (starts + rest)[:n]]


# ------------------------------------------------------------------ prices
_px = {}            # sid -> (time, price)


def _fetch_prices(sids):
    """{sid: ltp} for NSE equities in one Dhan request (replaced by a fake in tests). {} when Dhan cannot be reached."""
    sids = [int(s) for s in sids if str(s).isdigit()]
    if not sids:
        return {}
    r = dl.post("marketfeed/ltp", {"NSE_EQ": sorted(set(sids))}, "ltp")
    seg = ((r.json() or {}).get("data") or {}).get("NSE_EQ") or {}
    out = {}
    for sid, e in seg.items():
        p = (e or {}).get("last_price")
        if p:
            out[str(sid)] = float(p)
    return out


PRICE_FN = _fetch_prices


def prices(sids, max_age=PRICE_CACHE_S):
    """{sid: price}. Uses a 2-second cache so many students do not multiply the Dhan calls."""
    now = time.time()
    sids = [str(s) for s in sids]
    need = [s for s in sids if s not in _px or now - _px[s][0] > max_age]
    err = ""
    if need:
        try:
            for s, p in PRICE_FN(need).items():
                _px[str(s)] = (now, p)
        except Exception as e:
            err = str(e)
            log.warning(f"paper: price fetch failed: {e}")
    prices.last_err = err
    return {s: _px[s][1] for s in sids if s in _px}


prices.last_err = ""


def quote(symbol):
    symbol = (symbol or "").strip().upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return None, "Unknown symbol (NSE equities only)."
    p = prices([ent[0]]).get(ent[0])
    if p is None:
        return None, "Live price not available right now" + (f" ({prices.last_err})" if prices.last_err else "") + "."
    return p, ""


# ------------------------------------------------------------------ stock info panel (zones + volume spurt, same rules as the scanners)
_ci = {}
ZONE_TF = 15            # support / resistance zones are built on 15-minute candles (same as the stock scanner)
SPURT_TF = 10           # volume spurt = latest closed 10-minute candle volume / average of the 50 candles before it
SPURT_SMA = 50
ZONE_NEAR_PCT = 0.5


def _ist_label(ts):
    return (ts + pd.Timedelta(hours=5, minutes=30)).strftime("%d %b %H:%M")


def _zint(v, lo, hi, default):
    try:
        v = int(float(v))
    except (TypeError, ValueError):
        return default
    return min(max(v, lo), hi)


def chart_info(symbol, tf=15, zz=None, nob=None, zatr=None):
    """Nearest support / resistance zone, volume-spurt multiple and day change of one stock. Cached for 60 s. Returns (dict, error)."""
    symbol = (symbol or "").strip().upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return None, "Unknown symbol."
    tf = _tf(tf, "15")
    zz, nob, zatr = _zint(zz, 3, 50, 9), _zint(nob, 1, 5, 2), _zint(zatr, 2, 100, 14)   # zone settings (defaults = the indicator)
    ck = (symbol, tf, zz, nob, zatr)
    c = _ci.get(ck)
    if c and time.time() - c[0] < 120:
        return c[1], ""
    try:
        import paper_zones as spurt_zones
        df5 = _m5(ent[0])
        asof = dl.utc_now()
        d5 = dl.drop_forming(df5, 5, asof)
        if d5 is None or len(d5) < 20:
            return None, "Not enough candles for this stock yet."
        price = float(d5["close"].iloc[-1])
        days = sorted(set(d5.index.date))
        prev = d5[d5.index.date < days[-1]]["close"]
        day_chg = round((price / float(prev.iloc[-1]) - 1) * 100, 2) if len(prev) else None
        out = {"symbol": symbol, "price": round(price, 2), "day_chg": day_chg, "support": None, "resistance": None, "at_zone": "",
               "zones": [], "spurt": None, "spurt_peak": None}
        d15 = _closed_bars(ent[0], tf, asof)          # closed candles of the timeframe selected on the chart
        if d15 is not None and len(d15) >= 30:
            zs = spurt_zones.live_zones(d15, zz, nob, zatr) or []
            def _t0(z):
                try:
                    return int((d15.index[z["i0"]] - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1))
                except Exception:
                    return None
            out["zones"] = [{"type": z["type"], "lo": round(z["lo"], 2), "hi": round(z["hi"], 2), "t0": _t0(z)} for z in zs]
            sup = [z for z in zs if z["type"] == "SUPPORT"]
            res = [z for z in zs if z["type"] == "RESISTANCE"]
            zs_, zr = (max(sup, key=lambda z: z["hi"]) if sup else None), (min(res, key=lambda z: z["lo"]) if res else None)
            near = ZONE_NEAR_PCT / 100.0
            if zs_:
                out["support"] = {"lo": round(zs_["lo"], 2), "hi": round(zs_["hi"], 2), "dist": round(max(price - zs_["hi"], 0) / price * 100, 2)}
                if zs_["lo"] * (1 - near) <= price <= zs_["hi"] * (1 + near):
                    out["at_zone"] = "SUPPORT"
            if zr:
                out["resistance"] = {"lo": round(zr["lo"], 2), "hi": round(zr["hi"], 2), "dist": round(max(zr["lo"] - price, 0) / price * 100, 2)}
                if zr["lo"] * (1 - near) <= price <= zr["hi"] * (1 + near):
                    out["at_zone"] = "RESISTANCE"
        d10 = dl.drop_forming(dl.merge_bars(df5, SPURT_TF // 5), SPURT_TF, asof)
        if d10 is not None and len(d10) > SPURT_SMA + 1:
            vol = d10["volume"].to_numpy(dtype=float)
            mult = []
            for i in range(SPURT_SMA, len(vol)):
                base = vol[i - SPURT_SMA:i].mean()
                mult.append(vol[i] / base if base > 0 else 0.0)
            today_n = int(sum(1 for t in d10.index if t.date() == d10.index[-1].date()))
            out["spurt"] = {"multiple": round(mult[-1], 2), "time": _ist_label(d10.index[-1])[-5:]}
            tm = mult[-today_n:] if today_n else mult[-1:]
            if tm:
                k = int(np.argmax(tm))
                out["spurt_peak"] = {"multiple": round(tm[k], 2), "time": _ist_label(d10.index[len(d10) - len(tm) + k])[-5:]}
        out["tf"] = tf
        out["zone_cfg"] = {"zz": zz, "nob": nob, "atr": zatr}
        _ci[ck] = (time.time(), out)
        if len(_ci) > 300:
            for k, _ in sorted(list(_ci.items()), key=lambda kv: kv[1][0])[:100]:
                _ci.pop(k, None)
        return out, ""
    except Exception as e:
        log.warning(f"paper: chart info failed for {symbol}: {e}")
        return None, f"Could not read candles ({e})"


# ------------------------------------------------------------------ chart candles (every timeframe)
_cc = {}
CHART_DAYS = 8
# intraday timeframe (minutes) -> (Dhan candle size used to build it, days of history loaded)
TF_INTRA = {5: (5, 8), 15: (5, 8), 30: (15, 45), 45: (15, 75), 60: (15, 100), 120: (15, 160), 180: (15, 240), 240: (15, 300)}
TF_HTF = ("D", "W", "M")                  # daily / weekly / monthly, built from Dhan daily candles
DAILY_YEARS = 20                          # one daily download serves D, W and M
TF_ALL = tuple(str(k) for k in TF_INTRA) + TF_HTF
_bc = {}                                  # downloaded candles: key -> (time, DataFrame)
_IST_OFF = pd.Timedelta(minutes=330)


def _tf(tf, default="5"):
    s = str(tf if tf is not None else "").strip().upper()
    s = {"1D": "D", "DAY": "D", "1W": "W", "1M": "M"}.get(s, s)
    return s if s in TF_ALL else default


_bc_locks = {}
_bc_guard = threading.Lock()


def _cached(key, ttl, fn):
    """Returns the cached download, or downloads it. When the chart and the zones panel ask for the same candles at the
    same moment, only ONE download goes to Dhan and the other request waits for it (before, both downloaded = twice as slow)."""
    c = _bc.get(key)
    if c is not None and time.time() - c[0] < ttl:
        return c[1]
    with _bc_guard:
        lk = _bc_locks.setdefault(key, threading.Lock())
    with lk:
        c = _bc.get(key)
        if c is not None and time.time() - c[0] < ttl:
            return c[1]
        v = fn()
        _bc[key] = (time.time(), v)
    if len(_bc) > 80:                     # keep memory small on the 512 MB instance: forget the oldest downloads
        for k, _ in sorted(list(_bc.items()), key=lambda kv: kv[1][0])[:30]:
            _bc.pop(k, None)
            _bc_locks.pop(k, None)
    return v


def _m5(sid):
    """Last 8 days of 5-minute candles (the forming candle included), shared by the 5m/15m chart, the info panel and today's daily bar."""
    return _cached(("m5", str(sid)), 8, lambda: dl.candles(sid, "NSE_EQ", "EQUITY", CHART_DAYS, with_oi=False))


def _intraday(sid, interval, days):
    """`interval`-minute Dhan candles for the last `days` days, fetched in 88-day pieces (Dhan allows 90 days per call).
    The pieces sit on a fixed calendar grid, so 30m / 1h / 4h charts share the same downloads. Old pieces are kept for 6 hours,
    the newest piece for 10 minutes (the live part of the chart comes from the 5-minute candles, see _bars)."""
    end = datetime.now(IST).replace(tzinfo=None)
    grid0 = datetime(2020, 1, 1)
    a = grid0 + timedelta(days=88 * ((end - timedelta(days=days) - grid0).days // 88))
    parts = []
    while a < end:
        b = min(a + timedelta(days=88), end)
        recent = b >= end
        fa, fb = a.strftime("%Y-%m-%d"), b.strftime("%Y-%m-%d")

        def fetch(fa=fa, fb=fb):
            payload = {"securityId": str(sid), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "interval": interval,
                       "fromDate": fa, "toDate": fb}
            try:
                return dl.parse_intraday(dl.post("charts/intraday", payload, f"chart {sid} {interval}m").json())
            except ValueError:
                return pd.DataFrame()
        try:
            parts.append(_cached(("i", str(sid), interval, fa, fb), 600 if recent else 6 * 3600, fetch))
        except RuntimeError:
            if recent:
                raise
        a = b
    parts = [x for x in parts if x is not None and len(x)]
    if not parts:
        raise RuntimeError("no candles from Dhan for this stock")
    df = pd.concat(parts).sort_index()
    return df[~df.index.duplicated(keep="last")]


def _resample_intra(df, tf):
    """Joins intraday candles into `tf`-minute candles counted from 09:15 IST each day, like TradingView
    (so 45m / 2h / 3h / 4h never mix two days; the last candle of the day is shorter)."""
    if df is None or df.empty:
        return df
    ist = df.index + _IST_OFF
    mins = (ist.hour * 60 + ist.minute).to_numpy() - OPEN_MIN
    keep = (mins >= 0) & (mins < CLOSE_MIN - OPEN_MIN)
    df, ist, mins = df[keep], ist[keep], mins[keep]
    start = ist.normalize() + pd.to_timedelta(OPEN_MIN + (mins // tf) * tf, unit="m") - _IST_OFF
    g = df.groupby(start.to_numpy())
    out = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                        "close": g["close"].last(), "volume": g["volume"].sum()})
    out.index = pd.DatetimeIndex(out.index)
    return out.sort_index()


def _daily(sid):
    """Daily candles (about DAILY_YEARS years), each labelled with its IST date, plus today's candle built from the 5-minute candles."""
    def fetch():
        now = datetime.now(IST)
        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        d, last_err = None, None
        # A recently listed stock has no history before its listing date and Dhan answers 400 when the start date is too early.
        # So the start date is moved closer step by step until Dhan accepts it: the chart then shows whatever history exists.
        for back in [int(365.25 * DAILY_YEARS), 3650, 1825, 1095, 730, 365, 180, 90, 30, 10]:
            payload = {"securityId": str(sid), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "expiryCode": 0, "oi": False,
                       "fromDate": (now - timedelta(days=back)).strftime("%Y-%m-%d"), "toDate": now.strftime("%Y-%m-%d")}
            try:
                d = dl.parse_intraday(dl.post("charts/historical", payload, f"daily {sid}").json())
                break
            except ValueError:
                d = empty
                break
            except RuntimeError as e:
                last_err = e
                if "400 from Dhan" not in str(e):         # token problem / rate limit: not a date problem, report it as it is
                    raise
        if d is None:                                      # Dhan refused every date range: build daily candles from the last days of 5-minute candles
            try:
                m5 = _m5(sid)
                ist = m5.index + _IST_OFF
                g = m5.groupby(ist.normalize())
                d = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                                  "close": g["close"].last(), "volume": g["volume"].sum()})
                d.index = d.index - _IST_OFF
            except Exception:
                raise last_err
        if len(d):
            dates = (d.index + _IST_OFF).normalize()
            d = d[["open", "high", "low", "close", "volume"]].copy()
            d.index = dates
            d = d[~d.index.duplicated(keep="last")]
        return d
    d = _cached(("d", str(sid)), 1800, fetch).copy()             # index = IST date (midnight)
    try:                                                         # today's candle (Dhan's daily list ends yesterday while the market runs)
        m5 = _m5(sid)
        ist = m5.index + _IST_OFF
        today = pd.Timestamp(now_ist().date())
        t = m5[ist.normalize() == today]
        if len(t) and (not len(d) or d.index[-1] < today):
            d.loc[today] = [float(t["open"].iloc[0]), float(t["high"].max()), float(t["low"].min()), float(t["close"].iloc[-1]), float(t["volume"].sum())]
    except Exception:
        pass
    return d.sort_index()


def _period(d, tf):
    """Daily candles (IST-date index) -> weekly (week starts Monday) or monthly candles."""
    if d is None or d.empty:
        return d
    if tf == "W":
        key = d.index - pd.to_timedelta(d.index.weekday, unit="D")
    else:
        key = d.index.to_period("M").to_timestamp()
    g = d.groupby(key)
    out = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                        "close": g["close"].last(), "volume": g["volume"].sum()})
    out.index = pd.DatetimeIndex(out.index)
    return out.sort_index()


def _bars(sid, tf):
    """Candles of the timeframe, forming candle included, index in UTC (like every other candle in this app)."""
    if tf in TF_HTF:
        d = _daily(sid)
        if tf != "D":
            d = _period(d, tf)
        d = d.copy()
        d.index = d.index - _IST_OFF                             # IST midnight -> UTC, so the chart shows the right date
        return d
    t = int(tf)
    base, days = TF_INTRA[t]
    if base == 5:
        df = _m5(sid)
    else:
        df = _intraday(sid, base, days)
        try:                               # the last 8 days come from the live 5-minute candles, so 30m..4h charts move live too
            m5 = _m5(sid)
            if len(m5):
                cut = (m5.index[0] + _IST_OFF).normalize() - _IST_OFF        # start of the first full day in the 5-minute data
                df = pd.concat([df[df.index < cut], m5[m5.index >= cut][["open", "high", "low", "close", "volume"]]]).sort_index()
        except Exception:
            pass
    return df if t == base else _resample_intra(df, t)


def _same_period(a, b, tf):
    if tf == "D":
        return a == b
    if tf == "W":
        return (a - timedelta(days=a.weekday())) == (b - timedelta(days=b.weekday()))
    return (a.year, a.month) == (b.year, b.month)


def _closed_bars(sid, tf, asof):
    """Only finished candles (for the zones): the forming candle is left out."""
    d = _bars(sid, tf)
    if d is None or d.empty:
        return d
    if tf in TF_HTF:
        last = (d.index[-1] + _IST_OFF).date()
        if market_open() and _same_period(last, now_ist().date(), tf):
            d = d.iloc[:-1]
        return d
    return dl.drop_forming(d, int(tf), asof, session_end_utc_hour=10)   # 15:30 IST = 10:00 UTC ends the day's last candle


def candles_json(symbol, tf):
    """Candles of one stock for the chart: tf = 5/15/30/45/60/120/180/240 minutes or D/W/M (the forming candle included)."""
    symbol = (symbol or "").strip().upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return None, "Unknown symbol (NSE equities only)."
    tf = _tf(tf, "5")
    c = _cc.get((symbol, tf))
    if c and time.time() - c[0] < 8:
        return c[1], ""
    try:
        df = _bars(ent[0], tf).tail(700)
        secs = ((df.index - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)).tolist()
        bars = [{"t": int(t), "o": round(float(o), 2), "h": round(float(h), 2), "l": round(float(l), 2), "c": round(float(cl), 2), "v": int(v)}
                for t, o, h, l, cl, v in zip(secs, df["open"], df["high"], df["low"], df["close"], df["volume"])]
        if not bars:
            return None, "No candles for this stock yet."
        _cc[(symbol, tf)] = (time.time(), bars)
        return bars, ""
    except Exception as ex:
        log.warning(f"paper: chart candles failed for {symbol}: {ex}")
        return None, f"Could not read candles ({ex})"


# ------------------------------------------------------------------ trading
def _pnl(side, entry, exit_, qty):
    return (exit_ - entry) * qty * (1 if side == "BUY" else -1)


def _cost(entry, exit_, qty, product=INTRADAY):
    pct = COST_PCT_CNC if product == POSITIONAL else COST_PCT
    return (entry + exit_) * qty * pct / 100.0


def _exit_leg(a, pos, qty, price, reason, t):
    qty = min(int(qty), pos["qty"])
    if qty <= 0:
        return
    product = pos.get("product", INTRADAY)
    gross = _pnl(pos["side"], pos["entry"], price, qty)
    cost = _cost(pos["entry"], price, qty, product)
    a["cash"] += pos["entry"] * qty / _lev(pos) + gross - cost
    a["trades"].append({"pos": pos["id"], "symbol": pos["symbol"], "side": pos["side"], "product": product, "qty": qty, "qty0": pos["qty0"],
                        "entry": round(pos["entry"], 2), "exit": round(price, 2), "opened": pos["opened"],
                        "closed": t.strftime("%Y-%m-%d %H:%M:%S"), "reason": reason,
                        "gross": round(gross, 2), "cost": round(cost, 2), "net": round(gross - cost, 2)})
    _note(a, f"{pos['symbol']} ({_plabel(product)}) {reason}: {qty} sold/bought back at {price:g}, net Rs {gross - cost:,.0f}")
    pos["qty"] -= qty
    if pos["qty"] <= 0:
        a["positions"] = [p for p in a["positions"] if p["id"] != pos["id"]]
    if len(a["trades"]) > MAX_TRADES_KEPT:
        a["trades"] = a["trades"][-MAX_TRADES_KEPT:]


def _check_levels(side, ref, sl, t1, t2):
    """Returns an error text when the SL / targets are on the wrong side of the entry price."""
    if sl is None or sl <= 0:
        return "A stop-loss is required (that is the first rule of trading)."
    if side == "BUY":
        if not sl < ref:
            return f"For a BUY the stop-loss must be BELOW the entry price ({ref:g})."
        if t1 is not None and not t1 > ref:
            return "Target 1 must be ABOVE the entry price for a BUY."
        if t2 is not None and (t1 is None or not t2 > t1):
            return "Target 2 must be above Target 1 (and needs Target 1 set)."
    else:
        if not sl > ref:
            return f"For a SELL (short) the stop-loss must be ABOVE the entry price ({ref:g})."
        if t1 is not None and not t1 < ref:
            return "Target 1 must be BELOW the entry price for a SELL."
        if t2 is not None and (t1 is None or not t2 < t1):
            return "Target 2 must be below Target 1 (and needs Target 1 set)."
    return ""


def _num(v):
    try:
        if v in (None, "", "null"):
            return None
        x = float(v)
        return x if x == x and x > 0 else None
    except (TypeError, ValueError):
        return None


def place_order(user, symbol, side, qty, sl, t1=None, t2=None, limit=None, product=None):
    """Returns (ok, message). product = INTRADAY (default) or POSITIONAL."""
    symbol = (symbol or "").strip().upper()
    side = (side or "").upper()
    product = _product(product)
    ent = _sym["map"].get(symbol)
    if not ent:
        return False, "Unknown symbol (NSE equities only)."
    if side not in ("BUY", "SELL"):
        return False, "Choose Buy or Sell."
    if product == POSITIONAL and side == "SELL":
        return False, "Positional (delivery) is BUY only - shares you do not own cannot be held short overnight. Use Intraday to short."
    try:
        qty = int(qty)
    except (TypeError, ValueError):
        return False, "Quantity must be a whole number."
    if qty < 1 or qty > 1_000_000:
        return False, "Quantity must be at least 1."
    sl, t1, t2, limit = _num(sl), _num(t1), _num(t2), _num(limit)
    t = now_ist()
    if not can_enter(t):
        return False, "The market is closed for new orders (09:15 to 15:15 IST, Monday to Friday)."
    ltp, err = quote(symbol)
    if ltp is None:
        return False, err
    ref = limit if limit else ltp
    bad = _check_levels(side, ref, sl, t1, t2)
    if bad:
        return False, bad
    lev = 1 if product == POSITIONAL else LEVERAGE
    with _LOCK:
        a = acct(user)
        if any(p["symbol"] == symbol for p in a["positions"]) or any(o["symbol"] == symbol for o in a["pending"]):
            return False, f"You already have an open trade or pending order in {symbol}. Close or cancel it first."
        margin = ref * qty / lev
        if margin > a["cash"]:
            what = "the full amount" if product == POSITIONAL else "margin"
            return False, f"Not enough cash: needs Rs {margin:,.0f} ({what}), you have Rs {a['cash']:,.0f} free."
        if limit:
            if (side == "BUY" and limit >= ltp) or (side == "SELL" and limit <= ltp):
                return False, f"A limit {side} must be {'below' if side == 'BUY' else 'above'} the current price ({ltp:g}). Use a market order to trade now."
            a["pending"].append({"id": _nid(a, "O"), "symbol": symbol, "sid": ent[0], "side": side, "product": product, "qty": qty, "limit": limit,
                                 "sl": sl, "t1": t1, "t2": t2, "created": t.strftime("%H:%M:%S")})
            _save()
            return True, f"{_plabel(product)} limit order placed: {side} {qty} {symbol} at {limit:g}."
        a["cash"] -= margin
        a["positions"].append({"id": _nid(a, "P"), "symbol": symbol, "sid": ent[0], "side": side, "product": product, "qty": qty, "qty0": qty,
                               "entry": ltp, "sl": sl, "sl0": sl, "t1": t1, "t2": t2, "t1_done": False,
                               "opened": t.strftime("%Y-%m-%d %H:%M:%S")})
        _note(a, f"{_plabel(product)} {side} {qty} {symbol} filled at {ltp:g} (SL {sl:g}).")
        _save()
        return True, f"{_plabel(product)} {side} {qty} {symbol} filled at {ltp:g}."


def modify_position(user, pid, sl, t1=None, t2=None):
    """Changes the stop-loss / targets of an open trade. Checked against the LIVE price, so a new SL or target can never
    be already crossed (that would close the trade at once). After Target 1 is booked, only the SL and Target 2 can change."""
    sl, t1, t2 = _num(sl), _num(t1), _num(t2)
    with _LOCK:
        pos = next((p for p in acct(user)["positions"] if p["id"] == pid), None)
        if not pos:
            return False, "That trade is already closed."
        sid = pos["sid"]
    price = prices([sid]).get(sid)
    if price is None:
        return False, "Live price not available right now - try again in a moment."
    with _LOCK:
        a = acct(user)
        pos = next((p for p in a["positions"] if p["id"] == pid), None)
        if not pos:
            return False, "That trade is already closed."
        buy = pos["side"] == "BUY"
        if sl is None:
            return False, "A stop-loss is required."
        if buy and not sl < price:
            return False, f"For a BUY the stop-loss must be BELOW the current price ({price:g})."
        if not buy and not sl > price:
            return False, f"For a SELL the stop-loss must be ABOVE the current price ({price:g})."
        if pos["t1_done"]:                                   # half already booked at T1: T1 stays as it was
            t1 = pos["t1"]
            if t2 is not None and ((buy and not t2 > price) or (not buy and not t2 < price)):
                return False, f"Target 2 must be {'ABOVE' if buy else 'BELOW'} the current price ({price:g})."
        else:
            if t1 is not None and ((buy and not t1 > price) or (not buy and not t1 < price)):
                return False, f"Target 1 must be {'ABOVE' if buy else 'BELOW'} the current price ({price:g})."
            if t2 is not None and (t1 is None or (buy and not t2 > t1) or (not buy and not t2 < t1)):
                return False, f"Target 2 must be {'above' if buy else 'below'} Target 1 (and needs Target 1 set)."
        old = f"SL {pos['sl']:g}" + (f", T1 {pos['t1']:g}" if pos.get("t1") else "") + (f", T2 {pos['t2']:g}" if pos.get("t2") else "")
        pos["sl"], pos["t1"], pos["t2"] = sl, t1, t2
        new = f"SL {sl:g}" + (f", T1 {t1:g}" if t1 else "") + (f", T2 {t2:g}" if t2 else "")
        _note(a, f"{pos['symbol']} modified: {old} -> {new}")
        _save()
        return True, f"{pos['symbol']} updated: {new}."


def modify_order(user, oid, limit, sl, t1=None, t2=None):
    """Changes a waiting limit order (limit price, stop-loss, targets)."""
    limit, sl, t1, t2 = _num(limit), _num(sl), _num(t1), _num(t2)
    with _LOCK:
        o = next((x for x in acct(user)["pending"] if x["id"] == oid), None)
        if not o:
            return False, "That order is gone already (filled or cancelled)."
        sid = o["sid"]
    ltp = prices([sid]).get(sid)
    if ltp is None:
        return False, "Live price not available right now - try again in a moment."
    if limit is None:
        return False, "Enter the limit price."
    with _LOCK:
        a = acct(user)
        o = next((x for x in a["pending"] if x["id"] == oid), None)
        if not o:
            return False, "That order is gone already (filled or cancelled)."
        if (o["side"] == "BUY" and limit >= ltp) or (o["side"] == "SELL" and limit <= ltp):
            return False, f"A limit {o['side']} must be {'below' if o['side'] == 'BUY' else 'above'} the current price ({ltp:g})."
        bad = _check_levels(o["side"], limit, sl, t1, t2)
        if bad:
            return False, bad
        if limit * o["qty"] / _lev(o) > a["cash"]:
            return False, "Not enough cash for this limit price."
        o["limit"], o["sl"], o["t1"], o["t2"] = limit, sl, t1, t2
        _note(a, f"Limit order {o['side']} {o['symbol']} changed: limit {limit:g}, SL {sl:g}")
        _save()
        return True, f"Limit order {o['symbol']} updated."


def close_position(user, pid):
    with _LOCK:
        a = acct(user)
        pos = next((p for p in a["positions"] if p["id"] == pid), None)
        if not pos:
            return False, "That trade is already closed."
        sid = pos["sid"]
    price = prices([sid]).get(sid)
    if price is None:
        return False, "Live price not available right now - try again in a moment."
    with _LOCK:
        a = acct(user)
        pos = next((p for p in a["positions"] if p["id"] == pid), None)
        if not pos:
            return False, "That trade is already closed."
        _exit_leg(a, pos, pos["qty"], price, "MANUAL", now_ist())
        _save()
    return True, f"{pos['symbol']} closed at {price:g}."


def close_all(user):
    with _LOCK:
        ids = [p["id"] for p in acct(user)["positions"]]
    n = 0
    for pid in ids:
        ok, _ = close_position(user, pid)
        n += 1 if ok else 0
    return True, f"{n} trade(s) closed."


def cancel_order(user, oid):
    with _LOCK:
        a = acct(user)
        before = len(a["pending"])
        a["pending"] = [o for o in a["pending"] if o["id"] != oid]
        _save()
        return True, "Order cancelled." if len(a["pending"]) < before else "That order is gone already."


def reset_account(user, force=False):
    with _LOCK:
        a = acct(user)
        if a["positions"] and not force:
            return False, "Close your open trades before resetting."
        old = a
        n = _new_acct()
        n["resets"] = old.get("resets", 0) + 1
        n["joined"] = old.get("joined", n["joined"])
        n["archive"] = (old.get("archive") or []) + [{"until": now_ist().strftime("%Y-%m-%d"), "trades": len(old["trades"]),
                                                       "net": round(sum(x["net"] for x in old["trades"]), 2)}]
        _db["users"][user] = n
        _save()
        return True, f"Account reset to Rs {START_CASH:,.0f}."


# ------------------------------------------------------------------ the 5-second engine
def tick():
    """Moves every open trade and pending order forward with the latest prices: SL, targets, limit fills,
    15:20 square-off of INTRADAY trades. Only runs while the market is open (positional trades wait overnight)."""
    t = now_ist()
    if t.weekday() >= 5 or _mins(t) < OPEN_MIN or _mins(t) >= CLOSE_MIN:
        return
    with _LOCK:
        sids = {x["sid"] for a in _db["users"].values() for x in a["positions"] + a["pending"]}
    if not sids:
        return
    px = prices(sids, max_age=1.0)
    if not px:
        return
    sq = _mins(t) >= SQUAREOFF_MIN
    with _LOCK:
        for user, a in _db["users"].items():
            for o in list(a["pending"]):                                   # limit orders (day orders for both products)
                p = px.get(o["sid"])
                if p is None:
                    continue
                if sq:
                    a["pending"].remove(o)
                    _note(a, f"Limit order {o['side']} {o['symbol']} cancelled at 15:20 (not filled).")
                    continue
                hit = (o["side"] == "BUY" and p <= o["limit"]) or (o["side"] == "SELL" and p >= o["limit"])
                if not hit:
                    continue
                a["pending"].remove(o)
                margin = p * o["qty"] / _lev(o)
                bad = _check_levels(o["side"], p, o["sl"], o["t1"], o["t2"])
                if bad or margin > a["cash"]:
                    _note(a, f"Limit order {o['side']} {o['symbol']} was reached at {p:g} but dropped: " + (bad or "not enough cash"))
                    continue
                a["cash"] -= margin
                prod = o.get("product", INTRADAY)
                _note(a, f"{_plabel(prod)} limit order filled: {o['side']} {o['qty']} {o['symbol']} at {p:g}.")
                a["positions"].append({"id": _nid(a, "P"), "symbol": o["symbol"], "sid": o["sid"], "side": o["side"], "product": prod,
                                       "qty": o["qty"], "qty0": o["qty"], "entry": p, "sl": o["sl"], "sl0": o["sl"], "t1": o["t1"], "t2": o["t2"],
                                       "t1_done": False, "opened": t.strftime("%Y-%m-%d %H:%M:%S")})
            for pos in list(a["positions"]):                               # open trades
                p = px.get(pos["sid"])
                if p is None:
                    continue
                buy = pos["side"] == "BUY"
                if sq and pos.get("product", INTRADAY) != POSITIONAL:      # only intraday trades are squared off
                    _exit_leg(a, pos, pos["qty"], p, "SQUARE-OFF 15:20", t)
                    continue
                if (buy and p <= pos["sl"]) or (not buy and p >= pos["sl"]):
                    _exit_leg(a, pos, pos["qty"], p, "STOP-LOSS" if not pos["t1_done"] else "SL AT COST", t)
                    continue
                for _ in range(2):
                    if pos["qty"] <= 0:
                        break
                    tgt = pos["t2"] if pos["t1_done"] else pos["t1"]
                    if tgt is None or not ((buy and p >= tgt) or (not buy and p <= tgt)):
                        break
                    if not pos["t1_done"] and pos["t2"] is not None and pos["qty0"] >= 2:
                        _exit_leg(a, pos, max(1, pos["qty0"] // 2), tgt, "TARGET 1", t)
                        if pos["qty"] > 0:
                            pos["t1_done"], pos["sl"] = True, pos["entry"]
                    else:
                        _exit_leg(a, pos, pos["qty"], tgt, "TARGET 2" if pos["t1_done"] else "TARGET 1", t)
        _save()


def loop():
    log.info("paper engine started")
    sym_refresh()
    last_sym = time.time()
    while True:
        try:
            time.sleep(TICK_SECONDS)
            tick()
            if time.time() - last_sym > 6 * 3600:
                last_sym = time.time()
                sym_refresh()
        except Exception as e:
            log.error(f"paper tick failed: {e}")
            time.sleep(10)


# ------------------------------------------------------------------ views
def state(user):
    with _LOCK:
        a = json.loads(json.dumps(acct(user)))
    sids = [p["sid"] for p in a["positions"]]
    px = prices(sids) if sids else {}
    unreal, margin = 0.0, 0.0
    pos = []
    for p in a["positions"]:
        p.setdefault("product", INTRADAY)
        ltp = px.get(p["sid"])
        pnl = _pnl(p["side"], p["entry"], ltp, p["qty"]) if ltp else 0.0
        unreal += pnl
        margin += p["entry"] * p["qty"] / _lev(p)
        pos.append({**p, "ltp": ltp, "pnl": round(pnl, 2), "pnl_pct": round(pnl / (p["entry"] * p["qty"]) * 100, 2) if ltp else 0})
    for o in a["pending"]:
        o.setdefault("product", INTRADAY)
    today = now_ist().strftime("%Y-%m-%d")
    day_net = sum(x["net"] for x in a["trades"] if x["closed"].startswith(today)) + unreal
    equity = a["cash"] + margin + unreal
    return {"cash": round(a["cash"], 2), "margin": round(margin, 2), "unrealized": round(unreal, 2), "equity": round(equity, 2),
            "start": a["start"], "total_pnl": round(equity - a["start"], 2), "day_pnl": round(day_net, 2),
            "positions": pos, "pending": a["pending"], "market_open": market_open(), "can_enter": can_enter(),
            "price_err": prices.last_err, "symbols_loaded": len(_sym["map"]), "symbols_err": _sym["err"],
            "leverage": LEVERAGE, "cost_pct": COST_PCT, "cost_pct_cnc": COST_PCT_CNC, "notes": a.get("notes", [])[::-1]}


def positions_closed(trades):
    """Legs of one trade are joined: [{pos, symbol, side, product, net, opened, closed, reasons}] in closing order."""
    d, order = {}, []
    for x in trades:
        k = x["pos"] + "|" + x["opened"]
        if k not in d:
            d[k] = {"pos": x["pos"], "symbol": x["symbol"], "side": x["side"], "product": x.get("product", INTRADAY), "qty0": x["qty0"],
                    "entry": x["entry"], "net": 0.0, "gross": 0.0, "cost": 0.0, "opened": x["opened"], "closed": x["closed"], "reasons": []}
            order.append(k)
        r = d[k]
        r["net"] += x["net"]
        r["gross"] += x["gross"]
        r["cost"] += x["cost"]
        r["closed"] = x["closed"]
        r["exit"] = x["exit"]
        if x["reason"] not in r["reasons"]:
            r["reasons"].append(x["reason"])
    return [d[k] for k in order]


def stats(trades):
    cl = positions_closed(trades)
    n = len(cl)
    out = {"trades": n, "net": round(sum(c["net"] for c in cl), 2), "cost": round(sum(c["cost"] for c in cl), 2)}
    if not n:
        return {**out, "wins": 0, "losses": 0, "win_rate": 0, "avg_win": 0, "avg_loss": 0, "profit_factor": None, "expectancy": 0,
                "best": 0, "worst": 0, "max_dd": 0, "curve": [], "by_day": [], "by_reason": {}, "closed": []}
    w = [c["net"] for c in cl if c["net"] > 0]
    l = [c["net"] for c in cl if c["net"] <= 0]
    cum, curve, peak, dd = 0.0, [], 0.0, 0.0
    for c in cl:
        cum += c["net"]
        curve.append(round(cum, 2))
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    days = {}
    for c in cl:
        days[c["closed"][:10]] = days.get(c["closed"][:10], 0.0) + c["net"]
    br = {}
    for c in cl:
        k = c["reasons"][-1]
        br[k] = br.get(k, 0) + 1
    gl = abs(sum(l))
    return {**out, "wins": len(w), "losses": len(l), "win_rate": round(len(w) / n * 100, 1),
            "avg_win": round(sum(w) / len(w), 2) if w else 0, "avg_loss": round(sum(l) / len(l), 2) if l else 0,
            "profit_factor": round(sum(w) / gl, 2) if gl else None, "expectancy": round(out["net"] / n, 2),
            "best": round(max(c["net"] for c in cl), 2), "worst": round(min(c["net"] for c in cl), 2), "max_dd": round(dd, 2),
            "curve": curve, "by_day": [(d, round(v, 2)) for d, v in sorted(days.items())], "by_reason": br, "closed": cl}


def leaderboard():
    with _LOCK:
        snap = {u: json.loads(json.dumps(a)) for u, a in _db["users"].items()}
    allp = {p["sid"] for a in snap.values() for p in a["positions"]}
    px = prices(allp) if allp else {}
    rows = []
    for u, a in snap.items():
        un, mg = 0.0, 0.0
        for p in a["positions"]:
            ltp = px.get(p["sid"])
            if ltp:
                un += _pnl(p["side"], p["entry"], ltp, p["qty"])
            mg += p["entry"] * p["qty"] / _lev(p)
        eq = a["cash"] + mg + un
        st = stats(a["trades"])
        rows.append({"user": u, "equity": round(eq, 2), "pnl": round(eq - a["start"], 2), "pnl_pct": round((eq - a["start"]) / a["start"] * 100, 2),
                     "trades": st["trades"], "win_rate": st["win_rate"], "pf": st["profit_factor"], "resets": a.get("resets", 0)})
    rows.sort(key=lambda r: -r["pnl_pct"])
    return rows
