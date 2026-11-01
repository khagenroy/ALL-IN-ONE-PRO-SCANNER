"""
PAPER-TRADE engine - practice trading with virtual money at LIVE Dhan prices. Places no real orders, ever.
Each approved user has a virtual account (START_CASH). Orders: market or limit, buy or sell (short), always with a stop-loss and up to two targets.
 - Intraday style: margin = value / LEVERAGE is blocked while a trade is open; everything is squared off at 15:20 IST.
 - T1 with a T2 set: half the quantity is booked at T1 and the SL moves to the entry price; the rest runs to T2 or the SL.
 - Costs: a flat COST_PCT per side of the turnover is charged (an estimate of brokerage + taxes), so results are not flattered.
All data is kept in one json file on the Render disk (/var/data/paper when a disk is attached).
"""
import os
import json
import time
import threading
import logging
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

import paper_dhan as dl
import pandas as pd
import numpy as np

log = logging.getLogger("paper")
IST = ZoneInfo("Asia/Kolkata")

START_CASH = 2_500_000
LEVERAGE = 5
COST_PCT = 0.03                     # per side, % of turnover (estimate of brokerage + STT + charges)
OPEN_MIN = 9 * 60 + 15
LAST_ENTRY_MIN = 15 * 60 + 15       # no new orders after this
SQUAREOFF_MIN = 15 * 60 + 20
CLOSE_MIN = 15 * 60 + 30
TICK_SECONDS = 5
MAX_TRADES_KEPT = 5000
PRICE_CACHE_S = 2.0
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


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


def chart_info(symbol, tf=15):
    """Nearest support / resistance zone, volume-spurt multiple and day change of one stock. Cached for 60 s. Returns (dict, error)."""
    symbol = (symbol or "").strip().upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return None, "Unknown symbol."
    try:
        tf = int(tf)
    except (TypeError, ValueError):
        tf = 15
    if tf not in (5, 15, 60):
        tf = 15
    c = _ci.get((symbol, tf))
    if c and time.time() - c[0] < 120:
        return c[1], ""
    try:
        import paper_zones as spurt_zones
        df5 = dl.candles(ent[0], "NSE_EQ", "EQUITY", 8, with_oi=False)
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
        d15 = dl.drop_forming(dl.merge_bars(df5, tf // 5) if tf > 5 else df5, tf, asof)
        if d15 is not None and len(d15) >= 30:
            zs = spurt_zones.live_zones(d15) or []
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
        _ci[(symbol, tf)] = (time.time(), out)
        return out, ""
    except Exception as e:
        log.warning(f"paper: chart info failed for {symbol}: {e}")
        return None, f"Could not read candles ({e})"


# ------------------------------------------------------------------ chart candles
_cc = {}
CHART_DAYS = 8


def candles_json(symbol, tf):
    """Candles of one stock for the chart: tf = 5 / 15 / 60 minutes (built from Dhan 5-minute candles, the forming candle included)."""
    symbol = (symbol or "").strip().upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return None, "Unknown symbol (NSE equities only)."
    try:
        tf = int(tf)
    except (TypeError, ValueError):
        tf = 5
    if tf not in (5, 15, 60):
        tf = 5
    c = _cc.get((symbol, tf))
    if c and time.time() - c[0] < 8:
        return c[1], ""
    try:
        df = dl.candles(ent[0], "NSE_EQ", "EQUITY", CHART_DAYS, with_oi=False)
        if tf > 5:
            df = dl.merge_bars(df, tf // 5)
        df = df.tail(700)
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


def _cost(entry, exit_, qty):
    return (entry + exit_) * qty * COST_PCT / 100.0


def _exit_leg(a, pos, qty, price, reason, t):
    qty = min(int(qty), pos["qty"])
    if qty <= 0:
        return
    gross = _pnl(pos["side"], pos["entry"], price, qty)
    cost = _cost(pos["entry"], price, qty)
    a["cash"] += pos["entry"] * qty / LEVERAGE + gross - cost
    a["trades"].append({"pos": pos["id"], "symbol": pos["symbol"], "side": pos["side"], "qty": qty, "qty0": pos["qty0"],
                        "entry": round(pos["entry"], 2), "exit": round(price, 2), "opened": pos["opened"],
                        "closed": t.strftime("%Y-%m-%d %H:%M:%S"), "reason": reason,
                        "gross": round(gross, 2), "cost": round(cost, 2), "net": round(gross - cost, 2)})
    _note(a, f"{pos['symbol']} {reason}: {qty} sold/bought back at {price:g}, net Rs {gross - cost:,.0f}")
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


def place_order(user, symbol, side, qty, sl, t1=None, t2=None, limit=None):
    """Returns (ok, message)."""
    symbol = (symbol or "").strip().upper()
    side = (side or "").upper()
    ent = _sym["map"].get(symbol)
    if not ent:
        return False, "Unknown symbol (NSE equities only)."
    if side not in ("BUY", "SELL"):
        return False, "Choose Buy or Sell."
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
    with _LOCK:
        a = acct(user)
        if any(p["symbol"] == symbol for p in a["positions"]) or any(o["symbol"] == symbol for o in a["pending"]):
            return False, f"You already have an open trade or pending order in {symbol}. Close or cancel it first."
        margin = ref * qty / LEVERAGE
        if margin > a["cash"]:
            return False, f"Not enough margin: needs Rs {margin:,.0f}, you have Rs {a['cash']:,.0f} free."
        if limit:
            if (side == "BUY" and limit >= ltp) or (side == "SELL" and limit <= ltp):
                return False, f"A limit {side} must be {'below' if side == 'BUY' else 'above'} the current price ({ltp:g}). Use a market order to trade now."
            a["pending"].append({"id": _nid(a, "O"), "symbol": symbol, "sid": ent[0], "side": side, "qty": qty, "limit": limit,
                                 "sl": sl, "t1": t1, "t2": t2, "created": t.strftime("%H:%M:%S")})
            _save()
            return True, f"Limit order placed: {side} {qty} {symbol} at {limit:g}."
        a["cash"] -= margin
        a["positions"].append({"id": _nid(a, "P"), "symbol": symbol, "sid": ent[0], "side": side, "qty": qty, "qty0": qty,
                               "entry": ltp, "sl": sl, "sl0": sl, "t1": t1, "t2": t2, "t1_done": False,
                               "opened": t.strftime("%Y-%m-%d %H:%M:%S")})
        _note(a, f"{side} {qty} {symbol} filled at {ltp:g} (SL {sl:g}).")
        _save()
        return True, f"{side} {qty} {symbol} filled at {ltp:g}."


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
    """Moves every open trade and pending order forward with the latest prices: SL, targets, limit fills, 15:20 square-off."""
    t = now_ist()
    if t.weekday() >= 5 or _mins(t) < OPEN_MIN:
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
            for o in list(a["pending"]):                                   # limit orders
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
                margin = p * o["qty"] / LEVERAGE
                bad = _check_levels(o["side"], p, o["sl"], o["t1"], o["t2"])
                if bad or margin > a["cash"]:
                    _note(a, f"Limit order {o['side']} {o['symbol']} was reached at {p:g} but dropped: " + (bad or "not enough margin"))
                    continue
                a["cash"] -= margin
                _note(a, f"Limit order filled: {o['side']} {o['qty']} {o['symbol']} at {p:g}.")
                a["positions"].append({"id": _nid(a, "P"), "symbol": o["symbol"], "sid": o["sid"], "side": o["side"], "qty": o["qty"],
                                       "qty0": o["qty"], "entry": p, "sl": o["sl"], "sl0": o["sl"], "t1": o["t1"], "t2": o["t2"],
                                       "t1_done": False, "opened": t.strftime("%Y-%m-%d %H:%M:%S")})
            for pos in list(a["positions"]):                               # open trades
                p = px.get(pos["sid"])
                if p is None:
                    continue
                buy = pos["side"] == "BUY"
                if sq:
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
        ltp = px.get(p["sid"])
        pnl = _pnl(p["side"], p["entry"], ltp, p["qty"]) if ltp else 0.0
        unreal += pnl
        margin += p["entry"] * p["qty"] / LEVERAGE
        pos.append({**p, "ltp": ltp, "pnl": round(pnl, 2), "pnl_pct": round(pnl / (p["entry"] * p["qty"]) * 100, 2) if ltp else 0})
    today = now_ist().strftime("%Y-%m-%d")
    day_net = sum(x["net"] for x in a["trades"] if x["closed"].startswith(today)) + unreal
    equity = a["cash"] + margin + unreal
    return {"cash": round(a["cash"], 2), "margin": round(margin, 2), "unrealized": round(unreal, 2), "equity": round(equity, 2),
            "start": a["start"], "total_pnl": round(equity - a["start"], 2), "day_pnl": round(day_net, 2),
            "positions": pos, "pending": a["pending"], "market_open": market_open(), "can_enter": can_enter(),
            "price_err": prices.last_err, "symbols_loaded": len(_sym["map"]), "symbols_err": _sym["err"],
            "leverage": LEVERAGE, "cost_pct": COST_PCT, "notes": a.get("notes", [])[::-1]}


def positions_closed(trades):
    """Legs of one trade are joined: [{pos, symbol, side, net, opened, closed, reasons}] in closing order."""
    d, order = {}, []
    for x in trades:
        k = x["pos"] + "|" + x["opened"]
        if k not in d:
            d[k] = {"pos": x["pos"], "symbol": x["symbol"], "side": x["side"], "qty0": x["qty0"], "entry": x["entry"], "net": 0.0,
                    "gross": 0.0, "cost": 0.0, "opened": x["opened"], "closed": x["closed"], "reasons": []}
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
            mg += p["entry"] * p["qty"] / LEVERAGE
        eq = a["cash"] + mg + un
        st = stats(a["trades"])
        rows.append({"user": u, "equity": round(eq, 2), "pnl": round(eq - a["start"], 2), "pnl_pct": round((eq - a["start"]) / a["start"] * 100, 2),
                     "trades": st["trades"], "win_rate": st["win_rate"], "pf": st["profit_factor"], "resets": a.get("resets", 0)})
    rows.sort(key=lambda r: -r["pnl_pct"])
    return rows
