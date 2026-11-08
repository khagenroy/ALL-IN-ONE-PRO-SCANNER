"""
Daily volume spurt + daily support / resistance zones (page /spurt_daily).

WHAT IT DOES
  Once a day after the market closes (16:45 IST, Mon-Fri) it looks at every liquid NSE stock and compares TODAY's full-day volume with the
  average of the last 50 daily volumes. Stocks at DAILY_MIN_MULT (2x) or more are listed, biggest multiple first, together with the
  support / resistance zones of the DAILY chart (same ALL IN ONE PRO order-block rules as the 10-minute page, from spurt_zones.live_zones,
  just on daily candles). A row turns green when the price is AT a daily support zone and red when it is AT a daily resistance zone.
  A phone alert (ntfy, same NTFY_TOPIC) is sent for stocks with a multiple of DAILY_ALERT_MULT (3x) or more that sit at a zone.

LIQUID = price >= 50, traded at least Rs 1 crore today, and (when Dhan sends a depth) a bid-ask spread of 0.25% or less - the same rule as
the 10-minute spurt page. ETFs are left out (the same universe as /spurt10).

IT DOES NOT TOUCH the 10-minute spurt scan, the SELL signals, any signal / entry / SL / target logic. Read-only: it only reads Dhan
quotes and daily candles and writes its own files (results/spurt_daily.json / .csv / _history.csv).

MEMORY: one stock at a time, only small numbers are kept; the daily candles of a stock are thrown away after it is scored. It never runs
at start-up - only once a day after 16:45 IST (a catch-up run, 15 min after a restart, only if today's scan is missing).
No env variables needed (defaults in the code).
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import live_scanner as ls
import spurt10_live as sp
import spurt_zones

log = logging.getLogger("spurt_daily")
IST = sp.IST

AUTORUN = os.environ.get("SPURT_DAILY_AUTORUN", "true").strip().lower() == "true"
AVG_LEN = 50                 # days in the volume average
MIN_HISTORY = 30             # a stock needs at least this many earlier daily candles
MIN_MULT = 2.0               # listed from this multiple of the 50-day average
ALERT_MULT = 3.0             # phone alert (when also at a zone) from this multiple
MIN_AVG_VOL = 1000           # ignore stocks whose 50-day average volume is below this many shares
MIN_TURNOVER = 1e7           # liquid: traded at least Rs 1 crore today
SPLIT_JUMP = 0.30            # a day-to-day close change above this (30%) in the history = split / bonus / demerger (Dhan's daily history is not adjusted): older candles are ignored
MAX_ZONE_PCT = 0.25          # a zone wider than this share of the price is a data glitch and is dropped
HIST_DAYS = 420              # calendar days of daily candles to read (about 280 trading days)
RUN_AT_MIN = 16 * 60 + 45    # 16:45 IST (the swing scan runs at 16:00, so this stays clear of it)
TOP_N = 100
ALERT_MAX_LINES = 12

RESULTS_DIR = sp.RESULTS_DIR
JSON_PATH = os.path.join(RESULTS_DIR, "spurt_daily.json")
CSV_PATH = os.path.join(RESULTS_DIR, "spurt_daily.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt_daily_history.csv")

_state = {"last_run_date": None, "running": False}


# ---------------------------------------------------------------- data
def _fetch_quotes(uni):
    """{symbol: dict(px, vol, open, high, low, spread)} via marketfeed/quote, 1000 ids per call."""
    by_id = {sid: sym for sym, sid in uni}
    ids = [int(s) for s in by_id]
    out = {}
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": chunk}, f"daily quote batch {i // 1000 + 1}")
        data = (resp.json().get("data") or {}).get("NSE_EQ") or {}
        for sid, q in data.items():
            sym = by_id.get(str(sid))
            if not sym or not isinstance(q, dict):
                continue
            try:
                px = float(q.get("last_price"))
                vol = float(q.get("volume"))
            except (TypeError, ValueError):
                continue
            oh = q.get("ohlc") or {}
            try:
                o, h, l = float(oh.get("open")), float(oh.get("high")), float(oh.get("low"))
            except (TypeError, ValueError):
                o = h = l = px
            dp = sp._parse_depth(q)
            out[sym] = {"px": px, "vol": vol, "open": o or px, "high": h or px, "low": l or px, "spread": (dp[0] if dp else None), "bq": (dp[3] if dp and len(dp) > 4 else None), "sq": (dp[4] if dp and len(dp) > 4 else None), "circ": sp._circuit_side(q, px)}
        time.sleep(1.1)
    return out


def _low_depth(d):
    """ADDED 4 Nov: True when the total waiting bid or ask quantity (shares) is below sp.LIQ_MIN_DEPTH_QTY, or the price is near a circuit price (no depth data = False)."""
    try:
        if d.get("circ"):                                 # ADDED 4 Nov: within CIRCUIT_NEAR_PCT % of the upper / lower circuit price
            return True
        m = sp.LIQ_MIN_DEPTH_QTY
        return bool(m > 0 and d.get("bq") is not None and d.get("sq") is not None and (d["bq"] < m or d["sq"] < m))
    except Exception:
        return False


def _liquid(quotes):
    names = []
    for sym, d in quotes.items():
        if d["px"] < sp.MIN_PRICE or d["px"] * d["vol"] < MIN_TURNOVER:
            continue
        if d["spread"] is not None and d["spread"] > sp.LIQ_SPREAD_THIN:
            continue
        if _low_depth(d):
            continue
        names.append(sym)
    return names


def _to_date(ts):
    t = float(ts)
    if t < 1e9:                       # seconds counted from 1980-01-01 (Dhan's other timestamp style)
        t += 315513000
    return datetime.fromtimestamp(t, IST).date()


def _daily_candles(sym, sid, today):
    """Daily candles up to and including today (oldest first, with a date column 'd'), or None. Never raises."""
    frm = (today - timedelta(days=HIST_DAYS)).strftime("%Y-%m-%d")
    j = None
    for to in ((today + timedelta(days=1)), today):
        try:
            resp = ls._dhan_post("charts/historical", {"securityId": str(sid), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY",
                                                       "expiryCode": 0, "oi": False, "fromDate": frm, "toDate": to.strftime("%Y-%m-%d")},
                                 f"daily candles {sym}")
            j = resp.json()
            if j and j.get("close"):
                break
        except Exception:
            j = None
    if not j or not j.get("close"):
        return None
    df = pd.DataFrame({k: j.get(k) for k in ("open", "high", "low", "close", "volume", "timestamp")})
    df = df.dropna()
    df["d"] = [_to_date(t) for t in df["timestamp"]]
    df = df.sort_values("d").reset_index(drop=True)
    return df if len(df) else None


def _score(sym, sid, q, today):
    """q = the stock's quote (price / volume of today), or None = quote step unavailable: today's candle from the daily candles is used instead."""
    raw = _daily_candles(sym, sid, today)
    if raw is None:
        return {"_nodata": True}
    _oc = ["open", "high", "low", "close", "volume"]
    df = raw[raw["d"] < today][_oc + ["d"]].reset_index(drop=True)                                              # earlier days only (+ the date, for the zones)
    df[_oc] = df[_oc].astype(float)
    if q is None:
        t = raw[raw["d"] == today]
        if t.empty:
            return {"_nodata": True}
        t = t.iloc[-1]
        q = {"px": float(t["close"]), "vol": float(t["volume"]), "open": float(t["open"]), "high": float(t["high"]), "low": float(t["low"]), "spread": None}
        if q["px"] < sp.MIN_PRICE or q["px"] * q["vol"] < MIN_TURNOVER:
            return {"_skip": True, "_illiq": True}                  # not liquid
    cl = df["close"].to_numpy(dtype=float)
    if len(cl) > 1:                                                 # corporate action in the history: use only the candles after the last jump
        with np.errstate(divide="ignore", invalid="ignore"):
            chg = np.abs(cl[1:] / cl[:-1] - 1.0)
        jumps = np.where(np.isfinite(chg) & (chg > SPLIT_JUMP))[0]
        if len(jumps):
            df = df.iloc[jumps[-1] + 1:].reset_index(drop=True)
    if len(df) < MIN_HISTORY:
        return None
    avg = float(df["volume"].tail(AVG_LEN).mean())
    if avg < MIN_AVG_VOL:
        return None
    mult = q["vol"] / avg
    if mult < MIN_MULT:
        return {"_skip": True}
    prev_close = float(df["close"].iloc[-1])
    day_chg = round((q["px"] - prev_close) / prev_close * 100.0, 2) if prev_close > 0 else None
    if day_chg is not None and abs(day_chg) > SPLIT_JUMP * 100.0:   # a move this big in one day = ex-date of a split / demerger, not a real spurt
        return {"_skip": True}
    today_bar = pd.DataFrame([{"open": q["open"], "high": q["high"], "low": q["low"], "close": q["px"], "volume": q["vol"]}])
    zdf = pd.concat([df[_oc], today_bar], ignore_index=True)
    try:                                                            # real dates as index: the zones then count weekends / holidays like the indicator does
        zdf.index = pd.DatetimeIndex(pd.to_datetime(list(df["d"]) + [today]))
    except Exception:
        zdf = zdf.reset_index(drop=True)
    zs = [z for z in (spurt_zones.live_zones(zdf) or [])
          if (z["hi"] - z["lo"]) <= MAX_ZONE_PCT * q["px"]]
    row = {"symbol": sym, "price": round(q["px"], 2), "day_chg_pct": day_chg, "day_vol": int(q["vol"]), "avg_vol": int(avg),
           "multiple": round(mult, 2), "ohlc": (q["open"], q["high"], q["low"], q["px"])}
    row.update(sp._zone_fields(zs, q["px"], day_chg))
    row["zone_src"] = "Daily"
    return row


# ---------------------------------------------------------------- scan
def run_once():
    t0 = time.monotonic()
    now = datetime.now(IST)
    today = now.date()
    uni = sp._load_universe()
    sid_of = dict(uni)
    try:
        quotes = _fetch_quotes(uni)
        names = _liquid(quotes)
        if len(names) < 50:                               # holiday / no data: do not overwrite anything
            log.info(f"Spurt daily: only {len(names)} liquid stocks have traded today - nothing to scan (market holiday?)")
            return False
        mode = "quote"
    except Exception as e:                                # e.g. Dhan answers HTTP 401 to the quote call after hours: use the daily candles only
        log.warning(f"Spurt daily: quote step not available ({e}) - using today's daily candle instead (slower: every stock's candles are read)")
        quotes, names, mode = None, [s_ for s_, _i in uni], "candle"
    rows, failed, scanned, nodata, illiq = [], 0, 0, 0, 0
    for sym in names:
        try:
            r = _score(sym, sid_of[sym], (quotes[sym] if quotes else None), today)
        except Exception as e:
            failed += 1
            if failed <= 3:
                log.warning(f"Spurt daily: {sym} failed: {e}")
            if failed >= 40 and failed > scanned // 2:
                raise RuntimeError("too many daily-candle failures - Dhan problem?")
            continue
        scanned += 1
        if r and r.get("_nodata"):
            nodata += 1
        elif r and r.get("_illiq"):
            illiq += 1
        elif r and not r.get("_skip"):
            rows.append(r)
        time.sleep(0.15)
        if scanned == 60 and nodata >= 54:                # Dhan sends no candles: stop early, nothing is written
            raise RuntimeError("Dhan sent no daily candles (HTTP / token problem?)")
    if mode == "candle" and nodata > 0.9 * max(scanned, 1):
        log.info("Spurt daily: no stock has a daily candle for today - market holiday? Nothing written.")
        return False
    rows.sort(key=lambda r: -r["multiple"])
    meta = {"run": now.strftime("%Y-%m-%d %H:%M:%S IST"), "date": today.strftime("%Y-%m-%d"), "took_s": round(time.monotonic() - t0),
            "universe": len(uni), "liquid": (len(names) if mode == "quote" else scanned - nodata - illiq), "scored": scanned, "failed": failed, "listed": len(rows),
            "min_mult": MIN_MULT, "alert_mult": ALERT_MULT, "avg_len": AVG_LEN}
    _write(rows, meta)
    try:
        _append_history(rows, meta)
    except Exception as e:
        log.warning(f"Spurt daily history failed: {e}")
    try:
        _send_alert(rows, meta)
    except Exception as e:
        log.warning(f"Spurt daily alert failed: {e}")
    log.info(f"Spurt daily: {len(rows)} stocks at {MIN_MULT:g}x or more, {len(names)} liquid stocks scanned in {meta['took_s']}s")
    return True


def _write(rows, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    flat = [{k: v for k, v in r.items() if k != "ohlc"} for r in rows]
    pd.DataFrame(flat).to_csv(CSV_PATH + ".tmp", index=False)
    os.replace(CSV_PATH + ".tmp", CSV_PATH)
    with open(JSON_PATH + ".tmp", "w") as f:
        json.dump({"meta": meta, "rows": rows}, f)
    os.replace(JSON_PATH + ".tmp", JSON_PATH)


def _append_history(rows, meta):
    if not rows:
        return
    cols = ["date", "symbol", "price", "day_chg_pct", "day_vol", "avg_vol", "multiple", "support", "support_dist_pct",
            "resistance", "resistance_dist_pct", "zone_view", "at_zone"]
    df = pd.DataFrame([{**{k: r.get(k) for k in cols if k != "date"}, "date": meta["date"]} for r in rows])[cols]
    if os.path.exists(HIST_PATH):
        old = pd.read_csv(HIST_PATH)
        old = old[old["date"] != meta["date"]]
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(HIST_PATH + ".tmp", index=False)
    os.replace(HIST_PATH + ".tmp", HIST_PATH)


def _send_alert(rows, meta):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (sp.NTFY_ON and topic):
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and r.get("at_zone") in ("SUPPORT", "RESISTANCE")]
    try:                                                      # ADDED: no alert for a stock with important news (spurt10_live.news_symbols)
        import spurt10_live as _s10
        _nws = _s10.news_symbols()
        hits = [r for r in hits if r["symbol"] not in _nws]
    except Exception:
        pass
    if not hits:
        return
    lines = []
    for r in hits[:ALERT_MAX_LINES]:
        zone = r.get("support") if r["at_zone"] == "SUPPORT" else r.get("resistance")
        lines.append(f"{r['at_zone']}  {r['symbol']}  Rs {r['price']:.2f}  daily spurt {r['multiple']:.1f}x  zone {zone}"
                     + (f"  day {r['day_chg_pct']:+.2f}%" if r.get("day_chg_pct") is not None else ""))
    extra = f"\n+{len(hits) - ALERT_MAX_LINES} more on /spurt_daily" if len(hits) > ALERT_MAX_LINES else ""
    import requests
    last = None
    for attempt in range(4):                                  # the alert is sent only once a day: retry a short network problem (e.g. "Network is unreachable")
        try:
            resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                                 headers={"Title": f"Daily volume spurt at a daily zone ({len(hits)})", "Priority": "default", "Tags": "bar_chart"})
            if resp.status_code >= 300:
                log.warning(f"Spurt daily ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
        except Exception as e:
            last = e
            time.sleep(30)
    raise RuntimeError(f"ntfy not reachable after 4 tries: {last}")


# ---------------------------------------------------------------- ADDED: SAME-DAY alert - stock with daily volume spurt touches its DAILY resistance zone
TOUCH_ON = True              # phone alert, the same day, when a stock whose volume so far today is TOUCH_MIN_MULT x its 50-day average touches its daily resistance zone. False = off
TOUCH_MIN_MULT = MIN_MULT    # volume so far today / 50-day average volume (2x = the same as the /spurt_daily list)
TOUCH_NEAR_PCT = sp.ZONE_NEAR_PCT   # "touched" = the day's high reached the zone bottom less this % (same 'near' rule as the other zone alerts)
TOUCH_EVERY_S = 30           # seconds between two looks (one quote call per 1000 stocks; the day high catches a touch between two looks)
PREP_FROM_MIN = 8 * 60 + 45  # the 50-day average and the daily zones of every stock are prepared from this time (earlier days only, ~10 min), once a day
TOUCH_FROM_MIN = 9 * 60 + 20
TOUCH_TO_MIN = 15 * 60 + 25
TOUCH_MAX_LINES = 12
PREP_PATH = os.path.join(RESULTS_DIR, "spurt_daily_prep.json")           # today's 50-day averages + daily resistance zones (small; a restart does not redo it)
TOUCH_PATH = os.path.join(RESULTS_DIR, "spurt_daily_touch.csv")          # one line per stock per day (history stays)
TOUCH_STATE = os.path.join(RESULTS_DIR, "spurt_daily_touch_state.json")  # stocks already alerted today (a restart does not repeat the alert)
TOUCH_COLS = ["date", "time", "symbol", "price", "day_high", "resistance", "daily_spurt_x", "day_vol", "avg_vol", "day_chg_pct", "where",
              "ret_10m", "ret_30m", "ret_60m", "max_up_pct", "max_down_pct", "last_pct", "last_time", "result",
              "nifty_at_alert", "nifty_10m", "nifty_30m", "nifty_60m", "nifty_last", "rel_10m", "rel_30m", "rel_60m", "rel_last", "nifty_day_pct"]
# ret_* = follow-up after the alert (+ = price up from the alert price); nifty_* = NIFTY's move over the same time; rel_* = stock minus NIFTY (the market-adjusted move)


def _zone_nums(txt):
    try:
        lo, hi = str(txt).split("-")
        lo, hi = float(lo), float(hi)
        return (lo, hi) if lo > 0 and hi >= lo else None
    except Exception:
        return None


def _prep_one(sym, sid, today):
    """{avg, close, resistance 'lo-hi'} from the EARLIER daily candles of a stock (same cleaning as _score), or None. Never raises."""
    try:
        raw = _daily_candles(sym, sid, today)
        if raw is None:
            return {"_nodata": True}
        _oc = ["open", "high", "low", "close", "volume"]
        df = raw[raw["d"] < today][_oc + ["d"]].reset_index(drop=True)
        df[_oc] = df[_oc].astype(float)
        cl = df["close"].to_numpy(dtype=float)
        if len(cl) > 1:
            with np.errstate(divide="ignore", invalid="ignore"):
                chg = np.abs(cl[1:] / cl[:-1] - 1.0)
            jumps = np.where(np.isfinite(chg) & (chg > SPLIT_JUMP))[0]
            if len(jumps):
                df = df.iloc[jumps[-1] + 1:].reset_index(drop=True)
        if len(df) < MIN_HISTORY:
            return None
        avg = float(df["volume"].tail(AVG_LEN).mean())
        close = float(df["close"].iloc[-1])
        if avg < MIN_AVG_VOL or close < sp.MIN_PRICE * 0.8 or avg * close < MIN_TURNOVER / TOUCH_MIN_MULT * 0.8:
            return None
        zdf = df[_oc].copy()
        try:
            zdf.index = pd.DatetimeIndex(pd.to_datetime(list(df["d"])))
        except Exception:
            zdf = zdf.reset_index(drop=True)
        zs = [z for z in (spurt_zones.live_zones(zdf) or []) if (z["hi"] - z["lo"]) <= MAX_ZONE_PCT * close]
        res = [z for z in zs if z["type"] == "RESISTANCE" and z["lo"] >= close * 0.97]      # nearest resistance above the last close (a little tolerance for a gap down)
        if not res:
            return None
        z = min(res, key=lambda z: z["lo"])
        return {"avg": avg, "close": close, "resistance": f"{z['lo']:g}-{z['hi']:g}",
                "res_all": [f"{x['lo']:g}-{x['hi']:g}" for x in sorted(res, key=lambda x: x["lo"])]}      # ADDED 7 Nov: every live daily resistance zone (as the indicator draws them) for /daily_rej
    except Exception as e:
        log.warning(f"Spurt daily prep {sym} failed: {e}")
        return None


def _prep_today(today):
    """Prepares (once a day) the 50-day average volume and the daily resistance zone of every stock. Returns {symbol: {...}} or None."""
    today_s = today.strftime("%Y-%m-%d")
    try:
        with open(PREP_PATH) as f:
            d = json.load(f)
        if d.get("date") == today_s and d.get("stocks"):
            return d["stocks"]
    except Exception:
        pass
    uni = sp._load_universe()
    out, scanned, nodata = {}, 0, 0
    for sym, sid in uni:
        r = _prep_one(sym, sid, today)
        scanned += 1
        if r and r.get("_nodata"):
            nodata += 1
        elif r:
            out[sym] = r
        time.sleep(0.15)
        if scanned == 60 and nodata >= 54:
            raise RuntimeError("Dhan sent no daily candles (HTTP / token problem?)")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(PREP_PATH + ".tmp", "w") as f:
        json.dump({"date": today_s, "stocks": out}, f)
    os.replace(PREP_PATH + ".tmp", PREP_PATH)
    log.info(f"Spurt daily touch: prepared {len(out)} stocks (of {len(uni)}) with a 50-day average and a daily resistance zone")
    return out


def _touch_notify(hits):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (sp.NTFY_ON and topic and hits):
        return
    import requests
    lines = [f"{h['symbol']}  Rs {h['price']:.2f}  (day high {h['day_high']:.2f})  daily resistance {h['resistance']}  volume today {h['daily_spurt_x']:.1f}x of 50-day avg ({h['where']})"
             for h in hits[:TOUCH_MAX_LINES]]
    extra = f"\n+{len(hits) - TOUCH_MAX_LINES} more on /spurt_daily_touch.csv" if len(hits) > TOUCH_MAX_LINES else ""
    try:
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Volume spurt stock at DAILY resistance ({len(hits)})", "Priority": "default", "Tags": "chart_with_upwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt daily touch ntfy refused: {resp.status_code} {resp.text[:150]}")
    except Exception as e:
        log.warning(f"Spurt daily touch ntfy failed: {e}")


TRACK_MOVE = 0.3             # % move from the price at the alert: at least this much DOWN = REJECTED, UP = BROKE_OUT, in between = FLAT


def _touch_follow(row, px, now, nifty=None):
    """ADDED: follow-up of a stock after it reached the daily resistance: % move from the price at the alert (+ = up). Never raises."""
    try:
        mv = round((px / row["price"] - 1.0) * 100.0, 2)
        el = now.hour * 60 + now.minute - (int(row["time"][:2]) * 60 + int(row["time"][3:5]))
        row["max_up_pct"] = mv if row.get("max_up_pct") in (None, "") else max(row["max_up_pct"], mv)
        row["max_down_pct"] = mv if row.get("max_down_pct") in (None, "") else min(row["max_down_pct"], mv)
        row["last_pct"], row["last_time"] = mv, now.strftime("%H:%M")
        nm = None
        if nifty and row.get("nifty_at_alert") not in (None, ""):
            nm = round((nifty / float(row["nifty_at_alert"]) - 1.0) * 100.0, 2)
            row["nifty_last"], row["rel_last"] = nm, round(mv - nm, 2)
        for lab, nlab, rlab, mm in (("ret_10m", "nifty_10m", "rel_10m", 10), ("ret_30m", "nifty_30m", "rel_30m", 30), ("ret_60m", "nifty_60m", "rel_60m", 60)):
            if row.get(lab) in (None, "") and el >= mm:
                row[lab] = mv
                if nm is not None:
                    row[nlab], row[rlab] = nm, round(mv - nm, 2)
        done = row.get("ret_60m") not in (None, "") or now.hour * 60 + now.minute >= TOUCH_TO_MIN
        eff = mv - nm if nm is not None else mv                    # the result is judged on the move MINUS NIFTY's move (when NIFTY is known)
        row["result"] = ("open" if not done else "BROKE_OUT" if eff >= TRACK_MOVE else "REJECTED" if eff <= -TRACK_MOVE else "FLAT")
    except Exception as e:
        log.warning(f"Spurt daily touch follow-up failed: {e}")


def _touch_write(state, today_s):
    """Today's rows are rewritten every cycle (older days stay)."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    df = pd.DataFrame(list(state["track"].values()), columns=TOUCH_COLS)
    if os.path.exists(TOUCH_PATH):
        try:
            old = pd.read_csv(TOUCH_PATH)
            df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
        except Exception as e:
            log.warning(f"Spurt daily touch csv could not be read, appending anyway: {e}")
    df.to_csv(TOUCH_PATH + ".tmp", index=False)
    os.replace(TOUCH_PATH + ".tmp", TOUCH_PATH)
    with open(TOUCH_STATE + ".tmp", "w") as f:
        json.dump({"date": today_s, "done": state["done"], "track": state["track"]}, f)
    os.replace(TOUCH_STATE + ".tmp", TOUCH_STATE)


def _touch_cycle(state, now, prep):
    today_s = now.strftime("%Y-%m-%d")
    uni = dict(sp._load_universe())
    pairs = [(s_, uni[s_]) for s_ in prep if s_ in uni]
    if not pairs:
        return
    quotes = _fetch_quotes(pairs)                         # price, day volume, day high of the prepared stocks (1000 per call)
    try:
        _nws = sp.news_symbols()                          # same as the daily alert: a stock with important news is left to the news section
    except Exception:
        _nws = set()
    near = TOUCH_NEAR_PCT / 100.0
    nifty, _np = sp._nifty_px()                           # NIFTY baseline (from the indices strip, no extra Dhan call)
    new, changed = [], False
    rej_c = {}                                            # ADDED: stocks that qualify for the rejection check (same filters as the touch alert)
    for sym, q in quotes.items():
        try:
            _p = prep.get(sym)
            if _p and sym not in _nws and q["px"] >= sp.MIN_PRICE and q["px"] * q["vol"] >= MIN_TURNOVER and not (q["spread"] is not None and q["spread"] > sp.LIQ_SPREAD_THIN) \
                    and not _low_depth(q) and q["vol"] / _p["avg"] >= TOUCH_MIN_MULT:
                _lo, _zh = _zone_nums(_p["resistance"])
                if max(q["high"], q["px"]) >= _lo * (1 - near):
                    rej_c[sym] = (_p["resistance"], q["vol"] / _p["avg"], (q["px"] / _p["close"] - 1) * 100)
        except Exception:
            pass
    for sym, q in quotes.items():
        if sym in state["track"]:                         # already alerted today: follow it
            if state["track"][sym].get("result") in (None, "", "open"):
                _touch_follow(state["track"][sym], q["px"], now, nifty)
                changed = True
            continue
        if sym in _nws:
            continue
        p = prep.get(sym)
        if not p or q["px"] < sp.MIN_PRICE or q["px"] * q["vol"] < MIN_TURNOVER:
            continue
        if q["spread"] is not None and q["spread"] > sp.LIQ_SPREAD_THIN:
            continue
        if _low_depth(q):                                 # ADDED 4 Nov: too few shares waiting on the bid or the ask
            continue
        mult = q["vol"] / p["avg"]
        if mult < TOUCH_MIN_MULT:
            continue
        lo, zhi = _zone_nums(p["resistance"])
        hi = max(q["high"], q["px"])
        if hi < lo * (1 - near):
            continue
        chg = (q["px"] / p["close"] - 1) * 100
        if abs(chg) > SPLIT_JUMP * 100:                   # ex-date of a split / demerger, not a real move
            continue
        where = "above the zone" if q["px"] > zhi * (1 + near) else "in the zone" if q["px"] >= lo * (1 - near) else "touched, now below"
        row = {c: "" for c in TOUCH_COLS}
        row.update({"date": today_s, "time": now.strftime("%H:%M"), "symbol": sym, "price": round(q["px"], 2), "day_high": round(hi, 2), "resistance": p["resistance"],
                    "daily_spurt_x": round(mult, 2), "day_vol": int(q["vol"]), "avg_vol": int(p["avg"]), "day_chg_pct": round(chg, 2), "where": where, "result": "open", "max_up_pct": 0, "max_down_pct": 0,
                    "nifty_at_alert": (round(nifty, 2) if nifty else ""), "nifty_day_pct": ("" if _np is None else _np)})
        state["track"][sym] = row
        state["done"][sym] = row["time"]
        new.append(row)
    if new or changed:
        _touch_write(state, today_s)
    _rej_cycle(now, rej_c, quotes, nifty, uni)
    try:                                                  # ADDED 7 Nov: the same rejection for ALL liquid stocks (page /daily_rej)
        import daily_rej_all
        daily_rej_all.cycle(now, prep, quotes, nifty, uni)
    except Exception as _e:
        log.warning(f"daily_rej_all failed: {_e}")
    if new:
        _touch_notify(new)
        log.info(f"Spurt daily touch: {[h['symbol'] for h in new]} volume spurt + reached the daily resistance zone")


# ---------------------------------------------------------------- ADDED: spurt stock REJECTED at the DAILY resistance zone on a closed 15-min candle
# Same stocks as the live touch watch above (volume so far today >= TOUCH_MIN_MULT x the 50-day average, liquid, no news) whose day high has reached the daily
# resistance zone. For each of them every closed 15-min candle is checked: it touches the daily zone, has an upper wick of REJ_WICK_PCT % or more of its range and
# closes at / below the zone top  ->  phone alert + row in the table on /spurt_daily + results/spurt_daily_rejection.csv (all days, with follow-up and NIFTY baseline).
REJ_ON = True
REJ_TF = 15                  # candle size in minutes (multiple of 5)
REJ_WICK_PCT = 60.0          # upper wick >= this % of the candle's high-low range
REJ_UPPER_FROM = 0.5        # "upper part" of the daily zone = from this share of the zone height upwards (0.5 = top half); a rejection whose high reached it is also listed in the UPPER table
REJ_DAYS = 4                 # calendar days of 5-min candles read per stock (only the last closed candle is needed)
REJ_PATH = os.path.join(RESULTS_DIR, "spurt_daily_rejection.csv")
REJ_STATE = os.path.join(RESULTS_DIR, "spurt_daily_rejection_state.json")
REJ_COLS = ["date", "candle", "alert_time", "symbol", "price", "resistance", "wick_pct", "daily_spurt_x", "day_chg_pct",
            "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "result",
            "nifty_at_alert", "nifty_10m", "nifty_30m", "nifty_60m", "nifty_last", "rel_last", "upper_part"]
_rej = {"date": "", "checked": {}, "rows": []}


def _rej_load(today_s):
    try:
        with open(REJ_STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _rej.update(date=today_s, checked=dict(s_.get("checked") or {}), rows=list(s_.get("rows") or []))
            return
    except Exception:
        pass
    _rej.update(date=today_s, checked={}, rows=[])


def _rej_write(today_s):
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_rej["rows"], columns=REJ_COLS)
        if os.path.exists(REJ_PATH):
            try:
                old = pd.read_csv(REJ_PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"Spurt daily rejection csv could not be read, appending anyway: {e}")
        df.to_csv(REJ_PATH + ".tmp", index=False)
        os.replace(REJ_PATH + ".tmp", REJ_PATH)
        with open(REJ_STATE + ".tmp", "w") as f:
            json.dump({"date": today_s, "checked": _rej["checked"], "rows": _rej["rows"]}, f)
        os.replace(REJ_STATE + ".tmp", REJ_STATE)
    except Exception as e:
        log.warning(f"Spurt daily rejection write failed: {e}")


def _rej_candle(sym, sid):
    """Last CLOSED REJ_TF-minute candle of the stock as (start 'HH:MM', o, h, l, c), or None."""
    try:
        from datetime import timezone
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, REJ_DAYS)
        d = ls.drop_forming_bars(ls.build_merged_bars(df5, REJ_TF // 5), REJ_TF, as_of)
        if d is None or len(d) == 0:
            return None
        last = d.iloc[-1]
        return (sp._ist(d.index[-1]).strftime("%H:%M"), float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"]))
    except Exception as e:
        log.warning(f"Spurt daily rejection candle failed for {sym}: {e}")
        return None


def _rej_notify(rows, upper=False):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (sp.NTFY_ON and topic and rows):
        return
    import requests
    lines = [f"{h['symbol']}  Rs {h['price']:.2f}  daily resistance {h['resistance']}  {REJ_TF}m upper wick {h['wick_pct']:.0f}%  volume today {h['daily_spurt_x']:.1f}x of 50-day avg  candle {h['candle']}"
             for h in rows[:TOUCH_MAX_LINES]]
    extra = f"\n+{len(rows) - TOUCH_MAX_LINES} more on /spurt_daily" if len(rows) > TOUCH_MAX_LINES else ""
    try:
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": (f"Spurt stock REJECTED at UPPER part of DAILY resistance ({len(rows)})" if upper else f"Spurt stock REJECTED at DAILY resistance ({len(rows)})"), "Priority": "urgent", "Tags": "rotating_light,chart_with_downwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt daily rejection ntfy refused: {resp.status_code} {resp.text[:150]}")
    except Exception as e:
        log.warning(f"Spurt daily rejection ntfy failed: {e}")


def _rej_cycle(now, cands, quotes, nifty, uni):
    """cands = {symbol: (resistance 'lo-hi', daily_spurt_x, day_chg_pct)}. Never raises."""
    if not REJ_ON:
        return
    try:
        today_s = now.strftime("%Y-%m-%d")
        if _rej["date"] != today_s:
            _rej_load(today_s)
        slot = (now.hour * 60 + now.minute) // REJ_TF * REJ_TF                    # start of the candle that is forming now; the one before it is the last closed
        slot_key = f"{slot // 60:02d}:{slot % 60:02d}"
        todo = [s_ for s_ in cands if _rej["checked"].get(s_) != slot_key and s_ in uni]
        new = []
        if todo:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max(1, min(int(getattr(sp, "WORKERS", 8)), 8))) as ex:
                res = list(ex.map(lambda s_: (s_, _rej_candle(s_, uni[s_])), todo))
            near = TOUCH_NEAR_PCT / 100.0
            for sym, c in res:
                _rej["checked"][sym] = slot_key
                if not c:
                    continue
                t, o, h, l, cl = c
                zr, mult, chg = cands[sym]
                lo, zhi = _zone_nums(zr)
                rng = h - l
                if rng <= 0:
                    continue
                wick = (h - max(o, cl)) / rng * 100.0
                if wick < REJ_WICK_PCT or h < lo * (1 - near) or cl > zhi:
                    continue
                if any(r_["symbol"] == sym and r_["candle"] == t for r_ in _rej["rows"]):
                    continue
                px = quotes.get(sym, {}).get("px") or cl
                row = {c_: "" for c_ in REJ_COLS}
                row.update({"date": today_s, "candle": t, "alert_time": now.strftime("%H:%M"), "symbol": sym, "price": round(px, 2), "resistance": zr, "wick_pct": round(wick, 1),
                            "daily_spurt_x": round(mult, 2), "day_chg_pct": round(chg, 2), "result": "open", "nifty_at_alert": (round(nifty, 2) if nifty else ""),
                            "upper_part": ("YES" if h >= lo + REJ_UPPER_FROM * (zhi - lo) else "")})
                _rej["rows"].append(row)
                new.append(row)
        # follow-up of today's rows (fall after the alert counts as +)
        hm_now = now.hour * 60 + now.minute
        for r_ in _rej["rows"]:
            q = quotes.get(r_["symbol"])
            if not q or not r_.get("price"):
                continue
            try:
                mv = round((1.0 - q["px"] / float(r_["price"])) * 100.0, 2)
                el = hm_now - (int(r_["alert_time"][:2]) * 60 + int(r_["alert_time"][3:5]))
                r_["best_pct"] = mv if r_["best_pct"] in ("", None) else max(float(r_["best_pct"]), mv)
                r_["worst_pct"] = mv if r_["worst_pct"] in ("", None) else min(float(r_["worst_pct"]), mv)
                r_["last_pct"], r_["last_time"] = mv, now.strftime("%H:%M")
                nm = round((nifty / float(r_["nifty_at_alert"]) - 1.0) * 100.0, 2) if (nifty and r_.get("nifty_at_alert") not in ("", None)) else None
                if nm is not None:
                    r_["nifty_last"], r_["rel_last"] = nm, round(-mv - nm, 2)          # stock move minus NIFTY move, both in price direction
                for lab, nlab, mm in (("ret_10m", "nifty_10m", 10), ("ret_30m", "nifty_30m", 30), ("ret_60m", "nifty_60m", 60)):
                    if r_[lab] in ("", None) and el >= mm:
                        r_[lab] = mv
                        if nm is not None:
                            r_[nlab] = nm
                if r_["ret_60m"] not in ("", None):
                    r_["result"] = "WORKED" if mv >= 0.3 else "FAILED" if mv <= -0.3 else "FLAT"
            except Exception:
                continue
        if new or _rej["rows"]:
            _rej_write(today_s)
        if new:
            _rej_notify([r_ for r_ in new if r_.get("upper_part") != "YES"])
            _rej_notify([r_ for r_ in new if r_.get("upper_part") == "YES"], upper=True)
            log.info(f"Spurt daily rejection: {[h['symbol'] for h in new]} rejected at the daily resistance zone ({REJ_TF}m candle)")
    except Exception as e:
        log.warning(f"Spurt daily rejection step failed: {e}")


def _rej_html():
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        rows = []
        if os.path.exists(REJ_PATH):
            df = pd.read_csv(REJ_PATH)
            df = df[df["date"].astype(str) == today_s].fillna("")
            rows = df.to_dict("records")
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{r['resistance']}</td><td><b>{float(r['wick_pct']):.0f}%</b></td>"
            f"<td>{float(r['daily_spurt_x']):.2f}x</td><td>{pc(r['ret_10m'])}</td><td>{pc(r['ret_30m'])}</td><td>{pc(r['ret_60m'])}</td><td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td><td>{pc(r['last_pct'])}</td><td>{r['result']}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="14" style="text-align:center">No rejection at a daily resistance zone yet today.</td></tr>'
        up_rows = [r for r in rows if str(r.get("upper_part", "")) == "YES"]
        up_body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{r['resistance']}</td><td><b>{float(r['wick_pct']):.0f}%</b></td>"
            f"<td>{float(r['daily_spurt_x']):.2f}x</td><td>{pc(r['ret_10m'])}</td><td>{pc(r['ret_30m'])}</td><td>{pc(r['ret_60m'])}</td><td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td><td>{pc(r['last_pct'])}</td><td>{r['result']}</td></tr>"
            for r in reversed(up_rows)) or '<tr><td colspan="14" style="text-align:center">None yet today.</td></tr>'
        up_html = (f'<h2>Rejected at the UPPER part of the DAILY resistance zone &mdash; {REJ_TF}-min candle ({len(up_rows)})</h2>'
                   f'<div class="m">The same rejection as above, but the candle\'s high reached at least {REJ_UPPER_FROM * 100:.0f}% of the way up the daily zone: price went deep into the strongest zone and was still pushed back. '
                   '<a href="/spurt_daily_upper.csv" target="_blank" rel="noopener">upper-part CSV</a></div>'
                   '<div class="w"><table class="sp"><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Price</th><th>Daily resistance</th><th>Upper wick</th><th>Volume x</th>'
                   '<th>10 min</th><th>30 min</th><th>60 min</th><th>Best</th><th>Worst</th><th>Last</th><th>Result</th></tr>' + up_body + '</table></div><hr>')
        return (f'<h2>Rejected at the DAILY resistance &mdash; {REJ_TF}-min candle with an upper wick of {REJ_WICK_PCT:g}% or more ({len(rows)})</h2>'
                f'<div class="m">A stock with {TOUCH_MIN_MULT:g}x volume so far today whose closed {REJ_TF}-min candle touched its daily resistance zone, has an upper wick of {REJ_WICK_PCT:g}% or more of the candle range and closed at / below the zone top. '
                'Phone alert for each. Moves after the alert: a FALL counts as + (the rejection worked). Information only. '
                '<a href="/spurt_daily_rejection.csv" target="_blank" rel="noopener">rejection CSV</a></div>'
                '<div class="w"><table class="sp"><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Price</th><th>Daily resistance</th><th>Upper wick</th><th>Volume x</th>'
                '<th>10 min</th><th>30 min</th><th>60 min</th><th>Best</th><th>Worst</th><th>Last</th><th>Result</th></tr>' + body + '</table></div><hr>' + up_html)
    except Exception as e:
        log.warning(f"Spurt daily rejection table failed: {e}")
        return ""


def touch_loop():
    """Same-day watch: from 08:45 prepares every stock's 50-day average volume and daily resistance zone (earlier days), then from 09:20 to 15:25 looks every
    TOUCH_EVERY_S seconds; alerts once per stock per day when the volume so far today is TOUCH_MIN_MULT x the average AND the day's high reached the zone.
    Read-only; never raises."""
    if not TOUCH_ON:
        return
    log.info("Spurt daily touch watch started - same-day alert when a 2x volume stock reaches its DAILY resistance zone (09:20-15:25).")
    time.sleep(120)
    state = {"date": None, "done": {}, "track": {}}
    prep, prep_date, fails = None, None, 0
    while True:
        try:
            now = datetime.now(IST)
            m = now.hour * 60 + now.minute
            if now.weekday() < 5 and PREP_FROM_MIN <= m <= TOUCH_TO_MIN:
                today_s = now.strftime("%Y-%m-%d")
                if state["date"] != today_s:
                    state = {"date": today_s, "done": {}, "track": {}}
                    try:
                        with open(TOUCH_STATE) as f:
                            _s = json.load(f)
                        if _s.get("date") == today_s:
                            state["done"] = dict(_s.get("done") or {})
                            state["track"] = dict(_s.get("track") or {})
                    except Exception:
                        pass
                if prep_date != today_s:
                    prep = _prep_today(now.date())
                    prep_date = today_s if prep else None
                if prep and m >= TOUCH_FROM_MIN:
                    _touch_cycle(state, now, prep)
                fails = 0
        except Exception as e:
            fails += 1
            if fails <= 3 or fails % 30 == 0:
                log.warning(f"Spurt daily touch watch failed ({fails}): {e}")
            if fails <= 3:
                time.sleep(300)
        time.sleep(TOUCH_EVERY_S)


# ---------------------------------------------------------------- page
_LEGEND = sp.LEGEND.split(' <span style="background:#f08d84;padding:2px 8px;border:1px solid #c9564c">Offers')[0]      # same colour key as /spurt10, without the Offers % entry

def _touch_html():
    """ADDED: today's stocks that reached their daily resistance zone (live, with the follow-up after the alert) for the top of /spurt_daily. Never raises."""
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        rows = []
        if os.path.exists(TOUCH_PATH):
            df = pd.read_csv(TOUCH_PATH)
            df = df[df["date"].astype(str) == today_s].fillna("")
            rows = df.to_dict("records")
        def pc(v):
            if v == "" or v is None:
                return "-"
            v = float(v)
            return f'<span style="color:{"#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"}">{v:+.2f}%</span>'
        def rs(v):
            c = {"REJECTED": "#c62828", "BROKE_OUT": "#0a7d33"}.get(v, "#555")
            return f'<b style="color:{c}">{v}</b>'
        body = "".join(
            f"<tr><td>{r['time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{float(r['day_high']):.2f}</td><td>{r['resistance']}</td>"
            f"<td>{float(r['daily_spurt_x']):.2f}x</td><td>{r['where']}</td><td>{pc(r['ret_10m'])}</td><td>{pc(r['ret_30m'])}</td><td>{pc(r['ret_60m'])}</td>"
            f"<td>{pc(r['max_up_pct'])}</td><td>{pc(r['max_down_pct'])}</td><td>{pc(r['last_pct'])}</td><td>{pc(r.get('nifty_last', ''))}</td><td>{pc(r.get('rel_last', ''))}</td><td>{rs(r['result'])}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="16" style="text-align:center">No stock with a 2x volume spurt has reached its daily resistance zone yet today.</td></tr>'
        return (f'<h2>Live today &mdash; volume spurt stocks that reached their DAILY resistance ({len(rows)})</h2>'
                f'<div class="m">Alert when the volume so far today is {TOUCH_MIN_MULT:g}x the 50-day average and the day high reaches the daily resistance zone (checked every {TOUCH_EVERY_S}s, 09:20-15:25). '
                f'Moves are from the price at the alert (+ = up). The baseline is NIFTY over the same time: REJECTED = the stock is {TRACK_MOVE:g}% or more BELOW NIFTY\'s move after 60 min, BROKE_OUT = {TRACK_MOVE:g}% or more above it, FLAT = in between. '
                f'<a href="/spurt_daily_touch.csv" target="_blank" rel="noopener">touch CSV</a></div>'
                '<div class="w"><table class="sp"><tr><th>Alert time</th><th>Symbol</th><th>Price</th><th>Day high</th><th>Resistance (daily)</th><th>Volume x</th><th>Where</th>'
                '<th>10 min</th><th>30 min</th><th>60 min</th><th>Max up</th><th>Max down</th><th>Last</th><th>NIFTY (same time)</th><th>Stock minus NIFTY</th><th>Result</th></tr>' + body + '</table></div><hr>' + _rej_html())
    except Exception as e:
        log.warning(f"Spurt daily touch table failed: {e}")
        return ""


def page_html():
    """The /spurt_daily page, read from the last saved scan. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>Daily Volume Spurt</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}td:nth-child(2){text-align:left}'
            '.m{color:#555;font-size:13px;margin:6px 0 12px}th{position:sticky;top:0;z-index:5;box-shadow:0 1px 0 #555}</style></head><body>')      # no overflow wrapper: it would stop the heading row from freezing
    try:
        if not os.path.exists(JSON_PATH):
            return (head + _touch_html() + "<h2>Daily Volume Spurt</h2><p>No daily scan has run yet. It runs once a day after the market closes (16:45 IST, Mon-Fri) "
                    "and shows the stocks whose volume today is 2x or more of their 50-day average, with daily support / resistance zones.</p></body></html>")
        with open(JSON_PATH) as f:
            d = json.load(f)
        meta, rows = d["meta"], d["rows"]

        def col(v):
            return "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"

        def pct(v):
            return "-" if v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'

        def ctype(r):
            v = r.get("day_chg_pct")
            oh = r.get("ohlc")
            svg = sp._candle_svg(*oh) if oh else ""
            if v is None:
                return "<td>-</td>"
            if v > 0:
                return '<td style="white-space:nowrap">' + svg + ' <b style="color:#089981">GREEN</b></td>'
            if v < 0:
                return '<td style="white-space:nowrap">' + svg + ' <b style="color:#F23645">RED</b></td>'
            return '<td style="white-space:nowrap">' + svg + ' DOJI</td>'

        def mstyle(r):
            return ' style="background:#ffe066"' if r["multiple"] >= meta.get("alert_mult", 3.0) else ""

        shown = rows[:TOP_N]
        body = "".join(
            f"<tr{sp._rowstyle(r.get('at_zone'))}><td>{i}</td><td>{sp._tvl(r['symbol'])}</td><td>{r['price']:.2f}</td><td>{pct(r.get('day_chg_pct'))}</td>{ctype(r)}"
            f"<td>{r['day_vol']:,}</td><td>{r['avg_vol']:,}</td>"
            f"<td{mstyle(r)}><b>{r['multiple']:.2f}x</b></td>"
            f"<td{sp._zbg(r.get('at_zone'), 'SUPPORT')}>{sp._zc(r, 'support')}</td><td{sp._zbg(r.get('at_zone'), 'RESISTANCE')}>{sp._zc(r, 'resistance')}</td>"
            f"<td>{sp._zv(r.get('zone_view'), r.get('zone_src'))}</td></tr>"
            for i, r in enumerate(shown, 1)) or '<tr><td colspan="11">No stock reached the daily spurt today.</td></tr>'
        return (head + _touch_html() + f'<h2>Daily Volume Spurt &mdash; today\'s volume vs the {meta["avg_len"]}-day average volume</h2>'
                f'<div class="m">Scan of {meta["date"]}: {meta["run"]} (took {meta["took_s"]}s) &middot; {meta["liquid"]} liquid NSE stocks scanned '
                f'(price &ge; {sp.MIN_PRICE:.0f}, traded &ge; Rs {MIN_TURNOVER / 1e7:g} crore, thin spreads left out), {meta["listed"]} at {meta["min_mult"]:g}x or more '
                f'&middot; runs once a day after 16:45 IST &middot; phone alert at {meta["alert_mult"]:g}x or more when the price is AT a daily zone &middot; {_LEGEND} &middot; '
                f'zones = ALL IN ONE PRO order-block zones on the DAILY chart &middot; <a href="/spurt_daily.csv">CSV</a> &middot; <a href="/spurt_daily_history.csv">history CSV</a> &middot; <a href="/spurt_daily_touch.csv">resistance-touch CSV</a> &middot; '
                f'<a href="/spurt10">10-min volume spurt</a> &middot; <a href="/scanner">Scanner</a></div>'
                '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Day candle</th><th>Day volume</th>'
                f'<th>{meta["avg_len"]}-day avg vol</th><th>Multiple</th><th>Support zone (daily)</th><th>Resistance zone (daily)</th><th>Zone view</th></tr>{body}</table></div>'
                + sp._SORT_JS + "</body></html>")
    except Exception as e:
        log.warning(f"Spurt daily page failed: {e}")
        return head + "<p>The daily spurt page could not be built. Please refresh later.</p></body></html>"


# ---------------------------------------------------------------- loop
def _have_today(today_s):
    try:
        with open(JSON_PATH) as f:
            return json.load(f)["meta"].get("date") == today_s
    except Exception:
        return False


def loop():
    log.info("Spurt daily autorun started - once a day after 16:45 IST (Mon-Fri); catch-up 15 min after a restart if today's scan is missing.")
    import threading
    threading.Thread(target=touch_loop, daemon=True, name="spurt-daily-touch").start()      # ADDED: live resistance-touch watch
    time.sleep(900)                                       # stay out of the way of the start-up scans
    fails = 0
    while True:
        try:
            now = datetime.now(IST)
            today_s = now.strftime("%Y-%m-%d")
            due = now.weekday() < 5 and now.hour * 60 + now.minute >= RUN_AT_MIN
            if due and _state["last_run_date"] != today_s and not _have_today(today_s):
                try:
                    ok = run_once()
                    _state["last_run_date"] = today_s     # done for today (also when it was a holiday: nothing more to do)
                    fails = 0
                except Exception as e:
                    fails += 1
                    log.error(f"Spurt daily scan failed ({fails}): {e}")
                    if fails >= 3:
                        _state["last_run_date"] = today_s
                        fails = 0
                    else:
                        time.sleep(600)
            elif due:
                _state["last_run_date"] = today_s
        except Exception as e:
            log.error(f"Spurt daily loop error: {e}")
        time.sleep(300)
