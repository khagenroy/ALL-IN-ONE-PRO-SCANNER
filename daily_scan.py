"""
DAILY SCAN  (page: /daily_scan   csv: /daily_scan.csv)                                                         ADDED 19 Nov 2026

ONE scan after the market closes, on DAILY candles, of every stock except the thin ones (the same list as the swing page). BUY only. It lists every BUY signal of the
indicators of the chart layout, for the last closed daily candle:

  ALL IN ONE PRO   VOLUME SPURT + SMA (first candle of the move), SWEEP BUY, OB MITIGATION BUY, VS BUY, SPURT BUY      (swing_page.detect - the same rules as the swing page)
  CONSOL BOX       STRONG BUY (box breakout up + volume + close above SMA 9/21/60), PULLBACK BUY (trend, dip to SMA 9, back above it)
  GAP RETRACE      gap UP day (open at least 4% above the day before) -> pullback of 38.2% of the leg -> first green candle closing above the previous high

Every row shows entry / SL / T1 / T2 / T6 (1R / 2R / 6R) and the FREE ROOM to the nearest resistance in R, like the swing page.
Runs Mon-Fri at 15:50 IST (the daily candle is complete) with ONE phone alert "DAILY SCAN ALERT - BUY". The page has a "Scan now" button (admin, no alert).
It places NO order. It is called from swing_page.py (register / loop), so app.py needs NO change. Any error is caught and logged.
"""
import os
import re
import json
import time
import html
import logging
import threading
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

import swing_page as sw

log = logging.getLogger("daily_scan")
IST = timezone(timedelta(hours=5, minutes=30))

ON = True
RUN_AT = (15, 50)                  # IST, Mon-Fri
RETRY_MIN = 45                     # when the 15:50 run found no fresh daily candles, one more try this many minutes later
WIN_DAYS = 2                       # signals of the last N daily candles are listed (the day before is kept in case the daily file was one day late)
MIN_BARS = 120
KEEP_DAYS = 120
GAP_PCT, GAP_FIB, GAP_MAXDAYS = 4.0, 0.382, 20
PB_WIN, PB_TOL, PB_RUN, PB_SLOPE = 8, 0.3, 3.0, 5
ALERT_MAX = 12
NTFY_PRIORITY = "high"

PATH = os.path.join(sw.RESULTS_DIR, "daily_scan.json")
STATUS = os.path.join(sw.RESULTS_DIR, "daily_scan_status.json")
HOLD_PATH = os.path.join(sw.RESULTS_DIR, "daily_hold.json")   # ADDED 21 Nov: section F of the daily scan, {day: [rows]} (last 30 days)
_hold_new = []                                                 # rows of section F found during the running scan
_hold_lock = threading.Lock()
GROUPS = (("ALL IN ONE PRO", "#00e5ff"), ("CONSOL BOX", "#69f0ae"), ("GAP RETRACE", "#ffb300"), ("RS vs NIFTY", "#ce93d8"))
_lock = threading.RLock()
_run_lock = threading.Lock()
_rows = []
_mt = 0.0
_st = {"on": False, "msg": "", "done": 0, "total": 0, "last_end": "", "run_day": "", "tries": 0}


def _now():
    return datetime.now(IST)


# ------------------------------------------------------------------------------------------------------- store
def _set(**kw):
    _st.update(kw)
    try:
        os.makedirs(sw.RESULTS_DIR, exist_ok=True)
        with open(STATUS + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATUS + ".tmp", STATUS)
    except Exception:
        pass


def _status():
    try:
        with open(STATUS) as f:
            return json.load(f)
    except Exception:
        return dict(_st)


def _load():
    global _rows, _mt
    try:
        mt = os.path.getmtime(PATH)
        if mt != _mt:
            with open(PATH) as f:
                _rows = json.load(f)
            _mt = mt
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("daily scan: could not read the saved signals: %s", e)
    return _rows


def _save():
    global _mt
    try:
        cut = (_now() - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
        keep = [r for r in _rows if r.get("date", "") >= cut]
        _rows[:] = keep
        os.makedirs(sw.RESULTS_DIR, exist_ok=True)
        with open(PATH + ".tmp", "w") as f:
            json.dump(_rows, f)
        os.replace(PATH + ".tmp", PATH)
        _mt = os.path.getmtime(PATH)
    except Exception as e:
        log.warning("daily scan: could not save: %s", e)


def _add(group, sym, name, day, entry, sl, mult, extra, res):
    if not (entry > sl > 0):
        return None
    key = "%s|%s|%s|%s" % (sym, group, name, day)
    with _lock:
        rows = _load()
        if any(r.get("key") == key for r in rows):
            return None
        t1, t2, t6 = sw._levels(entry, sl)
        room_r, room = sw._room_text(entry, sl, res)
        row = {"key": key, "date": day, "group": group, "symbol": sym, "signal": name, "entry": round(entry, 2), "sl": round(sl, 2),
               "risk_pct": round((entry - sl) / entry * 100.0, 2), "t1": round(t1, 2), "t2": round(t2, 2), "t6": round(t6, 2),
               "spurt_x": round(mult, 1) if mult else None, "extra": extra, "res": round(res, 2) if res else None, "room_r": room_r, "room": room,
               "found": _now().strftime("%Y-%m-%d %H:%M")}
        rows.append(row)
        return row


# ------------------------------------------------------------------------------------------------------- the signals (CONSOL BOX, GAP RETRACE)
def _atr(h, l, c, n=14):
    pc = np.concatenate(([np.nan], c[:-1]))
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    tr[0] = h[0] - l[0]
    out = np.full(len(c), np.nan)
    if len(c) >= n:
        out[n - 1] = tr[:n].mean()
        for i in range(n, len(c)):
            out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def pullback_buy(o, h, l, c, v, win):
    """PULLBACK BUY of CONSOLIDATION_BOX.pine (defaults): SMA9 > SMA21 > SMA60 and SMA60 rising over 5 candles, a run of 3 x ATR above SMA21 in the last 40 candles, a low that
    touched SMA9 (within 0.3 x ATR) in the last 8 candles with every close above SMA60, volume of the dip below the 50-candle average, then the first green candle that
    closes back above SMA9. Returns [(i, entry, sl)]."""
    n = len(c)
    s = pd.Series
    s1, s2, s3 = (s(c).rolling(k).mean().to_numpy() for k in (9, 21, 60))
    va = s(v).rolling(50).mean().to_numpy()
    atr = _atr(h, l, c)
    out = []
    last = -1000
    for i in range(max(60 + PB_SLOPE, 50, 40), n):
        if np.isnan(s3[i]) or np.isnan(atr[i]) or np.isnan(va[i]):
            continue
        if not (s1[i] > s2[i] > s3[i] and s3[i] > s3[i - PB_SLOPE]):
            continue
        if h[i - 39:i + 1].max() - s2[i] < atr[i] * PB_RUN:
            continue
        a, b = i - PB_WIN + 1, i + 1
        if (l[a:b] - s1[a:b]).min() > atr[i] * PB_TOL:
            continue
        if (c[a:b] - s3[a:b]).min() <= 0:
            continue
        if not (v[a:b].mean() < va[i]):
            continue
        if not (c[i] > o[i] and c[i] > s1[i] and c[i - 1] <= s1[i - 1]):
            continue
        if i - last <= PB_WIN:
            continue
        last = i
        if win[i]:
            out.append((i, float(c[i]), float(l[a:b].min() - atr[i] * 0.1)))
    return out


def gap_retrace_buy(o, h, l, c, win):
    """GAP RETRACE (gap UP -> BUY the pullback) on daily candles: the day opens at least 4% above the day before; the leg runs from the day before's close to the highest high (a new
    high restarts the watch); price pulls back at least 38.2% of the leg; the first GREEN candle that closes above the previous candle's high = BUY. The watch ends when a candle
    closes at / below the day before's close (gap filled) or after 20 days. SL = the lowest low of the pullback. Returns [(i, entry, sl)]."""
    n = len(c)
    out = []
    for g in range(1, n):
        pc = c[g - 1]
        if not (pc > 0 and o[g] >= pc * (1 + GAP_PCT / 100.0)):
            continue
        leg_hi, retraced, pb_low = h[g], False, None
        for j in range(g + 1, min(n, g + GAP_MAXDAYS + 1)):
            if c[j] <= pc:
                break
            if h[j] > leg_hi:
                leg_hi, retraced, pb_low = h[j], False, None
                continue
            if l[j] <= leg_hi - GAP_FIB * (leg_hi - pc):
                retraced = True
                pb_low = l[j] if pb_low is None else min(pb_low, l[j])
            if retraced and c[j] > o[j] and c[j] > h[j - 1]:
                if win[j]:
                    out.append((j, float(c[j]), float(pb_low)))
                break
    return out


def rs_cross_buy(d, win):
    """Section C of the swing page on DAILY candles (ADDED 21 Nov): [(i, entry = high, sl = low, volume multiple, text)].
    The stock / NIFTY line crossed UP through its SMA 21 on the signal candle or the one before, on a green candle that closes above the 9, 21 and 60 SMA and crosses at least one of them
    from below, with 5x or more volume against the average of the 50 candles before. Same rule as the swing page (swing_page.rs_cross_signals)."""
    out = []
    if not sw.RS_ON or not _NIFD["dates"]:
        return out
    n = len(d)
    c, o, l, h, v = (d[k].to_numpy(dtype=float) for k in ("close", "open", "low", "high", "volume"))
    nm = dict(zip(_NIFD["dates"], _NIFD["close"]))
    nc = np.array([nm.get(pd.Timestamp(t).strftime("%Y-%m-%d"), np.nan) for t in d.index], dtype=float)
    sm = {k: sw._sma(c, k) for k in sw.SMAS}
    ratio = c / nc
    sma = pd.Series(ratio).rolling(sw.RS_SMA_LEN).mean().to_numpy()
    above = ratio > sma
    ok = ~np.isnan(ratio) & ~np.isnan(sma)
    for i in range(max(sw.RS_SMA_LEN + 1, sw.VOL_AVG), n):
        if not win[i]:
            continue
        if not (ok[i] and above[i] and c[i] > o[i] and l[i] > 0 and c[i] > l[i]):
            continue
        if any(np.isnan(sm[k][i]) or np.isnan(sm[k][i - 1]) for k in sw.SMAS) or not all(c[i] > sm[k][i] for k in sw.SMAS):
            continue
        cr = [k for k in sw.SMAS if c[i - 1] <= sm[k][i - 1]]
        if not cr:
            continue
        a = v[i - sw.VOL_AVG:i].mean()
        if a <= 0 or v[i] / a < sw.RS_SPURT_MULT:
            continue
        if any(k >= 1 and ok[k] and ok[k - 1] and above[k] and not above[k - 1] for k in range(i, i - sw.RS_CROSS_WITHIN, -1)):
            out.append((i, float(h[i]), float(l[i]), float(v[i] / a), "RS up SMA21; crossed SMA " + " ".join(str(k) for k in cr)))
    return out


def strong_buy(d, win):
    """STRONG BUY of CONSOLIDATION_BOX.pine: a confirmed box breakout UP (spurt10_live._box_breakouts, the same Python copy the scanner uses on the 5-minute candles) that also
    closes above SMA 9, 21 and 60. Entry = close, SL = the bottom of the box. Returns [(i, entry, sl, volume x, box candles)]."""
    import spurt10_live as sp
    c = d["close"].to_numpy(dtype=float)
    s = [pd.Series(c).rolling(k).mean().to_numpy() for k in (9, 21, 60)]
    out = []
    for b in sp._box_breakouts(d):
        i = b["i"]
        if b["dir"] != "UP" or not win[i] or any(np.isnan(x[i]) or c[i] <= x[i] for x in s):
            continue
        out.append((i, float(c[i]), float(b["lo"]), b.get("vol_x"), b.get("n")))
    return out


# ------------------------------------------------------------------------------------------------------- NIFTY for the 'vs Nifty' column (ADDED 19 Nov)
_NIFD = {"dates": [], "close": []}


def _nifty_load(ls):
    """NIFTY 50 daily candles (Dhan index id 13), once per scan; today's close is taken from the indices strip when Dhan's daily history does not hold the day just closed yet.
    Any failure leaves the lists empty: the column then shows '-' and the scan is not affected."""
    _NIFD["dates"], _NIFD["close"] = [], []
    try:
        now = _now().replace(tzinfo=None)
        payload = {"securityId": "13", "exchangeSegment": "IDX_I", "instrument": "INDEX", "expiryCode": 0, "oi": False,
                   "fromDate": (now - timedelta(days=400)).strftime("%Y-%m-%d"), "toDate": (now + timedelta(days=1)).strftime("%Y-%m-%d")}
        nd = _fix_dates(ls._parse_historical(ls._dhan_post("charts/historical", payload, "NIFTY (daily scan)").json()))
        dates = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in nd.index]
        closes = [float(x) for x in nd["close"]]
        try:
            with open(os.path.join(sw.RESULTS_DIR, "indices.json"), "r", encoding="utf-8") as f:
                ix = json.load(f)
            row = next((q for q in ix.get("rows", []) if q.get("name") == "NIFTY"), None)
            today = now.strftime("%Y-%m-%d")
            if row and row.get("price") and today not in dates and now.weekday() < 5 and str(ix.get("asof", "")).startswith(now.strftime("%d-%b")):
                dates.append(today)
                closes.append(float(row["price"]))
        except Exception:
            pass
        _NIFD["dates"], _NIFD["close"] = dates, closes
    except Exception as e:
        log.warning("daily scan: NIFTY candles not available (%s): the vs-Nifty column stays empty", e)


def _nifty_pct(day_txt):
    """(NIFTY's move on that day in %, its move over the 5 trading days up to that day) or None."""
    ds_, cs = _NIFD["dates"], _NIFD["close"]
    if day_txt not in ds_:
        return None
    k = ds_.index(day_txt)
    if k < 5 or cs[k - 1] <= 0 or cs[k - 5] <= 0:
        return None
    return round((cs[k] / cs[k - 1] - 1.0) * 100.0, 2), round((cs[k] / cs[k - 5] - 1.0) * 100.0, 2)



def scan_stock(sym, d, now_ist):
    """New rows of one stock. d = daily candles (index = date), the last row = the last closed day."""
    new = []
    n = len(d)
    if n < MIN_BARS:
        return new
    d = d.tail(500)
    n = len(d)
    last = pd.Timestamp(d.index[-1]).to_pydatetime().replace(tzinfo=None)
    if (now_ist.replace(tzinfo=None) - last).days > 5:
        return new                                                           # stale data (halted stock)
    o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
    win = np.zeros(n, dtype=bool)
    win[n - WIN_DAYS:] = True

    def day(i):
        return pd.Timestamp(d.index[i]).strftime("%Y-%m-%d")

    def put(group, name, i, entry, sl, mult=None, extra=""):
        entry = float(h[i])                                          # 20 Nov: ENTRY = ABOVE the HIGH of the signal candle (a stop entry); SL unchanged
        if not mult and not extra and i >= 50:                       # PULLBACK BUY / GAP RETRACE have no spurt: show the volume of the signal candle against the average of the 50 candles before it
            av = float(np.mean(v[i - 50:i]))
            if av > 0:
                extra = "volume %.1fx of the 50-candle average" % (v[i] / av)
        lv = sw._res_levels(d, i, entry)
        res0 = lv[0][0] if lv else None
        r = _add(group, sym, name, day(i), entry, sl, mult, extra, res0)
        fresh = r is not None
        if r is None:                                         # already saved: refresh its room (the room rules were improved after it was saved)
            key = "%s|%s|%s|%s" % (sym, group, name, day(i))
            with _lock:
                r = next((x for x in _load() if x.get("key") == key), None)
            if r is None:
                return
            if entry > sl > 0:                                # the numbers too (the day's close can change: built from the 15m candles first, then Dhan's own daily candle)
                t1_, t2_, t6_ = sw._levels(entry, sl)
                r.update(entry=round(entry, 2), sl=round(sl, 2), risk_pct=round((entry - sl) / entry * 100.0, 2), t1=round(t1_, 2), t2=round(t2_, 2), t6=round(t6_, 2))
            if extra and not r.get("extra"):                  # rows saved before the note existed get it now
                r["extra"] = extra
            r["room_r"], r["room"] = sw._room_text(entry, sl, res0)
            r["res"] = round(res0, 2) if res0 else None
        r["room"] = r["room"] + sw._levels_text(lv)          # which levels were found (diagnostic: kind + price of the nearest four)
        try:                                                   # diagnostic: the highest high of the last 20 candles in OUR data (to compare with the chart)
            j0 = max(0, i - 19)
            jm = j0 + int(np.argmax(h[j0:i + 1]))
            r["room"] += "  (data: highest high of last 20 = %.2f on %s)" % (h[jm], day(jm))
        except Exception:
            pass
        try:                                                   # stock against NIFTY: the signal day's move and the 5-day move (green = stronger than Nifty)
            if i >= 5:
                r["chg"], r["chg5"] = round((c[i] / c[i - 1] - 1.0) * 100.0, 2), round((c[i] / c[i - 5] - 1.0) * 100.0, 2)
                nn = _nifty_pct(day(i))
                r["nchg"], r["nchg5"] = nn if nn else (None, None)
        except Exception:
            pass
        if fresh:
            new.append(r)

    try:
        for i, _sec, nm, entry, sl, mult, crossed in sw.detect(d, win):
            put("ALL IN ONE PRO", nm, i, entry, sl, mult, ("crossed SMA " + crossed.replace(",", " ")) if crossed and crossed != "-" else "")
    except Exception as e:
        log.warning("daily scan: ALL IN ONE PRO failed for %s: %s", sym, e)
    try:
        for i, entry, sl, vx, bn in strong_buy(d, win):
            put("CONSOL BOX", "STRONG BUY", i, entry, sl, vx, "box of %s candles" % bn)
        for i, entry, sl in pullback_buy(o, h, l, c, v, win):
            put("CONSOL BOX", "PULLBACK BUY", i, entry, sl)
    except Exception as e:
        log.warning("daily scan: CONSOL BOX failed for %s: %s", sym, e)
    try:
        for i, entry, sl, mx, txt in rs_cross_buy(d, win):
            put("RS vs NIFTY", "RS CROSS + SPURT", i, entry, sl, mx, txt)
    except Exception as e:
        log.warning("daily scan: RS cross failed for %s: %s", sym, e)
    try:
        for i, entry, sl in gap_retrace_buy(o, h, l, c, win):
            put("GAP RETRACE", "GAP UP PULLBACK BUY", i, entry, sl)
    except Exception as e:
        log.warning("daily scan: GAP RETRACE failed for %s: %s", sym, e)
    try:                                                       # section F (21 Nov): volume spurt within the last 10 daily candles, holding near, SMA 9>21>60, crossed all SMAs within 20 candles (same rule as the swing page, daily candles)
        hr = sw.holding_row(sym, d, "D", [pd.Timestamp(t) for t in d.index])
        if hr:
            hr["chg"], hr["chg5"] = round((c[-1] / c[-2] - 1.0) * 100.0, 2), round((c[-1] / c[-6] - 1.0) * 100.0, 2)
            nn = _nifty_pct(hr["last_at"][:10])
            hr["nchg"], hr["nchg5"] = nn if nn else (None, None)
            _hold_new.append(hr)
    except Exception as e:
        log.warning("daily scan: section F failed for %s: %s", sym, e)
    return new


# ------------------------------------------------------------------------------------------------------- data, alert, scan
def _fix_dates(d):
    """Dhan stamps each daily candle at IST midnight = 18:30 UTC of the PREVIOUS day; read as UTC that shows one day too early (the 18 Nov candle as 17 Nov).
    When the candles are stamped 18:30, move them forward 5h30 so the date is the real trading day. Candles already at midnight are left as they are."""
    try:
        ix = pd.DatetimeIndex(d.index)
        if len(ix) and bool(((ix.hour == 18) & (ix.minute == 30)).mean() > 0.5):
            d = d.copy()
            d.index = (ix + pd.Timedelta(hours=5, minutes=30)).normalize()
    except Exception:
        pass
    return d


def _add_today(d, sid, ls):
    """Dhan's daily history does not hold the day just closed until the next day. After the close (15:35 IST or later, Mon-Fri) the day's candle is built from the 15-minute candles of
    today: open = first, high = highest, low = lowest, close = last, volume = sum. The close = the volume-weighted average of the last 30 minutes (like the exchange's closing price)."""
    try:
        now = _now().replace(tzinfo=None)
        if d is None or len(d) == 0 or now.weekday() >= 5 or now.hour * 60 + now.minute < 15 * 60 + 35:
            return d
        today = pd.Timestamp(now.date())
        if pd.Timestamp(d.index[-1]).normalize() >= today:
            return d
        m = ls.fetch_intraday_history(sid, "NSE_EQ", 15, 3)
        if m is None or m.empty:
            return d
        ist = pd.DatetimeIndex(m.index) + pd.Timedelta(hours=5, minutes=30)
        sel = ist.normalize() == today
        m, ist = m[sel], ist[sel]
        if len(m) < 1:                                      # no trade today (a thin stock has no candle in a quiet 15 minutes, so the NUMBER of candles says nothing)
            return d
        order = np.argsort(ist.values)
        m, ist = m.iloc[order], ist[order]
        close = float(m["close"].iloc[-1])                  # the exchange's closing price is the volume-weighted average of the LAST 30 MINUTES (15:00 - 15:30), not the last trade
        last = (ist.hour * 60 + ist.minute) >= 15 * 60
        if last.any():
            tp = (m["high"].to_numpy(float)[last] + m["low"].to_numpy(float)[last] + m["close"].to_numpy(float)[last]) / 3.0
            vv = m["volume"].to_numpy(float)[last]
            if vv.sum() > 0:
                close = float((tp * vv).sum() / vv.sum())
        row = pd.DataFrame({"open": [float(m["open"].iloc[0])], "high": [float(m["high"].max())], "low": [float(m["low"].min())],
                            "close": [close], "volume": [float(m["volume"].sum())]}, index=[today])
        return pd.concat([d, row])
    except Exception as e:
        log.warning("daily scan: today's candle from the 15m candles failed: %s", e)
        return d


def _daily(sym, sid, ls):
    """Fresh daily candles (the scanner's own daily history call, so the day just closed is in). Falls back to the saved daily file."""
    try:                                    # the end date is TOMORROW: Dhan's end date is not included, so with today's date the day just closed is left out
        now = _now().replace(tzinfo=None)
        payload = {"securityId": sid, "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "expiryCode": 0, "oi": False,
                   "fromDate": (now - timedelta(days=900)).strftime("%Y-%m-%d"), "toDate": (now + timedelta(days=1)).strftime("%Y-%m-%d")}
        d = ls._parse_historical(ls._dhan_post("charts/historical", payload, "%s (daily scan)" % sid).json())
        if d is not None and len(d) >= MIN_BARS:
            return _add_today(_fix_dates(d), sid, ls)
    except Exception:
        pass
    try:
        d = ls.fetch_daily_history(sid, "NSE_EQ")
        if d is not None and len(d) >= MIN_BARS:
            return _add_today(_fix_dates(d), sid, ls)
    except Exception:
        pass
    p = os.path.join(ls.DAILY_CACHE_DIR, sym + ".csv")
    if os.path.exists(p):
        return _fix_dates(pd.read_csv(p, index_col=0, parse_dates=True))
    return None


def _notify(items):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    by = {}
    for r in items:
        by[r["signal"]] = by.get(r["signal"], 0) + 1
    head = " | ".join("%s %d" % (k, v) for k, v in sorted(by.items()))
    lines = ["DAILY SCAN ALERT - %s (%s)" % (items[0]["date"], head)]
    for r in items[:ALERT_MAX]:
        lines.append("%s - %s | Entry %s SL %s (%.1f%%) | %s" % (r["signal"], r["symbol"], sw._f(r["entry"]), sw._f(r["sl"]), r["risk_pct"], r.get("room") or "n/a"))
    if len(items) > ALERT_MAX:
        lines.append("+%d more on the daily scan page" % (len(items) - ALERT_MAX))
    body = "\n".join(lines).encode("utf-8")
    title = "DAILY SCAN ALERT - BUY (%d signals)" % len(items)

    def _send():
        try:
            import requests
        except Exception:
            return
        for attempt in range(3):
            try:
                if requests.post("https://ntfy.sh/" + topic, data=body, timeout=10, headers={"Title": title, "Priority": NTFY_PRIORITY, "Tags": "calendar"}).status_code < 300:
                    return
            except Exception:
                pass
            time.sleep(8 * (attempt + 1))

    threading.Thread(target=_send, daemon=True, name="daily-ntfy").start()


def _hold_all():
    try:
        with open(HOLD_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _hold_day_save(day, rows):
    rows = sorted(rows, key=lambda r: (min(r["d_sp"], r["d_sma"]), r["symbol"]))
    with _hold_lock:
        a = _hold_all()
        a[day] = rows
        for k in sorted(a)[:-30]:
            a.pop(k, None)
        try:
            with open(HOLD_PATH + ".tmp", "w") as f:
                json.dump(a, f)
            os.replace(HOLD_PATH + ".tmp", HOLD_PATH)
        except Exception as e:
            log.warning("daily scan: section F not saved: %s", e)


def run_scan(alerts=True, label=""):
    """Scans the liquid list once, one stock at a time. Returns the number of stocks with a fresh last candle, or None when a scan is already running."""
    if not _run_lock.acquire(blocking=False):
        return None
    fresh_stocks = 0
    try:
        import live_scanner as ls
        _set(on=True, msg="starting: reading the liquid list", done=0, total=0)
        _nifty_load(ls)
        uni = sw._universe()
        _set(total=len(uni), msg="scanning 0 / %d" % len(uni))
        now = _now()
        new = []
        newest = ""
        _hold_new.clear()
        for k, (sym, sid) in enumerate(uni):
            try:
                d = _daily(sym, sid, ls)
                if d is not None and len(d) >= MIN_BARS:
                    ld = pd.Timestamp(d.index[-1]).strftime("%Y-%m-%d")
                    newest = max(newest, ld)
                    if ld == now.strftime("%Y-%m-%d") or (now.weekday() >= 5 and ld >= (now - timedelta(days=3)).strftime("%Y-%m-%d")):
                        fresh_stocks += 1
                    new += scan_stock(sym, d, now)
            except Exception as e:
                log.warning("daily scan: %s failed: %s", sym, e)
            _set(done=k + 1, msg="scanning %d / %d" % (k + 1, len(uni)))
        with _lock:
            _save()
        if newest:
            _hold_day_save(newest, [r for r in _hold_new if r["last_at"][:10] == newest])
        items = [r for r in new if r["date"] == newest]
        if alerts and items:
            _notify(items)
        _set(last_end=now.strftime("%Y-%m-%d %H:%M"), msg="finished %s IST (%s) - %d new signals, last candle %s, %d stocks had it" % (_now().strftime("%H:%M"), label or "scan", len(new), newest or "-", fresh_stocks))
        log.info("daily scan %s done: %d new signals, last candle %s", label, len(new), newest)
        return fresh_stocks
    except Exception as e:
        _set(msg="failed: %s" % e)
        log.warning("daily scan failed: %s", e)
        return 0
    finally:
        _set(on=False)
        _run_lock.release()


def run_now():
    if _status().get("on"):
        return False
    threading.Thread(target=run_scan, kwargs=dict(alerts=False, label="manual"), daemon=True, name="daily-now").start()
    return True


def loop():
    log.info("Daily scan started: Mon-Fri %02d:%02d IST, daily candles, every stock except the thin ones, BUY only (ALL IN ONE PRO + CONSOL BOX + GAP RETRACE).", RUN_AT[0], RUN_AT[1])
    while True:
        try:
            now = _now()
            today = now.strftime("%Y-%m-%d")
            if not ON or now.weekday() >= 5:
                time.sleep(600)
                continue
            m = now.hour * 60 + now.minute
            st = _status()
            if m >= RUN_AT[0] * 60 + RUN_AT[1] and m <= 22 * 60 and st.get("run_day") != today and not st.get("on"):
                _set(run_day=today)
                got = run_scan(alerts=True, label="%02d:%02d" % (RUN_AT[0], RUN_AT[1]))
                if got is not None and got < 30:                                      # the day's candle was not there yet: one more try later
                    _set(run_day="", tries=_status().get("tries", 0) + 1)
                    if _status().get("tries", 0) >= 2:
                        _set(run_day=today, tries=0)
                    else:
                        time.sleep(RETRY_MIN * 60)
                else:
                    _set(tries=0)
            time.sleep(60)
        except Exception as e:
            log.error("daily scan cycle failed: %s", e)
            time.sleep(60)


# ------------------------------------------------------------------------------------------------------- CSV / page
COLS = ["date", "group", "symbol", "signal", "entry", "sl", "risk_pct", "t1", "t2", "t6", "resistance", "room_r", "volume_x", "note", "found_at", "chg_pct", "nifty_chg_pct", "chg5_pct", "nifty_chg5_pct", "signals_same_day"]


def csv_text(day=None):
    with _lock:
        rows = [r for r in _load() if (day is None or r["date"] == day)]
    rows.sort(key=lambda r: (r["date"], r["group"], r["symbol"]))
    with _lock:
        _all = list(_load())
    _cnt = {}
    for r in _all:
        _cnt.setdefault((r["symbol"], r["date"]), set()).add((r["group"], r["signal"]))
    out = [",".join(COLS)]
    for r in rows:
        vals = [r.get("date"), r.get("group"), r.get("symbol"), r.get("signal"), r.get("entry"), r.get("sl"), r.get("risk_pct"), r.get("t1"), r.get("t2"), r.get("t6"),
                r.get("res"), r.get("room_r"), r.get("spurt_x"), r.get("extra"), r.get("found"), r.get("chg"), r.get("nchg"), r.get("chg5"), r.get("nchg5"), len(_cnt.get((r["symbol"], r["date"]), ()))]
        out.append(",".join("" if x is None else str(x).replace(",", " ") for x in vals))
    return "\n".join(out) + "\n"


def _table(rows):
    if not rows:
        return "<p style='color:#9fb0c0'>No signal.</p>"
    body = []
    for r in rows:
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=D" % html.escape(r["symbol"])
        blocked = r.get("room_r") is not None and r["room_r"] < 1
        body.append("<tr><td>%s</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td style='font-weight:700'>%s</td><td>%s</td>"
                    "<td style='color:#ff7070'>%s <span style='color:#9fb0c0'>(%.1f%%)</span></td><td style='color:#7be08a'>%s</td><td style='color:#7be08a'>%s</td>"
                    "<td style='color:%s'>%s</td><td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
                        r["date"][5:], tv, html.escape(sw._sym_label(r["symbol"])), html.escape(r["signal"]), sw._f(r["entry"]), sw._f(r["sl"]), r["risk_pct"], sw._f(r["t1"]), sw._f(r["t2"]),
                        "#ff7070" if blocked else "#ffb300", html.escape(r.get("room") or "-"), sw._rs_cell(r),
                        html.escape(("%sx " % r["spurt_x"] if r.get("spurt_x") else "") + (r.get("extra") or ""))))
    return ("<table><tr><th>Day</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL (risk)</th><th>T1</th><th>T2</th><th>Room to resistance</th><th>vs Nifty</th><th>Volume / note</th></tr>"
            + "".join(body) + "</table>")


def _same_day(rows):
    """Section E: stocks with 2 or more DIFFERENT BUY signals on the same daily candle (any group). Most signals first."""
    g = {}
    for r in rows:
        g.setdefault(r["symbol"], []).append(r)
    out = [v for v in g.values() if len({(x["group"], x["signal"]) for x in v}) >= 2]
    out.sort(key=lambda v: (-len({(x["group"], x["signal"]) for x in v}), v[0]["symbol"]))
    return out


def _table_same(groups):
    if not groups:
        return "<p style='color:#9fb0c0'>No stock with several signals on this day.</p>"
    body = []
    for v in groups:
        r0 = v[0]
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=D" % html.escape(r0["symbol"])
        names = "<br>".join("<b>%s</b> <span style='color:#9fb0c0'>(%s)</span>" % (html.escape(x["signal"]), html.escape(x["group"])) for x in v)
        ent = "<br>".join(sw._f(x["entry"]) for x in v)
        sls = "<br>".join("<span style='color:#ff7070'>%s</span> <span style='color:#9fb0c0'>(%.1f%%)</span>" % (sw._f(x["sl"]), x["risk_pct"]) for x in v)
        best = max(v, key=lambda x: (x.get("room_r") is not None, x.get("room_r") or 0))
        blocked = best.get("room_r") is not None and best["room_r"] < 1
        body.append("<tr><td>%s</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td><b>%d</b></td><td>%s</td><td>%s</td><td>%s</td>"
                    "<td style='color:%s'>%s</td><td>%s</td></tr>" % (
                        r0["date"][5:], tv, html.escape(sw._sym_label(r0["symbol"])), len({(x["group"], x["signal"]) for x in v}), names, ent, sls,
                        "#ff7070" if blocked else "#ffb300", html.escape((best.get("room") or "-").split(" [")[0]), sw._rs_cell(r0)))
    return ("<table><tr><th>Day</th><th>Stock</th><th>Signals together</th><th>Signals</th><th>Entry (above high)</th><th>SL (risk)</th><th>Room (best)</th><th>vs Nifty</th></tr>"
            + "".join(body) + "</table>")


def _table_hold_d(rows, day):
    if not rows:
        return "<p style='color:#9fb0c0'>No stock in this list%s.</p>" % ("" if day else " (the list is built by the scan of that day)")
    body = []
    for r in rows:
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=D" % html.escape(r["symbol"])
        nc = r.get("nchg")
        stronger = nc is not None and r.get("chg") is not None and r["chg"] > nc
        body.append("<tr><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td>%s</td><td>%sx</td><td>%s</td><td>%s</td><td>%s%%</td><td>%s%%</td><td style='color:#ffd54f'>%s</td><td>%s</td><td>%d</td>"
                    "<td style='color:%s'>%s%% vs Nifty %s%%</td></tr>" % (
                        tv, html.escape(sw._sym_label(r["symbol"])), html.escape(r["spurt_at"][5:10]), r["spurt_x"], sw._f(r["spurt_close"]), sw._f(r["last"]), r["d_sp"], r["d_sma"],
                        html.escape(r["near"]), html.escape(r["cross_at"][5:10]), r["ago"],
                        "#7be08a" if stronger else "#ff7070", "-" if r.get("chg") is None else "%+.1f" % r["chg"], "-" if nc is None else "%+.1f" % nc))
    return ("<table><tr><th>Stock</th><th>Spurt day</th><th>Spurt</th><th>Spurt close</th><th>Last close</th><th>From spurt close</th><th>From SMA 21</th><th>Nearer to</th>"
            "<th>Crossed all SMAs</th><th>Days since spurt</th><th>Day move</th></tr>" + "".join(body) + "</table>")


def hold_csv(day=None):
    a = _hold_all()
    days = [day] if day else sorted(a)
    out = ["date,symbol,spurt_day,spurt_x,spurt_close,last_close,pct_from_spurt_close,pct_from_sma21,nearer_to,crossed_all_sma_day,days_since_spurt,day_chg_pct,nifty_day_chg_pct,chg5_pct,nifty_chg5_pct"]
    for dd in days:
        for r in a.get(dd, []):
            vals = [dd, r["symbol"], r["spurt_at"][:10], r["spurt_x"], r["spurt_close"], r["last"], r["d_sp"], r["d_sma"], r["near"], r["cross_at"][:10], r["ago"],
                    r.get("chg"), r.get("nchg"), r.get("chg5"), r.get("nchg5")]
            out.append(",".join("" if x is None else str(x).replace(",", " ") for x in vals))
    return "\n".join(out) + "\n"


def page_html(day=None):
    with _lock:
        rows = list(_load())
    days = sorted({r["date"] for r in rows}, reverse=True)
    show = day or (days[0] if days else None)
    rows = [r for r in rows if r["date"] == show] if show else []
    rows.sort(key=lambda r: (r["signal"], r["symbol"]))
    nav = " &middot; ".join("<a href='/daily_scan?date=%s'%s>%s</a>" % (d, " style='font-weight:700;color:#fff'" if d == show else "", d[5:]) for d in days[:20]) or "-"
    dl = " &middot; ".join("<a href='/daily_scan.csv?date=%s' target='_blank' rel='noopener'>%s</a>" % (d, d[5:]) for d in days[:20])
    sd = _status()
    sections = []
    for g, col in GROUPS:
        gr = [r for r in rows if r["group"] == g]
        sections.append("<h3 style='color:%s'>%s &nbsp;<span style='color:#9fb0c0;font-weight:400;font-size:14px'>(%d)</span></h3>%s" % (col, g, len(gr), _table(gr)))
    same = _same_day(rows)
    sections.append("<h3 style='color:#ff80ab'>E. SEVERAL SIGNALS ON THE SAME DAY &nbsp;<span style='color:#9fb0c0;font-weight:400;font-size:14px'>(%d stocks)</span></h3>"
                    "<p style='color:#9fb0c0;margin:0 0 6px'>Stocks that gave 2 or more different BUY signals (any group above) on this daily candle. More signals together = more rules met.</p>%s" % (len(same), _table_same(same)))
    hrows = _hold_all().get(show, []) if show else []
    sections.append("<h3 style='color:#ffd54f'>F. VOLUME SPURT, HOLDING NEAR &nbsp;<span style='color:#9fb0c0;font-weight:400;font-size:14px'>(%d)</span></h3>"
                    "<p style='color:#9fb0c0;margin:0 0 6px'>Daily candles: a green volume spurt (5x or more) within the last 10 daily candles; the close is NOT more than 2%% from the spurt candle close or from the SMA 21; SMA 9 above 21 above 60; the price crossed above all three SMAs within the last 20 days. List only, no alert. <a href='/daily_scan_hold.csv?date=%s' target='_blank' rel='noopener'>Download this list (CSV)</a> &middot; <a href='/daily_scan_hold.csv' target='_blank' rel='noopener'>all days</a></p>%s" % (len(hrows), show or "", _table_hold_d(hrows, show)))
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Daily scan</title>"
            "<style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}table{border-collapse:collapse;width:100%%}th,td{padding:7px 10px;"
            "border-bottom:1px solid #243040;text-align:left;font-size:14px}th{color:#9fb0c0}a{color:#8ab4f8}h3{margin:22px 0 4px}</style></head><body>"
            "<h2>DAILY SCAN - BUY signals on the daily candle of %s</h2>"
            "<p style='color:#9fb0c0'>One scan after the close (Mon-Fri %02d:%02d IST), every NSE stock except the thin ones (average traded value under Rs 1 crore a day, or price under Rs 50). ALL IN ONE PRO signals + CONSOL BOX (STRONG BUY, PULLBACK BUY) + GAP RETRACE (gap up, pullback buy) + RS vs NIFTY (the stock / Nifty line crossed up its SMA 21 with a 5x volume spurt above the 9, 21 and 60 SMA). "
            "T1 / T2 / T6 = 1R / 2R / 6R. Room to resistance = free distance to the nearest resistance in R. vs Nifty = the stock's move on the signal day and over 5 days against Nifty (green = stronger than Nifty). This page places NO order. "
            "<a href='/swing_signals' target='_blank' rel='noopener'>Swing signals (30m / 60m)</a> &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<p>Days: %s &nbsp; <a href='/daily_scan/run' style='border:1px solid #8ab4f8;border-radius:6px;padding:4px 12px'>Scan now (no alert)</a> &nbsp; <span style='color:#9fb0c0'>%s</span><br><span style='color:#6b7c8d;font-size:12px'>%s</span></p>"
            "<p style='color:#9fb0c0'>Download CSV: <a href='/daily_scan.csv' target='_blank' rel='noopener'><b>all days</b></a> &middot; %s</p>%s</body></html>"
            ) % (show or "-", RUN_AT[0], RUN_AT[1], nav, html.escape(sd.get("msg") or "waiting for 15:50"), html.escape(sw.UNI_NOTE or "list not built yet"), dl or "-", "".join(sections))


def register(app, admin_check=None):
    """Adds /daily_scan, /daily_scan.csv and /daily_scan/run to the Flask app (called from swing_page.register)."""
    from flask import request, redirect, Response

    def _page():
        try:
            d = request.args.get("date", "")
            return page_html(d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else None)
        except Exception:
            log.exception("/daily_scan failed")
            return "<p>Daily scan page failed - see the logs.</p>", 500

    def _csv():
        d = request.args.get("date", "")
        d = d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else None
        name = "daily_scan_%s.csv" % (datetime.strptime(d, "%Y-%m-%d").strftime("%d-%m-%Y") if d else "all_days")
        return Response(csv_text(d), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=" + name})

    def _run():
        try:
            if admin_check is not None and not admin_check():
                return "Only the admin can start the scan.", 403
            run_now()
            return redirect("/daily_scan")
        except Exception:
            log.exception("/daily_scan/run failed")
            return "Could not start the scan - see the logs.", 500

    app.add_url_rule("/daily_scan", "dailyscan_page", _page, methods=["GET"])
    app.add_url_rule("/daily_scan.csv", "dailyscan_csv", _csv, methods=["GET"])
    def _hcsv():
        d = request.args.get("date", "")
        d = d if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) else None
        name = "daily_scan_F_%s.csv" % (datetime.strptime(d, "%Y-%m-%d").strftime("%d-%m-%Y") if d else "all_days")
        return Response(hold_csv(d), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=" + name})

    app.add_url_rule("/daily_scan_hold.csv", "dailyscan_hold_csv", _hcsv, methods=["GET"])
    app.add_url_rule("/daily_scan/run", "dailyscan_run", _run, methods=["GET"])
