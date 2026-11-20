"""
SWING BUY SIGNALS page of the scanner      page: /swing_signals      CSV: /swing_signals.csv        ADDED 19 Nov 2026

BUY signals only (selling is not possible in delivery), on the 30-MINUTE and 60-MINUTE candles, for the most liquid stocks only (SWING_TOP_N = 500).
Two separate sections on the page:

  A. VOLUME SPURT + 3 SMA   a candle with volume of 10x or more of the average of the 50 candles before it, GREEN, whose close is ABOVE the 9, 21 AND 60 SMA
                            (of the closes of the same candles). The candle itself must have crossed UP at least one of the three SMAs (it opened below it, or the
                            candle before closed below it) - the three need not be crossed at once, the others may have been crossed earlier.
                            Entry = close of the spurt candle, SL = low of the spurt candle, T1 / T2 / T6 = 1R / 2R / 6R.
  B. INDICATOR BUY SIGNALS  the existing ALL IN ONE PRO BUY rules, run on 30m and 60m candles: SWEEP BUY, OB MITIGATION BUY (Section A engine + rejection candle),
                            VS BUY (spurt_buy.py), SPURT BUY (CHoCH up + RSI above 60). Same code and limits as the 5 / 15-minute scan (signals_scan.py, spurt_buy.py);
                            the only differences: the "30-minute zone must be touched" test of SWEEP / OB is left out (on 30m / 60m candles it would test the
                            same candles), and the spurt must be within the last two sessions instead of "earlier today".

HOW IT RUNS     one stock at a time (no big memory), only the liquid list: top 500 of the stock list by average traded value of the last 20 daily candles
                (read from the daily candles the scanner has already saved; the list is rebuilt once a day, results/swing_universe.json).
                30m candles = the 15-minute candles paired up from 09:15 (same as a TradingView NSE 30m chart); 60m candles = Dhan's 60-minute candles.
                A scan is run after every 30-minute candle has closed (09:46, 10:16 ... 15:31), the 60-minute candles are added at the hour candles (10:16 ... 15:16, 15:31).
                Each scan makes 1 Dhan data call per stock for 30m (+1 for 60m at the hour candles) = 500-1000 calls, about 1-3 minutes at the scanner's own request limit.
RESULT          every signal is followed on its own timeframe with the same stop cascade as the signals page (SL = -1R, T1 -> SL to entry, T2 -> SL to T1 ... T6).
                A swing signal is followed for 10 days. Records: results/swing_signals.json (on the Render disk).
PHONE ALERT     one message per scan with all new signals (title SWING TRADING ALERT), at most MAX_ALERTS_PER_DAY a day, so that the phone-alert service (ntfy) is not overloaded.
                Signals older than ALERT_MAX_AGE_MIN after their candle closed (first scan after a restart, a manual scan, the first scan listing the last sessions) get no alert.
Places NO order (the scanner auto-trade is not connected to this page). Read-only. Any error is caught and logged. Needs no settings.
"""
import os
import json
import html
import time
import logging
import threading
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

log = logging.getLogger("swing_page")
IST = timezone(timedelta(hours=5, minutes=30))

ON = True
TOP_N = int(os.environ.get("SWING_TOP_N", "0") or "0")          # 0 = NO cap: every NSE stock (ETFs / funds already left out) except the thin ones - like the 5 / 15-minute scans
MIN_TURNOVER = float(os.environ.get("SWING_MIN_TURNOVER", "10000000") or "10000000")   # THIN = average traded value of the last 20 daily candles below Rs 1 crore a day (the /depth page uses the same limit)
TFS = (30, 60)
VOL_MULT, VOL_AVG = 10.0, 50
SMAS = (9, 21, 60)
CROSS_MIN = 0                        # the spurt candle itself must cross UP at least this many of the three SMAs (the candle before it closed at / below the SMA)
FROM_BELOW = 5                       # within this many candles before the spurt candle the price must have closed BELOW the highest of the three SMAs (the move comes from DOWN to UP; the SMAs need not all be crossed at once)
WIN_BARS = {30: 26, 60: 14}          # "spurt earlier" window = the last two sessions of candles (13 x 30m or 7 x 60m a session)
ALERT_MAX_AGE_MIN = {30: 45, 60: 75}
MAX_ALERTS_PER_DAY = 14
FOLLOW_DAYS = 10
KEEP_DAYS = 120
GRACE_SEC = 40
LAST_SCAN_MIN = 15 * 60 + 50
HIST_DAYS = {30: 25, 60: 40}         # calendar days of candles downloaded (15-minute candles for 30m)
MIN_PRICE = 50.0
MIN_BARS = 100

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(HERE, "results")
JSON_PATH = os.path.join(RESULTS_DIR, "swing_signals.json")
UNI_PATH = os.path.join(RESULTS_DIR, "swing_universe.json")
CSV_COLS = ["date", "candle", "tf", "section", "symbol", "signal", "entry", "sl", "risk_pct", "t1", "t2", "t6", "resistance", "room_r", "spurt_x", "crossed", "reached", "exit", "final_r",
            "last_price", "alert", "found_at", "updated", "chg_pct", "nifty_chg_pct", "chg5_pct", "nifty_chg5_pct", "strong_stars", "sequence"]
COLOURS = {"VOLUME SPURT + SMA": "#00e5ff", "SWEEP": "#ffb300", "OB MITIGATION": "#b388ff", "VS BUY": "#4caf50", "SPURT BUY": "#69f0ae", "RS CROSS + SPURT": "#f48fb1", "RS CROSS DOWN + SPURT": "#ff7070"}

_lock = threading.RLock()
_rows = None
_run_lock = threading.Lock()
_st = {"on": False, "msg": "", "done": 0, "total": 0, "last_end": "", "last_slot": "", "slot_done": "", "alerts_day": "", "alerts_n": 0, "universe": 0, "found": 0}


def _now():
    return datetime.now(IST)


def _f(v):
    try:
        return "-" if v is None else "%.2f" % float(v)
    except Exception:
        return "-"


# ------------------------------------------------------------------------------------------------------- records
STATUS_PATH = os.path.join(RESULTS_DIR, "swing_status.json")
_mt = {"rows": 0.0}


def _set(**kw):
    """Updates the scan status and writes it to the disk, so the page shows it whichever worker / thread serves the page."""
    _st.update(kw)
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        keep = {k: _st[k] for k in ("on", "msg", "done", "total", "last_end", "universe", "found")}
        keep["t"] = time.time()
        with open(STATUS_PATH + ".tmp", "w") as f:
            json.dump(keep, f)
        os.replace(STATUS_PATH + ".tmp", STATUS_PATH)
    except Exception:
        pass


def _status():
    """The status as saved by the worker that scans (falls back to this worker's own)."""
    try:
        with open(STATUS_PATH) as f:
            d = json.load(f)
        if time.time() - d.get("t", 0) < 3 * 3600:
            return d
    except Exception:
        pass
    return dict(_st)


def _load():
    global _rows
    try:
        mt = os.path.getmtime(JSON_PATH)
    except Exception:
        mt = 0.0
    if _rows is None or (not _st["on"] and mt > _mt["rows"] + 0.5):          # another worker wrote the file: read it again
        try:
            with open(JSON_PATH) as f:
                _rows = json.load(f)
            if not isinstance(_rows, list):
                _rows = []
        except Exception:
            _rows = [] if _rows is None else _rows
        _mt["rows"] = mt
    return _rows


def _save():
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        cut = (_now() - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
        rows = [r for r in (_rows or []) if r.get("date", "") >= cut]
        with open(JSON_PATH + ".tmp", "w") as f:
            json.dump(rows, f)
        os.replace(JSON_PATH + ".tmp", JSON_PATH)
        _mt["rows"] = os.path.getmtime(JSON_PATH)
    except Exception as e:
        log.warning("swing: could not save: %s", e)


def _levels(entry, sl):
    r = abs(entry - sl)
    return entry + r, entry + 2 * r, entry + 6 * r


def _res_levels(d, i, entry):
    """Every RESISTANCE level above the entry as it was at candle i: [(price, kind)], kind = "zone" (spurt_zones.live_zones, the zones the indicator draws) or "swing high" (an open
    3-left / 3-right pivot high that no later candle has exceeded). Sorted nearest first."""
    out = []
    try:
        import spurt_zones
        for z in (spurt_zones.live_zones(d.iloc[:i + 1]) or []):
            if z.get("type") == "RESISTANCE" and isinstance(z.get("lo"), (int, float)) and z["lo"] > entry:
                out.append((float(z["lo"]), "zone"))
    except Exception as e:
        log.warning("swing: zones failed: %s", e)
    try:
        h = d["high"].to_numpy(dtype=float)[:i + 1]
        cl = d["close"].to_numpy(dtype=float)[:i + 1]
        for k in range(max(3, len(h) - 200), len(h) - 3):
            # a swing high stays open until a later CLOSE is above it (same as the indicator: a wick through it does not break it)
            if h[k] > entry and h[k] >= h[k - 3:k].max() and h[k] >= h[k + 1:k + 4].max() and cl[k + 1:].max() <= h[k]:
                out.append((float(h[k]), "swing high"))
    except Exception as e:
        log.warning("swing: swing highs failed: %s", e)
    out.sort(key=lambda x: x[0])
    return out


def _levels_text(levels, n=4):
    """The nearest few levels, for the Room column: [zone 668.8 | swing high 701.2]."""
    return "  [" + " | ".join("%s %s" % (k, ("%.2f" % p).rstrip("0").rstrip(".")) for p, k in levels[:n]) + "]" if levels else "  [no levels found]"


def _nearest_res(d, i, entry):
    """Nearest RESISTANCE above the entry as it was at candle i (None when there is none)."""
    lv = _res_levels(d, i, entry)
    return lv[0][0] if lv else None


def _room_text(entry, sl, res):
    """(R of free room, short text) from the entry up to the resistance."""
    r = entry - sl
    if res is None or r <= 0:
        return None, "no resistance found above"
    x = (res - entry) / r
    tail = "BLOCKED before T1" if x < 1 else "T1 free, T2 blocked" if x < 2 else "T1, T2 free, T6 blocked" if x < 6 else "all targets free"
    return round(x, 1), "%.1fR to resistance %s (%s)" % (x, ("%.2f" % res).rstrip("0").rstrip("."), tail)


def _add(tf, section, sym, name, cand_start, cand_end, entry, sl, spurt_x, crossed, alert_ok, res=None):
    """Adds one signal (once). Returns the new row or None."""
    warn = section == "D"                                         # D = EXIT WARNING (RS cross down): not a trade, "sl" holds the candle HIGH for reference, no targets, no follow-up
    if not (warn and sl > entry > 0) and not (not warn and entry > sl > 0):
        return None
    key = "%s|%d|%s|%s" % (sym, tf, name, cand_start)
    with _lock:
        rows = _load()
        for r in rows:
            if r.get("key") == key:
                return None
        t1, t2, t6 = (None, None, None) if warn else _levels(entry, sl)
        age = (_now().replace(tzinfo=None) - cand_end).total_seconds() / 60.0
        row = {"key": key, "date": cand_start[:10], "candle": cand_start[11:16], "tf": tf, "section": section, "symbol": sym, "signal": name,
               "entry": round(entry, 2), "sl": round(sl, 2), "risk_pct": round(abs(entry - sl) / entry * 100.0, 2), "t1": None if warn else round(t1, 2), "t2": None if warn else round(t2, 2), "t6": None if warn else round(t6, 2),
               "spurt_x": round(spurt_x, 1) if spurt_x else None, "crossed": crossed, "reached": 0, "exit": "WARNING" if warn else "OPEN", "final_r": None, "last": None,
               "alert": bool(alert_ok and age <= ALERT_MAX_AGE_MIN.get(tf, 60)), "found": _now().strftime("%Y-%m-%d %H:%M"), "upd": "", "cs": cand_start, "res": round(res, 2) if res else None, "room_r": None if warn else _room_text(entry, sl, res)[0], "room": "" if warn else _room_text(entry, sl, res)[1]}
        rows.append(row)
        return row


# ------------------------------------------------------------------------------------------------------- liquid list
def _read_turnover(sym, ls):
    """(average traded value of the last 20 daily candles, last close) from the daily file the scanner has already saved; None if there is none."""
    p = os.path.join(ls.DAILY_CACHE_DIR, sym + ".csv")
    if not os.path.exists(p) or time.time() - os.path.getmtime(p) > 12 * 86400:
        return None
    df = pd.read_csv(p, index_col=0, usecols=lambda c: c in ("close", "volume") or c == 0 or c == "" or c.lower().startswith("unnamed") or c in ("date", "timestamp", "d"))
    df = df.tail(20)
    if len(df) < 10:
        return None
    t = (df["close"].astype(float) * df["volume"].astype(float)).mean()
    return float(t), float(df["close"].iloc[-1])


TURN_PATH = os.path.join(RESULTS_DIR, "swing_turnover.json")


def _market_closed(now):
    return now.weekday() < 5 and now.hour * 60 + now.minute >= 15 * 60 + 35


def _universe():
    """[(symbol, security_id)] of every NSE stock except the thin ones; TOP_N > 0 caps the list. Rebuilt once a day (and once more after the close, when the day's traded values are complete).
    THIN = average traded value of the last 20 daily candles under MIN_TURNOVER (when the scanner has saved daily candles). Without saved daily candles the traded value of the DAY from the quote call is
    used: after the close it is saved (swing_turnover.json) and the next days use that saved value, so a morning list is not judged on a half day of trading."""
    global UNI_NOTE
    now = _now()
    today = now.strftime("%Y-%m-%d")
    closed = _market_closed(now)
    try:
        with open(UNI_PATH) as f:
            d = json.load(f)
        if (d.get("date") == today and d.get("top_n") == TOP_N and d.get("min_turnover") == MIN_TURNOVER and d.get("ver") == 3 and d.get("list")
                and (d.get("closed") or not closed)):
            UNI_NOTE = d.get("note") or ""
            return [tuple(x) for x in d["list"]]
    except Exception:
        pass
    import live_scanner as ls
    import spurt10_live as sp
    _set(msg="building the liquid list: reading the stock list")
    t0 = time.time()
    uni = sp._load_universe()
    log.info("swing: stock list of %d read in %.1fs", len(uni), time.time() - t0)
    ranked = []
    cnt = {"all": len(uni), "nofile": 0, "cheap": 0, "thin": 0}
    for k, (sym, sid) in enumerate(uni):
        if k % 300 == 0:
            _set(msg="building the liquid list: daily candles %d / %d" % (k, len(uni)))
        try:
            r = _read_turnover(sym, ls)
        except Exception:
            r = None
        if not r:
            cnt["nofile"] += 1                                   # no saved daily candles (or older than 12 days)
        elif r[1] < MIN_PRICE:
            cnt["cheap"] += 1
        elif r[0] < MIN_TURNOVER:
            cnt["thin"] += 1
        else:                                                    # thin stocks (little traded value) and stocks under Rs 50 are left out
            ranked.append((r[0], sym, sid))
    src = "daily candles"
    if len(ranked) < 150:                      # no / few saved daily candles: the traded value of the day from the quote call
        ranked, src = [], "quotes"
        cnt["cheap"] = cnt["thin"] = 0
        _set(msg="building the liquid list: reading today's quotes (no saved daily candles found)")
        by_id = {str(sid): sym for sym, sid in uni}
        ids = [int(x) for x in by_id]
        today_t = {}
        for i in range(0, len(ids), 1000):
            resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": ids[i:i + 1000]}, "swing quote batch")
            for sid, q in ((resp.json().get("data") or {}).get("NSE_EQ") or {}).items():
                try:
                    px, vol = float(q.get("last_price")), float(q.get("volume"))
                except (TypeError, ValueError):
                    continue
                if str(sid) in by_id:
                    today_t[by_id[str(sid)]] = (px, px * vol, str(sid))
            time.sleep(1.1)
        saved = None
        if closed and len(today_t) > 500:                       # the day is complete: save it for the next days
            try:
                os.makedirs(RESULTS_DIR, exist_ok=True)
                with open(TURN_PATH, "w") as f:
                    json.dump({"date": today, "t": {k: round(v[1]) for k, v in today_t.items()}}, f)
                saved = {k: v[1] for k, v in today_t.items()}
            except Exception:
                pass
        if saved is None:
            try:
                with open(TURN_PATH) as f:
                    td = json.load(f)
                saved = td.get("t") or None
                src = "quotes + the traded value saved on " + str(td.get("date"))
            except Exception:
                saved = None
        if closed and saved is not None and src == "quotes":
            src = "quotes (full day)"
        for sym, (px, tv, sid) in today_t.items():
            if px < MIN_PRICE:
                cnt["cheap"] += 1
                continue
            t_use = saved.get(sym, 0.0) if saved is not None else None      # None = no saved value yet (first day): nobody is called thin
            if t_use is not None and t_use < MIN_TURNOVER:
                cnt["thin"] += 1
                continue
            ranked.append((t_use if t_use is not None else tv, sym, sid))
        if saved is None:
            src = "quotes, no thin filter yet (the traded values are saved after 15:35)"
    ranked.sort(reverse=True)
    out = [(x, sid) for _t, x, sid in (ranked[:TOP_N] if TOP_N > 0 else ranked)]
    UNI_NOTE = ("stock list %d - no saved daily candles %d - price under Rs %g: %d - thin (under Rs %g crore a day): %d - IN THE LIST: %d (%s)"
                % (cnt["all"], cnt["nofile"], MIN_PRICE, cnt["cheap"], MIN_TURNOVER / 1e7, cnt["thin"], len(out), src))
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(UNI_PATH, "w") as f:
            json.dump({"date": today, "top_n": TOP_N, "min_turnover": MIN_TURNOVER, "ver": 3, "closed": closed, "note": UNI_NOTE, "src": src, "list": out}, f)
    except Exception:
        pass
    log.info("swing: liquid list of %d stocks built from %s - %s", len(out), src, UNI_NOTE)
    return out


# ------------------------------------------------------------------------------------------------------- candles
def _candles(sid, tf, as_of, ls):
    """Closed candles of this timeframe (index = UTC open time, as Dhan gives it)."""
    if tf == 30:
        d15 = ls.fetch_intraday_history(sid, "NSE_EQ", 15, HIST_DAYS[30])
        if d15 is None or d15.empty:
            return None
        return ls.drop_forming_bars(ls.build_merged_bars(d15, 2), 30, as_of)
    d60 = ls.fetch_intraday_history(sid, "NSE_EQ", 60, HIST_DAYS[60])
    if d60 is None or d60.empty:
        return None
    return ls.drop_forming_bars(d60, 60, as_of)


# ADDED 20 Nov 2026 (speed): ONE 15-minute download per stock gives BOTH the 30-minute candles (pairs of 15m) and the 60-minute candles (groups of 4 from 09:15,
# the same grid Dhan uses: 09:15, 10:15 ... 15:15) - half the Dhan calls, so a scan finishes about twice as fast. False = the old way (a separate 60-minute download).
SIXTY_FROM_15 = True


def _candles_all(sid, tfs, as_of, ls):
    """{tf: closed candles}. With SIXTY_FROM_15 a single 15-minute download (HIST_DAYS[60] days) feeds both timeframes. Never raises."""
    out = {}
    if not SIXTY_FROM_15:
        for tf in tfs:
            out[tf] = _candles(sid, tf, as_of, ls)
        return out
    d15 = ls.fetch_intraday_history(sid, "NSE_EQ", 15, max(HIST_DAYS[60], HIST_DAYS[30]) if 60 in tfs else HIST_DAYS[30])
    if d15 is None or d15.empty:
        return {tf: None for tf in tfs}
    for tf in tfs:
        try:
            if tf == 30:
                out["h15"] = ls.drop_forming_bars(d15, 15, as_of)                    # ADDED 20 Nov: the closed 15m candles, for the section F list (no extra download)
                d = d15[d15.index >= d15.index.max() - pd.Timedelta(days=HIST_DAYS[30])]    # same history length as before
                out[tf] = ls.drop_forming_bars(ls.build_merged_bars(d, 2), 30, as_of)
            elif tf == 60:
                out[tf] = ls.drop_forming_bars(ls.build_merged_bars(d15, 4), 60, as_of)
            else:
                out[tf] = _candles(sid, tf, as_of, ls)
        except Exception as e:
            log.warning("swing: %s %dm build failed: %s", sid, tf, e)
            out[tf] = None
    return out


def _sma(c, n):
    return pd.Series(c).rolling(n, min_periods=n).mean().to_numpy()


# ------------------------------------------------------------------------------------------------------- the signals
def spurt_sma_signals(o, h, l, c, v, win):
    """Section A. Returns [(i, volume multiple, 'x,y' SMAs crossed by the candle)] for candles i inside the window.
    ONLY THE FIRST candle of a move: after a signal the rule is disarmed until the price closes at / below the highest of the three SMAs again."""
    n = len(c)
    s = {k: _sma(c, k) for k in SMAS}
    out = []
    armed = True
    for i in range(max(VOL_AVG, SMAS[-1]), n):
        top = max(s[9][i], s[21][i], s[60][i])
        fire = False
        if c[i] > o[i] and armed and not np.isnan(top) and all(c[i] > s[k][i] for k in SMAS):
            a = v[i - VOL_AVG:i].mean()
            if a > 0 and v[i] / a >= VOL_MULT:
                crossed = [k for k in SMAS if not np.isnan(s[k][i - 1]) and c[i - 1] <= s[k][i - 1]]
                if len(crossed) >= CROSS_MIN and any((not np.isnan(s[60][i - j])) and c[i - j] <= max(s[9][i - j], s[21][i - j], s[60][i - j]) for j in range(1, FROM_BELOW + 1)):
                    fire = True                                          # price was below the highest SMA within the last candles = a move from below
                    if win[i]:
                        out.append((i, float(v[i] / a), ",".join(str(k) for k in crossed) or "-"))
        if fire:
            armed = False
        elif not np.isnan(top) and c[i] <= top:
            armed = True
    return out


def _section_a_buy(d, win, ok_buy, mx, ss):
    """SWEEP BUY / OB MITIGATION BUY of the last two closed candles (same rules as signals_scan._section_a, without the 30-minute zone test)."""
    out = []
    try:
        import all_in_one_scanner as aio
        n = len(d)
        o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
        sa_sweep, sa_ob = getattr(ss, "SA_SWEEP_MULT", 3.0), getattr(ss, "SA_OB_MULT", 10.0)
        sa_wick, sa_vol = getattr(ss, "SA_WICK", 70.0), getattr(ss, "SA_VOL", 1.5)
        for k in (0, 1):
            m = n - k
            if m < 90 or not win[m - 1]:
                continue
            a = aio.compute_section_a(d.iloc[:m])
            if not a or not a.get("signal") or not a.get("levels"):
                continue
            if a["signal"] not in ("SWEEP_BUY", "OB_BUY"):
                continue
            nm = "SWEEP" if a["signal"] == "SWEEP_BUY" else "OB MITIGATION"
            i = m - 1
            if mx < (sa_sweep if nm == "SWEEP" else sa_ob) or not ok_buy[i]:
                continue
            rng = h[i] - l[i]
            if rng <= 0 or c[i] <= o[i]:
                continue
            wick = (min(o[i], c[i]) - l[i]) / rng * 100.0
            base = v[max(0, i - 50):i]
            if wick < sa_wick or not (v[i - 1] > 0 and v[i] / v[i - 1] >= sa_vol and len(base) >= 10 and base.mean() > 0 and v[i] / base.mean() >= sa_vol):
                continue
            lv = a["levels"]
            out.append((i, nm, float(lv["entry"]), float(lv["sl"])))
    except Exception as e:
        log.warning("swing: section A failed: %s", e)
    return out


def detect(d, win):
    """All BUY signals of the candles inside win (bool array) of one candle frame d (open/high/low/close/volume).
    Returns [(i, section, name, entry, sl, volume multiple or None, 'SMAs crossed')]. Used by the swing page and by the daily scan (daily_scan.py)."""
    import signals_scan as ss
    import spurt_buy
    n = len(d)
    o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
    res = []
    for i, mult, crossed in spurt_sma_signals(o, h, l, c, v, win):                                  # ---- section A
        res.append((i, "A", "VOLUME SPURT + SMA", float(c[i]), float(l[i]), mult, crossed))
    mult_all = np.zeros(n)
    for i in range(VOL_AVG, n):
        a = v[i - VOL_AVG:i].mean()
        mult_all[i] = v[i] / a if a > 0 else 0.0
    mx = float(mult_all[win].max()) if win.any() else 0.0
    if mx < min(getattr(ss, "SA_SWEEP_MULT", 3.0), VOL_MULT):
        return res
    try:
        import sma_rule
        ok_buy, _ok_sell = sma_rule.ok_arrays(c)
    except Exception:
        ok_buy = np.ones(n, dtype=bool)
    found = []                                                                                        # ---- section B
    for i, nm, entry, sl in _section_a_buy(d, win, ok_buy, mx, ss):
        found.append((i, nm, entry, sl))
    if mx >= ss.VS_MULT:
        try:
            for e in spurt_buy.buy_events(o, h, l, c, v, win, ss.VS_MULT):
                if float(e["sl"]) < float(c[e["i"]]):
                    found.append((e["i"], "VS BUY", float(c[e["i"]]), float(e["sl"])))
        except Exception as e:
            log.warning("swing: VS BUY failed: %s", e)
        try:
            rsi = ss._rsi(c)
            for i, side, sl in ss._spurt_d(o, h, l, c, v, win, rsi):
                if side == "BUY" and ok_buy[i]:
                    found.append((i, "SPURT BUY", float(c[i]), float(sl)))
        except Exception as e:
            log.warning("swing: SPURT BUY failed: %s", e)
    for i, nm, entry, sl in found:
        res.append((i, "B", nm, entry, sl, float(mult_all[i]) if mult_all[i] else None, ""))
    return res


# ------------------------------------------------------------------------------------------------------- SECTION C: RS vs NIFTY cross up + volume spurt (ADDED 20 Nov 2026)
# Same RS vs NIFTY as the TradingView indicator: ratio = stock close / NIFTY close, its SMA 21; "cross up" = the ratio moves from below to above its SMA.
# Signal: the ratio crossed up on this candle or the one before, this candle is green, closes ABOVE all three price SMAs (9, 21, 60) and is the candle that crosses them (the SMAs need not all be crossed on this candle,
# but at least one is, and the previous candle was not yet above all three), and its volume is RS_SPURT_MULT x (5x) the average of the 50 candles before it.
# Entry = close, SL = low of the candle (same as section A). Uses the NIFTY 15-minute candles loaded once per scan (_nifty_load); no NIFTY data = no section C signal, nothing else changes.
RS_ON = True
RS_SPURT_MULT = 5.0
RS_SMA_LEN = 21
RS_CROSS_WITHIN = 2           # the cross may be on the signal candle or the candle before it


def _nifty_by_end(tf):
    """{candle end (IST) -> NIFTY close of the 15-minute candle that ends there}, built once per scan."""
    m = _NIF.get("map")
    if m is None or _NIF.get("map_n") != len(_NIF["t"]):
        m = {t + timedelta(minutes=15): c for t, c in zip(_NIF["t"], _NIF["c"])}
        _NIF["map"], _NIF["map_n"] = m, len(_NIF["t"])
    return m


def rs_cross_signals(d, ist, tf):
    """[(i, entry, sl, volume multiple, text)] for the candles of d (newest candles only, see WIN_BARS)."""
    out = []
    if not RS_ON or not _NIF["t"]:
        return out
    n = len(d)
    c = d["close"].to_numpy(dtype=float)
    o = d["open"].to_numpy(dtype=float)
    l = d["low"].to_numpy(dtype=float)
    v = d["volume"].to_numpy(dtype=float)
    m = _nifty_by_end(tf)
    nc = np.full(n, np.nan)
    for i in range(n):
        e = (ist[i] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)
        for back in (0, 15, 30, 45):                                   # the last 30m / 60m candle of a day is shorter (market closes 15:30): use the last NIFTY candle that ended before it
            x = m.get(e - timedelta(minutes=back))
            if x:
                nc[i] = x
                break
    sm = {k: _sma(c, k) for k in SMAS}                                  # price SMAs 9 / 21 / 60: the candle must close above ALL of them, and it must be the candle that completes the cross
    ratio = c / nc
    sma = pd.Series(ratio).rolling(RS_SMA_LEN).mean().to_numpy()
    above = ratio > sma
    ok = ~np.isnan(ratio) & ~np.isnan(sma)
    for i in range(max(RS_SMA_LEN + 1, VOL_AVG), n):
        if not (ok[i] and above[i] and c[i] > o[i] and l[i] > 0 and c[i] > l[i]):
            continue
        if any(np.isnan(sm[k][i]) or np.isnan(sm[k][i - 1]) for k in SMAS):
            continue
        if not all(c[i] > sm[k][i] for k in SMAS):
            continue                                                     # not above all three SMAs
        cr = [k for k in SMAS if c[i - 1] <= sm[k][i - 1]]               # SMAs this candle crossed from below (the others were crossed earlier: not necessary all at once)
        if not cr:
            continue                                                     # price was already above all three before this candle: this candle did not cross
        a = v[i - VOL_AVG:i].mean()
        if a <= 0 or v[i] / a < RS_SPURT_MULT:
            continue
        crossed = False
        for k in range(i, i - RS_CROSS_WITHIN, -1):
            if k >= 1 and ok[k] and ok[k - 1] and above[k] and not above[k - 1]:
                crossed = True
        if crossed:
            out.append((i, float(c[i]), float(l[i]), float(v[i] / a), "RS up SMA21; candle crossed SMA " + ",".join(str(k) for k in cr)))
    return out


def rs_cross_down_signals(d, ist, tf):
    """The opposite of rs_cross_signals: [(i, close, candle high, volume multiple, text)]. The RS line crossed DOWN through its SMA 21 on this candle or the one before, the candle is red,
    closes BELOW all three SMAs (9, 21, 60), crosses at least one of them from above (the candle before was not yet below all three) and has RS_SPURT_MULT x volume.
    An EXIT WARNING for a stock you hold (section D) - not a trade."""
    out = []
    if not RS_ON or not _NIF["t"]:
        return out
    n = len(d)
    c = d["close"].to_numpy(dtype=float)
    o = d["open"].to_numpy(dtype=float)
    h = d["high"].to_numpy(dtype=float)
    v = d["volume"].to_numpy(dtype=float)
    m = _nifty_by_end(tf)
    nc = np.full(n, np.nan)
    for i in range(n):
        e = (ist[i] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)
        for back in (0, 15, 30, 45):
            x = m.get(e - timedelta(minutes=back))
            if x:
                nc[i] = x
                break
    sm = {k: _sma(c, k) for k in SMAS}
    ratio = c / nc
    sma = pd.Series(ratio).rolling(RS_SMA_LEN).mean().to_numpy()
    below = ratio < sma
    ok = ~np.isnan(ratio) & ~np.isnan(sma)
    for i in range(max(RS_SMA_LEN + 1, VOL_AVG), n):
        if not (ok[i] and below[i] and c[i] < o[i] and h[i] > c[i] > 0):
            continue
        if any(np.isnan(sm[k][i]) or np.isnan(sm[k][i - 1]) for k in SMAS):
            continue
        if not all(c[i] < sm[k][i] for k in SMAS):
            continue
        cr = [k for k in SMAS if c[i - 1] >= sm[k][i - 1]]
        if not cr:
            continue
        a = v[i - VOL_AVG:i].mean()
        if a <= 0 or v[i] / a < RS_SPURT_MULT:
            continue
        crossed = False
        for k in range(i, i - RS_CROSS_WITHIN, -1):
            if k >= 1 and ok[k] and ok[k - 1] and below[k] and not below[k - 1]:
                crossed = True
        if crossed:
            out.append((i, float(c[i]), float(h[i]), float(v[i] / a), "RS down SMA21; candle crossed down SMA " + ",".join(str(k) for k in cr)))
    return out


# ------------------------------------------------------------------------------------------------------- STRONG STOCK = signals in sequence (ADDED 20 Nov 2026)
# A BUY signal is marked "STRONG STOCK" when the RS vs NIFTY line crossed UP through its SMA 21 shortly BEFORE it (within RS_SEQ_BARS candles, about 3 sessions) and is still above it on the
# signal candle. Two stars when an earlier BUY signal of another type on the same stock (last SEQ_DAYS days) is in that sequence too. Shown on the page, in phone alerts and in the CSV.
# It does not change any signal rule.
RS_SEQ_BARS = {30: 39, 60: 21}
SEQ_DAYS = 3


def rs_first(d, ist, tf, i):
    """IST text of the most recent RS cross UP within RS_SEQ_BARS candles up to candle i, when the RS line is above its SMA at i; else None."""
    try:
        if not RS_ON or not _NIF["t"] or i < RS_SMA_LEN + 2:
            return None
        n = len(d)
        c = d["close"].to_numpy(dtype=float)
        m = _nifty_by_end(tf)
        nc = np.full(n, np.nan)
        lo = max(0, i - RS_SEQ_BARS.get(tf, 39) - RS_SMA_LEN - 2)
        for j in range(lo, i + 1):
            e = (ist[j] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)
            for back in (0, 15, 30, 45):
                x = m.get(e - timedelta(minutes=back))
                if x:
                    nc[j] = x
                    break
        ratio = c / nc
        sma = pd.Series(ratio).rolling(RS_SMA_LEN).mean().to_numpy()
        ok = ~np.isnan(ratio) & ~np.isnan(sma)
        if not (ok[i] and ratio[i] > sma[i]):
            return None
        for k in range(i, max(1, i - RS_SEQ_BARS.get(tf, 39)) - 1, -1):
            if ok[k] and ok[k - 1] and ratio[k] > sma[k] and not ratio[k - 1] > sma[k - 1]:
                return ist[k].strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return None


def _seq_mark(r, rs_txt):
    """Sets r['strong'] (0, 1, 2 stars) and r['seq'] (text) for a new swing row. Never raises."""
    try:
        earlier = []
        with _lock:
            for x in _load():
                if x is r or x["symbol"] != r["symbol"] or x.get("section") not in ("A", "B", "C") or x["cs"] >= r["cs"] or x["signal"] == r["signal"]:
                    continue
                if (datetime.strptime(r["date"], "%Y-%m-%d") - datetime.strptime(x["date"], "%Y-%m-%d")).days > SEQ_DAYS:
                    continue
                earlier.append(x)
        earlier.sort(key=lambda x: x["cs"])
        seen, parts = set(), []
        for x in earlier:
            k = (x["signal"], x["cs"])
            if k not in seen:
                seen.add(k)
                parts.append("%s %s (%dm)" % (x["signal"], datetime.strptime(x["cs"], "%Y-%m-%d %H:%M").strftime("%d %b %H:%M"), x["tf"]))
        stars, txt = 0, ""
        if rs_txt:
            stars = 2 if parts else 1
            txt = "RS crossed up %s" % datetime.strptime(rs_txt, "%Y-%m-%d %H:%M").strftime("%d %b %H:%M")
            if parts:
                txt += ", then " + ", ".join(parts[-3:])
            txt += ", now " + r["signal"]
        elif parts:
            txt = "earlier signals: " + ", ".join(parts[-3:])
        r["strong"], r["seq"] = stars, txt
    except Exception as e:
        log.warning("swing: sequence mark failed: %s", e)


# ------------------------------------------------------------------------------------------------------- SECTION F: spurt stocks holding near (ADDED 20 Nov 2026)
# A LIST (no alert, no follow-up) of stocks that now meet ALL of: a volume spurt (>= HOLD_SPURT_X times the 50 candles before, green candle) within the last HOLD_SPURT_BARS closed candles;
# the latest close within HOLD_NEAR_PCT of the spurt candle's close OR of the SMA 21 (the nearer one); SMA 9 > 21 > 60; and the price crossed above all three SMAs within the last HOLD_CROSS_BARS candles.
HOLD_SPURT_X, HOLD_SPURT_BARS, HOLD_NEAR_PCT, HOLD_CROSS_BARS = 5.0, 10, 2.0, 20      # HOLD_NEAR_PCT = the latest close may be at most this % away (1-2%, never more) from the spurt candle's close or the SMA 21; None = no limit
HOLD_PATH = os.path.join(RESULTS_DIR, "swing_holding.json")
_HOLD = {"rows": [], "at": ""}


def holding_row(sym, d, tf, ist):
    """One list row for this stock / timeframe, or None."""
    try:
        n = len(d)
        if n < max(VOL_AVG, SMAS[-1]) + HOLD_CROSS_BARS + 2:
            return None
        c, o, v = (d[k].to_numpy(dtype=float) for k in ("close", "open", "volume"))
        sm = {k: _sma(c, k) for k in SMAS}
        i = n - 1
        if any(np.isnan(sm[k][i]) for k in SMAS) or not (sm[9][i] > sm[21][i] > sm[60][i]):
            return None
        sp = None
        for j in range(i, i - HOLD_SPURT_BARS, -1):                                   # newest spurt first
            a = v[j - VOL_AVG:j].mean()
            if c[j] > o[j] and a > 0 and v[j] / a >= HOLD_SPURT_X:
                sp = (j, v[j] / a)
                break
        if sp is None:
            return None
        cx = None
        for k in range(i, i - HOLD_CROSS_BARS, -1):
            if any(np.isnan(sm[x][k]) or np.isnan(sm[x][k - 1]) for x in SMAS):
                continue
            if all(c[k] > sm[x][k] for x in SMAS) and any(c[k - 1] <= sm[x][k - 1] for x in SMAS):
                cx = k
                break
        if cx is None:
            return None
        d_sp = abs(c[i] - c[sp[0]]) / c[sp[0]] * 100
        d_sma = abs(c[i] - sm[21][i]) / sm[21][i] * 100
        if HOLD_NEAR_PCT is not None and min(d_sp, d_sma) > HOLD_NEAR_PCT:
            return None
        return {"symbol": sym, "tf": tf, "spurt_at": ist[sp[0]].strftime("%Y-%m-%d %H:%M"), "spurt_x": round(float(sp[1]), 1), "spurt_close": round(float(c[sp[0]]), 2),
                "last": round(float(c[i]), 2), "d_sp": round(float(d_sp), 2), "d_sma": round(float(d_sma), 2), "near": "spurt candle close" if d_sp <= d_sma else "SMA 21",
                "cross_at": ist[cx].strftime("%Y-%m-%d %H:%M"), "ago": i - sp[0], "last_at": ist[i].strftime("%Y-%m-%d %H:%M")}
    except Exception as e:
        log.warning("swing: holding list failed for %s: %s", sym, e)
        return None


_hold_lock = threading.Lock()


def _hold_save(rows, tfs=(30, 60)):
    """Replaces the rows of the timeframes in tfs, keeps the others (30m / 60m scan and the 5m pass write the same list)."""
    global _HOLD
    with _hold_lock:
        keep = [r for r in _hold_load().get("rows", []) if r["tf"] not in tfs]
        rows = sorted(rows + keep, key=lambda r: (min(r["d_sp"], r["d_sma"]), r["symbol"]))
        key = "at5" if tuple(tfs) == (5,) else "at"
        _HOLD = dict(_HOLD, rows=rows)
        _HOLD[key] = _now().strftime("%Y-%m-%d %H:%M")
        try:
            with open(HOLD_PATH, "w") as f:
                json.dump(_HOLD, f)
        except Exception as e:
            log.warning("swing: holding list not saved: %s", e)


HOLD5_ON, HOLD5_EVERY_SEC, HOLD5_DAYS = True, 600, 5      # ON (21 Nov request): the separate 5m pass = one more Dhan download per stock every 10 minutes, LAST in the Dhan queue. False = off


def _hold5_pass():
    """The 5m part of section F: one 5-minute download per stock (HOLD5_DAYS days), LAST in the Dhan queue (live_scanner._priority reads this function name), so the alerts and the swing scan stay fast."""
    import live_scanner as ls
    uni = _universe()
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    rows = []
    for sym, sid in uni:
        try:
            d5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, HOLD5_DAYS)
            if d5 is None or d5.empty:
                continue
            d = ls.drop_forming_bars(d5, 5, as_of)
            if len(d) < 100:
                continue
            ist = [t + pd.Timedelta(minutes=330) for t in d.index]
            if (_now().replace(tzinfo=None) - ist[-1].to_pydatetime().replace(tzinfo=None)).days > 3:
                continue
            r = holding_row(sym, d, 5, ist)
            if r:
                rows.append(r)
        except Exception as e:
            log.warning("swing: 5m list %s failed: %s", sym, e)
    _hold_save(rows, (5,))
    log.info("swing: 5m list done: %d stocks", len(rows))


def hold5_loop():
    log.info("Swing page section F: 5m list every %d minutes (Mon-Fri 09:25-15:35), last in the Dhan queue.", HOLD5_EVERY_SEC // 60)
    while True:
        t0 = time.time()
        try:
            now = _now()
            m = now.hour * 60 + now.minute
            if HOLD5_ON and ON and now.weekday() < 5 and 9 * 60 + 25 <= m <= 15 * 60 + 35:
                _hold5_pass()
            else:
                time.sleep(120)
                continue
        except Exception as e:
            log.warning("swing: 5m list pass failed: %s", e)
        time.sleep(max(30, HOLD5_EVERY_SEC - (time.time() - t0)))


def _hold_load():
    global _HOLD
    try:
        if not _HOLD["rows"] and os.path.exists(HOLD_PATH):
            with open(HOLD_PATH) as f:
                _HOLD = json.load(f)
    except Exception:
        pass
    return _HOLD


def _table_hold(rows, empty):
    if not rows:
        return "<p style='color:#9fb0c0'>%s</p>" % empty
    body = []
    for r in rows:
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=%d" % (html.escape(r["symbol"]), r["tf"])
        body.append("<tr><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td>%dm</td><td>%s</td><td>%sx</td><td>%s</td><td>%s</td><td>%s%%</td><td>%s%%</td><td style='color:#ffd54f'>%s</td><td>%s</td><td>%d</td></tr>" % (
            tv, html.escape(_sym_label(r["symbol"])), r["tf"], html.escape(r["spurt_at"][5:]), r["spurt_x"], _f(r["spurt_close"]), _f(r["last"]), r["d_sp"], r["d_sma"],
            html.escape(r["near"]), html.escape(r["cross_at"][5:]), r["ago"]))
    return ("<table><tr><th>Stock</th><th>TF</th><th>Spurt candle</th><th>Spurt</th><th>Spurt close</th><th>Last close</th><th>From spurt close</th><th>From SMA 21</th><th>Nearer to</th><th>Crossed all SMAs</th><th>Candles since spurt</th></tr>%s</table>" % "".join(body))


def scan_frame(sym, d, tf, alert_ok, now_ist):
    """All BUY signals of one stock on one timeframe. d = closed candles. Returns the new rows."""
    new = []
    n = len(d)
    if n < MIN_BARS:
        return new
    ist = [t + pd.Timedelta(minutes=330) for t in d.index]
    win = np.zeros(n, dtype=bool)
    win[n - WIN_BARS[tf]:] = True
    if (now_ist.replace(tzinfo=None) - ist[-1].to_pydatetime().replace(tzinfo=None)).days > 5:
        return new                                                           # stale data (halted stock / old candles)

    def st(i):
        return ist[i].strftime("%Y-%m-%d %H:%M")

    def en(i):
        return (ist[i] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)

    found = list(detect(d, win))
    try:
        for i, entry, sl, mult, txt in rs_cross_signals(d, ist, tf):
            if win[i]:
                found.append((i, "C", "RS CROSS + SPURT", entry, sl, mult, txt))
        for i, entry, hi, mult, txt in rs_cross_down_signals(d, ist, tf):
            if win[i]:
                found.append((i, "D", "RS CROSS DOWN + SPURT", entry, hi, mult, txt))
    except Exception as e:
        log.warning("swing: RS cross failed for %s: %s", sym, e)
    for i, sec, nm, entry, sl, mult, crossed in found:
        r = _add(tf, sec, sym, nm, st(i), en(i), entry, sl, mult, crossed, alert_ok, _nearest_res(d, i, entry))
        if r:
            if sec in ("A", "B", "C"):
                _seq_mark(r, rs_first(d, ist, tf, i))
            try:                                                                         # stock against NIFTY on the signal day (until the signal candle's close)
                r["chg"], r["nchg"] = _stock_chg_at(d, ist, i), _nifty_chg_at(en(i))
                r["chg5"], r["nchg5"] = None, None
            except Exception:
                pass
            new.append(r)
    return new


def _follow(sym, tf, d, ist_txt):
    """Follows the OPEN signals of this stock / timeframe with the candles after the signal candle."""
    import signals_page as spg
    with _lock:
        mine = [r for r in _load() if r["symbol"] == sym and r["tf"] == tf and r["exit"] == "OPEN"]
    if not mine:
        return
    o, h, l, c = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    for r in mine:
        bars = [(ist_txt[j], o[j], h[j], l[j], c[j]) for j in range(len(c)) if ist_txt[j] > r["cs"]]
        if not bars:
            continue
        res = spg.follow("BUY", r["entry"], r["sl"], bars, False)
        if res is None:
            continue
        with _lock:
            r.update(reached=res["reached"], exit=res["exit"], final_r=res["final_r"], last=res["last"], upd=_now().strftime("%m-%d %H:%M"))
            if r["exit"] == "OPEN" and (_now().replace(tzinfo=None) - datetime.strptime(r["date"], "%Y-%m-%d")).days > FOLLOW_DAYS:
                r["exit"] = "10D END"


# ------------------------------------------------------------------------------------------------------- RE-CROSS ALERT (ADDED 20 Nov 2026)
# Every BUY signal of the last FOLLOW_DAYS days - still open, stopped out (failed) or already closed - is watched. When the price closes BELOW its entry and later closes ABOVE the entry again,
# the newest closed candle gives a phone alert "SWING RE-CROSS ALERT" with the signal's date, time and type, its status and how far it had gone. One alert per crossing candle per signal
# (the candle is stored in the row as "rc"). Warnings (section D) are not watched. Needs nothing new from Dhan: it uses the candles the scan already downloaded.
RECROSS_ON = True
MAX_RECROSS_ALERTS_PER_DAY = 14


def _recross(sym, tf, d, ist_txt, alert_ok):
    """Signals of this stock / timeframe whose entry was crossed UP again by the newest closed candle. Returns the rows (marked)."""
    out = []
    if not RECROSS_ON or d is None or len(d) < 3:
        return out
    with _lock:
        mine = [r for r in _load() if r["symbol"] == sym and r["tf"] == tf and r.get("section") != "D"
                and (_now().replace(tzinfo=None) - datetime.strptime(r["date"], "%Y-%m-%d")).days <= FOLLOW_DAYS]
    if not mine:
        return out
    c = d["close"].to_numpy(dtype=float)
    last = len(c) - 1
    for r in mine:
        below = False
        cross_j = None
        for j in range(last + 1):
            if ist_txt[j] <= r["cs"]:
                continue
            if c[j] < r["entry"]:
                below = True
            elif below and c[j] > r["entry"]:
                cross_j, below = j, False
        if cross_j == last and r.get("rc") != ist_txt[last]:
            with _lock:
                r["rc"] = ist_txt[last]
            try:                                                         # only a FRESH crossing alerts (not the first scan after a restart / over a weekend)
                end = datetime.strptime(ist_txt[last], "%Y-%m-%d %H:%M") + timedelta(minutes=tf)
                fresh_ok = (_now().replace(tzinfo=None) - end).total_seconds() / 60.0 <= ALERT_MAX_AGE_MIN.get(tf, 60)
            except Exception:
                fresh_ok = False
            if alert_ok and fresh_ok:
                out.append((r, float(c[last]), ist_txt[last]))
    return out


def _notify_recross(items):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    today = _now().strftime("%Y-%m-%d")
    if _st.get("rc_day") != today:
        _st.update(rc_day=today, rc_n=0)
    if _st.get("rc_n", 0) >= MAX_RECROSS_ALERTS_PER_DAY:
        return
    _st["rc_n"] = _st.get("rc_n", 0) + 1
    blocks = []
    for r, px, cand in items[:8]:
        ex = r.get("exit")
        status = "OPEN" if ex == "OPEN" else ("FAILED - stop-loss was hit" if ex == "SL" else "closed (%s)" % ex)
        best = ("best target so far T%d" % r["reached"]) if r.get("reached") else "no target reached"
        blocks.append("%s (%dm) closed %s - back ABOVE its entry %s\nSignal: %s | given %s %s IST\nStatus: %s | %s | SL %s | T1 %s" % (
            r["symbol"], r["tf"], _f(px), _f(r["entry"]), r["signal"], r["date"], r["candle"], status, best, _f(r["sl"]), _f(r["t1"])))
    if len(items) > 8:
        blocks.append("+%d more" % (len(items) - 8))
    blocks.append("The price crossed up through the entry of an earlier signal again. Look at the chart before you act.")
    body = "\n\n".join(blocks).encode("utf-8")
    title = "SWING RE-CROSS ALERT - price above an earlier signal's entry (%d)" % len(items)

    def _send():
        try:
            import requests
            for attempt in range(3):
                resp = requests.post("https://ntfy.sh/" + topic, data=body, timeout=10, headers={"Title": title, "Priority": "high", "Tags": "repeat"})
                if resp.status_code < 300:
                    return
                time.sleep(8 * (attempt + 1))
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True, name="swing-ntfy-rc").start()


# ------------------------------------------------------------------------------------------------------- phone alert
def _notify(items):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    today = _now().strftime("%Y-%m-%d")
    if _st["alerts_day"] != today:
        _st.update(alerts_day=today, alerts_n=0)
    if _st["alerts_n"] >= MAX_ALERTS_PER_DAY:
        return
    _st["alerts_n"] += 1
    blocks = []
    for r in items[:8]:
        blocks.append("SWING TRADING ALERT\n%s - %s (%dm)\nEntry %s | SL %s (%.1f%%)\nT1 %s | T2 %s | T6 %s\nR:R 1 : 1 / 2 / 6 (T1 = 1R, T2 = 2R, T6 = 6R)\nFree room: %s\nCandle %s IST%s" % (
            r["signal"], r["symbol"], r["tf"], _f(r["entry"]), _f(r["sl"]), r["risk_pct"], _f(r["t1"]), _f(r["t2"]), _f(r["t6"]), r.get("room") or "n/a", r["candle"],
            (" | spurt %sx" % r["spurt_x"]) if r.get("spurt_x") else "") + (("\n%s STRONG STOCK: %s" % ("\u2605" * r["strong"], r["seq"])) if r.get("strong") and r.get("seq") else ""))
    if len(items) > 8:
        blocks.append("+%d more on the swing signals page" % (len(items) - 8))
    body = "\n\n".join(blocks).encode("utf-8")
    title = "SWING TRADING ALERT - BUY" if len(items) == 1 else "SWING TRADING ALERT - BUY (%d signals)" % len(items)

    def _send():
        try:
            import requests
        except Exception:
            return
        for attempt in range(3):
            try:
                resp = requests.post("https://ntfy.sh/" + topic, data=body, timeout=10, headers={"Title": title, "Priority": "high", "Tags": "chart_with_upwards_trend"})
                if resp.status_code < 300:
                    return
            except Exception:
                pass
            time.sleep(8 * (attempt + 1))

    threading.Thread(target=_send, daemon=True, name="swing-ntfy").start()


def _notify_warn(items):
    """EXIT WARNING phone alert (section D): a stock whose RS line crossed down with a red 5x volume candle below all three SMAs. For stocks you HOLD - not a sell order."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not items:
        return
    lines = ["%s (%dm)  close %s | candle high %s | volume %sx | candle %s IST\n%s" % (r["symbol"], r["tf"], _f(r["entry"]), _f(r["sl"]), r.get("spurt_x") or "-", r["candle"], r.get("crossed") or "") for r in items[:8]]
    if len(items) > 8:
        lines.append("+%d more on the swing signals page (section D)" % (len(items) - 8))
    lines.append("If you hold one of these, look at its chart now. This is a warning, not a sell order.")
    body = "\n\n".join(lines).encode("utf-8")
    title = "SWING EXIT WARNING - RS crossed down (%d)" % len(items)

    def _send():
        try:
            import requests
            for attempt in range(3):
                resp = requests.post("https://ntfy.sh/" + topic, data=body, timeout=10, headers={"Title": title, "Priority": "high", "Tags": "warning"})
                if resp.status_code < 300:
                    return
                time.sleep(8 * (attempt + 1))
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True, name="swing-ntfy-warn").start()


# ------------------------------------------------------------------------------------------------------- one scan
def run_scan(tfs=TFS, alerts=True, label=""):
    """Scans the liquid list on the given timeframes. One stock at a time. Returns False if a scan is already running."""
    if not _run_lock.acquire(blocking=False):
        return False
    try:
        import live_scanner as ls
        _set(on=True, msg="starting: building the liquid list", done=0, total=0)
        _t0 = _now().strftime("%H:%M")
        uni = _universe()
        _set(total=len(uni), universe=len(uni), msg="scanning 0 / %d" % len(uni))
        now_ist = _now()
        fresh = []
        recross = []
        hold = []
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        _nifty_load(ls)
        for k, (sym, sid) in enumerate(uni):
            try:
                _dd = _candles_all(sid, tfs, as_of, ls)
            except Exception as e:
                log.warning("swing: %s download failed: %s", sym, e)
                _dd = {}
            for tf in tfs:
                try:
                    d = _dd.get(tf)
                    if d is None or len(d) < MIN_BARS:
                        continue
                    fresh += scan_frame(sym, d, tf, alerts, now_ist)
                    _hr = holding_row(sym, d, tf, [t + pd.Timedelta(minutes=330) for t in d.index])
                    if _hr:
                        hold.append(_hr)
                    if tf == tfs[0] and _dd.get("h15") is not None and len(_dd["h15"]) >= 100:        # 15m rows of the list: same download, once per stock
                        _hr = holding_row(sym, _dd["h15"], 15, [t + pd.Timedelta(minutes=330) for t in _dd["h15"].index])
                        if _hr:
                            hold.append(_hr)
                    _ist_txt = [(t + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M") for t in d.index]
                    _follow(sym, tf, d, _ist_txt)
                    try:
                        recross += _recross(sym, tf, d, _ist_txt, alerts)
                    except Exception as e:
                        log.warning("swing: re-cross check failed for %s: %s", sym, e)
                except Exception as e:
                    log.warning("swing: %s %dm failed: %s", sym, tf, e)
            _set(done=k + 1, msg="scanning %d / %d (started %s IST)" % (k + 1, len(uni), _t0))
        with _lock:
            _save()
        _hold_save(hold, tuple(sorted(set(tfs) | {15})))                                                     # a 30m-only scan keeps the 60m and 5m rows (and the other way round)
        alert_items = [r for r in fresh if r.get("alert") and r.get("section") != "D"]
        warn_items = [r for r in fresh if r.get("alert") and r.get("section") == "D"]
        if alert_items:
            _notify(alert_items)
        if warn_items:
            _notify_warn(warn_items)
        if recross:
            with _lock:
                _save()
            _notify_recross(recross)
        _set(found=len(fresh), last_end=_now().strftime("%Y-%m-%d %H:%M"), msg="finished %s IST (%s) - %d new signals" % (_now().strftime("%H:%M"), label or "scan", len(fresh)))
        log.info("swing: scan %s done: %d new signals (%d alerted)", label, len(fresh), len(alert_items))
        return True
    except Exception as e:
        _set(msg="failed: %s" % e)
        log.warning("swing: scan failed: %s", e)
        return True
    finally:
        _set(on=False)
        _run_lock.release()


def run_now():
    """Page button (admin): scans both timeframes now on the newest closed candles, no phone alert. False when a scan is running."""
    if _st["on"]:
        return False
    threading.Thread(target=run_scan, kwargs=dict(tfs=TFS, alerts=False, label="manual"), daemon=True, name="swing-now").start()
    return True


def _slots(day):
    """[(close time, timeframes)] of the day: a 30m candle closes at 09:45, 10:15 ... 15:15, 15:30; the 60m candles close at 10:15 ... 15:15 and 15:30."""
    out = []
    m = 9 * 60 + 45
    while m <= 15 * 60 + 30:
        out.append((m, (30, 60) if (m % 60 == 15 or m == 15 * 60 + 30) else (30,)))
        m += 30
    return out


def loop():
    try:                                                                                      # ADDED 19 Nov: the daily scan (daily_scan.py) runs in its own thread
        import daily_scan
        threading.Thread(target=daily_scan.loop, daemon=True, name="daily-scan").start()
    except Exception as e:
        log.error("daily scan could not start: %s", e)
    if HOLD5_ON:
        threading.Thread(target=hold5_loop, daemon=True, name="swing-hold5").start()
    log.info("Swing signals scan started: Mon-Fri after every 30-minute candle close (09:46 ... 15:31), every stock except the thin ones, 30m + 60m, BUY only.")
    while True:
        try:
            now = _now()
            if not ON or now.weekday() >= 5:
                time.sleep(300)
                continue
            m = now.hour * 60 + now.minute
            today = now.strftime("%Y-%m-%d")
            due = [(cm, tfs) for cm, tfs in _slots(today) if m * 60 + now.second >= cm * 60 + GRACE_SEC]
            if due and m <= LAST_SCAN_MIN:
                cm, tfs = due[-1]
                tag = "%s %02d:%02d" % (today, cm // 60, cm % 60)
                if _st["slot_done"] != tag and not _st["on"]:
                    _st["slot_done"] = tag
                    run_scan(tfs=tfs, alerts=True, label="%02d:%02d" % (cm // 60, cm % 60))
            time.sleep(15)
        except Exception as e:
            log.error("swing cycle failed: %s", e)
            time.sleep(30)


# ------------------------------------------------------------------------------------------------------- EARLY SWING ALERT
# ADDED 20 Nov 2026: a heads-up BEFORE the 30m / 60m candle closes. The Spurt10 early check (spurt10_live._early_once, every 2 minutes) already finds stocks whose volume is
# spurting inside the forming candle; for those stocks (only) this runs the SAME swing rules (detect()) on the candle AS IT IS NOW. If a rule fires on the forming candle the phone
# gets "EARLY SWING ALERT - look at these". Information only: it is NOT saved to the swing table, places no order, and the candle can still change before it closes - the real
# swing alert / page row comes only after the close. Cost: 1 Dhan call per flagged stock (at most EARLY_SWING_MAX_STOCKS per check), at the highest request priority.
EARLY_SWING_ON = True
EARLY_SWING_MAX_STOCKS = 15
EARLY_SWING_ALERTS_PER_DAY = 40
_early_sw = {"day": "", "n": 0, "done": set()}


def early_swing(stocks, now_ist=None):
    """stocks = [(symbol, security id)] flagged by the early volume spurt. Returns the list of early swing hits (also sent to the phone). Never raises."""
    try:
        if not EARLY_SWING_ON or not stocks:
            return []
        import live_scanner as ls
        now_ist = now_ist or _now()
        today = now_ist.strftime("%Y-%m-%d")
        if _early_sw["day"] != today:
            _early_sw.update(day=today, n=0, done=set())
        if _early_sw["n"] >= EARLY_SWING_ALERTS_PER_DAY:
            return []
        now_utc = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        hits = []
        for sym, sid in list(stocks)[:EARLY_SWING_MAX_STOCKS]:
            try:
                d15 = ls.fetch_intraday_history(sid, "NSE_EQ", 15, HIST_DAYS[60])
                if d15 is None or d15.empty:
                    continue
                for tf, grp in ((30, 2), (60, 4)):
                    d = ls.build_merged_bars(d15, grp)
                    n = len(d)
                    if n < MIN_BARS or d.index[-1] + pd.Timedelta(minutes=tf) <= now_utc:
                        continue                                                      # the last candle is already closed: the normal scan handles it
                    cs = str(d.index[-1])
                    cstr = (d.index[-1] + pd.Timedelta(minutes=330)).strftime("%H:%M")
                    px = float(d["close"].iloc[-1])
                    win = np.zeros(n, dtype=bool)
                    win[n - 1] = True                                                 # ONLY the forming candle
                    if (sym, tf, cs, "buy") not in _early_sw["done"]:
                        found = [r for r in detect(d, win) if r[0] == n - 1]
                        if found:
                            _early_sw["done"].add((sym, tf, cs, "buy"))
                            hits.append({"kind": "buy", "symbol": sym, "tf": tf, "price": px, "sl": float(min(r[4] for r in found)), "signal": " + ".join(sorted({r[2] for r in found})),
                                         "mult": max([r[5] for r in found if r[5]] or [0.0]), "candle": cstr})
                    if RS_ON and (_NIF["t"] or _nifty_ensure(ls)):                    # ADDED 20 Nov: the RS vs NIFTY cross (sections C and D) on the forming candle
                        ist = [t + pd.Timedelta(minutes=330) for t in d.index]
                        if (sym, tf, cs, "rsup") not in _early_sw["done"]:
                            for i, entry, sl, mult, txt in rs_cross_signals(d, ist, tf):
                                if i == n - 1:
                                    _early_sw["done"].add((sym, tf, cs, "rsup"))
                                    hits.append({"kind": "buy", "symbol": sym, "tf": tf, "price": entry, "sl": sl, "signal": "RS CROSS + SPURT", "mult": mult, "candle": cstr})
                        if (sym, tf, cs, "rsdn") not in _early_sw["done"]:
                            for i, entry, hi, mult, txt in rs_cross_down_signals(d, ist, tf):
                                if i == n - 1:
                                    _early_sw["done"].add((sym, tf, cs, "rsdn"))
                                    hits.append({"kind": "warn", "symbol": sym, "tf": tf, "price": entry, "sl": hi, "signal": "RS CROSS DOWN + SPURT", "mult": mult, "candle": cstr})
                    if RS_ON and _NIF["t"]:                                           # ADDED 20 Nov: STRONG STOCK mark on the early buy hits of this stock
                        _ist2 = [t + pd.Timedelta(minutes=330) for t in d.index]
                        _rsx = rs_first(d, _ist2, tf, n - 1)
                        if _rsx:
                            for _h in hits:
                                if _h["kind"] == "buy" and _h["symbol"] == sym and _h["tf"] == tf and _h["candle"] == cstr:
                                    _h["rs"] = datetime.strptime(_rsx, "%Y-%m-%d %H:%M").strftime("%d %b %H:%M")
            except Exception as e:
                log.warning("early swing: %s failed: %s", sym, e)
        if hits:
            _early_sw["n"] += 1
            _early_swing_send(hits)
            log.info("early swing alert: %s", [(h["symbol"], h["tf"]) for h in hits])
        return hits
    except Exception as e:
        log.warning("early swing check failed: %s", e)
        return []


def _early_swing_send(hits):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic or not hits:
        return
    import requests
    for kind, title, tail, tag in (
            ("buy", "EARLY SWING ALERT - look at these (%d)", "Candle still forming - look at these charts; the real swing alert comes after the candle closes.", "eyes"),
            ("warn", "EARLY EXIT WARNING - RS crossing down (%d) - look if you hold", "Candle still forming - if you hold one of these, look at its chart now. A warning, not a sell order; the page row (section D) comes after the close.", "warning")):
        mine = [h for h in hits if h.get("kind", "buy") == kind]
        if not mine:
            continue
        if kind == "buy":
            lines = ["%s  Rs %s  %dm  %s%s  (SL if it closes like this: %s)%s" % (h["symbol"], _f(h["price"]), h["tf"], h["signal"], (" %.1fx so far" % h["mult"]) if h["mult"] else "", _f(h["sl"]),
                     ("\n   \u2605 STRONG STOCK: RS crossed up %s" % h["rs"]) if h.get("rs") else "") for h in mine[:10]]
        else:
            lines = ["%s  Rs %s  %dm  %s%s  (candle high %s)" % (h["symbol"], _f(h["price"]), h["tf"], h["signal"], (" %.1fx so far" % h["mult"]) if h["mult"] else "", _f(h["sl"])) for h in mine[:10]]
        if len(mine) > 10:
            lines.append("+%d more" % (len(mine) - 10))
        lines.append(tail)
        try:
            resp = requests.post("https://ntfy.sh/" + topic, data="\n".join(lines).encode("utf-8"), timeout=8, headers={"Title": title % len(mine), "Priority": "default", "Tags": tag})
            if resp.status_code >= 300:
                log.warning("early swing ntfy refused: %s", resp.status_code)
        except Exception as e:
            log.warning("early swing ntfy failed: %s", e)


# ------------------------------------------------------------------------------------------------------- CSV / page
def _csv_row(r):
    vals = [r.get("date"), r.get("candle"), "%dm" % r.get("tf", 0), {"A": "A volume spurt + SMA", "C": "C RS cross + spurt", "D": "D RS cross DOWN exit warning"}.get(r.get("section"), "B indicator"), r.get("symbol"), r.get("signal"), r.get("entry"),
            r.get("sl"), r.get("risk_pct"), r.get("t1"), r.get("t2"), r.get("t6"), r.get("res"), r.get("room_r"), r.get("spurt_x"), r.get("crossed"), r.get("reached"), r.get("exit"), r.get("final_r"),
            r.get("last"), "yes" if r.get("alert") else "", r.get("found"), r.get("upd"), r.get("chg"), r.get("nchg"), r.get("chg5"), r.get("nchg5"), r.get("strong") or "", r.get("seq") or ""]
    return ",".join("" if x is None else str(x).replace(",", " ") for x in vals)


def csv_text(day=None):
    with _lock:
        rows = [r for r in _load() if (not day or r["date"] == day)]
    rows.sort(key=lambda r: (r["date"], r["candle"]))
    return "\n".join([",".join(CSV_COLS)] + [_csv_row(r) for r in rows]) + "\n"


def _result(r):
    t = _result0(r)
    if r.get("rc"):
        t += " | closed above the entry again %s" % r["rc"][5:]
    return t


def _result0(r):
    ex = r.get("exit")
    if ex == "OPEN":
        return "open (best target %s, now %s R)" % ("T%d" % r["reached"] if r.get("reached") else "none", ("%+.2f" % r["final_r"]) if r.get("final_r") is not None else "-")
    if ex == "WARNING":
        return "exit warning - not a trade"
    if ex == "SL":
        return "stop-loss hit (-1R)"
    return "closed at %s (%+.2f R)" % (ex, r["final_r"] if r.get("final_r") is not None else 0.0)


UNI_NOTE = ""
_fno = {"day": "", "set": set()}


def _fno_symbols():
    """Symbols of the stocks that have stock FUTURES (F&O stocks), from Dhan's instrument list (the same file the scanner already downloads). Read once a day; empty set when it cannot be read."""
    today = _now().strftime("%Y-%m-%d")
    if _fno["day"] == today:
        return _fno["set"]
    out = set()
    try:
        import scrip_master as sm
        sm._ensure_fresh_cache()
        df = pd.read_csv(sm.CACHE_FILE, dtype=str, usecols=lambda c: c in ("SEM_EXM_EXCH_ID", "SEM_INSTRUMENT_NAME", "SEM_TRADING_SYMBOL"))
        df = df[(df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & (df["SEM_INSTRUMENT_NAME"].str.upper() == "FUTSTK")]
        for t in df["SEM_TRADING_SYMBOL"].astype(str).str.strip().str.upper():
            parts = t.rsplit("-", 2)                         # RELIANCE-Nov2026-FUT -> RELIANCE ; BAJAJ-AUTO-Nov2026-FUT -> BAJAJ-AUTO
            if len(parts) == 3 and parts[2] == "FUT":
                out.add(parts[0])
    except Exception as e:
        log.warning("swing: F&O stock list could not be read: %s", e)
    _fno["day"], _fno["set"] = today, out
    return out


def _sym_label(sym):
    """The symbol as shown on the pages: F&O stocks get ' (F&O)' after the symbol."""
    return sym + (" (F&O)" if sym.upper() in _fno_symbols() else "")


# ------------------------------------------------------------------------------------------------------- stock against NIFTY (ADDED 19 Nov)
_NIF = {"t": [], "c": []}        # NIFTY 15-minute candles of the last weeks: IST open time, close


def _nifty_load(ls):
    """NIFTY 50 15-minute candles (Dhan index id 13), once per scan. Any failure leaves the lists empty: the 'vs Nifty' column then shows '-' and nothing else is affected."""
    _NIF["t"], _NIF["c"] = [], []
    try:
        now = datetime.now()
        payload = {"securityId": "13", "exchangeSegment": "IDX_I", "instrument": "INDEX", "interval": 15,
                   "fromDate": (now - timedelta(days=max(HIST_DAYS.values()))).strftime("%Y-%m-%d"), "toDate": now.strftime("%Y-%m-%d")}
        df = ls._parse_intraday(ls._dhan_post("charts/intraday", payload, "NIFTY (interval=15)").json())
        if df is not None and len(df):
            _NIF["t"] = [(t + pd.Timedelta(minutes=330)).to_pydatetime().replace(tzinfo=None) for t in df.index]
            _NIF["c"] = [float(x) for x in df["close"]]
            _NIF["at"] = time.time()
    except Exception as e:
        log.warning("swing: NIFTY candles not available (%s): the vs-Nifty column stays empty", e)


def _nifty_ensure(ls):
    """For the early check: NIFTY candles are normally loaded once per scan; load them here when they are missing or older than 10 minutes. True when available."""
    try:
        if not _NIF["t"] or time.time() - _NIF.get("at", 0) > 600:
            _nifty_load(ls)
        return bool(_NIF["t"])
    except Exception:
        return False


def _nifty_chg_at(end_dt):
    """NIFTY's move (%) from the previous day's close to the last 15-minute candle that had closed by end_dt; None when not known."""
    ts, cs = _NIF["t"], _NIF["c"]
    if not ts:
        return None
    day = end_dt.date()
    now_i = prev_i = None
    for k in range(len(ts) - 1, -1, -1):
        t = ts[k]
        if now_i is None and t.date() == day and t + timedelta(minutes=15) <= end_dt:
            now_i = k
        if t.date() < day:
            prev_i = k
            break
    if now_i is None or prev_i is None or cs[prev_i] <= 0:
        return None
    return round((cs[now_i] / cs[prev_i] - 1.0) * 100.0, 2)


def _stock_chg_at(d, ist, i):
    """The stock's move (%) from the previous day's close to the close of candle i."""
    day = ist[i].date()
    for j in range(i - 1, -1, -1):
        if ist[j].date() < day:
            p = float(d["close"].iloc[j])
            return round((float(d["close"].iloc[i]) / p - 1.0) * 100.0, 2) if p > 0 else None
    return None


def _rs_cell(r):
    """The 'vs Nifty' cell: the stock's move on the signal day against NIFTY's, and (daily scan) over 5 days. Green = stronger than Nifty, red = weaker."""
    c, n = r.get("chg"), r.get("nchg")
    if c is None or n is None:
        return "<span style='color:#6b7c8d'>-</span>"
    strong = c > n
    out = "<span style='color:%s'>%+.1f%% vs Nifty %+.1f%% - %s</span>" % ("#7be08a" if strong else "#ff7070", c, n, "stronger" if strong else "weaker")
    c5, n5 = r.get("chg5"), r.get("nchg5")
    if c5 is not None and n5 is not None:
        out += "<br><span style='color:#9fb0c0'>5 days %+.1f%% vs Nifty %+.1f%%</span>" % (c5, n5)
    return out



def _seq_html(r):
    """The STRONG STOCK mark under the signal name."""
    st_, tx = r.get("strong") or 0, r.get("seq") or ""
    if not tx:
        return ""
    if st_:
        return "<br><span style='color:#ffd54f;font-size:12px;font-weight:400'>%s STRONG STOCK - %s</span>" % ("\u2605" * st_, html.escape(tx))
    return "<br><span style='color:#9fb0c0;font-size:12px;font-weight:400'>%s</span>" % html.escape(tx)


def _table(rows, empty):
    if not rows:
        return "<p style='color:#9fb0c0'>%s</p>" % empty
    body = []
    for r in rows:
        c = COLOURS.get(r["signal"], "#9fb0c0")
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=%d" % (html.escape(r["symbol"]), r["tf"])
        body.append("<tr><td>%s %s</td><td>%dm</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td style='color:%s;font-weight:700'>%s</td><td>%s</td>"
                    "<td style='color:#ff7070'>%s <span style='color:#9fb0c0'>(%.1f%%)</span></td><td style='color:#7be08a'>%s</td><td style='color:#7be08a'>%s</td><td style='color:%s'>%s</td><td>%s</td><td>%s</td>"
                    "<td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
                        r["date"][5:], r["candle"], r["tf"], tv, html.escape(_sym_label(r["symbol"])), c, html.escape(r["signal"]) + _seq_html(r), _f(r["entry"]), _f(r["sl"]), r["risk_pct"], _f(r["t1"]), _f(r["t2"]),
                        "#ff7070" if (r.get("room_r") is not None and r["room_r"] < 1) else "#ffb300", html.escape(r.get("room") or "-"),
                        ("%sx" % r["spurt_x"]) if r.get("spurt_x") else "-", html.escape(r.get("crossed") or "-"), _rs_cell(r), html.escape(_result(r))))
    return ("<table><tr><th>Candle</th><th>TF</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL (risk)</th><th>T1</th><th>T2</th><th>Room to resistance</th><th>Spurt</th><th>SMAs crossed</th><th>vs Nifty</th><th>What happened</th></tr>"
            + "".join(body) + "</table>")


def _same_candle(rows):
    """Groups of BUY signals (sections A, B, C) of one stock / timeframe / candle with 2 or more DIFFERENT signal names. Newest first."""
    g = {}
    for r in rows:
        if r.get("section") in ("A", "B", "C"):
            g.setdefault((r["symbol"], r["tf"], r["cs"]), []).append(r)
    out = [v for v in g.values() if len({x["signal"] for x in v}) >= 2]
    out.sort(key=lambda v: (v[0]["date"], v[0]["candle"]), reverse=True)
    return out


def _table_same(groups, empty):
    if not groups:
        return "<p style='color:#9fb0c0'>%s</p>" % empty
    body = []
    for v in groups[:200]:
        r0 = v[0]
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=%d" % (html.escape(r0["symbol"]), r0["tf"])
        names = "<br>".join("<span style='color:%s;font-weight:700'>%s</span>%s" % (COLOURS.get(x["signal"], "#9fb0c0"), html.escape(x["signal"]), _seq_html(x)) for x in v)
        sls = "<br>".join("<span style='color:#ff7070'>%s</span> <span style='color:#9fb0c0'>(%.1f%%)</span>" % (_f(x["sl"]), x["risk_pct"]) for x in v)
        res = "<br>".join(html.escape(_result(x)) for x in v)
        body.append("<tr><td>%s %s</td><td>%dm</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td><b>%d</b></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
            r0["date"][5:], r0["candle"], r0["tf"], tv, html.escape(_sym_label(r0["symbol"])), len({x["signal"] for x in v}), names, _f(r0["entry"]), sls, _rs_cell(r0), res))
    return ("<table><tr><th>Candle</th><th>TF</th><th>Stock</th><th>Signals together</th><th>Signals</th><th>Entry</th><th>SL of each (risk)</th><th>vs Nifty</th><th>What happened (each)</th></tr>"
            + "".join(body) + "</table>")


def _table_warn(rows, empty):
    if not rows:
        return "<p style='color:#9fb0c0'>%s</p>" % empty
    body = []
    for r in rows:
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=%d" % (html.escape(r["symbol"]), r["tf"])
        body.append("<tr><td>%s %s</td><td>%dm</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td style='color:#ff7070;font-weight:700'>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            r["date"][5:], r["candle"], r["tf"], tv, html.escape(_sym_label(r["symbol"])), html.escape(r["signal"]), _f(r["entry"]), _f(r["sl"]),
            ("%sx" % r["spurt_x"]) if r.get("spurt_x") else "-", html.escape(r.get("crossed") or "-"), _rs_cell(r)))
    return ("<table><tr><th>Candle</th><th>TF</th><th>Stock</th><th>Signal</th><th>Close</th><th>Candle high</th><th>Spurt</th><th>SMAs crossed down</th><th>vs Nifty</th></tr>" + "".join(body) + "</table>")


def _stats(rows):
    done = [r for r in rows if r.get("final_r") is not None]
    if not done:
        return ""
    t1 = sum(1 for r in done if r["reached"] >= 1)
    t2 = sum(1 for r in done if r["reached"] >= 2)
    avg = sum(r["final_r"] for r in done) / len(done)
    return "<p style='color:#9fb0c0'>%d signals followed - T1 reached %d%%, T2 reached %d%%, average result %+.2f R</p>" % (len(done), round(t1 * 100 / len(done)), round(t2 * 100 / len(done)), avg)


def page_html(tf=None):
    with _lock:
        rows = list(_load())
    if tf in (30, 60):
        rows = [r for r in rows if r["tf"] == tf]
    rows.sort(key=lambda r: (r["date"], r["candle"]), reverse=True)
    a = [r for r in rows if r["section"] == "A"][:300]
    b = [r for r in rows if r["section"] == "B"][:300]
    cc = [r for r in rows if r["section"] == "C"][:300]
    dd = [r for r in rows if r["section"] == "D"][:300]
    hh = [r for r in _hold_load().get("rows", []) if tf not in (30, 60) or r["tf"] == tf]
    days = sorted({r["date"] for r in rows}, reverse=True)
    dl = " &middot; ".join("<a href='/swing_signals.csv?date=%s' target='_blank' rel='noopener'>%s</a>" % (d, d[5:]) for d in days[:30])
    tabs = " ".join("<a href='/swing_signals%s' target='_blank' rel='noopener' style='border:1px solid #8ab4f8;border-radius:6px;padding:4px 12px;%s'>%s</a>" % (
        q, "background:#8ab4f8;color:#0f1419" if sel else "", lab) for q, sel, lab in (("", tf not in (30, 60), "30m + 60m"), ("?tf=30", tf == 30, "30m only"), ("?tf=60", tf == 60, "60m only")))
    sd = _status()
    status = html.escape(sd.get("msg") or "waiting for the next 30-minute candle")
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Swing BUY signals</title>"
            "<style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}table{border-collapse:collapse;width:100%%}th,td{padding:7px 10px;"
            "border-bottom:1px solid #243040;text-align:left;font-size:14px}th{color:#9fb0c0}a{color:#8ab4f8}h3{margin:22px 0 4px}</style></head><body>"
            "<h2>SWING BUY signals - 30 minute and 60 minute candles</h2>"
            "<p style='color:#9fb0c0'>BUY only (delivery), every NSE stock except the thin ones (average traded value under Rs 1 crore a day, or price under Rs 50). A scan runs after every 30-minute candle closes (09:46 ... 15:31); the 60-minute candles are added at the hour candles. "
            "Every signal is followed for %d days with the same stop cascade as the signals page (SL -1R, T1 -> SL to entry ...). This page places NO order. "
            "<a href='/spurt10_all_in_one_pro_signals' target='_blank' rel='noopener'>5 / 15-minute signals</a> &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<p>%s &nbsp; <a href='/swing_signals/run' style='border:1px solid #8ab4f8;border-radius:6px;padding:4px 12px'>Scan now (no alert)</a> &nbsp; <span style='color:#9fb0c0'>%s &middot; stocks in the list: %s</span><br><span style='color:#6b7c8d;font-size:12px'>%s</span></p>"
            "<p style='color:#9fb0c0'>Download CSV: <a href='/swing_signals.csv' target='_blank' rel='noopener'><b>all days</b></a> &middot; %s</p>"
            "<h3>A. Volume spurt (10x or more) + candle closed above the 9, 21 and 60 SMA</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>Green candle, volume 10x the average of the 50 candles before it, close above all three SMAs, and it comes from BELOW (within the last 5 candles the price closed below the highest of the 3 SMAs) - so every 10x candle of the move up qualifies. "
            "Entry = close, SL = low of the candle.</p>%s%s"
            "<h3>B. Indicator BUY signals (SWEEP, OB MITIGATION, VS BUY, SPURT BUY)</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>The ALL IN ONE PRO BUY rules on 30m / 60m candles, with the SMA 9/21/60 distance rule.</p>%s%s"
            "<h3>C. RS vs NIFTY cross up + volume spurt (5x or more)</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>The stock / NIFTY line crossed up through its SMA 21 on this candle or the one before (the green arrow of the RS-NIFTY indicator), on a green candle that closes above the 9, 21 and 60 SMA and crosses them (not all at once: some may have been crossed earlier, but this candle completes the cross), with volume 5x the average of the 50 candles before it. "
            "Entry = close, SL = low of the candle. Check the room to resistance on the chart before you act.</p>%s%s"
            "<h3>D. EXIT WARNING - RS vs NIFTY cross down + volume spurt (5x or more)</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>The opposite of section C, for stocks you HOLD: the stock / NIFTY line crossed DOWN through its SMA 21 on this candle or the one before (the red arrow of the RS-NIFTY indicator), on a red candle that closes below the 9, 21 and 60 SMA and crosses them from above, with volume 5x the average of the 50 candles before it. "
            "This is a warning, not a trade: no entry, no targets, no follow-up. The 'SL' column is the candle HIGH, for reference.</p>%s"
            "<h3>E. Several signals on the SAME candle</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>Stocks that gave 2 or more different BUY signals (sections A, B, C) on the same candle and timeframe, listed together. The signals are also in their own sections above; the stronger the agreement, the more rules the candle met. "
            "Each signal keeps its own SL and its own follow-up.</p>%s"
            "<h3>F. Volume-spurt stocks holding near (list, no alert)</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>Stocks that NOW have: a green volume spurt (5x or more) within the last 10 closed candles (5, 15, 30 or 60 minute candles); the latest close is NOT more than 2%% away (1 to 2%%, never more) from the spurt candle's close or from the SMA 21 - both distances are shown and the nearest is first; SMA 9 above 21 above 60; and the price crossed above all three SMAs within the last 20 candles. Rebuilt at every swing scan (last: %s), from candles the scan already downloads (15m, 30m, 60m)%s. It is a list to look at, not a signal: no entry, SL or follow-up.</p>%s</body></html>"
            ) % (FOLLOW_DAYS, tabs, status, sd.get("universe") or "-", html.escape(UNI_NOTE or "list not built yet"), dl or "-",
                 _table(a, "No signal yet."), _stats(a), _table(b, "No signal yet."), _stats(b), _table(cc, "No signal yet."), _stats(cc), _table_warn(dd, "No warning yet."), _table_same(_same_candle(rows), "No candle with several signals yet."),
                 _hold_load().get("at") or "not yet", ("; 5m rows last rebuilt: %s (every 10 minutes, slowest priority)" % _hold_load()["at5"]) if (HOLD5_ON and _hold_load().get("at5")) else "", _table_hold(hh, "No stock meets all the conditions at the last scan."))


# ------------------------------------------------------------------------------------------------------- Flask hook
def register(app, admin_check=None):
    """Adds /swing_signals, /swing_signals.csv and /swing_signals/run to the Flask app (called once from app.py)."""
    import re
    from flask import request, redirect, Response

    def _page():
        try:
            tf = request.args.get("tf", "")
            return page_html(int(tf) if tf in ("30", "60") else None)
        except Exception:
            log.exception("/swing_signals failed")
            return "<p>Swing signals page failed - see the logs.</p>", 500

    def _csv():
        day = request.args.get("date", "")
        day = day if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) else None
        name = "swing_signals_%s.csv" % (datetime.strptime(day, "%Y-%m-%d").strftime("%d-%m-%Y") if day else "all_days")
        return Response(csv_text(day), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=" + name})

    def _run():
        try:
            if admin_check is not None and not admin_check():
                return "Only the admin can start the scan.", 403
            run_now()
            return redirect("/swing_signals")
        except Exception:
            log.exception("/swing_signals/run failed")
            return "Could not start the scan - see the logs.", 500

    app.add_url_rule("/swing_signals", "swingbuy_page", _page, methods=["GET"])
    app.add_url_rule("/swing_signals.csv", "swingbuy_csv", _csv, methods=["GET"])
    app.add_url_rule("/swing_signals/run", "swingbuy_run", _run, methods=["GET"])
    try:                                                                                      # ADDED 19 Nov: /daily_scan page (daily_scan.py)
        import daily_scan
        daily_scan.register(app, admin_check)
    except Exception as e:
        log.error("daily scan page could not be added: %s", e)
