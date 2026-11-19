"""
SWING BUY SIGNALS page of the scanner      page: /swing_signals      CSV: /swing_signals.csv        ADDED 19 Nov 2026

BUY signals only (selling is not possible in delivery), on the 30-MINUTE and 60-MINUTE candles, for the most liquid stocks only (SWING_TOP_N = 300).
Two separate sections on the page:

  A. VOLUME SPURT + 3 SMA   a candle with volume of 10x or more of the average of the 50 candles before it, GREEN, whose close is ABOVE the 9, 21 AND 60 SMA
                            (of the closes of the same candles). The candle itself must have crossed UP at least one of the three SMAs (it opened below it, or the
                            candle before closed below it) - the three need not be crossed at once, the others may have been crossed earlier.
                            Entry = close of the spurt candle, SL = low of the spurt candle, T1 / T2 / T6 = 1R / 2R / 6R.
  B. INDICATOR BUY SIGNALS  the existing ALL IN ONE PRO BUY rules, run on 30m and 60m candles: SWEEP BUY, OB MITIGATION BUY (Section A engine + rejection candle),
                            VS BUY (spurt_buy.py), SPURT BUY (CHoCH up + RSI above 60). Same code and limits as the 5 / 15-minute scan (signals_scan.py, spurt_buy.py);
                            the only differences: the "30-minute zone must be touched" test of SWEEP / OB is left out (on 30m / 60m candles it would test the
                            same candles), and the spurt must be within the last two sessions instead of "earlier today".

HOW IT RUNS     one stock at a time (no big memory), only the liquid list: top 300 of the stock list by average traded value of the last 20 daily candles
                (read from the daily candles the scanner has already saved; the list is rebuilt once a day, results/swing_universe.json).
                30m candles = the 15-minute candles paired up from 09:15 (same as a TradingView NSE 30m chart); 60m candles = Dhan's 60-minute candles.
                A scan is run after every 30-minute candle has closed (09:46, 10:16 ... 15:31), the 60-minute candles are added at the hour candles (10:16 ... 15:16, 15:31).
                Each scan makes 1 Dhan data call per stock for 30m (+1 for 60m at the hour candles) = 300-600 calls, about 1-3 minutes at the scanner's own request limit.
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
TOP_N = int(os.environ.get("SWING_TOP_N", "300") or "300")
TFS = (30, 60)
VOL_MULT, VOL_AVG = 10.0, 50
SMAS = (9, 21, 60)
CROSS_MIN = 1                        # the spurt candle itself must cross UP at least this many of the three SMAs (0 = any candle above all three)
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
CSV_COLS = ["date", "candle", "tf", "section", "symbol", "signal", "entry", "sl", "risk_pct", "t1", "t2", "t6", "spurt_x", "crossed", "reached", "exit", "final_r",
            "last_price", "alert", "found_at", "updated"]
COLOURS = {"VOLUME SPURT + SMA": "#00e5ff", "SWEEP": "#ffb300", "OB MITIGATION": "#b388ff", "VS BUY": "#4caf50", "SPURT BUY": "#69f0ae"}

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


def _add(tf, section, sym, name, cand_start, cand_end, entry, sl, spurt_x, crossed, alert_ok):
    """Adds one signal (once). Returns the new row or None."""
    if not (entry > sl > 0):
        return None
    key = "%s|%d|%s|%s" % (sym, tf, name, cand_start)
    with _lock:
        rows = _load()
        for r in rows:
            if r.get("key") == key:
                return None
        t1, t2, t6 = _levels(entry, sl)
        age = (_now().replace(tzinfo=None) - cand_end).total_seconds() / 60.0
        row = {"key": key, "date": cand_start[:10], "candle": cand_start[11:16], "tf": tf, "section": section, "symbol": sym, "signal": name,
               "entry": round(entry, 2), "sl": round(sl, 2), "risk_pct": round((entry - sl) / entry * 100.0, 2), "t1": round(t1, 2), "t2": round(t2, 2), "t6": round(t6, 2),
               "spurt_x": round(spurt_x, 1) if spurt_x else None, "crossed": crossed, "reached": 0, "exit": "OPEN", "final_r": None, "last": None,
               "alert": bool(alert_ok and age <= ALERT_MAX_AGE_MIN.get(tf, 60)), "found": _now().strftime("%Y-%m-%d %H:%M"), "upd": "", "cs": cand_start}
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


def _universe():
    """[(symbol, security_id)] of the TOP_N most liquid stocks. Rebuilt once a day."""
    today = _now().strftime("%Y-%m-%d")
    try:
        with open(UNI_PATH) as f:
            d = json.load(f)
        if d.get("date") == today and d.get("top_n") == TOP_N and d.get("list"):
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
    for k, (sym, sid) in enumerate(uni):
        if k % 300 == 0:
            _set(msg="building the liquid list: daily candles %d / %d" % (k, len(uni)))
        try:
            r = _read_turnover(sym, ls)
        except Exception:
            r = None
        if r and r[1] >= MIN_PRICE:
            ranked.append((r[0], sym, sid))
    src = "daily candles"
    if len(ranked) < min(150, TOP_N):                      # no / few saved daily candles yet: today's traded value from the quote call
        ranked, src = [], "today's quotes"
        _set(msg="building the liquid list: reading today's quotes (no saved daily candles found)")
        by_id = {str(sid): sym for sym, sid in uni}
        ids = [int(s) for s in by_id]
        for i in range(0, len(ids), 1000):
            resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": ids[i:i + 1000]}, "swing quote batch")
            for sid, q in ((resp.json().get("data") or {}).get("NSE_EQ") or {}).items():
                try:
                    px, vol = float(q.get("last_price")), float(q.get("volume"))
                except (TypeError, ValueError):
                    continue
                if str(sid) in by_id and px >= MIN_PRICE:
                    ranked.append((px * vol, by_id[str(sid)], str(sid)))
            time.sleep(1.1)
    ranked.sort(reverse=True)
    out = [(s, sid) for _t, s, sid in ranked[:TOP_N]]
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(UNI_PATH, "w") as f:
            json.dump({"date": today, "top_n": TOP_N, "src": src, "list": out}, f)
    except Exception:
        pass
    log.info("swing: liquid list of %d stocks built from %s", len(out), src)
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


def _sma(c, n):
    return pd.Series(c).rolling(n, min_periods=n).mean().to_numpy()


# ------------------------------------------------------------------------------------------------------- the signals
def spurt_sma_signals(o, h, l, c, v, win):
    """Section A. Returns [(i, volume multiple, 'x,y' SMAs crossed by the candle)] for candles i inside the window."""
    n = len(c)
    s = {k: _sma(c, k) for k in SMAS}
    out = []
    for i in range(max(VOL_AVG, SMAS[-1]), n):
        if not win[i] or c[i] <= o[i]:
            continue
        a = v[i - VOL_AVG:i].mean()
        if a <= 0 or v[i] / a < VOL_MULT:
            continue
        if any(np.isnan(s[k][i]) or c[i] <= s[k][i] for k in SMAS):
            continue
        crossed = [k for k in SMAS if o[i] <= s[k][i] or (not np.isnan(s[k][i - 1]) and c[i - 1] <= s[k][i - 1])]
        if len(crossed) < CROSS_MIN:
            continue
        out.append((i, float(v[i] / a), ",".join(str(k) for k in crossed) or "-"))
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


def scan_frame(sym, d, tf, alert_ok, now_ist):
    """All BUY signals of one stock on one timeframe. d = closed candles. Returns the new rows."""
    import signals_scan as ss
    import spurt_buy
    new = []
    n = len(d)
    if n < MIN_BARS:
        return new
    o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
    ist = [t + pd.Timedelta(minutes=330) for t in d.index]
    win = np.zeros(n, dtype=bool)
    win[n - WIN_BARS[tf]:] = True
    last_day = ist[-1].strftime("%Y-%m-%d")
    if (now_ist.replace(tzinfo=None) - ist[-1].to_pydatetime().replace(tzinfo=None)).days > 5:
        return new                                                           # stale data (halted stock / old candles)

    def st(i):
        return ist[i].strftime("%Y-%m-%d %H:%M")

    def en(i):
        return (ist[i] + pd.Timedelta(minutes=tf)).to_pydatetime().replace(tzinfo=None)

    for i, mult, crossed in spurt_sma_signals(o, h, l, c, v, win):                                  # ---- section A
        r = _add(tf, "A", sym, "VOLUME SPURT + SMA", st(i), en(i), float(c[i]), float(l[i]), mult, crossed, alert_ok)
        if r:
            new.append(r)
    mult_all = np.zeros(n)
    for i in range(VOL_AVG, n):
        a = v[i - VOL_AVG:i].mean()
        mult_all[i] = v[i] / a if a > 0 else 0.0
    mx = float(mult_all[win].max())
    if mx < min(getattr(ss, "SA_SWEEP_MULT", 3.0), VOL_MULT):
        return new
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
            log.warning("swing: VS BUY failed for %s: %s", sym, e)
        try:
            rsi = ss._rsi(c)
            for i, side, sl in ss._spurt_d(o, h, l, c, v, win, rsi):
                if side == "BUY" and ok_buy[i]:
                    found.append((i, "SPURT BUY", float(c[i]), float(sl)))
        except Exception as e:
            log.warning("swing: SPURT BUY failed for %s: %s", sym, e)
    for i, nm, entry, sl in found:
        r = _add(tf, "B", sym, nm, st(i), en(i), entry, sl, float(mult_all[i]) if mult_all[i] else None, "", alert_ok)
        if r:
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
        blocks.append("SWING TRADING ALERT\n%s - %s (%dm)\nEntry %s | SL %s (%.1f%%)\nT1 %s | T2 %s | T6 %s\nCandle %s IST%s" % (
            r["signal"], r["symbol"], r["tf"], _f(r["entry"]), _f(r["sl"]), r["risk_pct"], _f(r["t1"]), _f(r["t2"]), _f(r["t6"]), r["candle"],
            (" | spurt %sx" % r["spurt_x"]) if r.get("spurt_x") else ""))
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


# ------------------------------------------------------------------------------------------------------- one scan
def run_scan(tfs=TFS, alerts=True, label=""):
    """Scans the liquid list on the given timeframes. One stock at a time. Returns False if a scan is already running."""
    if not _run_lock.acquire(blocking=False):
        return False
    try:
        import live_scanner as ls
        _set(on=True, msg="starting: building the liquid list", done=0, total=0)
        uni = _universe()
        _set(total=len(uni), universe=len(uni), msg="scanning 0 / %d" % len(uni))
        now_ist = _now()
        fresh = []
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        for k, (sym, sid) in enumerate(uni):
            for tf in tfs:
                try:
                    d = _candles(sid, tf, as_of, ls)
                    if d is None or len(d) < MIN_BARS:
                        continue
                    fresh += scan_frame(sym, d, tf, alerts, now_ist)
                    _follow(sym, tf, d, [(t + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M") for t in d.index])
                except Exception as e:
                    log.warning("swing: %s %dm failed: %s", sym, tf, e)
            _set(done=k + 1, msg="scanning %d / %d" % (k + 1, len(uni)))
        with _lock:
            _save()
        alert_items = [r for r in fresh if r.get("alert")]
        if alert_items:
            _notify(alert_items)
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
    log.info("Swing signals scan started: Mon-Fri after every 30-minute candle close (09:46 ... 15:31), top %d liquid stocks, 30m + 60m, BUY only.", TOP_N)
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


# ------------------------------------------------------------------------------------------------------- CSV / page
def _csv_row(r):
    vals = [r.get("date"), r.get("candle"), "%dm" % r.get("tf", 0), "A volume spurt + SMA" if r.get("section") == "A" else "B indicator", r.get("symbol"), r.get("signal"), r.get("entry"),
            r.get("sl"), r.get("risk_pct"), r.get("t1"), r.get("t2"), r.get("t6"), r.get("spurt_x"), r.get("crossed"), r.get("reached"), r.get("exit"), r.get("final_r"),
            r.get("last"), "yes" if r.get("alert") else "", r.get("found"), r.get("upd")]
    return ",".join("" if x is None else str(x).replace(",", " ") for x in vals)


def csv_text(day=None):
    with _lock:
        rows = [r for r in _load() if (not day or r["date"] == day)]
    rows.sort(key=lambda r: (r["date"], r["candle"]))
    return "\n".join([",".join(CSV_COLS)] + [_csv_row(r) for r in rows]) + "\n"


def _result(r):
    ex = r.get("exit")
    if ex == "OPEN":
        return "open (best target %s, now %s R)" % ("T%d" % r["reached"] if r.get("reached") else "none", ("%+.2f" % r["final_r"]) if r.get("final_r") is not None else "-")
    if ex == "SL":
        return "stop-loss hit (-1R)"
    return "closed at %s (%+.2f R)" % (ex, r["final_r"] if r.get("final_r") is not None else 0.0)


def _table(rows, empty):
    if not rows:
        return "<p style='color:#9fb0c0'>%s</p>" % empty
    body = []
    for r in rows:
        c = COLOURS.get(r["signal"], "#9fb0c0")
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=%d" % (html.escape(r["symbol"]), r["tf"])
        body.append("<tr><td>%s %s</td><td>%dm</td><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td style='color:%s;font-weight:700'>%s</td><td>%s</td>"
                    "<td style='color:#ff7070'>%s <span style='color:#9fb0c0'>(%.1f%%)</span></td><td style='color:#7be08a'>%s</td><td style='color:#7be08a'>%s</td><td>%s</td><td>%s</td>"
                    "<td style='color:#9fb0c0'>%s</td></tr>" % (
                        r["date"][5:], r["candle"], r["tf"], tv, html.escape(r["symbol"]), c, html.escape(r["signal"]), _f(r["entry"]), _f(r["sl"]), r["risk_pct"], _f(r["t1"]), _f(r["t2"]),
                        ("%sx" % r["spurt_x"]) if r.get("spurt_x") else "-", html.escape(r.get("crossed") or "-"), html.escape(_result(r))))
    return ("<table><tr><th>Candle</th><th>TF</th><th>Stock</th><th>Signal</th><th>Entry</th><th>SL (risk)</th><th>T1</th><th>T2</th><th>Spurt</th><th>SMAs crossed</th><th>What happened</th></tr>"
            + "".join(body) + "</table>")


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
            "<p style='color:#9fb0c0'>BUY only (delivery), %d most liquid stocks. A scan runs after every 30-minute candle closes (09:46 ... 15:31); the 60-minute candles are added at the hour candles. "
            "Every signal is followed for %d days with the same stop cascade as the signals page (SL -1R, T1 -> SL to entry ...). This page places NO order. "
            "<a href='/spurt10_all_in_one_pro_signals' target='_blank' rel='noopener'>5 / 15-minute signals</a> &nbsp;|&nbsp; <a href='/' target='_blank' rel='noopener'>Home</a></p>"
            "<p>%s &nbsp; <a href='/swing_signals/run' style='border:1px solid #8ab4f8;border-radius:6px;padding:4px 12px'>Scan now (no alert)</a> &nbsp; <span style='color:#9fb0c0'>%s &middot; stocks in the list: %s</span></p>"
            "<p style='color:#9fb0c0'>Download CSV: <a href='/swing_signals.csv' target='_blank' rel='noopener'><b>all days</b></a> &middot; %s</p>"
            "<h3>A. Volume spurt (10x or more) + candle closed above the 9, 21 and 60 SMA</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>Green candle, volume 10x the average of the 50 candles before it, close above all three SMAs, and the candle itself crossed up at least one of them. "
            "Entry = close, SL = low of the candle.</p>%s%s"
            "<h3>B. Indicator BUY signals (SWEEP, OB MITIGATION, VS BUY, SPURT BUY)</h3>"
            "<p style='color:#9fb0c0;margin:0 0 6px'>The ALL IN ONE PRO BUY rules on 30m / 60m candles, with the SMA 9/21/60 distance rule.</p>%s%s</body></html>"
            ) % (TOP_N, FOLLOW_DAYS, tabs, status, sd.get("universe") or "-", dl or "-",
                 _table(a, "No signal yet."), _stats(a), _table(b, "No signal yet."), _stats(b))


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
