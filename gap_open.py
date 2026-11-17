"""
GAP UP / GAP DOWN AT THE OPEN, WITH OR WITHOUT A SPURT  (tab "Gap at open" on /spurt10 -> /spurt10_gapopen    csv: /gap_open.csv)          ADDED 16 Nov

The first thing the scanner tells you in the morning. Every liquid NSE stock (the same universe as the spurt scanner: no ETFs, price at least SPURT10_MIN_PRICE) whose
OPEN is more than GAP_PCT (4%) away from the previous day's close is listed - a gap up or a gap down, spurt or not.

  * It reads ONE quote per stock (marketfeed/quote, 3 batched calls for the whole market) at about 09:16:10 and again every minute up to 09:19, so the phone
    alert comes BEFORE 09:20. No order is ever placed, nothing goes to the bridge.
  * Spurt x : day volume so far divided by the stock's average 5-minute candle volume (the previous 50 candles). While the first candle is still forming this is
    the "spurt so far"; at about 09:21 it is replaced by the real first-candle (09:15) volume x.
  * THIN stocks (little waiting on the bid or ask side, or near a circuit) are listed on the page tagged THIN but are left out of the phone alert, like everywhere else.
  * At 15:20 the day's last price / high / low are read once, so every gap gets its follow-up: move from the open to the close in the gap direction and whether the gap was filled.
  * GAP RETRACEMENT WATCH (ADDED 16 Nov; gap DOWN = SELL the bounce, gap UP = BUY the pullback, the same rule mirrored): every non-thin gap stock is followed on its RT_TF-minute (15) candles from 09:30 to 14:30 (one check per candle close).
        leg    = previous close (top) down to the lowest low of the day so far (a new low restarts the watch from that low)
        bounce = the price must retrace UP at least RT_FIB (38.2%) of that leg, but a close at / above the previous close = gap filled = FAILED
        SELL trigger = after the bounce, the first RED candle that closes below the previous candle's low. Entry = its close, stop = the bounce high.
        status: WATCHING (no bounce yet) / BOUNCING (bounced into the zone, waiting for the red candle) / TRIGGER / FILLED / EXPIRED (no trigger by RT_END)
        Gap UP is the mirror: pullback down 38.2% of the leg (previous close -> day high), first GREEN 15-minute candle closing above the previous candle's high = BUY, stop = the pullback low.
        Phone alert on the trigger. Alert only: nothing goes to the bridge.
  * Information only. Errors are caught and logged.
"""
import os
import csv
import time
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

import spurt10_live as sp

log = logging.getLogger("gap_open")
IST = sp.IST

ON = True
GAP_PCT = 4.0                  # the open must be MORE than this % away from the previous close
NTFY_ALERT = True              # phone alert (before 09:20)
FIRST_SCAN = 9 * 60 + 16       # minutes of the day: the scans run from 09:16 ...
LAST_SCAN = 9 * 60 + 19        # ... to 09:19 (one scan a minute); the alert is therefore always before 09:20
CANDLE_PASS = 9 * 60 + 21      # the real first-candle volume x
CLOSE_PASS = 15 * 60 + 20      # day high / low / last price for the follow-up
RT_ON = True                   # gap DOWN retracement watch (SELL setups)
RT_FIB = 0.382                 # the bounce must reach this part of the gap-down leg (previous close -> day low)
RT_TF = 15                     # candle size (minutes) of the retracement watch: 15 (Khagen trades the 15-minute chart). 5 = 5-minute candles
RT_TFS = (5, 15)               # the retracement watch runs on BOTH timeframes: 5-minute (first check 09:20) and 15-minute (first check 09:30)
RT_START = 9 * 60 + 15 + 5 + 0        # first check: the first 5-minute candle has closed (09:20)
RT_END = 14 * 60 + 30          # last check
BASE_MAX = 80                  # at most this many gap stocks get their volume average fetched per scan (largest gaps first)

PATH = os.path.join(sp.RESULTS_DIR, "gap_open.csv")
COLS = ["date", "symbol", "side", "gap_pct", "open", "prev_close", "first_seen", "last", "spurt_x", "candle_x", "liq", "close_px", "move_pct", "filled",
        "rt_status", "rt_bounce", "rt_time", "rt_entry", "rt_stop", "rt_risk",
        "rt5_status", "rt5_bounce", "rt5_time", "rt5_entry", "rt5_stop", "rt5_risk"]   # rt_* = the 15-minute watch, rt5_* = the 5-minute watch
_st = {"date": "", "rows": {}, "last": 0.0, "candle_done": False, "close_done": False, "none_sent": False, "alerted": set(), "rt_slot": -1}
_lock = threading.Lock()


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _write(today_s):
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        rows = []
        for r in sorted(_st["rows"].values(), key=lambda z: -abs(z["gap_pct"])):
            rows.append({k: r.get(k, "") for k in COLS})
        df = pd.DataFrame(rows, columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"gap_open csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
    except Exception as e:
        log.warning(f"gap_open write failed: {e}")


def _load(today_s):
    """New day -> empty; after a restart in the day -> the rows already saved for today come back."""
    _st.update(date=today_s, rows={}, last=0.0, candle_done=False, close_done=False, none_sent=False, alerted=set(), rt_slot=-1)
    try:
        if os.path.exists(PATH):
            for r in csv.DictReader(open(PATH, newline="", encoding="utf-8")):
                if r.get("date") == today_s and r.get("symbol"):
                    for k in ("gap_pct", "open", "prev_close", "last", "spurt_x", "candle_x", "close_px", "move_pct", "rt_bounce", "rt_entry", "rt_stop", "rt_risk", "rt5_bounce", "rt5_entry", "rt5_stop", "rt5_risk"):
                        r[k] = _f(r.get(k), None) if r.get(k) not in ("", None) else None
                    r["pc"] = r.get("prev_close")
                    _st["rows"][r["symbol"]] = r
                    _st["alerted"].add(r["symbol"])
                    if r.get("rt_status") == "TRIGGER":
                        r["_rt_sent"] = True
                    if r.get("rt5_status") == "TRIGGER":
                        r["_rt5_sent"] = True
    except Exception as e:
        log.warning(f"gap_open reload failed: {e}")


def _quotes(uni):
    """{symbol: dict} for every stock with an open and a previous close, from marketfeed/quote (1000 ids per call)."""
    import live_scanner as ls
    by_id = {int(s): sym for sym, s in uni}
    ids = list(by_id)
    out = {}
    for i in range(0, len(ids), 1000):
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": ids[i:i + 1000]}, f"gap open quote {i // 1000 + 1}")
        data = (resp.json().get("data") or {}).get("NSE_EQ") or {}
        for sid, q in data.items():
            sym = by_id.get(int(sid))
            if not sym or not isinstance(q, dict):
                continue
            try:
                px = float(q.get("last_price"))
                oh = q.get("ohlc") or {}
                op = _f(oh.get("open"))
                nc = q.get("net_change")
                pc = px - float(nc) if nc is not None else _f(oh.get("close"))
                if px <= 0 or op <= 0 or pc <= 0:
                    continue
                dp = sp._parse_depth(q)
                thin = bool(sp._circuit_side(q, px)) or bool(dp and sp.LIQ_MIN_DEPTH_QTY > 0 and len(dp) > 4 and (dp[3] < sp.LIQ_MIN_DEPTH_QTY or dp[4] < sp.LIQ_MIN_DEPTH_QTY))
                out[sym] = {"px": px, "op": op, "pc": pc, "hi": _f(oh.get("high")), "lo": _f(oh.get("low")), "vol": _f(q.get("volume")), "thin": thin}
            except Exception:
                continue
        time.sleep(1.1)
    return out


def _base_and_first(sym, sid, today_date):
    """(average volume of the previous 50 five-minute candles, volume of today's 09:15 candle or None). Never raises."""
    try:
        import live_scanner as ls
        as_of = pd.Timestamp(datetime.utcnow())
        df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, sp.HISTORY_DAYS)
        df = sp._spurt_bars(df5, as_of)
        if df is None or len(df) < 10:
            return None, None
        day = df.index.map(lambda t: sp._ist(t).date())
        prev = df[day != today_date]
        base = float(prev["volume"].to_numpy(dtype=float)[-50:].mean()) if len(prev) >= 10 else None
        tod = df[day == today_date]
        first = float(tod["volume"].iloc[0]) if len(tod) else None
        return base, first
    except Exception:
        return None, None


def _notify(rows, title_extra=""):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        rows = sorted(rows, key=lambda r: -abs(r["gap_pct"]))
        lines = []
        for r in rows[:15]:
            sx = f"spurt {r['spurt_x']:.0f}x" if r.get("spurt_x") is not None else "no volume data"
            lines.append(f"{r['symbol']}  {'GAP UP' if r['gap_pct'] > 0 else 'GAP DOWN'} {r['gap_pct']:+.1f}%  open {r['open']:.2f} (prev {r['prev_close']:.2f})  {sx}")
        more = f"\n+ {len(rows) - 15} more on the Gap at open tab" if len(rows) > 15 else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + more).encode("utf-8"), timeout=8,
                      headers={"Title": f"Gap at open > {GAP_PCT:g}%: {len(rows)} stocks{title_extra}", "Priority": "high", "Tags": "rotating_light"})
    except Exception as e:
        log.warning(f"gap_open ntfy failed: {e}")


def _scan(now_ist, last_pass=False):
    today_s = now_ist.strftime("%Y-%m-%d")
    uni = sp._load_universe()
    q = _quotes(uni)
    sid_of = dict(uni)
    hits = {s: v for s, v in q.items() if v["px"] >= sp.MIN_PRICE and abs((v["op"] / v["pc"] - 1.0) * 100.0) > GAP_PCT}
    order = sorted(hits, key=lambda s: -abs(hits[s]["op"] / hits[s]["pc"] - 1.0))
    need = [s for s in order if s not in _st["rows"] or _st["rows"][s].get("_base") is None][:BASE_MAX]
    bases = {}
    if need:
        with ThreadPoolExecutor(max_workers=max(1, sp.WORKERS)) as ex:
            for s, b in zip(need, ex.map(lambda z: _base_and_first(z, sid_of[z], now_ist.date()), need)):
                bases[s] = b[0]
    fresh = []
    with _lock:
        for s in order:
            v = hits[s]
            gp = round((v["op"] / v["pc"] - 1.0) * 100.0, 2)
            r = _st["rows"].get(s)
            if r is None:
                r = {"date": today_s, "symbol": s, "side": "GAP UP" if gp > 0 else "GAP DOWN", "gap_pct": gp, "open": round(v["op"], 2), "prev_close": round(v["pc"], 2),
                     "pc": v["pc"], "first_seen": now_ist.strftime("%H:%M:%S"), "spurt_x": None, "candle_x": None, "liq": "THIN" if v["thin"] else "", "close_px": None,
                     "move_pct": None, "filled": "", "_base": None}
                _st["rows"][s] = r
            if s in bases and bases[s]:
                r["_base"] = bases[s]
            r["last"] = round(v["px"], 2)
            r["liq"] = "THIN" if v["thin"] else ""
            if r.get("_base"):
                r["spurt_x"] = round(v["vol"] / r["_base"], 1)
            if s not in _st["alerted"] and not v["thin"]:
                _st["alerted"].add(s)
                fresh.append(r)
        _write(today_s)
    if fresh:
        _notify(fresh)
        log.info(f"gap_open: new gap stocks: {[(a['symbol'], a['gap_pct']) for a in fresh]}")
    elif last_pass and not _st["rows"] and not _st["none_sent"]:
        _st["none_sent"] = True
        topic = os.environ.get("NTFY_TOPIC", "").strip()
        if NTFY_ALERT and sp.NTFY_ON and topic:
            try:
                import requests
                requests.post(f"https://ntfy.sh/{topic}", data=f"No stock opened more than {GAP_PCT:g}% away from the previous close today.".encode("utf-8"), timeout=8,
                              headers={"Title": "Gap at open: none today", "Priority": "default", "Tags": "white_check_mark"})
            except Exception as e:
                log.warning(f"gap_open ntfy failed: {e}")


def _candle_pass(now_ist):
    """About 09:21: the real volume x of the 09:15 candle for every listed gap stock (no alert)."""
    uni = dict(sp._load_universe())
    with _lock:
        todo = [s for s, r in _st["rows"].items() if s in uni]
    with ThreadPoolExecutor(max_workers=max(1, sp.WORKERS)) as ex:
        res = list(ex.map(lambda z: _base_and_first(z, uni[z], now_ist.date()), todo))
    with _lock:
        for s, (base, first) in zip(todo, res):
            r = _st["rows"][s]
            if base and first is not None:
                r["candle_x"] = round(first / base, 1)
        _write(now_ist.strftime("%Y-%m-%d"))


def _close_pass(now_ist):
    """15:20: last price, day high / low -> move from the open in the gap direction and whether the previous close was reached again."""
    q = _quotes(sp._load_universe())
    with _lock:
        for s, r in _st["rows"].items():
            v = q.get(s)
            if not v:
                continue
            up = r["gap_pct"] > 0
            r["close_px"] = round(v["px"], 2)
            r["move_pct"] = round(((v["px"] / r["open"] - 1.0) if up else (1.0 - v["px"] / r["open"])) * 100.0, 2)
            pcv = r.get("pc") or r.get("prev_close")
            if pcv and v["hi"] > 0 and v["lo"] > 0:
                r["filled"] = "YES" if ((v["lo"] <= pcv) if up else (v["hi"] >= pcv)) else "NO"
        _write(now_ist.strftime("%Y-%m-%d"))


def retrace_eval(o, h, l, c, pc, up=False):
    """Retracement of a gap on today's closed 5-minute candles (index 0 = 09:15). up=False: gap DOWN, bounce up, SELL trigger. up=True: gap UP, pullback down, BUY trigger
    (the same rule on the mirrored prices). Returns a dict: status, bounce (% of the leg), trig_j, entry, stop. Never raises."""
    if up:
        e = retrace_eval(-np.asarray(o, float), -np.asarray(l, float), -np.asarray(h, float), -np.asarray(c, float), -float(pc), up=False)
        if e.get("entry") is not None:
            e["entry"], e["stop"] = -e["entry"], -e["stop"]
        return e
    n = len(c)
    out = {"status": "WATCHING", "bounce": 0.0, "trig_j": None, "entry": None, "stop": None}
    try:
        low_run, low_j = float(l[0]), 0
        bh, bj = None, None                                   # bounce high since the last low and the candle that first reached the zone
        for j in range(n):
            if c[j] >= pc:                                    # closed back at / above the previous close: the gap is filled
                out["status"] = "FILLED"
                return out
            if l[j] < low_run:                                # a new low: the leg is longer, the bounce starts again from here
                low_run, low_j, bh, bj = float(l[j]), j, None, None
            elif j > low_j:
                bh = float(h[j]) if bh is None else max(bh, float(h[j]))
            leg = pc - low_run
            if leg <= 0:
                continue
            if bh is not None:
                out["bounce"] = round((bh - low_run) / leg * 100.0, 1)
                if bj is None and bh >= low_run + RT_FIB * leg:
                    bj = j
                if bj is not None and j > bj and c[j] < o[j] and c[j] < l[j - 1]:
                    out.update(status="TRIGGER", trig_j=j, entry=float(c[j]), stop=float(bh))
                    return out
        out["status"] = "BOUNCING" if bj is not None else "WATCHING"
    except Exception:
        pass
    return out


def _pfx(tf):
    return "rt" if tf == 15 else "rt5"


def _retrace_pass(now_ist, tfs=(15,)):
    """One check of every gap stock (not thin) on its closed 5-minute and / or 15-minute candles (tfs). Phone alert on a new trigger (SELL after a gap down, BUY after a gap up)."""
    today_s = now_ist.strftime("%Y-%m-%d")
    uni = dict(sp._load_universe())
    with _lock:
        todo = [s for s, r in _st["rows"].items() if r.get("liq") != "THIN" and s in uni
                and any(r.get(_pfx(t) + "_status") not in ("FILLED", "TRIGGER") for t in tfs)]
    todo = sorted(todo, key=lambda s: -abs(_st["rows"][s]["gap_pct"]))[:BASE_MAX]

    def one(sym):
        try:
            import live_scanner as ls
            as_of = pd.Timestamp(datetime.utcnow())
            df5 = ls.fetch_intraday_history(uni[sym], "NSE_EQ", 5, 3)
            out = {}
            for tf in tfs:
                df = ls.drop_forming_bars(df5, 5, as_of) if tf <= 5 else ls.drop_forming_bars(ls.build_merged_bars(df5, tf // 5), tf, as_of)
                if df is None or not len(df):
                    continue
                day = df.index.map(lambda t: sp._ist(t).date())
                d = df[day == now_ist.date()]
                if len(d) < 2:
                    continue
                t = [(x + pd.Timedelta(minutes=330)).strftime("%H:%M") for x in d.index]
                o_, h_, l_, c_ = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
                e = retrace_eval(o_, h_, l_, c_, _st["rows"][sym]["pc"], up=_st["rows"][sym]["gap_pct"] > 0)
                e["time"] = t[e["trig_j"]] if e["trig_j"] is not None else ""
                out[tf] = e
            return out
        except Exception:
            return None
    if not todo:
        return
    with ThreadPoolExecutor(max_workers=max(1, sp.WORKERS)) as ex:
        res = list(ex.map(one, todo))
    fresh = []
    with _lock:
        for s, out in zip(todo, res):
            if not out:
                continue
            r = _st["rows"][s]
            for tf, e in out.items():
                p = _pfx(tf)
                if r.get(p + "_status") == "TRIGGER":
                    continue                                  # a trigger stays as it was found
                r[p + "_status"], r[p + "_bounce"] = e["status"], e["bounce"]
                if e["status"] == "TRIGGER":
                    r[p + "_time"] = e["time"]
                    r[p + "_entry"], r[p + "_stop"] = round(e["entry"], 2), round(e["stop"], 2)
                    r[p + "_risk"] = round(abs(e["stop"] - e["entry"]) / e["entry"] * 100.0, 2)
                    if not r.get("_" + p + "_sent"):
                        r["_" + p + "_sent"] = True
                        fresh.append((r, tf))
        _write(today_s)
    if fresh:
        _notify_rt(fresh)
        log.info(f"gap_open: gap retrace triggers: {[(a['symbol'], tf, a[_pfx(tf) + '_entry']) for a, tf in fresh]}")


def _notify_rt(items):
    """items = [(row, tf)]; every line starts with its timeframe."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and items):
        return
    try:
        import requests
        lines = []
        for r, tf in sorted(items[:12], key=lambda z: z[1]):
            p = _pfx(tf)
            sx = f"  spurt {r['candle_x']:.0f}x" if r.get("candle_x") is not None else (f"  spurt {r['spurt_x']:.0f}x" if r.get("spurt_x") is not None else "")
            _e, _s = float(r[p + '_entry']), float(r[p + '_stop'])
            _d = 1.0 if r['gap_pct'] > 0 else -1.0
            lines.append(f"[{tf} min] {r['symbol']}  {'BUY' if r['gap_pct'] > 0 else 'SELL'} {_e:.2f}  SL {_s:.2f} ({float(r[p + '_risk']):.2f}%)  T1 {_e + _d * abs(_e - _s):.2f}  T2 {_e + _d * 2 * abs(_e - _s):.2f}  candle {r[p + '_time']}  "
                         f"gap {r['gap_pct']:+.1f}%, {'pulled back' if r['gap_pct'] > 0 else 'bounced'} {float(r[p + '_bounce']):.0f}% of the leg{sx}")
        tfs = sorted({tf for _r, tf in items})
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"Gap retrace trigger {'+'.join(str(t) for t in tfs)} min ({len(items)}): " + ", ".join(sorted({'BUY' if r['gap_pct'] > 0 else 'SELL' for r, _t in items})), "Priority": "high", "Tags": "arrows_counterclockwise"})
    except Exception as e:
        log.warning(f"gap_open retrace ntfy failed: {e}")


def loop():
    log.info(f"Gap-at-open scan started: Mon-Fri, one scan a minute 09:16-09:19 (open more than {GAP_PCT:g}% from the previous close, alert before 09:20).")
    while True:
        try:
            now = datetime.now(IST)
            if not ON or now.weekday() >= 5:
                time.sleep(300)
                continue
            today_s = now.strftime("%Y-%m-%d")
            if _st["date"] != today_s:
                _load(today_s)
            m = now.hour * 60 + now.minute
            if FIRST_SCAN <= m <= LAST_SCAN and time.time() - _st["last"] >= 50:
                _st["last"] = time.time()
                _scan(now, last_pass=(m == LAST_SCAN))
            elif CANDLE_PASS <= m < CANDLE_PASS + 4 and not _st["candle_done"] and _st["rows"]:
                _st["candle_done"] = True
                _candle_pass(now)
            elif RT_ON and RT_START <= m <= RT_END and now.minute % 5 == 0 and now.second >= 20 and _st["rt_slot"] != m // 5 and _st["rows"]:
                _st["rt_slot"] = m // 5
                _retrace_pass(now, (5, 15) if (m % 15 == 0 and m >= 9 * 60 + 30) else (5,))
            elif CLOSE_PASS <= m < CLOSE_PASS + 4 and not _st["close_done"] and _st["rows"]:
                _st["close_done"] = True
                _close_pass(now)
            time.sleep(10)
        except Exception as e:
            log.error(f"gap_open cycle failed: {e}")
            time.sleep(30)


def html():
    """Body of the tab /spurt10_gapopen. Never raises."""
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        rows = sorted(_st["rows"].values(), key=lambda z: -abs(z["gap_pct"]))
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'

        def xcell(v):
            return "-" if v in ("", None) else (f"<b>{float(v):.0f}x</b>" if float(v) >= 10 else f"{float(v):.1f}x")
        def rt(r, p="rt"):
            st = r.get(p + "_status") or "WATCHING"
            col = {"TRIGGER": "#c62828", "BOUNCING": "#b26a00", "WATCHING": "#555", "FILLED": "#888", "EXPIRED": "#888"}.get(st, "#555")
            bn = f" {float(r[p + '_bounce']):.0f}%" if r.get(p + "_bounce") not in ("", None) and st in ("BOUNCING", "TRIGGER", "WATCHING") else ""
            return f'<b style="color:{col}">{st}</b>{bn}'

        def tfc(r, k):
            v = [f"{t} min: {r.get(p + '_' + k)}" for t, p in ((5, "rt5"), (15, "rt")) if r.get(p + "_" + k) not in ("", None)]
            return "<br>".join(v) if v else "-"
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td><td><b style=\"color:{'#0a7d33' if r['gap_pct'] > 0 else '#c62828'}\">{r['side']}</b></td><td>{pc(r['gap_pct'])}</td>"
            f"<td>{float(r['open']):.2f}</td><td>{float(r['prev_close']):.2f}</td><td>{r.get('last') if r.get('last') not in ('', None) else '-'}</td>"
            f"<td>{xcell(r.get('spurt_x'))}</td><td>{xcell(r.get('candle_x'))}</td><td>{r.get('liq') or '-'}</td><td>{r['first_seen']}</td>"
            f"<td>{rt(r, 'rt5')}</td><td>{rt(r, 'rt')}</td><td>{tfc(r, 'time')}</td><td>{tfc(r, 'entry')}</td><td>{tfc(r, 'stop')}</td>"
            f"<td>{r.get('close_px') if r.get('close_px') not in ('', None) else '-'}</td><td>{pc(r.get('move_pct'))}</td><td>{r.get('filled') or '-'}</td></tr>"
            for r in rows) or '<tr><td colspan="19" style="text-align:center">No stock has opened more than %g%% away from the previous close yet today (first scan 09:16).</td></tr>' % GAP_PCT
        return (f'<h2>Gap at open: gap up / gap down more than {GAP_PCT:g}% &mdash; spurt or not ({len(rows)} stocks)</h2>'
                f'<div class="m">The first list of the day: every liquid stock whose OPEN is more than {GAP_PCT:g}% away from yesterday\'s close, read at 09:16 and every minute to 09:19 '
                f'(phone alert before 09:20). <b>Spurt x</b> = day volume so far / average 5-minute candle volume (while the first candle forms); <b>1st candle x</b> = the real 09:15 candle, filled in at about 09:21. '
                f'THIN = little waiting on the bid or ask side or near a circuit (not in the alert). Close / Move / Gap filled are filled at 15:20 (move from the open in the gap direction; filled = the previous close was reached again). '
                f'<b>Retrace</b>: GAP DOWN = after the price bounces back up at least {RT_FIB * 100:.1f}% of the leg (previous close to the day low), the first RED 5-minute (or 15-minute) candle closing below the previous candle\'s low is the SELL trigger (stop = the bounce high). '
                f'GAP UP = the mirror: a pullback of {RT_FIB * 100:.1f}% of the leg (previous close to the day high), then the first GREEN 5-minute (or 15-minute) candle closing above the previous candle\'s high is the BUY trigger (stop = the pullback low). '
                f'The % beside the status is how much of the leg has been retraced; FILLED = closed back at the previous close. Watched on BOTH timeframes, each with its own column: 5-minute candles (checked every 5 minutes from 09:20) and 15-minute candles (checked every 15 minutes from 09:30), until 14:30. The phone alert starts with [5 min] or [15 min]. '
                f'Information only. <a href="/gap_open.csv" target="_blank" rel="noopener">CSV</a></div>'
                '<div class="w"><table class="sp"><tr><th>Symbol</th><th>Gap</th><th>Gap %</th><th>Open</th><th>Prev close</th><th>Last</th><th>Spurt x</th><th>1st candle x</th><th>Liq</th>'
                '<th>Seen</th><th>Retrace 5 min</th><th>Retrace 15 min</th><th>Trigger (time)</th><th>Entry</th><th>Stop</th><th>Close</th><th>Move</th><th>Gap filled</th></tr>' + body + '</table></div>')
    except Exception as e:
        log.warning(f"gap_open html failed: {e}")
        return '<div class="m">Gap at open could not be shown right now.</div>'
