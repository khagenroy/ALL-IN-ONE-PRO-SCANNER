"""
DAILY-ZONE REJECTION, ALL STOCKS  (page: /daily_rej   csv: /daily_rej.csv)       ADDED 7 Nov

A closed 15-minute candle that is REJECTED at the UPPER part of the DAILY resistance zone, for EVERY liquid stock (volume spurt or not).
(The older table on /spurt_daily does the same only for stocks with 2x volume or more.)

  Daily resistance zone = nearest daily order-block resistance above the last close (prepared once a day by spurt_daily._prep_today).
  Upper part            = from REJ_UPPER_FROM (0.5 = the top half) of the zone height upwards.
  Rejected              = the closed 15-min candle's high reached the upper part, its upper wick is WICK_PCT (60) % or more of the candle's
                          high-low range, and it closed at / below the zone top.
  Liquid                = same rule as the other pages (price, Rs 1 crore traded, spread, depth).

It adds NO quote call: it is called by spurt_daily's 30-second watch with the quotes already read. It reads one 15-minute candle per candidate
stock per 15-minute slot, and only for stocks whose day high reached the upper part of their daily zone and whose price is still near it.
Read-only: no orders, nothing else is touched; any error is caught and logged. No env variables needed.
"""
import os
import json
import logging
from datetime import datetime

import pandas as pd

import spurt10_live as sp
import spurt_daily as sd

log = logging.getLogger("daily_rej_all")
IST = sp.IST

ON = True
TF = 15                       # candle minutes
WICK_PCT = 60.0               # upper wick >= this % of the candle's high-low range
UPPER_FROM = 0.5              # upper part of the daily zone starts this far up the zone (0.5 = top half, 0.75 = top quarter)
NEAR_ZONE_PCT = 2.0           # only stocks whose price is within this % below the zone bottom (or higher) are checked: the rejection must have just happened
MAX_CHECK = 150               # most candle downloads per 15-minute slot
FROM_MIN = 9 * 60 + 30        # first closed 15-min candle
TO_MIN = 15 * 60 + 25
NTFY_ALERT = True             # phone alert for each new rejection (set False to switch it off)

PATH = os.path.join(sd.RESULTS_DIR, "daily_rej.csv")
STATE = os.path.join(sd.RESULTS_DIR, "daily_rej_state.json")
COLS = ["date", "candle", "alert_time", "symbol", "price", "resistance", "wick_pct", "daily_vol_x", "day_chg_pct",
        "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "result",
        "nifty_at_alert", "nifty_last", "rel_last"]
_st = {"date": "", "checked": {}, "rows": []}


def _load(today_s):
    try:
        with open(STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _st.update(date=today_s, checked=dict(s_.get("checked") or {}), rows=list(s_.get("rows") or []))
            return
    except Exception:
        pass
    _st.update(date=today_s, checked={}, rows=[])


def _write(today_s):
    try:
        os.makedirs(sd.RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_st["rows"], columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"daily_rej csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump({"date": today_s, "checked": _st["checked"], "rows": _st["rows"]}, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"daily_rej write failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  Rs {r['price']:.2f}  daily resistance {r['resistance']}  {TF}m upper wick {r['wick_pct']:.0f}%" for r in rows[:12]]
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"Rejected at UPPER part of DAILY resistance ({len(rows)})", "Priority": "high", "Tags": "chart_with_downwards_trend"})
    except Exception as e:
        log.warning(f"daily_rej ntfy failed: {e}")


def cycle(now, prep, quotes, nifty, uni):
    """Called every ~30 s by spurt_daily._touch_cycle with the prepared stocks and their quotes. Never raises."""
    if not ON:
        return
    try:
        mins = now.hour * 60 + now.minute
        today_s = now.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        slot = mins // TF * TF
        slot_key = f"{slot // 60:02d}:{slot % 60:02d}"
        new = []
        if FROM_MIN <= mins <= TO_MIN:
            near = NEAR_ZONE_PCT / 100.0
            cands = []
            for sym, q in quotes.items():
                try:
                    p = prep.get(sym)
                    if not p or sym not in uni or _st["checked"].get(sym) == slot_key:
                        continue
                    if q["px"] < sp.MIN_PRICE or q["px"] * q["vol"] < sd.MIN_TURNOVER:
                        continue
                    if q["spread"] is not None and q["spread"] > sp.LIQ_SPREAD_THIN:
                        continue
                    if sd._low_depth(q):
                        continue
                    zl = []                               # every live daily resistance zone whose upper part the day's high reached and which price is still near
                    for zr in (p.get("res_all") or [p["resistance"]]):
                        zz = sd._zone_nums(zr)
                        if zz and max(q["high"], q["px"]) >= zz[0] + UPPER_FROM * (zz[1] - zz[0]) and q["px"] >= zz[0] * (1 - near):
                            zl.append(zr)
                    if zl:
                        cands.append((min(abs(q["px"] / sd._zone_nums(z_)[1] - 1) for z_ in zl), sym, zl))
                except Exception:
                    continue
            cands.sort()
            todo = [(s_, zl) for _d, s_, zl in cands[:MAX_CHECK]]
            if todo:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=max(1, min(int(getattr(sp, "WORKERS", 4)), 4))) as ex:
                    res = list(ex.map(lambda t_: (t_[0], t_[1], sd._rej_candle(t_[0], uni[t_[0]])), todo))
                for sym, zl, c in res:
                    _st["checked"][sym] = slot_key
                    if not c:
                        continue
                    t, o, h, l, cl = c
                    rng = h - l
                    if rng <= 0:
                        continue
                    wick = (h - max(o, cl)) / rng * 100.0
                    if wick < WICK_PCT or any(r_["symbol"] == sym and r_["candle"] == t for r_ in _st["rows"]):
                        continue
                    hit = None
                    for zr in zl:                          # first zone the candle was rejected at
                        lo, zhi = sd._zone_nums(zr)
                        if h >= lo + UPPER_FROM * (zhi - lo) and cl <= zhi:
                            hit = zr
                            break
                    if not hit:
                        continue
                    p, q = prep[sym], quotes[sym]
                    row = {c_: "" for c_ in COLS}
                    row.update({"date": today_s, "candle": t, "alert_time": now.strftime("%H:%M"), "symbol": sym, "price": round(q["px"], 2),
                                "resistance": hit, "wick_pct": round(wick, 1), "daily_vol_x": round(q["vol"] / p["avg"], 2),
                                "day_chg_pct": round((q["px"] / p["close"] - 1) * 100, 2), "result": "open",
                                "nifty_at_alert": (round(nifty, 2) if nifty else "")})
                    _st["rows"].append(row)
                    new.append(row)
        # follow-up of today's rows (a FALL after the alert counts as +)
        for r_ in _st["rows"]:
            q = quotes.get(r_["symbol"])
            if not q or not r_.get("price"):
                continue
            try:
                mv = round((1.0 - q["px"] / float(r_["price"])) * 100.0, 2)
                el = mins - (int(r_["alert_time"][:2]) * 60 + int(r_["alert_time"][3:5]))
                r_["best_pct"] = mv if r_["best_pct"] in ("", None) else max(float(r_["best_pct"]), mv)
                r_["worst_pct"] = mv if r_["worst_pct"] in ("", None) else min(float(r_["worst_pct"]), mv)
                r_["last_pct"], r_["last_time"] = mv, now.strftime("%H:%M")
                nm = round((nifty / float(r_["nifty_at_alert"]) - 1.0) * 100.0, 2) if (nifty and r_.get("nifty_at_alert") not in ("", None)) else None
                if nm is not None:
                    r_["nifty_last"], r_["rel_last"] = nm, round(-mv - nm, 2)
                for lab, mm in (("ret_10m", 10), ("ret_30m", 30), ("ret_60m", 60)):
                    if r_[lab] in ("", None) and el >= mm:
                        r_[lab] = mv
                if r_["ret_60m"] not in ("", None):
                    r_["result"] = "WORKED" if mv >= 0.3 else "FAILED" if mv <= -0.3 else "FLAT"
            except Exception:
                continue
        if new or _st["rows"]:
            _write(today_s)
        if new:
            _notify(new)
            log.info(f"daily_rej: {[r['symbol'] for r in new]} rejected at the upper part of the daily resistance ({TF}m candle)")
    except Exception as e:
        log.warning(f"daily_rej step failed: {e}")


def page_html():
    """The /daily_rej page: today's rejections, newest first. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>Daily Resistance Rejection</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}td:nth-child(3){text-align:left}'
            '.m{color:#555;font-size:13px;margin:6px 0 12px}th{position:sticky;top:0;z-index:5}</style></head><body>')
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        rows = []
        if os.path.exists(PATH):
            df = pd.read_csv(PATH)
            df = df[df["date"].astype(str) == today_s].fillna("")
            rows = df.to_dict("records")

        def pc(v):
            if v in ("", None):
                return "-"
            v = float(v)
            return f'<span style="color:{"#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"}">{v:+.2f}%</span>'

        def rs(v):
            c = {"WORKED": "#0a7d33", "FAILED": "#c62828"}.get(v, "#555")
            return f'<b style="color:{c}">{v}</b>'
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{r['resistance']}</td>"
            f"<td><b>{float(r['wick_pct']):.0f}%</b></td><td>{float(r['daily_vol_x']):.2f}x</td><td>{pc(r['day_chg_pct'])}</td>"
            f"<td>{pc(r['ret_10m'])}</td><td>{pc(r['ret_30m'])}</td><td>{pc(r['ret_60m'])}</td><td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td>"
            f"<td>{pc(r['last_pct'])}</td><td>{rs(r['result'])}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="15" style="text-align:center">No rejection at the upper part of a daily resistance zone yet today.</td></tr>'
        return (head + f'<h2>Rejected at the UPPER part of the DAILY resistance &mdash; {TF}-min candle, upper wick &ge; {WICK_PCT:g}% &mdash; all stocks ({len(rows)})</h2>'
                f'<div class="m">Every liquid stock (volume spurt or not) whose closed {TF}-min candle pushed into the upper part (from {UPPER_FROM * 100:.0f}% of the zone height) of its '
                f'daily resistance zone, has an upper wick of {WICK_PCT:g}% or more of the candle range and closed at / below the zone top. Checked each {TF} min, 09:30-15:25. '
                f'"Volume x" = volume so far today / 50-day average. Moves after the alert: a FALL counts as + (the rejection worked). Information only. '
                f'<a href="/daily_rej.csv" target="_blank" rel="noopener">CSV</a> &middot; <a href="/spurt_daily" target="_blank" rel="noopener">Daily spurt page</a></div>'
                '<div class="w"><table class="sp"><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Price</th><th>Daily resistance</th><th>Upper wick</th><th>Volume x</th><th>Day %</th>'
                '<th>10 min</th><th>30 min</th><th>60 min</th><th>Best</th><th>Worst</th><th>Last</th><th>Result</th></tr>' + body + '</table></div></body></html>')
    except Exception as e:
        log.warning(f"daily_rej page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
