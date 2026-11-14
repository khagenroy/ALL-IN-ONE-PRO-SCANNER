"""
SPURT RETRACEMENT BUY  (page: /spurt_buy   csv: /spurt_buy.csv)                                      ADDED 13 Nov, rule changed 14 Nov

The same rule as the VS BUY (Section S) of the ALL IN ONE PRO indicator, run on the closed 5-minute candles:
  1. SPURT          a GREEN candle with volume SPURT_MULT x or more of the average of the 50 candles before it.
  2. LOWER LOW      after the spurt the price starts falling: the first candle whose low is below the previous candle's low. It keeps falling while each candle
                    makes a new lower low; the lowest low so far is the hold low.
  3. HOLD           a candle that does NOT break the hold low: the lower low is holding, for at least HOLD candles.
  4. HIGHER HIGH    a GREEN candle whose high is above the previous candle's high, that does not break the hold low, with volume higher than the previous candle
                    (NEED_VOL) = BUY at that close.
Cancelled when a candle CLOSES below the spurt candle's low or WAIT candles after the spurt are over. SL = the hold low. T1 / T2 = 1x / 2x the risk.
One signal per spurt. Only today's candles are used (a spurt of an earlier day never counts).

  * Uses the 5-minute candles the spurt scan has ALREADY downloaded: NO extra Dhan call, no orders. Any error is caught and logged.
  * After the signal the page follows the stock: T1 hit / SL hit (if both in one candle the SL is counted first) and the best / worst / last move.
  * Set NTFY_ALERT False to switch the phone alert off.
"""
import os
import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_buy")
IST = sp.IST

ON = True
SPURT_MULT = 20.0             # same size as the red spurt page (the indicator default is 10)
AVG_LEN = 50
WAIT = 20                     # candles after the spurt in which the lower low, the hold and the higher high must all happen
HOLD = 1                      # the lower low must hold (no new lower low) for at least this many candles before the higher high
LL_MIN = 2                    # a real lower low = at least this many candles IN A ROW, each with a lower low than the one before
NEED_VOL = True               # the higher-high candle needs volume higher than the previous candle
RR1, RR2 = 1.0, 2.0
NTFY_ALERT = True
NTFY_PRIORITY = "high"

PATH = os.path.join(sp.RESULTS_DIR, "spurt_buy.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_buy_state.json")
COLS = ["date", "spurt_time", "spurt_x", "candle", "alert_time", "symbol", "price", "sl", "t1", "t2", "risk_pct", "pullback_pct", "status", "best_pct", "worst_pct", "last_pct"]
_st = {"date": "", "rows": []}


def buy_events(o, h, l, c, v, day_ok, spurt_mult=None):
    """Bar-by-bar copy of the Pine rule. day_ok[i] = candle i is from today. Returns a list of dicts (index of the signal candle, spurt index, ...)."""
    n = len(c)
    out = []
    sBar = None
    sLo = sHi = pb = sMult = 0.0
    llBar = None
    fell = False
    llCnt = 0
    for i in range(1, n):
        if not day_ok[i]:
            sBar = None
            fell = False
            llCnt = 0
            continue
        active = sBar is not None and i > sBar and i - sBar <= WAIT
        held = fell and llBar is not None and i - llBar >= HOLD and l[i] >= pb
        vol_ok = (not NEED_VOL) or v[i] > v[i - 1]
        buy = active and held and h[i] > h[i - 1] and vol_ok and c[i] > o[i] and c[i] > sLo
        if buy:
            deep = (sHi - pb) / (sHi - sLo) * 100.0 if sHi > sLo else 0.0
            out.append(dict(i=i, spurt=sBar, mult=sMult, sl=pb, deep=max(deep, 0.0)))
            sBar = None
            fell = False
            llCnt = 0
        elif active:
            if c[i] < sLo:
                sBar = None
                fell = False
                llCnt = 0
            elif not fell:
                if l[i] < l[i - 1]:
                    llCnt += 1
                    pb, llBar = l[i], i
                    if llCnt >= LL_MIN:
                        fell = True
                else:
                    llCnt = 0
                    llBar = None
            elif l[i] < pb:
                pb, llBar = l[i], i
        if i >= AVG_LEN:
            avg = float(np.mean(v[i - AVG_LEN:i]))
            m = v[i] / avg if avg > 0 else 0.0
            if m >= (SPURT_MULT if spurt_mult is None else spurt_mult) and c[i] > o[i]:
                sBar, sLo, sHi, sMult, llBar, fell, llCnt = i, l[i], h[i], m, None, False, 0
    return out


def _load(today_s):
    try:
        with open(STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _st.update(date=today_s, rows=list(s_.get("rows") or []))
            return
    except Exception:
        pass
    _st.update(date=today_s, rows=[])


def _write(today_s):
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_st["rows"], columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"spurt_buy csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_buy write failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  BUY Rs {r['price']:.2f}  SL {r['sl']:.2f}  T1 {r['t1']:.2f}  spurt {r['spurt_x']:.1f}x at {r['spurt_time']}" for r in rows[:12]]
        extra = f"\n+{len(rows) - 12} more on /spurt_buy" if len(rows) > 12 else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"SPURT BUY setup ({len(rows)})", "Priority": NTFY_PRIORITY, "Tags": "chart_with_upwards_trend"})
    except Exception as e:
        log.warning(f"spurt_buy ntfy failed: {e}")


def update(rows, now_ist):
    """Called by spurt10_live.run_once with the scored rows (each still holds its 5-minute candles in '_df5'). Never raises."""
    if not ON:
        return
    try:
        import live_scanner as ls
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        new = []
        for r in rows:
            sym = r["symbol"]
            if r.get("_df5") is None:
                continue
            try:
                d = ls.drop_forming_bars(r["_df5"], 5, r.get("_as_of"))
                if d is None or len(d) < AVG_LEN + 3:
                    continue
                o, h, l, c, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
                ist = [(t + pd.Timedelta(minutes=330)) for t in d.index]
                day_ok = np.array([t.strftime("%Y-%m-%d") == today_s for t in ist])
                if not day_ok.any():
                    continue
                evs = buy_events(o, h, l, c, v, day_ok)
                for e in evs:
                    i = e["i"]
                    t = ist[i].strftime("%H:%M")
                    if any(x["symbol"] == sym and x["candle"] == t for x in _st["rows"]):
                        continue
                    ent = float(c[i])
                    sl = float(e["sl"])
                    risk = ent - sl
                    if risk <= 0:
                        continue
                    row = {k: "" for k in COLS}
                    row.update(date=today_s, spurt_time=ist[e["spurt"]].strftime("%H:%M"), spurt_x=round(float(e["mult"]), 1), candle=t,
                              alert_time=now_ist.strftime("%H:%M"), symbol=sym, price=round(ent, 2), sl=round(sl, 2), t1=round(ent + risk * RR1, 2), t2=round(ent + risk * RR2, 2),
                              risk_pct=round(risk / ent * 100.0, 2), pullback_pct=round(float(e["deep"]), 0), status="OPEN")
                    row["_i"] = int(i)
                    _st["rows"].append(row)
                    new.append(row)
                for x in _st["rows"]:                               # follow-up after the signal candle
                    if x["symbol"] != sym:
                        continue
                    ent, sl, t1 = float(x["price"]), float(x["sl"]), float(x["t1"])
                    after = [k for k in range(len(c)) if day_ok[k] and ist[k].strftime("%H:%M") > x["candle"]]
                    st_ = "OPEN"
                    best = worst = None
                    for k in after:
                        if st_ == "OPEN":
                            if l[k] <= sl:
                                st_ = "SL HIT"
                            elif h[k] >= t1:
                                st_ = "T1 HIT"
                        best = h[k] if best is None else max(best, h[k])
                        worst = l[k] if worst is None else min(worst, l[k])
                    x["status"] = st_
                    if best is not None:
                        x["best_pct"] = round((best / ent - 1.0) * 100.0, 2)
                        x["worst_pct"] = round((worst / ent - 1.0) * 100.0, 2)
                        x["last_pct"] = round((float(c[-1]) / ent - 1.0) * 100.0, 2)
            except Exception as e:
                log.warning(f"spurt_buy: {sym} failed: {e}")
                continue
        if _st["rows"]:
            for x in _st["rows"]:
                x.pop("_i", None)
            _write(today_s)
        if new:
            _notify(new)
            log.info(f"spurt_buy: new BUY setups: {[r['symbol'] for r in new]}")
    except Exception as e:
        log.warning(f"spurt_buy step failed: {e}")


def page_html():
    """The /spurt_buy page: today's spurt -> pullback -> strong candle BUY setups. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>Spurt Buy</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff;position:sticky;top:0}'
            'td:nth-child(3){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if os.path.exists(STATE):
            _load(today_s)
        rows = _st["rows"] if _st["date"] == today_s else []
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        col = {"OPEN": "#555", "T1 HIT": "#0a7d33", "SL HIT": "#c62828"}
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{float(r['sl']):.2f}</td>"
            f"<td>{float(r['t1']):.2f}</td><td>{float(r['t2']):.2f}</td><td>{r['risk_pct']}%</td><td>{r['spurt_time']}</td><td><b>{float(r['spurt_x']):.1f}x</b></td>"
            f"<td>{'-' if r['pullback_pct'] == '' else format(float(r['pullback_pct']), '.0f') + '%'}</td><td style=\"color:{col.get(r['status'], '#555')}\"><b>{r['status']}</b></td>"
            f"<td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td><td>{pc(r['last_pct'])}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="15" style="text-align:center">No spurt buy setup yet today.</td></tr>'
        return (head + f'<h2>Spurt buy setups &mdash; today ({len(rows)})</h2>'
                f'<div class="m">A GREEN 5-minute spurt candle ({SPURT_MULT:g}x or more of the 50-candle average); the price then makes a LOWER LOW, the lower low HOLDS ({HOLD} candle or more), '
                f'then a green candle makes a HIGHER HIGH{" with volume above the previous candle" if NEED_VOL else ""} = BUY at that close. SL = the hold low; T1 / T2 = {RR1:g}x / {RR2:g}x the risk. '
                f'Status counts the SL first if both are in one candle. Information only, no orders. '
                f'<a href="/spurt_buy.csv" target="_blank" rel="noopener">CSV (all days)</a> &middot; <a href="/spurt10" target="_blank" rel="noopener">Spurt10</a></div>'
                '<table><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Buy</th><th>SL</th><th>T1</th><th>T2</th><th>Risk</th><th>Spurt at</th><th>Spurt</th><th>Fall from spurt high</th><th>Status</th>'
                '<th>Best</th><th>Worst</th><th>Last</th></tr>' + body + '</table></body></html>')
    except Exception as e:
        log.warning(f"spurt_buy page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
