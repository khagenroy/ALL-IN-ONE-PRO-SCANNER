"""
CHoCH ON SPURT STOCKS  (section on /spurt10   csv: /spurt_choch.csv)                                      ADDED 9 Nov

The CHoCH UP / CHoCH DOWN break of the ALL IN ONE PRO "PA Toolkit" (ZigZag length 9), run on the 5-minute candles of every stock that had a
volume spurt of MIN_MULT x (20x) or more today. A line is listed when the CHoCH candle closes at / after the stock's first 20x spurt candle.

  * The rule is a bar-by-bar copy of the Pine code (zigzagLen 9): trend flips when the high / low from 9 bars ago is the highest / lowest of the
    last 9 bars; every flip stores that pivot; CHoCH UP = close above the last pivot high while the last state was 'down' (or none yet),
    CHoCH DOWN = close below the last pivot low while the last state was 'up' (or none yet).
  * Uses the 5-minute candles the spurt scan has ALREADY downloaded: NO extra Dhan call, no orders. Errors are caught and logged.
  * The state of the rule depends on how many candles it has seen (TradingView starts from its own chart history, the scanner from ~8 days
    of 5-min candles), so the very first CHoCH after a long flat period can differ: compare with the chart before trusting it.
  * Phone alert is OFF by default (NTFY_ALERT = False) until it has been checked against the chart.
"""
import os
import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_choch")
IST = sp.IST

ON = True
MIN_MULT = 20.0               # a stock is followed once it has a closed 5-minute candle of this many x its 50-candle volume average
ZZ_LEN = 9                    # ZigZag length, same as the Pine input
NTFY_ALERT = False            # phone alert for a new CHoCH on a spurt stock (set True after you have checked it against the chart)

PATH = os.path.join(sp.RESULTS_DIR, "spurt_choch.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_choch_state.json")
COLS = ["date", "symbol", "spurt_x", "spurt_time", "choch", "candle", "price", "after_spurt", "alert_time", "last_price", "move_pct"]
_st = {"date": "", "spurt": {}, "rows": []}


def choch_events(high, low, close, zz=ZZ_LEN):
    """Bar-by-bar copy of the Pine structure engine. Returns [(bar index, 'UP' | 'DOWN')] for every CHoCH trigger."""
    n = len(close)
    trend = 1
    last_state = None
    hi_vals, lo_vals = [], []
    out = []
    for i in range(n):
        if i >= zz:
            to_up = high[i - zz] >= high[i - zz + 1: i + 1].max()
            to_dn = low[i - zz] <= low[i - zz + 1: i + 1].min()
            new_trend = -1 if (trend == 1 and to_dn) else 1 if (trend == -1 and to_up) else trend
            if new_trend != trend:
                if new_trend == 1:
                    hi_vals.append(high[i - zz])
                else:
                    lo_vals.append(low[i - zz])
                trend = new_trend
        if len(lo_vals) > 1 and close[i] < lo_vals[-1]:
            if last_state is None or last_state == "up":
                out.append((i, "DOWN"))
            last_state = "down"
        if len(hi_vals) > 1 and close[i] > hi_vals[-1]:
            if last_state is None or last_state == "down":
                out.append((i, "UP"))
            last_state = "up"
    return out


def _load(today_s):
    try:
        with open(STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _st.update(date=today_s, spurt=dict(s_.get("spurt") or {}), rows=list(s_.get("rows") or []))
            return
    except Exception:
        pass
    _st.update(date=today_s, spurt={}, rows=[])


def _write(today_s):
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_st["rows"], columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"spurt_choch csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_choch write failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  CHoCH {r['choch']}  Rs {r['price']:.2f}  candle {r['candle']}  (spurt {r['spurt_x']:.0f}x at {r['spurt_time']})" for r in rows[:12]]
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"CHoCH on a spurt stock ({len(rows)})", "Priority": "high", "Tags": "chart_with_upwards_trend"})
    except Exception as e:
        log.warning(f"spurt_choch ntfy failed: {e}")


def update(rows, now_ist):
    """Called by spurt10_live.run_once with the scored rows (each still holds its 5-minute candles in '_df5'). Never raises."""
    if not ON:
        return
    try:
        import live_scanner as ls
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        for r in rows:                                          # stocks that spurted >= MIN_MULT today join the list
            try:
                if r["multiple"] >= MIN_MULT:
                    e = _st["spurt"].setdefault(r["symbol"], {"time": r["candle"], "peak": r["multiple"]})
                    if r["multiple"] > e["peak"]:
                        e["peak"] = r["multiple"]
            except Exception:
                continue
        new = []
        for r in rows:
            sym = r["symbol"]
            if sym not in _st["spurt"] or r.get("_df5") is None:
                continue
            try:
                d = ls.drop_forming_bars(r["_df5"], 5, r.get("_as_of"))
                if d is None or len(d) < ZZ_LEN + 5:
                    continue
                hi_, lo_, cl_ = (d[k].to_numpy(dtype=float) for k in ("high", "low", "close"))
                ist = [(t + pd.Timedelta(minutes=330)) for t in d.index]
                first = _st["spurt"][sym]["time"]
                last_px = float(cl_[-1])
                for i, kind in choch_events(hi_, lo_, cl_):
                    if ist[i].strftime("%Y-%m-%d") != today_s:
                        continue
                    t = ist[i].strftime("%H:%M")
                    if t < first:                                  # only a CHoCH at / after the first spurt candle
                        continue
                    if any(x["symbol"] == sym and x["candle"] == t for x in _st["rows"]):
                        continue
                    row = {k: "" for k in COLS}
                    row.update(date=today_s, symbol=sym, spurt_x=round(float(_st["spurt"][sym]["peak"]), 1), spurt_time=first, choch=kind, candle=t,
                              price=round(float(cl_[i]), 2), after_spurt="YES", alert_time=now_ist.strftime("%H:%M"))
                    _st["rows"].append(row)
                    new.append(row)
                for x in _st["rows"]:                              # move since the CHoCH candle closed (UP: price up is +, DOWN: price down is +)
                    if x["symbol"] == sym and x.get("price"):
                        e0 = float(x["price"])
                        x["last_price"] = round(last_px, 2)
                        x["move_pct"] = round(((last_px / e0 - 1.0) if x["choch"] == "UP" else (1.0 - last_px / e0)) * 100.0, 2)
            except Exception as e:
                log.warning(f"spurt_choch: {sym} failed: {e}")
                continue
        _write(today_s)
        if new:
            _notify(new)
            log.info(f"spurt_choch: new CHoCH on spurt stocks: {[(a['symbol'], a['choch']) for a in new]}")
    except Exception as e:
        log.warning(f"spurt_choch step failed: {e}")


def section_html():
    """The section for /spurt10: today's CHoCH UP / DOWN on the stocks that had a 20x+ spurt. Never raises."""
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s and os.path.exists(STATE):
            _load(today_s)
        rows = _st["rows"] if _st["date"] == today_s else []
        n_sp = len(_st["spurt"]) if _st["date"] == today_s else 0
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{sp._tvl(r['symbol'])}</td>"
            f"<td><b style=\"color:{'#0a7d33' if r['choch'] == 'UP' else '#c62828'}\">CHoCH {r['choch']}</b></td><td>{float(r['price']):.2f}</td>"
            f"<td>{float(r['spurt_x']):.0f}x</td><td>{r['spurt_time']}</td><td>{r['last_price'] if r['last_price'] != '' else '-'}</td><td>{pc(r['move_pct'])}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="8" style="text-align:center">No CHoCH yet on a stock with a 20x+ spurt today.</td></tr>'
        return (f'<h2>CHoCH UP / DOWN on spurt stocks ({MIN_MULT:g}x+) &mdash; 5-min candles ({len(rows)} signals, {n_sp} spurt stocks followed)</h2>'
                f'<div class="m">The PA Toolkit CHoCH break of the ALL IN ONE PRO indicator (ZigZag {ZZ_LEN}) on the 5-minute candles of stocks that had a {MIN_MULT:g}x+ spurt candle today; '
                f'listed when the CHoCH candle closes at / after the first spurt candle. UP = close above the last pivot high after a down state, DOWN = close below the last pivot low after an up state. '
                f'Move = price since the CHoCH candle close in the CHoCH direction. Information only (no phone alert yet) &mdash; compare with the chart. '
                f'<a href="/spurt_choch.csv" target="_blank" rel="noopener">CSV</a></div>'
                '<div class="w"><table class="sp"><tr><th>CHoCH candle</th><th>Symbol</th><th>Break</th><th>Close</th><th>Spurt</th><th>1st spurt</th><th>Last</th><th>Move</th></tr>'
                + body + '</table></div>')
    except Exception as e:
        log.warning(f"spurt_choch section failed: {e}")
        return ""
