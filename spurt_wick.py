"""
LONG-WICK SPURT CANDLES  (section on /spurt10   csv: /spurt_wick.csv)                                      ADDED 8 Nov

A spurt candle (a closed candle with volume of MIN_MULT x the 50-candle average or more) with a LONG WICK anywhere on the chart:
  BUY  setup : lower wick >= WICK_MIN_PCT % of the candle's high-low range AND a GREEN candle (close above open)
  SELL setup : upper wick >= WICK_MIN_PCT % of the candle's high-low range AND a RED candle (close below open)
It does NOT need a support / resistance zone. The zone (if any) is only shown as an extra column.

  * Uses the rows the spurt scan has already built: NO extra Dhan call, no orders. Any error is caught and logged.
  * Follow-up: the move in the favourable direction 10 / 30 / 60 minutes after the alert (BUY: price up, SELL: price down), saved to the CSV
    (all days) so the hit rate can be studied later. Follow-up prices are taken while the stock is still in the spurt scan's list.
  * Phone alert (urgent) for every new setup: "BUY setup" for long lower wicks, "SELL setup" for long upper wicks. Set NTFY_BUY / NTFY_SELL False to switch off.
"""
import os
import json
import logging
from datetime import datetime

import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_wick")
IST = sp.IST

ON = True
MIN_MULT = 3.0                # the candle's volume must be this many x the 50-candle average (same as the main spurt alert level)
WICK_MIN_PCT = 70.0           # wick >= this % of the candle's high-low range
FROM_MIN, TO_MIN = 9 * 60 + 20, 15 * 60 + 15      # candles that open between 09:20 and 15:15 IST
NTFY_BUY = True               # phone alert for the long LOWER wick BUY setup
NTFY_SELL = True              # phone alert for the long UPPER wick SELL setup
TRACK_MOVE = 0.3              # % move after 60 min that counts as WORKED / FAILED

PATH = os.path.join(sp.RESULTS_DIR, "spurt_wick.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_wick_state.json")
COLS = ["date", "candle", "alert_time", "symbol", "side", "price", "spurt_x", "wick_pct", "vol_prev_x", "at_zone", "support", "resistance",
        "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "result"]
_st = {"date": "", "rows": []}


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
                log.warning(f"spurt_wick csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_wick write failed: {e}")


def _notify(rows, side):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  Rs {r['price']:.2f}  candle {r['candle']}  spurt {r['spurt_x']:.1f}x  {'lower' if side == 'BUY' else 'upper'} wick {r['wick_pct']:.0f}%"
                 + (f"  at {r['at_zone'].lower()} {r['support'] if r['at_zone'] == 'SUPPORT' else r['resistance']}" if r.get("at_zone") else "") for r in rows[:12]]
        extra = f"\n+{len(rows) - 12} more on /spurt10" if len(rows) > 12 else ""
        title = (f"BUY setup: long lower wick on a spurt stock ({len(rows)})" if side == "BUY" else f"SELL setup: long upper wick on a spurt stock ({len(rows)})")
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": title, "Priority": "urgent", "Tags": "chart_with_upwards_trend" if side == "BUY" else "chart_with_downwards_trend"})
    except Exception as e:
        log.warning(f"spurt_wick ntfy failed: {e}")


def update(rows, now_ist):
    """Called by spurt10_live.run_once with the scored rows (before the zones are popped). Never raises."""
    if not ON:
        return
    try:
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        px_now = {}
        new = {"BUY": [], "SELL": []}
        for r in rows:
            try:
                px_now[r["symbol"]] = float(r["price"])
                if r["multiple"] < MIN_MULT or not r.get("ohlc"):
                    continue
                t = r["candle"]
                hm = int(t[:2]) * 60 + int(t[3:5])
                if not (FROM_MIN <= hm <= TO_MIN):
                    continue
                o, h, l, c = r["ohlc"][:4]
                rng = h - l
                if rng <= 0:
                    continue
                low_w, up_w = (min(o, c) - l) / rng * 100.0, (h - max(o, c)) / rng * 100.0
                if low_w >= WICK_MIN_PCT and c > o:
                    side, wick = "BUY", low_w
                elif up_w >= WICK_MIN_PCT and c < o:
                    side, wick = "SELL", up_w
                else:
                    continue
                if any(x["symbol"] == r["symbol"] and x["candle"] == t for x in _st["rows"]):
                    continue
                zf = {}
                try:
                    zf = sp._zone_fields(r.get("_zones"), r["price"], None)
                except Exception:
                    pass
                prev = r.get("prev")
                row = {k: "" for k in COLS}
                row.update(date=today_s, candle=t, alert_time=now_ist.strftime("%H:%M"), symbol=r["symbol"], side=side, price=round(float(r["price"]), 2),
                          spurt_x=round(float(r["multiple"]), 2), wick_pct=round(wick, 1),
                          vol_prev_x=(round(float(r["candle_vol"]) / prev[4], 2) if prev and prev[4] else ""),
                          at_zone=zf.get("at_zone", ""), support=zf.get("support", ""), resistance=zf.get("resistance", ""), result="open")
                _st["rows"].append(row)
                new[side].append(row)
            except Exception:
                continue
        hm_now = now_ist.hour * 60 + now_ist.minute            # follow-up: move in the favourable direction
        for x in _st["rows"]:
            px = px_now.get(x["symbol"])
            if px is None or not x.get("price"):
                continue
            try:
                e = float(x["price"])
                mv = round(((px / e - 1.0) if x["side"] == "BUY" else (1.0 - px / e)) * 100.0, 2)
                el = hm_now - (int(x["alert_time"][:2]) * 60 + int(x["alert_time"][3:5]))
                x["best_pct"] = mv if x["best_pct"] in ("", None) else max(float(x["best_pct"]), mv)
                x["worst_pct"] = mv if x["worst_pct"] in ("", None) else min(float(x["worst_pct"]), mv)
                x["last_pct"] = mv
                for lab, mm in (("ret_10m", 10), ("ret_30m", 30), ("ret_60m", 60)):
                    if x[lab] in ("", None) and el >= mm:
                        x[lab] = mv
                if x["ret_60m"] not in ("", None):
                    x["result"] = "WORKED" if mv >= TRACK_MOVE else "FAILED" if mv <= -TRACK_MOVE else "FLAT"
            except Exception:
                continue
        if new["BUY"] or new["SELL"] or _st["rows"]:
            _write(today_s)
        if NTFY_BUY:
            _notify(new["BUY"], "BUY")
        if NTFY_SELL:
            _notify(new["SELL"], "SELL")
        if new["BUY"] or new["SELL"]:
            log.info(f"spurt_wick: BUY {[r['symbol'] for r in new['BUY']]} SELL {[r['symbol'] for r in new['SELL']]}")
    except Exception as e:
        log.warning(f"spurt_wick step failed: {e}")


def section_html():
    """The section for /spurt10: today's long-wick spurt candles. Never raises."""
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s and os.path.exists(STATE):
            _load(today_s)
        rows = _st["rows"] if _st["date"] == today_s else []
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        col = {"WORKED": "#0a7d33", "FAILED": "#c62828"}

        def side(v):
            return f'<b style="color:{"#0a7d33" if v == "BUY" else "#c62828"}">{v}</b>'
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{side(r['side'])}</td><td>{float(r['price']):.2f}</td>"
            f"<td><b>{float(r['wick_pct']):.0f}%</b></td><td>{float(r['spurt_x']):.1f}x</td><td>{r['vol_prev_x'] if r['vol_prev_x'] != '' else '-'}{'x' if r['vol_prev_x'] != '' else ''}</td>"
            f"<td>{r['at_zone'] or '-'}</td><td>{pc(r['ret_10m'])}</td><td>{pc(r['ret_30m'])}</td><td>{pc(r['ret_60m'])}</td><td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td>"
            f"<td><b style=\"color:{col.get(r['result'], '#555')}\">{r['result']}</b></td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="15" style="text-align:center">No long-wick spurt candle yet today.</td></tr>'
        return (f'<h2>Long-wick spurt candles &mdash; BUY (lower wick, green) / SELL (upper wick, red) ({len(rows)})</h2>'
                f'<div class="m">A closed candle with volume {MIN_MULT:g}x or more of its 50-candle average and a wick of {WICK_MIN_PCT:g}% or more of its range: '
                f'<b>lower wick + green candle = BUY setup</b>, <b>upper wick + red candle = SELL setup</b>. No zone needed (the zone is only an extra column). Phone alert for each. '
                f'Moves after the alert are in the favourable direction (BUY: price up, SELL: price down); + means the wick worked. '
                f'<a href="/spurt_wick.csv" target="_blank" rel="noopener">CSV (all days)</a></div>'
                '<div class="w"><table class="sp"><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Side</th><th>Price</th><th>Wick</th><th>Spurt</th><th>Vol vs prev</th>'
                '<th>At zone</th><th>10m</th><th>30m</th><th>60m</th><th>Best</th><th>Worst</th><th>Result</th></tr>' + body + '</table></div>')
    except Exception as e:
        log.warning(f"spurt_wick section failed: {e}")
        return ""
