"""
RED SPURT CANDLES  (page: /spurt_red   csv: /spurt_red.csv)                                                ADDED 11 Nov

Every stock whose volume-spurt candle (a closed 5-minute candle with volume RED_MIN_MULT x or more of its 50-candle average, the same candles and
the same multiple as the Spurt10 page) is a RED candle (close below open). Listed with the candle, the wicks, the zone it was at, and what the
price did afterwards (10 / 30 / 60 minutes, best / worst) so the red spurts can be studied. PHONE ALERT with the HIGHEST priority (ntfy "urgent").

  * Uses the rows the spurt scan has ALREADY built: NO extra Dhan call, no orders. Any error is caught and logged.
  * Moves after the alert are the plain price change from the spurt candle close: + = price up, - = price down (no direction is assumed).
  * Set NTFY_ALERT False to switch the phone alert off; RED_MIN_MULT sets how big the spurt must be.
"""
import os
import json
import logging
from datetime import datetime

import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_red")
IST = sp.IST

ON = True
RED_MIN_MULT = 20.0           # CHANGED 11 Nov: 20x (was 10x). The red candle volume must be this many x the 50-candle average
RSI_MAX = 30.0                # ADDED 12 Nov: the RSI(14) of the 5-minute closes, at the close of the red spurt candle, must be BELOW this (0 = switch the RSI rule off)
RSI_LEN = 14
NTFY_ALERT = True             # phone alert, highest priority
NTFY_PRIORITY = "urgent"      # ntfy: min / low / default / high / urgent (urgent = the highest)

PATH = os.path.join(sp.RESULTS_DIR, "spurt_red.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_red_state.json")
COLS = ["date", "candle", "alert_time", "symbol", "price", "candle_pct", "spurt_x", "upper_wick", "lower_wick", "vol_prev_x", "day_chg_pct", "rsi", "at_zone", "support", "resistance",
        "mv_10m", "mv_30m", "mv_60m", "best_pct", "worst_pct", "last_pct"]
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
                log.warning(f"spurt_red csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_red write failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  Rs {r['price']:.2f}  candle {r['candle']}  spurt {r['spurt_x']:.1f}x  RED {r['candle_pct']:+.2f}%"
                 + (f"  at {str(r['at_zone']).lower()} {r['support'] if r['at_zone'] == 'SUPPORT' else r['resistance']}" if r.get("at_zone") else "") for r in rows[:12]]
        extra = f"\n+{len(rows) - 12} more on /spurt_red" if len(rows) > 12 else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"RED spurt candle ({len(rows)})", "Priority": NTFY_PRIORITY, "Tags": "rotating_light,chart_with_downwards_trend"})
    except Exception as e:
        log.warning(f"spurt_red ntfy failed: {e}")


def _rsi_at_close(d5, as_of):
    """Wilder RSI(RSI_LEN) of the closed 5-minute candles (the last one = the spurt candle), or None. Never raises."""
    try:
        import live_scanner as ls
        d = ls.drop_forming_bars(d5, 5, as_of)
        cl = d["close"].to_numpy(dtype=float)
        if len(cl) < RSI_LEN + 2:
            return None
        diff = cl[1:] - cl[:-1]
        up = diff.clip(min=0.0)
        dn = (-diff).clip(min=0.0)
        au, ad = up[:RSI_LEN].mean(), dn[:RSI_LEN].mean()
        for i in range(RSI_LEN, len(diff)):
            au = (au * (RSI_LEN - 1) + up[i]) / RSI_LEN
            ad = (ad * (RSI_LEN - 1) + dn[i]) / RSI_LEN
        return 100.0 if ad == 0 else round(100.0 - 100.0 / (1.0 + au / ad), 1)
    except Exception:
        return None


def update(rows, now_ist):
    """Called by spurt10_live.run_once with the scored rows (before the zones are popped). Never raises."""
    if not ON:
        return
    try:
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        px_now, new = {}, []
        for r in rows:
            try:
                px_now[r["symbol"]] = float(r["price"])
                if r["multiple"] < RED_MIN_MULT or not r.get("ohlc"):
                    continue
                o, h, l, c = r["ohlc"][:4]
                if not c < o:                                       # the spurt candle must be RED
                    continue
                t = r["candle"]
                if any(x["symbol"] == r["symbol"] and x["candle"] == t for x in _st["rows"]):
                    continue
                rsi = None
                if RSI_MAX > 0:                                     # the RSI must be below RSI_MAX; no candles to read it = not listed (cannot be checked)
                    rsi = _rsi_at_close(r.get("_df5"), r.get("_as_of"))
                    if rsi is None or not rsi < RSI_MAX:
                        continue
                rng = h - l
                zf = {}
                try:
                    zf = sp._zone_fields(r.get("_zones"), r["price"], None)
                except Exception:
                    pass
                prev = r.get("prev")
                row = {k: "" for k in COLS}
                row.update(date=today_s, candle=t, alert_time=now_ist.strftime("%H:%M"), symbol=r["symbol"], price=round(float(r["price"]), 2),
                          candle_pct=round((c / o - 1.0) * 100.0, 2), spurt_x=round(float(r["multiple"]), 2),
                          upper_wick=(round((h - max(o, c)) / rng * 100.0, 0) if rng > 0 else ""), lower_wick=(round((min(o, c) - l) / rng * 100.0, 0) if rng > 0 else ""),
                          vol_prev_x=(round(float(r["candle_vol"]) / prev[4], 2) if prev and prev[4] else ""), day_chg_pct=r.get("day_chg_pct", ""), rsi=("" if rsi is None else rsi),
                          at_zone=zf.get("at_zone", ""), support=zf.get("support", ""), resistance=zf.get("resistance", ""))
                _st["rows"].append(row)
                new.append(row)
            except Exception:
                continue
        hm_now = now_ist.hour * 60 + now_ist.minute
        for x in _st["rows"]:                                       # follow-up: plain price change since the spurt candle close
            px = px_now.get(x["symbol"])
            if px is None or not x.get("price"):
                continue
            try:
                e = float(x["price"])
                mv = round((px / e - 1.0) * 100.0, 2)
                el = hm_now - (int(x["alert_time"][:2]) * 60 + int(x["alert_time"][3:5]))
                x["best_pct"] = mv if x["best_pct"] in ("", None) else max(float(x["best_pct"]), mv)
                x["worst_pct"] = mv if x["worst_pct"] in ("", None) else min(float(x["worst_pct"]), mv)
                x["last_pct"] = mv
                for lab, mm in (("mv_10m", 10), ("mv_30m", 30), ("mv_60m", 60)):
                    if x[lab] in ("", None) and el >= mm:
                        x[lab] = mv
            except Exception:
                continue
        if _st["rows"]:
            _write(today_s)
        if new:
            _notify(new)
            log.info(f"spurt_red: new red spurt candles: {[r['symbol'] for r in new]}")
    except Exception as e:
        log.warning(f"spurt_red step failed: {e}")


def page_html():
    """The /spurt_red page: today's red spurt candles. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>Red Spurt Candles</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff;position:sticky;top:0}'
            'td:nth-child(3){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s and os.path.exists(STATE):
            _load(today_s)
        rows = _st["rows"] if _st["date"] == today_s else []
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        body = "".join(
            f"<tr><td>{r['candle']}</td><td>{r['alert_time']}</td><td>{sp._tvl(r['symbol'])}</td><td>{float(r['price']):.2f}</td><td>{pc(r['candle_pct'])}</td>"
            f"<td><b>{float(r['spurt_x']):.1f}x</b></td><td>{'-' if r['upper_wick'] == '' else format(float(r['upper_wick']), '.0f') + '%'}</td>"
            f"<td>{'-' if r['lower_wick'] == '' else format(float(r['lower_wick']), '.0f') + '%'}</td><td>{'-' if r['vol_prev_x'] == '' else str(r['vol_prev_x']) + 'x'}</td>"
            f"<td>{pc(r['day_chg_pct'])}</td><td>{'-' if r.get('rsi', '') in ('', None) else r['rsi']}</td><td>{r['at_zone'] or '-'}</td><td>{pc(r['mv_10m'])}</td><td>{pc(r['mv_30m'])}</td><td>{pc(r['mv_60m'])}</td>"
            f"<td>{pc(r['best_pct'])}</td><td>{pc(r['worst_pct'])}</td><td>{pc(r['last_pct'])}</td></tr>"
            for r in reversed(rows)) or '<tr><td colspan="17" style="text-align:center">No red spurt candle yet today.</td></tr>'
        return (head + f'<h2>Red spurt candles &mdash; today ({len(rows)})</h2>'
                f'<div class="m">Stocks whose closed 5-minute volume-spurt candle ({RED_MIN_MULT:g}x or more of the 50-candle average) is a <b style="color:#c62828">RED</b> candle (close below open) and the RSI(14) of the 5-minute closes is BELOW {RSI_MAX:g}. '
                f'Phone alert with the highest priority for each. Moves are the plain price change from the spurt candle close (+ up, - down). Information only. '
                f'<a href="/spurt_red.csv" target="_blank" rel="noopener">CSV (all days)</a> &middot; <a href="/spurt10" target="_blank" rel="noopener">Spurt10</a></div>'
                '<table><tr><th>Candle</th><th>Alert</th><th>Symbol</th><th>Price</th><th>Candle</th><th>Spurt</th><th>Upper wick</th><th>Lower wick</th><th>Vol vs prev</th><th>Day</th><th>RSI</th>'
                '<th>At zone</th><th>10m</th><th>30m</th><th>60m</th><th>Best</th><th>Worst</th><th>Last</th></tr>' + body + '</table></body></html>')
    except Exception as e:
        log.warning(f"spurt_red page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
