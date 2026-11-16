"""
CHoCH RETRACE ON SPURT STOCKS  (section on /spurt10   csv: /spurt_retrace.csv)                            ADDED 16 Nov

After a CHoCH UP / DOWN on a 20x+ spurt stock (the list built by spurt_choch.py) this follows the stock and waits for the PULLBACK:

  1. leg   : UP = from the lowest low of the 18 candles before the CHoCH (the CHoCH swing) up to the highest high since the CHoCH. DOWN = mirror.
  2. zone  : the pullback must come back 38.2% of the leg (a touch is enough; deeper than 61.8% is still followed, the depth is shown).
  3. trigger: after the candle that entered the zone, the first candle that closes in the CHoCH direction AND beyond the previous candle's high (UP) / low (DOWN).
              Stop = the pullback low (UP) / high (DOWN). Entry = that candle's close.
  4. status : WATCHING (no pullback yet) / IN ZONE (pulled back, no trigger yet) / TRIGGER / FAILED (a close beyond the CHoCH swing) / EXPIRED (no pullback
              or no trigger within 12 candles).
  * The leg must be at least 2 x ATR(14), otherwise it is too small to be worth a pullback entry.
  * 'dry' = YES when the volume during the pullback was lower than the average volume of the leg (information only, not required).
  * Uses the 5-minute candles the spurt scan has ALREADY downloaded: no extra Dhan call, NO orders, nothing goes to the bridge. Errors are caught and logged.
  * Everything is recomputed from the candles on every run (no hidden state); only the list of already-sent phone alerts is saved.
"""
import os
import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_retrace")
IST = sp.IST

ON = True
NTFY_ALERT = True             # phone alert when a pullback TRIGGER candle closes (only for a trigger less than ~15 minutes old)
ZONE_FIB = 0.382              # the pullback must reach this part of the leg
SWING_BACK = 18               # candles before the CHoCH candle searched for the swing low / high
MIN_LEG_ATR = 2.0             # the leg must be at least this many ATR(14)
EXPIRE_BARS = 12              # candles to wait for the pullback (from the CHoCH) and then for the trigger (from the pullback)
FOLLOW = ((2, "ret_10"), (6, "ret_30"), (12, "ret_60"))   # candles after the trigger -> csv column (10 / 30 / 60 minutes)

PATH = os.path.join(sp.RESULTS_DIR, "spurt_retrace.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_retrace_state.json")
COLS = ["date", "symbol", "choch", "choch_time", "status", "zone_time", "trigger_time", "entry", "stop", "risk_pct", "depth_pct", "dry",
        "ret_10", "ret_30", "ret_60", "best", "worst", "stop_hit", "alert_time", "last_price"]
_st = {"date": "", "alerted": [], "rows": []}


def _atr(h, l, c, n=14):
    tr = np.maximum(h - l, np.maximum(np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
    tr[0] = h[0] - l[0]
    out = np.zeros(len(c))
    for i in range(len(c)):
        out[i] = tr[max(0, i - n + 1): i + 1].mean()
    return out


def evaluate(o, h, l, c, v, ci, sgn):
    """One CHoCH at bar index ci. sgn = +1 for UP, -1 for DOWN. Works on mirrored arrays so DOWN is the same code as UP.
    Returns a dict (status, zone_j, trig_j, stop, depth, dry ...) in the ORIGINAL prices, or None if the leg is too small / not enough data."""
    n = len(c)
    if sgn == 1:
        O, H, L, C = o, h, l, c
    else:
        O, H, L, C = -o, -l, -h, -c                    # mirrored: a DOWN move becomes an UP move
    atr = _atr(h, l, c)
    a = max(0, ci - SWING_BACK)
    swing = float(L[a: ci + 1].min())                   # the CHoCH swing (low of the mirrored series)
    peak = float(H[ci])
    status, zone_j, trig_j, stop_t, dip, depth = "WATCHING", None, None, None, None, None
    for j in range(ci + 1, n):
        leg_prev = peak - swing
        if C[j] < swing:
            status = "FAILED"
            break
        if zone_j is None:
            if leg_prev >= MIN_LEG_ATR * atr[ci] and L[j] <= peak - ZONE_FIB * leg_prev:
                zone_j, dip, status = j, float(L[j]), "IN ZONE"
                zone_peak = peak
            elif j - ci > EXPIRE_BARS:
                status = "EXPIRED"
                break
        else:
            dip = min(dip, float(L[j]))
            if j > zone_j and C[j] > O[j] and C[j] > H[j - 1] and dip >= swing:
                trig_j, stop_t, status = j, dip, "TRIGGER"
                break
            if j - zone_j > EXPIRE_BARS:
                status = "EXPIRED"
                break
        peak = max(peak, float(H[j]))
    if zone_j is not None:
        depth = (zone_peak - dip) / max(1e-9, zone_peak - swing) * 100.0
    dry = ""
    if zone_j is not None and v is not None:
        end = trig_j if trig_j is not None else n
        try:
            leg_v = float(np.mean(v[a: zone_j])) if zone_j > a else 0.0
            pb_v = float(np.mean(v[zone_j: end])) if end > zone_j else 0.0
            dry = "YES" if (leg_v > 0 and pb_v < leg_v) else "NO"
        except Exception:
            dry = ""
    out = {"status": status, "zone_j": zone_j, "trig_j": trig_j, "depth": depth, "dry": dry}
    if trig_j is not None:
        entry = float(c[trig_j])
        stop = sgn * stop_t                              # back to the original price
        out.update(entry=entry, stop=stop, risk_pct=abs(entry - stop) / entry * 100.0)
        mv = (lambda x: (x / entry - 1.0) * 100.0) if sgn == 1 else (lambda x: (1.0 - x / entry) * 100.0)
        fol = {}
        for k, col in FOLLOW:
            if trig_j + k < n:
                fol[col] = mv(float(c[trig_j + k]))
        hi_f, lo_f = h[trig_j + 1: trig_j + 13], l[trig_j + 1: trig_j + 13]
        if len(hi_f):
            if sgn == 1:
                fol["best"], fol["worst"] = mv(float(hi_f.max())), mv(float(lo_f.min()))
                fol["stop_hit"] = "YES" if bool((lo_f <= stop).any()) else "NO"
            else:
                fol["best"], fol["worst"] = mv(float(lo_f.min())), mv(float(hi_f.max()))
                fol["stop_hit"] = "YES" if bool((hi_f >= stop).any()) else "NO"
        out["follow"] = fol
    return out


def _load(today_s):
    try:
        with open(STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _st.update(date=today_s, alerted=list(s_.get("alerted") or []), rows=list(s_.get("rows") or []))
            return
    except Exception:
        pass
    _st.update(date=today_s, alerted=[], rows=[])


def _write(today_s):
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_st["rows"], columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"spurt_retrace csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_retrace write failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['symbol']}  RETRACE {'BUY' if r['choch'] == 'UP' else 'SELL'}  entry {float(r['entry']):.2f}  stop {float(r['stop']):.2f} ({float(r['risk_pct']):.2f}%)  "
                 f"candle {r['trigger_time']}  (CHoCH {r['choch']} {r['choch_time']}, pullback {float(r['depth_pct'] or 0):.0f}%)" for r in rows[:12]]
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"CHoCH pullback trigger ({len(rows)})", "Priority": "high", "Tags": "arrows_counterclockwise"})
    except Exception as e:
        log.warning(f"spurt_retrace ntfy failed: {e}")


def update(rows, now_ist):
    """Called by spurt10_live.run_once right after spurt_choch.update. Never raises."""
    if not ON:
        return
    try:
        import live_scanner as ls
        import spurt_choch
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        by_sym = {r["symbol"]: r for r in rows if r.get("_df5") is not None}
        out_rows, fresh = [], []
        for ch in list(spurt_choch._st.get("rows") or []):
            sym = ch.get("symbol")
            if ch.get("date") != today_s or sym not in by_sym:
                continue
            try:
                r = by_sym[sym]
                d = ls.drop_forming_bars(r["_df5"], 5, r.get("_as_of"))
                if d is None or len(d) < 30:
                    continue
                o_, h_, l_, c_ = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
                v_ = d["volume"].to_numpy(dtype=float) if "volume" in d.columns else None
                ist = [(t + pd.Timedelta(minutes=330)) for t in d.index]
                tm = [x.strftime("%H:%M") for x in ist]
                day = [x.strftime("%Y-%m-%d") for x in ist]
                ci = next((i for i in range(len(tm)) if tm[i] == ch["candle"] and day[i] == today_s), None)
                if ci is None:
                    continue
                sgn = 1 if ch["choch"] == "UP" else -1
                e = evaluate(o_, h_, l_, c_, v_, ci, sgn)
                row = {k: "" for k in COLS}
                row.update(date=today_s, symbol=sym, choch=ch["choch"], choch_time=ch["candle"], status=e["status"], last_price=round(float(c_[-1]), 2))
                if e["zone_j"] is not None:
                    row["zone_time"] = tm[e["zone_j"]]
                    row["depth_pct"] = round(e["depth"], 1) if e["depth"] is not None else ""
                    row["dry"] = e["dry"]
                if e["trig_j"] is not None:
                    row.update(trigger_time=tm[e["trig_j"]], entry=round(e["entry"], 2), stop=round(e["stop"], 2), risk_pct=round(e["risk_pct"], 2))
                    for k, val in (e.get("follow") or {}).items():
                        row[k] = round(val, 2) if isinstance(val, float) else val
                    key = f"{sym}|{ch['candle']}"
                    old = next((x for x in _st["rows"] if x["symbol"] == sym and x["choch_time"] == ch["candle"]), None)
                    row["alert_time"] = (old or {}).get("alert_time") or now_ist.strftime("%H:%M")
                    if key not in _st["alerted"]:
                        _st["alerted"].append(key)
                        age_min = (now_ist.replace(tzinfo=None) - (ist[e["trig_j"]] + pd.Timedelta(minutes=5)).to_pydatetime()).total_seconds() / 60.0
                        if age_min <= 15:                          # not for an old trigger found again after a restart
                            fresh.append(row)
                out_rows.append(row)
            except Exception as e2:
                log.warning(f"spurt_retrace: {sym} failed: {e2}")
        _st["rows"] = out_rows
        _write(today_s)
        if fresh:
            _notify(fresh)
            log.info(f"spurt_retrace: new pullback triggers: {[(a['symbol'], a['choch']) for a in fresh]}")
    except Exception as e:
        log.warning(f"spurt_retrace step failed: {e}")


def section_html():
    """The section for /spurt10: today's CHoCH pullbacks. Never raises."""
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s and os.path.exists(STATE):
            _load(today_s)
        rows = _st["rows"] if _st["date"] == today_s else []
        order = {"TRIGGER": 0, "IN ZONE": 1, "WATCHING": 2, "FAILED": 3, "EXPIRED": 4}
        rows = sorted(rows, key=lambda r: (order.get(r["status"], 9), r.get("choch_time", "")), reverse=False)
        col = {"TRIGGER": "#0a7d33", "IN ZONE": "#b26a00", "WATCHING": "#555", "FAILED": "#c62828", "EXPIRED": "#888"}
        pc = lambda v: "-" if v in ("", None) else f'<span style="color:{"#0a7d33" if float(v) > 0 else "#c62828" if float(v) < 0 else "#555"}">{float(v):+.2f}%</span>'
        n_trig = sum(1 for r in rows if r["status"] == "TRIGGER")
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td>"
            f"<td><b style=\"color:{'#0a7d33' if r['choch'] == 'UP' else '#c62828'}\">CHoCH {r['choch']}</b> {r['choch_time']}</td>"
            f"<td><b style=\"color:{col.get(r['status'], '#555')}\">{r['status']}</b></td>"
            f"<td>{r['zone_time'] or '-'}</td><td>{r['depth_pct'] if r['depth_pct'] != '' else '-'}</td><td>{r['dry'] or '-'}</td>"
            f"<td>{r['trigger_time'] or '-'}</td><td>{r['entry'] if r['entry'] != '' else '-'}</td><td>{r['stop'] if r['stop'] != '' else '-'}</td>"
            f"<td>{pc(r['risk_pct']) if r['risk_pct'] != '' else '-'}</td><td>{pc(r['ret_10'])}</td><td>{pc(r['ret_30'])}</td><td>{pc(r['ret_60'])}</td>"
            f"<td>{r['stop_hit'] or '-'}</td></tr>"
            for r in rows) or '<tr><td colspan="14" style="text-align:center">No CHoCH on a 20x+ spurt stock to follow yet today.</td></tr>'
        return (f'<h2>CHoCH pullback (retrace) on spurt stocks &mdash; 5-min candles ({len(rows)} followed, {n_trig} triggers)</h2>'
                f'<div class="m">Each CHoCH from the list above is followed: the pullback must come back {ZONE_FIB * 100:.1f}% of the leg (leg at least {MIN_LEG_ATR:g} ATR), then the first candle that closes '
                f'in the CHoCH direction beyond the previous candle\'s high / low is the TRIGGER (stop = pullback low / high). FAILED = close beyond the CHoCH swing, EXPIRED = nothing within {EXPIRE_BARS} candles. '
                f'Dry = pullback volume lower than the leg volume. Returns are measured from the trigger close in the trade direction. Information and phone alert only &mdash; nothing goes to the bridge. '
                f'<a href="/spurt_retrace.csv" target="_blank" rel="noopener">CSV</a></div>'
                '<div class="w"><table class="sp"><tr><th>Symbol</th><th>CHoCH</th><th>Status</th><th>Pullback</th><th>Depth %</th><th>Dry</th><th>Trigger</th><th>Entry</th><th>Stop</th>'
                '<th>Risk</th><th>10m</th><th>30m</th><th>60m</th><th>Stop hit</th></tr>'
                + body + '</table></div>')
    except Exception as e:
        log.warning(f"spurt_retrace section failed: {e}")
        return ""
