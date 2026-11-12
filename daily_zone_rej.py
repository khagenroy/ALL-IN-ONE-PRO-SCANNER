"""
DAILY CANDLE AT A DAILY ZONE - sweep / order-block hit / rejections      (page: /daily_zone   csv: /daily_zone.csv)      ADDED 12 Nov

Once a day (after the daily scan at 16:45 IST) every liquid stock's FINISHED daily candle is tested against its own daily
support / resistance zones (same ALL IN ONE PRO order-block zones as /spurt_daily, from spurt_zones.live_zones, built from the EARLIER days).
A stock is listed when its daily candle is AT a zone (candle range inside the zone or within ZONE_NEAR_PCT of it) AND at least one pattern is true:

  WICK     resistance: upper wick >= 70 % of the candle range and a RED candle, closed at / below the zone top.
           support   : lower wick >= 70 % and a GREEN candle, closed at / above the zone bottom.
  CLEAN    the same, but the opposite wick is at most 5 % and the day's volume is more than the previous day's (the 'clean rejection').
  ENGULF   resistance: bearish engulfing (red candle whose body covers the previous green body); support: bullish engulfing. Volume above the previous day.
  SWEEP    resistance: the day's high went above the highest high of the previous 20 daily candles and the close came back below that level.
           support   : the day's low went below the lowest low of the previous 20 candles and the close came back above it.
           (a simple daily version of the 'liquidity sweep'; the 10-minute Section A engine is not used on daily candles)
  OBH      order-block hit: the daily candle went into the zone (range overlaps it) and closed on the zone's own side
           (resistance: close at / below the zone top; support: close at / above the zone bottom).

It reads no extra data from Dhan: it uses the daily candles the 16:45 scan already downloaded (spurt_daily._LAST).
Read-only: no orders, no signals changed; any error is caught and logged. No env variables needed.
"""
import os
import logging
from datetime import datetime

import numpy as np
import pandas as pd

import spurt10_live as sp
import spurt_daily as sd
import spurt_zones

log = logging.getLogger("daily_zone_rej")
IST = sp.IST

ON = True
NTFY_ALERT = True
WICK_PCT = 70.0
OPP_MAX_PCT = 5.0
SWEEP_LOOKBACK = 20
NEAR_PCT = sp.ZONE_NEAR_PCT          # 'at a zone' = inside it or within this % of it
MAX_LINES = 12
PATH = os.path.join(sd.RESULTS_DIR, "daily_zone.csv")
COLS = ["date", "symbol", "price", "day_chg_pct", "where", "zone", "patterns", "wick_pct", "vol_x", "vol_prev_x"]


def _eval(df, zones, close_prev_avg):
    """df: cleaned daily candles, last row = today. Returns a list of hit dicts (one per zone the candle is at)."""
    o, h, l, c, v = (float(df[k].iloc[-1]) for k in ("open", "high", "low", "close", "volume"))
    po, ph, pl, pc, pv = (float(df[k].iloc[-2]) for k in ("open", "high", "low", "close", "volume"))
    rng = h - l
    if rng <= 0:
        return []
    up = (h - max(o, c)) / rng * 100.0
    dn = (min(o, c) - l) / rng * 100.0
    prior = df.iloc[-1 - SWEEP_LOOKBACK:-1]
    out = []
    near = NEAR_PCT / 100.0
    for z in zones:
        lo, hi, kind = z["lo"], z["hi"], z["type"]
        pats = []
        if kind == "RESISTANCE":
            if not (h >= lo * (1 - near) and l <= hi):
                continue
            wick = up
            if up >= WICK_PCT and c < o and c <= hi:
                pats.append("WICK")
                if dn <= OPP_MAX_PCT and v > pv:
                    pats.append("CLEAN")
            if c < o and po < pc and o >= pc and c <= po and v > pv and h >= lo * (1 - near):
                pats.append("ENGULF")
            if len(prior) >= SWEEP_LOOKBACK:
                lvl = float(prior["high"].max())
                if h > lvl and c < lvl:
                    pats.append("SWEEP")
            if h >= lo and c <= hi:
                pats.append("OBH")
        else:
            if not (l <= hi * (1 + near) and h >= lo):
                continue
            wick = dn
            if dn >= WICK_PCT and c > o and c >= lo:
                pats.append("WICK")
                if up <= OPP_MAX_PCT and v > pv:
                    pats.append("CLEAN")
            if c > o and po > pc and o <= pc and c >= po and v > pv and l <= hi * (1 + near):
                pats.append("ENGULF")
            if len(prior) >= SWEEP_LOOKBACK:
                lvl = float(prior["low"].min())
                if l < lvl and c > lvl:
                    pats.append("SWEEP")
            if l <= hi and c >= lo:
                pats.append("OBH")
        if pats:
            out.append({"where": kind, "zone": f"{lo:g}-{hi:g}", "patterns": "+".join(pats), "wick_pct": round(wick, 1),
                        "vol_x": round(v / close_prev_avg, 2) if close_prev_avg else "", "vol_prev_x": round(v / pv, 2) if pv else ""})
    return out


def _one(sym, raw, today):
    try:
        if raw is None or len(raw) < sd.MIN_HISTORY + 2:
            return []
        oc = ["open", "high", "low", "close", "volume"]
        df = raw[oc + ["d"]].reset_index(drop=True)
        if df["d"].iloc[-1] != today:
            return []
        df[oc] = df[oc].astype(float)
        cl = df["close"].to_numpy(dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            chg = np.abs(cl[1:] / cl[:-1] - 1.0)
        jumps = np.where(np.isfinite(chg) & (chg > sd.SPLIT_JUMP))[0]
        if len(jumps):                                  # split / bonus: older candles are not comparable
            df = df.iloc[jumps[-1] + 1:].reset_index(drop=True)
        if len(df) < sd.MIN_HISTORY:
            return []
        price = float(df["close"].iloc[-1])
        earlier = df.iloc[:-1]
        avg = float(earlier["volume"].tail(sd.AVG_LEN).mean())
        if avg < sd.MIN_AVG_VOL or price < sp.MIN_PRICE:
            return []
        zdf = earlier[oc].copy()
        try:
            zdf.index = pd.DatetimeIndex(pd.to_datetime(list(earlier["d"])))
        except Exception:
            zdf = zdf.reset_index(drop=True)
        zs = [z for z in (spurt_zones.live_zones(zdf) or []) if (z["hi"] - z["lo"]) <= sd.MAX_ZONE_PCT * price]
        if not zs:
            return []
        hits = _eval(df, zs, avg)
        day_chg = (price / float(df["close"].iloc[-2]) - 1.0) * 100.0
        for x in hits:
            x.update(symbol=sym, price=round(price, 2), day_chg_pct=round(day_chg, 2))
        return hits
    except Exception as e:
        log.warning(f"daily_zone {sym} failed: {e}")
        return []


def run(today, names=None):
    """Called by spurt_daily.run_once after the daily scan. Uses the candles cached in sd._LAST. Never raises."""
    if not ON:
        return
    try:
        cache = dict(getattr(sd, "_LAST", {}) or {})
        rows = []
        for sym, raw in cache.items():
            if names is not None and sym not in names:
                continue
            for x in _one(sym, raw, today):
                x["date"] = today.strftime("%Y-%m-%d")
                rows.append(x)
        order = {"SWEEP": 0, "CLEAN": 1, "ENGULF": 2, "WICK": 3, "OBH": 4}
        rows.sort(key=lambda r: (min(order.get(p, 9) for p in r["patterns"].split("+")), -len(r["patterns"].split("+")), r["symbol"]))
        today_s = today.strftime("%Y-%m-%d")
        df = pd.DataFrame(rows, columns=COLS)
        os.makedirs(sd.RESULTS_DIR, exist_ok=True)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"daily_zone csv unreadable, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        _notify(rows)
        log.info(f"Daily zone: {len(rows)} stocks with a pattern at a daily zone ({len(cache)} candles checked)")
        try:
            sd._LAST.clear()
        except Exception:
            pass
    except Exception as e:
        log.warning(f"daily_zone run failed: {e}")


def _notify(rows):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and rows):
        return
    try:
        import requests
        lines = [f"{r['where'][:3]} {r['symbol']}  Rs {r['price']:.2f}  {r['patterns']}  zone {r['zone']}" for r in rows[:MAX_LINES]]
        extra = f"\n+{len(rows) - MAX_LINES} more on /daily_zone" if len(rows) > MAX_LINES else ""
        requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                      headers={"Title": f"Daily candle at daily zone ({len(rows)})", "Priority": "high", "Tags": "mag"})
    except Exception as e:
        log.warning(f"daily_zone ntfy failed: {e}")


def page_html():
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Daily candle at daily zone</title><style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}'
            'table{border-collapse:collapse;width:100%;background:#fff}th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}'
            'th{background:#222;color:#fff;position:sticky;top:0}td:nth-child(1),td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        rows, day = [], ""
        if os.path.exists(PATH):
            df = pd.read_csv(PATH).fillna("")
            if len(df):
                day = str(df["date"].iloc[-1])
                rows = df[df["date"].astype(str) == day].to_dict("records")

        def pc(v):
            v = float(v)
            return f'<span style="color:{"#0a7d33" if v > 0 else "#c62828" if v < 0 else "#555"}">{v:+.2f}%</span>'
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td><td style=\"color:{'#c62828' if r['where'] == 'RESISTANCE' else '#0a7d33'}\"><b>{r['where']}</b></td>"
            f"<td>{float(r['price']):.2f}</td><td>{pc(r['day_chg_pct'])}</td><td>{r['zone']}</td><td><b>{r['patterns'].replace('+', ' + ')}</b></td>"
            f"<td>{float(r['wick_pct']):.0f}%</td><td>{r['vol_x']}x</td><td>{r['vol_prev_x']}x</td></tr>" for r in rows) \
            or '<tr><td colspan="9" style="text-align:center">Nothing yet - the list is made once a day after 16:45 IST.</td></tr>'
        return (head + f'<h2>Daily candle at a daily support / resistance zone &mdash; {day or "-"} ({len(rows)})</h2>'
                '<div class="m">Every liquid stock whose finished DAILY candle is at a daily zone and shows: <b>SWEEP</b> (took out the 20-day high/low and closed back), '
                '<b>OBH</b> (order-block hit: went into the zone and closed on its own side), <b>WICK</b> (wick 70% or more, right colour), <b>CLEAN</b> (wick 70% or more, almost no wick on the other side, volume above the previous day), '
                '<b>ENGULF</b> (engulfing candle with volume above the previous day). Resistance needs a red candle, support a green one. Made once a day after 16:45 IST. Information only. '
                '<a href="/daily_zone.csv" target="_blank" rel="noopener">CSV (all days)</a> &middot; <a href="/spurt_daily" target="_blank" rel="noopener">Daily spurt page</a></div>'
                '<table><tr><th>Symbol</th><th>Zone type</th><th>Close</th><th>Day %</th><th>Daily zone</th><th>Patterns</th><th>Wick</th><th>Volume vs 50-day</th><th>Volume vs prev day</th></tr>'
                + body + '</table></body></html>')
    except Exception as e:
        log.warning(f"daily_zone page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
