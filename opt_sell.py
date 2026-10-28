"""
OPTION Spurt SELL / BUY tracker (read-only) - page /opt_sell, CSV /opt_sell.csv. Fed by opt_live.py (same 10-min candles of each option contract).

Same checklist as the stock Spurt SELL page, and its mirror for BUY, applied to each CE / PE contract's own price chart:
  SELL (price of the contract likely to fall)                      BUY (price of the contract likely to rise)
  1. GREEN 10-min candle with volume >= 5x the 20-candle average   1. RED 10-min candle with volume >= 5x the 20-candle average
  2. a RESISTANCE zone touched from the spurt to the signal        2. a SUPPORT zone touched from the spurt to the signal
  3. SOLID RED candle (body >= 60% of range) within 10 candles     3. SOLID GREEN candle (body >= 60% of range) within 10 candles
  4. that candle's volume > the candle before it                   4. that candle's volume > the candle before it
  5. RSI(14) above 70 on any candle from the spurt to the signal   5. RSI(14) below 30 on any candle from the spurt to the signal
Entry = close of the signal candle, stop loss = its high (SELL) / low (BUY), targets 1x and 2x the risk. Information only - no orders.
The tracker lists every contract that had a 10-min candle of 3x or more today and follows it for 10 candles.
"""
import os
import csv
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("opt_sell")
IST = ZoneInfo("Asia/Kolkata")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
TRACK_PATH = os.path.join(RESULTS_DIR, "opt_sell_track.json")
SIG_PATH = os.path.join(RESULTS_DIR, "opt_sell_history.csv")
AVG_LEN = 20
SELL_MULT = 5.0
TRACK_MULT = 3.0
WAIT = 10
BODY_PCT = 60.0
RSI_LEN = 14
RSI_HI, RSI_LO = 70.0, 30.0
NEAR_PCT = 0.5
SIG_COLS = ["date", "run", "side", "contract", "candle", "entry", "sl", "t1", "t2", "risk_pct", "spurt_candle", "spurt_mult",
            "candles_after_spurt", "red_vol_vs_prev", "zone", "rsi_extreme", "score", "sig_ohlc"]
_alerted = {"keys": set()}


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _rsi(close, n):
    d = pd.Series(close, dtype=float).diff()
    up = d.clip(lower=0).ewm(alpha=1.0 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1.0 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(100.0).to_numpy()


def _arr(df):
    return [df[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume")]


def _support_touch(df, start, end, tol_pct=NEAR_PCT):
    """Mirror of spurt_zones.resistance_touch: did price touch a live SUPPORT zone on any candle from start to end? 'lo-hi' text or None."""
    try:
        import spurt_zones
        high = df["high"].to_numpy(dtype=float)
        low = df["low"].to_numpy(dtype=float)
        tol = tol_pct / 100.0
        found = None
        for k in range(max(start, 0), end + 1):
            for z in spurt_zones.live_zones(df.iloc[:k + 1]):
                if z["type"] == "SUPPORT" and low[k] <= z["hi"] * (1 + tol) and high[k] >= z["lo"] * (1 - tol):
                    found = f"{z['lo']:g}-{z['hi']:g}"
        return found
    except Exception:
        return None


def _zone_touch(side, df, start, end):
    if side == "SELL":
        try:
            import spurt_zones
            return spurt_zones.resistance_touch(df, start, end, NEAR_PCT)
        except Exception:
            return None
    return _support_touch(df, start, end)


def check(df, contract, side):
    """Signal dict for the latest closed candle of one contract, or None. Never raises."""
    try:
        n = len(df)
        if n < AVG_LEN + 3:
            return None
        o, h, l, c, v = _arr(df)
        L = n - 1
        sell = side == "SELL"

        def solid(i):
            rng = h[i] - l[i]
            body = (o[i] - c[i]) if sell else (c[i] - o[i])
            return body > 0 and rng > 0 and body / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]

        if not solid(L):
            return None
        sj, smult = None, None
        for j in range(L - 1, max(L - WAIT, AVG_LEN) - 1, -1):
            avg = v[j - AVG_LEN:j].mean()
            if avg <= 0:
                continue
            m = v[j] / avg
            if m >= SELL_MULT and ((c[j] > o[j]) if sell else (c[j] < o[j])):
                sj, smult = j, m
                break
        if sj is None:
            return None
        for k in range(sj + 1, L):                                       # one signal per spurt
            if solid(k):
                return None
        entry = float(c[L])
        sl = float(h[L]) if sell else float(l[L])
        risk = (sl - entry) if sell else (entry - sl)
        if risk <= 0 or entry <= 0:
            return None
        zone = _zone_touch(side, df, sj, L)
        if not zone:
            return None
        r = _rsi(c, RSI_LEN)[sj:L + 1]
        ext = float(np.nanmax(r)) if sell else float(np.nanmin(r))
        if (sell and not ext > RSI_HI) or (not sell and not ext < RSI_LO):
            return None
        sgn = -1 if sell else 1
        return {
            "side": side, "contract": contract, "candle": _ist(df.index[L]).strftime("%H:%M"),
            "entry": round(entry, 2), "sl": round(sl, 2), "t1": round(entry + sgn * risk, 2), "t2": round(entry + sgn * 2 * risk, 2),
            "risk_pct": round(risk / entry * 100, 2), "spurt_candle": _ist(df.index[sj]).strftime("%H:%M"), "spurt_mult": round(float(smult), 1),
            "candles_after_spurt": int(L - sj), "red_vol_vs_prev": round(float(v[L] / v[L - 1]), 2) if v[L - 1] > 0 else None,
            "zone": zone, "rsi_extreme": round(ext, 1), "score": "5/5",
            "sig_ohlc": f"{o[L]:.2f}|{h[L]:.2f}|{l[L]:.2f}|{c[L]:.2f}",
        }
    except Exception:
        return None


def track(df, contract):
    """Today's latest spurt (>= 3x) of one contract and what the candles after it did. None if none today. Never raises."""
    try:
        n = len(df)
        if n < AVG_LEN + 2:
            return None
        o, h, l, c, v = _arr(df)
        L = n - 1
        loc = [_ist(t) for t in df.index]
        today = loc[L].date()
        avg = pd.Series(v).rolling(AVG_LEN).mean().shift(1).to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            mult = np.where(avg > 0, v / avg, 0.0)
        idx = [i for i in range(AVG_LEN, n) if loc[i].date() == today and mult[i] >= TRACK_MULT]
        if not idx:
            return None
        j = idx[-1]
        green = c[j] > o[j]
        if c[j] == o[j]:
            return None
        side = "SELL" if green else "BUY"

        def solid_opp(i):
            rng = h[i] - l[i]
            body = (o[i] - c[i]) if green else (c[i] - o[i])
            return body > 0 and rng > 0 and body / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]

        any_opp = any(((c[i] < o[i]) if green else (c[i] > o[i])) for i in range(j + 1, L + 1))
        solid_vol = any(solid_opp(i) for i in range(j + 1, L + 1))
        zone = None
        if L - j <= WAIT:
            zone = _zone_touch(side, df, j, L) or ""
        r = _rsi(c, RSI_LEN)[j:L + 1]
        colour = lambda i: "GREEN" if c[i] > o[i] else "RED" if c[i] < o[i] else "DOJI"
        return {
            "strip": [f"{o[i]:.2f}|{h[i]:.2f}|{l[i]:.2f}|{c[i]:.2f}" for i in range(j, min(L, j + WAIT) + 1)],
            "ohlc": f"{o[L]:.2f}|{h[L]:.2f}|{l[L]:.2f}|{c[L]:.2f}", "spurt_colour": colour(j),
            "first_after": colour(j + 1) if j + 1 <= L else "-", "latest_colour": colour(L),
            "off_high_pct": round(float((c[L] / (h[j:L + 1].max() if green else l[j:L + 1].min()) - 1) * 100), 2),
            "vol_vs_spurt": round(float(v[L] / v[j]), 2) if v[j] > 0 else None,
            "contract": contract, "side": side, "spurt_at": loc[j].strftime("%H:%M"), "spurt_mult": round(float(mult[j]), 1),
            "best_mult": round(float(max(mult[i] for i in idx)), 1), "since": int(L - j), "price": round(float(c[L]), 2),
            "latest_candle": loc[L].strftime("%H:%M"), "move_pct": round(float((c[L] / c[j] - 1) * 100), 2),
            "opp_any": bool(any_opp), "solid_vol": bool(solid_vol), "zone": zone,
            "rsi_ext": round(float(np.nanmax(r) if green else np.nanmin(r)), 1),
        }
    except Exception:
        return None


def update(trk_items, sigs, now):
    """Called once per scan cycle by opt_live. Never raises."""
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        day = now.strftime("%Y-%m-%d")
        data = {"date": day, "rows": {}, "signals": []}
        try:
            with open(TRACK_PATH) as fh:
                old = json.load(fh)
            if old.get("date") == day:
                data = old
        except Exception:
            pass
        stamp = now.strftime("%H:%M")
        for it in trk_items:
            if it:
                prev = data["rows"].get(it["contract"])
                if prev and prev.get("spurt_at") == it.get("spurt_at") and it.get("zone") is None and prev.get("zone") is not None:
                    it["zone"] = prev["zone"]
                it["seen"] = stamp
                data["rows"][it["contract"]] = it
        known = {(s["contract"], s["candle"], s["side"]) for s in data["signals"]}
        new = [s for s in sigs if (s["contract"], s["candle"], s["side"]) not in known]
        data["signals"].extend(new)
        data["updated"] = stamp + " IST"
        with open(TRACK_PATH + ".tmp", "w") as fh:
            json.dump(data, fh)
        os.replace(TRACK_PATH + ".tmp", TRACK_PATH)
        if new:
            _log_signals(new, day, now)
            _alert(new)
    except Exception as e:
        log.warning(f"Option sell/buy update failed: {e}")


def _log_signals(new, day, now):
    try:
        isnew = not os.path.exists(SIG_PATH)
        with open(SIG_PATH, "a", newline="") as f:
            w = csv.writer(f)
            if isnew:
                w.writerow(SIG_COLS)
            for s in new:
                w.writerow([day, now.strftime("%H:%M")] + [s.get(k) for k in SIG_COLS[2:]])
    except Exception as e:
        log.warning(f"Option sell/buy history not written: {e}")


def _alert(new):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    new = [s for s in new if (s["contract"], s["candle"], s["side"]) not in _alerted["keys"]]
    if not new:
        return
    lines = [f"{s['side']} {s['contract']}  entry {s['entry']:g}  SL {s['sl']:g}  T1 {s['t1']:g}" for s in new[:10]]
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                             headers={"Title": f"Option {new[0]['side']} signal after volume spurt ({new[0]['candle']} candle)", "Priority": "high",
                                      "Tags": "chart_with_downwards_trend" if new[0]["side"] == "SELL" else "chart_with_upwards_trend"})
        if resp.status_code < 300:
            for s in new:
                _alerted["keys"].add((s["contract"], s["candle"], s["side"]))
    except Exception as e:
        log.warning(f"Option sell/buy ntfy failed: {e}")


def _status(r, sig_side):
    side = r["side"]
    if r["contract"] in sig_side:
        ss = sig_side[r["contract"]]
        return f"<b style='color:{'#c62828' if ss == 'SELL' else '#0a7d33'}'>{ss} SIGNAL (below)</b>"
    if r["since"] == 0:
        return "spurt just happened"
    opp = "red" if side == "SELL" else "green"
    if r["solid_vol"]:
        miss = []
        if r["spurt_mult"] < SELL_MULT:
            miss.append(f"spurt below {SELL_MULT:g}x")
        if not r.get("zone"):
            miss.append("resistance zone" if side == "SELL" else "support zone")
        ok = r["rsi_ext"] > RSI_HI if side == "SELL" else r["rsi_ext"] < RSI_LO
        if not ok:
            miss.append("RSI above 70" if side == "SELL" else "RSI below 30")
        return f"solid {opp} + volume up seen" + (" - missing: " + ", ".join(miss) if miss else "")
    if r["since"] > WAIT:
        return f"window over - no solid {opp} candle with higher volume came"
    if r["opp_any"]:
        return f"{opp} candle seen but not solid / volume not higher ({r['since']}/{WAIT})"
    return f"waiting for {opp} candle ({r['since']}/{WAIT})"


# ------------------------------------------------------------------------------------------ page
G, R, M = "#0a7d33", "#c62828", "#555"
_live = {"t": 0.0, "d": {}}


def live_prices(rows):
    """{contract: last traded price now}, one batched Dhan call per segment, cached 15 s. Empty on any failure."""
    import time
    if time.time() - _live["t"] < 15:
        return _live["d"]
    out = {}
    try:
        import live_scanner as ls
        by = {}
        for r in rows:
            if r.get("sid") and r.get("seg"):
                by.setdefault(r["seg"], {})[str(r["sid"])] = r["contract"]
        for seg, m in by.items():
            resp = ls._dhan_post("marketfeed/ltp", {seg: [int(k) for k in m]}, "option live")
            for sid, q in ((resp.json().get("data") or {}).get(seg) or {}).items():
                try:
                    if str(sid) in m and isinstance(q, dict):
                        out[m[str(sid)]] = float(q.get("last_price"))
                except (TypeError, ValueError):
                    pass
    except Exception as e:
        log.warning(f"Option live prices failed: {e}")
    _live.update(t=time.time(), d=out)
    return out


def _score_pts(r):
    """Checklist points met so far (of 5): 1 spurt of 5x or more, 2 zone touched, 3+4 solid opposite candle with higher volume, 5 RSI extreme."""
    side = r["side"]
    rsi_ok = r["rsi_ext"] > RSI_HI if side == "SELL" else r["rsi_ext"] < RSI_LO
    return (1 if r["spurt_mult"] >= SELL_MULT else 0) + (1 if r.get("zone") else 0) + (2 if r["solid_vol"] else 0) + (1 if rsi_ok else 0)


def page_html(date=None):
    import html as _h
    try:
        import spurt_sell_x as sx
        draw, strip_svg = sx._draw, sx._strip_svg
    except Exception:
        draw = strip_svg = lambda *a, **k: "-"
    try:
        try:
            import spurt_sell_live as sl
            import opt_live
            banner, sort_js = opt_live._banner(), sl._SORT_JS
        except Exception:
            banner, sort_js = "", ""
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<meta http-equiv="refresh" content="60"><title>Option Spurt SELL / BUY</title>'
                '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;background:#fff}'
                'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}'
                'td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>' + banner)
        rule = (f"Rule: SELL = a green 10-min candle of the option contract with volume &ge; {SELL_MULT:g}x the average of the previous {AVG_LEN} candles, then within {WAIT} candles "
                f"a solid red candle (body &ge; {BODY_PCT:g}% of its range) with higher volume than the candle before it, a resistance zone touched since the spurt, and RSI above {RSI_HI:g} "
                f"on the spurt or any candle since. SELL at that red candle's close, stop loss = its high, targets 1x and 2x the risk. "
                f"BUY = the mirror: red spurt, solid green candle with higher volume, support zone touched, RSI below {RSI_LO:g}; stop loss = its low. "
                "One signal per spurt. Checklist is out of 5 (no higher-time-frame zone for options). Information only - the scanner places no orders.")
        today = datetime.now(IST).strftime("%Y-%m-%d")
        try:
            with open(TRACK_PATH) as f:
                td = json.load(f)
            if td.get("date") != today:
                td = {"rows": {}, "signals": []}
        except Exception:
            td = {"rows": {}, "signals": []}
        sig_side = {s["contract"]: s["side"] for s in td.get("signals", [])}
        cc = lambda t: G if t == "GREEN" else R if t == "RED" else M
        pc = lambda v: "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'
        trk = ""
        if date in (None, "", today) and td.get("rows"):
            rows = sorted(td["rows"].values(), key=lambda r: r["spurt_at"], reverse=True)
            lp = live_prices(rows)
            body = ""
            for i, r in enumerate(rows, 1):
                side = r["side"]
                rsi_ok = r["rsi_ext"] > RSI_HI if side == "SELL" else r["rsi_ext"] < RSI_LO
                v = lp.get(r["contract"])
                live = "-" if v is None else f"<b style='color:{G if v > r['price'] else R if v < r['price'] else M}'>{v:g}</b>"
                body += (f"<tr><td>{i}</td><td><b>{_h.escape(r['contract'])}</b></td><td>{r['spurt_at']}</td><td><b>{r['spurt_mult']:g}x</b></td><td>{r['best_mult']:g}x</td>"
                         f"<td style='color:{cc(r['spurt_colour'])}'><b>{r['spurt_colour']}</b></td><td><b>{side}?</b></td><td>{r['since']}</td>"
                         f"<td style='color:{cc(r['first_after'])}'><b>{r['first_after']}</b></td>"
                         f"<td style='white-space:nowrap'>{draw(r['ohlc'])} <b style='color:{cc(r['latest_colour'])}'>{r['latest_colour']}</b></td>"
                         f"<td style='white-space:nowrap;text-align:left'>{strip_svg(r.get('strip') or [])}</td>"
                         f"<td>{pc(r.get('move_pct'))}</td><td>{pc(r.get('off_high_pct'))}</td><td>{'-' if r.get('vol_vs_spurt') is None else format(r['vol_vs_spurt'], 'g') + 'x'}</td>"
                         f"<td>{'yes' if r['solid_vol'] else '-'}</td><td>{_h.escape(str(r['zone'])) if r.get('zone') else '-'}</td>"
                         f"<td style='color:{G if rsi_ok else M}'>{r['rsi_ext']:g}{' &#10004;' if rsi_ok else ''}</td><td><b>{5 if r['contract'] in sig_side else _score_pts(r)}/5</b></td>"
                         f"<td>{r['price']:g}</td><td>{live}</td><td style='text-align:left'>{_status(r, sig_side)}</td><td>{r.get('seen', '')}</td></tr>")
            trk = (f'<h3 style="margin:18px 0 4px">All option spurts today (&ge; {TRACK_MULT:g}x) &mdash; {len(rows)} &middot; updated {td.get("updated", "")}</h3>'
                   '<div class="m">Every contract with a 10-min candle of at least that volume multiple today, newest first, followed for 10 candles. SELL? = green spurt (waiting for a red candle), BUY? = red spurt (waiting for a green candle).</div>'
                   '<table class="s"><tr><th>#</th><th>Contract</th><th>Spurt at</th><th>Spurt x</th><th>Best x</th><th>Spurt candle</th><th>Looking for</th><th>Since</th><th>1st candle after</th>'
                   '<th>Latest</th><th>Spurt (dot) + next candles</th><th>Move</th><th>From extreme</th><th>Vol vs spurt</th><th>Solid candle + vol up</th><th>Zone touched</th><th>RSI</th>'
                   '<th>Checklist</th><th>Close</th><th>Live</th><th>Status</th><th>Seen</th></tr>' + body + '</table>')
        elif date in (None, "", today):
            trk = (f'<h3 style="margin:18px 0 4px">All option spurts today (&ge; {TRACK_MULT:g}x)</h3><div class="m">No option contract has had a 10-min candle of '
                   f'{TRACK_MULT:g}x volume or more yet today. Every one that reaches it is listed here and followed for {WAIT} candles. The {SELL_MULT:g}x in the rule is the level needed for a signal.</div>')
        df = pd.read_csv(SIG_PATH) if os.path.exists(SIG_PATH) else pd.DataFrame(columns=SIG_COLS)
        for c_ in SIG_COLS:
            if c_ not in df.columns:
                df[c_] = ""
        dates = sorted(df["date"].unique(), reverse=True) if len(df) else []
        d = date if date in dates else (dates[0] if dates else "")
        x = df[df["date"] == d].sort_values(["candle", "spurt_mult"], ascending=[False, False]) if d else df

        def sig_table(side):
            xs = x[x["side"] == side]
            if not len(xs):
                return f'<div class="m">No {side} signal yet.</div>'
            rws = "".join(
                f"<tr><td>{r.candle}</td><td><b>{_h.escape(str(r.contract))}</b></td><td>{r.entry:.2f}</td><td style='color:{R}'>{r.sl:.2f}</td>"
                f"<td style='color:{G}'>{r.t1:.2f}</td><td style='color:{G}'>{r.t2:.2f}</td><td>{r.risk_pct:g}%</td><td>{r.spurt_candle}</td><td><b>{r.spurt_mult:g}x</b></td>"
                f"<td>{int(r.candles_after_spurt)}</td><td>{_h.escape(str(r.zone))}</td><td style='white-space:nowrap;text-align:center'>{draw(r.sig_ohlc)}</td>"
                f"<td>{r.rsi_extreme}</td><td><b>{_h.escape(str(r.score))}</b></td></tr>" for r in xs.itertuples())
            return ('<table class="s"><tr><th>Signal candle</th><th>Contract</th><th>' + side + ' entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Risk %</th>'
                    '<th>Spurt candle</th><th>Spurt multiple</th><th>Candles after spurt</th><th>Zone touched</th><th>Signal<br>candle</th><th>RSI extreme</th><th>Checklist</th></tr>' + rws + '</table>')
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        return (head + f'<h2>Option contracts &mdash; Spurt SELL / BUY{" &mdash; " + _h.escape(d) if d else ""}</h2>'
                + (f'<form method="get" class="m">Day <select name="date">{opts}</select> <button>Show</button> &middot; {len(x)} signals &middot; ' if dates else '<div class="m">')
                + 'click any heading to sort &middot; <a href="/opt_sell.csv">download CSV (all days)</a> &middot; <a href="/opt_spurt">live /opt_spurt</a>' + ('</form>' if dates else '</div>')
                + f'<div class="m">{rule}</div>' + trk
                + '<h3 style="margin:18px 0 4px">SELL signals (all checklist points met)</h3>' + sig_table("SELL")
                + '<h3 style="margin:18px 0 4px">BUY signals (all checklist points met)</h3>' + sig_table("BUY") + sort_js + '</body></html>')
    except Exception as e:
        import html as _hh
        return f"<p>Option Spurt SELL / BUY page error: {_hh.escape(str(e))}</p>"
