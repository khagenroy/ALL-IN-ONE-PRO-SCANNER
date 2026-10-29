"""
SPURT SELL for COMMODITY (MCX) and CRYPTO (Delta)  -  the same idea as spurt_sell_live.py (stocks), one shared module.

Pages:  /mcx_sell   (csv /mcx_sell.csv)      /crypto_sell   (csv /crypto_sell.csv)

1. TRACKER   every symbol that had a candle with volume >= TRACK_MULT x the average of the previous 50 candles today (a "spurt"), kept all day:
             spurt time and multiple, colour of the spurt candle, the first candle after it, the latest candle (drawn), RSI peak, status.
2. SELL      the same checklist as the stock page and the TradingView "VS SELL CHK" indicator:
             1 a GREEN candle with volume >= SELL_MULT x average     (required)
             2 price touched a RESISTANCE zone since the spurt        (required)
             3 a solid RED candle (body >= 60% of its range) within 10 candles after the spurt   (required)
             4 that red candle's volume is higher than the candle before it                      (required)
             5 the higher-time-frame resistance zone also touched (commodity 30m, crypto 1H)   (bonus, shown in the score)
             6 RSI(14) above 70 on the spurt candle or any candle since                           (required)
             SELL = close of the red candle, stop loss = its high, targets 1x and 2x the risk. One signal per spurt.

Read-only: no orders. Any error is caught and logged; nothing here can stop the commodity / crypto scan.
All numbers are code defaults (change CFG below); no Render env variable is needed.
"""

import os
import json
import logging
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("spurt_sell_x")
IST = ZoneInfo("Asia/Kolkata")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

AVG_LEN = 50          # average volume of the previous N candles
WAIT = 10             # the red candle must come within N candles after the spurt
BODY_PCT = 60.0       # "solid" candle: body >= this % of its high-low range
RSI_LEN = 14
RSI_MIN = 70.0
NEAR_PCT = 0.5        # "at zone" tolerance %
R1, R2 = 1.0, 2.0

CFG = {
    "mcx": {"title": "Commodity (MCX)", "tf": "15m", "htf": "30m", "sell_mult": 5.0, "track_mult": 3.0, "layout": "gvpsrNIE",
            "tz_min": 330, "tz_name": "IST", "file": "mcx_sell_history.csv", "track": "mcx_sell_track.json",
            "page": "/mcx_sell", "csv": "/mcx_sell.csv", "home": "/commodity"},
    "crypto": {"title": "Crypto (Delta)", "tf": "30m", "htf": "1H", "sell_mult": 5.0, "track_mult": 3.0, "layout": "K1UYU3zl",
               "tz_min": 0, "tz_name": "UTC", "file": "crypto_sell_history.csv", "track": "crypto_sell_track.json",
               "page": "/crypto_sell", "csv": "/crypto_sell.csv", "home": "/crypto"},
}
COLS = ["date", "run", "candle", "symbol", "entry", "sl", "t1", "t2", "risk_pct", "spurt_candle", "spurt_mult", "candles_after_spurt",
        "red_vol_vs_prev", "res_zone", "rsi_peak", "htf_zone", "score", "sig_ohlc"]

_TV = {"mcx": {"CRUDEOILM": "MCX:CRUDEOILM1!", "NATURALGASM": "MCX:NATGASMINI1!", "GOLDM": "MCX:GOLDM1!", "SILVERM": "MCX:SILVERM1!"},
       "crypto": {"BTCUSD": "BINANCE:BTCUSDT.P", "ETHUSD": "BINANCE:ETHUSDT.P", "SOLUSD": "BINANCE:SOLUSDT.P",
                  "XAUTUSD": "BYBIT:XAUTUSDT.P", "SLVONUSD": "DELTAIN:SLVONUSD.P"}}
G, R, M = "#089981", "#F23645", "#9aa0a6"


def _local(ts, key):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=CFG[key]["tz_min"])


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _rsi(close, n):
    d = pd.Series(close, dtype=float).diff()
    up = d.clip(lower=0).ewm(alpha=1.0 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1.0 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(100.0).to_numpy()


def _arrays(df):
    return [df[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume")]


# ----------------------------------------------------------------------------- candle drawing (same look as the scanner pages)
def _candle_svg(o, h, l, c):
    try:
        o, h, l, c = float(o), float(h), float(l), float(c)
    except Exception:
        return "-"
    col = G if c > o else R if c < o else "#888"
    rng = (h - l) or 1e-9
    y = lambda v: 2 + (h - v) / rng * 32
    top, bot = min(y(o), y(c)), max(y(o), y(c))
    fm = (lambda v: f"{v:.2f}") if h >= 1 else (lambda v: f"{v:.5f}")
    svg = (f'<svg width="22" height="40" viewBox="0 0 22 36" preserveAspectRatio="none" style="vertical-align:middle">'
           f'<line x1="11" y1="{y(h):.1f}" x2="11" y2="{y(l):.1f}" stroke="{col}" stroke-width="2"/>'
           f'<rect x="5" y="{top:.1f}" width="12" height="{max(bot - top, 2):.1f}" fill="{col}" stroke="{col}"/></svg>')
    return f'<span title="O {fm(o)}  H {fm(h)}  L {fm(l)}  C {fm(c)}" style="white-space:nowrap">{svg}</span>'


def _draw(txt):
    try:
        p = [float(x) for x in str(txt).split("|")]
        return _candle_svg(p[0], p[1], p[2], p[3])
    except Exception:
        return "-"


def _link(key, sym):
    t = _TV[key].get(sym, sym)
    u = f"https://www.tradingview.com/chart/{CFG[key]['layout']}/?symbol=" + urllib.parse.quote(t, safe=":.")
    return f'<a href="{u}" target="_blank" rel="noopener" title="open chart in TradingView" style="color:inherit;text-decoration:none;border-bottom:1px dotted #888"><b>{sym}</b></a>'


# ----------------------------------------------------------------------------- the checklist rule
def _htf_touch(htf_df, hi, lo, sj, L):
    """Point 5: did price touch a live higher-time-frame RESISTANCE zone on any candle from the spurt to the red candle? 'lo-hi' text or ''."""
    try:
        if htf_df is None or len(htf_df) < 30:
            return ""
        import spurt_zones
        zs = [z for z in spurt_zones.live_zones(htf_df) if z["type"] == "RESISTANCE"]
        tol = NEAR_PCT / 100.0
        hit = ""
        for k in range(sj, L + 1):
            for z in zs:
                if lo[k] <= z["hi"] * (1 + tol) and hi[k] >= z["lo"] * (1 - tol):
                    hit = f"{z['lo']:g}-{z['hi']:g}"
        return hit
    except Exception:
        return ""


def check(key, df, sym, htf_df=None):
    """df = closed candles of the scan time frame (last row = latest closed candle). A signal dict or None. Never raises."""
    try:
        cfg = CFG[key]
        n = len(df)
        if n < AVG_LEN + 3:
            return None
        o, h, l, c, v = _arrays(df)
        L = n - 1

        def red(i):
            rng = h[i] - l[i]
            return c[i] < o[i] and rng > 0 and (o[i] - c[i]) / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]

        if not red(L):
            return None
        sj, smult = None, None
        for j in range(L - 1, max(L - WAIT, AVG_LEN) - 1, -1):
            avg = v[j - AVG_LEN:j].mean()
            if avg <= 0:
                continue
            m = v[j] / avg
            if m >= cfg["sell_mult"] and c[j] > o[j]:
                sj, smult = j, m
                break
        if sj is None:
            return None
        for k in range(sj + 1, L):                                       # one signal per spurt
            if red(k):
                return None
        entry, sl = float(c[L]), float(h[L])
        risk = sl - entry
        if risk <= 0 or entry <= 0:
            return None
        import spurt_zones
        zone_txt = spurt_zones.resistance_touch(df, sj, L, NEAR_PCT) or ""
        if not zone_txt:
            return None
        rsi_peak = float(np.nanmax(_rsi(c, RSI_LEN)[sj:L + 1]))
        if not rsi_peak > RSI_MIN:
            return None
        htf_txt = _htf_touch(htf_df, h, l, sj, L)
        score = 5 + (1 if htf_txt else 0)                                # points 1,2,3,4,6 are met
        rngs = h[max(L - AVG_LEN, 0):L] - l[max(L - AVG_LEN, 0):L]
        return {
            "symbol": sym, "candle": _local(df.index[L], key).strftime("%H:%M"),
            "entry": round(entry, 4), "sl": round(sl, 4), "t1": round(entry - risk * R1, 4), "t2": round(entry - risk * R2, 4),
            "risk_pct": round(risk / entry * 100, 2), "spurt_candle": _local(df.index[sj], key).strftime("%H:%M"),
            "spurt_mult": round(float(smult), 1), "candles_after_spurt": int(L - sj),
            "red_vol_vs_prev": round(float(v[L] / v[L - 1]), 2) if v[L - 1] > 0 else None,
            "res_zone": zone_txt, "rsi_peak": round(rsi_peak, 1), "htf_zone": htf_txt, "score": f"{score}/6",
            "sig_ohlc": f"{o[L]:.4f}|{h[L]:.4f}|{l[L]:.4f}|{c[L]:.4f}",
        }
    except Exception:
        return None


# ----------------------------------------------------------------------------- tracker (every spurt of the day)
def track(key, df, sym, htf_df=None):
    """Today's latest spurt (>= track_mult x) of one symbol and what the candles after it did. None when no spurt today. Never raises."""
    try:
        cfg = CFG[key]
        n = len(df)
        if n < AVG_LEN + 2:
            return None
        o, h, l, c, v = _arrays(df)
        L = n - 1
        loc = [_local(t, key) for t in df.index]
        today = loc[L].date()
        avg = pd.Series(v).rolling(AVG_LEN).mean().shift(1).to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            mult = np.where(avg > 0, v / avg, 0.0)
        idx = [i for i in range(AVG_LEN, n) if loc[i].date() == today and mult[i] >= cfg["track_mult"]]
        if not idx:
            return None
        j = idx[-1]
        colour = lambda i: "GREEN" if c[i] > o[i] else "RED" if c[i] < o[i] else "DOJI"
        red_solid_vol = any(c[i] < o[i] and h[i] > l[i] and (o[i] - c[i]) / (h[i] - l[i]) * 100.0 >= BODY_PCT and v[i] > v[i - 1]
                            for i in range(j + 1, L + 1))
        red_any = any(c[i] < o[i] for i in range(j + 1, L + 1))
        res_zone = htf_zone = None                                    # zone checks only while the sell window is open; older rows keep their last value
        if colour(j) == "GREEN" and L - j <= WAIT:
            try:
                import spurt_zones
                res_zone = spurt_zones.resistance_touch(df, j, L, NEAR_PCT) or ""
            except Exception:
                res_zone = None
            htf_zone = _htf_touch(htf_df, h, l, j, L)
        return {
            "res_zone": res_zone, "htf_zone": htf_zone, "red_any": bool(red_any),
            "move_pct": round(float((c[L] / c[j] - 1) * 100), 2), "off_high_pct": round(float((c[L] / h[j:L + 1].max() - 1) * 100), 2),
            "vol_vs_spurt": round(float(v[L] / v[j]), 2) if v[j] > 0 else None,
            "strip": [f"{o[i]:.4f}|{h[i]:.4f}|{l[i]:.4f}|{c[i]:.4f}" for i in range(max(j - PRE_CANDLES, 0), min(L, j + WAIT) + 1)],
            "pre": int(j - max(j - PRE_CANDLES, 0)),
            "symbol": sym, "price": round(float(c[L]), 4), "spurt_at": loc[j].strftime("%H:%M"), "spurt_mult": round(float(mult[j]), 1),
            "best_mult": round(float(max(mult[i] for i in idx)), 1), "spurt_colour": colour(j), "since": int(L - j),
            "first_after": colour(j + 1) if j + 1 <= L else "-", "latest_candle": loc[L].strftime("%H:%M"), "latest_colour": colour(L),
            "ohlc": f"{o[L]:.4f}|{h[L]:.4f}|{l[L]:.4f}|{c[L]:.4f}", "red_solid_vol": bool(red_solid_vol),
            "rsi_peak": round(float(np.nanmax(_rsi(c, RSI_LEN)[j:L + 1])), 1),
        }
    except Exception:
        return None


PRE_CANDLES = 10      # candles BEFORE the spurt candle shown in the strip (dimmed), so the strip is 10 before + the spurt + up to 10 after


def _strip_svg(strip, tag=0, pre=0):
    """The 10 candles before the spurt (dimmed), the spurt candle and the candles after it (up to 10) side by side on one shared price scale, so sizes can be compared. A dot marks the spurt candle."""
    try:
        cs = [[float(x) for x in str(t).split("|")[:4]] for t in strip]
        if not cs:
            return "-"
        hi = max(c[1] for c in cs)
        lo = min(c[2] for c in cs)
        rng = (hi - lo) or 1e-9
        y = lambda v: 3 + (hi - v) / rng * 58
        w = 13
        parts = []
        for i, (o, h, l, c) in enumerate(cs):
            col = G if c > o else R if c < o else "#888"
            x = i * w + w / 2
            op = ' opacity="0.55"' if i < pre else ""
            top, bot = min(y(o), y(c)), max(y(o), y(c))
            parts.append(f'<line x1="{x:.1f}" y1="{y(h):.1f}" x2="{x:.1f}" y2="{y(l):.1f}" stroke="{col}" stroke-width="1.5"{op}/>'
                         f'<rect x="{x - 4:.1f}" y="{top:.1f}" width="8" height="{max(bot - top, 1.5):.1f}" fill="{col}"{op}/>')
        parts.append(f'<circle cx="{pre * w + w / 2:.1f}" cy="68" r="2.4" fill="#d29922"/>')
        title = "10 candles before the spurt (dim), the spurt candle (dot) and the candles after it"
        return f'<svg width="{len(cs) * w}" height="72" viewBox="0 0 {len(cs) * w} 72" style="vertical-align:middle"><title>{title}</title>' + "".join(parts) + "</svg>"
    except Exception:
        return "-"


# ----------------------------------------------------------------------------- live price (read when the page is opened, cached 15 s)
_live_cache = {}


def live_prices(key, syms):
    """{symbol: last traded price} for the symbols of a tracker table, fetched when the page is opened (Dhan marketfeed/ltp for MCX and NSE,
    Delta public ticker for crypto). Cached 15 seconds. Any failure gives an empty result and the page shows a dash. Never raises."""
    import time
    c = _live_cache.get(key)
    if c and time.time() - c[0] < 15:
        return c[1]
    out = {}
    try:
        if key == "crypto":
            import crypto_live as cl
            for s in syms:
                try:
                    r = (cl._get(f"/v2/tickers/{s}", None, f"live {s}") or {}).get("result") or {}
                    v = r.get("close") or r.get("mark_price")
                    if v is not None:
                        out[s] = float(v)
                except Exception:
                    pass
        else:
            import live_scanner as ls
            ids, seg = {}, "NSE_EQ"
            if key == "mcx":
                import mcx_live as ml
                seg = "MCX_COMM"
                try:
                    ml._resolve_ids()
                except Exception:
                    pass
                ids = {s: str(ml._ids[s]) for s in syms if ml._ids.get(s)}
            else:
                import scrip_master as sm
                for s in syms:
                    try:
                        sid, sg = sm.get_security_id_and_segment(s)
                        if sid and sg == "NSE_EQ":
                            ids[s] = str(sid)
                    except Exception:
                        pass
            if ids:
                resp = ls._dhan_post("marketfeed/ltp", {seg: [int(v) for v in ids.values()]}, "live price")
                data = (resp.json().get("data") or {}).get(seg) or {}
                by = {v: k for k, v in ids.items()}
                for sid, q in data.items():
                    sym = by.get(str(sid))
                    try:
                        if sym and isinstance(q, dict):
                            out[sym] = float(q.get("last_price"))
                    except (TypeError, ValueError):
                        pass
    except Exception as e:
        log.warning(f"live price fetch failed: {e}")
    _live_cache[key] = (time.time(), out)
    return out


def _live_cell(lp, sym, close):
    v = lp.get(sym)
    if v is None:
        return "-"
    c = "#089981" if v > close else "#F23645" if v < close else "#888"
    return f"<b style='color:{c}'>{v:g}</b>"


def _status(r, need_mult, is_signal, red_col):
    """Plain-words status of a tracked spurt. 'Red candle' in the rule means a SOLID red candle (body >= 60% of its range) with higher volume than the candle before it."""
    if is_signal:
        return f"<b style='color:{red_col}'>SELL SIGNAL (below)</b>"
    if r["spurt_colour"] != "GREEN":
        return "red spurt - no sell setup"
    if r["since"] == 0:
        return "spurt just happened"
    if r.get("red_solid_vol"):
        miss = []
        if r.get("spurt_mult", 0) < need_mult:
            miss.append(f"spurt below {need_mult:g}x")
        if not r.get("res_zone"):
            miss.append("resistance zone")
        if not r.get("rsi_peak", 0) > RSI_MIN:
            miss.append(f"RSI above {RSI_MIN:g}")
        return "solid red + volume up seen" + (" - missing: " + ", ".join(miss) if miss else "")
    if r["since"] > WAIT:
        return "window over - no solid red candle with higher volume came"
    if r.get("red_any"):
        return f"red candle seen but not solid / volume not higher ({r['since']}/{WAIT})"
    return f"waiting for red candle ({r['since']}/{WAIT})"


def _trk_score(key, r):
    """Checklist points met so far: 1 green spurt (>= the SELL multiple), 2 resistance zone, 3+4 solid red candle with higher volume, 5 higher-time-frame zone, 6 RSI above the level."""
    return (1 if (r.get("spurt_colour") == "GREEN" and r.get("spurt_mult", 0) >= CFG[key]["sell_mult"]) else 0) + (1 if r.get("res_zone") else 0) \
        + (2 if r.get("red_solid_vol") else 0) + (1 if r.get("htf_zone") else 0) + (1 if r.get("rsi_peak", 0) > RSI_MIN else 0)


# ----------------------------------------------------------------------------- one cycle: save tracker + log new signals + phone alert
def update(key, trk_items, sells, now_ist):
    """Called once per scan cycle. Never raises."""
    try:
        cfg = CFG[key]
        os.makedirs(RESULTS_DIR, exist_ok=True)
        day = (datetime.now(ZoneInfo("UTC")) + pd.Timedelta(minutes=cfg["tz_min"])).strftime("%Y-%m-%d")      # the market's own day (MCX IST day, crypto UTC day)
        path = os.path.join(RESULTS_DIR, cfg["track"])
        data = {"date": day, "rows": {}}
        try:
            with open(path) as fh:
                old = json.load(fh)
            if old.get("date") == day:
                data = old
        except Exception:
            pass
        stamp = now_ist.strftime("%H:%M")
        for it in trk_items:
            if it:
                it["seen"] = stamp
                prev = data["rows"].get(it["symbol"])
                if prev and prev.get("spurt_at") == it.get("spurt_at"):      # same spurt: keep the zone results found while the window was open
                    for k in ("res_zone", "htf_zone"):
                        if it.get(k) is None and prev.get(k) is not None:
                            it[k] = prev[k]
                data["rows"][it["symbol"]] = it
        data["updated"] = stamp + " IST"
        with open(path + ".tmp", "w") as fh:
            json.dump(data, fh)
        os.replace(path + ".tmp", path)
        if sells:
            _log_signals(key, sells, day, now_ist)
    except Exception as e:
        log.warning(f"SpurtSellX {key} update failed: {e}")


def _log_signals(key, sells, day, now_ist):
    cfg = CFG[key]
    path = os.path.join(RESULTS_DIR, cfg["file"])
    seen = set()
    if os.path.exists(path):
        with open(path) as fh:
            first = fh.readline().strip().split(",")
        if first != COLS:
            pd.read_csv(path).reindex(columns=COLS).to_csv(path + ".tmp", index=False)
            os.replace(path + ".tmp", path)
        old = pd.read_csv(path, usecols=["date", "candle", "symbol"], dtype=str)
        seen = {(c_, s_) for d_, c_, s_ in old[old["date"] == day].itertuples(index=False)}
    new = [r for r in sells if (r["candle"], r["symbol"]) not in seen]
    if not new:
        return
    run = now_ist.strftime("%H:%M:%S")
    pd.DataFrame([{"date": day, "run": run, **{c_: r.get(c_) for c_ in COLS[2:]}} for r in new], columns=COLS).to_csv(
        path, mode="a", header=not os.path.exists(path), index=False)
    log.info(f"SpurtSellX {key}: {len(new)} new SELL signal(s): {[r['symbol'] for r in new]}")
    topic = os.environ.get("NTFY_TOPIC", "").strip()          # the scanner's existing phone-alert topic (no new variable)
    if not topic:
        return
    try:
        import requests
        lines = [f"{r['symbol']}  SELL {r['entry']:g}  SL {r['sl']:g}  T1 {r['t1']:g}  T2 {r['t2']:g}  (spurt {r['spurt_mult']:g}x at {r['spurt_candle']}; RSI {r['rsi_peak']}; checklist {r['score']})"
                 for r in new[:12]]
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"{cfg['title']} SELL after volume spurt ({new[0]['candle']} candle)", "Priority": "high", "Tags": "chart_with_downwards_trend"})
    except Exception as e:
        log.warning(f"SpurtSellX {key} alert failed: {e}")


# ----------------------------------------------------------------------------- page
_RULES_BANNER = ('<style>@keyframes rb{0%,49%{background:#fff176;color:#d50000}50%,100%{background:#d50000;color:#fff}}</style>'
                 '<div style="animation:rb 1s step-end infinite;position:sticky;top:0;z-index:99999;background:#fff176;border:3px solid #d50000;border-radius:6px;'
                 'padding:10px 14px;margin:0 0 14px;color:#d50000;font-weight:900;font-size:22px;line-height:1.5;text-align:center">'
                 '1. DON&#39;T RUSH&nbsp;&nbsp;|&nbsp;&nbsp; 2. DON&#39;T TRADE WITHOUT VALID SET UP&nbsp;&nbsp;|&nbsp;&nbsp; 3. 1 VALID SETUP ENOUGH TO EARN MONEY&nbsp;&nbsp;|&nbsp;&nbsp; 4. DON&#39;T BE EMOTIONAL ATTACHED</div>')



def page_html(key, date=None):
    import html as _h
    cfg = CFG[key]
    try:
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<meta http-equiv="refresh" content="60"><title>{cfg["title"]} Spurt SELL</title>'
                '<style>body{background:#0e1117;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:24px}'
                'table{border-collapse:collapse;width:100%;font-size:13px;margin-bottom:8px}th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #262b36}'
                'th{background:#161b22;color:#9aa0a6;font-weight:600;cursor:pointer}tr:hover{background:#161b22}a{color:#58a6ff}.m{color:#9aa0a6;font-size:13px;margin:6px 0 12px}'
                'h2{margin:24px 0 4px}</style></head><body>' + _RULES_BANNER)
        sort_js = """<script>
document.querySelectorAll('table.s th').forEach(function(th,i){th.title='click to sort';
th.addEventListener('click',function(){var t=th.closest('table'),b=t.tBodies[0],rows=Array.prototype.slice.call(b.rows),
asc=th.dataset.asc!=='1';th.dataset.asc=asc?'1':'0';
function val(r){var x=r.cells[i].innerText.replace(/[,%x]/g,'').trim();var n=parseFloat(x);return isNaN(n)?x.toLowerCase():n;}
rows.sort(function(a,b2){var p=val(a),q=val(b2);if(p<q)return asc?-1:1;if(p>q)return asc?1:-1;return 0;});
rows.forEach(function(r){b.appendChild(r);});});});
(function(){function fx(){var bn=document.querySelector('div[style*="rb 1s"]');var h=bn?bn.offsetHeight:0;document.querySelectorAll('table.s th').forEach(function(th){th.style.position='sticky';th.style.top=(h-3)+'px';th.style.zIndex=5;});}fx();window.addEventListener('resize',fx);})();
</script>"""
        rule = (f"Rule (same as the stock page): a green {cfg['tf']} candle with volume &ge; {cfg['sell_mult']:g}x the average of the previous {AVG_LEN} candles, then within {WAIT} candles "
                f"a solid red candle (body &ge; {BODY_PCT:g}% of its range) with higher volume than the candle before it, price touching a resistance zone since the spurt, and RSI above {RSI_MIN:g} "
                f"on the spurt or any candle since. The {cfg['htf']} resistance zone is a bonus (score 6/6). SELL at the red candle's close, stop loss = its high, targets {R1:g}x and {R2:g}x the risk. "
                f"Day = {cfg['tz_name']} day. Information only - no orders.")
        cc = lambda t: G if t == "GREEN" else R if t == "RED" else M
        # signals
        path = os.path.join(RESULTS_DIR, cfg["file"])
        df = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame(columns=COLS)
        for c_ in COLS:
            if c_ not in df.columns:
                df[c_] = ""
        sig_syms = set(df["symbol"].astype(str)) if len(df) else set()
        dates = sorted(df["date"].unique(), reverse=True) if len(df) else []
        # tracker (today's saved file)
        trk_html = ""
        try:
            with open(os.path.join(RESULTS_DIR, cfg["track"])) as fh:
                td = json.load(fh)
            if date in (None, "", td.get("date")):
                rows = sorted(td.get("rows", {}).values(), key=lambda r: r["spurt_at"], reverse=True)
                sig_today = set(df[df["date"] == td.get("date")]["symbol"].astype(str)) if len(df) else set()
                body = ""
                lp = live_prices(key, [r["symbol"] for r in rows])
                for i, r in enumerate(rows, 1):
                    status = _status(r, cfg["sell_mult"], r["symbol"] in sig_today, R)
                    zt = lambda z: "-" if z in (None, "") else _h.escape(str(z))
                    pc = lambda v: "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'
                    rsi_ok = r["rsi_peak"] > RSI_MIN
                    body += (f"<tr><td>{i}</td><td>{_link(key, r['symbol'])}</td><td>{r['spurt_at']}</td><td><b>{r['spurt_mult']:g}x</b></td><td>{r['best_mult']:g}x</td>"
                             f"<td style='color:{cc(r['spurt_colour'])}'><b>{r['spurt_colour']}</b></td><td>{r['since']}</td>"
                             f"<td style='color:{cc(r['first_after'])}'><b>{r['first_after']}</b></td>"
                             f"<td style='white-space:nowrap'>{_draw(r['ohlc'])} <b style='color:{cc(r['latest_colour'])}'>{r['latest_colour']}</b></td>"
                             f"<td class='st' style='white-space:nowrap'>{_strip_svg(r.get('strip') or [], 0, int(r.get('pre') or 0))}</td>"
                             f"<td>{pc(r.get('move_pct'))}</td><td>{pc(r.get('off_high_pct'))}</td><td>{'-' if r.get('vol_vs_spurt') is None else format(r['vol_vs_spurt'], 'g') + 'x'}</td>"
                             f"<td>{'yes' if r['red_solid_vol'] else '-'}</td><td>{zt(r.get('res_zone'))}</td><td>{zt(r.get('htf_zone'))}</td>"
                             f"<td style='color:{G if rsi_ok else M}'>{r['rsi_peak']:g}{' &#10004;' if rsi_ok else ''}</td><td><b>{_trk_score(key, r)}/6</b></td><td>{r['price']:g}</td><td>{_live_cell(lp, r['symbol'], r['price'])}</td><td>{status}</td><td>{r.get('seen', '')}</td></tr>")
                if rows:
                    trk_html = (f'<h2>All volume-spurt {"commodities" if key == "mcx" else "coins"} today (&ge; {cfg["track_mult"]:g}x) &mdash; {len(rows)} &middot; updated {td.get("updated", "")}</h2>'
                                '<div class="m">Every candle today with at least that volume multiple, newest first. "First candle after" and the latest candle show what price did after the spurt.</div>'
                                '<style>table.trk th,table.trk td{padding:4px 5px;font-size:12px;line-height:1.25}table.trk td.st{min-width:152px;padding:2px 4px;text-align:left}table.trk th{font-size:11.5px}</style><table class="s trk"><tr><th>#</th><th>Symbol</th><th>Spurt at</th><th title="volume multiple of the spurt candle">Spurt x</th><th title="best multiple today">Best x</th><th>Spurt candle</th><th title="candles since the spurt">Since</th>'
                                '<th>1st candle after</th><th>Latest</th><th title="10 candles before the spurt (dim), the spurt candle (dot) and the candles after it">10 before + spurt (dot) + next</th><th title="change since the spurt candle closed">Move</th><th title="distance from the highest high since the spurt">From high</th><th title="latest candle volume vs the spurt candle volume">Vol vs spurt</th><th title="solid red candle with higher volume has come">Red + vol up</th><th title="resistance zone touched since the spurt">Res. zone</th><th>' + cfg["htf"] + ' zone</th><th>RSI</th><th>Checklist</th><th title="close of the latest closed candle">Close</th><th title="last traded price now (refreshed when the page is opened)">Live</th><th>Status</th><th>Seen</th></tr>' + body + '</table>')
        except Exception:
            trk_html = ""
        if not trk_html:
            trk_html = (f'<h2>All volume-spurt {"commodities" if key == "mcx" else "coins"} today (&ge; {cfg["track_mult"]:g}x)</h2>'
                                f'<div class="m">None has had a {cfg["tf"]} candle of {cfg["track_mult"]:g}x volume or more yet today. Every one that reaches it is listed here and followed for {WAIT} candles. '
                                f'The {cfg["sell_mult"]:g}x in the rule above is only the level needed for a SELL signal.</div>')
        d = date if date in dates else (dates[0] if dates else "")
        x = df[df["date"] == d].sort_values(["candle", "spurt_mult"], ascending=[False, False]) if d else df
        sig_rows = "".join(
            f"<tr><td>{r.candle}</td><td>{_link(key, str(r.symbol))}</td><td>{r.entry:g}</td><td style='color:{R}'>{r.sl:g}</td><td style='color:{G}'>{r.t1:g}</td><td style='color:{G}'>{r.t2:g}</td>"
            f"<td>{r.risk_pct:g}%</td><td>{r.spurt_candle}</td><td><b>{r.spurt_mult:g}x</b></td><td>{int(r.candles_after_spurt)}</td><td>{_h.escape(str(r.res_zone))}</td>"
            f"<td style='white-space:nowrap'>{_draw(r.sig_ohlc)}</td><td>{r.rsi_peak}</td><td>{_h.escape(str(r.htf_zone)) if str(r.htf_zone) not in ('', 'nan') else '-'}</td><td><b>{r.score}</b></td></tr>"
            for r in x.itertuples()) if len(x) else ""
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        sig_html = (f'<h2>SELL signals (all required checklist points met){" &mdash; " + _h.escape(d) if d else ""}</h2>'
                    + (f'<form method="get" class="m">Day <select name="date">{opts}</select> <button>Show</button> &middot; {len(x)} signals</form>' if dates else "")
                    + ('<table class="s"><tr><th>Signal candle</th><th>Symbol</th><th>SELL entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Risk %</th>'
                       f'<th>Spurt candle</th><th>Spurt multiple</th><th>Candles after spurt</th><th>Resistance zone touched</th><th>Signal candle</th><th>RSI peak</th><th>{cfg["htf"]} resistance zone</th><th>Checklist</th></tr>'
                       + sig_rows + '</table>' if sig_rows else '<div class="m">No SELL signal yet.</div>'))
        return (head + f'<h1>{cfg["title"]} &mdash; Spurt SELL</h1>'
                f'<div class="m"><a href="{cfg["home"]}">back to {cfg["title"]} scanner</a> &middot; <a href="{cfg["csv"]}">download signals CSV</a></div>'
                f'<div class="m">{rule}</div>' + trk_html + sig_html + sort_js + '</body></html>')
    except Exception as e:
        return f"<p>Spurt SELL page error: {_h.escape(str(e))}</p>"
