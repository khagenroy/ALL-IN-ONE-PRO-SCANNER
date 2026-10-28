"""
OPTION volume-spurt scanner (read-only) - CE / PE contracts of the indices (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX)
and of all F&O stocks. Page: /opt_spurt   History CSV: /opt_spurt_history.csv

Same idea as the stock Volume Spurt page, separate files (nothing of the stock / commodity / crypto scanners is touched):
  1. Dhan instrument list -> nearest-expiry CE/PE contracts, only strikes close to the current price (indices +-10 strikes, stocks +-5).
  2. One batched Dhan quote -> day volume + OI + price of every such contract; the busiest ones (by money traded) are shortlisted.
  3. 5-min candles of the shortlist -> merged to 10-min closed candles -> latest candle volume / average of the previous 20 candles.
  4. Page + phone alert (ntfy, uses the NTFY_TOPIC you already have) when the multiple is 5x or more.
All settings are defaults in this file (no Render env vars needed). It places no orders.
"""
import os
import io
import csv
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("opt_spurt")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("OPT_AUTORUN", "true").strip().lower() == "true"
SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
INDEX_IDS = {"NIFTY": 13, "BANKNIFTY": 25, "SENSEX": 51, "FINNIFTY": 27, "MIDCPNIFTY": 442}     # Dhan IDX_I ids (an id that does not answer is skipped)
INDEX_STRIKES = 10          # strikes each side of the money, indices
STOCK_STRIKES = 5           # strikes each side of the money, stocks
SMA_LEN = 20                # average of the previous 20 closed 10-min candles
MIN_PRICE = 2.0             # option price (Rs) below this is ignored
MIN_TURNOVER = 300000       # day volume x price (Rs) a contract needs to be shortlisted
SHORTLIST = 100             # candles are fetched for the 100 busiest contracts
MIN_BASE_VOL = 100          # average candle volume (units) below this is ignored
SHOW_MULT = 2.0             # shown on the page from this multiple
ALERT_MULT = 5.0            # phone alert from this multiple
NTFY_MAX_PER_DAY = 40
TOP_N = 40
WORKERS = 4
HISTORY_DAYS = 5
ZONE_TF = 15                # support / resistance zones from 15-min candles (same as the stock page)
ZONE_NEAR_PCT = 0.5         # 'at' a zone = inside it or within this % of it
ZONE_AGAINST_PCT = 1.0      # opposite zone this close ahead = AGAINST
LIQ_OK, LIQ_THIN = 1.5, 4.0 # bid-ask spread % limits (options are wider than stocks)

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
JSON_PATH = os.path.join(RESULTS_DIR, "opt_live.json")
HIST_PATH = os.path.join(RESULTS_DIR, "opt_spurt_history.csv")
_contracts = {"day": None, "df": None, "eq": None}
_alerted = {"date": None, "keys": set(), "count": 0}


# ------------------------------------------------------------------------------------------ contracts
def _load_contracts(today):
    """(options DataFrame of nearest expiry per underlying, {stock symbol: NSE_EQ security id}). Downloaded once a day."""
    if _contracts["day"] == today and _contracts["df"] is not None:
        return _contracts["df"], _contracts["eq"]
    import requests
    r = requests.get(SCRIP_URL, timeout=120)
    r.raise_for_status()
    cols = ["SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID", "SEM_INSTRUMENT_NAME", "SEM_TRADING_SYMBOL", "SEM_LOT_UNITS",
            "SEM_EXPIRY_DATE", "SEM_STRIKE_PRICE", "SEM_OPTION_TYPE", "SEM_SERIES"]
    df = pd.read_csv(io.StringIO(r.text), usecols=lambda c: c in cols, dtype=str, low_memory=False)
    df = df.apply(lambda s: s.str.strip() if s.dtype == object else s)
    ex, ins = df["SEM_EXM_EXCH_ID"], df["SEM_INSTRUMENT_NAME"]
    eq = df[(ex == "NSE") & (df["SEM_SEGMENT"] == "E") & (df["SEM_SERIES"] == "EQ")]
    eq_ids = dict(zip(eq["SEM_TRADING_SYMBOL"], eq["SEM_SMST_SECURITY_ID"]))
    o = df[((ex == "NSE") & ins.isin(["OPTIDX", "OPTSTK"])) | ((ex == "BSE") & (ins == "OPTIDX"))].copy()
    o["under"] = o["SEM_TRADING_SYMBOL"].str.split("-").str[0]
    o = o[(o["SEM_EXM_EXCH_ID"] == "NSE") | (o["under"] == "SENSEX")]
    o["expiry"] = pd.to_datetime(o["SEM_EXPIRY_DATE"], errors="coerce").dt.date
    o["strike"] = pd.to_numeric(o["SEM_STRIKE_PRICE"], errors="coerce")
    o["lot"] = pd.to_numeric(o["SEM_LOT_UNITS"], errors="coerce")
    o = o.dropna(subset=["expiry", "strike"])
    o = o[o["SEM_OPTION_TYPE"].isin(["CE", "PE"]) & (o["expiry"] >= today)]
    near = o.groupby("under")["expiry"].transform("min")
    o = o[o["expiry"] == near]
    o["seg"] = np.where(o["SEM_EXM_EXCH_ID"] == "BSE", "BSE_FNO", "NSE_FNO")
    o["instr"] = o["SEM_INSTRUMENT_NAME"]
    o = o[["under", "SEM_TRADING_SYMBOL", "SEM_SMST_SECURITY_ID", "SEM_OPTION_TYPE", "strike", "expiry", "lot", "seg", "instr"]]
    o.columns = ["under", "name", "sid", "otype", "strike", "expiry", "lot", "seg", "instr"]
    _contracts.update(day=today, df=o.reset_index(drop=True), eq=eq_ids)
    log.info(f"Options: {o['under'].nunique()} underlyings, {len(o)} nearest-expiry contracts")
    return _contracts["df"], eq_ids


def _under_prices(unders, eq_ids):
    """{underlying: last price} - indices from IDX_I, stocks from NSE_EQ (one batched call each)."""
    import live_scanner as ls
    out = {}
    idx = {u: INDEX_IDS[u] for u in unders if u in INDEX_IDS}
    stk = {u: str(eq_ids[u]) for u in unders if u not in INDEX_IDS and u in eq_ids}
    for seg, ids in (("IDX_I", idx), ("NSE_EQ", stk)):
        if not ids:
            continue
        ids = list(ids.items())
        for i in range(0, len(ids), 900):
            part = dict(ids[i:i + 900])
            try:
                resp = ls._dhan_post("marketfeed/ltp", {seg: [int(v) for v in part.values()]}, f"options underlying {seg}")
                data = (resp.json().get("data") or {}).get(seg) or {}
                by = {str(v): k for k, v in part.items()}
                for sid, q in data.items():
                    u = by.get(str(sid))
                    try:
                        if u and isinstance(q, dict):
                            out[u] = float(q.get("last_price"))
                    except (TypeError, ValueError):
                        pass
            except Exception as e:
                log.warning(f"Options: underlying prices ({seg}) failed: {e}")
            time.sleep(1.1)
    return out


def _near_money(o, prices):
    """Contracts whose strike is within INDEX_STRIKES / STOCK_STRIKES strikes of the underlying price."""
    keep = []
    for u, g in o.groupby("under"):
        px = prices.get(u)
        if px is None:
            continue
        strikes = np.array(sorted(g["strike"].unique()))
        if len(strikes) == 0:
            continue
        k = int(np.abs(strikes - px).argmin())
        n = INDEX_STRIKES if u in INDEX_IDS else STOCK_STRIKES
        keep.append(g[g["strike"].isin(strikes[max(0, k - n):k + n + 1])])
    return pd.concat(keep) if keep else o.iloc[0:0]


def _quotes(c):
    """Adds day volume, OI and price to the contracts (batched marketfeed/quote, 1 call a second)."""
    import live_scanner as ls
    q = {}
    for seg, g in c.groupby("seg"):
        sids = list(g["sid"])
        for i in range(0, len(sids), 900):
            try:
                resp = ls._dhan_post("marketfeed/quote", {seg: [int(s) for s in sids[i:i + 900]]}, f"options quote {seg}")
                for sid, d in ((resp.json().get("data") or {}).get(seg) or {}).items():
                    if isinstance(d, dict):
                        q[str(sid)] = d
            except Exception as e:
                log.warning(f"Options: quote batch failed: {e}")
            time.sleep(1.1)
    c = c.copy()
    c["ltp"] = c["sid"].map(lambda s: float(q.get(str(s), {}).get("last_price") or 0))
    c["day_vol"] = c["sid"].map(lambda s: float(q.get(str(s), {}).get("volume") or 0))
    c["oi"] = c["sid"].map(lambda s: float(q.get(str(s), {}).get("oi") or 0))
    c["spread"] = c["sid"].map(lambda s: _spread(q.get(str(s), {})))
    return c


def _spread(q):
    """Best bid-ask spread % of one quote row (None when the depth fields are not there)."""
    try:
        d = q.get("depth") or {}
        buy, sell = d.get("buy") or [], d.get("sell") or []
        bid = float(buy[0].get("price")) if buy else 0.0
        ask = float(sell[0].get("price")) if sell else 0.0
        if bid > 0 and ask > 0 and ask >= bid:
            return round((ask - bid) / ((ask + bid) / 2.0) * 100.0, 2)
    except Exception:
        pass
    return None


def _zones(df):
    try:
        import spurt_zones
        import math
        return [z for z in (spurt_zones.live_zones(df) or [])
                if all(isinstance(z.get(k), (int, float)) and math.isfinite(z[k]) for k in ("lo", "hi"))]
    except Exception as e:
        log.warning(f"Options zone calculation failed: {e}")
        return []


def _zone_fields(zs, price, candle_chg):
    """Nearest support below / resistance above the price and whether the candle's direction agrees with it (same rule as the stock page)."""
    out = {"support": "", "support_dist_pct": None, "resistance": "", "resistance_dist_pct": None, "zone_view": "no zone", "at_zone": ""}
    try:
        if not zs or not price:
            return out
        near, ag = ZONE_NEAR_PCT / 100.0, ZONE_AGAINST_PCT / 100.0
        sup = [z for z in zs if z["type"] == "SUPPORT"]
        res = [z for z in zs if z["type"] == "RESISTANCE"]
        zsup = max(sup, key=lambda z: z["hi"]) if sup else None
        zres = min(res, key=lambda z: z["lo"]) if res else None
        if zsup:
            out["support"] = f"{zsup['lo']:g}-{zsup['hi']:g}"
            out["support_dist_pct"] = round(max(price - zsup["hi"], 0) / price * 100, 2)
        if zres:
            out["resistance"] = f"{zres['lo']:g}-{zres['hi']:g}"
            out["resistance_dist_pct"] = round(max(zres["lo"] - price, 0) / price * 100, 2)
        at_s = zsup is not None and zsup["lo"] * (1 - near) <= price <= zsup["hi"] * (1 + near)
        at_r = zres is not None and zres["lo"] * (1 - near) <= price <= zres["hi"] * (1 + near)
        out["zone_view"] = "between zones" if (zsup or zres) else "no zone"
        if at_s and at_r:
            at_s, at_r = (out["support_dist_pct"] <= out["resistance_dist_pct"]), (out["support_dist_pct"] > out["resistance_dist_pct"])
        out["at_zone"] = "SUPPORT" if at_s else "RESISTANCE" if at_r else ""
        if candle_chg is not None and abs(candle_chg) >= 0.05:
            up = candle_chg > 0
            if (up and at_s) or (not up and at_r):
                out["zone_view"] = "ALIGNED"
            elif (up and zres and (at_r or 0 <= (zres["lo"] - price) / price <= ag)) or \
                 (not up and zsup and (at_s or 0 <= (price - zsup["hi"]) / price <= ag)):
                out["zone_view"] = "AGAINST"
            elif at_s:
                out["zone_view"] = "at support"
            elif at_r:
                out["zone_view"] = "at resistance"
        else:
            out["zone_view"] = "at support" if at_s else "at resistance" if at_r else out["zone_view"]
    except Exception:
        pass
    return out


# ------------------------------------------------------------------------------------------ scoring
def _score(row):
    import live_scanner as ls
    now = datetime.now(IST)
    payload = {"securityId": str(row["sid"]), "exchangeSegment": row["seg"], "instrument": row["instr"], "interval": 5,
               "fromDate": (now - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d"), "toDate": now.strftime("%Y-%m-%d")}
    resp = ls._dhan_post("charts/intraday", payload, f"option {row['name']}")
    df5 = ls._parse_intraday(resp.json())
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    df = ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of)
    if df is None or len(df) < SMA_LEN + 1:
        return None
    vol = df["volume"].to_numpy(dtype=float)
    base = vol[-(SMA_LEN + 1):-1].mean()
    if base < MIN_BASE_VOL:
        return None
    last = df.iloc[-1]
    o, cl = float(last["open"]), float(last["close"])
    ts = df.index[-1]
    t_ist = (ts.tz_localize("UTC") if ts.tzinfo is None else ts).tz_convert(IST)
    zdf = df
    try:
        z15 = ls.drop_forming_bars(ls.build_merged_bars(df5, ZONE_TF // 5), ZONE_TF, as_of)
        if z15 is not None and len(z15) >= 30:
            zdf = z15
    except Exception:
        pass
    chg = round((cl / o - 1) * 100, 2) if o else None
    days = df.index.map(lambda t: (t + pd.Timedelta(minutes=330)).date())
    today_mask = days == days[-1]
    prev_close = float(df["close"][~today_mask].iloc[-1]) if (~today_mask).any() else None
    zf = _zone_fields(_zones(zdf), cl, chg)
    return {
        **zf, "ohlc": f"{o:.2f}|{float(last['high']):.2f}|{float(last['low']):.2f}|{cl:.2f}",
        "day_chg_pct": round((cl / prev_close - 1) * 100, 2) if prev_close else None, "spread_pct": row.get("spread"),
        "under_px": row.get("under_px"),
        "underlying": row["under"], "contract": row["name"], "type": row["otype"], "strike": float(row["strike"]),
        "expiry": str(row["expiry"]), "price": round(cl, 2), "candle": t_ist.strftime("%H:%M"),
        "candle_chg_pct": round((cl / o - 1) * 100, 2) if o else None, "colour": "GREEN" if cl >= o else "RED",
        "candle_vol": int(vol[-1]), "avg_vol": int(round(base)), "multiple": round(float(vol[-1] / base), 2),
        "day_vol": int(row["day_vol"]), "oi": int(row["oi"]), "lot": int(row["lot"]) if row["lot"] == row["lot"] else 0,
        "_df": df, "_sid": str(row["sid"]), "_seg": row["seg"],
    }


def _send_alerts(rows, now):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    today = now.strftime("%Y-%m-%d")
    if _alerted["date"] != today:
        _alerted.update(date=today, keys=set(), count=0)
    if _alerted["count"] >= NTFY_MAX_PER_DAY:
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and (r["contract"], r["candle"]) not in _alerted["keys"]]
    if not hits:
        return
    hits.sort(key=lambda r: -r["multiple"])
    lines = [f"{r['underlying']} {r['strike']:g} {r['type']}  {r['multiple']:.1f}x  Rs {r['price']:.2f}  {r['colour'].lower()} candle" for r in hits[:12]]
    extra = f"\n+{len(hits) - 12} more on the option page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Option volume spurt >= {ALERT_MULT:g}x ({hits[0]['candle']} candle)", "Priority": "high",
                                      "Tags": "chart_with_upwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Options ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Options ntfy failed: {e}")
        return
    for r in hits:
        _alerted["keys"].add((r["contract"], r["candle"]))
    _alerted["count"] += 1


def _append_history(rows, now):
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        new = not os.path.exists(HIST_PATH)
        with open(HIST_PATH, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["run_time"] + list(rows[0].keys()))
            for r in rows:
                w.writerow([now.strftime("%Y-%m-%d %H:%M")] + list(r.values()))
    except Exception as e:
        log.warning(f"Options history not written: {e}")


def run_once():
    t0 = time.time()
    now = datetime.now(IST)
    o, eq_ids = _load_contracts(now.date())
    prices = _under_prices(set(o["under"]), eq_ids)
    c = _near_money(o, prices)
    meta = {"run": now.strftime("%Y-%m-%d %H:%M:%S"), "underlyings": len(prices), "contracts": len(c)}
    if c.empty:
        meta.update(note="no underlying prices from Dhan", took_s=round(time.time() - t0, 1), shortlisted=0, scored=0)
        _write([], meta)
        return
    c = _quotes(c)
    c = c.assign(under_px=c["under"].map(prices))
    c = c[(c["ltp"] >= MIN_PRICE) & (c["day_vol"] * c["ltp"] >= MIN_TURNOVER)]
    c = c.assign(turn=c["day_vol"] * c["ltp"]).sort_values("turn", ascending=False).head(SHORTLIST)
    rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_score, r) for _, r in c.iterrows()]
        for f in futs:
            try:
                r = f.result()
                if r:
                    rows.append(r)
            except Exception as e:
                log.warning(f"Options candles failed: {str(e)[:120]}")
    try:
        import opt_sell
        trk, sigs = [], []
        for r in rows:
            it = opt_sell.track(r["_df"], r["contract"])
            if it:
                it["sid"], it["seg"] = r["_sid"], r["_seg"]
            trk.append(it)
            for side in ("SELL", "BUY"):
                sg = opt_sell.check(r["_df"], r["contract"], side)
                if sg:
                    sigs.append(sg)
        opt_sell.update(trk, sigs, now)
    except Exception as e:
        log.warning(f"Options sell/buy tracker failed: {e}")
    for r in rows:
        r.pop("_df", None), r.pop("_sid", None), r.pop("_seg", None)
    rows.sort(key=lambda r: -r["multiple"])
    shown = [r for r in rows if r["multiple"] >= SHOW_MULT][:TOP_N]
    meta.update(shortlisted=len(c), scored=len(rows), took_s=round(time.time() - t0, 1))
    _write(shown, meta)
    strong = [r for r in shown if r["multiple"] >= ALERT_MULT]
    if strong:
        _append_history(strong, now)
    _send_alerts(shown, now)
    log.info(f"Options cycle: {len(c)} shortlisted, {len(shown)} shown, {meta['took_s']}s")


def _write(rows, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(JSON_PATH + ".tmp", "w") as f:
        json.dump({"meta": meta, "rows": rows}, f)
    os.replace(JSON_PATH + ".tmp", JSON_PATH)


# ------------------------------------------------------------------------------------------ page
_BANNER7 = ('<style>@keyframes rb{0%,49%{background:#fff176;color:#d50000}50%,100%{background:#d50000;color:#fff}}</style><div style="animation:rb 1s step-end infinite;position:sticky;top:0;z-index:99999;background:#fff176;border:3px solid #d50000;border-radius:6px;padding:10px 14px;margin:0 0 14px;color:#d50000;font-weight:900;font-size:16px;line-height:1.45;text-align:center">1. DON&#39;T RUSH<br>2. DON&#39;T TRADE WITHOUT VALID SET UP<br>3. ONE VALID SETUP ENOUGH TO EARN MONEY<br>4. DON&#39;T BE EMOTIONAL ATTACHED<br>5. CHECK MINIMUM CRITERIA FOR SELL - VOLUME SPURTS, RSI ABOVE 70, INCREASE VOLUME OF RED CANDLE, RESISTANCE ZONE<br>6. CHECK MINIMUM CRITERIA FOR BUY - VOLUME SPURTS, RSI BELOW 30, INCREASE VOLUME OF GREEN CANDLE, SUPPORT ZONE<br>7. ALWAYS CALCULATE THE RISK AND CHECK THE LOSS BEFORE AVERAGING. AVERAGING TO BE DONE ONLY AFTER CONFIRMATION OF RED CANDLE FOR SELL AND GREEN CANDLE FOR BUY WITH INCREASE VOLUME</div>')


def _banner():
    """7-line rules banner (option pages only) + the indices strip."""
    try:
        import spurt_sell_live
        return _BANNER7 + spurt_sell_live._idx()
    except Exception:
        return _BANNER7


def page_html():
    import html as _h
    try:
        with open(JSON_PATH) as f:
            d = json.load(f)
    except Exception:
        d = {"meta": {}, "rows": []}
    meta, rows = d.get("meta", {}), d.get("rows", [])
    try:
        import spurt_sell_x as sx
        draw = sx._draw
    except Exception:
        draw = lambda *a, **k: ""
    try:
        import spurt_sell_live as sl
        sort_js = sl._SORT_JS
    except Exception:
        sort_js = ""
    G, R, M = "#0a7d33", "#c62828", "#777"
    ROW = {"SUPPORT": "#d4f1dc", "RESISTANCE": "#fadad6"}
    pct = lambda v: "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'

    def zc(r, k):
        z = r.get(k) or ""
        dd = r.get(k + "_dist_pct")
        if not z:
            return "-"
        return f"{z}<br><small>{'inside/at' if (dd or 0) == 0 else f'{dd:.2f}% away'}</small>"

    def zbg(at, kind):
        if at == kind:
            return ' style="background:#6fd68a;font-weight:bold"' if kind == "SUPPORT" else ' style="background:#f08d84;font-weight:bold"'
        return ""

    body = ""
    for i, r in enumerate(rows, 1):
        chg = r.get("candle_chg_pct")
        hot = "background:#ffe066;" if r["multiple"] >= ALERT_MULT else ""
        zv = r.get("zone_view") or "-"
        zcol = {"ALIGNED": G, "AGAINST": R, "at support": G, "at resistance": R}.get(zv, M)
        sp = r.get("spread_pct")
        liq = "" if sp is None else ("LIQUID" if sp <= LIQ_OK else "THIN" if sp > LIQ_THIN else "OK")
        lcol = {"LIQUID": G, "THIN": R}.get(liq, M)
        rs = f' style="background:{ROW[r["at_zone"]]}"' if r.get("at_zone") in ROW else ""
        gcol = G if r["colour"] == "GREEN" else R
        body += (f"<tr{rs}><td>{i}</td><td style='text-align:left'><b>{_h.escape(r['contract'])}</b><br><small>{_h.escape(r['underlying'])}"
                 f"{'' if not r.get('under_px') else ' @ ' + format(r['under_px'], 'g')}</small></td><td><b>{r['type']}</b></td><td>{r['strike']:g}</td><td>{r['expiry']}</td>"
                 f"<td>{r['price']:.2f}</td><td>{pct(r.get('day_chg_pct'))}</td><td>{r['candle']}</td><td>{pct(chg)}</td>"
                 f"<td style='white-space:nowrap'>{draw(r.get('ohlc'))} <b style='color:{gcol}'>{r['colour']}</b></td>"
                 f"<td>{r['candle_vol']:,}</td><td>{r['avg_vol']:,}</td><td style='{hot}font-weight:bold'>{r['multiple']:.2f}x</td><td>{r['day_vol']:,}</td><td>{r['oi']:,}</td>"
                 f"<td>{'-' if sp is None else format(sp, 'g') + '%'}<br><small style='color:{lcol}'><b>{liq}</b></small></td>"
                 f"<td{zbg(r.get('at_zone'), 'SUPPORT')}>{zc(r, 'support')}</td><td{zbg(r.get('at_zone'), 'RESISTANCE')}>{zc(r, 'resistance')}</td>"
                 f"<td><span style='color:{zcol}'><b>{zv}</b></span><br><small>15m zones</small></td></tr>")
    if not body:
        body = "<tr><td colspan='19'>No option contract has a 10-min volume of 2x or more in this cycle.</td></tr>"
    legend = ('<span style="background:#d4f1dc;padding:2px 8px;border:1px solid #9ccfae">green row = price AT a support zone</span> '
              '<span style="background:#fadad6;padding:2px 8px;border:1px solid #e0a39c">red row = price AT a resistance zone</span> '
              '<span style="background:#ffe066;padding:2px 8px;border:1px solid #d9b800">yellow multiple = phone-alert strength</span> '
              '<span style="background:#6fd68a;padding:2px 8px;border:1px solid #3ea85c">green cell = price at support zone</span> '
              '<span style="background:#f08d84;padding:2px 8px;border:1px solid #c9564c">red cell = price at resistance zone</span>')
    note = meta.get("note", "")
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<meta http-equiv='refresh' content='60'><title>Option Volume Spurt</title><style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}"
            "table{border-collapse:collapse;width:100%;background:#fff}th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}"
            "th{background:#222;color:#fff}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>" + _banner() +
            f"<h2>Option Volume Spurt &mdash; latest 10-min candle vs {SMA_LEN}-candle volume average (top {len(rows)})</h2>"
            f"<div class='m'>Run: {meta.get('run', 'not run yet')} (took {meta.get('took_s', '-')}s) &middot; {meta.get('underlyings', 0)} underlyings, "
            f"{meta.get('contracts', 0)} near-the-money CE/PE contracts of the nearest expiry (indices &plusmn;{INDEX_STRIKES} strikes, stocks &plusmn;{STOCK_STRIKES}), "
            f"{meta.get('shortlisted', 0)} busiest checked, {meta.get('scored', 0)} scored &middot; refreshes every 10 min &middot; {legend} &middot; "
            f"phone alert at {ALERT_MULT:g}x or more &middot; zones = ALL IN ONE PRO order-block zones on the contract's own {ZONE_TF}-min chart &middot; "
            "spread = best bid-ask gap (LIQUID / OK / THIN) &middot; click any heading to sort &middot; "
            f"<a href='/opt_spurt_history.csv'>history CSV</a> &middot; <a href='/opt_sell'>Option Spurt SELL / BUY</a> &middot; <a href='/spurt10'>Stock spurt</a> {note}</div>"
            "<table class='s'><tr><th>#</th><th>Contract</th><th>Type</th><th>Strike</th><th>Expiry</th><th>Price</th><th>Day %</th>"
            "<th>Candle</th><th>Candle %</th><th>Candle type</th><th>Candle volume</th><th>20-candle avg vol</th><th>Multiple</th><th>Day volume</th><th>OI</th><th>Spread</th>"
            f"<th>Support zone ({ZONE_TF}m)</th><th>Resistance zone ({ZONE_TF}m)</th><th>Zone view</th></tr>{body}</table>{sort_js}</body></html>")


# ------------------------------------------------------------------------------------------ loop
def _seconds_to_next_run(now):
    nxt = now.replace(second=40, microsecond=0)
    nxt += timedelta(minutes=(10 - now.minute % 10) % 10)
    if nxt <= now:
        nxt += timedelta(minutes=10)
    return max((nxt - now).total_seconds(), 1)


def _in_session(now):
    if now.weekday() >= 5:
        return False
    m = now.hour * 60 + now.minute
    return 9 * 60 + 25 <= m <= 15 * 60 + 40


def loop():
    log.info("Options spurt autorun started - every 10 min at candle close (09:25-15:40 IST, Mon-Fri).")
    while True:
        try:
            time.sleep(_seconds_to_next_run(datetime.now(IST)))
            if _in_session(datetime.now(IST)):
                run_once()
        except Exception as e:
            log.error(f"Options cycle failed: {e}")
            time.sleep(30)
