"""
OI SUPPORT / RESISTANCE ZONES  (shown at the bottom of /scanner; CSV: /oizones.csv)

WHAT IT DOES
  1. ZONES (built once a day after the close, and once at start-up if none exist yet)
     For every F&O stock it downloads ~90 days of daily candles of the near-month FUTURE together with its
     open interest (OI). A day where OI jumped a lot is a "buildup" day:
        price UP   + OI UP  = long buildup  -> SUPPORT zone    (that day's low  ..  bottom of its body)
        price DOWN + OI UP  = short buildup -> RESISTANCE zone (top of its body ..  that day's high)
     Overlapping zones are merged. A zone is dropped once price CLOSES through it (support broken / resistance
     broken). The 3 strongest zones of each kind per stock are kept.
  2. NEAR TABLE (refreshed every 10 min in market hours, uses 1 batched LTP call)
     Lists the stocks whose price is inside, or within NEAR_PCT (0.5%) of, a support zone (buy watch) or a
     resistance zone (sell watch).
  3. SIGNALS vs ZONES (same refresh)
     Takes the current Section A, Section B and RSI Flush signals from the last intraday scan and tells for each
     F&O stock whether the signal agrees with the OI zones:
        ALIGNED  : BUY at/near OI support, or SELL at/near OI resistance
        AGAINST  : BUY with OI resistance within 1% above (or inside it), SELL with OI support within 1% below
        neutral  : no zone close by
  It is read-only, places no orders and does not touch any signal logic.

THE DHAN CALL IS NOT VERIFIED FROM HERE. The first run probes four request shapes on the first stocks, keeps the
first one that returns OI, and prints which one worked (and any error) in the diagnostics line of the table.

ENV (optional): OIZONES_AUTORUN=true  OIZONES_NEAR_PCT=0.5  OIZONES_DAYS=90  OIZONES_MAX_STOCKS=0 (0 = all)
"""

import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import live_scanner as ls
import scrip_master as sm
from scrip_master import get_security_id_and_segment

log = logging.getLogger("oizones")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("OIZONES_AUTORUN", "true").strip().lower() == "true"
NEAR_PCT = float(os.environ.get("OIZONES_NEAR_PCT", "0.5") or "0.5") / 100.0
AGAINST_PCT = 0.01
DAYS = int(os.environ.get("OIZONES_DAYS", "90") or "90")
MAX_STOCKS = int(os.environ.get("OIZONES_MAX_STOCKS", "0") or "0")
WORKERS = 4
MIN_OI_JUMP_PCT = 0.015        # a buildup day must add at least 1.5% to OI ...
JUMP_QUANTILE = 0.80           # ... and be among the biggest 20% OI additions in the window
KEEP_PER_SIDE = 3
MERGE_PAD = 0.002

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
ZONES_PATH = os.path.join(RESULTS_DIR, "oi_zones.json")
NEAR_CSV = os.path.join(RESULTS_DIR, "oizones.csv")
LIVE_PATH = os.path.join(RESULTS_DIR, "oi_zones_live.json")

_fno = None          # {underlying: (future_security_id, cash_security_id)}


# ---------------------------------------------------------------- universe
def load_fno():
    global _fno
    if _fno is not None:
        return _fno
    sm._ensure_fresh_cache()
    df = pd.read_csv(sm.CACHE_FILE, dtype=str)
    m = (df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & (df["SEM_INSTRUMENT_NAME"].str.upper() == "FUTSTK")
    f = df[m].copy()
    f["_exp"] = pd.to_datetime(f["SEM_EXPIRY_DATE"], errors="coerce")
    f = f[f["_exp"] >= pd.Timestamp.now().normalize()].sort_values("_exp")
    out = {}
    for _, r in f.iterrows():
        und = str(r["SEM_TRADING_SYMBOL"]).split("-")[0].strip().upper()
        if und in out:
            continue
        try:
            cash_id, seg = get_security_id_and_segment(und)
        except Exception:
            continue
        if not cash_id or seg != "NSE_EQ":
            continue
        out[und] = (str(r["SEM_SMST_SECURITY_ID"]).strip(), str(cash_id))
    if MAX_STOCKS > 0:
        out = dict(list(out.items())[:MAX_STOCKS])
    _fno = out
    log.info(f"OI zones: {len(out)} F&O stocks with a near-month future")
    return out


# ---------------------------------------------------------------- Dhan fetch
VARIANTS = [   # (which id, expiryCode)
    ("future", 0), ("future", 1), ("cash", 1), ("cash", 0),
]
_variant = {"v": None, "note": ""}


def _parse_oi(j):
    oi = j.get("open_interest")
    if oi is None:
        oi = j.get("oi")
    if oi is None or len(oi) == 0:
        return None
    d = pd.DataFrame({k: j[k] for k in ("open", "high", "low", "close", "volume", "timestamp")})
    d["oi"] = oi
    d["timestamp"] = pd.to_datetime(d["timestamp"], unit="s", errors="coerce")
    d = d.set_index("timestamp").sort_index().astype(float)
    d = d[d["oi"] > 0]
    return d if len(d) >= 15 else None


def _fetch(und, ids, variant):
    which, exp = variant
    sid = ids[0] if which == "future" else ids[1]
    payload = {
        "securityId": sid, "exchangeSegment": "NSE_FNO", "instrument": "FUTSTK", "expiryCode": exp, "oi": True,
        "fromDate": (datetime.now() - timedelta(days=DAYS)).strftime("%Y-%m-%d"),
        "toDate": datetime.now().strftime("%Y-%m-%d"),
    }
    resp = ls._dhan_post("charts/historical", payload, f"{und} OI daily")
    return _parse_oi(resp.json())


def _probe(fno):
    errs = []
    for und, ids in list(fno.items())[:4]:
        for v in VARIANTS:
            try:
                d = _fetch(und, ids, v)
                if d is not None:
                    _variant["v"] = v
                    _variant["note"] = f"request shape: {v[0]} id, expiryCode {v[1]} (worked on {und})"
                    return True
                errs.append(f"{und} {v}: no OI in response")
            except Exception as e:
                errs.append(f"{und} {v}: {str(e)[:90]}")
    _variant["note"] = "NO OI DATA: " + " | ".join(errs[:4])
    return False


# ---------------------------------------------------------------- zone maths
def zones_from_df(d):
    d = d.tail(70)
    o, h, l, c, oi = (d[k].to_numpy(float) for k in ("open", "high", "low", "close", "oi"))
    n = len(d)
    if n < 15:
        return []
    doi = np.diff(oi, prepend=np.nan)
    pct = doi / np.where(np.roll(oi, 1) > 0, np.roll(oi, 1), np.nan)
    pos = doi[doi > 0]
    if pos.size < 5:
        return []
    thr = np.quantile(pos, JUMP_QUANTILE)
    raw = []
    for i in range(1, n):
        if not (doi[i] >= thr and pct[i] >= MIN_OI_JUMP_PCT):
            continue
        body_lo, body_hi = min(o[i], c[i]), max(o[i], c[i])
        rng = max(h[i] - l[i], 1e-9)
        if c[i] > c[i - 1]:                                   # long buildup -> support
            hi = body_lo if body_lo - l[i] > 0.1 * rng else l[i] + 0.25 * rng
            raw.append(["SUPPORT", float(l[i]), float(hi), float(doi[i]), i])
        elif c[i] < c[i - 1]:                                 # short buildup -> resistance
            lo = body_hi if h[i] - body_hi > 0.1 * rng else h[i] - 0.25 * rng
            raw.append(["RESISTANCE", float(lo), float(h[i]), float(doi[i]), i])
    merged = []
    for kind in ("SUPPORT", "RESISTANCE"):
        zs = sorted([z for z in raw if z[0] == kind], key=lambda z: z[1])
        cur = None
        for z in zs:
            if cur and z[1] <= cur[2] * (1 + MERGE_PAD):
                cur[1] = min(cur[1], z[1]); cur[2] = max(cur[2], z[2]); cur[3] += z[3]; cur[4] = max(cur[4], z[4])
            else:
                if cur:
                    merged.append(cur)
                cur = list(z)
        if cur:
            merged.append(cur)
    out = []
    for kind, lo, hi, add, i in merged:
        later = c[i + 1:]
        if kind == "SUPPORT" and later.size and (later < lo * (1 - MERGE_PAD)).any():
            continue
        if kind == "RESISTANCE" and later.size and (later > hi * (1 + MERGE_PAD)).any():
            continue
        out.append({"type": kind, "lo": round(lo, 2), "hi": round(hi, 2), "oi_added": int(add),
                    "oi_added_pct": round(add / oi[-1] * 100, 1), "date": str(d.index[i].date()),
                    "bars_ago": int(n - 1 - i)})
    res = []
    for kind in ("SUPPORT", "RESISTANCE"):
        z = sorted([x for x in out if x["type"] == kind], key=lambda x: -x["oi_added"])[:KEEP_PER_SIDE]
        res += z
    return res


def build_zones():
    t0 = time.monotonic()
    fno = load_fno()
    if not fno:
        raise RuntimeError("no F&O futures found in the scrip master")
    if not _probe(fno):
        log.error(_variant["note"])
        _save_zones({"built": datetime.now(IST).isoformat(), "stocks": {}, "note": _variant["note"], "errors": len(fno)})
        return
    v = _variant["v"]
    zones, errors = {}, 0

    def one(item):
        und, ids = item
        try:
            d = _fetch(und, ids, v)
            return und, (zones_from_df(d) if d is not None else None), None
        except Exception as e:
            return und, None, str(e)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for und, z, err in ex.map(one, list(fno.items())):
            if err or z is None:
                errors += 1
            elif z:
                zones[und] = z
    _save_zones({"built": datetime.now(IST).isoformat(), "stocks": zones, "note": _variant["note"],
                 "errors": errors, "universe": len(fno), "took_s": round(time.monotonic() - t0)})
    log.info(f"OI zones built: {len(zones)} stocks with zones, {errors} errors, {time.monotonic() - t0:.0f}s")


def _save_zones(obj):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(ZONES_PATH + ".tmp", "w") as f:
        json.dump(obj, f)
    os.replace(ZONES_PATH + ".tmp", ZONES_PATH)


def load_zones():
    try:
        with open(ZONES_PATH) as f:
            return json.load(f)
    except Exception:
        return None


# ---------------------------------------------------------------- live refresh
def _ltp(fno):
    by_id = {cash: und for und, (fut, cash) in fno.items()}
    ids = [int(x) for x in by_id]
    out = {}
    for i in range(0, len(ids), 1000):
        resp = ls._dhan_post("marketfeed/ltp", {"NSE_EQ": ids[i:i + 1000]}, "OI zones ltp")
        for sid, q in ((resp.json().get("data") or {}).get("NSE_EQ") or {}).items():
            try:
                out[by_id[str(sid)]] = float(q["last_price"])
            except Exception:
                pass
        time.sleep(1.1)
    return out


def _near(price, z):
    return z["lo"] * (1 - NEAR_PCT) <= price <= z["hi"] * (1 + NEAR_PCT)


def _read_signals():
    rows = []
    files = [("A", "latest_signals.csv"), ("B", "latest_signals_b.csv"), ("RSI", "latest_rsi_flush.csv")]
    for tag, fn in files:
        p = os.path.join(RESULTS_DIR, fn)
        if not os.path.exists(p):
            continue
        try:
            d = pd.read_csv(p)
        except Exception:
            continue
        for _, r in d.iterrows():
            sig = str(r.get("signal", r.get("side", ""))).upper()
            side = "BUY" if "BUY" in sig else "SELL" if "SELL" in sig else ""
            if tag == "RSI" and str(r.get("status", "")).upper() not in ("PENDING", "ACTIVE"):
                continue
            px = r.get("entry", r.get("close"))
            if not side or pd.isna(px):
                continue
            rows.append({"symbol": str(r["symbol"]).upper(), "section": tag, "tf": r.get("timeframe", ""),
                         "signal": sig if tag != "RSI" else f"RSI_FLUSH_{side}", "side": side, "price": float(px)})
    return rows


def judge(side, price, zs):
    sup = [z for z in zs if z["type"] == "SUPPORT"]
    res = [z for z in zs if z["type"] == "RESISTANCE"]
    if side == "BUY":
        for z in sup:
            if _near(price, z):
                return "ALIGNED", f"at OI support {z['lo']}-{z['hi']}"
        for z in res:
            if z["lo"] * (1 - NEAR_PCT) <= price <= z["hi"] or 0 <= (z["lo"] - price) / price <= AGAINST_PCT:
                return "AGAINST", f"OI resistance {z['lo']}-{z['hi']} just above"
    else:
        for z in res:
            if _near(price, z):
                return "ALIGNED", f"at OI resistance {z['lo']}-{z['hi']}"
        for z in sup:
            if z["lo"] <= price <= z["hi"] * (1 + NEAR_PCT) or 0 <= (price - z["hi"]) / price <= AGAINST_PCT:
                return "AGAINST", f"OI support {z['lo']}-{z['hi']} just below"
    return "neutral", ""


def refresh_live():
    zd = load_zones()
    if not zd or not zd.get("stocks"):
        _write_live({"meta": {"note": (zd or {}).get("note", "zones not built yet")}, "near": [], "signals": []})
        return
    fno = load_fno()
    px = _ltp(fno)
    near = []
    for und, zs in zd["stocks"].items():
        p = px.get(und)
        if not p:
            continue
        for z in zs:
            if _near(p, z):
                ref = z["lo"] if z["type"] == "SUPPORT" else z["hi"]
                near.append({"symbol": und, "price": round(p, 2), "zone": z["type"], "zone_lo": z["lo"], "zone_hi": z["hi"],
                             "dist_pct": round((p / ref - 1) * 100, 2), "oi_added_pct": z["oi_added_pct"],
                             "zone_date": z["date"]})
    near.sort(key=lambda r: abs(r["dist_pct"]))
    sigs = []
    for s in _read_signals():
        zs = zd["stocks"].get(s["symbol"])
        if zs is None:
            continue
        verdict, why = judge(s["side"], s["price"], zs)
        sigs.append({**s, "verdict": verdict, "why": why})
    order = {"ALIGNED": 0, "AGAINST": 1, "neutral": 2}
    sigs.sort(key=lambda r: (order[r["verdict"]], r["symbol"]))
    meta = {"run": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"), "zones_built": zd.get("built", "")[:16].replace("T", " "),
            "stocks_with_zones": len(zd["stocks"]), "universe": zd.get("universe"), "errors": zd.get("errors"),
            "note": zd.get("note", ""), "near_pct": NEAR_PCT * 100}
    _write_live({"meta": meta, "near": near, "signals": sigs})
    os.makedirs(RESULTS_DIR, exist_ok=True)
    pd.DataFrame(near).to_csv(NEAR_CSV, index=False)


def _write_live(obj):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(LIVE_PATH + ".tmp", "w") as f:
        json.dump(obj, f)
    os.replace(LIVE_PATH + ".tmp", LIVE_PATH)


# ---------------------------------------------------------------- html
def section_html():
    try:
        with open(LIVE_PATH) as f:
            d = json.load(f)
    except Exception:
        return ""
    m, near, sigs = d["meta"], d["near"], d["signals"]
    colr = {"ALIGNED": "#0a7d33", "AGAINST": "#c62828", "neutral": "#666"}

    def table(head, rows):
        return ('<div style="overflow-x:auto"><table style="border-collapse:collapse;width:100%;background:#fff">'
                + "<tr>" + "".join(f'<th style="border:1px solid #ddd;padding:6px 8px;background:#222;color:#fff">{h}</th>' for h in head) + "</tr>"
                + "".join("<tr>" + "".join(f'<td style="border:1px solid #ddd;padding:6px 8px;text-align:left">{c}</td>' for c in r) + "</tr>" for r in rows)
                + "</table></div>")

    near_rows = [[f"<b>{r['symbol']}</b>", r["price"],
                  f'<span style="color:{"#0a7d33" if r["zone"] == "SUPPORT" else "#c62828"}">{r["zone"]}</span>',
                  f"{r['zone_lo']} - {r['zone_hi']}", f"{r['dist_pct']:+.2f}%", f"{r['oi_added_pct']}%", r["zone_date"]] for r in near]
    sig_rows = [[f"<b>{r['symbol']}</b>", r["section"], r["tf"], r["signal"], round(r["price"], 2),
                 f'<b style="color:{colr[r["verdict"]]}">{r["verdict"]}</b>', r["why"]] for r in sigs]
    return (
        '<div style="margin-top:28px;font-family:Arial,sans-serif">'
        f'<h2>OI support / resistance zones (futures OI buildup)</h2>'
        f'<div style="color:#555;font-size:13px;margin:6px 0 12px">Zones built {m.get("zones_built", "-")} &middot; '
        f'{m.get("stocks_with_zones", 0)} of {m.get("universe", "?")} F&amp;O stocks have zones &middot; errors {m.get("errors", "?")} &middot; '
        f'near = within {m.get("near_pct", 0.5):.1f}% &middot; run {m.get("run", "-")} &middot; <a href="/oizones.csv">CSV</a><br>'
        f'<i>{m.get("note", "")}</i></div>'
        f'<h3>Stocks at an OI zone now ({len(near)})</h3>'
        + (table(["Symbol", "Price", "Zone", "Zone range", "Dist from edge", "OI added", "Zone day"], near_rows) if near_rows else "<p>None right now.</p>")
        + f'<h3>Our signals vs OI zones ({len(sigs)})</h3>'
        + (table(["Symbol", "Sec", "TF", "Signal", "Price", "Verdict", "Why"], sig_rows) if sig_rows else "<p>No signals on F&amp;O stocks in the last scan.</p>")
        + "</div>")


# ---------------------------------------------------------------- loop
def _zones_due(now):
    z = load_zones()
    if z is None:
        return True
    if now.weekday() >= 5 or now.hour < 16:
        return False
    return z.get("built", "")[:10] < now.strftime("%Y-%m-%d")


def loop():
    log.info("OI zones autorun started - zones once a day after 16:00 IST (and at start-up if none), near/signal table every 10 min.")
    last_try = 0.0
    last_live = 0.0
    while True:
        try:
            now = datetime.now(IST)
            if _zones_due(now) and time.monotonic() - last_try > 1800:
                last_try = time.monotonic()
                build_zones()
                last_live = 0.0
            in_mkt = now.weekday() < 5 and 9 * 60 + 20 <= now.hour * 60 + now.minute <= 15 * 60 + 40
            if (in_mkt and time.monotonic() - last_live >= 600) or (not in_mkt and last_live == 0.0):
                last_live = time.monotonic()
                refresh_live()
        except Exception as e:
            log.error(f"OI zones cycle failed: {e}")
        time.sleep(30)
