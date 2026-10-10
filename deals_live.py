"""
BULK / BLOCK DEALS PAGE   page: /deals

Read-only. Places no orders and is not connected to the scans, the bot or the bridges.

WHAT IT DOES
  You upload NSE's daily "Large deals" CSV files (BULK and/or BLOCK, as downloaded from nseindia.com). The page
    1. nets every client's buys and sells per stock per day,
    2. throws away the churn (high-frequency / arbitrage firms that buy and sell the same quantity the same day, and funds that
       just pass shares to each other: matched buy = matched sell) and keeps ONLY the genuinely one-sided deals,
    3. shows each one-sided deal with its size in rupees, weighted price, who did it, its share of that day's volume and the day's move,
    4. keeps a history (results/deals_history.csv) and, as days pass, fills in what the price did afterwards (+1 / +3 / +5 sessions)
       from Dhan's daily candles, with a hit-rate summary. That history is how the "strategy" below gets tested before real money.

HOW A STOCK IS CLASSIFIED (per stock, per day)
  Client is "directional" when |its net quantity| >= DEALS_DIRECTIONAL_PCT (25) % of its own bought+sold quantity. Others are churn.
  Stock verdict (directional clients only):
    ONE-SIDED BUY / ONE-SIDED SELL   net value >= DEALS_MIN_CR (2.0) crore and the buyers/sellers do not offset each other
    MATCHED TRANSFER                  directional buyers and sellers offset (e.g. fund A sells exactly what fund B buys)
    CHURN / SMALL                     everything else

THE STRATEGY (a hypothesis - NOT back-tested; the history table is what tests it)
  The news arrives after the close, so the edge is not in the news but in the level it leaves behind: the weighted price of a big one-sided deal.
  BUY watch   one-sided BUY, deal >= 5 % of day volume (or >= 10 crore): next session, take a long ONLY if price holds above the deal price AND
              one of our Section A / B / RSI Flush BUY signals fires. SL below the deal price / the day's low. Targets = the signal's own T1-T6.
  SELL watch  one-sided SELL: no long entries for 3 sessions unless price closes back above the deal price; a SELL signal at / below the
              deal price is an aligned short (cash stocks: intraday only).
  Skip        illiquid names (day volume small), MATCHED / CHURN stocks, and anything where the signal does not fire.
  Run it paper-only (the outcome columns) for 4-6 weeks before using any money.

ENV (optional)   DEALS_MIN_CR=2.0   DEALS_DIRECTIONAL_PCT=25   DEALS_KEY=<password>  (if set, uploads need it)   DEALS_MAX_FETCH=25
"""

import os
import io
import time
import json
import html
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("deals")
IST = ZoneInfo("Asia/Kolkata")

MIN_CR = float(os.environ.get("DEALS_MIN_CR", "2.0") or "2.0")
DIR_PCT = float(os.environ.get("DEALS_DIRECTIONAL_PCT", "25") or "25") / 100.0
KEY = os.environ.get("DEALS_KEY", "").strip()
MAX_FETCH = int(os.environ.get("DEALS_MAX_FETCH", "25") or "25")
MAX_BYTES = 20 * 1024 * 1024
OFFSET_DAYS = (1, 3, 5)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
HIST_CSV = os.path.join(RESULTS_DIR, "deals_history.csv")
ONE_CSV = os.path.join(RESULTS_DIR, "deals_onesided.csv")
OUT_JSON = os.path.join(RESULTS_DIR, "deals_outcomes.json")
HCOLS = ["date", "symbol", "name", "client", "side", "qty", "price", "kind"]


# ======================================================================================= reading the files
def _pick(cols, *starts):
    for c in cols:
        u = str(c).strip().upper()
        if any(u.startswith(s) for s in starts):
            return c
    return None


def parse_file(raw, filename=""):
    """NSE large-deals CSV (or this page's own history CSV) -> DataFrame with HCOLS. Raises ValueError with a plain message."""
    d = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    d.columns = [str(c).strip() for c in d.columns]
    low = {c.lower(): c for c in d.columns}
    if {"date", "symbol", "client", "side", "qty", "price"} <= set(low):                    # our own history file (restore)
        out = pd.DataFrame({k: d[low[k]].astype(str).str.strip() for k in ("date", "symbol", "client", "side")})
        out["name"] = d[low["name"]].astype(str).str.strip() if "name" in low else ""
        out["qty"] = pd.to_numeric(d[low["qty"]], errors="coerce")
        out["price"] = pd.to_numeric(d[low["price"]], errors="coerce")
        out["kind"] = d[low["kind"]].astype(str).str.strip() if "kind" in low else "deal"
        return out.dropna(subset=["qty", "price"])[HCOLS]
    cmap = {"date": _pick(d.columns, "DATE"), "symbol": _pick(d.columns, "SYMBOL"), "name": _pick(d.columns, "SECURITY NAME"),
            "client": _pick(d.columns, "CLIENT NAME"), "side": _pick(d.columns, "BUY/SELL", "BUY"),
            "qty": _pick(d.columns, "VOLUME", "QUANTITY"), "price": _pick(d.columns, "TRADE PRICE", "WEIGHTED", "PRICE")}
    miss = [k for k, v in cmap.items() if v is None and k != "name"]
    if miss:
        raise ValueError(f"{filename or 'file'}: this does not look like an NSE large-deals file (missing {', '.join(miss)} column)")
    out = pd.DataFrame({k: (d[v].astype(str).str.strip() if v else "") for k, v in cmap.items()})
    out["qty"] = pd.to_numeric(out["qty"].str.replace(",", ""), errors="coerce")
    out["price"] = pd.to_numeric(out["price"].str.replace(",", ""), errors="coerce")
    out["side"] = out["side"].str.upper().str[:1].map({"B": "BUY", "S": "SELL"})
    dt = pd.to_datetime(out["date"], format="%d-%b-%Y", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(out["date"], errors="coerce", dayfirst=True)
    out["date"] = dt.dt.strftime("%Y-%m-%d")
    out["symbol"] = out["symbol"].str.upper()
    fn = (filename or "").upper()
    out["kind"] = "block" if "BLOCK" in fn else "bulk" if "BULK" in fn else "deal"
    out = out.dropna(subset=["date", "side", "qty", "price"])
    out = out[(out["qty"] > 0) & (out["symbol"] != "")]
    if out.empty:
        raise ValueError(f"{filename or 'file'}: no usable rows found")
    return out[HCOLS]


def load_history():
    try:
        h = pd.read_csv(HIST_CSV, dtype={"symbol": str, "client": str, "side": str, "date": str, "name": str, "kind": str})
        return h[HCOLS]
    except Exception:
        return pd.DataFrame(columns=HCOLS)


def add_to_history(new):
    """Append rows; the same deal uploaded twice is stored once. Returns (rows added, total rows)."""
    h = load_history()
    allr = pd.concat([h, new], ignore_index=True)
    key = ["date", "symbol", "client", "side", "qty", "price"]
    before = len(h)
    allr = allr.drop_duplicates(subset=key, keep="first").sort_values(["date", "symbol"], kind="stable").reset_index(drop=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    allr.to_csv(HIST_CSV + ".tmp", index=False)
    os.replace(HIST_CSV + ".tmp", HIST_CSV)
    return len(allr) - before, len(allr)


# ======================================================================================= netting
def analyse(h):
    """One row per (date, symbol): gross / net in crore, directional net, verdict. Pure function of the deals table (vectorised)."""
    if h is None or h.empty:
        return pd.DataFrame()
    d = h.copy()
    d["val"] = d["qty"] * d["price"]
    # per (date, symbol, client): bought / sold quantity and value
    cl = d.groupby(["date", "symbol", "client", "side"]).agg(q=("qty", "sum"), v=("val", "sum")).unstack("side", fill_value=0)
    cl.columns = [f"{a}{b[0].lower()}" for a, b in cl.columns]            # qb qs vb vs
    for c in ("qb", "qs", "vb", "vs"):
        if c not in cl.columns:
            cl[c] = 0.0
    cl["nq"], cl["nv"], cl["gq"] = cl["qb"] - cl["qs"], cl["vb"] - cl["vs"], cl["qb"] + cl["qs"]
    cl["directional"] = cl["nq"].abs() >= DIR_PCT * cl["gq"]
    keys = ["date", "symbol"]
    tot = cl.groupby(level=keys).agg(vb=("vb", "sum"), vs=("vs", "sum"), qb=("qb", "sum"), qs=("qs", "sum"))
    dcl = cl[cl["directional"]]
    dg = dcl.groupby(level=keys).agg(dnv=("nv", "sum"), dq=("nq", "sum"), dvb=("vb", "sum"), dvs=("vs", "sum"), dqb=("qb", "sum"), dqs=("qs", "sum"))
    dg["dgross"] = dcl["nv"].abs().groupby(level=keys).sum()
    t = tot.join(dg, how="left").fillna(0.0)
    meta = d.groupby(keys).agg(name=("name", "first"), kind=("kind", lambda x: "/".join(sorted(set(x)))))
    t = t.join(meta)
    t["gross_cr"] = (t["vb"] + t["vs"]) / 1e7
    t["net_cr"] = (t["vb"] - t["vs"]) / 1e7
    t["dnet_cr"] = t["dnv"] / 1e7
    t["dgross_cr"] = t["dgross"] / 1e7
    offset = (t["dgross_cr"] > 0) & (t["dnet_cr"].abs() < 0.2 * t["dgross_cr"])
    weak = (t["dgross_cr"] == 0) | (t["dnet_cr"].abs() < MIN_CR)
    verdict = np.where(offset & (t["dgross_cr"] >= MIN_CR), "MATCHED TRANSFER",
              np.where(weak, "CHURN / SMALL",
              np.where(offset, "MATCHED TRANSFER", np.where(t["dnet_cr"] > 0, "ONE-SIDED BUY", "ONE-SIDED SELL"))))
    t["verdict"] = verdict
    allp = (t["vb"] + t["vs"]) / (t["qb"] + t["qs"]).replace(0, np.nan)
    buy_p = t["dvb"] / t["dqb"].replace(0, np.nan)
    sell_p = t["dvs"] / t["dqs"].replace(0, np.nan)
    t["deal_price"] = np.where(t["dnet_cr"] > 0, buy_p, np.where(t["dnet_cr"] < 0, sell_p, allp))
    t["deal_price"] = pd.Series(t["deal_price"], index=t.index).fillna(allp).round(2)
    t["matched_pct"] = (100 * (1 - t["net_cr"].abs() / t["gross_cr"].replace(0, np.nan))).fillna(0).round(1)
    top = dcl.assign(_a=dcl["nv"].abs()).sort_values("_a", ascending=False)
    top = top.groupby(level=keys).head(3)
    top["s"] = [f"{c} ({'buy' if q > 0 else 'sell'} {abs(v) / 1e7:.1f} cr)" for c, q, v in zip(top.index.get_level_values("client"), top["nq"], top["nv"])]
    who = top.groupby(level=keys)["s"].agg("; ".join)
    t["who"] = who
    t["who"] = t["who"].fillna("")
    out = t.reset_index().rename(columns={"dnet_cr": "directional_net_cr", "dq": "dir_net_qty"})
    out["gross_cr"], out["net_cr"], out["directional_net_cr"] = out["gross_cr"].round(2), out["net_cr"].round(2), out["directional_net_cr"].round(2)
    out["_a"] = out["directional_net_cr"].abs()
    out = out.sort_values(["date", "_a"], ascending=[False, False], kind="stable")
    return out[["date", "symbol", "name", "verdict", "gross_cr", "net_cr", "directional_net_cr", "matched_pct", "deal_price", "dir_net_qty", "who", "kind"]].reset_index(drop=True)


# ======================================================================================= prices (Dhan daily candles) - optional
_daily_cache = {}


def _ist_day_index(idx):
    """Dhan daily stamps are IST midnight written as UTC: either 18:30 of the PREVIOUS day (true epoch) or 00:00. Both -> the IST calendar day."""
    idx = pd.DatetimeIndex(pd.to_datetime(idx))
    shift = (idx.hour == 18) & (idx.minute == 30)
    idx = idx + pd.to_timedelta(np.where(shift, 330, 0), unit="m")
    return idx.normalize()


def _fetch_daily_df(sym, cached=True):
    """Daily candles for an NSE stock from Dhan. cached=True uses the scanner's own once-a-day disk cache; False fetches directly (backtest)."""
    import scrip_master as sm
    import live_scanner as ls
    sid, seg = sm.get_security_id_and_segment(sym)
    if not (sid and seg == "NSE_EQ"):
        return None
    df = ls.fetch_daily_history_cached(sym, str(sid), seg) if cached else ls.fetch_daily_history(str(sid), seg)
    df = df.copy()
    df.index = _ist_day_index(df.index)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def _daily(sym):
    """Daily candles for an NSE stock (cached in memory). None if unavailable."""
    if sym in _daily_cache:
        return _daily_cache[sym]
    df = None
    try:
        df = _fetch_daily_df(sym, cached=True)
    except Exception as e:
        log.info(f"Deals: no daily candles for {sym} ({str(e)[:80]})")
    _daily_cache[sym] = df
    return df


def enrich(onesided):
    """Add day volume, deal share of volume, day move and close-vs-deal-price to one-sided rows (only for the rows given)."""
    out = []
    for r in onesided.to_dict("records"):
        r = dict(r)
        df = _daily(r["symbol"])
        r.update(day_vol=None, vol_pct=None, day_chg_pct=None, close=None)
        if df is not None and not df.empty:
            ts = pd.Timestamp(r["date"])
            if ts in df.index:
                i = df.index.get_loc(ts)
                row = df.iloc[i]
                prev = df["close"].iloc[i - 1] if i > 0 else np.nan
                r["day_vol"] = int(row["volume"])
                r["close"] = round(float(row["close"]), 2)
                r["vol_pct"] = round(abs(r["dir_net_qty"]) / row["volume"] * 100, 1) if row["volume"] > 0 else None
                r["day_chg_pct"] = round(float((row["close"] / prev - 1) * 100), 2) if prev == prev and prev > 0 else None
        out.append(r)
    return out


def _load_outcomes():
    try:
        with open(OUT_JSON) as f:
            return json.load(f)
    except Exception:
        return {}


def update_outcomes(one, limit=None):
    """For each one-sided row: next-day open, then close after +1 / +3 / +5 sessions vs the deal day's close. Only fills what is available."""
    res = _load_outcomes()
    todo = 0
    limit = limit or MAX_FETCH
    syms_done = 0
    for r in one.to_dict("records"):
        k = f"{r['date']}|{r['symbol']}"
        cur = res.get(k, {})
        if all(f"d{n}" in cur for n in OFFSET_DAYS):
            continue
        if r["symbol"] not in _daily_cache and syms_done >= limit:
            continue
        had = r["symbol"] in _daily_cache
        df = _daily(r["symbol"])
        if not had:
            syms_done += 1
        if df is None or df.empty:
            continue
        ts = pd.Timestamp(r["date"])
        if ts not in df.index:
            continue
        i = df.index.get_loc(ts)
        base = float(df["close"].iloc[i])
        sign = 1 if r["verdict"] == "ONE-SIDED BUY" else -1
        o = {"base": round(base, 2)}
        if i + 1 < len(df):
            o["open1"] = round((float(df["open"].iloc[i + 1]) / base - 1) * 100 * sign, 2)
        for n in OFFSET_DAYS:
            if i + n < len(df):
                o[f"d{n}"] = round((float(df["close"].iloc[i + n]) / base - 1) * 100 * sign, 2)
        res[k] = {**cur, **o}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(OUT_JSON + ".tmp", "w") as f:
        json.dump(res, f)
    os.replace(OUT_JSON + ".tmp", OUT_JSON)
    return res


# ======================================================================================= upload
def handle_upload(files, key=""):
    """files: list of werkzeug FileStorage. Returns (ok, message)."""
    if KEY and key.strip() != KEY:
        return False, "Wrong or missing key."
    files = [f for f in files if f and getattr(f, "filename", "")]
    if not files:
        return False, "Choose at least one CSV file first."
    added_total, msgs = 0, []
    for f in files:
        raw = f.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            msgs.append(f"{f.filename}: too large"); continue
        try:
            new = parse_file(raw, f.filename)
            added, total = add_to_history(new)
            added_total += added
            ds = sorted(set(new["date"]))
            days = ds[0] if len(ds) == 1 else f"{ds[0]} to {ds[-1]}, {len(ds)} days"
            msgs.append(f"{html.escape(f.filename)}: {len(new)} rows read ({days}), {added} new")
        except ValueError as e:
            msgs.append(html.escape(str(e)))
        except Exception as e:
            log.warning(f"Deals upload failed for {f.filename}: {e}")
            msgs.append(f"{html.escape(f.filename)}: could not read this file")
    try:
        build_outputs()
    except Exception as e:
        log.warning(f"Deals analysis failed: {e}")
        msgs.append("Saved, but the analysis step failed - the page will show what it can.")
    return added_total > 0 or any("rows read" in m for m in msgs), " | ".join(msgs)


def build_outputs():
    h = load_history()
    a = analyse(h)
    one = a[a["verdict"].isin(["ONE-SIDED BUY", "ONE-SIDED SELL"])] if not a.empty else a
    if not one.empty:
        try:
            cutoff = (datetime.now(IST) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
            update_outcomes(one[one["date"] >= cutoff])
        except Exception as e:
            log.warning(f"Deals outcomes failed: {e}")
        res = _load_outcomes()
        rows = []
        for r in one.to_dict("records"):
            o = res.get(f"{r['date']}|{r['symbol']}", {})
            rows.append({**r, **{k: o.get(k) for k in ("base", "open1", "d1", "d3", "d5")}})
        os.makedirs(RESULTS_DIR, exist_ok=True)
        pd.DataFrame(rows).to_csv(ONE_CSV, index=False)
    return a, one


# ======================================================================================= page
def _css():
    return ('<style>body{background:#0e1117;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:24px}'
            'table{border-collapse:collapse;width:100%;font-size:13px;margin-bottom:8px}th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #262b36}'
            'th{background:#161b22;color:#9aa0a6;font-weight:600}tr:hover{background:#161b22}a{color:#58a6ff}.m{color:#9aa0a6;font-size:13px;margin:6px 0 12px}'
            'h2{margin:30px 0 4px}.box{background:#161b22;border:1px solid #262b36;border-radius:8px;padding:14px 16px;margin:12px 0}'
            'input,button{font-size:14px}button{background:#238636;color:#fff;border:0;border-radius:6px;padding:8px 16px;cursor:pointer}'
            '.ok{color:#3fb950}.bad{color:#f85149}li{margin:4px 0}</style>')


def page_html(message=None, ok=True):
    G, R, M = "#3fb950", "#f85149", "#9aa0a6"
    h = load_history()
    a = analyse(h)
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Bulk and block deals</title>' + _css() + '</head><body>')
    out = [head, "<h1>Bulk and block deals &mdash; what is real</h1>",
           '<div class="m"><a href="/scanner">Stock scanner</a> &middot; <a href="/commodity">Commodity</a> &middot; <a href="/crypto">Crypto</a></div>']
    keyf = '<input type="password" name="key" placeholder="key"> ' if KEY else ""
    out.append('<div class="box"><b>Upload today\'s NSE large-deals file(s)</b> (BULK and/or BLOCK .csv, or this page\'s own history .csv to restore it)'
               '<form method="post" enctype="multipart/form-data" style="margin-top:10px">'
               '<input type="file" name="files" accept=".csv" multiple> ' + keyf + '<button type="submit">Upload and analyse</button></form>'
               + (f'<div style="margin-top:10px;color:{G if ok else R}">{message}</div>' if message else "") + "</div>")
    if a.empty:
        out.append("<p>No deals yet. Upload a file above.</p></body></html>")
        return "".join(out)

    one = a[a["verdict"].isin(["ONE-SIDED BUY", "ONE-SIDED SELL"])]
    days = sorted(a["date"].unique(), reverse=True)
    latest = days[0]
    res = _load_outcomes()
    out.append(f'<div class="m">History: {len(h):,} deal rows over {len(days)} day(s) ({days[-1]} to {days[0]}) &middot; '
               f'<a href="/deals_onesided.csv">one-sided deals CSV</a> &middot; <a href="/deals_history.csv">history CSV</a> '
               f'<span style="color:{M}">(Render wipes files when it redeploys: keep the history CSV and upload it back to restore)</span></div>')

    # 1 latest day, one-sided
    lt = one[one["date"] == latest]
    en = {(r["date"], r["symbol"]): r for r in enrich(lt)} if not lt.empty else {}
    rows = []
    for r in lt.to_dict("records"):
        e = en.get((r["date"], r["symbol"]), {})
        side = "BUY" if r["verdict"].endswith("BUY") else "SELL"
        col = G if side == "BUY" else R
        pct = lambda v: "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'
        strong = (e.get("vol_pct") or 0) >= 5 or abs(r["directional_net_cr"]) >= 10
        rows.append(f"<tr><td><b>{html.escape(r['symbol'])}</b></td><td style='color:{col}'><b>{side}</b></td><td>{abs(r['directional_net_cr']):.1f}</td>"
                    f"<td>{r['deal_price']}</td><td>{e.get('close') or '-'}</td><td>{pct(e.get('day_chg_pct'))}</td>"
                    f"<td>{'-' if e.get('vol_pct') is None else str(e['vol_pct']) + '%'}</td><td>{'<b class=ok>strong</b>' if strong else 'small'}</td>"
                    f"<td>{html.escape(r['who'])}</td></tr>")
    out.append(f"<h2>1. One-sided deals on {latest} ({len(lt)})</h2>"
               "<div class='m'>Only real positions: churn and matched transfers removed. Deal price = weighted price of the directional side. "
               "Strong = deal is 5% or more of the day's volume, or 10 crore or more.</div>"
               + ("<div style='overflow-x:auto'><table><tr><th>Stock</th><th>Side</th><th>Net crore</th><th>Deal price</th><th>Close</th><th>Day %</th>"
                  "<th>Deal % of day volume</th><th>Size</th><th>Who</th></tr>" + "".join(rows) + "</table></div>"
                  if rows else f'<p style="color:{M}">No one-sided deals on {latest}.</p>'))

    # 2 plan for next session
    plan = []
    for r in lt.to_dict("records"):
        if r["verdict"].endswith("BUY"):
            plan.append(f"<li><b>{html.escape(r['symbol'])}</b> &mdash; BUY watch: only if price holds above <b>{r['deal_price']}</b> and a Section A / B / RSI Flush BUY signal fires. SL below {r['deal_price']} or the day's low.</li>")
        else:
            plan.append(f"<li><b>{html.escape(r['symbol'])}</b> &mdash; SELL watch: no long entries for 3 sessions unless it closes back above <b>{r['deal_price']}</b>; a SELL signal at or below {r['deal_price']} is aligned (intraday only).</li>")
    out.append("<h2>2. Plan for the next session</h2><div class='m'>Rules, not predictions. Our scanner decides the entry; the deal only sets the level and the bias.</div>"
               + (f"<ul>{''.join(plan)}</ul>" if plan else f'<p style="color:{M}">Nothing to watch.</p>'))

    # 3 what was thrown away
    rest = a[(a["date"] == latest) & (~a["verdict"].isin(["ONE-SIDED BUY", "ONE-SIDED SELL"]))].sort_values("gross_cr", ascending=False)
    tot_g, tot_n = a[a["date"] == latest]["gross_cr"].sum(), a[a["date"] == latest]["net_cr"].sum()
    rr = [[f"<b>{html.escape(r['symbol'])}</b>", r["verdict"], f"{r['gross_cr']:.1f}", f"{r['net_cr']:+.1f}", f"{r['matched_pct']:.1f}%"] for r in rest.head(40).to_dict("records")]
    out.append(f"<h2>3. Churn and matched transfers on {latest} ({len(rest)} stocks)</h2>"
               f"<div class='m'>Total traded {tot_g:,.0f} crore, net only {tot_n:+,.1f} crore &mdash; most of the money bought and sold the same day. Largest 40 shown.</div>"
               + "<div style='overflow-x:auto'><table><tr><th>Stock</th><th>Verdict</th><th>Gross crore</th><th>Net crore</th><th>Matched</th></tr>"
               + "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rr) + "</table></div>")

    # 4 history with outcomes
    hist = []
    stat = {"ONE-SIDED BUY": [], "ONE-SIDED SELL": []}
    for r in one.to_dict("records"):
        o = res.get(f"{r['date']}|{r['symbol']}", {})
        if o.get("d3") is not None:
            stat[r["verdict"]].append(o)
        f = lambda v: "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}%</span>'
        hist.append([r["date"], f"<b>{html.escape(r['symbol'])}</b>", "BUY" if r["verdict"].endswith("BUY") else "SELL", f"{abs(r['directional_net_cr']):.1f}",
                     r["deal_price"], f(o.get("open1")), f(o.get("d1")), f(o.get("d3")), f(o.get("d5"))])
    summ = []
    for k, lst in stat.items():
        if lst:
            d3 = [x["d3"] for x in lst]
            summ.append(f"{k.split()[-1]}: {len(lst)} deals with a 3-session result, {sum(1 for v in d3 if v > 0) / len(d3) * 100:.0f}% moved the deal's way, average {np.mean(d3):+.2f}%")
    out.append(f"<h2>4. History and what happened next ({len(one)} one-sided deals)</h2>"
               "<div class='m'>Moves are measured from the deal day's close and counted <b>in the deal's direction</b> (a fall after a SELL deal shows as +). "
               "This is the test of the strategy: it needs weeks of data before it means anything. "
               + (" &middot; ".join(summ) if summ else "No results yet: they appear once 3 trading days have passed after a deal and Dhan prices are reachable.") + "</div>"
               + "<div style='overflow-x:auto'><table><tr><th>Date</th><th>Stock</th><th>Side</th><th>Net crore</th><th>Deal price</th><th>Next open</th><th>+1 session</th><th>+3 sessions</th><th>+5 sessions</th></tr>"
               + "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in hist[:150]) + "</table></div>")

    out.append(backtest_html())
    out.append('<h2>6. How to read this</h2><ul>'
               '<li><b>Churn</b>: a firm that buys and sells about the same quantity the same day (high-frequency / arbitrage). It has no view.</li>'
               '<li><b>Matched transfer</b>: one party sells exactly what another buys (e.g. two funds of one group). Ownership moves, the market view does not.</li>'
               '<li><b>One-sided</b>: someone is genuinely adding or reducing. A big seller is a supply overhang; a big buyer is weaker evidence than a big seller.</li>'
               '<li>Deals are published after the close, so the price has often already reacted. The page therefore tracks what happens <i>after</i>.</li></ul>'
               "</body></html>")
    return "".join(out)


# ======================================================================================= BACKTEST (on your own deal history)
BT_JSON = os.path.join(RESULTS_DIR, "deals_backtest.json")
BT_CSV = os.path.join(RESULTS_DIR, "deals_backtest_events.csv")
BT_PROG = os.path.join(RESULTS_DIR, "deals_backtest_progress.json")
COST_PCT = float(os.environ.get("DEALS_BT_COST_PCT", "0.25") or "0.25")      # round-trip cost + slippage taken off every trade, in %
HORIZONS = (1, 3, 5)
_bt_lock = {"running": False}


def simulate_event(df, date, verdict, deal_price, dir_net_qty):
    """One deal against the stock's daily candles. Entry is the NEXT session's open (the deal is only public after the close).
    r<n>  = % move in the deal's direction from that open to the close of session n (n=1,3,5), before costs.
    gap   = % move in the deal's direction from the deal day's close to the next open (what is already gone by the time we can act).
    rule  = the strategy: enter at the next open only if it is on the right side of the deal price (above for BUY, below for SELL),
            stop at the deal price, otherwise exit at the close of session 5.   Returns None if prices are missing."""
    ts = pd.Timestamp(date)
    if df is None or ts not in df.index:
        return None
    i = df.index.get_loc(ts)
    if i + 1 >= len(df):
        return None
    o, h, l, c, v = (df[k].to_numpy(float) for k in ("open", "high", "low", "close", "volume"))
    sgn = 1 if verdict == "ONE-SIDED BUY" else -1
    open1 = o[i + 1]
    if not (open1 > 0 and c[i] > 0):
        return None
    out = {"gap": (open1 / c[i] - 1) * 100 * sgn, "day_vol": v[i], "close0": c[i],
           "vol_pct": (abs(dir_net_qty) / v[i] * 100) if v[i] > 0 else None}
    for n in HORIZONS:
        out[f"r{n}"] = (c[i + n] / open1 - 1) * 100 * sgn if i + n < len(df) else None
    out["rule"], out["rule_stop"] = None, None
    right_side = open1 >= deal_price if sgn == 1 else open1 <= deal_price
    if right_side and i + 5 < len(df):
        exit_px, stopped = None, False
        for j in range(i + 1, i + 6):
            if sgn == 1 and l[j] <= deal_price:
                exit_px, stopped = (deal_price if (j == i + 1 or o[j] >= deal_price) else o[j]), True
                break
            if sgn == -1 and h[j] >= deal_price:
                exit_px, stopped = (deal_price if (j == i + 1 or o[j] <= deal_price) else o[j]), True
                break
        if exit_px is None:
            exit_px = c[i + 5]
        out["rule"], out["rule_stop"] = (exit_px / open1 - 1) * 100 * sgn, stopped
    return out


def _baseline(df, d0, d1):
    """The stock's own average n-session move (next open -> close of session n) over the same period, as a % (long direction)."""
    w = df[(df.index >= pd.Timestamp(d0)) & (df.index <= pd.Timestamp(d1))]
    if len(w) < 20:
        return {n: 0.0 for n in HORIZONS}
    idx = df.index.get_indexer(w.index)
    o, c = df["open"].to_numpy(float), df["close"].to_numpy(float)
    res = {}
    for n in HORIZONS:
        ok = idx[idx + n < len(df)]
        vals = c[ok + n] / o[ok + 1] - 1
        res[n] = float(np.nanmean(vals) * 100) if len(vals) else 0.0
    return res


def _summ(name, g, cost):
    if g.empty:
        return None
    r3 = g["r3"].dropna() - cost
    if r3.empty:
        return None
    ex3 = (g["r3"] - g["bs"]).dropna() - cost
    rr = g["rule"].dropna() - cost
    r5 = g["r5"].dropna() - cost
    return {"group": name, "n": int(len(r3)), "hit3": round(float((r3 > 0).mean() * 100), 1), "mean3": round(float(r3.mean()), 2),
            "median3": round(float(r3.median()), 2), "excess3": round(float(ex3.mean()), 2) if len(ex3) else None,
            "mean5": round(float(r5.mean()), 2) if len(r5) else None, "gap": round(float(g["gap"].dropna().mean()), 2),
            "rule_n": int(len(rr)), "rule_hit": round(float((rr > 0).mean() * 100), 1) if len(rr) else None,
            "rule_mean": round(float(rr.mean()), 2) if len(rr) else None,
            "rule_stop": round(float(g["rule_stop"].dropna().astype(float).mean() * 100), 1) if g["rule_stop"].notna().any() else None}


def summarise(ev, cost=None):
    """Result rows, all net of costs. 'bs' = the stock's own average 3-session move in the deal's direction (so r3 - bs is the excess)."""
    cost = COST_PCT if cost is None else cost
    ev = ev.copy()
    ev["bs"] = ev["base3"] * ev["sgn"]
    rows = []
    for side, lab in (("ONE-SIDED BUY", "BUY"), ("ONE-SIDED SELL", "SELL")):
        e = ev[ev["verdict"] == side]
        rows.append(_summ(f"{lab}: all one-sided deals", e, cost))
        size = pd.cut(e["net_cr_abs"], [0, 5, 10, 50, 1e9], labels=["2-5 cr", "5-10 cr", "10-50 cr", "50 cr+"], right=False)
        for k in size.cat.categories:
            rows.append(_summ(f"{lab}: deal size {k}", e[size == k], cost))
        vp = pd.cut(e["vol_pct"], [0, 1, 5, 10, 1e9], labels=["under 1%", "1-5%", "5-10%", "10%+"], right=False)
        for k in vp.cat.categories:
            rows.append(_summ(f"{lab}: deal = {k} of day volume", e[vp == k], cost))
        pr = pd.cut(e["deal_price"], [0, 20, 100, 500, 1e9], labels=["under Rs 20", "Rs 20-100", "Rs 100-500", "over Rs 500"], right=False)
        for k in pr.cat.categories:
            rows.append(_summ(f"{lab}: share price {k}", e[pr == k], cost))
        for k in ("bulk", "block"):
            rows.append(_summ(f"{lab}: {k} deals", e[e["kind"].astype(str).str.contains(k)], cost))
        strong = e[(e["vol_pct"] >= 5) & (e["net_cr_abs"] >= 5)]
        rows.append(_summ(f"{lab}: strong (5%+ of volume AND 5 cr+)", strong, cost))
    return [r for r in rows if r]


def build_events(hist, fetch):
    """Every one-sided deal -> outcome row. fetch(sym) returns daily candles or None. Returns (events df, coverage dict)."""
    a = analyse(hist)
    one = a[a["verdict"].isin(["ONE-SIDED BUY", "ONE-SIDED SELL"])]
    if one.empty:
        return pd.DataFrame(), {"events": 0, "with_prices": 0, "symbols": 0, "no_data_symbols": 0, "from": "", "to": ""}
    d0, d1 = one["date"].min(), one["date"].max()
    base_cache, rows, nodata, bad = {}, [], set(), set()
    for r in one.to_dict("records"):
        sym = r["symbol"]
        if sym in bad:
            continue
        try:
            df = fetch(sym)
            if df is None or df.empty:
                nodata.add(sym)
                continue
            if not df.index.is_unique:
                df = df[~df.index.duplicated(keep="last")].sort_index()
            sim = simulate_event(df, r["date"], r["verdict"], r["deal_price"], r["dir_net_qty"])
            if sim is None:
                continue
            if sym not in base_cache:
                base_cache[sym] = _baseline(df, d0, d1)
            rows.append({**{k: r[k] for k in ("date", "symbol", "verdict", "directional_net_cr", "deal_price", "kind", "who")}, **sim,
                         "base3": base_cache[sym][3], "sgn": 1 if r["verdict"] == "ONE-SIDED BUY" else -1, "net_cr_abs": abs(r["directional_net_cr"])})
        except Exception as e:
            log.info(f"Deals backtest: skipped {sym}: {str(e)[:80]}")
            bad.add(sym)
    ev = pd.DataFrame(rows)
    cov = {"events": int(len(one)), "with_prices": int(len(ev)), "symbols": int(one["symbol"].nunique()), "no_data_symbols": int(len(nodata)),
           "skipped_symbols": int(len(bad)), "from": d0, "to": d1}
    return ev, cov


def _bt_progress(**kw):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    try:
        cur = {}
        if os.path.exists(BT_PROG):
            with open(BT_PROG) as f:
                cur = json.load(f)
        cur.update(kw)
        cur["updated"] = time.time()
        with open(BT_PROG + ".tmp", "w") as f:
            json.dump(cur, f)
        os.replace(BT_PROG + ".tmp", BT_PROG)
    except Exception:
        pass


def run_backtest(fetch_raw=None):
    """Whole backtest (blocking). fetch_raw(sym) -> daily candles (default: Dhan, no disk cache). Writes BT_CSV / BT_JSON."""
    hist = load_history()
    a = analyse(hist)
    one = a[a["verdict"].isin(["ONE-SIDED BUY", "ONE-SIDED SELL"])]
    syms = sorted(one["symbol"].unique())
    d0 = (pd.Timestamp(one["date"].min()) - pd.Timedelta(days=45)) if len(one) else None
    d1 = (pd.Timestamp(one["date"].max()) + pd.Timedelta(days=30)) if len(one) else None
    fetch_raw = fetch_raw or (lambda s: _fetch_daily_df(s, cached=False))
    store = {}
    _bt_progress(running=True, done=0, total=len(syms), started=datetime.now(IST).strftime("%Y-%m-%d %H:%M"), error="", finished="")
    for n, sym in enumerate(syms, 1):
        try:
            df = fetch_raw(sym)
            if df is not None and not df.empty:
                store[sym] = df[(df.index >= d0) & (df.index <= d1)].copy()      # keep only the window: small in memory
        except Exception as e:
            log.info(f"Deals backtest: {sym}: {str(e)[:80]}")
        if n % 5 == 0 or n == len(syms):
            _bt_progress(done=n)
    ev, cov = build_events(hist, lambda s: store.get(s))
    os.makedirs(RESULTS_DIR, exist_ok=True)
    if not ev.empty:
        ev.drop(columns=["sgn"], errors="ignore").to_csv(BT_CSV, index=False)
        stats = summarise(ev)
    else:
        stats = []
    with open(BT_JSON + ".tmp", "w") as f:
        json.dump({"built": datetime.now(IST).strftime("%Y-%m-%d %H:%M"), "cost_pct": COST_PCT, "coverage": cov, "stats": stats}, f)
    os.replace(BT_JSON + ".tmp", BT_JSON)
    _bt_progress(running=False, done=len(syms), finished=datetime.now(IST).strftime("%Y-%m-%d %H:%M"))
    return cov, stats


def start_backtest(force=False):
    """Start the backtest in the background. Refused during NSE hours (it would share Dhan's request budget with the live scan)."""
    now = datetime.now(IST)
    if not force and now.weekday() < 5 and 9 * 60 + 5 <= now.hour * 60 + now.minute <= 15 * 60 + 40:
        return False, "Not during NSE hours (09:05-15:40 IST, Mon-Fri): it would slow the live scanner. Start it in the evening or at the weekend."
    try:
        with open(BT_PROG) as f:
            pg = json.load(f)
    except Exception:
        pg = {}
    if _bt_lock["running"] or (pg.get("running") and time.time() - float(pg.get("updated", 0)) < 300):
        return False, "The backtest is already running."
    if load_history().empty:
        return False, "No deal history yet - upload files first."
    import threading

    def _go():
        _bt_lock["running"] = True
        try:
            run_backtest()
        except Exception as e:
            log.error(f"Deals backtest failed: {e}")
            _bt_progress(running=False, error=str(e)[:200])
        finally:
            _bt_lock["running"] = False
    threading.Thread(target=_go, daemon=True, name="deals-backtest").start()
    return True, "Backtest started. Refresh this page in a few minutes."


def backtest_html():
    G, R, M = "#3fb950", "#f85149", "#9aa0a6"
    prog, res = {}, None
    for path, name in ((BT_PROG, "p"), (BT_JSON, "r")):
        try:
            with open(path) as f:
                v = json.load(f)
            if name == "p":
                prog = v
            else:
                res = v
        except Exception:
            pass
    run = bool(prog.get("running")) and (_bt_lock["running"] or time.time() - float(prog.get("updated", 0)) < 300)
    out = ['<h2>5. Backtest on your deal history</h2>',
           "<div class='m'>Takes every one-sided deal in the history, buys at the <b>next session's open</b> (the deal is public only after the close) and measures the move "
           "in the deal's direction, net of a cost allowance. 'Rule' = the strategy: enter only if that open is on the right side of the deal price, stop at the deal price, exit at session 5. "
           "'vs own average' = the move minus that stock's own average move over the same months, so general market drift is removed.</div>"]
    out.append('<form method="post" action="/deals_backtest" style="margin:8px 0"><button type="submit">Run / refresh backtest</button> '
               f'<span style="color:{M}">uses Dhan daily prices; takes several minutes; not allowed during NSE hours</span></form>')
    if run:
        out.append(f'<p style="color:#d29922">Running: {prog.get("done", 0)} of {prog.get("total", "?")} stocks fetched (started {prog.get("started", "")}). Refresh in a minute.</p>')
    elif prog.get("error"):
        out.append(f'<p style="color:{R}">Last run failed: {html.escape(str(prog["error"]))}</p>')
    if not res:
        out.append(f'<p style="color:{M}">No backtest result yet.</p>')
        return "".join(out)
    cv = res["coverage"]
    out.append(f'<div class="m">Built {res["built"]} &middot; {cv["events"]:,} one-sided deals ({cv["from"]} to {cv["to"]}) in {cv["symbols"]} stocks &middot; '
               f'prices found for {cv["with_prices"]:,} of them ({cv["no_data_symbols"]} stocks had no Dhan prices: renamed or delisted{", " + str(cv["skipped_symbols"]) + " skipped on errors" if cv.get("skipped_symbols") else ""}) &middot; cost allowance {res["cost_pct"]}% per trade '
               f'&middot; <a href="/deals_backtest_events.csv">events CSV</a></div>')
    col = lambda v, suf="%": "-" if v is None else f'<span style="color:{G if v > 0 else R if v < 0 else M}">{v:+.2f}{suf}</span>'
    rows = []
    for r in res["stats"]:
        rows.append(f"<tr><td>{html.escape(r['group'])}</td><td>{r['n']}</td><td>{r['hit3']}%</td><td>{col(r['mean3'])}</td><td>{col(r['median3'])}</td>"
                    f"<td>{col(r['excess3'])}</td><td>{col(r['gap'])}</td><td>{col(r['mean5'])}</td><td>{r['rule_n']}</td>"
                    f"<td>{'-' if r['rule_hit'] is None else str(r['rule_hit']) + '%'}</td><td>{col(r['rule_mean'])}</td>"
                    f"<td>{'-' if r['rule_stop'] is None else str(r['rule_stop']) + '%'}</td></tr>")
    out.append("<div style='overflow-x:auto'><table><tr><th>Group</th><th>Deals</th><th>3-session hit rate</th><th>Avg 3-session</th><th>Median</th>"
               "<th>vs own average</th><th>Gap before entry</th><th>Avg 5-session</th><th>Rule trades</th><th>Rule hit rate</th><th>Rule avg</th><th>Rule stopped out</th></tr>"
               + "".join(rows) + "</table></div>")
    out.append('<div class="m">All moves are in the deal\'s direction: for SELL rows a positive number means the stock fell. SELL rows show whether a big seller is a warning, '
               'not a trade you can take in cash stocks. Treat any group with fewer than ~30 deals as noise.</div>')
    return "".join(out)
