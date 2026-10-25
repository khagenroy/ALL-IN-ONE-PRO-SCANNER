"""
Crypto page extras, same idea as the stock /spurt10 page (read-only; no orders; signals are NOT changed):

  1. SUPPORT / RESISTANCE ZONES on the 30-minute chart for every coin in the volume-spurt table (the same ALL IN ONE PRO
     order-block zones as the stock page: spurt_zones.live_zones), with an "AT zone" highlight.
  2. WATCH LIST: a coin that reaches CRYPTO_WATCH_MULT (default 3) times its 50-candle volume average is watched for the rest of the
     UTC day (Delta's day, 05:30 IST to 05:30 IST). A phone alert is sent the first time a watched coin is AT a support / resistance zone.
  3. OB MITIGATION LINK: when Section A fires OB_BUY / OB_SELL on the 30-minute chart for a coin that spurted, you get one phone
     alert and the signal is listed in a table.

The page block is drawn by html(); crypto_live.py calls update() once per cycle (every 30 minutes).

ENV (all optional - the defaults work without any Render setting)
  CRYPTO_WATCH_MULT=3        spurt multiple that puts a coin on the watch list
  CRYPTO_ZONE_ALERT=true     phone alert when a watched coin is at a zone
  CRYPTO_LINK=true           phone alert + table for OB_BUY / OB_SELL on spurt coins
  CRYPTO_WATCH_MAX_PER_DAY=40   safety cap per kind of notification, per day
"""
import os
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger("crypto_watch")

WATCH_MULT = float(os.environ.get("CRYPTO_WATCH_MULT", "3") or "3")
ZONE_ALERT_ON = os.environ.get("CRYPTO_ZONE_ALERT", "true").strip().lower() == "true"
LINK_ON = os.environ.get("CRYPTO_LINK", "true").strip().lower() == "true"
MAX_PER_DAY = int(os.environ.get("CRYPTO_WATCH_MAX_PER_DAY", "40") or "40")
LINK_SIGNALS = ("OB_BUY", "OB_SELL")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
STATE_JSON = os.path.join(RESULTS_DIR, "crypto_watch.json")

_st = {"day": None, "seen": {}, "zone_done": [], "link_done": [], "links": [], "zone_n": 0, "link_n": 0}


# ----------------------------------------------------------------------------------------------- zones
def ob_zones(df):
    """Order-block support / resistance zones from closed candles (same engine as the stock page). [] on any problem."""
    try:
        import spurt_zones
        return spurt_zones.live_zones(df)
    except Exception as e:
        log.warning(f"Crypto zone calculation failed: {e}")
        return []


def zone_fields(zs, price, candle_chg, near, against, tf="30m"):
    """Nearest support below / resistance above the price, whether the price is AT one (within `near`, a fraction), and whether the
    candle direction agrees with it. Same rules as spurt10_live._zone_fields; the buffers are this coin's own."""
    out = {"support": "", "support_dist_pct": None, "resistance": "", "resistance_dist_pct": None,
           "zone_view": "no zone", "zone_src": f"Section A {tf}", "at_zone": ""}
    try:
        if not zs or not price:
            out["zone_src"] = ""
            return out
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
            elif (up and zres and (at_r or 0 <= (zres["lo"] - price) / price <= against)) or \
                 (not up and zsup and (at_s or 0 <= (price - zsup["hi"]) / price <= against)):
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


# ----------------------------------------------------------------------------------------------- state
def _save():
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(STATE_JSON + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE_JSON + ".tmp", STATE_JSON)
    except Exception as e:
        log.warning(f"Crypto watch: could not save state: {e}")


def _roll(day):
    """New UTC day -> start fresh. After a restart, today's state is read back from the file."""
    if _st["day"] == day:
        return
    fresh = {"day": day, "seen": {}, "zone_done": [], "link_done": [], "links": [], "zone_n": 0, "link_n": 0}
    try:
        if _st["day"] is None and os.path.exists(STATE_JSON):
            with open(STATE_JSON) as f:
                old = json.load(f)
            if old.get("day") == day:
                fresh = {**fresh, **old}
    except Exception as e:
        log.warning(f"Crypto watch: could not read state: {e}")
    _st.clear()
    _st.update(fresh)


def _ntfy(title, lines, tags):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                             headers={"Title": title, "Priority": "high", "Tags": tags})
        if resp.status_code >= 300:
            log.warning(f"Crypto watch ntfy refused: {resp.status_code}")
    except Exception as e:
        log.warning(f"Crypto watch ntfy failed: {e}")


# ----------------------------------------------------------------------------------------------- one cycle
def update(spurt, cur_sigs, now_ist, tf="30m"):
    """spurt = this cycle's scored coin rows (already carrying the zone fields); cur_sigs = this cycle's Section A/B/RSI signals.
    Returns the dict the page draws. Never raises into the caller (crypto_live catches errors anyway)."""
    day = now_ist.astimezone(timezone.utc).strftime("%Y-%m-%d")
    hhmm = now_ist.strftime("%H:%M")
    _roll(day)
    by_sym = {r["symbol"]: r for r in spurt}
    # 1 watch list
    for r in spurt:
        if r["multiple"] >= WATCH_MULT:
            cur = _st["seen"].get(r["symbol"])
            if cur is None:
                _st["seen"][r["symbol"]] = {"mult": float(r["multiple"]), "at": r["candle"]}
            elif r["multiple"] > cur["mult"]:
                cur["mult"] = float(r["multiple"])
    watch = []
    for sym, w in _st["seen"].items():
        r = by_sym.get(sym)
        if r is not None:
            w["last"] = {"price": r["price"], "support": r.get("support", ""), "resistance": r.get("resistance", ""),
                         "at_zone": r.get("at_zone", ""), "time": hhmm}
        last = w.get("last") or {}
        watch.append({"symbol": sym, "since": w["at"], "mult": w["mult"], "price": last.get("price"), "support": last.get("support", ""),
                      "resistance": last.get("resistance", ""), "at_zone": last.get("at_zone", ""), "time": last.get("time", ""),
                      "alerted": any(k[0] == sym for k in _st["zone_done"])})
    watch.sort(key=lambda x: (0 if x["at_zone"] else 1, -x["mult"]))

    # 2 phone alert: watched coin AT a zone (once per coin + zone per day)
    if ZONE_ALERT_ON and _st["zone_n"] < MAX_PER_DAY:
        hits = []
        for w in watch:
            if w["at_zone"]:
                zone = w["support"] if w["at_zone"] == "SUPPORT" else w["resistance"]
                key = [w["symbol"], w["at_zone"], zone]
                if key not in _st["zone_done"]:
                    hits.append((w, key))
        if hits:
            lines = []
            for w, _k in hits[:12]:
                r = by_sym.get(w["symbol"]) or {}
                chg = r.get("candle_chg_pct")
                zone = w["support"] if w["at_zone"] == "SUPPORT" else w["resistance"]
                lines.append(f"{w['at_zone']}  {w['symbol']}  {w['price']:g}  zone {zone}  spurt {w['mult']:.1f}x"
                             + (f"  candle {chg:+.2f}%" if chg is not None else "")
                             + (f"  [{r['zone_view']}]" if r.get("zone_view") in ("ALIGNED", "AGAINST") else ""))
            kinds = " / ".join(sorted({w["at_zone"].lower() for w, _k in hits}))
            _ntfy(f"Crypto spurt coin at {kinds} ({len(hits)})", lines, "bell")
            for _w, k in hits:
                _st["zone_done"].append(k)
            _st["zone_n"] += 1

    # 3 OB mitigation signal on a coin that spurted
    new = []
    if LINK_ON:
        for s in cur_sigs:
            if s.get("section") != "A" or s.get("tf") != tf or s.get("signal") not in LINK_SIGNALS or s["symbol"] not in _st["seen"]:
                continue
            key = [s["symbol"], s["signal"], s["bar"]]
            if key in _st["link_done"]:
                continue
            sp = _st["seen"][s["symbol"]]
            row = {"bar": s["bar"], "symbol": s["symbol"], "signal": s["signal"], "price": s.get("price"), "sl": s.get("sl"),
                   "t1": s.get("t1"), "t2": s.get("t2"), "mult": sp["mult"], "at": sp["at"], "alert": hhmm}
            _st["links"].append(row)
            _st["link_done"].append(key)
            new.append(row)
        if new and _st["link_n"] < MAX_PER_DAY:
            lines = [f"{n['signal']}  {n['symbol']}  {n['price']}" + (f"  SL {n['sl']}" if n.get("sl") is not None else "")
                     + (f"  T1 {n['t1']}" if n.get("t1") is not None else "") + f"  | spurt {n['mult']:.1f}x at {n['at']}" for n in new[:12]]
            _ntfy("Crypto OB signal on spurt coin: " + ", ".join(sorted({n["signal"] for n in new})) + f" ({len(new)})", lines, "zap")
            _st["link_n"] += 1
    _save()
    return {"day": day, "watch": watch, "links": list(_st["links"]), "watch_mult": WATCH_MULT, "tf": tf}


# ----------------------------------------------------------------------------------------------- page block
def html(w):
    """Dark-theme HTML for the crypto page: OB signals on spurt coins, then the watch list. '' if there is nothing yet."""
    if not w:
        return ""
    G, R, M = "#3fb950", "#f85149", "#9aa0a6"
    SUP, RES = "rgba(63,185,80,.16)", "rgba(248,81,73,.16)"
    SUPC, RESC = "rgba(63,185,80,.45)", "rgba(248,81,73,.45)"
    tf = w.get("tf", "30m")

    def tbl(cols, rows):
        h = "".join("<th>" + str(c) + "</th>" for c in cols)
        b = ""
        for style, cells in rows:
            tr = "<tr style='background:" + style + "'>" if style else "<tr>"
            tds = ""
            for c in cells:
                if isinstance(c, tuple):
                    tds += "<td style='background:" + c[1] + "'>" + str(c[0]) + "</td>"
                else:
                    tds += "<td>" + str(c) + "</td>"
            b += tr + tds + "</tr>"
        return '<div style="overflow-x:auto"><table><tr>' + h + "</tr>" + b + "</table></div>"

    out = []
    links = w.get("links") or []
    out.append(f'<h2>OB mitigation signals on spurt coins ({len(links)}) &middot; Section A {tf}</h2>'
               f'<div class="m">Section A OB_BUY / OB_SELL on a coin that reached {w.get("watch_mult", 3):g}x or more today (UTC day). A phone alert is sent for each.</div>')
    if links:
        rows = [(SUP if str(x["signal"]).endswith("BUY") else RES,
                 [x["bar"], f"<b>{x['symbol']}</b>", x["signal"], x["price"], x.get("sl") if x.get("sl") is not None else "-",
                  x.get("t1") if x.get("t1") is not None else "-", x.get("t2") if x.get("t2") is not None else "-",
                  f"{x['mult']:.1f}x", x["at"], x["alert"]]) for x in reversed(links)]
        out.append(tbl(["Signal bar (IST)", "Symbol", "Signal", "Entry", "SL", "T1", "T2", "Spurt (best)", "Spurt at", "Alert time"], rows))
    else:
        out.append(f'<p style="color:{M}">None yet today.</p>')

    wl = w.get("watch") or []
    at_now = sum(1 for x in wl if x.get("at_zone"))
    out.append(f'<h2>Watch list &mdash; {len(wl)} spurt coin(s) waiting for a support / resistance zone{(" &middot; " + str(at_now) + " at a zone now") if wl else ""}</h2>'
               f'<div class="m">A coin joins when it reaches {w.get("watch_mult", 3):g}x or more and stays until the UTC day ends (05:30 IST). '
               f'A phone alert is sent the first time it is at a zone. Zones = ALL IN ONE PRO order blocks on the {tf} chart. Updated every 30 min.</div>')
    if wl:
        rows = []
        for i, x in enumerate(wl, 1):
            at = x.get("at_zone", "")
            rows.append((SUP if at == "SUPPORT" else RES if at == "RESISTANCE" else "",
                         [i, f"<b>{x['symbol']}</b>", x.get("since", ""), f"{x['mult']:.1f}x", "-" if x.get("price") is None else f"{x['price']:g}",
                          (x.get("support") or "-", SUPC) if at == "SUPPORT" else (x.get("support") or "-"),
                          (x.get("resistance") or "-", RESC) if at == "RESISTANCE" else (x.get("resistance") or "-"),
                          f"AT {at}" if at else "between zones", "yes" if x.get("alerted") else "-", x.get("time", "")]))
        out.append(tbl(["#", "Symbol", "Spurt at", "Best spurt", "Price", f"Support zone ({tf})", f"Resistance zone ({tf})", "Status now", "Alert sent", "Checked"], rows))
    else:
        out.append(f'<p style="color:{M}">Nothing on the watch list yet.</p>')
    return "".join(out)
