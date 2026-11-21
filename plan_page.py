"""
INTRADAY PLAN  (pages: /plan  = make the plan,  /plan/today = what is happening,  /plan/log.csv = history)                         ADDED 21 Nov 2026

Evening work: look at the daily scan (BUY and SELL candidates), study each stock, mark the levels on the 15-minute chart, then type them here:
tick the stock, choose BUY / SELL, type the ENTRY (BUY: above this price, SELL: below this price) and the STOP LOSS. The plan is for the NEXT trading day.
Next morning the WATCHER follows ONLY the planned stocks on closed 5-minute candles and sends an order to the bridge (planned flag; the bridge sizes it: risk Rs 2000 per position) only when the plan comes true:

  * No order in the first 5 minutes: the first candle (09:15-09:20) is judged only after it closes, so the earliest order is 09:20.
  * BUY: a candle reaches the entry price -> checks -> send. SELL: a candle reaches the entry price from above -> checks -> send.
  * SL touched BEFORE the entry broke (gap down through the SL, or a candle low at / below the SL) = SKIPPED, the plan is dead for the day.
  * A gap that stays between the SL and the entry (BUY: gap down but above SL) keeps WAITING.
  * Gap THROUGH the entry: when the breaking candle closes more than 0.25 R beyond the entry (R = entry - SL) the move has already run = SKIPPED "gapped past the entry". Within 0.25 R it is taken.
  * Rejection check (same as the scanner): the breaking candle must close within the top 25% (BUY) / bottom 25% (SELL) of its own range, and on the right side of the entry price.
  * The order goes only while the breaking candle closed 15 minutes ago or less (a late watcher never replays an old break).
  * 14:30 and nothing broke = EXPIRED (not triggered).
The bridge takes the order with every rule it has (loss lock, 4 symbols a day, cooldown, 14:30 cutoff, one position per stock) - only the 09:30 start rule is relaxed to 09:20 for a PLANNED order.
Everything is caught and logged. The page places no order by itself; the watcher sends the order through the same bridge webhook as the scanner auto-trade (quantity = Rs 2000 risk / (entry - SL), set by the bridge) (and the bridge's scanner auto-trade switch must be ON).
"""
import os
import re
import csv
import json
import time
import html
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests

import swing_page as sw

log = logging.getLogger("plan_page")
IST = timezone(timedelta(hours=5, minutes=30))
ON = True
EARLIEST = (9, 20)               # no order before this (the first 5-minute candle closes)
CUTOFF = (14, 30)                # a plan that has not broken by now is EXPIRED
GAP_TOL_R = 0.25                 # a break candle that closes more than this many R beyond the entry = gapped past the entry
CLOSE_ZONE_PCT = 25.0            # break candle must close within 25% of its high (BUY) / low (SELL)
FRESH_MIN = 15                   # the break candle must have closed this recently
POLL = 15                        # seconds between looks
MAX_ITEMS = 12
BRIDGE_URL = "https://dhan-bridge-tv.onrender.com/webhook/khagenindrajeetmuktakabitakartiksanjucta"
PLAN_PATH = os.path.join(sw.RESULTS_DIR, "intraday_plan.json")
LOG_PATH = os.path.join(sw.RESULTS_DIR, "plan_log.csv")
LOG_COLS = ["day", "symbol", "side", "entry", "sl", "qty", "status", "reason", "break_candle", "sent_price", "signals", "note", "updated"]
_lock = threading.RLock()
_started = False


def _now():
    return datetime.now(IST)


# ---------------------------------------------------------------------------------------------------------- storage
def load():
    try:
        with open(PLAN_PATH) as f:
            p = json.load(f)
        if isinstance(p, dict) and isinstance(p.get("items"), list):
            return p
    except Exception:
        pass
    return {"day": "", "items": [], "saved": ""}


def _save(p):
    try:
        os.makedirs(sw.RESULTS_DIR, exist_ok=True)
        with open(PLAN_PATH + ".tmp", "w") as f:
            json.dump(p, f)
        os.replace(PLAN_PATH + ".tmp", PLAN_PATH)
    except Exception as e:
        log.warning("plan: could not save: %s", e)


def plan_day(now=None):
    """The trading day a plan saved NOW is for: today until 14:30 on a weekday, otherwise the next weekday (exchange holidays are not known - change the day by saving again)."""
    now = now or _now()
    d = now.date()
    if now.weekday() < 5 and (now.hour, now.minute) < CUTOFF:
        return d.strftime("%Y-%m-%d")
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def _log_row(day, it):
    try:
        new = not os.path.exists(LOG_PATH)
        with open(LOG_PATH, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(LOG_COLS)
            w.writerow([day, it["symbol"], it["side"], it["entry"], it["sl"], it.get("qty") or "auto", it["status"], it.get("reason", ""), it.get("brk", ""), it.get("sent_price", ""),
                        it.get("signals", ""), it.get("note", ""), it.get("updated", "")])
    except Exception as e:
        log.warning("plan: log failed: %s", e)


# ---------------------------------------------------------------------------------------------------------- the rules (pure, testable)
def evaluate(it, bars, now):
    """it = plan item {side, entry, sl}; bars = CLOSED 5-minute candles of the day, oldest first: [(open time IST naive datetime, open, high, low, close)]; now = IST datetime.
    Returns (status, reason, break_candle_text, price) with status WAITING / SKIPPED / EXPIRED / SEND."""
    side, entry, sl = it["side"], float(it["entry"]), float(it["sl"])
    buy = side == "BUY"
    r = abs(entry - sl)
    for t, o, h, l, c in bars:
        ctxt = t.strftime("%Y-%m-%d %H:%M")
        close_at = t + timedelta(minutes=5)
        if (close_at.hour, close_at.minute) < EARLIEST:
            continue                                                            # (a candle can never close before 09:20, kept as a guard)
        hit_sl = (l <= sl) if buy else (h >= sl)
        reached = (h >= entry) if buy else (l <= entry)
        if hit_sl:
            return "SKIPPED", "SL %s touched before the entry broke (candle %s) - plan dead for the day" % (sw._f(sl), t.strftime("%H:%M")), ctxt, None
        if not reached:
            continue
        # the level broke on this candle
        beyond = (c - entry) if buy else (entry - c)
        if beyond < 0:
            return "SKIPPED", "candle %s touched the entry but closed back on the wrong side of it (%s) - not a break" % (t.strftime("%H:%M"), sw._f(c)), ctxt, None
        if beyond > GAP_TOL_R * r:
            return "SKIPPED", "gapped past the entry: candle %s closed %.2f R beyond it (limit %.2f R)" % (t.strftime("%H:%M"), beyond / r if r else 0, GAP_TOL_R), ctxt, None
        rng = h - l
        if rng > 0:
            cp = ((h - c) if buy else (c - l)) / rng * 100.0
            if round(cp, 1) > CLOSE_ZONE_PCT:
                return "SKIPPED", "rejection: candle %s closed %.0f%% away from its %s (limit %.0f%%)" % (t.strftime("%H:%M"), cp, "high" if buy else "low", CLOSE_ZONE_PCT), ctxt, None
        age = (now.replace(tzinfo=None) - close_at.replace(tzinfo=None)).total_seconds() / 60.0
        if age > FRESH_MIN:
            return "SKIPPED", "break at %s is too old (%.0f min) - not sent" % (t.strftime("%H:%M"), age), ctxt, None
        return "SEND", "entry %s broke on candle %s" % (sw._f(entry), t.strftime("%H:%M")), ctxt, round(float(c), 2)
    if (now.hour, now.minute) >= CUTOFF:
        return "EXPIRED", "the entry did not break before %02d:%02d" % CUTOFF, "", None
    return "WAITING", "", "", None


# ---------------------------------------------------------------------------------------------------------- sending
def _send(it):
    """1 share to the bridge, source scanner, planned flag. Returns the answer text."""
    pay = {"type": "SPURT_CHOCH_BUY" if it["side"] == "BUY" else "SPURT_CHOCH_SELL", "symbol": it["symbol"], "side": it["side"].lower(), "price": it["sent_price"], "sl": float(it["sl"]),
           "qty": 1, "fixed_lot": "false", "source": "scanner", "tf": 5, "planned": "true"}   # no quantity typed: the bridge sizes a planned trade by risk, Rs 2000 / (entry - SL)
    if it.get("qty"):                                                                 # quantity typed on the plan page: sent exactly as saved
        pay["qty"], pay["fixed_lot"] = int(it["qty"]), "true"
    last = ""
    for n in range(3):
        try:
            r = requests.post(BRIDGE_URL, json=pay, timeout=15)
            last = "HTTP %s %s" % (r.status_code, (r.text or "")[:80])
            if r.status_code < 500:
                break
        except Exception as e:
            last = "error: %s" % e
        time.sleep(5)
    return last


# ---------------------------------------------------------------------------------------------------------- the watcher
def _bars_today(sid, now):
    """Closed 5-minute candles of today for one stock as [(open time IST, o, h, l, c)]. The function runs in module plan_page: the shared Dhan rate limiter gives it the top priority."""
    import pandas as pd
    import live_scanner as ls
    df = ls.fetch_intraday_history(sid, "NSE_EQ", 5, 1)
    if df is None or not len(df):
        return []
    df = ls.drop_forming_bars(df, 5, pd.Timestamp(datetime.utcnow()))
    out = []
    today = now.strftime("%Y-%m-%d")
    for ts, row in df.iterrows():
        t = (pd.Timestamp(ts) + pd.Timedelta(hours=5, minutes=30)).to_pydatetime().replace(tzinfo=None)
        if t.strftime("%Y-%m-%d") == today:
            out.append((t, float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])))
    return out


def tick(now=None, bars_fn=None, send_fn=None):
    """One look at every WAITING planned stock (only today's plan, only 09:20 - 14:30 + the expiry look). Returns the number of items changed."""
    now = now or _now()
    if not ON or now.weekday() >= 5:
        return 0
    hm = (now.hour, now.minute)
    if hm < EARLIEST or hm > (15, 0):
        return 0
    bars_fn = bars_fn or _bars_today
    send_fn = send_fn or _send
    changed = 0
    with _lock:
        p = load()
        if p.get("day") != now.strftime("%Y-%m-%d"):
            return 0
        items = [it for it in p["items"] if it.get("status") == "WAITING"]
    for it in items:
        try:
            bars = bars_fn(it["sid"], now)
            if not bars and hm < CUTOFF:
                continue
            st, why, brk, price = evaluate(it, bars, now)
            if st == "WAITING":
                continue
            with _lock:
                p = load()
                cur = next((x for x in p["items"] if x["id"] == it["id"]), None)
                if cur is None or cur.get("status") != "WAITING":
                    continue
                cur["reason"], cur["brk"], cur["updated"] = why, brk, now.strftime("%H:%M:%S")
                if st == "SEND":
                    cur["status"], cur["sent_price"] = "TRIGGERED", price
                    _save(p)                                                          # saved BEFORE the send: a restart can never send it twice
                    ans = send_fn(cur)
                    p = load()
                    cur = next((x for x in p["items"] if x["id"] == it["id"]), cur)
                    cur["status"], cur["reason"] = "SENT", why + " - bridge: " + ans
                else:
                    cur["status"] = st
                _save(p)
                _log_row(p["day"], cur)
                changed += 1
        except Exception as e:
            log.warning("plan: %s failed: %s", it.get("symbol"), e)
    return changed


def _loop():
    while True:
        try:
            tick()
        except Exception as e:
            log.warning("plan loop: %s", e)
        time.sleep(POLL)


# ---------------------------------------------------------------------------------------------------------- pages
_CSS = ("<style>body{background:#0f1419;color:#e6edf3;font-family:Arial,sans-serif;margin:18px}table{border-collapse:collapse;width:100%}th,td{padding:7px 10px;border-bottom:1px solid #243040;"
        "text-align:left;font-size:14px;vertical-align:top}th{color:#9fb0c0}a{color:#8ab4f8}input[type=text],select{background:#16202b;color:#e6edf3;border:1px solid #35495e;border-radius:5px;padding:5px;width:90px}"
        "input.sym{width:110px}input.note{width:200px}.btn{background:#1b6e3c;color:#fff;border:0;border-radius:6px;padding:9px 22px;font-size:15px;cursor:pointer}.sell{color:#ff7070}.buy{color:#7be08a}</style>")


def _candidates():
    """Rows of the latest daily scan day, grouped by stock: {symbol: [rows]} and the day."""
    try:
        import daily_scan as ds
        with ds._lock:
            rows = list(ds._load())
    except Exception:
        return "", {}
    if not rows:
        return "", {}
    day = max(r["date"] for r in rows)
    g = {}
    for r in rows:
        if r["date"] == day:
            g.setdefault(r["symbol"], []).append(r)
    return day, g


def plan_html(msg=""):
    day, g = _candidates()
    p = load()
    target = plan_day()
    saved = {it["symbol"]: it for it in p["items"]} if p.get("day") == target else {}
    syms = sorted(g, key=lambda s: (-len({(x["group"], x["signal"]) for x in g[s]}), s))
    body = []
    k = 0

    def row(sym, rows, it):
        nonlocal k
        sides = {x.get("side", "BUY") for x in rows} if rows else set()
        side0 = it["side"] if it else ("SELL" if sides == {"SELL"} else "BUY")
        sigs = "<br>".join("<span class='%s'>%s</span> %s <span style='color:#9fb0c0'>E %s / SL %s</span>" % ("sell" if x.get("side") == "SELL" else "buy", html.escape(x.get("side", "BUY")), html.escape(x["signal"]),
                                                                                                           sw._f(x["entry"]), sw._f(x["sl"])) for x in rows) if rows else "<span style='color:#9fb0c0'>typed by hand</span>"
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=15" % html.escape(sym)
        symcell = ("<b><a href='%s' target='_blank' rel='noopener'>%s</a></b><input type='hidden' name='sym_%d' value='%s'>" % (tv, html.escape(sw._sym_label(sym)), k, html.escape(sym))) if sym else \
                  "<input class='sym' type='text' name='sym_%d' placeholder='SYMBOL'>" % k
        body.append("<tr><td><input type='checkbox' name='tick_%d' value='1'%s></td><td>%s</td><td>%s</td><td><select name='side_%d'><option%s>BUY</option><option%s>SELL</option></select></td>"
                    "<td><input type='text' name='en_%d' value='%s' inputmode='decimal'></td><td><input type='text' name='sl_%d' value='%s' inputmode='decimal'></td><td><input type='text' name='qty_%d' value='%s' inputmode='numeric' placeholder='auto'></td><td><input class='note' type='text' name='note_%d' value='%s'></td></tr>" % (
                        k, " checked" if it else "", symcell, sigs, k, " selected" if side0 == "BUY" else "", " selected" if side0 == "SELL" else "",
                        k, sw._f(it["entry"]) if it else "", k, sw._f(it["sl"]) if it else "", k, str(it.get("qty") or "") if it else "", k, html.escape(it.get("note", "")) if it else ""))
        k += 1

    for s in syms:
        row(s, g[s], saved.get(s))
    for s, it in saved.items():                                   # planned stocks that are not in today's daily scan list stay editable
        if s not in g:
            row(s, [], it)
    for _ in range(3):
        row("", [], None)
    note = ""
    if msg:
        note = "<p style='background:#223245;border-radius:6px;padding:8px 12px'>%s</p>" % html.escape(msg)
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Intraday plan</title>" + _CSS.replace("%", "%%") + "</head><body>"
            "<h2>INTRADAY PLAN for %s</h2>%s<p style='color:#9fb0c0'>Candidates = the daily scan of %s (BUY and SELL, SELL = intraday only). Analyse the stock, mark the levels on the 15-minute chart, then tick it and type "
            "<b>ENTRY</b> (BUY: order only after price goes ABOVE it - SELL: only after price goes BELOW it) and <b>STOP LOSS</b>. Max %d stocks. Saving replaces the whole plan of that day. "
            "Tomorrow the watcher follows only these stocks on closed 5-minute candles; earliest order 09:20; a gap through the entry (more than %.2f R beyond) is skipped; SL touched before the entry = dead; "
            "a candle that closes more than %d%% away from its high / low (rejection) is skipped. Qty: type the quantity you want and it is sent exactly as saved; leave it empty and the bridge uses Rs 2000 risk / (entry - SL), rounded down. <a href='/plan/today'>What is happening today</a> &middot; <a href='/daily_scan'>Daily scan</a> &middot; "
            "<a href='/plan/log.csv' target='_blank' rel='noopener'>History (CSV)</a></p>"
            "<form method='post' action='/plan/save'><table><tr><th>Tick</th><th>Stock</th><th>Daily scan signals</th><th>Side</th><th>Entry</th><th>SL</th><th>Qty</th><th>Note (your reason)</th></tr>%s</table>"
            "<p><button class='btn' type='submit'>Save plan</button></p></form></body></html>") % (target, note, day or "-", MAX_ITEMS, GAP_TOL_R, int(CLOSE_ZONE_PCT), "".join(body))


def today_html():
    p = load()
    now = _now()
    items = p["items"] if p.get("day") == now.strftime("%Y-%m-%d") else []
    col = {"WAITING": "#ffd54f", "TRIGGERED": "#8ab4f8", "SENT": "#7be08a", "SKIPPED": "#ff7070", "EXPIRED": "#9fb0c0"}
    body = []
    for it in items:
        tv = "https://www.tradingview.com/chart/?symbol=NSE%%3A%s&interval=5" % html.escape(it["symbol"])
        body.append("<tr><td><b><a href='%s' target='_blank' rel='noopener'>%s</a></b></td><td class='%s'>%s</td><td>%s</td><td>%s</td><td>%s</td><td style='color:%s;font-weight:700'>%s</td><td>%s</td><td style='color:#9fb0c0'>%s</td></tr>" % (
            tv, html.escape(sw._sym_label(it["symbol"])), "sell" if it["side"] == "SELL" else "buy", it["side"], sw._f(it["entry"]), sw._f(it["sl"]), it.get("qty") or "auto (Rs 2000 risk)", col.get(it["status"], "#fff"), it["status"],
            html.escape(it.get("reason", "")), html.escape(it.get("note", ""))))
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='30'><title>Plan today</title>" + _CSS.replace("%", "%%") + "</head><body>"
            "<h2>PLAN OF TODAY (%s)</h2><p style='color:#9fb0c0'>Refreshes every 30 seconds. Now %s IST. <a href='/plan'>Edit / make the plan</a> &middot; <a href='/plan/log.csv' target='_blank' rel='noopener'>History (CSV)</a></p>"
            "%s</body></html>") % (now.strftime("%d %b %Y"), now.strftime("%H:%M:%S"),
                                   ("<table><tr><th>Stock</th><th>Side</th><th>Entry</th><th>SL</th><th>Qty</th><th>Status</th><th>What happened</th><th>Note</th></tr>" + "".join(body) + "</table>") if body else "<p>No plan for today.</p>")


def save_form(form, now=None):
    """Builds and saves the plan from the posted form. Returns (message, ok)."""
    now = now or _now()
    uni = {}
    try:
        uni = {s: sid for s, sid in sw._universe()}
    except Exception as e:
        log.warning("plan: universe failed: %s", e)
    day, g = _candidates()
    items, errs = [], []
    ks = sorted({int(m.group(1)) for k in form for m in [re.fullmatch(r"sym_(\d+)", k)] if m})
    for k in ks:
        if not form.get("tick_%d" % k):
            continue
        sym = (form.get("sym_%d" % k) or "").strip().upper()
        if not sym:
            continue
        side = (form.get("side_%d" % k) or "BUY").upper()
        try:
            en, sl = float(str(form.get("en_%d" % k, "")).replace(",", "")), float(str(form.get("sl_%d" % k, "")).replace(",", ""))
        except ValueError:
            errs.append("%s: entry or SL is not a number" % sym)
            continue
        if side not in ("BUY", "SELL") or en <= 0 or sl <= 0 or (side == "BUY" and sl >= en) or (side == "SELL" and sl <= en):
            errs.append("%s: for a %s the SL must be %s the entry" % (sym, side, "below" if side == "BUY" else "above"))
            continue
        qraw = str(form.get("qty_%d" % k, "")).strip().replace(",", "")
        qty = 0
        if qraw:
            try:
                qty = int(float(qraw))
            except ValueError:
                qty = -1
            if qty < 1 or qty > 100000:
                errs.append("%s: quantity must be a whole number of 1 or more (or empty = Rs 2000 risk)" % sym)
                continue
        if sym not in uni:
            errs.append("%s: not in the scanner's stock list" % sym)
            continue
        if any(x["symbol"] == sym for x in items):
            errs.append("%s: listed twice" % sym)
            continue
        sig = "; ".join("%s %s" % (x.get("side", "BUY"), x["signal"]) for x in g.get(sym, []))
        items.append({"id": "%s-%d" % (sym, k), "symbol": sym, "sid": uni[sym], "side": side, "entry": round(en, 2), "sl": round(sl, 2), "qty": qty or None, "note": (form.get("note_%d" % k) or "").strip()[:80],
                      "signals": sig, "status": "WAITING", "reason": "", "brk": "", "updated": ""})
    if len(items) > MAX_ITEMS:
        errs.append("more than %d stocks - plan not saved" % MAX_ITEMS)
    if errs:
        return "Plan NOT saved: " + " | ".join(errs), False
    with _lock:
        _save({"day": plan_day(now), "items": items, "saved": now.strftime("%Y-%m-%d %H:%M")})
    return "Plan saved for %s: %d stocks." % (plan_day(now), len(items)), True


def register(app, admin_check=None):
    """Adds /plan, /plan/save, /plan/today, /plan/log.csv and starts the watcher thread (called from daily_scan.register)."""
    global _started
    from flask import request, Response, redirect

    def _plan():
        try:
            return plan_html(request.args.get("m", ""))
        except Exception:
            log.exception("/plan failed")
            return "Plan page failed - see the logs.", 500

    def _savep():
        try:
            if admin_check is not None and not admin_check():
                return "Only the admin can save the plan.", 403
            msg, _ok = save_form(request.form)
            return redirect("/plan?m=" + requests.utils.quote(msg))
        except Exception:
            log.exception("/plan/save failed")
            return "Could not save the plan - see the logs.", 500

    def _today():
        try:
            return today_html()
        except Exception:
            log.exception("/plan/today failed")
            return "Plan page failed - see the logs.", 500

    def _csv():
        try:
            txt = open(LOG_PATH).read() if os.path.exists(LOG_PATH) else ",".join(LOG_COLS) + "\n"
        except Exception:
            txt = ",".join(LOG_COLS) + "\n"
        return Response(txt, mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=plan_log.csv"})

    app.add_url_rule("/plan", "plan_page", _plan, methods=["GET"])
    app.add_url_rule("/plan/save", "plan_save", _savep, methods=["POST"])
    app.add_url_rule("/plan/today", "plan_today", _today, methods=["GET"])
    app.add_url_rule("/plan/log.csv", "plan_log", _csv, methods=["GET"])
    if ON and not _started:
        _started = True
        threading.Thread(target=_loop, daemon=True, name="plan-watcher").start()
