"""
SPURT-AT-ZONE REVERSAL  (page: /spz    csv: /spz.csv)      PAPER TRADES + PHONE ALERTS ONLY - it never sends an order.

Khagen's rule (2026-10-10), both sides:

  SHORT   1. a candle with a volume spurt (>= SPZ_SPURT_MULT x the previous-50-candle average volume, and the biggest
             volume of the day so far) reaches an OI RESISTANCE zone (futures OI buildup zones from oi_zones_live)
          2. then a RED candle with volume above the average (within SPZ_TRIGGER_BARS candles)
          3. ENTRY  = close of that red candle
          4. SL     = highest high from the spurt candle up to the red candle (the swing high)
  LONG    the mirror: spurt candle reaches an OI SUPPORT zone, then a GREEN candle with volume above the average,
          entry = its close, SL = lowest low from the spurt candle up to the green candle.

  AFTER ENTRY (the "volume must not dry up" check): wait SPZ_WAIT_BARS candles (default 3, you said 3-4).
          At the close of that last waiting candle the trade is exited if
              volume did not come  = none of the waiting candles went in our direction with volume >= the average
              price not going      = the close is not at least SPZ_PROGRESS_R x risk beyond the entry, in our direction
          SPZ_EXIT_LOGIC=and (default) exits when BOTH are true; =or exits when EITHER is true.
  OTHER EXITS: stop hit (a candle that touches both the stop and the target counts as the STOP, and a gap through the
          stop exits at the open), target hit (nearest opposite OI zone, or 2R when there is none / it is closer than 1R),
          15:10 IST square-off.

HOW IT RUNS: every 10 minutes at candle close (+45 s), it downloads the 5-min candles of the stocks that have OI zones
(merged to closed 10-min candles) and RE-PLAYS today's session from scratch. So it needs no saved state: after a redeploy
it rebuilds the day from the candles. Only the "already alerted" memory and the closed-trade history live on disk.

ENV (all optional)
  SPZ_AUTORUN=true        SPZ_SPURT_MULT=3.0        SPZ_REQUIRE_DAY_MAX=true   SPZ_TRIGGER_BARS=6
  SPZ_NEAR_PCT=0.5        (how close the spurt candle's high/low must come to the zone edge, in %)
  SPZ_TRIG_VOL_MULT=1.0   (trigger candle volume must be >= this x the 50-candle average)
  SPZ_WAIT_BARS=3         SPZ_PROGRESS_R=0.5        SPZ_EXIT_LOGIC=and
  SPZ_SL_BUFFER_PCT=0.0   (extra room beyond the swing, in %)      SPZ_MAX_RISK_PCT=2.0 (skip if the stop is further than this)
  SPZ_START=09:30  SPZ_CUTOFF=14:30   (entry window, by the trigger candle's close time)
  SPZ_SIDES=both|short|long         SPZ_PAPER_RISK_RS=2000   (rupees risked per paper trade)
  SPZ_NTFY=true   SPZ_NTFY_MAX_PER_DAY=40   (uses NTFY_TOPIC, like the other scanner alerts)
  SPZ_WORKERS=4
"""

import os
import io
import csv
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("spz")
IST = ZoneInfo("Asia/Kolkata")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
LIVE_PATH = os.path.join(RESULTS_DIR, "spz_live.json")
CSV_PATH = os.path.join(RESULTS_DIR, "spz.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "spz_history.csv")
ALERT_PATH = os.path.join(RESULTS_DIR, "spz_alerted.json")
HISTORY_DAYS = 8
SMA_LEN = 50
SQUAREOFF_MIN = 15 * 60 + 10          # 15:10 IST: exit at the close of the 15:00 candle


def _f(name, default):
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return float(default)


def _hhmm(name, default):
    h, m = (os.environ.get(name, default) or default).strip().split(":")
    return int(h) * 60 + int(m)


def cfg():
    """Read at call time, so a Render env change applies on the next cycle."""
    return {
        "autorun": os.environ.get("SPZ_AUTORUN", "true").strip().lower() == "true",
        "spurt_mult": _f("SPZ_SPURT_MULT", 3.0),
        "require_day_max": os.environ.get("SPZ_REQUIRE_DAY_MAX", "true").strip().lower() == "true",
        "trigger_bars": int(_f("SPZ_TRIGGER_BARS", 6)),
        "near": _f("SPZ_NEAR_PCT", 0.5) / 100.0,
        "trig_vol_mult": _f("SPZ_TRIG_VOL_MULT", 1.0),
        "wait_bars": int(_f("SPZ_WAIT_BARS", 3)),
        "progress_r": _f("SPZ_PROGRESS_R", 0.5),
        "exit_logic": os.environ.get("SPZ_EXIT_LOGIC", "and").strip().lower(),
        "sl_buffer": _f("SPZ_SL_BUFFER_PCT", 0.0) / 100.0,
        "max_risk": _f("SPZ_MAX_RISK_PCT", 2.0) / 100.0,
        "start_min": _hhmm("SPZ_START", "09:30"),
        "cutoff_min": _hhmm("SPZ_CUTOFF", "14:30"),
        "sides": os.environ.get("SPZ_SIDES", "both").strip().lower(),
        "risk_rs": _f("SPZ_PAPER_RISK_RS", 2000),
        "ntfy": os.environ.get("SPZ_NTFY", "true").strip().lower() == "true",
        "ntfy_max": int(_f("SPZ_NTFY_MAX_PER_DAY", 40)),
        "workers": int(_f("SPZ_WORKERS", 4)),
    }


# ======================================================================================= the rule (pure, testable)
def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def replay(df, zones, c):
    """df: CLOSED 10-minute candles (index = bar START, UTC like Dhan; columns open high low close volume), several
    sessions long. zones: the stock's OI zones [{type, lo, hi, ...}]. Returns the list of trades found TODAY (the last
    session in df), each followed candle by candle up to the last closed candle. One trade per side per stock per day."""
    if df is None or len(df) < SMA_LEN + 5 or not zones:
        return []
    idx = [_ist(t) for t in df.index]
    day = np.array([t.date() for t in idx])
    today = day[-1]
    o = df["open"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    cl = df["close"].to_numpy(float); v = df["volume"].to_numpy(float)
    n = len(df)
    avg = np.full(n, np.nan)
    for i in range(SMA_LEN, n):
        avg[i] = v[i - SMA_LEN:i].mean()               # previous 50 candles, the candle itself excluded
    first_today = int(np.argmax(day == today))
    mins = np.array([t.hour * 60 + t.minute for t in idx])
    sides = ["SHORT", "LONG"] if c["sides"] == "both" else ["SHORT"] if c["sides"] == "short" else ["LONG"]
    trades = []
    for side in sides:
        want = "RESISTANCE" if side == "SHORT" else "SUPPORT"
        zs = [z for z in zones if z["type"] == want]
        opp = [z for z in zones if z["type"] != want]
        if not zs:
            continue
        trade = None
        for i in range(first_today, n):
            if trade:
                break
            if not (avg[i] > 0):
                continue
            if v[i] < c["spurt_mult"] * avg[i]:
                continue
            if c["require_day_max"] and v[i] < v[first_today:i + 1].max():
                continue
            # the spurt candle must reach a zone of the right kind
            zone = None
            for z in zs:
                if side == "SHORT" and z["lo"] * (1 - c["near"]) <= h[i] <= z["hi"] * (1 + c["near"]):
                    zone = z
                if side == "LONG" and z["lo"] * (1 - c["near"]) <= l[i] <= z["hi"] * (1 + c["near"]):
                    zone = z
                if zone:
                    break
            if not zone:
                continue
            # trigger candle
            for j in range(i + 1, min(i + 1 + c["trigger_bars"], n)):
                if day[j] != today:
                    break
                broke = (cl[j] > zone["hi"] * (1 + c["near"])) if side == "SHORT" else (cl[j] < zone["lo"] * (1 - c["near"]))
                if broke:
                    break                                    # price closed through the zone: the setup is dead
                right_colour = (cl[j] < o[j]) if side == "SHORT" else (cl[j] > o[j])
                if not (right_colour and avg[j] > 0 and v[j] >= c["trig_vol_mult"] * avg[j]):
                    continue
                close_min = mins[j] + 10
                if not (c["start_min"] <= close_min <= c["cutoff_min"]):
                    continue
                entry = cl[j]
                if side == "SHORT":
                    sl = float(h[i:j + 1].max()) * (1 + c["sl_buffer"])
                    risk = sl - entry
                else:
                    sl = float(l[i:j + 1].min()) * (1 - c["sl_buffer"])
                    risk = entry - sl
                if risk <= 0 or risk / entry > c["max_risk"]:
                    break                                    # stop too far / invalid: skip this setup, keep looking
                # target: nearest opposite OI zone edge that is at least 1R away, else 2R
                tgt, tgt_src = None, "2R"
                if side == "SHORT":
                    cand = [z["hi"] for z in opp if z["hi"] < entry - risk]
                    if cand:
                        tgt, tgt_src = max(cand), "OI support"
                    else:
                        tgt = entry - 2 * risk
                else:
                    cand = [z["lo"] for z in opp if z["lo"] > entry + risk]
                    if cand:
                        tgt, tgt_src = min(cand), "OI resistance"
                    else:
                        tgt = entry + 2 * risk
                trade = {"side": side, "symbol": None, "zone": f"{zone['lo']}-{zone['hi']}",
                         "spurt_bar": idx[i].strftime("%H:%M"), "spurt_x": round(v[i] / avg[i], 1),
                         "trigger_bar": idx[j].strftime("%H:%M"), "trigger_vol_x": round(v[j] / avg[j], 1),
                         "entry": round(entry, 2), "sl": round(sl, 2), "target": round(tgt, 2), "target_src": tgt_src,
                         "risk": round(risk, 2), "entry_bar_idx": j, "status": "OPEN", "exit": None, "exit_reason": "",
                         "exit_bar": "", "bars_held": 0, "watch": f"0/{c['wait_bars']}"}
                _follow(trade, o, h, l, cl, v, avg, mins, day, today, n, c)
                break
        if trade:
            trades.append(trade)
    return trades


def _follow(t, o, h, l, cl, v, avg, mins, day, today, n, c):
    """Walk the candles after the entry candle and close the trade the way the rule says."""
    j = t["entry_bar_idx"]
    side, entry, sl, tgt, risk = t["side"], t["entry"], t["sl"], t["target"], t["risk"]
    wait = max(1, c["wait_bars"])
    sgn = 1 if side == "LONG" else -1
    came = False
    for k in range(j + 1, n):
        if day[k] != today:
            break
        held = k - j
        t["bars_held"] = held
        # stop first (pessimistic), with a gap through the stop filled at the open
        if (side == "SHORT" and h[k] >= sl) or (side == "LONG" and l[k] <= sl):
            gap = (side == "SHORT" and o[k] > sl) or (side == "LONG" and o[k] < sl)
            return _close(t, o[k] if gap else sl, "STOP", k, mins)
        if (side == "SHORT" and l[k] <= tgt) or (side == "LONG" and h[k] >= tgt):
            return _close(t, tgt, "TARGET", k, mins)
        if held <= wait:
            in_dir = (cl[k] > o[k]) if side == "LONG" else (cl[k] < o[k])
            if in_dir and avg[k] > 0 and v[k] >= avg[k]:
                came = True
            t["watch"] = f"{held}/{wait}" + (" vol came" if came else " no vol yet")
        if held == wait:
            moved = sgn * (cl[k] - entry) >= c["progress_r"] * risk
            vol_bad, px_bad = (not came), (not moved)
            bad = (vol_bad and px_bad) if c["exit_logic"] != "or" else (vol_bad or px_bad)
            t["watch"] = (f"{wait}/{wait} " + ("volume came" if came else "NO volume") + ", " +
                          ("price moved" if moved else "price NOT moving"))
            if bad:
                return _close(t, cl[k], "DRIED UP", k, mins)
        if mins[k] + 10 >= SQUAREOFF_MIN:
            return _close(t, cl[k], "SQUARE-OFF", k, mins)
    # still open: mark to the last close
    last = cl[n - 1]
    t["mark"] = round(float(last), 2)
    t["r"] = round(sgn * (last - entry) / risk, 2)


def _close(t, px, reason, k, mins):
    sgn = 1 if t["side"] == "LONG" else -1
    t["status"] = "CLOSED"
    t["exit"] = round(float(px), 2)
    t["exit_reason"] = reason
    t["r"] = round(sgn * (px - t["entry"]) / t["risk"], 2) + 0.0
    t["mark"] = t["exit"]
    t["exit_bar"] = f"{(mins[k] + 10) // 60:02d}:{(mins[k] + 10) % 60:02d}"


# ======================================================================================= data + loop
def _ntfy(title, text, tags="rotating_light", priority="high"):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        return False
    try:
        import requests
        r = requests.post(f"https://ntfy.sh/{topic}", data=text.encode("utf-8"), timeout=8,
                          headers={"Title": title, "Priority": priority, "Tags": tags})
        return r.status_code < 300
    except Exception as e:
        log.warning(f"SPZ ntfy failed: {e}")
        return False


def _load_alerted(today):
    try:
        with open(ALERT_PATH) as f:
            d = json.load(f)
        if d.get("date") == today:
            return d
    except Exception:
        pass
    return {"date": today, "keys": [], "count": 0}


def _save_alerted(d):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(ALERT_PATH, "w") as f:
        json.dump(d, f)


def _alerts(trades, c, now):
    if not c["ntfy"] or not os.environ.get("NTFY_TOPIC", "").strip():
        return
    today = now.strftime("%Y-%m-%d")
    st = _load_alerted(today)
    keys = set(st["keys"])
    for t in sorted(trades, key=lambda t: (t["trigger_bar"], t["symbol"])):
        if st["count"] >= c["ntfy_max"]:
            break
        base = f"{t['symbol']}|{t['side']}|{t['trigger_bar']}"
        if base + "|ENTRY" not in keys:
            verb = "SELL" if t["side"] == "SHORT" else "BUY"
            txt = (f"{t['symbol']}  {verb} at {t['entry']}   SL {t['sl']}   target {t['target']} ({t['target_src']})\n"
                   f"spurt {t['spurt_x']}x at {t['spurt_bar']} reached the OI zone {t['zone']}; trigger candle {t['trigger_bar']} "
                   f"({t['trigger_vol_x']}x volume).\nWatch the next {c['wait_bars']} candles: volume must come and price must follow, or exit.")
            if _ntfy(f"SPZ {verb} {t['symbol']} (paper)", txt):
                keys.add(base + "|ENTRY"); st["count"] += 1
        if t["status"] == "CLOSED" and base + "|EXIT" not in keys and st["count"] < c["ntfy_max"]:
            txt = f"{t['symbol']} {t['side']} closed: {t['exit_reason']} at {t['exit']}  ({t['r']:+.2f}R)  entry {t['entry']}"
            if _ntfy(f"SPZ EXIT {t['symbol']} {t['exit_reason']}", txt, tags="checkered_flag", priority="default"):
                keys.add(base + "|EXIT"); st["count"] += 1
    st["keys"] = sorted(keys)
    _save_alerted(st)


def _fetch_closed_10m(sid):
    import live_scanner as ls
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, HISTORY_DAYS)
    return ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of)


def run_once():
    import oi_zones_live as oz
    from concurrent.futures import ThreadPoolExecutor
    c = cfg()
    now = datetime.now(IST)
    zd = oz.load_zones()
    stocks = (zd or {}).get("stocks") or {}
    if not stocks:
        log.info("SPZ: no OI zones built yet - nothing to scan")
        return
    fno = oz.load_fno()
    todo = [(und, str(fno[und][1])) for und in stocks if und in fno]
    out, errors = [], 0

    def one(item):
        und, sid = item
        df = _fetch_closed_10m(sid)
        ts = replay(df, stocks[und], c)
        for t in ts:
            t["symbol"] = und
            t.pop("entry_bar_idx", None)
        return ts

    with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
        futs = [ex.submit(one, it) for it in todo]
        for f in futs:
            try:
                out += f.result()
            except Exception as e:
                errors += 1
                if errors <= 3:
                    log.warning(f"SPZ candle fetch failed: {e}")
    out.sort(key=lambda t: (t["status"] != "OPEN", t["trigger_bar"]))
    qty_of = lambda t: max(1, int(c["risk_rs"] / t["risk"])) if t["risk"] else 0
    for t in out:
        t["qty"] = qty_of(t)
        t["pnl_rs"] = round(t["r"] * c["risk_rs"], 0)
    meta = {"run": now.strftime("%Y-%m-%d %H:%M:%S IST"), "date": now.strftime("%Y-%m-%d"), "stocks": len(todo), "errors": errors,
            "cfg": {k: c[k] for k in ("spurt_mult", "trigger_bars", "wait_bars", "progress_r", "exit_logic", "max_risk", "risk_rs", "sides")}}
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(LIVE_PATH + ".tmp", "w") as f:
        json.dump({"meta": meta, "trades": out}, f)
    os.replace(LIVE_PATH + ".tmp", LIVE_PATH)
    _write_csv(out)
    _append_history(out, meta["date"])
    try:
        _alerts(out, c, now)
    except Exception as e:
        log.warning(f"SPZ alert step failed: {e}")
    log.info(f"SPZ cycle: {len(todo)} stocks, {len(out)} trades today ({sum(1 for t in out if t['status'] == 'OPEN')} open), {errors} errors")


COLS = ["symbol", "side", "zone", "spurt_bar", "spurt_x", "trigger_bar", "trigger_vol_x", "entry", "sl", "target", "target_src",
        "status", "exit", "exit_reason", "exit_bar", "bars_held", "watch", "r", "qty", "pnl_rs"]


def _write_csv(trades):
    with open(CSV_PATH + ".tmp", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        for t in trades:
            w.writerow([t.get(k, "") for k in COLS])
    os.replace(CSV_PATH + ".tmp", CSV_PATH)


def _append_history(trades, date):
    """Closed trades of the day, one row per trade (the file is rewritten for today's date so a re-play never duplicates)."""
    rows = []
    if os.path.exists(HIST_PATH):
        try:
            rows = [r for r in csv.reader(open(HIST_PATH)) if r and r[0] != date and r[0] != "date"]
        except Exception:
            rows = []
    with open(HIST_PATH, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date"] + COLS)
        for r in rows:
            w.writerow(r)
        for t in trades:
            if t["status"] == "CLOSED":
                w.writerow([date] + [t.get(k, "") for k in COLS])


# ======================================================================================= page
def page_html():
    try:
        with open(LIVE_PATH) as f:
            d = json.load(f)
    except Exception:
        return ("<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='60'><title>Spurt at zone</title></head>"
                "<body style='background:#0e1117;color:#e6e6e6;font-family:Arial,sans-serif;padding:24px'><h2>Spurt at zone (paper)</h2>"
                "<p>No cycle has run yet today. It starts after the first closed 10-minute candles in market hours.</p></body></html>")
    m, ts = d["meta"], d["trades"]
    closed = [t for t in ts if t["status"] == "CLOSED"]
    tot_r = sum(t["r"] for t in closed)
    tot_rs = sum(t["pnl_rs"] for t in closed)
    wins = sum(1 for t in closed if t["r"] > 0)

    def colr(x):
        return "#3fb950" if x > 0 else "#f85149" if x < 0 else "#9aa0a6"

    def row(t):
        side = f'<b style="color:{"#f85149" if t["side"] == "SHORT" else "#3fb950"}">{t["side"]}</b>'
        stat = t["status"] if t["status"] == "OPEN" else f'{t["exit_reason"]} @ {t["exit"]} ({t["exit_bar"]})'
        extra = t["watch"] if t["status"] == "OPEN" else ""
        return ("<tr>" + "".join(f"<td>{x}</td>" for x in [
            f"<b>{t['symbol']}</b>", side, t["zone"], f"{t['spurt_bar']} ({t['spurt_x']}x)", f"{t['trigger_bar']} ({t['trigger_vol_x']}x)",
            t["entry"], t["sl"], f"{t['target']} <small>({t['target_src']})</small>", stat, extra,
            f'<b style="color:{colr(t["r"])}">{t["r"]:+.2f}R</b>', f'<span style="color:{colr(t["pnl_rs"])}">{t["pnl_rs"]:+,.0f}</span>']) + "</tr>")

    body = "".join(row(t) for t in ts) or '<tr><td colspan="12" style="color:#9aa0a6">No setups yet today.</td></tr>'
    cf = m["cfg"]
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60"><title>Spurt at zone</title>
<style>body{{background:#0e1117;color:#e6e6e6;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;padding:24px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{padding:8px 10px;text-align:left;border-bottom:1px solid #262b36}}
th{{background:#161b22;color:#9aa0a6;font-weight:600}}tr:hover{{background:#161b22}}a{{color:#58a6ff}}.m{{color:#9aa0a6;font-size:13px;margin:6px 0 14px}}</style></head><body>
<h2>Spurt at OI zone, then reversal candle (PAPER - no orders)</h2>
<div class="m">Run {m['run']} &middot; {m['stocks']} stocks with OI zones &middot; errors {m['errors']} &middot; <a href="/spz.csv">CSV today</a> &middot;
<a href="/spz_history.csv">CSV history</a> &middot; <a href="/spurt10">Volume spurt</a> &middot; <a href="/scanner">Scanner</a><br>
Rule: spurt &ge; {cf['spurt_mult']:g}x reaches an OI zone, then a reversal candle with volume above average within {cf['trigger_bars']} candles;
entry at its close, stop at the swing; wait {cf['wait_bars']} candles for volume and price ({cf['exit_logic'].upper()} logic, price must move {cf['progress_r']:g}R);
risk Rs {cf['risk_rs']:,.0f} per paper trade. Today: {len(closed)} closed, {wins} winners, <b style="color:{colr(tot_r)}">{tot_r:+.2f}R</b>
(<span style="color:{colr(tot_rs)}">{tot_rs:+,.0f}</span> Rs).</div>
<div style="overflow-x:auto"><table><tr><th>Symbol</th><th>Side</th><th>OI zone</th><th>Spurt candle</th><th>Trigger candle</th><th>Entry</th><th>SL</th>
<th>Target</th><th>Status</th><th>Watch</th><th>R</th><th>P&amp;L Rs</th></tr>{body}</table></div></body></html>"""


def history_csv_path():
    return HIST_PATH


# ======================================================================================= loop
def _seconds_to_next_run(now):
    nxt = now.replace(second=45, microsecond=0)
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
    log.info("SPZ (spurt-at-zone paper tracker) autorun started - every 10 min at candle close, 09:25-15:40 IST, Mon-Fri.")
    while True:
        try:
            time.sleep(_seconds_to_next_run(datetime.now(IST)))
            if cfg()["autorun"] and _in_session(datetime.now(IST)):
                run_once()
        except Exception as e:
            log.error(f"SPZ cycle failed: {e}")
            time.sleep(30)
