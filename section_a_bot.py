"""
Section A auto-trade bot (Strategy 1 = Liquidity Sweep, Strategy 2 = Order Block mitigation).

WHAT IT DOES
  After every intraday scan, this reads the Section A signals the scanner just found (the scanner's own
  volume rule - 5x the 20-bar average, with the 10-bar spike window - has ALREADY been applied; this file
  does not change any signal logic). For each 10m signal it then does what the TradingView script does:
    1. waits for the CONFIRMATION: price breaking the signal candle's high (buy) / low (sell), within 15 candles
    2. checks the stop is still on the right side of the confirming candle's close
    3. sends the same "..._CONFIRMED" message TradingView would send to your dhan-bridge webhook.
  dhan-bridge then does everything it already does (price-drift check, Super Order with stop-loss, cascade, cutoff).

SAFE BY DEFAULT
  BOT_ENABLED   (default false) - nothing happens at all unless this is "true".
  BOT_LIVE      (default false) - PAPER MODE: decisions are only logged ("WOULD_SEND"); no order is sent.
                                  Set "true" to really send to the bridge.

RECORD
  Every decision is written to results/bot_log.jsonl and shown on /botlog (CSV: /botlog.csv).
  Render wipes its disk on each redeploy; set NTFY_TOPIC to also get each decision on your phone.

ENV VARS
  BOT_ENABLED, BOT_LIVE, BRIDGE_WEBHOOK_URL (full dhan-bridge URL, https://.../webhook/<secret>),
  BOT_TIMEFRAMES (default "10m"), BOT_START (09:30), BOT_CUTOFF (14:30), BOT_MAX_TRADES_PER_DAY (10),
  BOT_ONE_PER_SYMBOL_PER_DAY (true), BOT_CONFIRM_EXPIRY_BARS (15), BOT_MAX_DELAY_MINUTES (20), NTFY_TOPIC,
  BOT_PAPER_RISK_RS (2000 - rupees risked per paper trade; set it equal to RISK_RUPEES_STOCKS on dhan-bridge).

RSI FLUSH (added 2026-10-07) - run_rsi() near the bottom
  The RSI Flush scan (rsi_flush_live.py, table on /scanner) hands its rows here after every intraday scan:
    - PHONE ALERT (needs NTFY_TOPIC): once per setup when a break is confirmed (optionally also when a signal candle
      forms: RSIFLUSH_NTFY_PENDING=true). Capped per day (RSIFLUSH_NTFY_MAX_PER_DAY, default 40).
    - PAPER TRADE (needs BOT_ENABLED=true): the confirmed break is followed exactly like a Section A paper trade
      (entry = close of the confirming candle, the same cascade, same P&L page, signal shown as RSI_FLUSH_BUY_10m ...).
      ALWAYS PAPER, even when BOT_LIVE=true - the bridge does not know the RSI Flush alert types yet, so nothing is ever sent.
  Own switches/limits: BOT_RSI_ENABLED (true), BOT_RSI_TIMEFRAMES (10m), BOT_RSI_MAX_TRADES_PER_DAY (20),
  BOT_RSI_ONE_PER_SYMBOL_PER_DAY (true), RSIFLUSH_NTFY (true), RSIFLUSH_NTFY_TIMEFRAMES (10m,1H,4H),
  RSIFLUSH_NTFY_PENDING (false), RSIFLUSH_NTFY_MAX_PER_DAY (40). Section A's own daily limit is not touched.

PAPER P&L (/botpnl, /botpnl.csv)
  Every paper "WOULD_SEND" is followed on the next 10m candles and closed the way the bridge would manage it:
    entry   = close of the confirming candle (the price sent), quantity = BOT_PAPER_RISK_RS / (entry - stop)
    cascade = T1 hit -> stop to the signal level ("cost"), T2 -> stop to T1, T3 -> T2, T4 -> T3, T5 -> T4
    exit    = stop hit, T6 reached (the Super Order target), or 15:10 IST square-off (close of the 15:00 candle)
  Assumptions: if one candle touches both the stop and a target, the STOP is taken first (pessimistic); a gap
  through the stop exits at the open; results are BEFORE brokerage/taxes/slippage.
"""

import os
import json
import html
import logging
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import pandas as pd

log = logging.getLogger("section-a-bot")
IST = ZoneInfo("Asia/Kolkata")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
LOG_PATH = os.path.join(RESULTS_DIR, "bot_log.jsonl")
STATE_PATH = os.path.join(RESULTS_DIR, "bot_state.json")
_lock = threading.Lock()


def _env_bool(name, default):
    return os.environ.get(name, str(default)).strip().lower() == "true"


def _hhmm(name, default):
    h, m = os.environ.get(name, default).strip().split(":")
    return int(h), int(m)


def cfg():
    """Read at call time so a Render env change takes effect on the next scan."""
    return {
        "enabled": _env_bool("BOT_ENABLED", False),
        "live": _env_bool("BOT_LIVE", False),
        "url": os.environ.get("BRIDGE_WEBHOOK_URL", "").strip(),
        "tfs": [t.strip() for t in os.environ.get("BOT_TIMEFRAMES", "10m").split(",") if t.strip()],
        "start": _hhmm("BOT_START", "09:30"),
        "cutoff": _hhmm("BOT_CUTOFF", "14:30"),
        "max_per_day": int(os.environ.get("BOT_MAX_TRADES_PER_DAY", "10") or "10"),
        "one_per_symbol": _env_bool("BOT_ONE_PER_SYMBOL_PER_DAY", True),
        "expiry_bars": int(os.environ.get("BOT_CONFIRM_EXPIRY_BARS", "15") or "15"),
        "max_delay_min": int(os.environ.get("BOT_MAX_DELAY_MINUTES", "20") or "20"),
        "ntfy": os.environ.get("NTFY_TOPIC", "").strip(),
        "paper_risk": float(os.environ.get("BOT_PAPER_RISK_RS", "2000") or "2000"),
    }


STRATEGY_TYPE = {
    "SWEEP_BUY": "STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED",
    "SWEEP_SELL": "STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED",
    "OB_BUY": "STRATEGY_2_ORDER_BLOCK_CONFIRMED",
    "OB_SELL": "STRATEGY_2_ORDER_BLOCK_CONFIRMED",
}


# ---------------------------------------------------------------- record keeping
def _utc_to_ist(ts) -> str:
    try:
        return (pd.Timestamp(str(ts)) + pd.Timedelta(minutes=330)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _record(cf, action, p, **extra):
    """One line per decision - the manual-check record."""
    row = {"time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"), "action": action,
           "mode": "LIVE" if cf["live"] else "PAPER",
           "symbol": p.get("symbol"), "signal": p.get("signal"), "side": p.get("side"),
           "signal_bar_ist": _utc_to_ist(p.get("signal_ts")), "level": p.get("level"), "sl": p.get("sl"),
           **{k.lower(): v for k, v in (p.get("targets") or {}).items()}, **extra}
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with _lock, open(LOG_PATH, "a") as f:
            f.write(json.dumps(row, default=str) + "\n")
    except Exception as e:
        log.warning(f"bot log write failed: {e}")
    log.info(f"[bot] {action} {p.get('symbol')} {p.get('signal')} {extra}")
    if cf["ntfy"] and action in ("WOULD_SEND", "SENT", "SEND_FAILED", "CONFIRMED_BUT_SKIPPED"):
        try:
            requests.post(f"https://ntfy.sh/{cf['ntfy']}", timeout=5,
                          data=f"{row['mode']} {action}: {p.get('symbol')} {p.get('signal')} "
                               f"level {p.get('level')} sl {p.get('sl')} {extra.get('why', '')}".encode("utf-8"),
                          headers={"Title": f"[Section A bot] {row['mode']} {action}", "Priority": "urgent", "Tags": "warning"})
        except Exception:
            pass


def _load_state():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(st):
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f, default=str)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        log.warning(f"bot state write failed: {e}")


def read_log(limit=500):
    rows = []
    try:
        with open(LOG_PATH) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return rows[-limit:][::-1]  # newest first


COLS = ["time_ist", "mode", "action", "symbol", "signal", "side", "signal_bar_ist", "level", "sl",
        "t1", "t2", "t3", "t4", "t5", "t6", "confirm_bar_ist", "price", "why", "status", "response"]


def log_csv() -> str:
    rows = read_log(100000)
    df = pd.DataFrame(rows)
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    return df[COLS].to_csv(index=False)


def log_html() -> str:
    c = cfg()
    rows = read_log(300)
    head = "".join(f"<th>{html.escape(h)}</th>" for h in COLS)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(r.get(h, '')))}</td>" for h in COLS) + "</tr>" for r in rows)
    state = ("OFF (BOT_ENABLED is not true)" if not c["enabled"]
             else ("LIVE - orders are being sent to the bridge" if c["live"] else "PAPER - logging only, no orders sent"))
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Section A bot log</title><style>
body{{font-family:system-ui,Arial;margin:16px;background:#fff;color:#111}}
table{{border-collapse:collapse;font-size:13px}} th,td{{border:1px solid #ccc;padding:4px 8px;white-space:nowrap}}
th{{background:#f0f0f0;position:sticky;top:0}} .w{{overflow-x:auto}}
@media(prefers-color-scheme:dark){{body{{background:#111;color:#eee}}th{{background:#222}}th,td{{border-color:#444}}}}
</style></head><body><h2>Section A bot log</h2>
<p><b>Status:</b> {state}. Newest first, last 300 decisions. <a href="/botlog.csv">Download CSV</a>.
Render clears this on every redeploy - download the CSV if you want to keep it.</p>
<div class="w"><table><tr>{head}</tr>{body}</table></div></body></html>"""


# ---------------------------------------------------------------- the bot
def _recent_10m_bars(symbol):
    """Closed 10m bars for the last few days (UTC-naive index, same as the scanner)."""
    import live_scanner as ls
    from scrip_master import get_security_id_and_segment
    sid, seg = get_security_id_and_segment(symbol)
    if not sid:
        raise RuntimeError("no security id")
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    df5 = ls.fetch_intraday_history(sid, seg, ls.INTRADAY_5MIN_INTERVAL, 3)
    return ls.drop_forming_bars(ls.build_merged_bars(df5, 2), 10, as_of)


def _payload(p, price):
    t = p["targets"]
    out = {"symbol": p["symbol"], "side": p["side"], "qty": 1, "fixed_lot": False,
           "type": STRATEGY_TYPE[p["signal"]], "price": round(float(price), 2), "sl": p["sl"]}
    for k in ("T1", "T2", "T3", "T4", "T5", "T6"):
        out[k.lower()] = t.get(k)
    return out


def _allowed_now(cf, now):
    if now.weekday() >= 5:
        return "weekend"
    mins = now.hour * 60 + now.minute
    if mins < cf["start"][0] * 60 + cf["start"][1]:
        return "before start time"
    if mins > cf["cutoff"][0] * 60 + cf["cutoff"][1]:
        return "after cutoff time"
    return None


def _send(cf, p, price, confirm_bar_ist, now, st, confirm_ts=None):
    payload = _payload(p, price)
    why = _allowed_now(cf, now)
    today = now.strftime("%Y-%m-%d")
    day = st.setdefault("day", {"date": today, "count": 0, "symbols": []})
    if day["date"] != today:
        day.update({"date": today, "count": 0, "symbols": []})
    if not why and day["count"] >= cf["max_per_day"]:
        why = f"daily limit of {cf['max_per_day']} trades reached"
    if not why and cf["one_per_symbol"] and p["symbol"] in day["symbols"]:
        why = "already traded this symbol today"
    if why:
        _record(cf, "CONFIRMED_BUT_SKIPPED", p, why=why, confirm_bar_ist=confirm_bar_ist, price=payload["price"])
        return
    day["count"] += 1
    day["symbols"].append(p["symbol"])
    if not cf["live"]:
        _record(cf, "WOULD_SEND", p, confirm_bar_ist=confirm_bar_ist, price=payload["price"],
                why="paper mode - no order sent", response=json.dumps(payload))
        _open_paper(cf, st, p, payload["price"], confirm_bar_ist, confirm_ts)
        return
    if not cf["url"]:
        _record(cf, "SEND_FAILED", p, why="BRIDGE_WEBHOOK_URL not set", confirm_bar_ist=confirm_bar_ist)
        return
    try:
        r = requests.post(cf["url"], json=payload, timeout=20)
        _record(cf, "SENT" if r.status_code < 300 else "SEND_FAILED", p, confirm_bar_ist=confirm_bar_ist,
                price=payload["price"], status=r.status_code, response=r.text[:200])
    except Exception as e:
        _record(cf, "SEND_FAILED", p, confirm_bar_ist=confirm_bar_ist, price=payload["price"], why=str(e)[:200])


# ---------------------------------------------------------------- paper P&L tracker
STAGE_NAMES = ["SL", "SL_AT_COST", "SL_AT_T1", "SL_AT_T2", "SL_AT_T3", "SL_AT_T4"]
SQUAREOFF_IST_MINUTES = 15 * 60 + 10  # 15:10 IST


def _open_paper(cf, st, p, entry, confirm_bar_ist, confirm_ts):
    risk = abs(float(entry) - float(p["sl"]))
    if risk <= 0:
        return
    trades = st.setdefault("paper", [])
    trades.append({
        "id": p["key"], "opened_ist": confirm_bar_ist, "symbol": p["symbol"], "signal": p["signal"], "side": p["side"],
        "entry": round(float(entry), 2), "level": p["level"], "sl": p["sl"], "targets": p["targets"],
        "qty": max(1, round(cf["paper_risk"] / risk)), "status": "OPEN", "stage": 0, "cur_sl": p["sl"],
        "last_ts": str(confirm_ts) if confirm_ts is not None else str(p["signal_ts"]),
        "last_close": round(float(entry), 2), "exit_time_ist": "", "exit_reason": "", "exit_price": ""})


def _step_trade(t, ts, o, h, l, c):
    """Apply one closed 10m candle to an open paper trade. Returns True when the trade closed on it."""
    buy = t["side"] == "buy"
    tg = [t["targets"].get(k) for k in ("T1", "T2", "T3", "T4", "T5", "T6")]
    sl = float(t["cur_sl"])
    # 1) stop first (pessimistic when one candle touches both the stop and a target)
    if (buy and l <= sl) or ((not buy) and h >= sl):
        px = (min(o, sl) if buy else max(o, sl))
        return _close_trade(t, ts, px, STAGE_NAMES[t["stage"]])
    # 2) targets, one stage at a time: stop moves up the cascade as each T is touched
    while t["stage"] < 6:
        k = t["stage"]
        if tg[k] is None:
            break
        if (buy and h >= tg[k]) or ((not buy) and l <= tg[k]):
            if k == 5:
                return _close_trade(t, ts, tg[5], "TARGET_T6")
            t["stage"] = k + 1
            t["cur_sl"] = t["level"] if k == 0 else tg[k - 1]
        else:
            break
    # 3) 15:10 square-off (the 15:00-15:10 candle closes at 15:10)
    end_ist = (pd.Timestamp(ts) + pd.Timedelta(minutes=330 + 10))
    if end_ist.hour * 60 + end_ist.minute >= SQUAREOFF_IST_MINUTES:
        return _close_trade(t, ts, c, "SQUAREOFF_1510")
    t["last_close"] = round(float(c), 2)
    return False


def _close_trade(t, ts, px, reason):
    t["status"] = "CLOSED"
    t["exit_reason"] = reason
    t["exit_price"] = round(float(px), 2)
    t["exit_time_ist"] = _utc_to_ist(pd.Timestamp(ts) + pd.Timedelta(minutes=10))
    t["last_close"] = t["exit_price"]
    return True


def _track_paper(cf, st):
    for t in st.get("paper", []):
        if t.get("status") != "OPEN":
            continue
        try:
            bars = _recent_10m_bars(t["symbol"])
        except Exception as e:
            log.warning(f"[bot] paper tracker: no bars for {t['symbol']}: {e}")
            continue
        trade_day = _utc_to_ist(t["last_ts"])[:10]
        for ts, b in bars[bars.index > pd.Timestamp(str(t["last_ts"]))].iterrows():
            if _utc_to_ist(ts)[:10] != trade_day:  # the day ended without a 15:10 candle - close at the last price
                _close_trade(t, pd.Timestamp(str(t["last_ts"])), t["last_close"], "SQUAREOFF_1510")
                break
            t["last_ts"] = str(ts)
            if _step_trade(t, ts, float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"])):
                break


PNL_COLS = ["opened_ist", "symbol", "signal", "side", "entry", "sl", "t1", "t2", "t3", "t4", "t5", "t6", "qty", "status",
            "highest_target", "exit_time_ist", "exit_reason", "exit_price", "pnl_per_share", "pnl_rupees", "r_multiple"]


def pnl_rows():
    rows = []
    for t in _load_state().get("paper", []):
        buy = t["side"] == "buy"
        px = t["exit_price"] if t["status"] == "CLOSED" else t.get("last_close")
        risk = abs(t["entry"] - float(t["sl"]))
        try:
            pps = round((float(px) - t["entry"]) * (1 if buy else -1), 2)
        except Exception:
            pps = ""
        rows.append({"opened_ist": t["opened_ist"], "symbol": t["symbol"], "signal": t["signal"], "side": t["side"],
                     "entry": t["entry"], "sl": t["sl"], **{k.lower(): v for k, v in t["targets"].items()},
                     "qty": t["qty"], "status": t["status"] if t["status"] == "CLOSED" else "OPEN (unrealised)",
                     "highest_target": ("T%d" % t["stage"]) if t["stage"] else "none",
                     "exit_time_ist": t["exit_time_ist"], "exit_reason": t["exit_reason"], "exit_price": t["exit_price"],
                     "pnl_per_share": pps, "pnl_rupees": round(pps * t["qty"]) if pps != "" else "",
                     "r_multiple": round(pps / risk, 2) if pps != "" and risk else ""})
    return rows


def pnl_csv() -> str:
    df = pd.DataFrame(pnl_rows(), columns=PNL_COLS)
    return df.to_csv(index=False)


def pnl_html() -> str:
    rows = pnl_rows()
    closed = [r for r in rows if r["status"] == "CLOSED"]
    wins = [r for r in closed if r["pnl_rupees"] != "" and r["pnl_rupees"] > 0]
    tot = sum(r["pnl_rupees"] for r in closed if r["pnl_rupees"] != "")
    totr = sum(r["r_multiple"] for r in closed if r["r_multiple"] != "")
    opn = [r for r in rows if r["status"] != "CLOSED"]
    unreal = sum(r["pnl_rupees"] for r in opn if r["pnl_rupees"] != "")
    summary = (f"Closed trades: {len(closed)} | Winners: {len(wins)}"
               + (f" ({100 * len(wins) / len(closed):.0f}%)" if closed else "")
               + f" | Total P&L (closed): Rs {tot:,.0f} | Total R: {totr:.2f} | Open trades: {len(opn)} "
               f"(unrealised Rs {unreal:,.0f})")
    # split by strategy: Section A (sweep / order block) vs RSI Flush
    by_src = []
    for label, test in (("Section A (Sweep + OB)", lambda r: not str(r["signal"]).startswith("RSI_FLUSH")),
                        ("RSI Flush", lambda r: str(r["signal"]).startswith("RSI_FLUSH"))):
        sub = [r for r in rows if test(r)]
        if not sub:
            continue
        sc = [r for r in sub if r["status"] == "CLOSED"]
        sw = [r for r in sc if r["pnl_rupees"] != "" and r["pnl_rupees"] > 0]
        sp = sum(r["pnl_rupees"] for r in sc if r["pnl_rupees"] != "")
        sr = sum(r["r_multiple"] for r in sc if r["r_multiple"] != "")
        so = [r for r in sub if r["status"] != "CLOSED"]
        su = sum(r["pnl_rupees"] for r in so if r["pnl_rupees"] != "")
        by_src.append(f"<b>{label}</b>: closed {len(sc)}"
                      + (f", winners {len(sw)} ({100 * len(sw) / len(sc):.0f}%)" if sc else "")
                      + f", P&amp;L Rs {sp:,.0f}, R {sr:.2f}, open {len(so)} (unrealised Rs {su:,.0f})")
    by_src_html = "".join(f"<p>{x}</p>" for x in by_src) if len(by_src) > 1 else ""
    head = "".join(f"<th>{html.escape(h)}</th>" for h in PNL_COLS)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(r.get(h, '')))}</td>" for h in PNL_COLS) + "</tr>"
                   for r in rows[::-1])
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Section A paper P&amp;L</title><style>
body{{font-family:system-ui,Arial;margin:16px;background:#fff;color:#111}}
table{{border-collapse:collapse;font-size:13px}} th,td{{border:1px solid #ccc;padding:4px 8px;white-space:nowrap}}
th{{background:#f0f0f0;position:sticky;top:0}} .w{{overflow-x:auto}} small{{color:#666}}
@media(prefers-color-scheme:dark){{body{{background:#111;color:#eee}}th{{background:#222}}th,td{{border-color:#444}}small{{color:#aaa}}}}
</style></head><body><h2>Section A paper trades - P&amp;L</h2>
<p><b>{summary}</b></p>
{by_src_html}
<p><small>Entry = close of the confirming candle. Stop moves to cost at T1, then to T1, T2, T3, T4 as each next target is hit;
exit at the stop, at T6, or at the 15:10 square-off. If one candle touches both the stop and a target, the stop is taken first.
Before brokerage, taxes and slippage. Newest first. <a href="/botpnl.csv">Download CSV</a>.
Render clears this on every redeploy - download the CSV if you want to keep it.</small></p>
<div class="w"><table><tr>{head}</tr>{body}</table></div></body></html>"""


def run(signals):
    """Call once after each intraday scan with the Section A signals list. Never raises."""
    try:
        cf = cfg()
        if not cf["enabled"]:
            return
        now = datetime.now(IST)
        st = _load_state()
        pending = st.setdefault("pending", [])
        seen = set(st.setdefault("seen", []))

        # 1. new signals -> waiting for confirmation
        for s in signals or []:
            if s.get("timeframe") not in cf["tfs"] or s.get("signal") not in STRATEGY_TYPE:
                continue
            lv = s.get("levels") or {}
            if _utc_to_ist(s.get("timestamp"))[:10] != now.strftime("%Y-%m-%d"):
                continue  # a leftover candle from an earlier day (scan started before today's first candle closed)
            key = f"{s['symbol']}|{s['signal']}|{s['timestamp']}"
            if key in seen or not lv:
                continue
            seen.add(key)
            side = "buy" if s["signal"].endswith("BUY") else "sell"
            p = {"key": key, "symbol": s["symbol"], "signal": s["signal"], "side": side,
                 "signal_ts": s["timestamp"], "level": lv.get("entry"), "sl": lv.get("sl"),
                 "targets": {k: lv.get(k) for k in ("T1", "T2", "T3", "T4", "T5", "T6")}}
            pending.append(p)
            _record(cf, "SIGNAL_SEEN", p, why="waiting for price to break the signal candle")

        # 2. check every waiting signal for confirmation / expiry
        keep = []
        today = now.strftime("%Y-%m-%d")
        for p in pending:
            try:
                if _utc_to_ist(p["signal_ts"])[:10] != today:
                    _record(cf, "EXPIRED", p, why="new day - signal dropped")
                    continue
                bars = _recent_10m_bars(p["symbol"])
                after = bars[bars.index > pd.Timestamp(str(p["signal_ts"]))].iloc[:cf["expiry_bars"]]
                hit = None
                for ts, b in after.iterrows():
                    if (p["side"] == "buy" and b["high"] > p["level"]) or (p["side"] == "sell" and b["low"] < p["level"]):
                        hit = (ts, b)
                        break
                if hit is None:
                    if len(after) >= cf["expiry_bars"]:
                        _record(cf, "EXPIRED", p, why=f"no breakout within {cf['expiry_bars']} candles")
                    else:
                        keep.append(p)
                    continue
                ts, b = hit
                cbar = _utc_to_ist(ts)
                close = float(b["close"])
                if (p["side"] == "buy" and close <= p["sl"]) or (p["side"] == "sell" and close >= p["sl"]):
                    _record(cf, "CONFIRMED_BUT_SKIPPED", p, confirm_bar_ist=cbar, price=round(close, 2),
                            why="confirming candle closed beyond the stop - setup invalid")
                    continue
                bar_end = pd.Timestamp(ts) + pd.Timedelta(minutes=10)
                late = (pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) - bar_end).total_seconds() / 60
                if late > cf["max_delay_min"]:
                    _record(cf, "CONFIRMED_BUT_SKIPPED", p, confirm_bar_ist=cbar, price=round(close, 2),
                            why=f"confirmation was {late:.0f} min ago - too old to chase")
                    continue
                _send(cf, p, close, cbar, now, st, confirm_ts=ts)
            except Exception as e:
                _record(cf, "CHECK_ERROR", p, why=str(e)[:200])
                keep.append(p)  # try again next scan

        st["pending"] = keep
        st["seen"] = list(seen)[-3000:]
        try:
            _track_paper(cf, st)
        except Exception as e:
            log.error(f"paper tracker failed: {e}")
        _save_state(st)
    except Exception as e:
        log.error(f"section_a_bot.run failed: {e}")



# ---------------------------------------------------------------- RSI Flush: phone alerts + paper trades
RSI_TF_MINUTES = {"10m": 10, "1H": 60, "4H": 240}


def _rsi_cfg():
    def lst(name, default):
        return [t.strip() for t in os.environ.get(name, default).split(",") if t.strip()]
    return {
        "paper": _env_bool("BOT_RSI_ENABLED", True),
        "tfs": lst("BOT_RSI_TIMEFRAMES", "10m"),
        "max_per_day": int(os.environ.get("BOT_RSI_MAX_TRADES_PER_DAY", "20") or "20"),
        "one_per_symbol": _env_bool("BOT_RSI_ONE_PER_SYMBOL_PER_DAY", True),
        "ntfy": _env_bool("RSIFLUSH_NTFY", True),
        "ntfy_tfs": lst("RSIFLUSH_NTFY_TIMEFRAMES", "10m,1H,4H"),
        "ntfy_pending": _env_bool("RSIFLUSH_NTFY_PENDING", False),
        "ntfy_cap": int(os.environ.get("RSIFLUSH_NTFY_MAX_PER_DAY", "40") or "40"),
    }


def _rsi_push(topic, title, text, tags="chart_with_upwards_trend"):
    try:
        requests.post(f"https://ntfy.sh/{topic}", data=text.encode("utf-8"), timeout=5,
                      headers={"Title": title, "Priority": "high", "Tags": tags})
    except Exception:
        pass


def run_rsi(rows):
    """Call once after each intraday scan with the RSI Flush rows. Never raises, never sends an order."""
    try:
        if not rows:
            return
        cf = cfg()
        rc = _rsi_cfg()
        paper_on = cf["enabled"] and rc["paper"]
        push_on = bool(cf["ntfy"] and rc["ntfy"])
        if not paper_on and not push_on:
            return
        cf_quiet = dict(cf, ntfy="")           # RSI Flush decisions are logged without the Section A style urgent push
        now = datetime.now(IST)
        today = now.strftime("%Y-%m-%d")
        st = _load_state()
        seen = set(st.setdefault("rsi_seen", []))
        nd = st.setdefault("rsi_ntfy_day", {"date": today, "count": 0})
        if nd.get("date") != today:
            nd.update({"date": today, "count": 0})
        dd = st.setdefault("rsi_day", {"date": today, "count": 0, "symbols": []})
        if dd.get("date") != today:
            dd.update({"date": today, "count": 0, "symbols": []})

        def push(title, text):
            if not push_on:
                return
            if nd["count"] >= rc["ntfy_cap"]:
                if nd["count"] == rc["ntfy_cap"]:
                    nd["count"] += 1
                    _rsi_push(cf["ntfy"], "[RSI Flush] daily alert cap reached",
                              f"{rc['ntfy_cap']} alerts sent today - further setups are still on /scanner and in the paper P&L.")
                return
            nd["count"] += 1
            _rsi_push(cf["ntfy"], title, text)

        for r in rows:
            tf, sym, side = r.get("timeframe"), r.get("symbol"), r.get("side")
            tfmin = RSI_TF_MINUTES.get(tf)
            if not tfmin or side not in ("BUY", "SELL"):
                continue
            levels = f"entry {r.get('entry')} | SL {r.get('sl')} | T1 {r.get('T1')} | T6 {r.get('T6')} | risk {r.get('risk_pct')}%"

            # --- signal candle formed, waiting for the break (alert only, optional)
            if r.get("status") == "PENDING":
                if not (push_on and rc["ntfy_pending"] and tf in rc["ntfy_tfs"]):
                    continue
                if _utc_to_ist(r["signal_ts"])[:10] != today:
                    continue
                key = f"P|{sym}|{tf}|{side}|{r['signal_ts']}"
                if key in seen:
                    continue
                seen.add(key)
                late = (pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
                        - (pd.Timestamp(str(r["signal_ts"])) + pd.Timedelta(minutes=tfmin))).total_seconds() / 60
                if late > cf["max_delay_min"]:
                    continue
                push(f"[RSI Flush] {side} signal candle - {sym} ({tf})", f"Waiting for the break. {levels}")
                continue

            # --- break confirmed
            if not r.get("confirm_ts") or _utc_to_ist(r["confirm_ts"])[:10] != today:
                continue
            key = f"{sym}|RSI_FLUSH_{side}|{tf}|{r['confirm_ts']}"
            if key in seen:
                continue
            seen.add(key)
            bar_end = pd.Timestamp(str(r["confirm_ts"])) + pd.Timedelta(minutes=tfmin)
            late = (pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) - bar_end).total_seconds() / 60
            if late > cf["max_delay_min"]:
                continue            # old confirmation (e.g. after a restart) - not alerted, not traded
            cbar = _utc_to_ist(r["confirm_ts"])
            close = r.get("confirm_close")
            ok_stage = r.get("status") == "ACTIVE"

            if tf in rc["ntfy_tfs"] and ok_stage:
                push(f"[RSI Flush] {side} CONFIRMED - {sym} ({tf})",
                     f"Break confirmed on the {cbar} candle (close {close}). {levels}")

            if not (paper_on and tf in rc["tfs"]):
                continue
            p = {"key": key, "symbol": sym, "signal": f"RSI_FLUSH_{side}_{tf}", "side": side.lower(),
                 "signal_ts": r["signal_ts"], "level": r.get("entry"), "sl": r.get("sl"),
                 "targets": {k: r.get(k) for k in ("T1", "T2", "T3", "T4", "T5", "T6")}}
            why = None
            if not ok_stage:
                why = f"already {str(r.get('status')).lower()} on its first look - not traded"
            elif close is None or (side == "BUY" and close <= p["sl"]) or (side == "SELL" and close >= p["sl"]):
                why = "confirming candle closed beyond the stop - setup invalid"
            if not why:
                why = _allowed_now(cf, now)
            if not why and dd["count"] >= rc["max_per_day"]:
                why = f"RSI Flush daily limit of {rc['max_per_day']} paper trades reached"
            if not why and rc["one_per_symbol"] and sym in dd["symbols"]:
                why = "already traded this symbol today (RSI Flush)"
            if why:
                _record(cf_quiet, "CONFIRMED_BUT_SKIPPED", p, why=why, confirm_bar_ist=cbar, price=close)
                continue
            dd["count"] += 1
            dd["symbols"].append(sym)
            _record(cf_quiet, "WOULD_SEND", p, confirm_bar_ist=cbar, price=close,
                    why="RSI Flush is paper only - no order sent")
            # the paper tracker follows 10m candles: start after the last 10m candle of the confirming bar
            last_ts = pd.Timestamp(str(r["confirm_ts"])) + pd.Timedelta(minutes=tfmin - 10)
            _open_paper(cf, st, p, close, cbar, last_ts)

        st["rsi_seen"] = list(seen)[-3000:]
        _save_state(st)
    except Exception as e:
        log.error(f"section_a_bot.run_rsi failed: {e}")
