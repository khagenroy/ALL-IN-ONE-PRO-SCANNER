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
  BOT_ONE_PER_SYMBOL_PER_DAY (true), BOT_CONFIRM_EXPIRY_BARS (15), BOT_MAX_DELAY_MINUTES (20), NTFY_TOPIC.
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
           "signal_bar_ist": _utc_to_ist(p.get("signal_ts")), "level": p.get("level"), "sl": p.get("sl"), **extra}
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
        "confirm_bar_ist", "price", "why", "status", "response"]


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


def _send(cf, p, price, confirm_bar_ist, now, st):
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
                _send(cf, p, close, cbar, now, st)
            except Exception as e:
                _record(cf, "CHECK_ERROR", p, why=str(e)[:200])
                keep.append(p)  # try again next scan

        st["pending"] = keep
        st["seen"] = list(seen)[-3000:]
        _save_state(st)
    except Exception as e:
        log.error(f"section_a_bot.run failed: {e}")
