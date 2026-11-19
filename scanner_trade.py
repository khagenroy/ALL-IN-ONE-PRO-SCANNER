"""
scanner_trade.py - SCANNER AUTO-TRADE (scanner side)                                                        ADDED 15 Nov 2026

When the scanner itself finds one of the six ALL IN ONE PRO signals (SWEEP, OB MITIGATION, VS SELL, VS BUY, SPURT BUY, SPURT SELL) it sends that signal to the Dhan bridge,
which trades it for 1 SHARE with all its normal risk rules (the bridge has an ON/OFF page: /scanner_auto/<secret>).

Safety rules here:
  * only a FRESH signal (its candle closed within FRESH_MIN minutes) is sent - a restart or a late scan can never replay old signals,
  * it waits DELAY seconds first: if the TradingView alert of the same signal reaches the bridge in that time (it is then marked "tv" on the page's row), the scanner does NOT send it,
    so the TradingView trade (your Rs 2000 risk) is never blocked by or doubled with a scanner one,
  * it is sent once (up to 3 tries only if the bridge could not be reached).
Read-only for the scanner itself. Any error is caught.
"""
import logging
import threading
import time
from datetime import datetime, timezone, timedelta

import requests

log = logging.getLogger("scanner_trade")
IST = timezone(timedelta(hours=5, minutes=30))
BRIDGE_URL = "https://dhan-bridge-tv.onrender.com/webhook/khagenindrajeetmuktakabitakartiksanjucta"
DELAY = 90          # seconds to wait for the TradingView alert of the same signal
FRESH_MIN = 15      # minutes: the signal candle must have closed this recently
SOURCES = ("scan", "section A")
TYPE = {"SWEEP": "STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED", "OB MITIGATION": "STRATEGY_2_ORDER_BLOCK_CONFIRMED", "VS SELL": "VS_SELL", "VS BUY": "VS_BUY",
        "SPURT BUY": "SPURT_CHOCH_BUY", "SPURT SELL": "SPURT_CHOCH_SELL"}


def schedule(row):
    """Called by signals_page.add() for a NEW scanner-found signal row."""
    try:
        if row.get("src") not in SOURCES or row.get("signal") not in TYPE or None in (row.get("price"), row.get("sl")):
            return
        ft = str(row.get("from_ts") or "")
        if not ft:
            return
        close = datetime.strptime(ft, "%Y-%m-%d %H:%M").replace(tzinfo=IST)
        age = (datetime.now(IST) - close).total_seconds() / 60.0
        if age > FRESH_MIN or age < -2:
            row["fwd"] = "old signal - not sent"
            return
        t = threading.Timer(DELAY, _send, args=(row,))
        t.daemon = True
        t.start()
    except Exception as e:
        log.warning("scanner_trade schedule failed: %s", e)


def _send(row):
    try:
        if row.get("tv"):
            row["fwd"] = "TradingView alert came - not sent"
            return
        payload = {"type": TYPE[row["signal"]], "symbol": row["symbol"], "side": str(row["side"]).lower(), "price": row["price"], "sl": row["sl"],
                   "t1": row.get("t1"), "t2": row.get("t2"), "t6": row.get("t6"), "qty": 1, "fixed_lot": "true", "source": "scanner"}
        try:                                                 # 19 Nov: the signal's timeframe in minutes (5 / 15) - the bridge's time stop waits 8 candles of it
            if row.get("tf") not in (None, ""):
                payload["tf"] = int(row["tf"])
        except Exception:
            pass
        payload = {k: v for k, v in payload.items() if v is not None}
        last = ""
        for n in range(3):
            try:
                r = requests.post(BRIDGE_URL, json=payload, timeout=15)
                last = "HTTP %s %s" % (r.status_code, (r.text or "")[:80])
                if r.status_code < 500:
                    break
            except Exception as e:
                last = "error: %s" % e
            time.sleep(5)
        row["fwd"] = "sent to bridge: " + last
        log.info("scanner_trade %s %s -> %s", row.get("symbol"), row.get("signal"), last)
    except Exception as e:
        log.warning("scanner_trade send failed: %s", e)
