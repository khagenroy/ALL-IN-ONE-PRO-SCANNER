"""
ALL IN ONE PRO Live Scanner - standalone Flask app, separate repo/service
from dhan-bridge entirely (shares no code or imports with it - see
scrip_master.py's header for why). Runs live_scanner.run_scan() every 10
minutes during NSE market hours in a background thread (same in-process
pattern as dhan-bridge's own SL watchdog / EOD scanner autorun), and serves
the latest result as a webpage.

This process does NOT place orders, does NOT touch dhan-bridge, and does
NOT execute trades of any kind - it only reads market data and reports
signals for a person to act on manually (or wire up separately later, if
ever wanted).
"""

import os
import time
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, send_file

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("all-in-one-pro-live")

IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("LIVE_SCANNER_AUTORUN", "true").strip().lower() == "true"
POLL_INTERVAL_SECONDS = int(os.environ.get("LIVE_SCANNER_INTERVAL_SECONDS", "600") or "600")  # 10 min
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)


def _within_market_hours(now_ist: datetime) -> bool:
    if now_ist.weekday() >= 5:  # Sat/Sun
        return False
    open_t = now_ist.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    close_t = now_ist.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_t <= now_ist <= close_t


def _live_scanner_loop():
    log.info(f"Live scanner autorun started - every {POLL_INTERVAL_SECONDS}s during "
             f"{MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}-{MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d} IST, Mon-Fri.")
    while True:
        try:
            now = datetime.now(IST)
            if _within_market_hours(now):
                try:
                    import live_scanner
                    log.info("Running live scan...")
                    live_scanner.run_scan()
                    log.info("Live scan complete.")
                except Exception as e:
                    log.error(f"Error during live scan: {e}")
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                # outside market hours - sleep until the next likely useful
                # check rather than busy-polling every 10 min all night
                next_check = now + timedelta(minutes=30)
                time.sleep(max((next_check - now).total_seconds(), 60))
        except Exception as e:
            log.error(f"Error in live scanner loop: {e}")
            time.sleep(60)


if AUTORUN:
    threading.Thread(target=_live_scanner_loop, daemon=True, name="live-scanner-autorun").start()


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


@app.route("/scanner", methods=["GET"])
def scanner_results():
    path = os.path.join(os.path.dirname(__file__), "results", "latest.html")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    with open(path) as f:
        return f.read()


@app.route("/scanner/signals.csv", methods=["GET"])
def scanner_signals_csv():
    path = os.path.join(os.path.dirname(__file__), "results", "latest_signals.csv")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    return send_file(path, mimetype="text/csv", as_attachment=True, download_name="all_in_one_pro_signals.csv")


@app.route("/scanner/signals_b.csv", methods=["GET"])
def scanner_signals_b_csv():
    path = os.path.join(os.path.dirname(__file__), "results", "latest_signals_b.csv")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    return send_file(path, mimetype="text/csv", as_attachment=True, download_name="all_in_one_pro_signals_section_b.csv")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
