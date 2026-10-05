"""
ALL IN ONE PRO Live Scanner - standalone Flask app, separate repo/service
from dhan-bridge entirely (shares no code or imports with it - see
scrip_master.py's header for why). Two background scans run in this process:

  INTRADAY (10m/1H/4H) - every 10 minutes during NSE market hours, on a FIXED
      schedule (the next scan is due 10 minutes after the previous one STARTED,
      not after it finished; if a scan overruns, the next one starts right
      away - scans never overlap). Page: /scanner
  SWING (1D/1W/1M)     - once a day after the close (16:00 IST), plus one seed
      scan if no swing results exist yet (fresh deploy). Page: /swing

This process does NOT place orders, does NOT touch dhan-bridge, and does
NOT execute trades of any kind - it only reads market data and reports
signals for a person to act on manually (or wire up separately later, if
ever wanted).
"""

import os
import time
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, send_file

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("all-in-one-pro-live")

IST = ZoneInfo("Asia/Kolkata")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

AUTORUN = os.environ.get("LIVE_SCANNER_AUTORUN", "true").strip().lower() == "true"
POLL_INTERVAL_SECONDS = int(os.environ.get("LIVE_SCANNER_INTERVAL_SECONDS", "600") or "600")  # 10 min
SWING_AUTORUN = os.environ.get("SWING_SCANNER_AUTORUN", "true").strip().lower() == "true"
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
SWING_RUN_AFTER = (16, 0)          # IST - after the daily candle is final
SWING_RETRY_COOLDOWN_SECONDS = 1800  # don't hammer Dhan if a swing run fails


def _within_market_hours(now_ist: datetime) -> bool:
    if now_ist.weekday() >= 5:  # Sat/Sun
        return False
    open_t = now_ist.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    close_t = now_ist.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_t <= now_ist <= close_t


def _intraday_loop():
    log.info(f"Intraday scanner autorun started - every {POLL_INTERVAL_SECONDS}s (fixed schedule) during "
             f"{MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}-{MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d} IST, Mon-Fri.")
    while True:
        try:
            if _within_market_hours(datetime.now(IST)):
                t0 = time.monotonic()
                try:
                    import live_scanner
                    log.info("Running intraday scan...")
                    live_scanner.run_scan("intraday")
                    log.info("Intraday scan complete.")
                except Exception as e:
                    log.error(f"Error during intraday scan: {e}")
                # fixed-rate: next scan is due POLL_INTERVAL after this one STARTED
                time.sleep(max(5.0, POLL_INTERVAL_SECONDS - (time.monotonic() - t0)))
            else:
                time.sleep(60)  # outside hours: check the clock every minute so 9:15 starts on time
        except Exception as e:
            log.error(f"Error in intraday scanner loop: {e}")
            time.sleep(60)


def _swing_results_path() -> str:
    return os.path.join(RESULTS_DIR, "latest_swing.json")


def _swing_due(now_ist: datetime) -> bool:
    """True if there are no swing results yet (seed), or it's a weekday past
    16:00 IST and the last swing results were written before today."""
    path = _swing_results_path()
    if not os.path.exists(path):
        return True
    if now_ist.weekday() >= 5:
        return False
    after_t = now_ist.replace(hour=SWING_RUN_AFTER[0], minute=SWING_RUN_AFTER[1], second=0, microsecond=0)
    if now_ist < after_t:
        return False
    last_run_date = datetime.fromtimestamp(os.path.getmtime(path), IST).date()
    return last_run_date < now_ist.date()


def _swing_loop():
    log.info(f"Swing scanner autorun started - once a day after {SWING_RUN_AFTER[0]:02d}:{SWING_RUN_AFTER[1]:02d} IST "
             f"(Mon-Fri), plus one seed run if no swing results exist yet.")
    last_attempt = 0.0
    while True:
        try:
            if _swing_due(datetime.now(IST)) and (time.monotonic() - last_attempt) >= SWING_RETRY_COOLDOWN_SECONDS:
                last_attempt = time.monotonic()
                try:
                    import live_scanner
                    log.info("Running swing scan...")
                    live_scanner.run_scan("swing")
                    log.info("Swing scan complete.")
                except Exception as e:
                    log.error(f"Error during swing scan: {e}")
            time.sleep(300)
        except Exception as e:
            log.error(f"Error in swing scanner loop: {e}")
            time.sleep(300)


GAINERS_AUTORUN = os.environ.get("GAINERS_STUDY_AUTORUN", "true").strip().lower() == "true"
GAINERS_RUN_AFTER = (16, 30)       # IST - after the intraday/swing scans have had their turn
GAINERS_RETRY_COOLDOWN_SECONDS = 1800


def _gainers_results_path() -> str:
    return os.path.join(RESULTS_DIR, "gainers.json")


def _gainers_due(now_ist: datetime) -> bool:
    """Seed run when there are no results (fresh deploy) - but never during
    market hours, so it cannot steal request budget from the 10-minute
    intraday cycle - and once a day on weekdays after 16:30 IST."""
    path = _gainers_results_path()
    if not os.path.exists(path):
        return not _within_market_hours(now_ist)
    if now_ist.weekday() >= 5:
        return False
    after_t = now_ist.replace(hour=GAINERS_RUN_AFTER[0], minute=GAINERS_RUN_AFTER[1], second=0, microsecond=0)
    if now_ist < after_t:
        return False
    return datetime.fromtimestamp(os.path.getmtime(path), IST).date() < now_ist.date()


def _gainers_loop():
    log.info(f"Gainers study autorun started - once a day after {GAINERS_RUN_AFTER[0]:02d}:{GAINERS_RUN_AFTER[1]:02d} IST "
             f"(Mon-Fri), plus one seed run if no results exist yet (outside market hours).")
    last_attempt = 0.0
    while True:
        try:
            if _gainers_due(datetime.now(IST)) and (time.monotonic() - last_attempt) >= GAINERS_RETRY_COOLDOWN_SECONDS:
                last_attempt = time.monotonic()
                try:
                    import gainers_study
                    log.info("Running gainers study...")
                    gainers_study.run_study()
                    log.info("Gainers study complete.")
                except Exception as e:
                    log.error(f"Error during gainers study: {e}")
            time.sleep(300)
        except Exception as e:
            log.error(f"Error in gainers loop: {e}")
            time.sleep(300)


VOLSPURT_AUTORUN = os.environ.get("VOLSPURT_STUDY_AUTORUN", "true").strip().lower() == "true"
VOLSPURT_RUN_AFTER = (17, 30)      # IST - after the gainers study (16:30) has finished
VOLSPURT_RETRY_COOLDOWN_SECONDS = 1800


def _volspurt_results_path() -> str:
    return os.path.join(RESULTS_DIR, "volspurt.json")


def _volspurt_due(now_ist: datetime) -> bool:
    """Seed run when there are no results, outside market hours, and only once
    the gainers study has produced its file (so the two never run together).
    Afterwards once a day on weekdays after 17:30 IST."""
    path = _volspurt_results_path()
    if not os.path.exists(path):
        return (not _within_market_hours(now_ist)) and os.path.exists(_gainers_results_path())
    if now_ist.weekday() >= 5:
        return False
    after_t = now_ist.replace(hour=VOLSPURT_RUN_AFTER[0], minute=VOLSPURT_RUN_AFTER[1], second=0, microsecond=0)
    if now_ist < after_t:
        return False
    return datetime.fromtimestamp(os.path.getmtime(path), IST).date() < now_ist.date()


def _volspurt_loop():
    log.info(f"Volume-spurt backtest autorun started - once a day after {VOLSPURT_RUN_AFTER[0]:02d}:{VOLSPURT_RUN_AFTER[1]:02d} IST "
             f"(Mon-Fri), plus one seed run if no results exist yet (outside market hours, after the gainers study).")
    last_attempt = 0.0
    while True:
        try:
            if _volspurt_due(datetime.now(IST)) and (time.monotonic() - last_attempt) >= VOLSPURT_RETRY_COOLDOWN_SECONDS:
                last_attempt = time.monotonic()
                try:
                    import volume_spurt_study
                    log.info("Running volume-spurt backtest...")
                    volume_spurt_study.run_study()
                    log.info("Volume-spurt backtest complete.")
                except Exception as e:
                    log.error(f"Error during volume-spurt backtest: {e}")
            time.sleep(300)
        except Exception as e:
            log.error(f"Error in volume-spurt loop: {e}")
            time.sleep(300)


if AUTORUN:
    threading.Thread(target=_intraday_loop, daemon=True, name="intraday-autorun").start()
if SWING_AUTORUN:
    threading.Thread(target=_swing_loop, daemon=True, name="swing-autorun").start()
if GAINERS_AUTORUN:
    threading.Thread(target=_gainers_loop, daemon=True, name="gainers-autorun").start()
if VOLSPURT_AUTORUN:
    threading.Thread(target=_volspurt_loop, daemon=True, name="volspurt-autorun").start()


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


def _serve_html(filename: str):
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    with open(path) as f:
        return f.read()


def _serve_csv(filename: str, download_name: str):
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    return send_file(path, mimetype="text/csv", as_attachment=True, download_name=download_name)


@app.route("/scanner", methods=["GET"])
def scanner_results():
    return _serve_html("latest.html")


@app.route("/scanner/signals.csv", methods=["GET"])
def scanner_signals_csv():
    return _serve_csv("latest_signals.csv", "all_in_one_pro_intraday_signals.csv")


@app.route("/scanner/signals_b.csv", methods=["GET"])
def scanner_signals_b_csv():
    return _serve_csv("latest_signals_b.csv", "all_in_one_pro_intraday_signals_section_b.csv")


@app.route("/swing", methods=["GET"])
def swing_results():
    return _serve_html("latest_swing.html")


@app.route("/swing/signals.csv", methods=["GET"])
def swing_signals_csv():
    return _serve_csv("latest_swing_signals.csv", "all_in_one_pro_swing_signals.csv")


@app.route("/swing/signals_b.csv", methods=["GET"])
def swing_signals_b_csv():
    return _serve_csv("latest_swing_signals_b.csv", "all_in_one_pro_swing_signals_section_b.csv")


@app.route("/gainers", methods=["GET"])
def gainers_results():
    return _serve_html("gainers.html")


@app.route("/gainers/picks.csv", methods=["GET"])
def gainers_picks_csv():
    return _serve_csv("gainers_picks.csv", "morning_gainers_all_picks.csv")


@app.route("/gainers/latest.csv", methods=["GET"])
def gainers_latest_csv():
    return _serve_csv("gainers_latest.csv", "morning_gainers_latest_day.csv")


@app.route("/gainers/summary.csv", methods=["GET"])
def gainers_summary_csv():
    return _serve_csv("gainers_summary.csv", "morning_gainers_per_stock.csv")


@app.route("/volspurt", methods=["GET"])
def volspurt_results():
    return _serve_html("volspurt.html")


@app.route("/volspurt/grid.csv", methods=["GET"])
def volspurt_grid_csv():
    return _serve_csv("volspurt_grid.csv", "volume_spurt_rules.csv")


@app.route("/volspurt/signals.csv", methods=["GET"])
def volspurt_signals_csv():
    return _serve_csv("volspurt_signals.csv", "volume_spurt_all_signals.csv")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
