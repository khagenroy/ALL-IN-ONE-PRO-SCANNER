"""
TradingView -> Dhan webhook bridge for the "PA+UNI Combined" Pine script.

WHAT THIS DOES
---------------
TradingView sends every alert() from the Pine script to this server as one
webhook. This server does exactly ONE thing with them:

  - A CONFIRMED buy/sell signal (any of "STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED",
    "STRATEGY_2_ORDER_BLOCK_CONFIRMED", "CONFIRMED_BUY", "CONFIRMED_SELL")
    -> first checks Dhan directly for whether this exact security already
    has an open position; if it does, the signal is logged only and
    skipped, so it never stacks/nets on top of an existing trade (manual
    or automatic). Only once confirmed flat does it place a LIMIT entry
    order on Dhan at the alert's "price" (the breakout level the script
    confirmed on) - not a MARKET order. This way
    a trade only fills if price actually comes back to that level; if it
    runs away and never returns, the order simply never fills and nothing
    is traded. A background thread checks every ~15 seconds whether a
    pending entry has filled; only once it has does this place the
    STOP_LOSS_MARKET order at the alert's "sl" price (the strict, original
    signal-candle stop) - never before, since there's nothing to protect
    until the position actually exists. If the entry order gets cancelled,
    rejected, or expires unfilled, that's logged and nothing more happens -
    no trade, no SL needed.

  - The symbol is looked up against Dhan's scrip master to decide whether
    it's an NSE stock or an MCX commodity, which decides both the
    exchangeSegment sent to Dhan (NSE_EQ vs MCX_COMM) and which entry
    cutoff time applies (STOCK_CUTOFF_TIME, default 2:30pm IST, vs
    COMMODITY_CUTOFF_TIME, default 9:30pm IST). Past the applicable
    cutoff, a confirmed signal is logged only - no new order. This only
    blocks NEW entries; a stop-loss order already placed at entry time is
    a standing order on Dhan and keeps working past the cutoff on its own.

  - A "MOVE_SL_TO_COST" alert (fired by the script once a trade's T1 is
    touched) -> MODIFIES the existing STOP_LOSS_MARKET order for that
    symbol to the entry price (breakeven), using the order id recorded
    when the trade was opened. It does not place any new order and is not
    affected by the entry cutoff times above (it's not a new entry). If
    this server has no record of that symbol's SL order (e.g. it was
    restarted since the entry was placed - see the in-memory note below),
    it just logs a warning instead - move the SL manually in the Dhan app
    in that case.

  - A RE-ENTRY alert (any of "STRATEGY_1_LIQUIDITY_SWEEP_RE_ENTRY",
    "STRATEGY_2_ORDER_BLOCK_RE_ENTRY", "RE_ENTRY_SSL_SWEEP",
    "RE_ENTRY_BSL_SWEEP", "RE_ENTRY_RSI_CHECKLIST") -> same cutoff/segment/
    MCX-lot/anti-stacking handling as a CONFIRMED entry above, but ONLY
    acted on when the alert's own "auto_execute" field is true - that field
    mirrors the
    script's "Enable Re-Entry Auto-Execution" toggle, so the toggle in
    TradingView's Pine settings is what actually gates this, not anything
    set here. Re-entry alerts never carry a "price" (only "sl"), so
    open_position's missing-price fallback takes over and this enters at
    MARKET rather than LIMIT - appropriate here since the retest candle
    that triggered it has already closed. Once filled, the SL is attached
    exactly like a CONFIRMED entry (see below), from the alert's "sl".

EVERY OTHER ALERT TYPE IS LOGGED ONLY - NO DHAN API CALL AT ALL.
This includes: the primary (non-CONFIRMED) Section A signals, SL_BREACHED,
PROFIT_TAKEN_T5, SL_TAKEN, and any RE-ENTRY alert whose "auto_execute" is
false. They still show up in trade_log.jsonl and in Render's logs so you
can see them, and TradingView still delivers them to your phone/email/app
exactly as before if you keep those notification channels on for the
alert - this server just doesn't act on them. That means: once a trade is
placed, this bridge does not square it off automatically at T5 or on the
SL_TAKEN alert - the protective SL order already placed on Dhan (moved to
breakeven at T1, per above) is what actually closes the trade when price
hits it; a T5 target hit is only ever a notification here, not an
auto-exit, unless you close it yourself.

WHAT THIS DOES NOT DO
-----------------------
  - It does not square off / exit positions automatically - the SL order
    placed at entry (and moved to breakeven at T1) is what closes the
    trade on Dhan's side. If a target (T5, or Section A's T1-T4) is hit,
    that's alert-only; you close it yourself if you want out before the SL.
  - Its memory of which SL order belongs to which symbol (used for the
    T1 breakeven move) lives only in this process - it is lost on a
    restart/redeploy. If that happens between your entry and T1, the
    breakeven shift is skipped and logged; move that SL manually.
  - It does not act on a RE-ENTRY alert unless that alert's own
    "auto_execute" field is true (see above) - with the script's Re-Entry
    Auto-Execution toggle left OFF, re-entries stay alert-only, same as
    before this was added.
  - It relies on the "sl" value the Pine script includes in its CONFIRMED
    alert JSON. If that field is missing/NaN, the SL order is skipped once
    the entry fills and logged as a warning - check your Dhan order book
    manually in that case.
  - It relies on the "price" value in the CONFIRMED alert JSON as the LIMIT
    entry price. If that's missing, it falls back to a MARKET order rather
    than silently doing nothing.
  - A pending LIMIT entry is only checked once every ~15 seconds by a
    background timer in this process - so a fill can take up to that long
    to be noticed and get its SL attached. This tracking is in-memory only
    (see below), so it does not survive a restart either.
  - It does not validate that Dhan accepted/filled an order beyond checking
    the HTTP response - always keep an eye on your Dhan order book too,
    especially for the first few days.

SAFETY
------
This is LIVE from the moment DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are set and
the service is reachable - there is no dry-run switch in this file. If you
want a test period first, set DRY_RUN=true in the environment; in that mode
every decision is logged exactly as it would be, but no Dhan API call is
made.
"""

import math
import os
import re
import json
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, request, jsonify, send_file

from scrip_master import get_security_id_and_segment, get_lot_size, warm_cache_async, get_index_security_id, get_symbol_for_security_id

# Build Dhan's NSE/MCX symbol lookup in the background right now, at process
# startup, instead of waiting for it to happen lazily inside the first real
# trade webhook (see scrip_master.warm_cache_async's docstring for why - a
# CONFIRMED_BUY alert once hit this cold and timed out, so the order never
# reached Dhan).
warm_cache_async()

# Order statuses Dhan uses that mean "fully filled" / "will never fill".
# Checked case-insensitively against whatever Dhan's API returns.
FILLED_STATUSES = {"TRADED", "EXECUTED", "FILLED"}
DEAD_STATUSES = {"CANCELLED", "REJECTED", "EXPIRED", "FAILED"}
PENDING_POLL_SECONDS = 15

# --------------------------------------------------------------------------
# CONFIG (all from environment variables - set these in Render's dashboard,
# never hard-code real keys into this file)
# --------------------------------------------------------------------------
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
PRODUCT_TYPE = os.environ.get("DHAN_PRODUCT_TYPE", "INTRADAY")  # INTRADAY (MIS) or CNC
DRY_RUN = os.environ.get("DRY_RUN", "false").strip().lower() == "true"
# Per-segment overrides - e.g. set DRY_RUN_NSE=false and DRY_RUN_MCX=true to
# go live on stocks while commodities stay in simulation. Either one left
# unset falls back to the global DRY_RUN above.
DRY_RUN_NSE = os.environ.get("DRY_RUN_NSE")
DRY_RUN_MCX = os.environ.get("DRY_RUN_MCX")


def is_dry_run(segment: str) -> bool:
    override = DRY_RUN_NSE if segment == "NSE_EQ" else DRY_RUN_MCX
    if override is not None:
        return override.strip().lower() == "true"
    return DRY_RUN

# No new order is placed on Dhan after these times, India time (IST) - one
# cutoff for stocks (NSE_EQ) and a later one for commodities (MCX_COMM),
# since MCX trades into the evening. Whichever cutoff applies is picked by
# looking up which exchange segment the alert's symbol belongs to. Only
# NEW entries are blocked by this - a stop-loss order already placed at
# entry time is a standing order on Dhan and keeps working on its own past
# the cutoff; there is nothing for this bridge to re-send.
STOCK_CUTOFF_TIME = os.environ.get("STOCK_CUTOFF_TIME", "14:30")
COMMODITY_CUTOFF_TIME = os.environ.get("COMMODITY_CUTOFF_TIME", "21:30")
IST = ZoneInfo("Asia/Kolkata")

# ADDED 2026-10-01 (Khagen's request: too much log clutter overnight, when
# both NSE (closes 15:30 IST) and MCX (closes 23:45 IST) are shut and
# nothing is happening). This does NOT stop the service or any watchdog -
# cascade trailing, manual-trade detection and SL protection all keep
# running exactly as before, 24/7. It only silences the two purely
# cosmetic "still alive" heartbeat log lines (SL_WATCHDOG_ALIVE,
# POLL_LOOP_ALIVE) during the dead window - every real event (a trade, an
# SL move, an error) still logs at any hour, unaffected by this.
LOG_QUIET_HOURS_START = os.environ.get("LOG_QUIET_HOURS_START", "00:00")
LOG_QUIET_HOURS_END = os.environ.get("LOG_QUIET_HOURS_END", "08:30")


def _in_log_quiet_hours() -> bool:
    try:
        start_h, start_m = (int(x) for x in LOG_QUIET_HOURS_START.split(":"))
        end_h, end_m = (int(x) for x in LOG_QUIET_HOURS_END.split(":"))
    except (ValueError, AttributeError):
        return False  # a bad env value should never silence real diagnostics - fail open
    now_dt = datetime.now(IST)
    now = now_dt.time()
    start = now_dt.replace(hour=start_h, minute=start_m, second=0, microsecond=0).time()
    end = now_dt.replace(hour=end_h, minute=end_m, second=0, microsecond=0).time()
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end  # window wraps past midnight (e.g. 00:00-08:30 doesn't wrap, but kept general)

# The mirror image of the cutoff times above: no NEW order is placed on
# Dhan BEFORE these times either, India time (IST). This exists to skip
# the first few minutes right after the market opens, when price is often
# most whipsaw-y and a confirmed signal is more likely to be a bad one.
# NSE stocks open 9:15, MCX commodities open 9:00 - defaults below are
# each open time plus a 15-minute buffer. Set either to "00:00" to turn
# the filter off entirely for that segment. Only NEW entries are blocked
# by this - it has no effect on a position that's already open.
STOCK_START_TIME = os.environ.get("STOCK_START_TIME", "09:30")
COMMODITY_START_TIME = os.environ.get("COMMODITY_START_TIME", "09:15")

# MCX orders must be an exact multiple of the exchange lot size (e.g. Gold
# Mini = 10 grams/lot). Every alert now carries "fixed_lot" (true/false)
# telling this server which of two sizing modes the script used for that
# specific commodity:
#   fixed_lot=true  - "Use Fixed Lot Size" is ON on that chart; qty IS the
#                      lot count directly. Multiplied by the real lot size
#                      (from Dhan's own scrip master) to get the exact
#                      exchange quantity. MCX_MAX_LOTS guards against an
#                      implausibly large "lot count" (usually a sign the
#                      toggle was actually left OFF).
#   fixed_lot=false - risk-based mode, same ₹-risk formula stocks use. qty
#                      is NOT a lot count; dividing it by the real lot size
#                      gives the number of lots that keeps this trade's
#                      risk at or under the chart's configured risk amount.
#                      If that comes out to less than 1 full lot - common
#                      on Gold/Silver, where even 1 lot can risk more than
#                      a modest ₹ budget - the signal is skipped entirely
#                      rather than firing an oversized position. See
#                      resolve_mcx_qty() for the exact math.
MCX_MAX_LOTS = int(os.environ.get("MCX_MAX_LOTS", "10"))

# FIX 2026-09-29 - real incident: a NATGASMINI CONFIRMED_BUY alert got a
# quantity of 526 sent to Dhan and RMS rejected it. Traced to
# get_lot_size("NATGASMINI1!") returning 1 instead of the real ~250-unit
# lot size Dhan's own scrip master would be expected to carry for this
# contract - meaning resolve_mcx_qty()'s "lots" recompute below was
# effectively treating this MCX commodity as if it had no lot granularity
# at all (same as an NSE stock), so the resulting quantity wasn't aligned
# to whatever real lot restriction Dhan's exchange actually enforces on
# this contract. Root cause (why the scrip master reports 1 for this
# specific underlying) isn't confirmed - could be a real quirk of how Dhan
# quotes this contract, or a data issue - so rather than guessing a
# hardcoded lot size, this refuses to trade any MCX symbol whose resolved
# lot size looks implausible (no real commodity contract trades in
# single-unit lots) until it's verified. Skips loudly rather than silently
# trusting a number that already produced one rejected order.
MCX_MIN_PLAUSIBLE_LOT_SIZE = int(os.environ.get("MCX_MIN_PLAUSIBLE_LOT_SIZE", "2"))

# RISK-BASED SIZING, RECOMPUTED SERVER-SIDE (separate settings for stocks vs
# MCX, since they trade completely different capital - set each in Render's
# environment independently): when an alert is in risk-based mode
# (fixed_lot=false), this bridge no longer trusts the script's qty field -
# it recomputes the size itself from this alert's own entry/SL prices:
#   stocks: qty_shares = RISK_RUPEES_STOCKS / abs(entry_price - sl_price)
#   MCX:    lots = RISK_RUPEES_MCX / (abs(entry_price - sl_price) * lot_size)
# A wider stop correctly gives fewer shares/lots, a tighter stop more - that
# part is expected, not a bug. What this removes is dependence on the
# script having done that division correctly; the bridge always does its
# own version of the same math from numbers already on the alert. Keep
# these equal to the ₹ risk amount set in the script's own risk inputs
# (riskRupees / u_riskRupees) - if they drift apart, trading stays safe
# (the bridge's number always wins) but you'll see a MISMATCH_QTY_VS_SCRIPT
# log line as a signal the two settings disagree.
RISK_RUPEES_STOCKS = float(os.environ.get("RISK_RUPEES_STOCKS", "2000"))
RISK_RUPEES_MCX = float(os.environ.get("RISK_RUPEES_MCX", "2000"))

# How far (in %) the current market price is allowed to have drifted away
# from the signal's confirmed price before an entry is skipped instead of
# chased. e.g. 0.3 means: signal price 220, current price up to 220.66 is
# fine, but 222+ means the move has already happened and the entry is
# skipped rather than taken late at a much worse price. Set to "0" to
# disable this check entirely (never skip on price drift).
ENTRY_PRICE_TOLERANCE_PCT = float(os.environ.get("ENTRY_PRICE_TOLERANCE_PCT", "0.3"))

DHAN_BASE_URL = "https://api.dhan.co/v2"
LOG_FILE = os.path.join(os.path.dirname(__file__), "trade_log.jsonl")

# Persists last_entries/pending_entries (below) to disk so a redeploy or
# restart doesn't erase this bridge's memory of which SL order belongs to
# which open trade. Without this, MOVE_SL_TO_COST (the breakeven-at-T1
# move) silently does nothing for any trade that was already open going
# into a restart - the exact failure that turned two should-be-breakeven
# trades into real losses today. Render's disk for a web service is
# ephemeral across a full redeploy in some plans, so this is a best-effort
# safety net for same-day restarts (config/env changes, crashes), not a
# guarantee across every possible redeploy - but it fixes the common case.
TRADE_STATE_FILE = os.path.join(os.path.dirname(__file__), "trade_state.json")

# Dhan's order-placement API requires the request to come from a whitelisted
# static IP (without this, real orders fail with DH-905 "Invalid IP", even
# though Render's own IP is never fixed/whitelistable). DHAN_PROXY_URL routes
# every outbound call to Dhan through a dedicated static-IP HTTPS proxy
# instead - set it in Render's environment to a URL of the form
# "https://USERNAME:PASSWORD@HOST:PORT" (e.g. from StaticIP.in's "Proxy URL"
# field). Leave it unset to call Dhan directly (useful for local testing) -
# but for real trading it must be set, since that's the whole IP-whitelist fix.
DHAN_PROXY_URL = os.environ.get("DHAN_PROXY_URL", "").strip()
DHAN_PROXIES = {"https": DHAN_PROXY_URL, "http": DHAN_PROXY_URL} if DHAN_PROXY_URL else None

# Push-notification topic for ntfy.sh (https://ntfy.sh) - a free service
# with no account needed: pick a hard-to-guess topic name, install the ntfy
# app, subscribe to that topic, and set NTFY_TOPIC in Render's environment
# to the same name. Used ONLY for events that mean real money is exposed
# without protection (a position open with no SL) - never for routine
# activity, so every notification received is worth looking at immediately.
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()


def notify(title: str, message: str, priority: str = "urgent"):
    """Sends a push notification via ntfy.sh. Best-effort only - never
    raises, never blocks trading logic if it fails or NTFY_TOPIC is unset.
    UPDATED 2026-10-08: a non-2xx answer from ntfy (e.g. 429 rate limit) used to be silent - the
    message was simply lost. It is now logged (NTFY_FAILED) and retried up to 3 times in a background
    thread, so a rate-limited notification still arrives and the failure is visible in the logs."""
    if not NTFY_TOPIC:
        return

    def _send():
        for attempt in range(3):
            try:
                r = requests.post(
                    f"https://ntfy.sh/{NTFY_TOPIC}",
                    data=message.encode("utf-8"),
                    headers={"Title": title, "Priority": priority, "Tags": "warning"},
                    timeout=10,
                )
                if r.status_code < 300:
                    return
                log.warning(f"NTFY_FAILED status={r.status_code} attempt={attempt + 1} title={title!r} body={r.text[:200]!r}")
            except Exception as e:
                log.warning(f"ntfy notify failed (attempt {attempt + 1}): {e}")
            time.sleep(8 * (attempt + 1))

    threading.Thread(target=_send, daemon=True, name="ntfy-send").start()

# Only these alert types ever result in a Dhan order. Everything else that
# reaches the webhook is logged and nothing more.
# ADDED 14 Nov 2026 (Khagen's request): the four new indicator signals place REAL orders through exactly the same path as Section A's confirmed entries
# (risk lock, 9:30 start, 14:30 cutoff, anti-stacking, flip, Super Order with SL + target T2). Defaults ON; set NEW_SIGNALS_AUTO_EXECUTE=false in Render to
# make them logged-only again (no other change needed).
#   VS_SELL           volume-spurt SELL (VS SELL checklist, Section V):  entry = red candle low, SL = swing high, target = T2
#   VS_BUY            spurt retracement BUY (Section S):                 entry = close,          SL = hold low,   target = T2
#   SPURT_CHOCH_BUY / SPURT_CHOCH_SELL  (Section D):                     entry = close,          SL = last swing, target = T2
NEW_SIGNALS_AUTO_EXECUTE = os.environ.get("NEW_SIGNALS_AUTO_EXECUTE", "true").strip().lower() != "false"
NEW_SIGNAL_TYPES = ("VS_SELL", "VS_BUY", "SPURT_CHOCH_BUY", "SPURT_CHOCH_SELL")
CONFIRMED_ENTRY_TYPES = (
    "STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED",
    "STRATEGY_2_ORDER_BLOCK_CONFIRMED",
    "CONFIRMED_BUY",
    "CONFIRMED_SELL",
) + (NEW_SIGNAL_TYPES if NEW_SIGNALS_AUTO_EXECUTE else ())

# ADDED 2026-10-01 (Khagen's request, backed by his own 1-year/~41,000-trade
# backtest across 267 NSE F&O symbols - see the uploaded ALL_FO_stock.zip
# analysis): CONFIRMED_BUY/CONFIRMED_SELL ("Section B" - the Unified setup,
# i.e. the "u_" prefixed logic in the live script) tested at 39.5% win rate
# / profit factor 0.84 on fresh signals (net LOSING after costs), versus
# 50-52% win rate / profit factor 2.0-2.4 for Section A's
# STRATEGY_1_LIQUIDITY_SWEEP_CONFIRMED and STRATEGY_2_ORDER_BLOCK_CONFIRMED.
# Defaults to NOT auto-executing Section B any more - the alert still
# arrives and gets logged exactly like a re-entry alert does (see
# is_re_entry_type's handling below), Khagen can still see it and take it
# manually if he wants, the bridge just no longer places it automatically.
# Set SECTION_B_AUTO_EXECUTE=true in Render's env vars to restore the old
# behavior - no code change/redeploy of this file needed to flip it back.
SECTION_B_AUTO_EXECUTE = os.environ.get("SECTION_B_AUTO_EXECUTE", "false").strip().lower() == "true"
SECTION_B_CONFIRMED_TYPES = ("CONFIRMED_BUY", "CONFIRMED_SELL")

# MANUAL TRADE DETECTOR - ADDED 2026-10-01 (Khagen's request: he sometimes
# opens a position AND places its SL entirely by hand on Dhan's own app, with
# no alert ever sent to this bridge at all - not a re-entry after a bridge-
# tracked SL hit (reconcile_symbol_from_dhan already covers that), a genuinely
# fresh manual trade this bridge has never heard of. See
# detect_and_register_manual_trades()/manual_trade_poll_loop() below (kept as
# their own separate, clearly-labelled section per Khagen's request, not
# folded into webhook()/open_position()/reconcile_symbol_from_dhan() - it
# never places or modifies an entry order, only ever reads positions/orders
# and, once, registers an already-existing manual trade into last_entries so
# the EXISTING cascade watchdog (_check_cascade_targets(), already running
# every SL_WATCHDOG_POLL_SECONDS) picks up its SL shifting automatically -
# no change to that watchdog or to _handle_move_sl_to_cost() was needed).
MANUAL_TRADE_DETECTION_ENABLED = os.environ.get("MANUAL_TRADE_DETECTION_ENABLED", "true").strip().lower() == "true"
MANUAL_TRADE_POLL_SECONDS = int(os.environ.get("MANUAL_TRADE_POLL_SECONDS", "30"))

# Section A's re-entry types are suffixed ("..._RE_ENTRY"); Section B's are
# prefixed ("RE_ENTRY_..."). Unlike CONFIRMED_ENTRY_TYPES, matching one of
# these is not enough on its own to place an order - see is_re_entry_type()
# and the webhook handler below, which additionally requires the alert's
# own "auto_execute" field to be true (the script's Re-Entry
# Auto-Execution toggle).
def is_re_entry_type(alert_type: str) -> bool:
    return bool(alert_type) and (alert_type.endswith("_RE_ENTRY") or alert_type.startswith("RE_ENTRY"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dhan-bridge")


def _past_cutoff(cutoff_str: str):
    now_ist = datetime.now(IST)
    cutoff_h, cutoff_m = (int(x) for x in cutoff_str.split(":"))
    cutoff = now_ist.replace(hour=cutoff_h, minute=cutoff_m, second=0, microsecond=0)
    return now_ist >= cutoff


def past_entry_cutoff(segment: str):
    """True once it's past the entry cutoff (IST) for this segment - no more
    new orders. segment: "NSE_EQ" (stocks, cutoff STOCK_CUTOFF_TIME) or
    "MCX_COMM" (commodities, cutoff COMMODITY_CUTOFF_TIME)."""
    cutoff_str = COMMODITY_CUTOFF_TIME if segment == "MCX_COMM" else STOCK_CUTOFF_TIME
    return _past_cutoff(cutoff_str)


def cutoff_for(segment: str):
    return COMMODITY_CUTOFF_TIME if segment == "MCX_COMM" else STOCK_CUTOFF_TIME


def _before_start(start_str: str):
    if start_str == "00:00":
        return False  # filter turned off for this segment
    now_ist = datetime.now(IST)
    start_h, start_m = (int(x) for x in start_str.split(":"))
    start = now_ist.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    return now_ist < start


def before_entry_start(segment: str):
    """True until it's past the entry start time (IST) for this segment -
    too early, no new orders yet. segment: "NSE_EQ" (stocks, STOCK_START_TIME)
    or "MCX_COMM" (commodities, COMMODITY_START_TIME)."""
    start_str = COMMODITY_START_TIME if segment == "MCX_COMM" else STOCK_START_TIME
    return _before_start(start_str)


def start_time_for(segment: str):
    return COMMODITY_START_TIME if segment == "MCX_COMM" else STOCK_START_TIME

app = Flask(__name__)

# A simple in-memory + logged record of the last entry per symbol, purely
# for /health and your own reference - nothing here gates or triggers any
# order. There is no square-off logic in this bridge (see module docstring).
last_entries = {}

# STUCK-SL WATCHDOG - ADDED 2026-09-28 (real incident: AU Small Finance
# Bank's Super Order STOP_LOSS_LEG triggered when price crossed its SL, but
# Dhan places the triggered SL as a LIMIT order at the SL price, not a
# market order. Price kept falling past that limit price with no buyer
# left at it, so the SL order just sat unfilled while the position kept
# losing - "SL hits but no exit". Khagen had to close it manually. See
# check_stuck_sl_legs() below for the fix: force a market exit if a
# STOP_LOSS_LEG stays TRIGGERED-but-unfilled for too long.
_sl_stuck_since = {}  # {leg_order_id: first_seen_utc_timestamp}
_sl_stuck_lock = threading.Lock()
SL_WATCHDOG_POLL_SECONDS = 5
SL_WATCHDOG_GRACE_SECONDS = 10
# ADDED 2026-10-05: the "vanished order" fallback below force-closes a
# position at market when a tracked SL order is missing from /super/orders.
# That closed healthy manual positions, so it is now OFF by default (it only
# logs + notifies once). Set SL_VANISHED_FORCE_EXIT_ENABLED=true to restore it.
# The other force-exit (a TRIGGERED SL leg left unfilled) is unaffected.
SL_VANISHED_FORCE_EXIT_ENABLED = os.environ.get("SL_VANISHED_FORCE_EXIT_ENABLED", "false").strip().lower() == "true"

# Dhan access tokens expire 24 hours after they're generated (Dhan's own
# policy - there is no long-lived token). This bridge cannot refresh it for
# you automatically (that would require storing your Dhan login/2FA here,
# which is not something this does) - but it actively checks the token
# every 30 minutes and after every failed order, so a dead token is caught
# and logged loudly instead of silently failing your next real trade.
token_status = {"valid": None, "checked_at": None, "message": "not checked yet"}
TOKEN_CHECK_SECONDS = 30 * 60


def log_event(record: dict):
    record["ts"] = datetime.now(timezone.utc).isoformat()
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")
    log.info(json.dumps(record))


# --------------------------------------------------------------------------
# DHAN API HELPERS
# --------------------------------------------------------------------------
def dhan_headers():
    # FIX 2026-09-28 (real incident: /check_option_chain returned 401
    # "ClientId is invalid" (Dhan error 810) for both NIFTY and BANKNIFTY -
    # confirmed against Dhan's own option-chain docs that "client-id" is a
    # REQUIRED header for optionchain/expirylist and optionchain, alongside
    # access-token - something the order/margin endpoints apparently don't
    # enforce the same way (they've been working fine without it, sending
    # dhanClientId in the BODY instead), but option-chain does. Adding it
    # here for every call rather than a one-off special case, since Dhan's
    # docs list it as required and it can't hurt any other endpoint.
    return {"access-token": DHAN_ACCESS_TOKEN, "client-id": DHAN_CLIENT_ID, "Content-Type": "application/json"}


def _is_auth_failure(status_code: int, body) -> bool:
    """True if this response looks like an expired/invalid access token,
    rather than some other order-specific rejection (bad qty, market
    closed, etc). Checked broadly (status code + text match) since Dhan's
    exact error code for this isn't worth hard-coding and getting wrong."""
    if status_code in (401, 403):
        return True
    text = json.dumps(body).lower() if not isinstance(body, str) else body.lower()
    return any(needle in text for needle in
               ("invalid token", "invalid access", "token expired", "authentication failed",
                "not authorized", "dh-901", "dh-902"))


def _note_token_check(valid: bool, message: str):
    token_status["valid"] = valid
    token_status["checked_at"] = datetime.now(timezone.utc).isoformat()
    token_status["message"] = message
    if not valid:
        log.error(f"DHAN_TOKEN_EXPIRED: {message} - regenerate DHAN_ACCESS_TOKEN on Dhan's site "
                  "and update it in Render's environment variables now.")
        log_event({"action": "DHAN_TOKEN_EXPIRED", "message": message})


def check_token_validity():
    """A single lightweight, read-only, authenticated call used purely to
    confirm the current DHAN_ACCESS_TOKEN still works - does not place or
    touch any order. Called at startup, every TOKEN_CHECK_SECONDS on a
    background timer, and opportunistically whenever a real order call
    fails, so an expired token is caught even on a quiet day with no
    trades attempted."""
    if not DHAN_ACCESS_TOKEN:
        _note_token_check(False, "DHAN_ACCESS_TOKEN is not set")
        return
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/fundlimit", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        if resp.ok:
            _note_token_check(True, "token OK")
        elif _is_auth_failure(resp.status_code, body):
            _note_token_check(False, f"HTTP {resp.status_code}: {body}")
        else:
            # Reachable and authenticated, just an unexpected response shape -
            # don't flag the token itself as bad over this.
            _note_token_check(True, f"token OK (unexpected /fundlimit response: HTTP {resp.status_code})")
    except Exception as e:
        log.warning(f"Token validity check couldn't complete (network/Dhan issue, not necessarily the token): {e}")


def _token_check_loop():
    while True:
        try:
            check_token_validity()
        except Exception as e:
            log.error(f"Error during scheduled token check: {e}")
        time.sleep(TOKEN_CHECK_SECONDS)


threading.Thread(target=_token_check_loop, daemon=True).start()


def _is_ip_whitelist_failure(body) -> bool:
    """True if Dhan's response looks like the DH-905 'Invalid IP' rejection
    specifically (DH-905 is reused for several unrelated validation errors -
    e.g. 'Invalid correlationId' also comes back as DH-905 - so this checks
    the message text, not just the error code)."""
    text = json.dumps(body).lower() if not isinstance(body, str) else body.lower()
    return "invalid ip" in text


def check_outbound_ip():
    """Asks a public IP-echo service, THROUGH the same DHAN_PROXIES this
    bridge uses for every real Dhan call, what outbound IP Dhan is actually
    seeing right now. Used to self-diagnose a DH-905 'Invalid IP' failure
    without you having to guess whether the static-IP proxy is still
    presenting the IP that's whitelisted on Dhan."""
    try:
        resp = requests.get("https://api.ipify.org?format=json", timeout=10, proxies=DHAN_PROXIES)
        return resp.json().get("ip", "unknown (unexpected response)")
    except Exception as e:
        return f"lookup failed: {e}"


def _log_ip_mismatch_diagnostic(context: str):
    outbound_ip = check_outbound_ip()
    log.error(f"IP WHITELIST MISMATCH SUSPECTED ({context}): the proxy's outbound IP right now is "
              f"{outbound_ip} - confirm this EXACT IP is whitelisted on Dhan's site. If it doesn't match "
              "what StaticIP.in shows as active, the proxy's IP has likely changed/rotated.")
    log_event({"action": "IP_WHITELIST_MISMATCH_SUSPECTED", "context": context, "outbound_ip": outbound_ip})


def safe_correlation_id(symbol: str, suffix: str) -> str:
    """Dhan's correlationId rejects at least some special characters (seen
    live: a TradingView continuous-futures symbol like "SILVERM1!" - the
    trailing "!" - got DH-905 "Invalid correlationId", even though the
    security id resolved fine and the rest of the payload was valid).
    Strips everything except letters/digits/hyphen/underscore before
    building the id, then truncates to Dhan's 30-char limit."""
    clean_symbol = re.sub(r"[^A-Za-z0-9_-]", "", symbol) or "SYM"
    return f"{clean_symbol}-{suffix}"[:30]


def check_sufficient_margin(security_id: str, exchange_segment: str, transaction_type: str,
                             qty: int, price: float, order_type: str = "MARKET"):
    """ADDED 2026-09-30 (real incident: GOLDM order rejected by Dhan's own
    RMS with "insufficient funds, add Rs 26,38,093.27" - the bridge had no
    idea beforehand and just fired the order straight at Dhan). Calls
    Dhan's POST /margincalculator to find out the REAL margin this specific
    order needs and compares it against the REAL available balance Dhan
    itself returns in the same response - before we ever call
    place_order()/place_super_order() for real.

    Returns (ok: bool, detail: dict). ok=True means either the check
    passed, or the check itself couldn't be completed (network/shape
    issue) - in that case we let the order attempt proceed rather than
    block a trade over OUR inability to check, and Dhan's own RMS is still
    there as the final backstop either way. ok=False means Dhan's margin
    calculator itself said this specific order does not have enough funds
    - detail carries the numbers for the alert."""
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "exchangeSegment": exchange_segment,
        "transactionType": transaction_type,
        "quantity": int(qty),
        "productType": PRODUCT_TYPE,
        "securityId": security_id,
        "price": price if price else 0,
    }
    try:
        resp = requests.post(f"{DHAN_BASE_URL}/margincalculator", headers=dhan_headers(),
                              json=payload, timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        if not resp.ok or not isinstance(body, dict):
            # Couldn't get a real answer from Dhan - don't block the trade
            # over our own check failing; log it and let the real order
            # attempt (with Dhan's own RMS) be the backstop.
            log_event({"action": "MARGIN_CHECK_INCONCLUSIVE", "security_id": security_id,
                       "http_status": resp.status_code, "response": body})
            return True, {}
        insufficient = body.get("insufficientBalance")
        total_margin = body.get("totalMargin")
        available = body.get("availableBalance")
        # insufficientBalance is Dhan's own field for exactly this check -
        # a positive value means this order needs that much more than what's
        # available right now.
        try:
            insufficient = float(insufficient) if insufficient is not None else 0.0
        except (TypeError, ValueError):
            insufficient = 0.0
        detail = {"total_margin_required": total_margin, "available_balance": available,
                   "insufficient_by": insufficient}
        if insufficient > 0:
            # ADDED 2026-10-01 (real incident: BHARTIARTL blocked as
            # "insufficient" with available_balance showing 3.6x the
            # required margin - these three summarized fields weren't
            # enough to tell why. Logging Dhan's full raw response here so
            # the next block is diagnosable from this log alone, since
            # Dhan's /margincalculator response may carry segment-wise or
            # other detail (e.g. equity vs commodity fund allocation) that
            # explains an apparent mismatch between these three numbers.
            log_event({"action": "MARGIN_CHECK_FAILED", "security_id": security_id,
                       "qty": qty, "raw_dhan_response": body, **detail})
            return False, detail
        log_event({"action": "MARGIN_CHECK_OK", "security_id": security_id, "qty": qty, **detail})
        return True, detail
    except Exception as e:
        log_event({"action": "MARGIN_CHECK_ERROR", "security_id": security_id, "error": str(e)})
        return True, {}


def place_order(security_id: str, transaction_type: str, qty: int, order_type: str,
                 exchange_segment: str = "NSE_EQ", price: float = None, trigger_price: float = None,
                 correlation_id: str = "", product_type: str = None):
    """transaction_type: BUY or SELL. order_type: LIMIT, MARKET, STOP_LOSS_MARKET or STOP_LOSS (stop with a limit price; ADDED 16 Nov, used by the swing SL).
    exchange_segment: "NSE_EQ" (stocks) or "MCX_COMM" (commodities)."""
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "correlationId": correlation_id[:30],
        "transactionType": transaction_type,
        "exchangeSegment": exchange_segment,
        "productType": product_type or PRODUCT_TYPE,
        "orderType": order_type,
        "validity": "DAY",
        "securityId": security_id,
        "quantity": int(qty),
        "price": price if order_type in ("LIMIT", "STOP_LOSS") else 0,
    }
    if order_type in ("STOP_LOSS_MARKET", "STOP_LOSS"):
        payload["triggerPrice"] = trigger_price

    if is_dry_run(exchange_segment):
        log_event({"action": "DRY_RUN_ORDER", "payload": payload})
        return {"orderId": "DRY_RUN", "orderStatus": "SIMULATED"}

    resp = requests.post(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), json=payload, timeout=15, proxies=DHAN_PROXIES)
    ok = resp.ok
    body = safe_json(resp)
    log_event({"action": "PLACE_ORDER", "payload": payload, "http_status": resp.status_code, "response": body})
    if not ok:
        log.error(f"Dhan order FAILED: {resp.status_code} {body}")
        if _is_auth_failure(resp.status_code, body):
            _note_token_check(False, f"order placement failed with HTTP {resp.status_code}: {body}")
        if _is_ip_whitelist_failure(body):
            _log_ip_mismatch_diagnostic(f"place_order {payload.get('transactionType')} {payload.get('securityId')}")
    return body


def cancel_regular_order(order_id: str, segment: str) -> bool:
    """ADDED 2026-09-27 for FLIP support (see webhook()'s CONFIRMED-entry
    handling below). Cancels a single plain (non-Super) order - used for the
    standalone SL order _attach_sl() places, via Dhan's DELETE /orders/
    {order-id}. Best-effort: if it already filled/cancelled on its own, this
    just logs and moves on - the market close that follows is what actually
    matters, not this cleanup step. segment picks the right DRY_RUN_NSE/
    DRY_RUN_MCX override, same as every other order call in this file."""
    if not order_id or order_id == "DRY_RUN":
        return True
    if is_dry_run(segment):
        log_event({"action": "DRY_RUN_CANCEL_ORDER", "order_id": order_id})
        return True
    try:
        resp = requests.delete(f"{DHAN_BASE_URL}/orders/{order_id}", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        ok = resp.ok
        log_event({"action": "ORDER_CANCELLED" if ok else "ORDER_CANCEL_FAILED",
                   "order_id": order_id, "http_status": resp.status_code, "response": safe_json(resp)})
        return ok
    except Exception as e:
        log_event({"action": "ORDER_CANCEL_ERROR", "order_id": order_id, "error": str(e)})
        return False


def cancel_super_order(order_id: str, segment: str) -> bool:
    """ADDED 2026-09-27 for FLIP support. Cancels a Super Order (entry +
    target + SL) via Dhan's DELETE /super/orders/{order-id}/{order-leg} -
    per Dhan's own docs, cancelling the ENTRY_LEG of the main order id
    cancels ALL of that order's legs together (target and SL included), so
    this is the single call needed to fully tear down one placed leg from
    open_position(). Best-effort, same as cancel_regular_order() above."""
    if not order_id or order_id == "DRY_RUN":
        return True
    if is_dry_run(segment):
        log_event({"action": "DRY_RUN_CANCEL_SUPER_ORDER", "order_id": order_id})
        return True
    try:
        resp = requests.delete(f"{DHAN_BASE_URL}/super/orders/{order_id}/ENTRY_LEG",
                                headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        ok = resp.ok
        log_event({"action": "SUPER_ORDER_CANCELLED" if ok else "SUPER_ORDER_CANCEL_FAILED",
                   "order_id": order_id, "http_status": resp.status_code, "response": safe_json(resp)})
        return ok
    except Exception as e:
        log_event({"action": "SUPER_ORDER_CANCEL_ERROR", "order_id": order_id, "error": str(e)})
        return False


def cancel_super_order_leg(order_id: str, leg_name: str, segment: str) -> bool:
    """ADDED 2026-09-28 for the stuck-SL watchdog (see check_stuck_sl_legs()).
    Unlike cancel_super_order() above (which always cancels ENTRY_LEG - fine
    for FLIP support, since that runs before/at entry), this cancels ONE
    specific already-live leg (TARGET_LEG or STOP_LOSS_LEG) after entry has
    already filled - per Dhan's own docs, cancelling a single target/SL leg
    this way leaves the other legs alone (only cancelling ENTRY_LEG tears
    down everything). Best-effort, same pattern as every other cancel here."""
    if not order_id or order_id == "DRY_RUN":
        return True
    if is_dry_run(segment):
        log_event({"action": "DRY_RUN_CANCEL_SUPER_ORDER_LEG", "order_id": order_id, "leg": leg_name})
        return True
    try:
        resp = requests.delete(f"{DHAN_BASE_URL}/super/orders/{order_id}/{leg_name}",
                                headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        ok = resp.ok
        log_event({"action": "SUPER_ORDER_LEG_CANCELLED" if ok else "SUPER_ORDER_LEG_CANCEL_FAILED",
                   "order_id": order_id, "leg": leg_name, "http_status": resp.status_code, "response": safe_json(resp)})
        return ok
    except Exception as e:
        log_event({"action": "SUPER_ORDER_LEG_CANCEL_ERROR", "order_id": order_id, "leg": leg_name, "error": str(e)})
        return False


def check_stuck_sl_legs():
    """ADDED 2026-09-28 (real incident: AU Small Finance Bank's SL leg
    triggered when price crossed 997.90, but Dhan places a triggered
    STOP_LOSS_LEG as a LIMIT order AT that price, not a market order (per
    Dhan's own docs: the leg carries its own "price" field and an
    orderStatus of "TRIGGERED", with remainingQuantity showing how much of
    it is still unfilled once triggered - confirmed live on the Dhan Orders
    dashboard, which showed both SL legs sitting in "Pending Orders" as
    unfilled Limit sells while LTP had already dropped below their price).
    If nobody's watching, that order can sit there indefinitely while the
    price runs further away - "SL hits but no exit".

    This polls all live Super Orders once per tick, finds any
    STOP_LOSS_LEG that is TRIGGERED with quantity still remaining, and if
    that state persists for SL_WATCHDOG_GRACE_SECONDS (giving a normal fill
    a fair chance first), force-exits: cancels that stuck SL leg AND the
    sibling TARGET_LEG (so a resting target can't also fire later against a
    position that's about to be closed), then places a plain MARKET order
    for the exact remaining quantity in the leg's own transactionType
    (already the correct closing direction - SELL to close a BUY, etc).
    This trades a small amount of extra slippage for a guaranteed exit,
    which is the whole point - a filled market order late beats an unfilled
    limit order forever."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "SL_WATCHDOG_LOOKUP_FAILED", "status": resp.status_code, "body": safe_json(resp)})
            return
        orders = safe_json(resp)
        if not isinstance(orders, list):
            return
    except Exception as e:
        log_event({"action": "SL_WATCHDOG_LOOKUP_ERROR", "error": str(e)})
        return

    orders_by_id = {str(o.get("orderId")): o for o in orders}
    now = time.time()
    seen_this_tick = set()

    for symbol, entry in list(last_entries.items()):
        legs = entry.get("legs") or []
        segment = entry.get("segment", "NSE_EQ")
        for leg in list(legs):
            # FIX 2026-10-05 (manual trades showed "Tracked SL order not found"
            # seconds after being registered): a manual trade whose SL is a
            # plain standalone order is registered with order_kind == "regular".
            # Such an order is NEVER listed under /super/orders, so looking it
            # up there always "failed" and it was mistaken for a vanished
            # Super Order. This watchdog only covers Super Order legs, so
            # plain-SL legs are skipped here. Their SL moves (cost/T1/T2...)
            # are handled separately by _handle_move_sl_to_cost()/
            # modify_sl_order() and do not use this lookup.
            if leg.get("order_kind") == "regular":
                continue
            order_id = str(leg.get("order_id"))
            order = orders_by_id.get(order_id)
            if not order:
                # FIX 2026-09-30 (real incident: AU Small Finance Bank) - this
                # used to just "continue" here with NO log at all, silently
                # giving up the instant this order left /super/orders (Dhan
                # marked it "Closed" on its own - confirmed on Dhan's own
                # Orders page showing "Exit at 1010.40" - well before the
                # REAL net position on Dhan ever actually went to zero: the
                # Positions page kept showing 357 qty open and losing money
                # for minutes afterward). Because the order was gone from the
                # only endpoint this watchdog polled, the TRIGGERED/
                # remainingQuantity logic below never got a chance to run,
                # no matter how long the real position sat open. Now: when a
                # tracked leg's order has vanished from the open list,
                # cross-check the REAL position size on Dhan directly
                # (get_position_size(), the same ground-truth lookup used
                # elsewhere in this file) instead of trusting that the order
                # disappearing means the position is gone too. If Dhan still
                # shows a real open position for this security, that's the
                # same "stuck" signal as a TRIGGERED-with-remaining-qty leg -
                # force a market exit for it, with the same grace period and
                # dedup tracking as the branch below.
                security_id = entry.get("security_id")
                if not security_id:
                    continue
                real_size = get_position_size(security_id, segment)
                if not real_size:
                    continue  # genuinely flat (or lookup failed) - nothing to do

                stuck_key = f"vanished-{order_id}"
                seen_this_tick.add(stuck_key)
                with _sl_stuck_lock:
                    first_seen = _sl_stuck_since.setdefault(stuck_key, now)
                stuck_for = now - first_seen
                if stuck_for < SL_WATCHDOG_GRACE_SECONDS:
                    log_event({"action": "SL_WATCHDOG_WATCHING_VANISHED_ORDER", "symbol": symbol,
                               "order_id": order_id, "stuck_for_seconds": round(stuck_for, 1),
                               "real_position_qty": real_size,
                               "note": "this leg's super order is no longer in Dhan's open orders list at all "
                                        "(Dhan likely marked it Closed/expired on its own) but Dhan still shows "
                                        "a real open position for this security - within grace period, waiting "
                                        "briefly before forcing a market exit"})
                    continue

                if not SL_VANISHED_FORCE_EXIT_ENABLED:
                    with _sl_stuck_lock:
                        already = _sl_stuck_since.get(f"warned-{stuck_key}")
                        _sl_stuck_since.setdefault(f"warned-{stuck_key}", now)
                    seen_this_tick.add(f"warned-{stuck_key}")
                    if not already:
                        log_event({"action": "SL_VANISHED_ORDER_NOT_CLOSED", "symbol": symbol, "order_id": order_id,
                                   "real_position_qty": real_size,
                                   "note": "tracked SL order is not in /super/orders but a real position is open - "
                                            "NOT force-closing (SL_VANISHED_FORCE_EXIT_ENABLED is off). Check its SL on Dhan."})
                        notify("[Dhan] Tracked SL order not found - position left open",
                               f"{symbol}: the bridge can't find its SL order but a {real_size} qty position is open. "
                               "Not closing it automatically. Check the SL on Dhan.")
                    continue

                close_side = "SELL" if real_size > 0 else "BUY"
                close_resp = place_order(security_id, close_side, abs(real_size), "MARKET",
                                          exchange_segment=segment,
                                          correlation_id=safe_correlation_id(symbol, f"SLVANISHED-{int(now)}"))
                close_ok = isinstance(close_resp, dict) and close_resp.get("orderId") is not None
                log_event({"action": "SL_VANISHED_ORDER_FORCE_MARKET_EXIT" if close_ok else
                                      "SL_VANISHED_ORDER_FORCE_EXIT_FAILED",
                           "symbol": symbol, "order_id": order_id, "stuck_for_seconds": round(stuck_for, 1),
                           "real_position_qty": real_size, "close_side": close_side, "close_response": close_resp,
                           "note": "this leg's super order vanished from Dhan's open orders list while a real "
                                    "position was still open on Dhan - forced a market order to guarantee the "
                                    "exit instead of leaving it silently unwatched"})
                if close_ok:
                    notify("[Dhan] Stuck position found with no live order - forced market exit",
                           f"{symbol}'s tracked SL order disappeared from Dhan's open orders (Dhan likely marked "
                           f"it Closed on its own) but a real {real_size} qty position was still open - closed it "
                           f"at market just now. Check the fill price on Dhan.")
                else:
                    notify("[Dhan] Stuck position found with no live order - market exit FAILED",
                           f"{symbol}'s tracked SL order disappeared from Dhan's open orders and a real "
                           f"{real_size} qty position is still open, but the automatic market-exit fallback ALSO "
                           "failed. Check and close it manually on Dhan right now.")
                with _sl_stuck_lock:
                    _sl_stuck_since.pop(stuck_key, None)
                leg["order_id"] = None
                entry["legs"] = [l for l in legs if l.get("order_id")]
                save_state()
                continue
            sl_leg = next((l for l in (order.get("legDetails") or []) if l.get("legName") == "STOP_LOSS_LEG"), None)
            if not sl_leg:
                continue
            sl_status = str(sl_leg.get("orderStatus", "")).upper()
            remaining = sl_leg.get("remainingQuantity") or 0
            try:
                remaining = int(float(remaining))
            except (TypeError, ValueError):
                remaining = 0

            if sl_status != "TRIGGERED" or remaining <= 0:
                # Not in the stuck state (yet) - clear any prior tracking so
                # a later, unrelated trigger on this same order starts fresh.
                with _sl_stuck_lock:
                    _sl_stuck_since.pop(order_id, None)
                continue

            seen_this_tick.add(order_id)
            with _sl_stuck_lock:
                first_seen = _sl_stuck_since.setdefault(order_id, now)
            stuck_for = now - first_seen

            if stuck_for < SL_WATCHDOG_GRACE_SECONDS:
                log_event({"action": "SL_WATCHDOG_WATCHING", "symbol": symbol, "order_id": order_id,
                           "stuck_for_seconds": round(stuck_for, 1), "remaining_qty": remaining,
                           "sl_leg_price": sl_leg.get("price"),
                           "note": "SL leg triggered but not yet filled - within grace period, waiting to see "
                                    "if it fills on its own before forcing a market exit"})
                continue

            close_side = sl_leg.get("transactionType")
            close_resp = place_order(order.get("securityId"), close_side, remaining, "MARKET",
                                      exchange_segment=segment,
                                      correlation_id=safe_correlation_id(symbol, f"SLSTUCK-{int(now)}"))
            close_ok = isinstance(close_resp, dict) and close_resp.get("orderId") is not None
            cancel_super_order_leg(order_id, "STOP_LOSS_LEG", segment)
            cancel_super_order_leg(order_id, "TARGET_LEG", segment)

            log_event({"action": "SL_STUCK_FORCE_MARKET_EXIT" if close_ok else "SL_STUCK_FORCE_EXIT_FAILED",
                       "symbol": symbol, "order_id": order_id, "stuck_for_seconds": round(stuck_for, 1),
                       "remaining_qty": remaining, "sl_leg_price": sl_leg.get("price"), "close_side": close_side,
                       "close_response": close_resp,
                       "note": "SL leg was TRIGGERED but sat unfilled (limit at the SL price, price moved past "
                                "it) for over the grace period - forced a market order to guarantee the exit, "
                                "then cancelled the stuck SL and its sibling target leg"})
            if close_ok:
                notify("[Dhan] SL was stuck - forced market exit",
                       f"{symbol}'s SL triggered at {sl_leg.get('price')} but didn't fill (price moved past it "
                       f"before any buyer/seller was there). Closed {remaining} qty at market after "
                       f"{int(stuck_for)}s instead of leaving it unprotected. Check the fill price on Dhan.")
            else:
                notify("[Dhan] SL stuck AND market exit FAILED",
                       f"{symbol}'s SL triggered at {sl_leg.get('price')} and didn't fill, and the automatic "
                       f"market-exit fallback ALSO failed. This position is still open and unprotected - "
                       f"check and close it manually on Dhan right now.")

            with _sl_stuck_lock:
                _sl_stuck_since.pop(order_id, None)
            leg["order_id"] = None  # this leg is closed one way or another now - stop tracking it
            entry["legs"] = [l for l in legs if l.get("order_id")]
            save_state()

    # Prune tracking for any order_id that's no longer present at all this
    # tick (e.g. the whole symbol's entry vanished from last_entries) so
    # this dict can't grow forever.
    with _sl_stuck_lock:
        for stale_id in [oid for oid in _sl_stuck_since if oid not in seen_this_tick and oid not in orders_by_id]:
            _sl_stuck_since.pop(stale_id, None)


def _check_cascade_targets():
    """ADDED 2026-09-30 (Khagen's request, mirrors the identical fix just
    made on the Delta/crypto bridge - real incident that same night: an
    ETHUSD.P short ran price straight through T1 with no MOVE_SL_TO_COST
    alert ever arriving from TradingView). This is a bridge-side BACKUP for
    the Pine script's own T1/T2/T3/T4/T5 cascade alerts (MOVE_SL_TO_COST/
    T1/T2/T3/T4). TradingView's own alert is still the PRIMARY, faster path
    - it fires the instant a bar sees the touch. This is the slower
    fallback that catches it independently within SL_WATCHDOG_POLL_SECONDS
    even if that alert never leaves TradingView at all - it polls Dhan's
    OWN live LTP directly via get_ltp(), so it has NO dependency on
    TradingView whatsoever.

    Cascade mapping (mirrors the Pine script's own stages exactly):
      stage 1: price touches T1 -> SL moves to entry (cost/breakeven)
      stage 2: price touches T2 -> SL moves to T1
      stage 3: price touches T3 -> SL moves to T2
      stage 4: price touches T4 -> SL moves to T3
      stage 5: price touches T5 -> SL moves to T4
    (T6, where sent, is only the Super Order's own take-profit exit level -
    no further SL move happens beyond stage 5, same as the Pine script.)

    Safe to run alongside the TradingView-triggered path because
    _handle_move_sl_to_cost() itself guards on last_entries[symbol]
    ["cascade_stage"]: whichever path (TradingView alert or this watchdog)
    notices a level crossing FIRST wins and actually moves the SL; the
    other becomes a no-op instead of a duplicate/backwards edit."""
    # ONE price request per tick for every tracked trade (see get_ltp_batch()).
    _need = [(e.get("segment") or "NSE_EQ", e.get("security_id")) for e in list(last_entries.values())
             if e.get("targets") and e.get("side") in ("BUY", "SELL") and e.get("entry_price") is not None
             and e.get("security_id") and e.get("cascade_stage", 0) < 5]
    if not _need:
        return
    _ltp_map = get_ltp_batch(_need)
    for symbol, entry in list(last_entries.items()):
        targets = entry.get("targets")
        side = entry.get("side")  # "BUY"/"SELL", as stored by open_position()
        entry_price = entry.get("entry_price")
        security_id = entry.get("security_id")
        segment = entry.get("segment")
        if not targets or side not in ("BUY", "SELL") or entry_price is None or not security_id:
            continue
        already_at = entry.get("cascade_stage", 0)
        if already_at >= 5:
            continue  # already walked all the way out to T4 - nothing left to watch

        ltp = _ltp_map.get((segment or "NSE_EQ", str(security_id)))
        if ltp is None:
            continue

        reached_stage = already_at
        for i in range(already_at, 5):  # i = 0..4 -> T-level index (T1..T5), stage = i+1
            level = targets[i] if i < len(targets) else None
            if level is None:
                break
            touched = (side == "BUY" and ltp >= level) or (side == "SELL" and ltp <= level)
            if not touched:
                break
            reached_stage = i + 1

        if reached_stage <= already_at:
            continue

        new_sl = entry_price if reached_stage == 1 else targets[reached_stage - 2]
        if new_sl is None:
            continue
        log_event({"action": "WATCHDOG_CASCADE_STAGE_DETECTED", "symbol": symbol, "stage": reached_stage,
                   "ltp": ltp, "new_sl_price": new_sl, "already_at_stage": already_at,
                   "note": "bridge's own price poll found this T-level touched - TradingView's own "
                            "MOVE_SL_TO_COST/T1/T2/T3/T4 alert may simply not have arrived yet, or at all. "
                            "Applying the SL move directly now as a backup."})
        _handle_move_sl_to_cost(symbol, {"price": new_sl}, stage=reached_stage, source="watchdog")


def _sl_watchdog_loop():
    log_event({"action": "SL_WATCHDOG_STARTED", "poll_seconds": SL_WATCHDOG_POLL_SECONDS,
               "grace_seconds": SL_WATCHDOG_GRACE_SECONDS})
    tick = 0
    while True:
        time.sleep(SL_WATCHDOG_POLL_SECONDS)
        tick += 1
        try:
            if tick % 24 == 0 and not _in_log_quiet_hours():  # heartbeat roughly every 2 minutes, skipped overnight
                log_event({"action": "SL_WATCHDOG_ALIVE", "tick": tick})
            check_stuck_sl_legs()
        except Exception as e:
            log.error(f"Error in stuck-SL watchdog: {e}")
        try:
            _check_cascade_targets()
        except Exception as e:
            log_event({"action": "CASCADE_WATCHDOG_ERROR", "error": str(e)})


threading.Thread(target=_sl_watchdog_loop, daemon=True, name="stuck-sl-watchdog").start()


def place_super_order(security_id: str, transaction_type: str, qty: int, order_type: str,
                       price, target_price: float, sl_price: float,
                       exchange_segment: str = "NSE_EQ", correlation_id: str = ""):
    """ADDED 2026-09-25 (real incidents: multiple positions this session
    filled with NO stop-loss - BHARTIARTL, a GOLDM1 SELL whose backwards SL
    got instantly executed as an accidental exit - all stemming from the
    same structural gap: entry and SL were always two SEPARATE API calls,
    with a real window between "order filled" and "SL actually attached"
    where a filled position sits completely unprotected, or where the
    second call can go wrong in ways the first one never sees).

    Dhan's Super Order places the entry, target and stop-loss as ONE atomic
    order - Dhan holds all three legs server-side from the moment this call
    succeeds, and activates the target/SL legs itself the instant the entry
    leg fills. There is no window, no second API call, no poller, and
    nothing this bridge could still get wrong between fill and protection -
    the SL exists on Dhan's side before this function even returns.

    trailingJump is sent as 0 (no trailing) - the script already handles
    moving the SL to breakeven itself via MOVE_SL_TO_COST, which now calls
    modify_super_order_leg() below instead of managing a separate order."""
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "correlationId": correlation_id[:30],
        "transactionType": transaction_type,
        "exchangeSegment": exchange_segment,
        "productType": PRODUCT_TYPE,
        "orderType": order_type,
        "securityId": security_id,
        "quantity": int(qty),
        "price": price if order_type == "LIMIT" else 0,
        "targetPrice": target_price,
        "stopLossPrice": sl_price,
        "trailingJump": 0,
    }
    if is_dry_run(exchange_segment):
        log_event({"action": "DRY_RUN_SUPER_ORDER", "payload": payload})
        return {"orderId": "DRY_RUN", "orderStatus": "SIMULATED"}

    # REMOVED 2026-10-01 (Khagen's decision, real incident: BHARTIARTL
    # blocked with available_balance showing 3.6x the required margin -
    # root cause traced to Dhan segregating margin by exchange segment
    # (equity vs commodity), with stock and commodity traded simultaneously
    # from the same account, so this bridge-side pre-check's numbers don't
    # reliably reflect what's really free for a given segment. Margin
    # sufficiency is Dhan's own job via its RMS at real order placement -
    # it will reject the order itself (same as it always did before the
    # 2026-09-30 GOLDM incident that originally added this pre-check) if
    # funds are genuinely short, with a clear rejection reason in the
    # response below. No local pre-check, no local block - qty is still
    # independently verified above before this point either way.
    def _submit(q):
        p = dict(payload)
        p["quantity"] = int(q)
        r = requests.post(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), json=p, timeout=15, proxies=DHAN_PROXIES)
        b = safe_json(r)
        log_event({"action": "PLACE_SUPER_ORDER", "payload": p, "http_status": r.status_code, "response": b})
        return r, b

    resp, body = _submit(qty)
    ok = resp.ok
    if not ok:
        log.error(f"Dhan Super Order FAILED: {resp.status_code} {body}")
        if _is_auth_failure(resp.status_code, body):
            _note_token_check(False, f"super order placement failed with HTTP {resp.status_code}: {body}")
        if _is_ip_whitelist_failure(body):
            _log_ip_mismatch_diagnostic(f"place_super_order {payload.get('transactionType')} {payload.get('securityId')}")
        # ADDED 2026-09-30 (real incident: NATURALGASM 1000 qty rejected -
        # "exceeds the maximum quantity that can be placed", MCX's per-order
        # freeze limit). One retry at half quantity - covers the common case
        # where the freeze limit sits between half and full qty. This does
        # NOT place the other half automatically (that would mean two
        # different order_ids for one trade, which the rest of this file's
        # SL-cascade tracking isn't built to handle) - it alerts you to place
        # the remainder manually instead of silently under-filling.
        body_text = str(body).lower()
        if "exceeds the maximum quantity" in body_text or "freeze" in body_text:
            half_qty = int(qty) // 2
            if half_qty >= 1:
                log_event({"action": "FREEZE_LIMIT_RETRY_HALF_QTY", "security_id": security_id,
                           "original_qty": qty, "retry_qty": half_qty})
                resp2, body2 = _submit(half_qty)
                if resp2.ok:
                    notify("[Dhan] Order size hit MCX freeze limit - placed HALF only",
                           f"{transaction_type} {security_id}: {qty} qty was rejected (exceeds max quantity per order). "
                           f"Placed {half_qty} qty instead. The remaining {qty - half_qty} qty was NOT placed - "
                           "place it manually as a separate order if you still want full size.")
                    return body2
                else:
                    log.error(f"Dhan Super Order retry at half qty ALSO failed: {resp2.status_code} {body2}")
    return body


def modify_super_order_leg(order_id: str, leg_name: str, exchange_segment: str = "NSE_EQ", **fields):
    """Modifies one leg of an already-placed Super Order (e.g. the
    STOP_LOSS_LEG's stopLossPrice, for shifting SL to breakeven once T1 is
    hit). Replaces the old modify_sl_order(), which modified a standalone
    STOP_LOSS_MARKET order - there's no separate order anymore, just one
    Super Order with legs."""
    payload = {"dhanClientId": DHAN_CLIENT_ID, "orderId": order_id, "legName": leg_name, **fields}
    if is_dry_run(exchange_segment):
        log_event({"action": "DRY_RUN_MODIFY_SUPER_ORDER", "payload": payload})
        return True, {"orderId": order_id, "orderStatus": "SIMULATED_MODIFY"}

    resp = requests.put(f"{DHAN_BASE_URL}/super/orders/{order_id}", headers=dhan_headers(), json=payload, timeout=15, proxies=DHAN_PROXIES)
    ok = resp.ok
    body = safe_json(resp)
    log_event({"action": "MODIFY_SUPER_ORDER", "payload": payload, "http_status": resp.status_code, "response": body})
    if not ok:
        log.error(f"Dhan Super Order modify FAILED: {resp.status_code} {body}")
        if _is_auth_failure(resp.status_code, body):
            _note_token_check(False, f"super order modify failed with HTTP {resp.status_code}: {body}")
        if _is_ip_whitelist_failure(body):
            _log_ip_mismatch_diagnostic(f"modify_super_order_leg {order_id}")
    return ok, body


def get_position_size(security_id: str, exchange_segment: str):
    """Returns the CURRENT net quantity Dhan shows open for this exact
    security (positive = long, negative = short, 0 = flat), or None if the
    lookup itself failed. Always asked fresh from Dhan - never assumed -
    so the anti-stacking check below reflects what's really open, not what
    this process merely remembers placing."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log.warning(f"Position lookup failed: {resp.status_code} {safe_json(resp)}")
            return None
        positions = safe_json(resp)
        if not isinstance(positions, list):
            return 0
        for p in positions:
            if str(p.get("securityId")) == str(security_id) and p.get("exchangeSegment") == exchange_segment:
                try:
                    return int(float(p.get("netQty", 0) or 0))
                except (TypeError, ValueError):
                    return 0
        return 0  # not in the list at all = flat
    except Exception as e:
        log.warning(f"Position lookup error: {e}")
        return None


def find_live_plain_sl_order(security_id: str, segment: str):
    """ADDED 2026-10-01 (Khagen's manual re-entry workflow - real incidents:
    AUBANK, SILVERMIC1!, CHOLAFIN, ABB all hit a cascade T-level after Khagen
    manually re-entered the symbol on Dhan following an SL hit, and the
    bridge replied "no order on file to shift - check manually" instead of
    moving the SL.

    reconcile_symbol_from_dhan() below only ever looked at GET /super/orders
    for a resting STOP_LOSS_LEG - exactly right for a position THIS BRIDGE
    opened (always placed as one Super Order), but it has nothing to find
    there for a position Khagen re-enters by hand, where he places a plain
    standalone STOP_LOSS_MARKET/STOP_LOSS order instead of a full Super
    Order. This is the missing second place to look: Dhan's plain GET
    /orders book, for any live (non-terminal) stop-loss order on this exact
    security+segment. Returns that raw order dict, or None if none found or
    the lookup itself failed. Read-only - places nothing, cancels nothing."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "RECONCILE_PLAIN_ORDERS_LOOKUP_FAILED", "security_id": security_id,
                       "status": resp.status_code, "body": safe_json(resp)})
            return None
        orders = safe_json(resp)
        if not isinstance(orders, list):
            log_event({"action": "RECONCILE_PLAIN_ORDERS_UNEXPECTED_BODY", "security_id": security_id, "body": orders})
            return None
    except Exception as e:
        log_event({"action": "RECONCILE_PLAIN_ORDERS_LOOKUP_ERROR", "security_id": security_id, "error": str(e)})
        return None

    terminal = ("CANCELLED", "REJECTED", "TRADED", "CLOSED", "EXPIRED")
    same_security = []  # DIAGNOSTIC (added 2026-10-01, see below) - every order on this exact security+segment, regardless of whether it matched
    for o in orders:
        if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
            continue
        same_security.append(o)
        if str(o.get("orderType", "")).upper() not in ("STOP_LOSS", "STOP_LOSS_MARKET"):
            continue
        if str(o.get("orderStatus", "")).upper() in terminal:
            continue
        return o

    # DIAGNOSTIC (added 2026-10-01, Khagen's manual-trade-detector test on
    # NATURALGASM: a live-looking "Buy Stop" order on Dhan's own app was not
    # being found here, with no visibility into why - was it a different
    # orderType string than "STOP_LOSS"/"STOP_LOSS_MARKET", an orderStatus
    # already counted as terminal, or no order at all for this security on
    # Dhan's /orders list). Read-only, changes no matching behavior - only
    # logs what WAS seen for this exact security+segment so the real reason
    # is visible in the logs on the very next poll cycle instead of just
    # "not found".
    if same_security:
        log_event({"action": "MANUAL_DETECT_SL_LOOKUP_DIAGNOSTIC", "security_id": security_id, "segment": segment,
                   "orders_seen": [{"orderId": o.get("orderId"), "orderType": o.get("orderType"),
                                     "orderStatus": o.get("orderStatus"), "triggerPrice": o.get("triggerPrice"),
                                     "productType": o.get("productType")} for o in same_security],
                   "note": "no order matched STOP_LOSS/STOP_LOSS_MARKET + non-terminal status - these are "
                            "ALL the orders Dhan's /orders list has for this exact security+segment, whatever "
                            "their type/status actually is, to see why the match failed."})
    return None


def find_live_super_order_for_position(security_id: str, segment: str):
    """ADDED 2026-10-01 (manual-trade-detector test on NATURALGASM: Khagen's
    manual SL turned out to be a Dhan SUPER ORDER - entry+target+SL placed as
    one bracket from the chart's quick-order ticket, confirmed via Dhan's own
    Super Orders tab - "Stop loss: 292.40" sitting as a leg inside it, NOT a
    standalone order. find_live_plain_sl_order() only ever checks the plain
    GET /orders book, which a Super Order's SL leg never appears in - it only
    shows up under GET /super/orders (exactly what reconcile_symbol_from_dhan()
    below already checks FIRST, for the redeploy-recovery case - this detector
    skipped that same check, which was the actual bug).

    Mirrors reconcile_symbol_from_dhan()'s own Super Order matching exactly
    (same endpoint, same securityId+exchangeSegment match, same STOP_LOSS_LEG
    lookup, same non-terminal status check) so both code paths agree on what
    counts as 'a live Super Order SL'. Returns (order, sl_leg) - the raw
    Super Order dict and its STOP_LOSS_LEG dict - or (None, None) if nothing
    live was found or the lookup itself failed. Read-only."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "MANUAL_DETECT_SUPER_ORDERS_LOOKUP_FAILED", "security_id": security_id,
                       "status": resp.status_code, "body": safe_json(resp)})
            return None, None
        orders = safe_json(resp)
        if not isinstance(orders, list):
            return None, None
    except Exception as e:
        log_event({"action": "MANUAL_DETECT_SUPER_ORDERS_LOOKUP_ERROR", "security_id": security_id, "error": str(e)})
        return None, None

    terminal_leg = ("CANCELLED", "REJECTED", "TRADED", "CLOSED")
    for o in orders:
        if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
            continue
        if str(o.get("orderStatus", "")).upper() in ("CANCELLED", "REJECTED"):
            continue
        leg_details = o.get("legDetails") or []
        sl_leg = next((l for l in leg_details if l.get("legName") == "STOP_LOSS_LEG"), None)
        if sl_leg is None:
            continue
        sl_status = str(sl_leg.get("orderStatus", sl_leg.get("legStatus", ""))).upper()
        if sl_status in terminal_leg:
            continue
        return o, sl_leg
    return None, None


def reconcile_symbol_from_dhan(symbol: str, security_id: str, segment: str):
    """ADDED 2026-09-27 - mirrors a real incident and fix already made on the
    Delta side: a redeploy wipes trade_state.json (see the TRADE_STATE_FILE
    note above), and when MOVE_SL_TO_COST fires afterwards for a symbol
    still genuinely open on the broker, last_entries has no record of it -
    the SL never moves and Khagen has to do it by hand.

    Unlike delta-bridge, this bridge trades an unbounded set of NSE/MCX
    symbols (any stock or commodity contract the script fires on), not a
    fixed list of 3 - so there's no small checklist to proactively poll at
    startup the way KNOWN_CRYPTO_SYMBOLS works over there. Instead this
    reconciles ON DEMAND: called only from inside the MOVE_SL_TO_COST
    handler, at the exact moment a gap is discovered, using the same
    security_id/segment already resolved for that alert (the same forward
    lookup already proven correct for placing the original entry - no
    fragile reverse security-id-to-symbol mapping needed).

    Ground truth for "is there really a position open" is
    get_position_size(), already used everywhere else. To find the
    resting Super Order leg(s) actually protecting it, this calls
    GET /super/orders - confirmed via Dhan's own API docs to list ALL
    super orders including ones whose entry leg has already filled
    (unlike Delta's GET /v2/orders, which drops a filled order entirely -
    Dhan's own docs state a super order stays listed, moving through
    orderStatus values like TRADED/PART_TRADED, until its target or SL leg
    also completes) - and matches by securityId + a STOP_LOSS_LEG that
    hasn't itself been closed out yet. Returns the rebuilt entry dict (also
    already saved into last_entries) on success, or None if there was
    nothing to recover or the lookup failed."""
    current_qty = get_position_size(security_id, segment)
    if current_qty is None:
        # FIX 2026-09-28 (real incident: a COALINDIA MOVE_SL_TO_COST failed
        # with "no order on file" and there was NO reconcile log line at all
        # in between - this early return had no log_event, so it was
        # impossible to tell from the logs whether the position was already
        # flat or whether get_position_size() itself failed/errored. Now
        # distinguished and logged either way.
        log_event({"action": "RECONCILE_POSITION_LOOKUP_FAILED", "symbol": symbol,
                   "note": "get_position_size() itself failed/errored - could not even determine whether "
                            "a position exists. Check manually, this is NOT the same as confirmed-flat."})
        return None
    if current_qty == 0:
        log_event({"action": "RECONCILE_SKIPPED_ALREADY_FLAT", "symbol": symbol,
                   "note": "Dhan confirms NO open position for this symbol - it was already closed some "
                            "other way (e.g. SL/target already hit, or closed manually) - nothing to "
                            "reconcile or protect, this is the expected/safe case."})
        return None

    try:
        resp = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "RECONCILE_SUPER_ORDERS_LOOKUP_FAILED", "symbol": symbol,
                       "status": resp.status_code, "body": safe_json(resp)})
            return None
        orders = safe_json(resp)
        if not isinstance(orders, list):
            log_event({"action": "RECONCILE_SUPER_ORDERS_UNEXPECTED_BODY", "symbol": symbol, "body": orders})
            return None
    except Exception as e:
        log_event({"action": "RECONCILE_SUPER_ORDERS_LOOKUP_ERROR", "symbol": symbol, "error": str(e)})
        return None

    legs = []
    entry_price = None
    for o in orders:
        if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
            continue
        if str(o.get("orderStatus", "")).upper() in ("CANCELLED", "REJECTED"):
            continue
        leg_details = o.get("legDetails") or []
        sl_leg = next((l for l in leg_details if l.get("legName") == "STOP_LOSS_LEG"), None)
        if sl_leg is None:
            continue
        sl_status = str(sl_leg.get("orderStatus", sl_leg.get("legStatus", ""))).upper()
        if sl_status in ("CANCELLED", "REJECTED", "TRADED", "CLOSED"):
            continue  # this leg's own SL is no longer live - nothing to move
        legs.append({"order_id": o.get("orderId"), "qty": o.get("quantity") or o.get("remainingQuantity"),
                     "target_price": None, "leg": "RECONCILED"})
        entry_price = entry_price or o.get("averageTradedPrice") or o.get("price")

    if not legs:
        # ADDED 2026-10-01 (Khagen's manual re-entry workflow): before giving
        # up, check whether this is a manually re-entered position protected
        # by a plain standalone stop-loss order rather than a Super Order -
        # see find_live_plain_sl_order()'s docstring for the real incidents
        # this covers (AUBANK/SILVERMIC1!/CHOLAFIN/ABB).
        plain_order = find_live_plain_sl_order(security_id, segment)
        if plain_order is not None:
            qty_val = plain_order.get("quantity") or plain_order.get("remainingQuantity") or abs(current_qty)
            legs.append({"order_id": plain_order.get("orderId"), "qty": qty_val, "target_price": None,
                         "leg": "MANUAL_SL", "order_kind": "regular",
                         "order_type": plain_order.get("orderType", "STOP_LOSS_MARKET")})
            entry_price = entry_price or plain_order.get("averageTradedPrice")
            log_event({"action": "RECONCILED_PLAIN_SL_ORDER_FOUND", "symbol": symbol, "security_id": security_id,
                       "order_id": plain_order.get("orderId"),
                       "note": "no Super Order found, but a live plain stop-loss order exists on this "
                                "security - almost certainly Khagen's own manually-placed SL after a manual "
                                "re-entry. Tracking it as a MANUAL_SL leg so cascade alerts can shift its "
                                "trigger price via modify_sl_order() instead of modify_super_order_leg()."})

    if not legs:
        log_event({"action": "RECONCILE_NO_LIVE_SL_LEG_FOUND", "symbol": symbol, "security_id": security_id,
                   "segment": segment, "qty": current_qty,
                   "note": "a real position is open on Dhan for this symbol but no resting super order AND "
                            "no resting plain stop-loss order was found - it may genuinely have no SL "
                            "attached (check manually right now). MOVE_SL_TO_COST cannot proceed for it."})
        return None

    side = "BUY" if current_qty > 0 else "SELL"
    entry = {"side": side, "qty": abs(current_qty), "legs": legs, "security_id": security_id,
             "segment": segment, "entry_price": entry_price, "sl_price": None}
    last_entries[symbol] = entry
    save_state()
    log_event({"action": "RECONCILED_FROM_DHAN", "symbol": symbol, "legs": len(legs),
               "note": "no local trade_state.json record for this symbol (likely a redeploy wiped it) - "
                        "rebuilt its tracked leg(s) directly from Dhan's own super orders just in time to "
                        "carry out this MOVE_SL_TO_COST"})
    return entry


# =============================================================================
# MANUAL TRADE DETECTOR - ADDED 2026-10-01, kept deliberately SEPARATE from
# everything above (Khagen's request). Covers a different gap than
# reconcile_symbol_from_dhan(): that function rebuilds tracking for a
# position THIS BRIDGE (or Khagen, re-entering after a bridge-tracked SL
# hit) already has some history with, and only runs reactively, from inside
# a cascade alert handler. This covers a position Khagen opened AND
# protected with his own SL entirely by hand, that the bridge has NEVER
# heard of from any alert at all - nothing would ever call
# reconcile_symbol_from_dhan() for it, because no alert referencing that
# symbol is ever going to arrive.
#
# Read-only against Dhan except for the one last_entries[symbol] = ... write
# when a genuine new manual trade is found - this NEVER places, modifies, or
# cancels an order on Dhan. Once a trade is registered here, the EXISTING
# cascade watchdog (_check_cascade_targets(), already polling every
# SL_WATCHDOG_POLL_SECONDS inside _sl_watchdog_loop above) takes over the
# actual SL-shifting exactly as it already does for alert-driven trades -
# nothing about that watchdog or about _handle_move_sl_to_cost() changes.
# =============================================================================

# Tracks which security_ids have already triggered a "no SL found"/"could
# not compute R" notification, so the poller (every MANUAL_TRADE_POLL_SECONDS)
# doesn't re-notify Khagen's phone every cycle for the same still-unresolved
# position - only once, until that position goes flat (cleared then) or
# actually gets a usable SL (also cleared then, by simply registering).
_manual_trade_warned = set()


def _compute_r_multiple_targets(entry_price: float, sl_price: float, side: str):
    """Pure math, no API calls: R = |entry - sl|, returns the 6 ascending
    R-multiple price levels (1R..6R) on the correct side of entry for this
    trade's direction. Used both for the cascade's targets[0:5] (T1-T5, fed
    to the exact same _check_cascade_targets()/_handle_move_sl_to_cost() used
    by alert-driven trades) and for T6, which here is informational only -
    unlike an alert-driven Super Order, a manually-placed trade has no
    bridge-placed target/limit order actually resting at T6, only the SL
    this function is managing, so T6 is reported in the notification but
    never enforced as a real exit price."""
    r = abs(entry_price - sl_price)
    sign = 1 if side == "BUY" else -1
    return [round(entry_price + sign * r * n, 4) for n in range(1, 7)]


def collect_live_sl_orders(security_id: str, segment: str):
    """ADDED 2026-10-08 (Khagen: bought 300, bridge tracked it, later added extra qty with its own SL
    and the bridge never saw that second SL order). Returns EVERY live stop-loss order for this
    security as a list of dicts {order_id, kind ("super"/"regular"), qty, trigger, order_type, target_price},
    or None if either Dhan lookup failed (so callers never mistake a failed lookup for "no orders").
    Read-only."""
    out = []
    terminal = ("CANCELLED", "REJECTED", "TRADED", "CLOSED", "EXPIRED")
    try:
        r1 = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        r2 = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not (r1.ok and r2.ok):
            return None
        supers, plains = safe_json(r1), safe_json(r2)
        if not isinstance(supers, list) or not isinstance(plains, list):
            return None
    except Exception as e:
        log_event({"action": "ADDQTY_LOOKUP_ERROR", "security_id": security_id, "error": str(e)})
        return None
    for o in supers:
        if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
            continue
        if str(o.get("orderStatus", "")).upper() in ("CANCELLED", "REJECTED"):
            continue
        sl_leg = next((l for l in (o.get("legDetails") or []) if l.get("legName") == "STOP_LOSS_LEG"), None)
        if sl_leg is None:
            continue
        if str(sl_leg.get("orderStatus", sl_leg.get("legStatus", ""))).upper() in terminal:
            continue
        trig = (o.get("stopLossPrice") or sl_leg.get("stopLossPrice") or sl_leg.get("price") or sl_leg.get("triggerPrice"))
        try:
            trig = float(trig)
        except (TypeError, ValueError):
            trig = None
        out.append({"order_id": o.get("orderId"), "kind": "super",
                    "qty": o.get("quantity") or o.get("remainingQuantity"),
                    "trigger": trig, "order_type": None, "target_price": o.get("targetPrice")})
    for o in plains:
        if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
            continue
        if str(o.get("orderType", "")).upper() not in ("STOP_LOSS", "STOP_LOSS_MARKET"):
            continue
        if str(o.get("orderStatus", "")).upper() in terminal:
            continue
        try:
            trig = float(o.get("triggerPrice"))
        except (TypeError, ValueError):
            trig = None
        out.append({"order_id": o.get("orderId"), "kind": "regular",
                    "qty": o.get("quantity") or o.get("remainingQuantity"),
                    "trigger": trig, "order_type": o.get("orderType", "STOP_LOSS_MARKET"), "target_price": None})
    return out


def _sync_manual_trade_addons(symbol: str, entry: dict, net_qty: int, avg_price=None):
    """ADDED 2026-10-08. For an already-tracked MANUAL trade: pick up extra quantity the user added later
    with its own SL order. New live SL orders on this security that are not yet in entry["legs"] become
    new legs (so every later cascade move shifts them too); legs whose order is gone are dropped; leg
    qty is refreshed from Dhan (so a stale qty is never written back by a modify); entry["qty"] follows
    the live position. If the cascade already moved the SL (cascade_stage > 0) and the new order's trigger
    is WORSE than the current cascade SL, it is moved up/down to that SL so the added quantity is
    protected the same way. Entry price / R / targets are left exactly as registered. Never touches
    signal logic."""
    security_id, segment = entry.get("security_id"), entry.get("segment", "NSE_EQ")
    live = collect_live_sl_orders(security_id, segment)
    if live is None:
        return  # lookup failed - change nothing this poll
    side = entry.get("side")
    legs = entry.get("legs") or []
    live_by_id = {str(o["order_id"]): o for o in live}
    changed = False

    kept = []
    for leg in legs:
        o = live_by_id.get(str(leg.get("order_id")))
        if o is None:
            if live:   # position still has other live SL orders -> this one was cancelled/replaced/filled
                log_event({"action": "ADDQTY_LEG_DROPPED", "symbol": symbol, "order_id": leg.get("order_id")})
                changed = True
                continue
            kept.append(leg)   # no live SL at all: keep as is (lookup oddity) - old behaviour
            continue
        if o["qty"] and leg.get("qty") != o["qty"]:
            log_event({"action": "ADDQTY_LEG_QTY_REFRESHED", "symbol": symbol, "order_id": leg.get("order_id"),
                       "old_qty": leg.get("qty"), "new_qty": o["qty"]})
            leg["qty"] = o["qty"]
            changed = True
        kept.append(leg)
    legs = kept

    known = {str(l.get("order_id")) for l in legs}
    cur_sl = entry.get("sl_price")
    stage = entry.get("cascade_stage", 0)
    added = []
    for o in live:
        if str(o["order_id"]) in known:
            continue
        leg = {"order_id": o["order_id"], "qty": o["qty"] or abs(net_qty), "target_price": o["target_price"],
               "leg": "MANUAL_SUPER_SL" if o["kind"] == "super" else "MANUAL_SL"}
        if o["kind"] == "regular":
            leg["order_kind"] = "regular"
            leg["order_type"] = o["order_type"] or "STOP_LOSS_MARKET"
        aligned = ""
        if stage > 0 and cur_sl is not None and o["trigger"] is not None:
            worse = (o["trigger"] < cur_sl) if side == "BUY" else (o["trigger"] > cur_sl)
            if worse:
                new_sl = _tick_round(cur_sl, segment)
                if o["kind"] == "regular":
                    resp = modify_sl_order(o["order_id"], security_id, leg["qty"], new_sl,
                                            exchange_segment=segment, order_type=leg["order_type"])
                    ok = isinstance(resp, dict) and resp.get("orderId") is not None
                else:
                    ok, _ = modify_super_order_leg(o["order_id"], "STOP_LOSS_LEG", exchange_segment=segment,
                                                    stopLossPrice=new_sl, trailingJump=0)
                aligned = (f" Its SL was {o['trigger']} - moved to the current trail {new_sl}."
                           if ok else f" Its SL is {o['trigger']}; could NOT move it to {new_sl} - move it yourself.")
        legs.append(leg)
        added.append((leg["qty"], o["trigger"], aligned))
        changed = True

    # AVERAGING: Dhan's average price of the position moves when quantity is added at a new price.
    # While no cascade stage has fired yet (stage 0), re-base entry price, R and T1-T5 on the new
    # average, using the widest live SL trigger (the safest R). Once the SL has started moving
    # (stage > 0) the levels are left alone so the trail never jumps backwards.
    if stage == 0:
        try:
            avg = float(avg_price) if avg_price else entry.get("entry_price")
        except (TypeError, ValueError):
            avg = entry.get("entry_price")
        trigs = [o["trigger"] for o in live if o["trigger"] is not None]
        sl_now = (min(trigs) if side == "BUY" else max(trigs)) if trigs else None
        # re-base when the average price changed OR the user moved the SL (tighter or wider) before T1
        if avg and sl_now and (abs(avg - entry.get("entry_price", avg)) >= 0.01
                               or abs(sl_now - (entry.get("sl_price") or sl_now)) >= 0.01):
            sl_use = sl_now
            if (side == "BUY" and sl_use < avg) or (side == "SELL" and sl_use > avg):
                tg = _compute_r_multiple_targets(avg, sl_use, side)
                old_entry = entry.get("entry_price")
                entry["entry_price"], entry["sl_price"], entry["targets"] = avg, sl_use, tg[:5]
                changed = True
                log_event({"action": "ADDQTY_REBASED_ON_AVERAGE", "symbol": symbol, "old_entry": old_entry,
                           "new_entry": avg, "sl": sl_use, "r": abs(avg - sl_use)})
                notify("[Dhan] Entry/SL changed - targets recalculated",
                       f"{symbol}: entry {avg} (was {old_entry}), SL {sl_use}, R={abs(avg - sl_use):.2f}.\n"
                       f"T1 {tg[0]}  T2 {tg[1]}  T3 {tg[2]}  T4 {tg[3]}  T5 {tg[4]}. SL moves to the new "
                       "average (cost) at T1.")

    # ADDED 15 Nov (Khagen): quantity added to a manual position WITHOUT an SL of its own gets the SAME SL as the position (its current SL level) after about a
    # minute - time for you to place your own first. It is tracked as one more leg, so every later cascade move shifts it too.
    try:
        _cov = sum(int(float(l.get("qty") or 0)) for l in legs)
        _unc = abs(net_qty) - _cov
        _now = time.time()
        if _unc > 0 and _now >= float(entry.get("addsl_hold_until") or 0):
            _first = entry.get("uncovered_since")
            if _first is None:
                entry["uncovered_since"] = _now
            elif _now - _first >= ADD_SL_WAIT_SECONDS:
                _fresh = get_position_size(security_id, segment)
                _trig = entry.get("sl_price")
                if _fresh is not None and abs(_fresh) - _cov > 0 and _trig:
                    _unc = abs(_fresh) - _cov
                    _ltp = get_ltp(security_id, segment)
                    _trig = _tick_round(_trig, segment)
                    _close = "SELL" if side == "BUY" else "BUY"
                    if _ltp is None:
                        pass                                            # try again next poll
                    elif (side == "BUY" and _trig >= _ltp) or (side == "SELL" and _trig <= _ltp):
                        entry["addsl_hold_until"] = _now + 3600
                        notify("[Dhan] SL for added quantity NOT placed - " + symbol, f"{symbol}: you added {_unc}, but the price ({_ltp}) is already beyond the position's SL ({_trig}). Place an SL for the extra {_unc} yourself.", priority="high")
                    else:
                        _r = place_order(security_id, _close, _unc, "STOP_LOSS_MARKET", exchange_segment=segment, trigger_price=_trig,
                                         correlation_id=safe_correlation_id(symbol, f"ADDSL-{int(_now)}"))
                        _ok = isinstance(_r, dict) and _r.get("orderId") is not None
                        entry["addsl_hold_until"] = _now + 60
                        entry.pop("uncovered_since", None)
                        log_event({"action": "ADD_SL_PLACED" if _ok else "ADD_SL_FAILED", "symbol": symbol, "qty": _unc, "sl": _trig, "ltp": _ltp, "response": _r})
                        if _ok:
                            legs.append({"order_id": _r.get("orderId"), "qty": _unc, "target_price": None, "leg": "MANUAL_SL", "order_kind": "regular", "order_type": "STOP_LOSS_MARKET"})
                            changed = True
                            notify("[Dhan] SL placed for the added quantity - " + symbol, f"{symbol} {side}: you added {_unc} with no SL. The bridge placed the same SL as the position ({_trig}) for it. It is tracked with the rest and moves with the cascade.", priority="high")
                        else:
                            notify("[Dhan] SL for added quantity FAILED - " + symbol, f"{symbol}: you added {_unc} with no SL and the bridge could not place one at {_trig} ({_r}). Place an SL for the extra {_unc} yourself NOW.", priority="urgent")
        elif _unc <= 0:
            entry.pop("uncovered_since", None)
    except Exception as _ad_e:
        log_event({"action": "ADD_SL_ERROR", "symbol": symbol, "error": str(_ad_e)})

    live_qty = abs(net_qty)
    if entry.get("qty") != live_qty:
        log_event({"action": "ADDQTY_QTY_UPDATED", "symbol": symbol, "old_qty": entry.get("qty"), "new_qty": live_qty})
        entry["qty"] = live_qty
        changed = True
    if changed:
        entry["legs"] = legs
        save_state()
    for qty, trig, aligned in added:
        log_event({"action": "ADDQTY_LEG_ADDED", "symbol": symbol, "qty": qty, "sl_trigger": trig, "stage": stage})
        notify("[Dhan] Extra quantity detected & now tracked",
               f"{symbol}: new SL order for qty {qty} (trigger {trig}) is now managed with the same targets "
               f"(position {live_qty}, cascade stage {stage}).{aligned}")


def _cancel_leftover_orders_manual(symbol: str, entry: dict):
    """ADDED 2026-10-08 (Khagen: when a manual position is closed, its SL / target orders stayed live on
    the exchange). Called only after a manually-tracked position is confirmed flat. Cancels every
    live stop-loss order (plain SL orders and Super Order SL/target legs) still resting on that security,
    since an orphan SELL stop left behind after a long is closed would open an unwanted SHORT when
    triggered. Plain LIMIT orders are NOT touched (a fresh entry order of yours looks identical to a
    leftover plain target), so a plain limit target must still be cancelled by hand."""
    security_id, segment = entry.get("security_id"), entry.get("segment", "NSE_EQ")
    live = collect_live_sl_orders(security_id, segment)
    if not live:
        return 0
    cancelled = 0
    for o in live:
        if o["kind"] == "regular":
            ok = cancel_regular_order(str(o["order_id"]), segment)
        else:
            ok1 = cancel_super_order_leg(str(o["order_id"]), "STOP_LOSS_LEG", segment)
            ok2 = cancel_super_order_leg(str(o["order_id"]), "TARGET_LEG", segment)
            ok = ok1 or ok2
        cancelled += 1 if ok else 0
    log_event({"action": "MANUAL_LEFTOVER_ORDERS_CANCELLED", "symbol": symbol, "found": len(live), "cancelled": cancelled})
    if cancelled:
        notify("[Dhan] Leftover SL/target orders cancelled",
               f"{symbol}: position is closed - cancelled {cancelled} leftover SL/target order(s) so nothing "
               "can trigger by mistake." + ("" if cancelled == len(live) else f" {len(live) - cancelled} could NOT be cancelled - check Dhan."))
    return cancelled


def _remind_pending_orders_after_close(symbol: str, entry: dict):
    """ADDED 2026-10-08 (Khagen's request): after a manually-tracked position is closed and the leftover
    SL/target orders were cancelled, look once more at what is STILL pending on Dhan for that security
    (plain orders and Super Orders, anything not finished) and send a reminder listing it, so nothing
    forgotten can fire later. Read-only."""
    time.sleep(3)  # let the cancellations above settle
    security_id, segment = entry.get("security_id"), entry.get("segment", "NSE_EQ")
    done = ("CANCELLED", "REJECTED", "TRADED", "CLOSED", "EXPIRED")
    rows = []
    try:
        for path in ("/orders", "/super/orders"):
            resp = requests.get(f"{DHAN_BASE_URL}{path}", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
            if not resp.ok:
                return
            data = safe_json(resp)
            if not isinstance(data, list):
                return
            for o in data:
                if str(o.get("securityId")) != str(security_id) or o.get("exchangeSegment") != segment:
                    continue
                if str(o.get("orderStatus", "")).upper() in done:
                    continue
                rows.append(f"{'SUPER ' if path == '/super/orders' else ''}{o.get('transactionType', '')} "
                            f"{o.get('orderType', '')} qty {o.get('quantity', '')} "
                            f"@ {o.get('price') or o.get('triggerPrice') or o.get('stopLossPrice') or '-'} "
                            f"({o.get('orderStatus', '')})")
    except Exception as e:
        log_event({"action": "MANUAL_PENDING_REMINDER_ERROR", "symbol": symbol, "error": str(e)})
        return
    if rows:
        log_event({"action": "MANUAL_PENDING_ORDERS_REMAIN", "symbol": symbol, "orders": rows})
        notify("[Dhan] Reminder: pending orders still on a closed position",
               f"{symbol}: position is closed but {len(rows)} order(s) are still pending on Dhan:\n" +
               "\n".join(rows) + "\nCancel any you do not want.", priority="high")


# =============================================================================
# ADDED 15 Nov 2026 (Khagen's request) - two helpers for MANUAL trades found by the detector below. Nothing here touches signal logic.
#  1) SWING STOP-LOSS: a manual position with NO stop-loss gets one automatically, SWING_SL_WAIT_SECONDS after the bridge first sees it
#     (time to place your own first). The SL goes beyond the last swing low (buy) / swing high (sell) of the 5-minute AND the 15-minute
#     chart - whichever is FARTHER from the entry - plus a small buffer. You can shift it by hand later; the cascade then tracks it as usual. NSE stocks only.
#  2) COOL-OFF DAY: the trading day after a day that ended with Rs 50,000+ profit (capital_lock.COOL_WIN), every newly found MANUAL trade has
#     50% of its quantity exited at market and its SL order(s) resized to the rest. TradingView / scanner trades are never touched.
# =============================================================================
SWING_SL_AUTO = os.environ.get("SWING_SL_AUTO", "true").strip().lower() == "true"
SWING_SL_WAIT_SECONDS = 60
ADD_SL_WAIT_SECONDS = 45                     # added quantity with no SL: the same SL is placed for it after this long (poll is every 30 s, so about 1 minute)
SWING_ATR_MULT = 0.15                       # buffer beyond the swing = 0.15 x ATR(14) of the chart whose swing is used
SWING_ATR_LEN = 14
SWING_SL_LIMIT_BUFFER_PCT = 0.3                # ADDED 16 Nov: the swing SL is a stop-LIMIT order; its limit price sits this % beyond the trigger so it fills when the price moves fast
SWING_BUFFER_PCT = 0.10                     # fallback buffer (% of entry price) only if the ATR cannot be worked out
SWING_PIVOT_K = 2                           # a swing low/high = lower/higher than 2 candles each side
SWING_LOOKBACK = {5: 60, 15: 40}            # candles looked back on each chart
_nosl_first_seen = {}
_swing_sl_placed = set()


def _symbol_pending(symbol) -> bool:
    """True while the bridge itself has an entry order for this symbol waiting to fill (so a position seen now may be the bridge's own, not a manual one)."""
    try:
        with pending_lock:
            return any(str(v.get("symbol")) == str(symbol) for v in pending_entries.values())
    except Exception:
        return False


def _fetch_swing_candles(security_id, segment, interval):
    """Completed candles of the last few days -> (highs, lows, closes), oldest first, or None if the lookup failed. The newest (still forming) candle is dropped."""
    try:
        now = datetime.now(IST)
        payload = {"securityId": str(security_id), "exchangeSegment": segment, "instrument": "EQUITY", "interval": interval,
                   "fromDate": (now - timedelta(days=5)).strftime("%Y-%m-%d 09:15:00"), "toDate": now.strftime("%Y-%m-%d %H:%M:%S")}
        resp = requests.post(f"{DHAN_BASE_URL}/charts/intraday", headers=dhan_headers(), json=payload, timeout=20, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        if not resp.ok or not isinstance(body, dict):
            log_event({"action": "SWING_CANDLES_FAILED", "security_id": security_id, "interval": interval, "http_status": resp.status_code})
            return None
        hi, lo = body.get("high"), body.get("low")
        if not isinstance(hi, list) or not isinstance(lo, list) or len(hi) != len(lo) or len(hi) < 8:
            log_event({"action": "SWING_CANDLES_BAD_SHAPE", "security_id": security_id, "interval": interval, "keys": list(body.keys())[:10]})
            return None
        cl = body.get("close")
        hi = [float(x) for x in hi][:-1]
        lo = [float(x) for x in lo][:-1]
        cl = [float(x) for x in cl][:-1] if isinstance(cl, list) and len(cl) == len(body["high"]) else None
        return hi, lo, cl
    except Exception as e:
        log_event({"action": "SWING_CANDLES_ERROR", "security_id": security_id, "interval": interval, "error": str(e)})
        return None


def _swing_level(hi, lo, side, ref, lookback, k=SWING_PIVOT_K):
    """BUY: the most recent swing LOW that is below ref (the entry). SELL: the most recent swing HIGH above ref. If no pivot is found in the lookback,
    falls back to the lowest low / highest high of the last 12 candles when that is beyond ref. None if nothing usable."""
    hi, lo = hi[-lookback:], lo[-lookback:]
    n = len(lo)
    if n < 2 * k + 1:
        return None
    if side == "BUY":
        for i in range(n - 1 - k, k - 1, -1):
            if lo[i] < ref and all(lo[i] < lo[j] for j in range(i - k, i)) and all(lo[i] <= lo[j] for j in range(i + 1, i + k + 1)):
                return lo[i]
        w = min(lo[-12:])
        return w if w < ref else None
    for i in range(n - 1 - k, k - 1, -1):
        if hi[i] > ref and all(hi[i] > hi[j] for j in range(i - k, i)) and all(hi[i] >= hi[j] for j in range(i + 1, i + k + 1)):
            return hi[i]
    w = max(hi[-12:])
    return w if w > ref else None


def _tick_out(price, side):
    """Round an SL trigger OUTWARD to the 0.05 tick (buy: down, sell: up)."""
    t = NSE_EQ_TICK
    q = float(price) / t
    q = math.floor(q + 1e-9) if side == "BUY" else math.ceil(q - 1e-9)
    return round(q * t, 2)


def _atr(hi, lo, cl, n=SWING_ATR_LEN):
    """Average true range of the last n completed candles, or None."""
    if not cl or len(cl) < n + 1:
        return None
    trs = [max(hi[i] - lo[i], abs(hi[i] - cl[i - 1]), abs(lo[i] - cl[i - 1])) for i in range(len(cl) - n, len(cl))]
    v = sum(trs) / len(trs)
    return v if v > 0 else None


def compute_swing_sl(security_id, segment, side, entry_price):
    """-> (sl_price, info) or (None, reason). The farther of the 5-minute and 15-minute swing, plus a buffer of 0.15 x ATR (of the chart whose swing is used)."""
    levels, atrs = {}, {}
    for iv in (5, 15):
        c = _fetch_swing_candles(security_id, segment, iv)
        if c is None:
            continue
        lv = _swing_level(c[0], c[1], side, entry_price, SWING_LOOKBACK[iv])
        if lv is not None:
            levels[iv] = lv
            atrs[iv] = _atr(c[0], c[1], c[2])
    if not levels:
        return None, "no usable swing found on the 5-minute or 15-minute chart (or the candle lookup failed)"
    pick = min(levels, key=levels.get) if side == "BUY" else max(levels, key=levels.get)
    swing = levels[pick]
    a = atrs.get(pick)
    buf = (a * SWING_ATR_MULT) if a else (entry_price * SWING_BUFFER_PCT / 100.0)
    raw = swing - buf if side == "BUY" else swing + buf
    return _tick_out(raw, side), {"levels": levels, "swing": swing, "buffer": round(buf, 2), "atr": (round(a, 3) if a else None), "chart": pick}


def _swing_try(symbol, security_id, segment, net_qty, entry_price, product_type=None):
    """Called every poll for a manual position with no SL. Waits SWING_SL_WAIT_SECONDS, then places the swing SL once."""
    key = (segment, str(security_id))
    if key in _swing_sl_placed:
        return
    first = _nosl_first_seen.setdefault(key, time.time())
    if time.time() - first < SWING_SL_WAIT_SECONDS:
        return
    side = "BUY" if net_qty > 0 else "SELL"
    qty = abs(net_qty)
    wkey = f"swingfail:{segment}:{security_id}"
    sl, info = compute_swing_sl(security_id, segment, side, entry_price)
    if sl is None:
        if wkey not in _manual_trade_warned:
            _manual_trade_warned.add(wkey)
            notify("[Dhan] Swing SL not placed - " + symbol, f"{symbol}: {info}. No SL was placed - place one yourself now.", priority="high")
        return
    ltp = get_ltp(security_id, segment)
    if ltp is None:
        return                                           # try again next poll - never place an SL blind
    if (side == "BUY" and sl >= ltp) or (side == "SELL" and sl <= ltp):
        _swing_sl_placed.add(key)
        notify("[Dhan] Swing SL not placed - " + symbol, f"{symbol}: price ({ltp}) is already beyond the swing SL ({sl}); an SL there would fire at once. Place your own SL now.", priority="high")
        return
    close_side = "SELL" if side == "BUY" else "BUY"
    # CHANGED 16 Nov: a stop-LIMIT order (the same kind the profit-lock / max-loss exits use and Dhan accepts) in the position's own product type;
    # the limit sits SWING_SL_LIMIT_BUFFER_PCT beyond the trigger (SELL to close a long: below it; BUY to close a short: above it). If Dhan still refuses it,
    # the old stop-market order is tried once, and BOTH replies are kept in the log and the alert.
    lim = sl * (1 - SWING_SL_LIMIT_BUFFER_PCT / 100.0) if close_side == "SELL" else sl * (1 + SWING_SL_LIMIT_BUFFER_PCT / 100.0)
    lim = _tick_round(lim, segment)
    if lim == sl:
        lim = round(sl - NSE_EQ_TICK, 2) if close_side == "SELL" else round(sl + NSE_EQ_TICK, 2)
    resp = place_order(security_id, close_side, qty, "STOP_LOSS", exchange_segment=segment, price=lim, trigger_price=sl,
                       correlation_id=safe_correlation_id(symbol, f"SWSL-{int(time.time())}"), product_type=product_type)
    ok = isinstance(resp, dict) and resp.get("orderId") is not None
    resp1 = resp
    if not ok:
        resp = place_order(security_id, close_side, qty, "STOP_LOSS_MARKET", exchange_segment=segment, trigger_price=sl,
                           correlation_id=safe_correlation_id(symbol, f"SWSM-{int(time.time())}"), product_type=product_type)
        ok = isinstance(resp, dict) and resp.get("orderId") is not None
        if not ok:
            resp = {"stop_limit_reply": resp1, "stop_market_reply": resp}
    _swing_sl_placed.add(key)
    log_event({"action": "SWING_SL_PLACED" if ok else "SWING_SL_FAILED", "symbol": symbol, "side": side, "qty": qty, "sl": sl, "limit": lim, "product": product_type or PRODUCT_TYPE, "ltp": ltp, "info": info, "response": resp})
    lv = info["levels"]
    lvtxt = "  ".join(f"{iv}-min swing {v}" for iv, v in sorted(lv.items()))
    if ok:
        dist = abs(entry_price - sl)
        notify("[Dhan] Swing SL placed - " + symbol,
               f"{symbol} {side} qty {qty} had no SL. Placed a stop-loss at {sl} ({lvtxt}; the farther one used = {info['chart']}-min; buffer {info['buffer']} = 0.15 x ATR{'' if info.get('atr') else ' (ATR unavailable, 0.1% used)'}). "
               f"Distance {dist:.2f} per share = about Rs {dist * qty:,.0f} if it hits. Shift it yourself if you want it tighter; the bridge will then track it like any manual trade.", priority="high")
    else:
        notify("[Dhan] Swing SL FAILED - " + symbol, f"{symbol}: tried to place a swing SL at {sl} but Dhan refused it ({resp}). Place an SL yourself NOW.", priority="urgent")


# ---- cool-off day: 50% exit of a newly found manual trade -------------------------------------------------------------------
_COOL_FILE = None


def _cool_path():
    return os.path.join(_risk_data_dir(), "cooloff_trimmed.json")


def _cool_ids():
    try:
        with open(_cool_path(), "r") as f:
            d = json.load(f)
        if d.get("date") == datetime.now(IST).strftime("%Y-%m-%d"):
            return set(d.get("ids", []))
    except Exception:
        pass
    return set()


def _cool_save(ids):
    try:
        with open(_cool_path() + ".tmp", "w") as f:
            json.dump({"date": datetime.now(IST).strftime("%Y-%m-%d"), "ids": sorted(ids)}, f)
        os.replace(_cool_path() + ".tmp", _cool_path())
    except Exception as e:
        log_event({"action": "COOLOFF_SAVE_ERROR", "error": str(e)})


def _cool_forget(security_id):
    ids = _cool_ids()
    if str(security_id) in ids:
        ids.discard(str(security_id))
        _cool_save(ids)


def _resize_manual_sl(symbol, entry, security_id, segment, close_side, remaining, live):
    """Makes the live SL order(s) of a manual position cover exactly `remaining` units: the position's original SL orders are kept first, any extra order
    (e.g. one placed for quantity that was just exited) is cancelled, and an order that is too big is resized (a Super Order SL cannot be resized: it is replaced
    by a plain SL order). Returns (new_legs, problems)."""
    old_ids = {str(l.get("order_id")) for l in (entry.get("legs") or [])}
    live = sorted(live, key=lambda o: 0 if str(o["order_id"]) in old_ids else 1)
    old_legs = {str(l.get("order_id")): l for l in (entry.get("legs") or [])}
    new_legs, left, problems = [], remaining, []
    for o in live:
        try:
            q = int(float(o["qty"] or 0))
        except (TypeError, ValueError):
            q = 0
        nq = min(q, left)
        left -= nq
        if nq <= 0:
            if o["kind"] == "regular":
                cancel_regular_order(str(o["order_id"]), segment)
            else:
                cancel_super_order_leg(str(o["order_id"]), "STOP_LOSS_LEG", segment)
                cancel_super_order_leg(str(o["order_id"]), "TARGET_LEG", segment)
            continue
        leg = dict(old_legs.get(str(o["order_id"])) or {"order_id": o["order_id"], "target_price": o.get("target_price"), "leg": "MANUAL_SUPER_SL" if o["kind"] == "super" else "MANUAL_SL"})
        if nq == q:
            leg["qty"] = q
            new_legs.append(leg)
            continue
        if o["kind"] == "regular":
            r = modify_sl_order(o["order_id"], security_id, nq, o["trigger"], exchange_segment=segment, order_type=o["order_type"] or "STOP_LOSS_MARKET")
            if isinstance(r, dict) and r.get("orderId") is not None:
                leg["qty"] = nq
                leg["order_kind"] = "regular"
                leg["order_type"] = o["order_type"] or "STOP_LOSS_MARKET"
                new_legs.append(leg)
            else:
                problems.append(f"SL {o['order_id']} could not be resized to {nq}")
                new_legs.append(leg)
        else:                                             # a Super Order's SL leg cannot be resized: replace it with a plain SL order for the rest
            cancel_super_order_leg(str(o["order_id"]), "STOP_LOSS_LEG", segment)
            cancel_super_order_leg(str(o["order_id"]), "TARGET_LEG", segment)
            _trg = _tick_round(o["trigger"], segment)
            _lim = _tick_round(_trg * (1 - SWING_SL_LIMIT_BUFFER_PCT / 100.0) if close_side == "SELL" else _trg * (1 + SWING_SL_LIMIT_BUFFER_PCT / 100.0), segment)   # CHANGED 16 Nov: stop-LIMIT first
            if _lim == _trg:
                _lim = round(_trg - NSE_EQ_TICK, 2) if close_side == "SELL" else round(_trg + NSE_EQ_TICK, 2)
            _otype = "STOP_LOSS"
            r = place_order(security_id, close_side, nq, "STOP_LOSS", exchange_segment=segment, price=_lim, trigger_price=_trg,
                            correlation_id=safe_correlation_id(symbol, f"COOLSL-{int(time.time())}"))
            if not (isinstance(r, dict) and r.get("orderId") is not None):
                _otype = "STOP_LOSS_MARKET"
                r = place_order(security_id, close_side, nq, "STOP_LOSS_MARKET", exchange_segment=segment, trigger_price=_trg,
                                correlation_id=safe_correlation_id(symbol, f"COOLSM-{int(time.time())}"))
            if isinstance(r, dict) and r.get("orderId") is not None:
                new_legs.append({"order_id": r.get("orderId"), "qty": nq, "target_price": None, "leg": "MANUAL_SL", "order_kind": "regular", "order_type": _otype})
            else:
                problems.append(f"new SL for {nq} could not be placed after cancelling the Super Order")
    return new_legs, problems


def _cooloff_trim(symbol, entry, net_qty):
    """Cool-off day: exit 50% (rounded up, whole lots for MCX) of this newly found manual position at market, then make its SL order(s) cover only what is left."""
    security_id, segment, side = entry.get("security_id"), entry.get("segment", "NSE_EQ"), entry.get("side")
    ids = _cool_ids()
    if str(security_id) in ids:
        return
    qty = abs(int(net_qty))
    lot = 1
    if segment == "MCX_COMM":
        try:
            lot = int(get_lot_size(symbol) or 1)
        except Exception:
            lot = 1
    lots = qty // lot
    if lots <= 0:
        return
    exit_qty = ((lots + 1) // 2) * lot
    remaining = qty - exit_qty
    close_side = "SELL" if side == "BUY" else "BUY"
    entry["addsl_hold_until"] = time.time() + 90            # the exit below lowers the position: the "SL for added quantity" check must not read Dhan's old quantity as an add
    ids.add(str(security_id))
    _cool_save(ids)                                       # marked first: never trim the same position twice, even after a restart
    resp = place_order(security_id, close_side, exit_qty, "MARKET", exchange_segment=segment,
                       correlation_id=safe_correlation_id(symbol, f"COOL-{int(time.time())}"))
    ok = isinstance(resp, dict) and resp.get("orderId") is not None
    log_event({"action": "COOLOFF_EXIT_PLACED" if ok else "COOLOFF_EXIT_FAILED", "symbol": symbol, "side": side, "position": qty, "exit_qty": exit_qty, "remaining": remaining, "response": resp})
    if not ok:
        notify("[Dhan] Cool-off exit FAILED - " + symbol, f"{symbol}: today is a cool-off day and the bridge tried to exit {exit_qty} of your {qty}, but Dhan refused it ({resp}). Exit half yourself now.", priority="urgent")
        return
    time.sleep(3)
    if remaining <= 0:
        try:
            _cancel_leftover_orders_manual(symbol, entry)
        except Exception as e:
            log_event({"action": "COOLOFF_CANCEL_ERROR", "symbol": symbol, "error": str(e)})
        notify("[Dhan] Cool-off day - " + symbol + " closed", f"{symbol}: cool-off day, 50% of {qty} rounds up to the whole position, so all {exit_qty} were exited and its SL order(s) cancelled.", priority="high")
        return
    live = collect_live_sl_orders(security_id, segment)
    if live is None:
        notify("[Dhan] Cool-off: check SL - " + symbol, f"{symbol}: exited {exit_qty} of {qty} (cool-off day) but could not read the SL orders to resize them. The SL order may still be for {qty} - fix it NOW (it must be for {remaining}).", priority="urgent")
        return
    new_legs, problems = _resize_manual_sl(symbol, entry, security_id, segment, close_side, remaining, live)
    entry["legs"] = new_legs
    entry["qty"] = remaining
    save_state()
    if problems:
        notify("[Dhan] Cool-off: FIX THE SL - " + symbol, f"{symbol}: exited {exit_qty} of {qty} (cool-off day) but: " + "; ".join(problems) + f". The position is now {remaining} - make sure an SL for {remaining} exists on Dhan NOW.", priority="urgent")
    else:
        notify("[Dhan] Cool-off day - 50% exited - " + symbol, f"{symbol}: yesterday ended with a big profit, so today is a cool-off day. Exited {exit_qty} of your {qty} at market. {remaining} remain, SL resized to {remaining} and still tracked.", priority="high")


def detect_and_register_manual_trades():
    """Runs on its own timer (manual_trade_poll_loop() below). For every
    symbol/security Dhan shows a real open position for for that this bridge
    is NOT already tracking in last_entries (whether never tracked at all, or
    already resolved by reconcile_symbol_from_dhan() for a different reason -
    either way, already-tracked means nothing to do here): looks for a live,
    plain stop-loss order protecting it (find_live_plain_sl_order() - the
    exact same lookup the manual re-entry fix already uses), computes R =
    |entry - sl| and the 1R-6R targets, and registers it into last_entries
    with cascade_stage=0 so the existing cascade watchdog takes it from
    there - same as any other tracked trade.

    Deliberately conservative: a position found with NO live SL order is
    NOT auto-protected by placing one on Khagen's behalf (he said he places
    the SL himself) - it's only notified once, so he knows the bridge sees
    an unprotected manual position and can place the SL himself. Likewise a
    position whose SL is already on the wrong side of entry (would compute
    a meaningless/negative R) is reported, not guessed at."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "MANUAL_DETECT_POSITIONS_LOOKUP_FAILED", "status": resp.status_code})
            return
        positions = safe_json(resp)
        if not isinstance(positions, list):
            return
        if PRODUCT_TYPE != "CNC":       # ADDED 2026-11-12: swing (CNC) positions are not adopted by the manual-trade detector
            positions = [_p for _p in positions if str(_p.get("productType") or "").upper() != "CNC"]
    except Exception as e:
        log_event({"action": "MANUAL_DETECT_POSITIONS_LOOKUP_ERROR", "error": str(e)})
        return

    still_open_security_ids = set()

    for p in positions:
        segment = p.get("exchangeSegment")
        if segment not in ("NSE_EQ", "MCX_COMM"):
            continue  # scope stays to exactly what this bridge already trades
        try:
            net_qty = int(float(p.get("netQty", 0) or 0))
        except (TypeError, ValueError):
            continue
        if net_qty == 0:
            continue

        security_id = p.get("securityId")
        if not security_id:
            continue
        still_open_security_ids.add((segment, str(security_id)))

        symbol = get_symbol_for_security_id(security_id, segment)
        if not symbol:
            key = f"unresolved:{segment}:{security_id}"
            if key not in _manual_trade_warned:
                _manual_trade_warned.add(key)
                log_event({"action": "MANUAL_DETECT_SYMBOL_UNRESOLVED", "security_id": security_id,
                           "segment": segment, "net_qty": net_qty,
                           "note": "a real open position exists for this security_id but it could not be "
                                    "matched back to a trading symbol (not in the scrip master's NSE/MCX "
                                    "tables - may be an options/futures contract this bridge doesn't "
                                    "otherwise trade). Not auto-tracked - manage its SL manually."})
            continue

        if symbol in last_entries:
            _avg = (p.get("costPrice") or (p.get("buyAvg") if net_qty > 0 else p.get("sellAvg"))
                    or p.get("averageTradedPrice") or p.get("avgPrice"))
            # ADDED 2026-10-08: a MANUAL trade already tracked - check for extra quantity / extra SL orders
            if last_entries[symbol].get("manual_trade"):
                try:
                    _sync_manual_trade_addons(symbol, last_entries[symbol], net_qty, _avg)
                except Exception as e:
                    log_event({"action": "ADDQTY_SYNC_ERROR", "symbol": symbol, "error": str(e)})
            continue  # already tracked - nothing more to register

        # Dhan's /positions field for the average price of the current net
        # position - tried in this order since this bridge has not
        # previously needed to read it off a /positions row (every other
        # entry_price in this file comes from an ORDER's own
        # averageTradedPrice, not a position row). costPrice is Dhan's
        # documented field for this; buyAvg/sellAvg and averageTradedPrice
        # are included as fallbacks in case of an API variant. The first
        # notification for every newly-registered manual trade echoes this
        # number explicitly so Khagen can sanity-check it against Dhan's
        # own app once - see the notify() call below.
        entry_price_raw = (p.get("costPrice") or (p.get("buyAvg") if net_qty > 0 else p.get("sellAvg"))
                            or p.get("averageTradedPrice") or p.get("avgPrice"))
        try:
            entry_price = float(entry_price_raw) if entry_price_raw not in (None, "", "0", 0) else None
        except (TypeError, ValueError):
            entry_price = None
        if entry_price is None:
            key = f"noentryprice:{segment}:{security_id}"
            if key not in _manual_trade_warned:
                _manual_trade_warned.add(key)
                log_event({"action": "MANUAL_DETECT_NO_ENTRY_PRICE", "symbol": symbol, "security_id": security_id,
                           "net_qty": net_qty,
                           "note": "a real open position exists but no usable average price field was found "
                                    "on its /positions row - cannot compute R without it. Not auto-tracked."})
            continue

        # ADDED 2026-10-01 (real test, NATURALGASM): Khagen's manual SL turned
        # out to be a Dhan SUPER ORDER (entry+target+SL as one bracket, placed
        # from the chart's quick-order ticket) - checked FIRST here, same as
        # reconcile_symbol_from_dhan() already does for the redeploy-recovery
        # case, before falling back to a plain standalone SL order.
        super_order, sl_leg = find_live_super_order_for_position(security_id, segment)
        if super_order is not None:
            source_order_id = super_order.get("orderId")
            source_order_kind = "super"
            qty_val = super_order.get("quantity") or super_order.get("remainingQuantity") or abs(net_qty)
            target_price = super_order.get("targetPrice")
            sl_price_raw = (super_order.get("stopLossPrice") or sl_leg.get("stopLossPrice")
                             or sl_leg.get("price") or sl_leg.get("triggerPrice"))
            # DIAGNOSTIC: the field used for sl_price_raw above is a best-guess
            # fallback chain (this bridge had never needed to READ a Super
            # Order's SL price before, only WRITE one) - logged once per
            # symbol so it can be verified/corrected against Dhan's real
            # response, same reasoning as find_live_plain_sl_order()'s
            # diagnostic above.
            diag_key = f"superdiag:{segment}:{security_id}"
            if diag_key not in _manual_trade_warned:
                _manual_trade_warned.add(diag_key)
                log_event({"action": "MANUAL_DETECT_SUPER_ORDER_DIAGNOSTIC", "symbol": symbol,
                           "order_id": source_order_id, "sl_price_raw_picked": sl_price_raw,
                           "order_fields": {k: super_order.get(k) for k in
                               ("orderStatus", "quantity", "remainingQuantity", "averageTradedPrice", "price",
                                "targetPrice", "stopLossPrice")},
                           "sl_leg_fields": {k: sl_leg.get(k) for k in
                               ("orderStatus", "legStatus", "price", "triggerPrice", "stopLossPrice",
                                "remainingQuantity")},
                           "note": "first live Super Order seen for this symbol - dumping raw fields to verify "
                                    "sl_price_raw picked the right one."})
        else:
            plain_order = find_live_plain_sl_order(security_id, segment)
            if plain_order is None:
                key = f"nosl:{segment}:{security_id}"
                if SWING_SL_AUTO and segment == "NSE_EQ" and not _symbol_pending(symbol):   # ADDED 15 Nov: place a swing SL after a short wait
                    try:
                        _swing_try(symbol, security_id, segment, net_qty, entry_price, product_type=p.get("productType"))
                    except Exception as _sw_e:
                        log_event({"action": "SWING_SL_ERROR", "symbol": symbol, "error": str(_sw_e)})
                if key not in _manual_trade_warned:
                    _manual_trade_warned.add(key)
                    log_event({"action": "MANUAL_DETECT_NO_SL_FOUND", "symbol": symbol, "security_id": security_id,
                               "net_qty": net_qty, "entry_price": entry_price,
                               "note": "a real open manual position was found with NO live stop-loss order "
                                        "protecting it (checked both Super Orders and plain orders) - not "
                                        "auto-protecting it (Khagen places his own SL), just flagging it once "
                                        "so he knows the bridge sees it."})
                    _sw_txt = (f" If you do not place one, the bridge places a swing-based SL in about {SWING_SL_WAIT_SECONDS} seconds."
                               if (SWING_SL_AUTO and segment == "NSE_EQ") else "")
                    notify("[Dhan] Manual position has NO stop-loss",
                           f"{symbol}: found an open position (qty {net_qty}, entry ~{entry_price}) with no "
                           "resting SL order on Dhan. The bridge will not auto-track it or compute R/targets "
                           "until a real SL order exists for it - place one on Dhan now." + _sw_txt)
                continue
            source_order_id = plain_order.get("orderId")
            source_order_kind = "regular"
            qty_val = plain_order.get("quantity") or plain_order.get("remainingQuantity") or abs(net_qty)
            target_price = None
            sl_price_raw = plain_order.get("triggerPrice")

        try:
            sl_price = float(sl_price_raw) if sl_price_raw not in (None, "", "0") else None
        except (TypeError, ValueError):
            sl_price = None
        if sl_price is None:
            key = f"badsl:{segment}:{security_id}"
            if key not in _manual_trade_warned:
                _manual_trade_warned.add(key)
                log_event({"action": "MANUAL_DETECT_SL_PRICE_UNREADABLE", "symbol": symbol,
                           "order_id": source_order_id, "order_kind": source_order_kind, "raw": sl_price_raw})
            continue

        side = "BUY" if net_qty > 0 else "SELL"
        wrong_side = (side == "BUY" and sl_price >= entry_price) or (side == "SELL" and sl_price <= entry_price)
        if wrong_side:
            key = f"wrongside:{segment}:{security_id}"
            if key not in _manual_trade_warned:
                _manual_trade_warned.add(key)
                log_event({"action": "MANUAL_DETECT_SL_WRONG_SIDE", "symbol": symbol, "side": side,
                           "entry_price": entry_price, "sl_price": sl_price,
                           "note": "the live SL order's trigger price is on the wrong side of entry for this "
                                    "position's direction (R would be meaningless/negative) - not auto-tracked."})
                notify("[Dhan] Manual position SL looks wrong",
                       f"{symbol}: entry ~{entry_price}, but the live SL trigger ({sl_price}) is on the wrong "
                       f"side for a {side} position. Not auto-tracking this - check the SL on Dhan.")
            continue

        targets_1r_to_6r = _compute_r_multiple_targets(entry_price, sl_price, side)
        leg = {"order_id": source_order_id, "qty": qty_val, "target_price": target_price,
               "leg": "MANUAL_SUPER_SL" if source_order_kind == "super" else "MANUAL_SL"}
        if source_order_kind == "regular":
            # Only a plain standalone order needs "order_kind": "regular" -
            # _handle_move_sl_to_cost() reads that to call modify_sl_order()
            # instead of modify_super_order_leg(). A Super-Order-sourced leg
            # (no "order_kind" key) takes the default path, exactly like
            # reconcile_symbol_from_dhan()'s own Super-Order-sourced legs.
            leg["order_kind"] = "regular"
            leg["order_type"] = plain_order.get("orderType", "STOP_LOSS_MARKET")

        last_entries[symbol] = {
            "side": side, "qty": abs(net_qty), "legs": [leg], "security_id": security_id,
            "segment": segment, "entry_price": entry_price, "sl_price": sl_price,
            # Only T1-T5 feed the cascade watchdog (targets[0..4], same as
            # every alert-driven trade) - T6 is informational-only, see
            # _compute_r_multiple_targets()'s docstring.
            "targets": targets_1r_to_6r[:5],
            "cascade_stage": 0,
            "manual_trade": True,
        }
        save_state()
        _manual_trade_warned.discard(f"nosl:{segment}:{security_id}")
        t1, t2, t3, t4, t5, t6 = targets_1r_to_6r
        r_value = abs(entry_price - sl_price)
        log_event({"action": "MANUAL_TRADE_REGISTERED", "symbol": symbol, "side": side, "qty": abs(net_qty),
                   "entry_price": entry_price, "sl_price": sl_price, "r": r_value,
                   "t1": t1, "t2": t2, "t3": t3, "t4": t4, "t5": t5, "t6": t6,
                   "note": "found a manually-opened, manually-protected position with no prior alert/record - "
                            "registered it with computed R-multiple targets. The existing cascade watchdog "
                            "will now shift its SL to cost at T1, then trail T1->T4 through T2-T5, same as "
                            "any alert-driven trade. VERIFY the entry/SL numbers below against Dhan's app once."})
        notify("[Dhan] Manual trade detected & now tracked",
               f"{symbol} {side} qty {abs(net_qty)} - entry ~{entry_price}, SL {sl_price} (R={r_value:.2f}).\n"
               f"Targets: T1 {t1}  T2 {t2}  T3 {t3}  T4 {t4}  T5 {t5}  T6 {t6}.\n"
               "Now auto-managed: SL moves to cost at T1, then trails through T2-T5. "
               "Please verify entry/SL above match Dhan's app - this is the first time the bridge has seen "
               "this trade.")
        try:                                               # ADDED 15 Nov: cool-off day - exit 50% of a newly found manual trade
            if CL is not None and not _symbol_pending(symbol) and CL.cool_active(datetime.now(IST).strftime("%Y-%m-%d")):
                _cooloff_trim(symbol, last_entries[symbol], net_qty)
        except Exception as _cool_e:
            log_event({"action": "COOLOFF_ERROR", "symbol": symbol, "error": str(_cool_e)})

    # Clean up: a manual trade this function registered, once its position
    # genuinely goes flat on Dhan, would otherwise sit in last_entries
    # forever (nothing else watches for a manually-tracked symbol going
    # flat - SL_TAKEN alerts only ever come from the Pine script's OWN
    # signal-driven state, which was never tracking this trade to begin
    # with, since it didn't open it). Clear it here instead.
    for _k in list(_nosl_first_seen):                      # ADDED 15 Nov: forget swing-SL bookkeeping of positions that are gone
        if _k not in still_open_security_ids:
            _nosl_first_seen.pop(_k, None)
    for _k in list(_swing_sl_placed):
        if _k not in still_open_security_ids:
            _swing_sl_placed.discard(_k)
    for symbol, entry in list(last_entries.items()):
        if not entry.get("manual_trade"):
            continue
        key = (entry.get("segment"), str(entry.get("security_id")))
        if key not in still_open_security_ids:
            try:
                _cool_forget(key[1])
            except Exception:
                pass
            log_event({"action": "MANUAL_TRADE_CLOSED", "symbol": symbol,
                       "note": "this manually-tracked position is no longer open on Dhan (SL/target hit, or "
                                "closed manually) - clearing it from tracking."})
            # FIX 2026-10-01 (real gap: NATURALGASM closed and this cleanup
            # ran exactly as designed, but silently - only a log_event line,
            # no notify() - so there was no phone notification telling
            # Khagen the position had actually closed and tracking stopped.
            # Every other state change in this manual-trade feature notifies
            # (registered, cascade stage applied) - this one should too.
            last_stage = entry.get("cascade_stage", 0)
            notify("[Dhan] Manual trade closed - tracking stopped",
                   f"{symbol}: Dhan no longer shows this position open (SL/target hit, or closed manually). "
                   f"It had reached cascade stage {last_stage} before closing. No longer tracked - nothing "
                   "more to auto-manage for it.")
            try:
                _cancel_leftover_orders_manual(symbol, entry)
            except Exception as e:
                log_event({"action": "MANUAL_LEFTOVER_CANCEL_ERROR", "symbol": symbol, "error": str(e)})
            try:
                _remind_pending_orders_after_close(symbol, entry)
            except Exception as e:
                log_event({"action": "MANUAL_PENDING_REMINDER_ERROR", "symbol": symbol, "error": str(e)})
            last_entries.pop(symbol, None)
            _manual_trade_warned.discard(f"nosl:{key[0]}:{key[1]}")
            save_state()


def manual_trade_poll_loop():
    log_event({"action": "MANUAL_TRADE_DETECTOR_STARTED", "enabled": MANUAL_TRADE_DETECTION_ENABLED,
               "poll_seconds": MANUAL_TRADE_POLL_SECONDS})
    while True:
        time.sleep(MANUAL_TRADE_POLL_SECONDS)
        if not MANUAL_TRADE_DETECTION_ENABLED:
            continue
        try:
            detect_and_register_manual_trades()
        except Exception as e:
            log_event({"action": "MANUAL_TRADE_DETECTOR_ERROR", "error": str(e)})


threading.Thread(target=manual_trade_poll_loop, daemon=True, name="manual-trade-detector").start()


NSE_EQ_TICK = 0.05


def _tick_round(price, segment):
    """ADDED 2026-10-07 (real incident: RATNAVEER's stop-loss move to cost was sent with a trigger of
    346.1111 - the average fill price - Dhan answered HTTP 200 / "TRANSIT" but the order stayed at its
    old trigger. NSE equity prices must be multiples of the 0.05 tick, and the 4-decimal cost/target
    numbers computed from a manual trade's average entry are not). NSE_EQ only - other segments (MCX
    commodities) have their own, different tick sizes, so their prices are passed through unchanged."""
    if price is None or segment != "NSE_EQ":
        return price
    return round(round(float(price) / NSE_EQ_TICK) * NSE_EQ_TICK, 2)


def get_order_record(order_id: str):
    """The full Dhan order record (dict) for one plain order, or None if it can't be fetched."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/orders/{order_id}", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        if not resp.ok:
            return None
        record = body[0] if isinstance(body, list) and body else body
        return record if isinstance(record, dict) else None
    except Exception:
        return None


def _confirm_sl_modified(order_id: str, expected_trigger, tries: int = 3, delay: float = 2.0):
    """ADDED 2026-10-07: Dhan answers a modify request with HTTP 200 and orderStatus "TRANSIT" ("being
    processed at the exchange") - that is NOT confirmation. This re-reads the order a couple of seconds
    later and checks the trigger price really is the new one. Returns (confirmed, detail)."""
    detail = {}
    for _ in range(tries):
        time.sleep(delay)
        rec = get_order_record(order_id)
        if not rec:
            detail = {"note": "could not read the order back from Dhan"}
            continue
        status = str(rec.get("orderStatus", "")).upper()
        trig = rec.get("triggerPrice")
        detail = {"status": status, "trigger_now": trig,
                  "dhan_reason": rec.get("omsErrorDescription") or rec.get("omsErrorCode") or ""}
        if status == "TRADED":
            return True, detail  # the stop already triggered - nothing left to move
        if status in ("REJECTED", "CANCELLED", "EXPIRED"):
            return False, detail
        try:
            if trig is not None and abs(float(trig) - float(expected_trigger)) <= 0.011:
                return True, detail
        except (TypeError, ValueError):
            pass
    return False, detail


def _handle_move_sl_to_cost(symbol: str, alert: dict, stage: int = None, source: str = "tradingview"):
    """FIX 2026-09-28 (real incident: COALINDIA's MOVE_SL_TO_COST alert
    showed "Webhook delivery failed - request took too long and timed out"
    on TradingView's side twice in one morning, even though the bridge's
    own logs show it eventually completed and responded 200 each time.
    This used to run entirely INLINE inside the webhook request/response
    cycle - get_security_id_and_segment() (can trigger a scrip-master
    rebuild), get_position_size(), a full GET /super/orders scan for
    reconcile, then a PUT per leg to modify its SL - several sequential
    real network calls to Dhan, all before ever responding to TradingView.
    Any one of those being a bit slow is enough to blow past TradingView's
    webhook timeout. Fresh entries already avoid this exact problem by
    running in a background thread (see open_position's call site) -
    MOVE_SL_TO_COST just never got the same treatment until now. The
    webhook handler now responds immediately and calls this in a thread
    instead - the logic itself (REWRITTEN 2026-09-25 for Super Order legs,
    UPDATED 2026-09-27 for reconcile-from-Dhan, UPDATED 2026-09-28 to tell
    "already flat" apart from "genuinely lost track of it") is unchanged.

    ADDED 2026-09-30 (Khagen's request, mirrors the same fix on the Delta/
    crypto bridge): now called from TWO independent places - the webhook's
    MOVE_SL_TO_COST/T1/T2/T3/T4 handler (source="tradingview") AND the new
    bridge-side cascade watchdog (source="watchdog", see
    _check_cascade_targets() below), which polls Dhan's own live LTP
    directly and does not depend on TradingView ever sending that alert at
    all. "stage" (1=cost/T1-touch, 2=T1, 3=T2, 4=T3, 5=T4) plus
    last_entries[symbol]["cascade_stage"] (the highest stage actually
    applied so far) decide which of the two wins if both notice the same
    crossing - a call for a stage at or below what's already recorded is
    skipped rather than re-applied or backwards-moved."""
    entry = last_entries.get(symbol)
    if stage is not None and entry is not None:
        already_at = entry.get("cascade_stage", 0)
        if stage <= already_at:
            log_event({"action": "SKIPPED_CASCADE_STAGE_ALREADY_PASSED", "symbol": symbol, "stage": stage,
                       "already_at_stage": already_at, "source": source,
                       "note": "this stage (or a later one) was already applied by the other path "
                                "(TradingView alert vs bridge watchdog, whichever got there first) - "
                                "skipped so the SL never moves backwards or gets redundantly re-edited"})
            return
    new_sl = parse_num(alert, "price")
    legs = entry.get("legs") if entry else None
    security_id, segment = (None, None)
    if not entry or not legs:
        # before giving up, try to recover this symbol's live super order
        # leg(s) directly from Dhan (see reconcile_symbol_from_dhan()'s
        # docstring for the full incident/design rationale) - a restart/
        # redeploy since entry is the normal reason last_entries would be
        # empty here even though the position is genuinely still open.
        security_id, segment = get_security_id_and_segment(symbol)
        if security_id:
            entry = reconcile_symbol_from_dhan(symbol, security_id, segment)
            legs = entry.get("legs") if entry else None
    if not entry or not legs:
        # tell "already flat" (nothing to do, safe) apart from "genuinely
        # lost track of a still-open position" (needs manual attention) -
        # crying wolf on an already-flat position erodes trust in the real
        # alerts.
        really_flat = False
        if security_id:
            current_qty_check = get_position_size(security_id, segment)
            really_flat = (current_qty_check == 0)
        if really_flat:
            log_event({"action": "SKIPPED_MOVE_SL_ALREADY_FLAT", "symbol": symbol,
                       "note": "confirmed via Dhan positions API - no open position for this symbol "
                                "any more, nothing to protect. Clearing any stale tracked entry."})
            notify("[Dhan] SL breakeven skipped - already flat",
                   f"{symbol} hit T1 but Dhan shows no open position for it any more (closed some other "
                   "way already). Nothing to do.")
            last_entries.pop(symbol, None)
            save_state()
        else:
            log_event({"action": "SKIPPED_MOVE_SL_NO_TRACKED_ORDER", "symbol": symbol,
                       "note": "no live super order leg(s) on file for this symbol (bridge may have restarted since entry), and reconciling directly from Dhan found nothing to recover either - move it manually in Dhan"})
            notify("[Dhan] Could not move SL to breakeven", f"{symbol} hit T1 but the bridge has no order on file to shift. Check its SL manually on Dhan.")
        return
    if new_sl is None:
        log_event({"action": "SKIPPED_MOVE_SL_NO_PRICE", "symbol": symbol})
        return
    _raw_sl = new_sl
    new_sl = _tick_round(new_sl, entry.get("segment", "NSE_EQ"))
    if new_sl != _raw_sl:
        log_event({"action": "SL_ROUNDED_TO_TICK", "symbol": symbol, "requested": _raw_sl, "sent": new_sl})

    failed_legs = []
    unconfirmed = {}
    for leg in legs:
        if leg.get("order_kind") == "regular":
            # ADDED 2026-10-01: a MANUAL_SL leg (see find_live_plain_sl_order())
            # is a plain standalone stop order, not a Super Order leg - it has
            # to be shifted via modify_sl_order() (PUT /orders/{id}), not
            # modify_super_order_leg() (PUT /super/orders/{id}), which would
            # 404/no-op against it.
            resp = modify_sl_order(leg["order_id"], entry.get("security_id"), leg.get("qty"), new_sl,
                                    exchange_segment=entry.get("segment", "NSE_EQ"),
                                    order_type=leg.get("order_type", "STOP_LOSS_MARKET"))
            ok = isinstance(resp, dict) and resp.get("orderId") is not None
            if ok and resp.get("orderStatus") != "SIMULATED_MODIFY":
                confirmed, detail = _confirm_sl_modified(leg["order_id"], new_sl)
                if not confirmed:
                    log_event({"action": "SL_MODIFY_NOT_CONFIRMED_RETRYING", "symbol": symbol,
                               "order_id": leg["order_id"], "wanted": new_sl, "detail": detail})
                    resp = modify_sl_order(leg["order_id"], entry.get("security_id"), leg.get("qty"), new_sl,
                                            exchange_segment=entry.get("segment", "NSE_EQ"),
                                            order_type=leg.get("order_type", "STOP_LOSS_MARKET"))
                    ok = isinstance(resp, dict) and resp.get("orderId") is not None
                    if ok:
                        confirmed, detail = _confirm_sl_modified(leg["order_id"], new_sl)
                if not confirmed:
                    ok = False
                    unconfirmed[str(leg.get("leg", leg["order_id"]))] = detail
        else:
            ok, resp = modify_super_order_leg(leg["order_id"], "STOP_LOSS_LEG",
                                               exchange_segment=entry.get("segment", "NSE_EQ"),
                                               stopLossPrice=new_sl, trailingJump=0)
        if not ok:
            failed_legs.append(leg.get("leg", leg["order_id"]))

    if failed_legs:
        log_event({"action": "SL_MODIFY_FAILED_SOME_LEGS", "symbol": symbol, "failed_legs": failed_legs,
                   "total_legs": len(legs), "not_confirmed_detail": unconfirmed})
        if unconfirmed:
            notify("[Dhan] SL move NOT confirmed - move it yourself",
                   f"{symbol}: asked Dhan to move the stop-loss to {new_sl} but the order did not change "
                   f"({'; '.join(f'{k}: {v}' for k, v in unconfirmed.items())}). Check Dhan and move it manually.")
        else:
            notify("[Dhan] SL move to breakeven PARTIALLY failed",
                   f"{symbol} hit T1 - {len(legs) - len(failed_legs)}/{len(legs)} leg(s) moved to breakeven "
                   f"({', '.join(failed_legs)} failed). Check and move the failed leg(s) manually to ~{new_sl}.")
    else:
        log_event({"action": "SL_MODIFIED", "symbol": symbol, "legs": len(legs), "new_sl": new_sl})

    # ADDED 2026-09-30: record the stage reached (even on a partial-leg
    # failure - the SL price itself still moved for at least one leg, and
    # this is also what lets the watchdog/webhook paths recognize this
    # stage as done so neither re-fires it) and keep sl_price current for
    # anything else that reads it.
    if symbol in last_entries:
        last_entries[symbol]["sl_price"] = new_sl
        if stage is not None:
            last_entries[symbol]["cascade_stage"] = stage
    save_state()
    if stage is not None:
        log_event({"action": "CASCADE_STAGE_APPLIED", "symbol": symbol, "stage": stage,
                   "new_sl_price": new_sl, "source": source})
        if not failed_legs:
            _STAGE_LABEL = {1: "Cost (breakeven)", 2: "T1", 3: "T2", 4: "T3", 5: "T4"}
            stage_label = _STAGE_LABEL.get(stage, f"stage {stage}")
            via = "TradingView alert" if source == "tradingview" else "bridge watchdog (backup path)"
            notify(f"[Dhan] SL moved to {stage_label}",
                   f"{symbol}: SL moved to {new_sl} ({stage_label}). Triggered via {via}.",
                   priority="default")


def get_ltp(security_id: str, exchange_segment: str):
    """Returns the current last-traded-price for one security via Dhan's
    marketfeed/ltp endpoint, or None if the lookup fails. Used only for the
    entry price-drift guard below - never blocks a trade on its own if it
    fails, since a data-fetch failure here shouldn't become a new way to
    silently skip a real signal."""
    try:
        body = {exchange_segment: [int(security_id)]}
        resp = requests.post(f"{DHAN_BASE_URL}/marketfeed/ltp", headers=dhan_headers(), json=body,
                              timeout=10, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "LTP_LOOKUP_FAILED", "security_id": security_id, "segment": exchange_segment,
                       "status": resp.status_code, "body": safe_json(resp)})
            return None
        body_json = safe_json(resp)
        if not isinstance(body_json, dict):
            return None
        seg_data = (body_json.get("data") or {}).get(exchange_segment) or {}
        entry = seg_data.get(str(security_id))
        if not entry:
            return None
        ltp = entry.get("last_price")
        return float(ltp) if ltp is not None else None
    except Exception as e:
        log_event({"action": "LTP_LOOKUP_ERROR", "security_id": security_id, "segment": exchange_segment, "error": str(e)})
        return None


def get_ltp_batch(pairs):
    """ADDED 2026-10-05: ONE marketfeed/ltp request for many securities.
    pairs = iterable of (exchange_segment, security_id). Returns
    {(exchange_segment, str(security_id)): last_price}. The cascade watchdog
    used to call get_ltp() once per tracked trade back-to-back every 5s;
    with several tracked trades that bursts past Dhan's ~1 request/second
    limit on this endpoint (HTTP 429 "Too many requests. Further requests may
    result in the user being blocked"). Dhan accepts many ids per segment in
    one call, so the watchdog now makes a single request per tick. Returns {}
    on any failure (callers just skip that tick and retry 5s later)."""
    by_seg = {}
    for seg, sid in pairs:
        try:
            by_seg.setdefault(seg, set()).add(int(sid))
        except (TypeError, ValueError):
            continue
    if not by_seg:
        return {}
    try:
        body = {seg: sorted(ids) for seg, ids in by_seg.items()}
        resp = requests.post(f"{DHAN_BASE_URL}/marketfeed/ltp", headers=dhan_headers(), json=body,
                              timeout=10, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "LTP_BATCH_LOOKUP_FAILED", "segments": list(by_seg), "status": resp.status_code,
                       "body": safe_json(resp)})
            return {}
        body_json = safe_json(resp)
        if not isinstance(body_json, dict):
            return {}
        out = {}
        for seg in by_seg:
            seg_data = (body_json.get("data") or {}).get(seg) or {}
            for sid, entry in seg_data.items():
                ltp = (entry or {}).get("last_price")
                if ltp is not None:
                    out[(seg, str(sid))] = float(ltp)
        return out
    except Exception as e:
        log_event({"action": "LTP_BATCH_LOOKUP_ERROR", "error": str(e)})
        return {}


def price_drifted_too_far(signal_price, security_id: str, exchange_segment: str):
    """Returns (True, ltp, deviation_pct) if the current market price has
    moved more than ENTRY_PRICE_TOLERANCE_PCT away from the signal's
    confirmed price - meaning the move already happened and this entry
    should be skipped rather than chased at a much worse price. Returns
    (False, ltp_or_None, deviation_or_None) otherwise, including when the
    check can't be performed at all (no signal price, tolerance disabled,
    or the LTP lookup failed) - in every "can't check" case this returns
    False so a data hiccup here never blocks a real trade."""
    if not signal_price or ENTRY_PRICE_TOLERANCE_PCT <= 0:
        return False, None, None
    ltp = get_ltp(security_id, exchange_segment)
    if ltp is None:
        log_event({"action": "PRICE_DRIFT_CHECK_SKIPPED", "security_id": security_id,
                   "note": "LTP lookup failed - proceeding without the drift check for this trade"})
        return False, None, None
    deviation_pct = abs(ltp - signal_price) / signal_price * 100
    return deviation_pct > ENTRY_PRICE_TOLERANCE_PCT, ltp, deviation_pct


def get_order_status(order_id: str):
    """Returns Dhan's orderStatus string for one order (e.g. "TRADED",
    "PENDING", "CANCELLED"), or None if it couldn't be fetched.

    Every path that returns None also logs via log_event (the structured
    JSON logger visible in Render's Application logs) - not plain
    log.warning, which was found NOT to reliably show up there. A SILVERM
    trade filled on Dhan but never got its SL attached because this
    function silently returned None every poll cycle (Dhan's response body
    here was not a plain dict) with zero trace in the visible logs -
    never again silent.
    """
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/orders/{order_id}", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        if not resp.ok:
            log_event({"action": "ORDER_STATUS_LOOKUP_FAILED", "order_id": order_id,
                       "http_status": resp.status_code, "response": body})
            return None
        # Dhan returns either a single object or a one-item list depending on endpoint version.
        record = body[0] if isinstance(body, list) and body else body
        if isinstance(record, dict):
            return str(record.get("orderStatus", "")).upper()
        log_event({"action": "ORDER_STATUS_LOOKUP_UNEXPECTED_BODY", "order_id": order_id,
                   "response": body, "note": "response was not a dict - Dhan may no longer be returning this order via this endpoint"})
    except Exception as e:
        log_event({"action": "ORDER_STATUS_LOOKUP_ERROR", "order_id": order_id, "error": str(e)})
    return None


def get_super_order_status(order_id: str):
    """FIX 2026-10-01 (real incident: _check_super_order_entries_for_failure()
    was calling get_order_status() - which hits GET /orders/{id} - on Super
    Order IDs too. Dhan's plain /orders/{id} endpoint doesn't know about
    Super Orders at all and returns an empty list for them, so that check
    logged ORDER_STATUS_LOOKUP_UNEXPECTED_BODY every poll cycle forever for
    every Super-Order-tracked leg and could never actually confirm a status.
    Super Orders only exist under /super/orders, and that endpoint is
    list-only (no per-ID GET), so this scans the list and matches by
    orderId - same approach already used by find_live_super_order_for_position().
    Returns the SAME orderStatus string shape get_order_status() returns
    ("TRADED"/"PENDING"/"CANCELLED"/etc), or None if it couldn't be found
    or the lookup failed, so callers can treat the two interchangeably."""
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            log_event({"action": "SUPER_ORDER_STATUS_LOOKUP_FAILED", "order_id": order_id,
                       "http_status": resp.status_code, "response": safe_json(resp)})
            return None
        orders = safe_json(resp)
        if not isinstance(orders, list):
            return None
        for o in orders:
            if str(o.get("orderId")) == str(order_id):
                return str(o.get("orderStatus", "")).upper()
        return None  # not in the list - could mean it fell out of Dhan's recent-orders window, not necessarily dead
    except Exception as e:
        log_event({"action": "SUPER_ORDER_STATUS_LOOKUP_ERROR", "order_id": order_id, "error": str(e)})
        return None


def get_leg_order_status(leg: dict):
    """Picks the right status lookup for one tracked leg: a plain standalone
    order (order_kind == "regular", e.g. a MANUAL_SL leg from
    find_live_plain_sl_order()) uses get_order_status() against /orders/{id};
    everything else (a Super-Order-sourced leg, the normal case since
    place_super_order()/reconcile_symbol_from_dhan()/the manual-trade
    detector's Super Order path all store the Super Order's own orderId
    here) uses get_super_order_status() against /super/orders instead."""
    order_id = leg.get("order_id")
    if not order_id:
        return None
    if leg.get("order_kind") == "regular":
        return get_order_status(order_id)
    return get_super_order_status(order_id)


def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text[:500]}


def modify_sl_order(order_id: str, security_id: str, qty: int, new_trigger_price: float,
                     exchange_segment: str = "NSE_EQ", order_type: str = "STOP_LOSS_MARKET"):
    """Moves an already-placed plain STOP_LOSS_MARKET/STOP_LOSS order's
    trigger price - used for shifting SL to cost (breakeven) and each
    subsequent cascade stage. Does not touch qty or side; only the trigger
    price changes.

    RESURRECTED 2026-10-01 (Khagen's manual re-entry workflow): this was the
    bridge's only SL-modify path before the 2026-09-25 Super Order rewrite,
    then left unused once every entry this bridge places became a Super
    Order. It's needed again now for exactly the case the rewrite didn't
    cover: a position Khagen re-enters BY HAND on Dhan, protected by a plain
    standalone stop-loss order instead of a Super Order - see
    find_live_plain_sl_order() and reconcile_symbol_from_dhan() above. Added
    the order_type param (was hardcoded "STOP_LOSS_MARKET") so a manually
    placed "STOP_LOSS" (SL-with-limit-price) order isn't silently converted
    to a different order type by this modify call."""
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "orderId": order_id,
        "orderType": order_type,
        "legName": "STOP_LOSS_LEG",
        "quantity": int(qty),
        "price": 0,
        "triggerPrice": new_trigger_price,
        "validity": "DAY",
    }
    if order_type == "STOP_LOSS":                       # ADDED 16 Nov: a stop-LIMIT order needs its limit price moved with the trigger (price 0 is refused)
        try:
            rec = get_order_record(order_id) or {}
            old_p, old_t = float(rec.get("price") or 0), float(rec.get("triggerPrice") or 0)
            tt = str(rec.get("transactionType", "")).upper()
            if old_p > 0 and old_t > 0:
                payload["price"] = _tick_round(new_trigger_price + (old_p - old_t), exchange_segment)      # keep the order's own gap between trigger and limit
            elif tt in ("SELL", "BUY"):
                b = SWING_SL_LIMIT_BUFFER_PCT / 100.0
                payload["price"] = _tick_round(new_trigger_price * (1 - b) if tt == "SELL" else new_trigger_price * (1 + b), exchange_segment)
        except Exception:
            pass
    if is_dry_run(exchange_segment):
        log_event({"action": "DRY_RUN_MODIFY_SL", "payload": payload})
        return {"orderId": order_id, "orderStatus": "SIMULATED_MODIFY"}

    resp = requests.put(f"{DHAN_BASE_URL}/orders/{order_id}", headers=dhan_headers(), json=payload, timeout=15, proxies=DHAN_PROXIES)
    ok = resp.ok
    body = safe_json(resp)
    log_event({"action": "MODIFY_SL_ORDER", "payload": payload, "http_status": resp.status_code, "response": body})
    if not ok:
        log.error(f"Dhan SL modify FAILED: {resp.status_code} {body}")
        if _is_auth_failure(resp.status_code, body):
            _note_token_check(False, f"SL modify failed with HTTP {resp.status_code}: {body}")
        if _is_ip_whitelist_failure(body):
            _log_ip_mismatch_diagnostic(f"modify_sl_order {order_id}")
    return body


# --------------------------------------------------------------------------
# THE ONLY TRADE ACTION THIS BRIDGE TAKES: a LIMIT entry, then (only once
# filled) a strict STOP_LOSS_MARKET. pending_entries tracks entries whose
# fill hasn't been confirmed yet; check_pending_fills() (run on a
# background timer) is what moves them into last_entries with an SL.
# --------------------------------------------------------------------------
pending_entries = {}   # {order_id: {symbol, qty, sl_price, security_id, segment, side}}
pending_lock = threading.Lock()
state_lock = threading.Lock()


def save_state():
    """Writes both last_entries and pending_entries to TRADE_STATE_FILE.
    Called after every mutation to either dict, so the file on disk is
    never more than one trade event stale. Best-effort - a write failure
    is logged but never blocks or fails the actual trade action that
    triggered it."""
    try:
        with state_lock:
            with open(TRADE_STATE_FILE, "w") as f:
                json.dump({"last_entries": last_entries, "pending_entries": pending_entries}, f)
    except Exception as e:
        log.warning(f"Could not save trade state to disk: {e}")


def load_state():
    """Restores last_entries/pending_entries from TRADE_STATE_FILE at
    startup, if it exists - so a restart/redeploy doesn't forget which SL
    order belongs to which open trade (breaking MOVE_SL_TO_COST) or which
    LIMIT entries were still waiting on a fill. Missing/corrupt file ->
    start empty, exactly like before this existed."""
    try:
        if os.path.exists(TRADE_STATE_FILE):
            with open(TRADE_STATE_FILE, "r") as f:
                data = json.load(f)
            last_entries.update(data.get("last_entries", {}))
            pending_entries.update(data.get("pending_entries", {}))
            log_event({"action": "TRADE_STATE_LOADED", "last_entries_count": len(last_entries),
                       "pending_entries_count": len(pending_entries)})
    except Exception as e:
        log.warning(f"Could not load trade state from disk (starting empty): {e}")


load_state()


# How many times (and how often) open_position's own synchronous fast-poll
# waits right after placing a LIMIT entry, checking whether it has filled
# yet, before handing off to the slower ~15s background poller as a backup.
# Default 10 x 2s = 20s total - long enough to catch the vast majority of
# breakout fills (which are usually near-instant once price is at the
# level), short enough that a webhook request doesn't hang for minutes.
FAST_FILL_POLL_ATTEMPTS = int(os.environ.get("FAST_FILL_POLL_ATTEMPTS", "10"))
FAST_FILL_POLL_SECONDS = float(os.environ.get("FAST_FILL_POLL_SECONDS", "2"))


def open_position(symbol: str, side: str, qty: int, sl_price, security_id: str, segment: str, entry_price,
                   near_target_price=None, far_target_price=None, lot_size=None, targets=None):
    """side: 'buy' or 'sell' (lowercase, as the Pine alert sends it).
    security_id/segment must already be resolved (see webhook()).

    REWRITTEN 2026-09-25 (real incidents this session: BHARTIARTL and
    COALINDIA filled with NO SL because SL-attachment was a SEPARATE step
    after entry, dependent on a poller that could miss a fill; a GOLDM1
    SELL had its SL placed on the wrong side of entry and Dhan silently
    executed it within 3 seconds as an accidental exit instead of ever
    acting as protection. Both incidents share one root structural cause:
    entry and SL were always two independent API calls, with a real gap
    between them where anything could go wrong.

    This now places entry + target + stop-loss as ONE Dhan Super Order
    (place_super_order) - Dhan holds all three legs from the moment this
    call succeeds and activates target/SL itself the instant entry fills.
    There is no gap, no poller, no second call that can fail or drift out
    of sync - the SL exists on Dhan's side before this function returns,
    or the whole order was never accepted at all (in which case nothing
    opened - never "opened but unprotected").

    Before placing at all: still checks price drift (ENTRY_PRICE_TOLERANCE_PCT,
    same as before), AND - new - refuses to place the order at all if
    sl_price is on the wrong side of entry_price for this trade's
    direction (the exact GOLDM1 failure mode), rather than letting Dhan's
    own validation be the only thing standing between a backwards SL and
    an accidental instant exit."""
    # DAILY LOSS LIMIT (added 2026-10-23): once the day's loss limit has been hit, no new entry is placed.
    if risk_is_locked():
        log_event({"action": "RISK_LOCK_BLOCKED_ENTRY", "symbol": symbol, "side": side,
                   "note": "daily loss limit already hit today - new entry not placed"})
        return
    if _risk_before_start(None, segment):         # ADDED 4 Nov: strict rule - no NSE entry before 09:30 IST
        log_event({"action": "RISK_BEFORE_930_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "before 09:30 IST - new entry not placed"})
        return
    if _risk_cooldown_active(security_id):        # ADDED 4 Nov: no new trade in this symbol during its re-entry cooldown
        log_event({"action": "RISK_COOLDOWN_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "re-entry cooldown running - new entry not placed"})
        return
    if _risk_acct_pause_active():                 # ADDED 4 Nov: whole-account pause after a stop-loss
        log_event({"action": "RISK_ACCOUNT_PAUSE_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "account pause running - new entry not placed"})
        return
    transaction_type = "BUY" if side == "buy" else "SELL"

    too_far, ltp, deviation_pct = price_drifted_too_far(entry_price, security_id, segment)
    if too_far:
        log_event({"action": "SKIPPED_PRICE_DRIFTED_TOO_FAR", "symbol": symbol, "signal_price": entry_price,
                   "current_ltp": ltp, "deviation_pct": round(deviation_pct, 3),
                   "tolerance_pct": ENTRY_PRICE_TOLERANCE_PCT,
                   "note": "price has already moved past the allowed tolerance - entry skipped, not chased"})
        notify("[Dhan] Entry skipped - price moved too far",
               f"{symbol} signal was at {entry_price}, current price {ltp} "
               f"({deviation_pct:.2f}% away, tolerance {ENTRY_PRICE_TOLERANCE_PCT}%). No trade taken.")
        return

    if sl_price is None:
        log_event({"action": "SKIPPED_NO_SL_PRICE", "symbol": symbol,
                   "note": "signal carried no SL price - Super Order requires one, refusing to place an unprotected order"})
        notify("[Dhan] No SL price given", f"{symbol} signal had no SL price. No trade taken (Super Order requires one).")
        return

    if (transaction_type == "SELL" and sl_price <= entry_price) or \
       (transaction_type == "BUY" and sl_price >= entry_price):
        log_event({"action": "SKIPPED_SL_WRONG_SIDE", "symbol": symbol, "side": transaction_type,
                   "entry_price": entry_price, "sl_price": sl_price,
                   "note": "sl_price is on the WRONG side of entry_price for this trade direction - "
                            "refusing to place it rather than risk Dhan accepting a backwards stop and "
                            "executing it instantly as an accidental exit (this happened live to GOLDM1)."})
        notify("[Dhan] Entry skipped - SL wrong side", f"{symbol} signal at {entry_price} carried an SL "
               f"({sl_price}) on the wrong side for a {transaction_type}. No trade taken - check the script/signal.")
        return

    if near_target_price is None and far_target_price is None:
        # Super Order requires a target - fall back to a very wide,
        # effectively-unreachable target so the trade still runs the way it
        # always has (SL/breakeven-driven, not target-driven) rather than
        # refusing the trade outright just because a target field happened
        # to be missing from this particular alert.
        risk_dist = abs(entry_price - sl_price)
        far_target_price = entry_price + (risk_dist * 50 if transaction_type == "BUY" else -risk_dist * 50)
        log_event({"action": "NO_TARGET_PROVIDED_USING_WIDE_FALLBACK", "symbol": symbol,
                   "fallback_target": far_target_price,
                   "note": "alert carried no target price - Super Order requires one, so a very wide "
                            "(50x risk) fallback was used so this behaves like an SL-only trade in practice"})

    # CASCADE TRAILING SL (ADDED 2026-09-29, Khagen's design, mirrors the
    # matching change already live on the Delta/crypto bridge): the old
    # 50/50 T2_HALF/RUNNER_HALF partial-exit split is removed. Full size
    # now stays in the trade and rides the cascading SL (T1->cost, T2->T1,
    # T3->T2, T4->T3, T5->T4) all the way out to the single far target
    # (T6, or whatever the alert's furthest available target field is - see
    # the target_price fallback chain in webhook() above). A single full-
    # size leg also sidesteps any risk of a broker-side merge/collision
    # between two same-side legs (the exact incident already hit live on
    # Delta on 2026-09-28 with its own two-leg design).
    single_target = far_target_price if far_target_price is not None else near_target_price
    legs_to_place = [("SINGLE", qty, single_target)]

    placed_legs = []
    for leg_name, leg_qty, leg_target in legs_to_place:
        entry_resp = place_super_order(security_id, transaction_type, leg_qty, "LIMIT" if entry_price else "MARKET",
                                        entry_price, leg_target, sl_price, exchange_segment=segment,
                                        correlation_id=safe_correlation_id(symbol, f"SO-{leg_name}-{int(time.time())}"))
        order_id = entry_resp.get("orderId") if isinstance(entry_resp, dict) else None
        if not order_id:
            log_event({"action": "SUPER_ORDER_ID_MISSING", "symbol": symbol, "leg": leg_name, "qty": leg_qty,
                       "response": entry_resp,
                       "note": "this leg's super order was not accepted - it was simply never opened (the "
                                "safe outcome: no order means no unprotected fill), other legs are unaffected"})
            continue
        log_event({"action": "SUPER_ORDER_PLACED", "symbol": symbol, "leg": leg_name, "order_id": order_id,
                   "qty": leg_qty, "entry_price": entry_price, "target_price": leg_target, "sl_price": sl_price,
                   "note": "entry + target + SL all placed atomically - SL is live on Dhan now, "
                            "activates automatically the instant the entry leg fills"})
        placed_legs.append({"order_id": order_id, "qty": leg_qty, "target_price": leg_target, "leg": leg_name})

    if not placed_legs:
        notify("[Dhan] Entry order failed", f"{symbol} {transaction_type} - all order legs were rejected/not "
               "accepted by Dhan. No position was opened - check Dhan and the logs.")
        return

    if len(placed_legs) < len(legs_to_place):
        notify("[Dhan] Entry PARTIALLY placed", f"{symbol} {transaction_type} - only {len(placed_legs)} of "
               f"{len(legs_to_place)} order legs were accepted by Dhan. Check your actual position size on Dhan "
               "(it will be smaller than intended, not unprotected - each placed leg still has its own SL).")

    last_entries[symbol] = {
        "side": transaction_type,
        "qty": qty,
        "legs": placed_legs,
        "security_id": security_id,
        "segment": segment,
        "entry_price": entry_price,
        "sl_price": sl_price,
        # ADDED 2026-09-30: the full T1-T6 cascade and a fresh cascade_stage=0
        # - see _handle_move_sl_to_cost()'s and _check_cascade_targets()'s
        # docstrings.
        "targets": targets,
        "cascade_stage": 0,
    }
    save_state()


def close_position_and_cancel_legs(symbol: str, security_id: str, segment: str, current_size: int):
    """FLIP SUPPORT - ADDED 2026-09-27, same fix as delta-bridge's identically
    named function (see that file's docstring for the full reasoning):
    Khagen's manual backtest counted every confirmed signal, assuming a new
    OPPOSITE-direction signal closes whatever's open and takes the new trade
    - matching the old flip-alert behavior this bridge's CONFIRMED-entry flow
    never had (it only ever skipped a new signal while already in a
    position). This is the "close the old side" half of that fix: cancels
    the current position's tracked order(s) first - a Super Order leg via
    cancel_super_order() (cancels entry+target+SL together) or a standalone
    SL order via cancel_regular_order(), whichever this symbol has on file -
    so nothing is left resting at a stale price/qty once the position it
    belonged to no longer exists, then closes the position itself with a
    real market order. Used by flip_and_open_position() below, never called
    directly from the webhook."""
    # NOTE: no lock here, matching this file's existing convention - every
    # other last_entries read/write in this file (open_position, _attach_sl,
    # check_pending_fills) also accesses it as a plain dict with no lock.
    entry = last_entries.get(symbol)
    if entry:
        for leg in entry.get("legs", []):
            cancel_super_order(leg.get("order_id"), segment)
        if entry.get("sl_order_id"):
            cancel_regular_order(entry.get("sl_order_id"), segment)

    close_side = "SELL" if current_size > 0 else "BUY"
    close_resp = place_order(security_id, close_side, abs(current_size), "MARKET", exchange_segment=segment,
                              correlation_id=safe_correlation_id(symbol, f"FLIPCLOSE-{int(time.time())}"))
    ok = isinstance(close_resp, dict) and close_resp.get("orderId") is not None
    log_event({"action": "FLIP_CLOSED_EXISTING_POSITION", "symbol": symbol,
               "closed_size": current_size, "close_side": close_side, "close_ok": ok, "response": close_resp})
    if not ok:
        notify("[Dhan] FLIP close FAILED", f"{symbol} - tried to close the existing position to flip into "
               "a new opposite signal, but the market close order failed. Check Dhan manually now - the old "
               "position may still be open and the new trade was NOT taken.")
    last_entries.pop(symbol, None)
    save_state()
    return ok


def flip_and_open_position(symbol: str, side: str, qty: int, sl_price, security_id: str, segment: str, entry_price,
                            current_size: int, near_target_price=None, far_target_price=None, lot_size=None, targets=None):
    """FLIP SUPPORT - ADDED 2026-09-27 (see close_position_and_cancel_legs()'s
    docstring for the full reasoning). Runs in the background thread
    webhook() starts for a CONFIRMED signal that arrived while an OPPOSITE-
    side position was already open. Closes the old position first; only
    opens the new one if that close actually succeeded - if it failed, this
    deliberately does NOT open the new position, to avoid ending up with
    both the old (still open, now-unprotected - its SL/target was already
    cancelled) and new positions open at once."""
    # DAILY LOSS LIMIT (added 2026-10-23): once the day's loss limit has been hit, no new entry is placed.
    if risk_is_locked():
        log_event({"action": "RISK_LOCK_BLOCKED_ENTRY", "symbol": symbol, "side": side,
                   "note": "daily loss limit already hit today - flip not done"})
        return
    if _risk_before_start(None, segment):         # ADDED 4 Nov: strict rule - no NSE entry before 09:30 IST
        log_event({"action": "RISK_BEFORE_930_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "before 09:30 IST - flip not done"})
        return
    if _risk_cooldown_active(security_id):        # ADDED 4 Nov: no new trade in this symbol during its re-entry cooldown
        log_event({"action": "RISK_COOLDOWN_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "re-entry cooldown running - flip not done"})
        return
    if _risk_acct_pause_active():                 # ADDED 4 Nov: whole-account pause after a stop-loss
        log_event({"action": "RISK_ACCOUNT_PAUSE_BLOCKED_ENTRY", "symbol": symbol, "side": side, "note": "account pause running - flip not done"})
        return
    ok = close_position_and_cancel_legs(symbol, security_id, segment, current_size)
    if not ok:
        log_event({"action": "FLIP_ABORTED_NEW_ENTRY_SKIPPED", "symbol": symbol,
                   "note": "closing the old position failed - the new signal was NOT taken, to avoid ending "
                            "up with both the old and new positions open at once. Check Dhan manually."})
        return
    open_position(symbol, side, qty, sl_price, security_id, segment, entry_price,
                  near_target_price=near_target_price, far_target_price=far_target_price, lot_size=lot_size,
                  targets=targets)


def _attach_sl(symbol, transaction_type, qty, sl_price, security_id, segment, entry_price=None):
    """Places the protective SL once an entry is confirmed filled, and
    records the trade in last_entries (used by /health and MOVE_SL_TO_COST).

    FIX 2026-09-25 (real incident: a GOLDM1 SELL filled at 151581, and the
    "SL" order placed at 151570 - BELOW entry, backwards for a sell - got
    accepted by Dhan and executed almost immediately as if it were a market
    order, closing the trade within 3 seconds instead of ever acting as a
    real stop-loss). Unlike Delta, which rejected an equivalent backwards
    stop outright, Dhan silently filled it. The root cause (the Pine script
    sometimes sending an SL on the wrong side of entry) is now fixed at the
    source, but this adds a second, independent safety net here: before
    placing the SL, verify entry_price is known and sl_price is actually on
    the protective side of it. If not, refuse to place it - exactly like the
    "no SL provided" case below - rather than risk sending Dhan a backwards
    stop it might silently execute as an accidental exit."""
    sl_order_id = None
    if sl_price is not None and entry_price is not None and (
        (transaction_type == "SELL" and sl_price <= entry_price) or
        (transaction_type == "BUY" and sl_price >= entry_price)
    ):
        log_event({"action": "SL_REJECTED_WRONG_SIDE", "symbol": symbol, "side": transaction_type,
                   "entry_price": entry_price, "sl_price": sl_price,
                   "note": "sl_price is on the WRONG side of entry_price for this trade direction - "
                            "refusing to place it (would likely be accepted and instantly executed as an "
                            "accidental exit instead of a real stop-loss). Place SL manually on Dhan."})
        notify("[Dhan] SL REJECTED - wrong side", f"{symbol} filled at {entry_price} but the SL value "
               f"{sl_price} sent by the script is on the wrong side of entry for a {transaction_type}. "
               "Bridge refused to place it rather than risk an instant accidental exit. Place SL manually now.")
    elif sl_price is not None:
        opposite = "SELL" if transaction_type == "BUY" else "BUY"
        sl_resp = place_order(security_id, opposite, qty, "STOP_LOSS_MARKET", exchange_segment=segment,
                               trigger_price=sl_price,
                               correlation_id=safe_correlation_id(symbol, f"SL-{int(time.time())}"))
        sl_order_id = sl_resp.get("orderId") if isinstance(sl_resp, dict) else None
        if sl_order_id is None:
            log_event({"action": "SL_ORDER_PLACEMENT_FAILED", "symbol": symbol, "sl_price": sl_price,
                       "response": sl_resp, "note": "entry is filled and OPEN WITHOUT PROTECTION - place SL manually"})
            notify("[Dhan] SL FAILED TO PLACE", f"{symbol} is filled with NO stop-loss on Dhan. Place it manually now at ~{sl_price}.")
    else:
        log_event({"action": "NO_SL_PROVIDED", "symbol": symbol, "note": "entry filled without a protective SL price - check manually"})
        notify("[Dhan] No SL price given", f"{symbol} filled but the signal carried no SL price. Check the position on Dhan.")

    last_entries[symbol] = {
        "side": transaction_type,
        "qty": qty,
        "sl_order_id": sl_order_id,
        "security_id": security_id,
        "segment": segment,
    }
    save_state()


def check_pending_fills():
    """Runs on a background timer. For each LIMIT entry still awaiting
    fill: if Dhan now shows it TRADED, place the SL and start tracking the
    trade; if it was cancelled/rejected/expired, drop it - no trade, no SL
    needed, exactly as intended for a limit order that never got hit."""
    with pending_lock:
        items = list(pending_entries.items())
    for order_id, info in items:
        status = get_order_status(order_id)
        if status is None:
            # get_order_status already logged the specific reason via
            # log_event (ORDER_STATUS_LOOKUP_FAILED/UNEXPECTED_BODY/ERROR).
            # Log here too so a symbol stuck with no status for many cycles
            # in a row is easy to spot by searching its order_id, instead
            # of vanishing from the logs entirely like the SILVERM SL miss.
            log_event({"action": "STATUS_CHECK_RETURNED_NONE", "symbol": info["symbol"], "order_id": order_id,
                       "note": "will retry next cycle"})
            # FALLBACK: the order-status endpoint can silently fail/return
            # an unusable body for an order that DID fill on Dhan's side -
            # exactly what happened to the SILVERM trade whose SL never got
            # placed. Cross-check via /positions (a separate, already-proven
            # endpoint) so a real fill still gets its SL attached even when
            # the status endpoint is unreliable for this specific order.
            expected_sign = 1 if info["side"] == "BUY" else -1
            net_qty = get_position_size(info["security_id"], info["segment"])
            if net_qty is not None and net_qty != 0 and (net_qty * expected_sign) > 0 and abs(net_qty) >= info["qty"]:
                log_event({"action": "ENTRY_FILLED_VIA_POSITION_FALLBACK", "symbol": info["symbol"],
                           "order_id": order_id, "net_qty": net_qty,
                           "note": "order-status check failed but /positions shows this filled - attaching SL now"})
                _attach_sl(info["symbol"], info["side"], info["qty"], info["sl_price"],
                           info["security_id"], info["segment"], info.get("entry_price"))
                with pending_lock:
                    pending_entries.pop(order_id, None)
                save_state()
            continue  # couldn't check via order-status right now - try again next cycle
        if status in FILLED_STATUSES:
            log_event({"action": "ENTRY_FILLED", "symbol": info["symbol"], "order_id": order_id, "status": status})
            _attach_sl(info["symbol"], info["side"], info["qty"], info["sl_price"],
                       info["security_id"], info["segment"], info.get("entry_price"))
            with pending_lock:
                pending_entries.pop(order_id, None)
            save_state()
        elif status in DEAD_STATUSES:
            log_event({"action": "ENTRY_NOT_FILLED", "symbol": info["symbol"], "order_id": order_id,
                       "status": status, "note": "limit order never reached its price - no trade taken"})
            with pending_lock:
                pending_entries.pop(order_id, None)
            save_state()
        else:
            # Still PENDING/TRANSIT/etc, OR Dhan returned a status string
            # that doesn't match FILLED_STATUSES/DEAD_STATUSES above (e.g.
            # a status spelling this bridge doesn't know about yet) - log it
            # every cycle rather than staying silent, so a real fill that
            # doesn't get recognized shows up here instead of the SL simply
            # never appearing with no trace of why.
            log_event({"action": "STILL_PENDING", "symbol": info["symbol"], "order_id": order_id,
                       "status": status})


def _check_super_order_entries_for_failure():
    """FIX 2026-09-28 (real incident: CRUDEOILM1! placed a valid split
    Super Order [T2_HALF+RUNNER_HALF] that Dhan accepted (SUPER_ORDER_PLACED,
    orderId returned) but the entry legs then never filled at all - 0/28 and
    0/27 filled, both showing under Dhan's own "Failed" tab, most likely
    because the MCX session closed right after the order was placed.
    open_position() writes last_entries[symbol] the moment Dhan ACCEPTS the
    Super Order, not once it actually fills - nothing anywhere later noticed
    a Super Order entry leg going terminal-failed and cleaned up the
    resulting ghost record. Left alone, that ghost record sits in
    last_entries forever: MOVE_SL_TO_COST would later try to modify a SL leg
    that was never live, fail, and send a confusing "SL move PARTIALLY
    failed" notification for a trade that was never actually open, and
    /health would misreport this symbol as an open position.

    Runs on the same timer as the legacy pending-fill poll loop. For every
    last_entries record that came from the Super Order path (has a "legs"
    list with order_id's, as opposed to _attach_sl's plain sl_order_id
    tracking), checks each leg's live order status. Only cleans up once
    EVERY leg has gone fully dead (CANCELLED/REJECTED/EXPIRED/FAILED) AND a
    live position-size check independently confirms zero - so a leg still
    genuinely PENDING/TRANSIT, or any leg that actually filled, is left
    completely alone."""
    for symbol, entry in list(last_entries.items()):
        legs = entry.get("legs")
        if not legs:
            continue  # not a Super-Order-tracked entry - not this check's job
        statuses = [get_leg_order_status(leg) for leg in legs]
        if any(s is None for s in statuses):
            continue  # couldn't confirm one or more legs right now - retry next cycle, never guess
        if not all(s in DEAD_STATUSES for s in statuses):
            continue  # at least one leg is filled or still genuinely pending - nothing to clean up

        security_id = entry.get("security_id")
        segment = entry.get("segment")
        current_size = get_position_size(security_id, segment) if security_id else None
        if current_size:
            # non-zero AND not None - a real position exists despite the
            # tracked legs showing dead, so do NOT touch last_entries; this
            # would need a manual look, not an auto-cleanup.
            log_event({"action": "SUPER_ORDER_LEGS_DEAD_BUT_POSITION_EXISTS", "symbol": symbol,
                       "statuses": statuses, "current_size": current_size,
                       "note": "all tracked legs show a terminal non-fill status but Dhan shows a real open "
                                "position for this symbol - NOT cleaning up last_entries, check manually"})
            continue

        log_event({"action": "SUPER_ORDER_ENTRY_FAILED_CLEANED_UP", "symbol": symbol, "statuses": statuses,
                   "note": "every tracked leg went to a terminal non-fill status (REJECTED/CANCELLED/EXPIRED/"
                            "FAILED) and Dhan confirms no live position for this symbol - the entry never "
                            "actually opened. Removed the ghost tracking record so MOVE_SL_TO_COST/health "
                            "don't later act on a position that doesn't exist."})
        notify("[Dhan] Entry FAILED to fill", f"{symbol} order was accepted by Dhan but never filled "
               f"(status: {', '.join(sorted(set(statuses)))}) - most likely the market session ended before "
               "it could execute. No position was opened, nothing to protect. Ghost tracking cleaned up "
               "automatically, no action needed.")
        last_entries.pop(symbol, None)
        save_state()


def _pending_fill_poll_loop():
    # Logged once immediately on thread start (before the first sleep) -
    # this is the one line that proves the background thread actually
    # started running at all, independent of whether any order is pending
    # yet. If this line is missing from the logs right after a deploy, the
    # thread never started - a code/environment problem, not a Dhan-API one.
    log_event({"action": "POLL_LOOP_STARTED", "interval_seconds": PENDING_POLL_SECONDS})
    tick = 0
    while True:
        time.sleep(PENDING_POLL_SECONDS)
        tick += 1
        try:
            with pending_lock:
                pending_count = len(pending_entries)
            # A heartbeat every ~2 minutes (every 8th tick at the default
            # 15s interval) even with nothing pending - proves the loop is
            # still alive and iterating, not just that it started once.
            if tick % 8 == 0 and not _in_log_quiet_hours():
                log_event({"action": "POLL_LOOP_ALIVE", "tick": tick, "pending_count": pending_count})
            check_pending_fills()
        except Exception as e:
            log.error(f"Error while checking pending fills: {e}")
        try:
            _check_super_order_entries_for_failure()
        except Exception as e:
            log.error(f"Error while checking super order entries for failure: {e}")


# Always runs - harmless when everything is DRY_RUN (pending_entries simply
# stays empty, since dry-run entries resolve immediately in open_position).
# This way flipping either segment live via DRY_RUN_NSE/DRY_RUN_MCX doesn't
# require touching this at all.
threading.Thread(target=_pending_fill_poll_loop, daemon=True, name="pending-fill-poller").start()


# ==========================================================================
# DAILY LOSS LIMIT (RISK GUARD) - ADDED 2026-10-23
# --------------------------------------------------------------------------
# Khagen's request: one bad manual trade wiped a month of profit, so set a
# maximum loss for the day; the bridge watches the TOTAL account P&L on Dhan
# (every position, NSE + MCX, realised + unrealised, manual trades included)
# and the moment the loss reaches the limit it
#   1. cancels every pending order (entry / target / SL, Super Orders too),
#   2. squares off every open position at MARKET,
#   3. LOCKS the day: no new bridge entry is placed, and any position opened
#      by hand afterwards is squared off again,
#   4. sends an ntfy alert (and logs RISK_* events in trade_log.jsonl).
# The lock clears itself on the next trading day (IST).
#
# Set / change the limit on the page:  /risk/<WEBHOOK_SECRET>   (no restart)
# Optional env vars (all optional):
#   RISK_DAILY_MAX_LOSS   starting limit in rupees if no saved limit exists
#                         (Render's disk is wiped on redeploy, so set this
#                         once as a safety net - the page overrides it)
#   RISK_POLL_SECONDS     how often P&L is checked (default 3)
#   RISK_WARN_PCT         early-warning notification at this % of the limit
#                         (default 80, 0 = off)
#   RISK_ALLOW_RAISE_SAME_DAY  default false: once the limit is set for a
#                         day it can only be LOWERED that day (prevents
#                         raising it in a panic). true = allow raising,
#                         which also releases a lock if the new limit is
#                         bigger than the current loss.
# Nothing here touches entry detection, SL, target or cascade logic - it
# only adds a check at the top of open_position()/flip_and_open_position()
# and its own background thread.
# ==========================================================================
def _risk_data_dir() -> str:
    """Folder for risk_state.json. A Render persistent disk (mount path /var/data) survives every redeploy;
    without a disk the app folder is used and it is wiped on each update."""
    for d in ("/var/data", "/data", "/opt/render/project/data"):
        try:
            if os.path.isdir(d) and os.access(d, os.W_OK):
                return d
        except Exception:
            pass
    return os.path.dirname(os.path.abspath(__file__))


RISK_STATE_FILE = os.path.join(_risk_data_dir(), "risk_state.json")


def _risk_env_float(name: str, default: float) -> float:
    try:
        return float(str(os.environ.get(name, default)).strip() or default)
    except (TypeError, ValueError):
        return float(default)


RISK_POLL_SECONDS = max(3.0, _risk_env_float("RISK_POLL_SECONDS", 3))   # default 3 s (was 10 s) so a fast fall is caught sooner; a Render value still overrides
RISK_WARN_PCT = _risk_env_float("RISK_WARN_PCT", 80)
RISK_DEFAULT_LIMIT = max(0.0, _risk_env_float("RISK_DAILY_MAX_LOSS", 0))
RISK_ALLOW_RAISE_SAME_DAY = False   # fixed: once the day is locked it stays locked until the next trading day (a Render env value can no longer release it)
RISK_REFLATTEN_MIN_GAP_SECONDS = 20
RISK_AUTO_DAILY_LIMIT = 8000.0   # CHANGED 16 Nov: 4 symbols x Rs 2000 risk = Rs 8000 (was 30000). ADDED 7 Nov: the daily loss limit (Rs) every new trading day starts with. It can still be LOWERED on the /risk page during the day (never raised). 0 = off (then the saved limit is kept as before).
RISK_AUTO_ON = True               # ADDED 7 Nov: the guard switches itself ON at the start of every new day (no morning click). Once ON it stays ON until the day ends, as before.

risk_lock = threading.RLock()
CL = None   # ADDED 13 Nov: capital_lock module (weekly capital-loss lock), set where the risk monitor starts
_risk_flatten_busy = False
_risk_last_flatten_ts = 0.0
risk_state = {
    "limit": RISK_DEFAULT_LIMIT,   # rupees, 0 = guard off
    "limit_date": "",              # IST date the limit was last set on the page
    "floor": None,                 # profit lock: exit if total P&L falls to this (can be 0 or +), None = off
    "floor_armed": False,          # the lock only starts protecting once P&L has gone ABOVE the floor
    "day": "",                     # IST date the fields below belong to
    "locked": False,
    "locked_at": "",
    "locked_pnl": None,
    "ticks": {},                   # ADDED: price tick learned per NSE security id (most stocks 0.05, some 0.10) - kept across days
    "kill_on": False,              # ADDED: Dhan's Kill Switch was switched on today by the bridge
    "kill_tries": 0,               # ADDED: catch-up attempts made today (max 5)
    "kill_locked": False,          # ADDED: the second activation (cannot be undone by hand today) was done
    "locked_reason": "",           # ADDED: why the day was locked ("profit floor hit" / "loss limit reached") - shown on the page
    "warned": False,
    "profit_step": 0,              # ADDED: highest day-profit level (Rs) already reminded about (10000, 15000, 20000 ...); cleared every new day
    "last_pnl": None,
    "last_poll": "",
    "poll_fail": 0,
    "blind_notified": False,
    "report": "",
    "enabled": False,              # master switch of the whole guard: OFF by default every day; once switched ON it stays ON until the day ends
    "targets_on": False,           # master switch for per-position profit targets: OFF by default every day
    "targets": {},                 # per-position profit target / max loss: securityId -> {"amount", "loss", "symbol", "fails"}
    "prot": {},                    # the ONE protective stop-loss limit order kept on Dhan per open position: securityId -> {"oid","px","qty","src"}
    "positions": [],               # last seen open positions (for the page)
    "acct_pause": {},              # ADDED 4 Nov: whole-account pause after a stop-loss: {"until","until_txt","why","existing": [securityIds open when it started]}
    "cooldown": {},                # ADDED 4 Nov: securityId -> {"symbol","why","until"(epoch),"until_txt"}: no new trade in that instrument until then (after its profit lock / stop-loss exit)
    "sl_seen": {},                 # ADDED 4 Nov: ids of filled stop-loss orders already counted as an SL hit (so an old one is never counted twice)
    "auto_lock_seen": {},          # ADDED 4 Nov: open positions that already crossed the auto profit-lock trigger (the lock is set once per open position)
    "auto_loss_seen": {},          # ADDED 4 Nov: open positions that already got (or had) their own max loss - the default stock max loss is applied once per open position
}


def _risk_today() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


_risk_seen_mtime = 0
_risk_leader = False
_risk_lock_handle = None
RISK_LEADER_FILE = os.path.join(os.path.dirname(__file__), "risk_monitor.lock")


def _risk_save():
    """Atomic write; other worker processes pick the change up via _risk_sync()."""
    global _risk_seen_mtime
    try:
        with risk_lock:
            tmp = RISK_STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(risk_state, f)
            os.replace(tmp, RISK_STATE_FILE)
            _risk_seen_mtime = os.stat(RISK_STATE_FILE).st_mtime_ns
    except Exception as e:
        log.warning(f"Could not save risk state: {e}")


def _risk_sync():
    """If the gunicorn service runs more than one worker process, each has its own
    memory. Everything that matters lives in the state file, so before every read
    this reloads it when another process has changed it."""
    global _risk_seen_mtime
    try:
        m = os.stat(RISK_STATE_FILE).st_mtime_ns
    except OSError:
        return
    if m == _risk_seen_mtime:
        return
    try:
        with open(RISK_STATE_FILE, "r") as f:
            data = json.load(f)
        with risk_lock:
            for k in risk_state:
                if k in data:
                    risk_state[k] = data[k]
            _risk_seen_mtime = m
    except Exception as e:
        log.warning(f"Could not sync risk state: {e}")


def _risk_try_become_leader() -> bool:
    """Only ONE process may run the monitor / forced exit (otherwise two workers
    could both send a market close for the same position)."""
    global _risk_leader, _risk_lock_handle
    if _risk_leader:
        return True
    try:
        import fcntl
        fh = open(RISK_LEADER_FILE, "w")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _risk_lock_handle = fh   # keep open for the life of the process
        _risk_leader = True
        log_event({"action": "RISK_MONITOR_LEADER", "pid": os.getpid()})
        return True
    except Exception:
        return False


def _risk_load():
    try:
        _marker = os.path.join(os.path.dirname(RISK_STATE_FILE), "RESET_RISK_STATE")
        if os.path.exists(_marker):      # manual safety valve: create this empty file in the disk folder, then restart, to start today with a clean risk state
            try:
                os.remove(_marker)
                if os.path.exists(RISK_STATE_FILE):
                    os.remove(RISK_STATE_FILE)
            except Exception as _e:
                log.warning(f"Risk state reset failed: {_e}")
            log_event({"action": "RISK_STATE_RESET_BY_MARKER"})
            return
        if os.path.exists(RISK_STATE_FILE):
            with open(RISK_STATE_FILE, "r") as f:
                data = json.load(f)
            with risk_lock:
                for k in risk_state:
                    if k in data:
                        risk_state[k] = data[k]
            global _risk_seen_mtime
            _risk_seen_mtime = os.stat(RISK_STATE_FILE).st_mtime_ns
            log_event({"action": "RISK_STATE_LOADED", "limit": risk_state["limit"], "day": risk_state["day"],
                       "locked": risk_state["locked"]})
    except Exception as e:
        log.warning(f"Could not load risk state (using defaults): {e}")


def _risk_roll_day():
    """New IST day -> clear the lock and the day's counters (limit stays)."""
    _risk_sync()
    today = _risk_today()
    _auto_msg = None
    with risk_lock:
        if risk_state["day"] != today:
            was_locked = risk_state["locked"]
            risk_state.update({"day": today, "floor": None, "floor_armed": False, "locked": False, "locked_at": "", "locked_pnl": None, "locked_reason": "", "kill_on": False, "kill_tries": 0, "kill_locked": False,
                               "warned": False, "profit_step": 0, "last_pnl": None, "poll_fail": 0,
                               "blind_notified": False, "report": "", "enabled": False, "targets_on": False, "targets": {}, "prot": {}, "positions": [], "auto_loss_seen": {}, "auto_lock_seen": {}, "cooldown": {}, "sl_seen": {}, "acct_pause": {}})
            if RISK_AUTO_DAILY_LIMIT > 0:          # ADDED 7 Nov: every new day starts with the fixed daily loss limit (lower-only during the day, like a limit set on the page)
                risk_state["limit"] = RISK_AUTO_DAILY_LIMIT
                risk_state["limit_date"] = today
            if RISK_AUTO_ON and risk_state["limit"] > 0:   # ADDED 7 Nov: the guard turns itself ON for the new day
                risk_state["enabled"] = True
            try:                                           # ADDED 13 Nov: a running capital-loss lock keeps every new day locked (the Kill Switch is switched on again by the monitor)
                if CL is not None and CL.is_active(today):
                    risk_state.update({"locked": True, "locked_at": "00:00:00", "locked_reason": ("capital profit floor" if CL.kind() == "floor" else "capital loss") + " lock until " + CL.until(), "enabled": True})
            except Exception as _cl_e:
                log_event({"action": "CAPITAL_LOCK_ROLL_ERROR", "error": str(_cl_e)})
            _risk_save()
            log_event({"action": "RISK_NEW_DAY_AUTO_SETUP", "day": today, "limit": risk_state["limit"], "guard_on": bool(risk_state.get("enabled"))})
            if was_locked:
                log_event({"action": "RISK_LOCK_CLEARED_NEW_DAY", "day": today})
            if risk_state.get("enabled"):                  # ADDED 7 Nov: phone message every new day when the guard switches itself ON
                _auto_msg = ("Risk guard switched ON. It stays ON until the day ends."
                             + (f" Daily loss limit Rs {risk_state['limit']:,.0f}." if risk_state.get("limit", 0) > 0 else ""))
    if _auto_msg:
        notify("[Dhan] Risk guard switched ON", _auto_msg, priority="high")


def risk_is_locked() -> bool:
    """True while today's loss limit has been hit. Used to block new entries."""
    _risk_roll_day()
    try:                                                   # ADDED 13 Nov: the capital-loss lock blocks new entries for its whole week
        if CL is not None and CL.is_active(_risk_today()):
            return True
    except Exception:
        pass
    with risk_lock:
        return bool(risk_state.get("enabled", False) and risk_state["locked"] and (risk_state["limit"] > 0 or risk_state["floor"] is not None))


def risk_status() -> dict:
    _risk_roll_day()
    with risk_lock:
        s = dict(risk_state)
    s["raise_allowed"] = RISK_ALLOW_RAISE_SAME_DAY
    s["poll_seconds"] = RISK_POLL_SECONDS
    return s


def _risk_fetch_positions():
    try:
        resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not resp.ok:
            body = safe_json(resp)
            log.warning(f"Risk monitor: position fetch failed {resp.status_code} {body}")
            if _is_auth_failure(resp.status_code, body):
                _note_token_check(False, f"risk monitor position fetch failed with HTTP {resp.status_code}")
            return None
        data = safe_json(resp)
        if not isinstance(data, list):
            return []
        if PRODUCT_TYPE != "CNC":       # ADDED 2026-11-12: delivery (swing) positions belong to /swing, not to the daily risk guard
            data = [_p for _p in data if str(_p.get("productType") or "").upper() != "CNC"]
        for _p in data:
            try:
                _risk_fix_mcx_row(_p)
            except Exception as _e:
                log.warning(f"Risk monitor: MCX P&L fix skipped: {_e}")
        return data
    except Exception as e:
        log.warning(f"Risk monitor: position fetch error: {e}")
        return None


# MCX contracts: rupees per 1 price point per lot (what Dhan's own screen uses for P&L), and the price tick.
# Used when the position row from Dhan does not carry a multiplier.
_MCX_MULT = {"CRUDEOIL": 100, "CRUDEOILM": 10, "NATURALGAS": 1250, "NATGASMINI": 250, "NATURALGASM": 250,
             "GOLD": 100, "GOLDM": 10, "GOLDGUINEA": 1, "GOLDPETAL": 1, "GOLDTEN": 1,
             "SILVER": 30, "SILVERM": 5, "SILVERMIC": 1, "SILVER100": 30,
             "COPPER": 2500, "ZINC": 5000, "ZINCMINI": 1000, "LEAD": 5000, "LEADMINI": 1000,
             "ALUMINIUM": 5000, "ALUMINI": 1000, "NICKEL": 1500, "NICKELM": 250, "MENTHAOIL": 360}
_MCX_TICK = {"CRUDEOIL": 1.0, "CRUDEOILM": 1.0, "NATURALGAS": 0.1, "NATGASMINI": 0.1, "NATURALGASM": 0.1,
             "GOLD": 1.0, "GOLDM": 1.0, "GOLDGUINEA": 1.0, "GOLDPETAL": 1.0, "GOLDTEN": 1.0,
             "SILVER": 1.0, "SILVERM": 1.0, "SILVERMIC": 1.0, "SILVER100": 1.0,
             "COPPER": 0.05, "ZINC": 0.05, "ZINCMINI": 0.05, "LEAD": 0.05, "LEADMINI": 0.05,
             "ALUMINIUM": 0.05, "ALUMINI": 0.05, "NICKEL": 0.1, "NICKELM": 0.1, "MENTHAOIL": 0.1}
_risk_raw_logged = set()


def _risk_underlying(p) -> str:
    return str(p.get("tradingSymbol") or "").split("-")[0].strip().upper()


def _risk_fix_mcx_row(p):
    """Dhan's positions list gives MCX profit without the contract multiplier (CRUDEOILM: Rs -41 where Dhan's own screen shows Rs -410).
    The profit (open and booked) of a commodity row is multiplied here so the guard, the exits and the page all see the real rupees."""
    if str(p.get("exchangeSegment", "")) != "MCX_COMM" or p.get("_mult_fixed"):
        return
    m = _risk_f(p.get("multiplier"))
    if m <= 1:
        m = float(_MCX_MULT.get(_risk_underlying(p), 1))
    sid = str(p.get("securityId"))
    if (sid, _risk_today()) not in _risk_raw_logged:
        _risk_raw_logged.add((sid, _risk_today()))
        log_event({"action": "RISK_MCX_ROW_RAW", "symbol": p.get("tradingSymbol"), "multiplier_field": p.get("multiplier"),
                   "multiplier_used": m, "netQty": p.get("netQty"), "costPrice": p.get("costPrice"),
                   "unrealizedProfit": p.get("unrealizedProfit"), "realizedProfit": p.get("realizedProfit")})
    p["_mult_fixed"] = True
    if m > 1:      # only the OPEN profit lacks the multiplier; the booked (realized) profit from Dhan is already in real rupees
        p["unrealizedProfit"] = _risk_f(p.get("unrealizedProfit")) * m
    p["multiplier"] = m


def _risk_round_dir(price: float, tick: float, up: bool) -> float:
    n = price / tick
    n = math.ceil(n - 1e-9) if up else math.floor(n + 1e-9)
    return round(n * tick, 4)


def _risk_f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _risk_net_qty(p) -> int:
    try:
        return int(float(p.get("netQty", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _risk_total_pnl(positions) -> float:
    return sum(_risk_f(p.get("realizedProfit")) + _risk_f(p.get("unrealizedProfit")) for p in positions)


def _risk_cancel_all_orders() -> int:
    """Cancels every live Super Order and every live plain order on the
    account. Returns how many cancel calls were sent. Best-effort."""
    sent = 0
    super_dead = ("CANCELLED", "REJECTED", "CLOSED", "EXPIRED", "FAILED")
    leg_dead = ("CANCELLED", "REJECTED", "TRADED", "CLOSED", "EXPIRED", "FAILED")
    try:
        r = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        orders = safe_json(r) if r.ok else []
        for o in (orders if isinstance(orders, list) else []):
            top = str(o.get("orderStatus", "")).upper()
            if top in super_dead:
                continue
            legs = o.get("legDetails") or []
            if top == "TRADED" and legs and all(
                    str(l.get("orderStatus", l.get("legStatus", ""))).upper() in leg_dead for l in legs):
                continue
            cancel_super_order(str(o.get("orderId")), o.get("exchangeSegment", "NSE_EQ"))
            sent += 1
    except Exception as e:
        log_event({"action": "RISK_CANCEL_SUPER_LIST_ERROR", "error": str(e)})
    try:
        r = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        orders = safe_json(r) if r.ok else []
        live = ("PENDING", "TRANSIT", "PART_TRADED", "TRIGGERED", "CONFIRM", "OPEN")
        for o in (orders if isinstance(orders, list) else []):
            if str(o.get("orderStatus", "")).upper() not in live:
                continue
            cancel_regular_order(str(o.get("orderId")), o.get("exchangeSegment", "NSE_EQ"))
            sent += 1
    except Exception as e:
        log_event({"action": "RISK_CANCEL_ORDERS_LIST_ERROR", "error": str(e)})
    return sent


def _risk_market_close(p) -> bool:
    qty = _risk_net_qty(p)
    if qty == 0:
        return True
    segment = p.get("exchangeSegment", "NSE_EQ")
    sym = str(p.get("tradingSymbol") or p.get("securityId") or "POS")
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "correlationId": safe_correlation_id(sym, f"RISK{int(time.time()) % 100000}"),
        "transactionType": "SELL" if qty > 0 else "BUY",
        "exchangeSegment": segment,
        "productType": p.get("productType") or PRODUCT_TYPE,
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": str(p.get("securityId")),
        "quantity": abs(qty),
        "price": 0,
    }
    if is_dry_run(segment):
        log_event({"action": "RISK_DRY_RUN_WOULD_SQUARE_OFF", "payload": payload})
        return True
    try:
        resp = requests.post(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), json=payload,
                             timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        log_event({"action": "RISK_SQUARE_OFF_ORDER", "symbol": sym, "payload": payload,
                   "http_status": resp.status_code, "response": body})
        return resp.ok
    except Exception as e:
        log_event({"action": "RISK_SQUARE_OFF_ERROR", "symbol": sym, "error": str(e)})
        return False


RISK_KILL_SWITCH_LOCK_IN = True   # ADDED 3 Nov: Dhan's own rule - a Kill Switch activated a SECOND time the same day cannot be switched off by hand until the day ends. After the first ON the bridge switches it off and on once more so you cannot undo it. False = one activation only (you can switch it off in the Dhan app)
RISK_KILL_SWITCH_ON = True    # ADDED 3 Nov: when the profit lock or the daily loss limit has closed everything, also switch ON Dhan's own Kill Switch (blocks ALL orders on the account for the rest of the day, even manual ones). False = off


def _risk_kill_call(status: str):
    resp = requests.post(f"{DHAN_BASE_URL}/killswitch", params={"killSwitchStatus": status}, headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
    body = safe_json(resp)
    log_event({"action": "RISK_KILL_SWITCH", "call": status, "http_status": resp.status_code, "response": body})
    return resp.ok, str(body)[:200]


def _risk_kill_lock_in(reason: str):
    """Dhan's rule: a Kill Switch activated a second time on the same day cannot be deactivated manually (it ends by itself at the end of the day).
    So: switch it off once and on again. If the switch-off is refused it is already locked for the day. If the second ON is refused it is tried again and,
    as a last resort, the person is told to switch it on by hand. Never raises."""
    try:
        off_ok, off_txt = _risk_kill_call("DEACTIVATE")
        if not off_ok:      # Dhan will not switch it off = it is already the locked kind
            with risk_lock:
                risk_state["kill_locked"] = True
                _risk_save()
            notify("[Dhan] KILL SWITCH ON - locked for the day", f"The {reason} closed everything and Dhan's Kill Switch is ON. It cannot be switched off by hand today; it ends by itself at the end of the day.", priority="high")
            return
        time.sleep(2)
        for n in range(1, 4):
            on_ok, on_txt = _risk_kill_call("ACTIVATE")
            if on_ok:
                with risk_lock:
                    risk_state["kill_on"] = True
                    risk_state["kill_locked"] = True
                    _risk_save()
                notify("[Dhan] KILL SWITCH ON - locked for the day", f"The {reason} closed everything. Dhan's Kill Switch was switched on twice, so it cannot be switched off by hand today: no order of any kind can be placed on the account until the day ends.", priority="high")
                return
            time.sleep(3)
        with risk_lock:
            risk_state["kill_on"] = False
            _risk_save()
        notify("[Dhan] KILL SWITCH IS OFF - SWITCH IT ON NOW", f"After the {reason} the bridge switched the Kill Switch off and could not switch it on again ({on_txt}). Switch it ON in the Dhan app now; the bridge will also keep trying.", priority="high")
    except Exception as e:
        log_event({"action": "RISK_KILL_LOCKIN_ERROR", "error": str(e)})


def _risk_kill_switch(reason: str, quiet: bool = False) -> bool:
    """Dhan's Kill Switch (POST /killswitch?killSwitchStatus=ACTIVATE). Dhan only accepts it when no position is open and no order is pending, so this is called
    only after the forced exit finished cleanly; it is tried up to 3 times (a cancelled order can take a few seconds to clear). A failure never changes the lock:
    the bridge's own lock stays on either way. Returns True when Dhan confirmed. Never raises."""
    try:
        if not RISK_KILL_SWITCH_ON or (is_dry_run("NSE_EQ") and is_dry_run("MCX_COMM")):
            return False
        last = ""
        for n in range(1, 4):
            resp = requests.post(f"{DHAN_BASE_URL}/killswitch", params={"killSwitchStatus": "ACTIVATE"}, headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
            body = safe_json(resp)
            log_event({"action": "RISK_KILL_SWITCH", "reason": reason, "attempt": n, "http_status": resp.status_code, "response": body})
            last = str(body)[:200]
            if resp.ok:                       # Dhan answers 200 with killSwitchStatus "...successfully activated"; a refusal (open position / pending order) is an HTTP error
                with risk_lock:
                    risk_state["kill_on"] = True
                    risk_state["report"] = (risk_state.get("report") or "") + " - Dhan Kill Switch ON"
                    _risk_save()
                if RISK_KILL_SWITCH_LOCK_IN and not risk_state.get("kill_locked"):
                    _risk_kill_lock_in(reason)
                else:
                    notify("[Dhan] KILL SWITCH ON", f"The {reason} closed everything, so Dhan's Kill Switch was switched ON: no order of any kind can be placed on the account for the rest of the day.", priority="high")
                return True
            time.sleep(3)
        if not quiet:
            notify("[Dhan] Kill Switch NOT switched on", f"Dhan did not accept the Kill Switch after the {reason} ({last}). The bridge's own lock is still on and blocks new entries from TradingView; manual orders on Dhan are not blocked.", priority="high")
    except Exception as e:
        log_event({"action": "RISK_KILL_SWITCH_ERROR", "error": str(e)})
    return False


_risk_kill_last = 0.0


def _risk_kill_retry():
    """Catch-up: the day is locked and nothing is open, but the Kill Switch is not on yet (the bridge was updated after the lock, or Dhan refused the first time).
    Tried at most every 2 minutes and 5 times a day; only the last failed try sends a phone alert."""
    global _risk_kill_last
    try:
        if not RISK_KILL_SWITCH_ON or int(risk_state.get("kill_tries") or 0) >= 5:
            return
        if risk_state.get("kill_on") and (risk_state.get("kill_locked") or not RISK_KILL_SWITCH_LOCK_IN):
            return                       # on, and already in the kind that cannot be switched off by hand (or lock-in is off): nothing to do
        if time.time() - _risk_kill_last < (5 if (risk_state.get("kill_on") and not risk_state.get("kill_locked")) else 120):
            return
        _risk_kill_last = time.time()
        with risk_lock:
            risk_state["kill_tries"] = int(risk_state.get("kill_tries") or 0) + 1
            n = risk_state["kill_tries"]
            _risk_save()
        _why = "profit lock" if _risk_lock_is_profit(risk_state) else ("capital profit lock" if "profit" in str(risk_state.get("locked_reason") or "") else "capital loss lock") if str(risk_state.get("locked_reason") or "").startswith("capital") else "daily loss limit"
        if risk_state.get("kill_on"):    # already switched on once (before the lock-in existed, or by hand): do the second activation now
            _risk_kill_lock_in(_why)
        else:
            _risk_kill_switch(_why, quiet=(n < 5))
    except Exception as e:
        log_event({"action": "RISK_KILL_RETRY_ERROR", "error": str(e)})


def _risk_flatten(reason: str, pnl):
    """Locks the day, cancels all orders, squares off everything at market,
    re-checks and repeats up to 3 rounds."""
    global _risk_flatten_busy, _risk_last_flatten_ts
    with risk_lock:
        if _risk_flatten_busy:
            return
        if time.time() - _risk_last_flatten_ts < RISK_REFLATTEN_MIN_GAP_SECONDS:
            return
        _risk_flatten_busy = True
        first_time = not risk_state["locked"]
        if first_time:
            risk_state["locked"] = True
            risk_state["locked_at"] = datetime.now(IST).strftime("%H:%M:%S")
            risk_state["locked_pnl"] = pnl
            risk_state["locked_reason"] = reason
        limit = risk_state["limit"]
        _risk_save()
    try:
        log_event({"action": "RISK_LIMIT_HIT" if first_time else "RISK_REFLATTEN", "reason": reason,
                   "pnl": pnl, "limit": limit})
        if first_time and reason.startswith("profit floor"):
            notify("[Dhan] PROFIT LOCK HIT",
                   f"P&L fell to Rs {pnl:,.0f} (your floor was Rs {(risk_state.get('floor') or 0):,.0f}). "
                   f"Cancelling all orders and squaring off every position at market now. "
                   f"New trades are blocked for today.")
        elif first_time and reason.startswith("capital profit"):
            notify("[Dhan] WEEKLY PROFIT FLOOR - closing everything",
                   f"Your weekly profit floor was hit (profit fell back to it). Closing all positions and cancelling all orders now to keep the rest of the week's profit. Trading is locked until {(CL.until() if CL is not None else '')} and cannot be released by hand.")
        elif first_time and reason.startswith("capital loss"):
            notify("[Dhan] CAPITAL LOSS LOCK - closing everything",
                   f"Closing all positions and cancelling all orders now. Trading is locked until {(CL.until() if CL is not None else '')} and cannot be released by hand.")
        elif first_time:
            notify("[Dhan] DAILY LOSS LIMIT HIT",
                   f"P&L Rs {pnl:,.0f} reached the limit of Rs -{limit:,.0f}. Cancelling all orders and "
                   f"squaring off every position at market now. New trades are blocked for today.")
        else:
            notify("[Dhan] Risk lock: position closed again",
                   f"A position was found open after the daily loss limit was hit ({reason}). Squaring it "
                   f"off again. Trading stays blocked for today.")
        remaining = []
        for rnd in range(1, 4):
            cancelled = _risk_cancel_all_orders()
            positions = _risk_fetch_positions()
            if positions is None:
                time.sleep(2)
                continue
            open_pos = [p for p in positions if _risk_net_qty(p) != 0]
            log_event({"action": "RISK_FLATTEN_ROUND", "round": rnd, "orders_cancel_sent": cancelled,
                       "open_positions": len(open_pos)})
            if not open_pos:
                remaining = []
                break
            for p in open_pos:
                _risk_market_close(p)
            time.sleep(3)
        final = _risk_fetch_positions()
        if final is None:
            remaining = ["(could not re-check - look at Dhan now)"]
        else:
            remaining = [str(p.get("tradingSymbol") or p.get("securityId")) for p in final
                         if _risk_net_qty(p) != 0 and not is_dry_run(p.get("exchangeSegment", "NSE_EQ"))]
            pnl_final = _risk_total_pnl(final)
            with risk_lock:
                risk_state["last_pnl"] = pnl_final
        # forget the bridge's own SL/pending tracking - everything is cancelled
        try:
            with state_lock:
                last_entries.clear()
            with pending_lock:
                pending_entries.clear()
            save_state()
        except Exception as e:
            log.warning(f"Risk flatten: could not clear tracking state: {e}")
        with risk_lock:
            _risk_last_flatten_ts = time.time()
            if remaining:
                risk_state["report"] = "STILL OPEN: " + ", ".join(remaining)
            else:
                risk_state["report"] = "All positions closed at " + datetime.now(IST).strftime("%H:%M:%S")
            _risk_save()
        if remaining:
            log_event({"action": "RISK_FLATTEN_INCOMPLETE", "still_open": remaining})
            notify("[Dhan] RISK EXIT INCOMPLETE - ACT NOW",
                   "Still open after the forced exit: " + ", ".join(remaining) +
                   ". Close them on Dhan manually immediately.")
        else:
            log_event({"action": "RISK_FLATTEN_DONE", "pnl": risk_state.get("last_pnl")})
            if first_time:
                notify("[Dhan] Risk exit complete",
                       f"All positions closed and orders cancelled. Day P&L about Rs "
                       f"{(risk_state.get('last_pnl') or 0):,.0f}. No new trades today.", priority="high")
                _risk_kill_switch("profit lock" if reason.startswith("profit floor") else ("capital profit lock" if reason.startswith("capital profit") else "capital loss lock") if reason.startswith("capital") else "daily loss limit")
    finally:
        with risk_lock:
            _risk_last_flatten_ts = time.time()
            _risk_flatten_busy = False


def _risk_cancel_orders_for(sec_id: str) -> int:
    """Cancels the live Super Orders and plain orders of ONE security only (used when a per-position
    profit target is hit). Returns how many cancel calls were sent. Best-effort."""
    sent = 0
    sec_id = str(sec_id)
    super_dead = ("CANCELLED", "REJECTED", "CLOSED", "EXPIRED", "FAILED")
    leg_dead = ("CANCELLED", "REJECTED", "TRADED", "CLOSED", "EXPIRED", "FAILED")
    try:
        r = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        orders = safe_json(r) if r.ok else []
        for o in (orders if isinstance(orders, list) else []):
            if str(o.get("securityId")) != sec_id:
                continue
            top = str(o.get("orderStatus", "")).upper()
            if top in super_dead:
                continue
            legs = o.get("legDetails") or []
            if top == "TRADED" and legs and all(
                    str(l.get("orderStatus", l.get("legStatus", ""))).upper() in leg_dead for l in legs):
                continue
            cancel_super_order(str(o.get("orderId")), o.get("exchangeSegment", "NSE_EQ"))
            sent += 1
    except Exception as e:
        log_event({"action": "RISK_TARGET_CANCEL_SUPER_ERROR", "error": str(e)})
    try:
        r = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        orders = safe_json(r) if r.ok else []
        live = ("PENDING", "TRANSIT", "PART_TRADED", "TRIGGERED", "CONFIRM", "OPEN")
        for o in (orders if isinstance(orders, list) else []):
            if str(o.get("securityId")) != sec_id:
                continue
            if str(o.get("orderStatus", "")).upper() not in live:
                continue
            cancel_regular_order(str(o.get("orderId")), o.get("exchangeSegment", "NSE_EQ"))
            sent += 1
    except Exception as e:
        log_event({"action": "RISK_TARGET_CANCEL_ORDERS_ERROR", "error": str(e)})
    return sent


RISK_LOCK_LIMIT_BUFFER_PCT = 0.3     # the Dhan order's limit price sits this % beyond its trigger so it fills even when the price moves fast
_risk_prot_try = {}


_RISK_TICK_LADDER = [0.05, 0.10, 0.25, 0.50, 1.00]


def _risk_nse_tick(key) -> float:
    with risk_lock:
        return _risk_f((risk_state.get("ticks") or {}).get(str(key))) or NSE_EQ_TICK


def _risk_nse_round(price, key) -> float:
    """Round to the stock's own tick (0.05 unless Dhan has refused 0.05 for it before - see _risk_learn_tick)."""
    t = _risk_nse_tick(key)
    return round(round(float(price) / t) * t, 2)


def _risk_learn_tick(key, seg, info, sym) -> bool:
    """Real incident 2026-11-03 (TATACONSUM): Dhan refused every protective stop with "EXCH:16283 The order price is not multiple of the tick size" because
    this stock trades in 0.10 steps, not 0.05. When that answer comes back the next larger tick is saved for the stock and the order is sent again at the
    next check (a few seconds later); the learned tick is kept on the disk."""
    try:
        if seg != "NSE_EQ" or "tick size" not in str(info).lower():
            return False
        cur = _risk_nse_tick(key)
        nxt = next((t for t in _RISK_TICK_LADDER if t > cur + 1e-9), None)
        if not nxt:
            return False
        with risk_lock:
            tk = dict(risk_state.get("ticks") or {})
            tk[str(key)] = nxt
            risk_state["ticks"] = tk
            _risk_save()
        for k in [k for k in list(_risk_prot_try) if isinstance(k, tuple) and k and k[0] == str(key)]:
            _risk_prot_try.pop(k, None)
        log_event({"action": "RISK_TICK_LEARNED", "symbol": sym, "security_id": str(key), "old_tick": cur, "new_tick": nxt})
        return True
    except Exception:
        return False


def _risk_prot_price(p, target_pnl: float):
    """Exit prices for ONE position so that its own P&L equals target_pnl (rupees): (side, trigger, limit) or None.
    Long: trigger = entry + P&L / (qty x multiplier) and the order is a SELL; short: trigger = entry - P&L / (qty x multiplier), BUY."""
    qty = _risk_net_qty(p)
    if qty == 0:
        return None
    cost = _risk_f(p.get("costPrice") or (p.get("buyAvg") if qty > 0 else p.get("sellAvg")))
    if cost <= 0:
        return None
    mult = _risk_f(p.get("multiplier")) or 1.0
    seg = p.get("exchangeSegment", "NSE_EQ")
    move = target_pnl / (abs(qty) * mult)
    if seg == "MCX_COMM":
        tick = _MCX_TICK.get(_risk_underlying(p)) or (1.0 if cost >= 100 else 0.05)
        if qty > 0:      # long: SELL stop below the market, rounded UP (toward the market = the safer side)
            trig = _risk_round_dir(cost + move, tick, True)
            lim = _risk_round_dir(trig * (1 - RISK_LOCK_LIMIT_BUFFER_PCT / 100.0), tick, False)
            side = "SELL"
        else:            # short: BUY stop above the market, rounded DOWN
            trig = _risk_round_dir(cost - move, tick, False)
            lim = _risk_round_dir(trig * (1 + RISK_LOCK_LIMIT_BUFFER_PCT / 100.0), tick, True)
            side = "BUY"
    elif qty > 0:
        _k = str(p.get("securityId"))
        trig = _risk_nse_round(cost + move, _k)
        lim = _risk_nse_round(trig * (1 - RISK_LOCK_LIMIT_BUFFER_PCT / 100.0), _k)
        side = "SELL"
    else:
        _k = str(p.get("securityId"))
        trig = _risk_nse_round(cost - move, _k)
        lim = _risk_nse_round(trig * (1 + RISK_LOCK_LIMIT_BUFFER_PCT / 100.0), _k)
        side = "BUY"
    if trig <= 0 or lim <= 0:
        return None
    if abs(trig - cost) / cost > 0.20:      # FIX: an exit level that far from the entry (for example a day profit lock on a 1-share position) is outside Dhan's price band and is always refused - the bridge's own exit at market handles it, so no order and no failed rows
        return None
    return side, round(trig, 2), round(lim, 2)


def _risk_prot_best(p, positions):
    """Of the exits that apply to this position (its profit lock, its max loss, the daily loss limit, the total profit lock)
    returns the one nearest to the market as (name, position_pnl_level), or None. Only ONE order is kept on Dhan per position
    (the nearest one) so two waiting orders can never both fire and reverse the position. The others stay as the bridge's own market exits."""
    key = str(p.get("securityId"))
    with risk_lock:
        t = dict((risk_state.get("targets") or {}).get(key) or {})
        enabled = bool(risk_state.get("enabled", False))
        limit = _risk_f(risk_state.get("limit"))
        floor = risk_state.get("floor")
        floor_armed = bool(risk_state.get("floor_armed"))
        locked = bool(risk_state.get("locked"))
    pnl_pos = _risk_f(p.get("unrealizedProfit"))
    realized = sum(_risk_f(q.get("realizedProfit")) for q in positions)
    others = sum(_risk_f(q.get("unrealizedProfit")) for q in positions if str(q.get("securityId")) != key)
    cands = []
    if _risk_f(t.get("lock")) > 0:
        cands.append(("lock", _risk_f(t.get("lock"))))
    if _risk_f(t.get("loss")) > 0:
        cands.append(("loss", -_risk_f(t.get("loss"))))
    if enabled and not locked:
        if limit > 0:
            cands.append(("limit", -limit - realized - others))
        if floor is not None and floor_armed:
            cands.append(("floor", _risk_f(floor) - realized - others))
    cands = [c for c in cands if c[1] < pnl_pos]
    return max(cands, key=lambda c: c[1]) if cands else None


_risk_prot_mutex = threading.Lock()


def _risk_prot_sync(positions, force: bool = False):
    """FIX 2 Nov: the background monitor and the risk page (and every gunicorn worker) could run this at the same moment, so the same stop order
    was sent twice. Dhan's margin check then refused the second one ("insufficient funds") and the bridge recorded the refused order instead of the
    live one - so the next change could not cancel the live stop. Now only ONE run at a time (a lock inside this process plus a file lock between workers)."""
    if not _risk_prot_mutex.acquire(timeout=(25 if force else 1)):
        return []
    _fh = None
    try:
        try:
            import fcntl
            _fh = open(os.path.join(os.path.dirname(RISK_STATE_FILE), "risk_prot.lock"), "w")
            _end = time.time() + (25 if force else 1)
            while True:
                try:
                    fcntl.flock(_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.time() >= _end:
                        return []
                    time.sleep(0.2)
        except ImportError:
            pass
        return _risk_prot_sync_run(positions, force)
    finally:
        try:
            if _fh:
                _fh.close()
        except Exception:
            pass
        _risk_prot_mutex.release()


def _risk_prot_sync_run(positions, force: bool = False):
    """Keeps exactly one stop-loss LIMIT order on Dhan for each open position, at the price worked out from its quantity and
    entry price. Called every poll (local arithmetic only) and whenever a lock / limit / max loss is set or changed; Dhan is
    contacted only when the price or quantity changed (old order cancelled, new one placed). Returns a list of short messages."""
    msgs = []
    if not positions:
        return msgs
    open_pos = [p for p in positions if _risk_net_qty(p) != 0]
    for p in open_pos:
        key = str(p.get("securityId"))
        sym = str(p.get("tradingSymbol") or key)
        seg = p.get("exchangeSegment", "NSE_EQ")
        qty = abs(_risk_net_qty(p))
        best = _risk_prot_best(p, positions)
        with risk_lock:
            cur = dict((risk_state.get("prot") or {}).get(key) or {})
        if best is None:
            if cur:
                _risk_prot_drop(key, cur, seg)
            continue
        src, level = best
        prices = _risk_prot_price(p, level)
        if not prices:
            continue
        side, trig, lim = prices
        label = {"lock": "profit lock", "loss": "max loss", "limit": "daily loss limit", "floor": "total profit lock"}[src]   # FIX: was defined further down but used earlier (error "cannot access local variable 'label'")
        if cur.get("oid") and cur.get("px") == trig and cur.get("qty") == qty:
            continue
        tk = (key, trig, qty)
        if not force and time.time() - _risk_prot_try.get(tk, 0) < 120:
            continue
        sk = (key, trig, qty, "skip")
        if not force and time.time() - _risk_prot_try.get(sk, 0) < 20:
            continue
        # ask Dhan whether another stop order (for example the trade's own SL from TradingView) already protects this position
        # nearer to the market than ours: then ours would only be a second order waiting to fire, so it is not placed
        try:
            _others = collect_live_sl_orders(key, seg) or []
        except Exception:
            _others = []
        _mine = str(cur.get("oid") or "")
        _trigs = []
        for _o in _others:
            if str(_o.get("order_id")) == _mine:
                continue
            try:
                _trigs.append(float(_o.get("trigger")))
            except (TypeError, ValueError):
                pass
        if _trigs and ((side == "SELL" and max(_trigs) >= trig) or (side == "BUY" and min(_trigs) <= trig)):
            _risk_prot_try[sk] = time.time()
            if cur:
                _risk_prot_drop(key, cur, seg)
            log_event({"action": "RISK_PROT_SKIPPED_OTHER_ORDER", "symbol": sym, "ours": trig, "other_triggers": _trigs})
            msgs.append(f"{sym}: another stop order on Dhan (trigger {max(_trigs) if side == 'SELL' else min(_trigs):,.2f}) is already nearer, so no second order was placed.")
            if (key, trig, qty, "skipnote") not in _risk_prot_try:
                _risk_prot_try[(key, trig, qty, "skipnote")] = time.time()
                notify(f"[Dhan] No extra exit order - {sym}",
                       f"Another stop order on Dhan (trigger {max(_trigs) if side == 'SELL' else min(_trigs):,.2f}) is already nearer than the {label} price {trig:,.2f}.")
            continue
        _risk_prot_try[tk] = time.time()
        _old_oid = str(cur.get("oid") or "")
        if cur:
            _risk_prot_drop(key, cur, seg)
            _risk_wait_cancelled(_old_oid)
        _risk_cancel_stale_prot(key, seg)       # FIX: also remove any earlier bridge stop (correlation id "...-PROT...") the bridge lost track of, or it would double the buy side and Dhan refuses the new one
        payload = {
            "dhanClientId": DHAN_CLIENT_ID,
            "correlationId": safe_correlation_id(sym, f"PROT{int(time.time()) % 100000}"),
            "transactionType": side,
            "exchangeSegment": seg,
            "productType": p.get("productType") or PRODUCT_TYPE,
            "orderType": "STOP_LOSS",
            "validity": "DAY",
            "securityId": key,
            "quantity": qty,
            "price": lim,
            "triggerPrice": trig,
        }
        label = {"lock": "profit lock", "loss": "max loss", "limit": "daily loss limit", "floor": "total profit lock"}[src]
        if is_dry_run(seg):
            log_event({"action": "RISK_PROT_ORDER_DRY_RUN", "payload": payload, "src": src})
            msgs.append(f"{sym}: {label} exit price {trig:,.2f} (dry run - no order sent).")
            continue
        try:
            for _try_no in range(3):            # FIX: if Dhan's risk check still answers "insufficient funds" / RMS (the old stop is still being cancelled), wait and send again
                resp = requests.post(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), json=payload, timeout=15, proxies=DHAN_PROXIES)
                body = safe_json(resp)
                log_event({"action": "RISK_PROT_ORDER", "symbol": sym, "src": src, "payload": payload,
                           "http_status": resp.status_code, "response": body, "attempt": _try_no + 1})
                _txt = str(body).lower()
                if resp.ok or _try_no == 2 or not any(w in _txt for w in ("insufficient", "rms", "margin", "funds")):
                    break
                time.sleep(3)
            oid = str((body or {}).get("orderId") or "") if isinstance(body, dict) else ""
            _tick_fixed = False
            if not (resp.ok and oid):
                _tick_fixed = _risk_learn_tick(key, seg, body, sym)
            if resp.ok and oid:                 # FIX: Dhan answers 200 / TRANSIT even for an order its risk check refuses a moment later - look again before trusting it
                time.sleep(2)
                _rec = get_order_record(oid)
                if str((_rec or {}).get("orderStatus", "")).upper() in ("REJECTED", "CANCELLED", "FAILED"):
                    log_event({"action": "RISK_PROT_ORDER_REFUSED_AFTER_ACK", "symbol": sym, "order_id": oid, "record": _rec})
                    _tick_fixed = _risk_learn_tick(key, seg, _rec, sym) or _tick_fixed
                    oid = ""
            if resp.ok and oid:
                with risk_lock:
                    pr = risk_state.get("prot") or {}
                    pr[key] = {"oid": oid, "px": trig, "qty": qty, "src": src, "seg": seg, "symbol": sym}
                    risk_state["prot"] = pr
                    _risk_save()
                msgs.append(f"{sym}: Dhan order placed for the {label} - {side} {qty}, trigger {trig:,.2f}, limit {lim:,.2f}.")
                notify(f"[Dhan] Exit order placed - {sym}",
                       f"{label.capitalize()}: {side} {qty} stop-loss limit placed on Dhan. Trigger {trig:,.2f}, limit {lim:,.2f}.", priority="high")
            elif _tick_fixed:
                msgs.append(f"{sym}: Dhan refused the price tick - the stock's tick size was learned and the {label} order is sent again at the next check.")
            else:
                msgs.append(f"{sym}: Dhan did not accept the {label} order ({body}). The bridge's own exit at market still protects it.")
                if (key, trig, qty, "failnote") not in _risk_prot_try:
                    _risk_prot_try[(key, trig, qty, "failnote")] = time.time()
                    notify(f"[Dhan] Exit order REJECTED - {sym}",
                           f"Dhan did not accept the {label} order (trigger {trig:,.2f}). The bridge's own market exit still protects it. Check the Risk page.", priority="high")
        except Exception as e:
            log_event({"action": "RISK_PROT_ORDER_ERROR", "symbol": sym, "error": str(e)})
            msgs.append(f"{sym}: could not send the {label} order to Dhan. The bridge's own exit at market still protects it.")
    return msgs


def _risk_cancel_stale_prot(key: str, seg: str = "NSE_EQ"):
    """Cancels live stop-loss orders of THIS security that the bridge itself placed earlier (correlation id contains "-PROT") and waits until Dhan
    shows them cancelled. Orders from TradingView or by hand are never touched. Never raises."""
    try:
        r = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        orders = safe_json(r) if r.ok else []
        live = ("PENDING", "TRANSIT", "PART_TRADED", "TRIGGERED", "CONFIRM", "OPEN")
        ids = []
        for o in (orders if isinstance(orders, list) else []):
            if str(o.get("securityId")) != str(key) or str(o.get("orderStatus", "")).upper() not in live:
                continue
            if "-PROT" not in str(o.get("correlationId", "")):
                continue
            ids.append(str(o.get("orderId")))
        for oid in ids:
            cancel_regular_order(oid, seg)
            log_event({"action": "RISK_PROT_STALE_CANCELLED", "security_id": key, "order_id": oid})
        for oid in ids:
            _risk_wait_cancelled(oid)
    except Exception as e:
        log_event({"action": "RISK_PROT_STALE_ERROR", "security_id": key, "error": str(e)})


def _risk_wait_cancelled(oid: str, tries: int = 6, delay: float = 1.0):
    """Waits (up to about 6 seconds) until Dhan shows the given order as cancelled / finished. Never raises."""
    if not oid or oid == "DRY_RUN":
        return
    for _ in range(tries):
        try:
            rec = get_order_record(oid)
            st = str((rec or {}).get("orderStatus", "")).upper()
            if st in ("CANCELLED", "REJECTED", "TRADED", "EXPIRED", "CLOSED", "FAILED"):
                return
        except Exception:
            return
        time.sleep(delay)


def _risk_prot_drop(key: str, cur: dict, seg: str = "NSE_EQ"):
    """Cancels the position's protective order on Dhan and forgets it."""
    try:
        if cur.get("oid"):
            cancel_regular_order(str(cur.get("oid")), cur.get("seg") or seg)
    except Exception as e:
        log_event({"action": "RISK_PROT_CANCEL_ERROR", "security_id": key, "error": str(e)})
    with risk_lock:
        pr = risk_state.get("prot") or {}
        pr.pop(key, None)
        risk_state["prot"] = pr
        _risk_save()


def _risk_prot_closed(live_keys, pnl):
    """A position with a protective order is no longer open. Filled on Dhan -> clean up leftovers (and lock the day when it was
    the daily loss limit / total profit lock). Anything else (closed by hand) -> just cancel our own order. Returns True when the day was locked."""
    with risk_lock:
        gone = [(k, dict(v)) for k, v in (risk_state.get("prot") or {}).items() if k not in live_keys]
    locked_day = False
    for k, v in gone:
        sym = v.get("symbol") or k
        rec = get_order_record(str(v.get("oid"))) if v.get("oid") else None
        status = str((rec or {}).get("orderStatus", "")).upper()
        with risk_lock:
            pr = risk_state.get("prot") or {}
            pr.pop(k, None)
            risk_state["prot"] = pr
            _risk_save()
        if status == "TRADED":
            n = _risk_cancel_orders_for(k)
            log_event({"action": "RISK_PROT_FILLED", "security_id": k, "src": v.get("src"), "orders_cancel_sent": n})
            notify(f"[Dhan] Exit order filled - {sym}", f"{sym} was closed by your {v.get('src')} order on Dhan. Its other waiting orders were cancelled.", priority="high")
            if v.get("src") in ("lock", "loss"):
                _risk_cooldown_start(k, sym, "profit lock" if v.get("src") == "lock" else "max loss (stop-loss)")        # ADDED 4 Nov
            if v.get("src") in ("limit", "floor") and not risk_state.get("locked"):
                _risk_flatten("loss limit reached" if v.get("src") == "limit" else "profit floor hit", pnl)
                locked_day = True
        else:
            cancel_regular_order(str(v.get("oid")), v.get("seg") or "NSE_EQ")
            log_event({"action": "RISK_PROT_CANCELLED_POSITION_CLOSED", "security_id": k, "status": status})
    return locked_day


def _risk_book_profit(p, target: dict, pnl_pos: float, kind: str = "profit") -> bool:
    """A position reached its rupee profit target (or its rupee max loss): cancel ITS orders, square it off at market.
    Does NOT lock the day and does not touch any other position. Returns True when done."""
    sec = str(p.get("securityId"))
    sym = str(p.get("tradingSymbol") or sec)
    amount = target.get("amount", 0)
    loss_amt = target.get("loss", 0)
    lock_amt = target.get("lock", 0)
    log_event({"action": "RISK_TARGET_HIT" if kind == "profit" else ("RISK_PROFIT_LOCK_HIT" if kind == "lock" else "RISK_POSITION_LOSS_HIT"), "symbol": sym,
               "security_id": sec, "pnl_position": pnl_pos, "target": amount, "loss": loss_amt, "lock": lock_amt})
    cancelled = _risk_cancel_orders_for(sec)
    _fr = _risk_fetch_positions()
    if _fr is not None:
        _row = next((q for q in _fr if str(q.get("securityId")) == sec), None)
        if _row is None or _risk_net_qty(_row) == 0:
            p = dict(p, netQty=0)      # already closed (for example by the order on Dhan) - never sell again
        else:
            p = _row
    ok = _risk_market_close(p)
    time.sleep(2)
    fresh = _risk_fetch_positions()
    still_open = False
    if fresh is not None:
        for q in fresh:
            if str(q.get("securityId")) == sec and _risk_net_qty(q) != 0 and not is_dry_run(q.get("exchangeSegment", "NSE_EQ")):
                still_open = True
    if ok and not still_open:
        try:
            with state_lock:
                for k, e in list(last_entries.items()):
                    if str(e.get("security_id")) == sec:
                        last_entries.pop(k, None)
            save_state()
        except Exception as e:
            log.warning(f"Profit target: could not clear tracking state: {e}")
        log_event({"action": "RISK_TARGET_DONE", "symbol": sym, "orders_cancel_sent": cancelled})
        if kind in ("lock", "loss"):
            _risk_cooldown_start(sec, sym, "profit lock" if kind == "lock" else "max loss (stop-loss)")        # ADDED 4 Nov
        if kind == "profit":
            notify(f"[Dhan] PROFIT TARGET HIT - {sym}",
                   f"{sym} reached your target of Rs {amount:,.0f} (open profit about Rs {pnl_pos:,.0f}). "
                   f"Its orders were cancelled and the position was closed at market. Other positions are untouched.",
                   priority="high")
        elif kind == "lock":
            notify(f"[Dhan] PROFIT LOCK HIT - {sym}",
                   f"{sym} profit fell back to your locked Rs {lock_amt:,.0f} (open profit about Rs {pnl_pos:,.0f}). "
                   f"Its orders were cancelled and the position was closed at market. Other positions are untouched.",
                   priority="high")
        else:
            notify(f"[Dhan] POSITION LOSS LIMIT HIT - {sym}",
                   f"{sym} reached your max loss of Rs {loss_amt:,.0f} (open P&L about Rs {pnl_pos:,.0f}). "
                   f"Its orders were cancelled and the position was closed at market. Other positions are untouched.",
                   priority="high")
        return True
    return False


def _risk_process_targets(open_pos, only_lock: bool = False):
    """Checks every open position against its own rupee profit target / profit lock / max loss.
    The three per-position exits work whether the guard is ON or OFF. (only_lock is kept for compatibility, not used.)"""
    with risk_lock:
        targets = {k: dict(v) for k, v in (risk_state.get("targets") or {}).items()}
    if not targets:
        return
    for p in open_pos:
        key = str(p.get("securityId"))
        t = targets.get(key)
        if not t:
            continue
        amount = _risk_f(t.get("amount"))
        loss_amt = _risk_f(t.get("loss"))
        lock_amt = _risk_f(t.get("lock"))
        pnl_pos = _risk_f(p.get("unrealizedProfit"))
        if only_lock:
            amount = 0.0
            loss_amt = 0.0
        if amount > 0 and pnl_pos >= amount:
            kind = "profit"
        elif lock_amt > 0 and pnl_pos <= lock_amt:
            kind = "lock"
        elif loss_amt > 0 and pnl_pos <= -loss_amt:
            kind = "loss"
        else:
            continue
        done = _risk_book_profit(p, t, pnl_pos, kind)
        with risk_lock:
            cur = risk_state.get("targets") or {}
            if done:
                cur.pop(key, None)
            else:
                fails = int((cur.get(key) or {}).get("fails", 0)) + 1
                if key in cur:
                    cur[key]["fails"] = fails
                if fails == 1:
                    notify(f"[Dhan] Position exit - close FAILED - {t.get('symbol') or key}",
                           "Profit target / profit lock / max loss reached but the close did not go through. Retrying. Check Dhan.", priority="high")
                if fails >= 5:
                    cur.pop(key, None)
                    notify(f"[Dhan] Position exit - GIVE UP - {t.get('symbol') or key}",
                           "Could not close this position after 5 tries. Close it on Dhan manually NOW.", priority="high")
            risk_state["targets"] = cur
            _risk_save()


RISK_NO_TRADE_BEFORE_MIN = 9 * 60 + 30   # ADDED 4 Nov (changed to 09:30 on 15 Nov): STRICT RULE - no NSE trade before 09:30 IST (the first 5-min candle closes 09:20). A position opened in the NSE segments before this time is closed at market within seconds (and the bridge places no new entry). 0 = off. MCX is not affected.
_risk_early_fails = {}
_risk_early_notified = set()


def _risk_before_start(now=None, segment: str = "NSE_EQ") -> bool:
    """ADDED 4 Nov: True on a weekday before 09:30 IST for an NSE segment (MCX is not covered)."""
    if RISK_NO_TRADE_BEFORE_MIN <= 0 or str(segment).upper().startswith("MCX"):
        return False
    now = now or datetime.now(IST)
    return now.weekday() < 5 and now.hour * 60 + now.minute < RISK_NO_TRADE_BEFORE_MIN


def _risk_early_exit(open_pos, now):
    """ADDED 4 Nov: before 09:30 IST any open NSE position (opened by hand or any other way) is closed at market and its waiting orders are cancelled.
    Does not lock the day and does not touch MCX positions. Retries every poll; gives up (with an alert) after 5 failed closes. Never raises."""
    try:
        if not _risk_before_start(now):
            return
        for p in open_pos:
            seg = str(p.get("exchangeSegment", "NSE_EQ"))
            if str(seg).upper().startswith("MCX") or is_dry_run(seg):
                continue
            sec = str(p.get("securityId"))
            sym = str(p.get("tradingSymbol") or sec)
            if _risk_early_fails.get(sec, 0) >= 5:
                continue
            log_event({"action": "RISK_EARLY_TRADE_EXIT", "symbol": sym, "security_id": sec, "qty": _risk_net_qty(p),
                       "note": f"position open before {RISK_NO_TRADE_BEFORE_MIN // 60}:{RISK_NO_TRADE_BEFORE_MIN % 60:02d} IST - closing at market"})
            _risk_cancel_orders_for(sec)
            ok = _risk_market_close(p)
            if ok:
                try:
                    with state_lock:
                        for k, e in list(last_entries.items()):
                            if str(e.get("security_id")) == sec:
                                last_entries.pop(k, None)
                    save_state()
                except Exception:
                    pass
                if sec not in _risk_early_notified:
                    _risk_early_notified.add(sec)
                    notify(f"[Dhan] Trade before 9:30 - {sym} closed",
                           f"{sym} was opened before 09:30 IST. Your rule: no trading before 9:30, so the bridge closed it at market.", priority="high")
            else:
                _risk_early_fails[sec] = _risk_early_fails.get(sec, 0) + 1
                if _risk_early_fails[sec] >= 5:
                    notify(f"[Dhan] Trade before 9:30 - close FAILED - {sym}",
                           f"{sym} is open before 09:30 and the bridge could not close it after 5 tries. Close it on Dhan NOW.", priority="urgent")
    except Exception as e:
        log_event({"action": "RISK_EARLY_EXIT_ERROR", "error": str(e)})


RISK_AUTO_LOCK_TRIGGER = 10000.0   # ADDED 4 Nov: when ANY open position's profit crosses this (Rs) ...
RISK_AUTO_LOCK_AMOUNT = 1000.0     # ... the bridge sets that position's own "Lock profit" to this (Rs): it is closed at market if its profit falls back to it (and the exit order is placed on Dhan). 0 = off. Not a trailing lock - raise it by hand on the /risk page; a lock you set yourself is never overwritten.
RISK_REENTRY_COOLDOWN_MIN = 45     # ADDED 4 Nov: 3 candles of 15 minutes, the candle of the exit counting as the FIRST: the same instrument is blocked until the 3rd candle closes (exit anywhere in 11:00-11:15 -> until 11:45). So 30-45 minutes. After a position is closed by its PROFIT LOCK or by a STOP-LOSS (the position max loss, a stop order on Dhan, a Super Order stop-loss leg), a position opened in that instrument before then is closed at market within seconds and the bridge places no entry. 0 = off. Cleared every new day.
RISK_ACCOUNT_PAUSE_MIN = 30        # ADDED 4 Nov: after a STOP-LOSS exit (position max loss / stop order) the WHOLE account is paused for 2 candles of 15 minutes, the candle in which the stop-loss happened counting as the FIRST: the pause ends when the 2nd candle closes (stop-loss anywhere in 11:00-11:15 -> until 11:30; in 11:15-11:30 -> until 11:45). So 15-30 minutes. A position opened in ANY symbol in that time is closed at market (positions that were already open stay). 0 = off. Profit-lock exits do not start it.


def _risk_candle_until(now, minutes):
    """ADDED 4 Nov: the close of the (minutes / 15)th 15-minute candle counting the candle that contains `now` as the first, as a datetime
    (minutes = 30 -> the close of the next candle, 45 -> the close of the one after that)."""
    start = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return start + timedelta(minutes=15 * max(1, int(round(minutes / 15.0))))


def _risk_pause_until(now):
    """ADDED 4 Nov: end of the whole-account pause = the close of the 2nd 15-minute candle, the stop-loss candle counting as the first (see RISK_ACCOUNT_PAUSE_MIN)."""
    return _risk_candle_until(now, RISK_ACCOUNT_PAUSE_MIN)
_risk_cd_fails = {}
_risk_cd_notified = set()


def _risk_acct_pause_active():
    """ADDED 4 Nov: the running whole-account pause record, else None."""
    try:
        with risk_lock:
            a = dict(risk_state.get("acct_pause") or {})
        if a and float(a.get("until", 0)) > time.time():
            return a
    except Exception:
        pass
    return None


def _risk_cooldown_active(key):
    """ADDED 4 Nov: the cooldown record of this instrument while it is running, else None."""
    try:
        with risk_lock:
            c = (risk_state.get("cooldown") or {}).get(str(key))
        if c and float(c.get("until", 0)) > time.time():
            return dict(c)
    except Exception:
        pass
    return None


def _risk_cooldown_start(key, sym, why):
    """ADDED 4 Nov: starts the re-entry cooldown (once; a running one is not extended). Never raises."""
    try:
        if RISK_REENTRY_COOLDOWN_MIN <= 0 or _risk_cooldown_active(key):
            return
        _u = _risk_candle_until(datetime.now(IST), RISK_REENTRY_COOLDOWN_MIN)
        until = _u.timestamp()
        txt = _u.strftime("%H:%M")
        with risk_lock:
            cd = risk_state.get("cooldown") or {}
            cd[str(key)] = {"symbol": sym, "why": why, "until": until, "until_txt": txt}
            risk_state["cooldown"] = cd
            _risk_save()
        log_event({"action": "RISK_REENTRY_COOLDOWN_STARTED", "symbol": sym, "security_id": str(key), "why": why, "until": txt})
        extra = ""
        if RISK_ACCOUNT_PAUSE_MIN > 0 and "stop-loss" in why and not _risk_acct_pause_active():          # ADDED 4 Nov: stop-loss -> the whole account pauses too
            a_dt = _risk_pause_until(datetime.now(IST))
            a_until = a_dt.timestamp()
            a_txt = a_dt.strftime("%H:%M")
            with risk_lock:
                existing = [str(x.get("key")) for x in (risk_state.get("positions") or []) if str(x.get("key")) != str(key)]
                risk_state["acct_pause"] = {"until": a_until, "until_txt": a_txt, "why": f"{sym} stop-loss", "existing": existing}
                _risk_save()
            log_event({"action": "RISK_ACCOUNT_PAUSE_STARTED", "symbol": sym, "until": a_txt, "existing": existing})
            extra = f" The whole account is also paused until {a_txt} (until the 2nd 15-minute candle, counted from the stop-loss candle, has closed): a NEW position in any symbol is closed. Positions already open stay."
        notify(f"[Dhan] {sym} blocked until {txt}", f"{sym} was closed by its {why}. Your rule: no new trade in the same symbol until the 3rd 15-minute candle (the exit candle is the 1st) has closed "
               f"(until {txt}). A position opened before then is closed at market by the bridge." + extra, priority="high")
    except Exception as e:
        log_event({"action": "RISK_COOLDOWN_ERROR", "error": str(e)})


def _risk_exit_was_sl(key) -> bool:
    """ADDED 4 Nov: True when a NEW filled stop-loss order (plain stop order or Super Order stop-loss leg) exists for this instrument today. Each filled order is counted once. Never raises."""
    try:
        found, ids = False, []
        r = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        for o in (safe_json(r) if r.ok else []) or []:
            if str(o.get("securityId")) == str(key) and str(o.get("orderStatus", "")).upper() == "TRADED" \
                    and str(o.get("orderType", "")).upper() in ("STOP_LOSS", "STOP_LOSS_MARKET"):
                ids.append("o" + str(o.get("orderId")))
        r = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        for o in (safe_json(r) if r.ok else []) or []:
            if str(o.get("securityId")) != str(key):
                continue
            for lg in (o.get("legDetails") or []):
                if str(lg.get("legName")) == "STOP_LOSS_LEG" and str(lg.get("orderStatus", lg.get("legStatus", ""))).upper() == "TRADED":
                    ids.append("s" + str(o.get("orderId")))
        with risk_lock:
            seen = risk_state.get("sl_seen") or {}
            found = any(i not in seen for i in ids)
            for i in ids:
                seen[i] = 1
            risk_state["sl_seen"] = seen
            _risk_save()
        return found
    except Exception as e:
        log_event({"action": "RISK_SL_LOOKUP_ERROR", "error": str(e)})
        return False


def _risk_cooldown_enforce(open_pos):
    """ADDED 4 Nov: a position that is open in an instrument whose cooldown is running is cancelled / closed at market (does not lock the day, other positions untouched). Never raises."""
    try:
        for p in open_pos:
            sec = str(p.get("securityId"))
            c = _risk_cooldown_active(sec)
            ap = _risk_acct_pause_active()
            if not c and ap and sec not in (ap.get("existing") or []):      # ADDED 4 Nov: a NEW position (not open when the stop-loss happened) during the whole-account pause
                c = {"until_txt": ap.get("until_txt"), "why": f"account pause after {ap.get('why')}"}
            seg = str(p.get("exchangeSegment", "NSE_EQ"))
            if not c or is_dry_run(seg) or _risk_cd_fails.get(sec, 0) >= 5:
                continue
            sym = str(p.get("tradingSymbol") or sec)
            log_event({"action": "RISK_COOLDOWN_EXIT", "symbol": sym, "security_id": sec, "until": c.get("until_txt")})
            _risk_cancel_orders_for(sec)
            if _risk_market_close(p):
                try:
                    with state_lock:
                        for k, e in list(last_entries.items()):
                            if str(e.get("security_id")) == sec:
                                last_entries.pop(k, None)
                    save_state()
                except Exception:
                    pass
                if sec not in _risk_cd_notified:
                    _risk_cd_notified.add(sec)
                    notify(f"[Dhan] {sym} re-entry blocked - closed", f"{sym} was opened again before {c.get('until_txt')} ({c.get('why')} cooldown). The bridge closed it at market.", priority="high")
            else:
                _risk_cd_fails[sec] = _risk_cd_fails.get(sec, 0) + 1
                if _risk_cd_fails[sec] >= 5:
                    notify(f"[Dhan] {sym} cooldown close FAILED", f"{sym} is open during its cooldown and the bridge could not close it after 5 tries. Close it on Dhan NOW.", priority="urgent")
    except Exception as e:
        log_event({"action": "RISK_COOLDOWN_ENFORCE_ERROR", "error": str(e)})


RISK_FAST_POLL_FROM = 6000.0       # ADDED 4 Nov: while any open position has a profit of at least this (Rs) and no profit lock yet, the bridge looks every second (instead of every 3) so a fast spike through the trigger is not missed
RISK_FAST_POLL_SECONDS = 1.0
RISK_AUTO_STOCK_LOSS = 20000.0   # ADDED 4 Nov: DEFAULT max loss (Rs) of every individual STOCK position (NSE equity), set the moment the bridge first sees the position open. It is the position's own "Loss" switch on the /risk page (change or switch it off there any time) and is separate from the daily loss limit. 0 = off. F&O and MCX positions are not given a default.
RISK_REMIND_FIRST = 10000.0   # ADDED: first day-profit level that sends the "set your profit lock" reminder
RISK_REMIND_STEP = 5000.0     # ADDED: then again at every further +5000 (15000, 20000, ...)


def _risk_profit_reminder(pnl):
    """ADDED: phone alert when the day's total profit first reaches Rs 10,000 and then at every further Rs 5,000, so you remember
    to set / raise the profit lock. Once per level per day; does nothing else (no orders, no state change except the level counter)."""
    if pnl is None or pnl < RISK_REMIND_FIRST:
        return
    level = RISK_REMIND_FIRST + int((pnl - RISK_REMIND_FIRST) // RISK_REMIND_STEP) * RISK_REMIND_STEP
    with risk_lock:
        if risk_state.get("locked") or level <= float(risk_state.get("profit_step") or 0):
            return
        risk_state["profit_step"] = level
        floor = risk_state.get("floor")
        enabled = bool(risk_state.get("enabled", False))
        _risk_save()
    lines = [f"Day profit is Rs {pnl:,.0f}."]
    if floor is not None:
        lines.append(f"Your profit lock is Rs {floor:,.0f}. Raise it (it must stay below Rs {pnl:,.0f}).")
    else:
        lines.append(f"Set your profit lock now, a little below Rs {pnl:,.0f}, so one more trade cannot give it back.")
    if not enabled:
        lines.append("The risk guard is OFF - switch it ON first.")
    log_event({"action": "RISK_PROFIT_REMINDER", "level": level, "pnl": pnl, "floor": floor})
    notify(f"[Dhan] Profit Rs {pnl:,.0f} - protect it", " ".join(lines), priority="high")


def _risk_check_once():
    if not _risk_try_become_leader():
        return
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return
    if now.hour * 60 + now.minute < 8 * 60 + 45:
        return
    _risk_roll_day()
    positions = _risk_fetch_positions()
    if positions is None:
        with risk_lock:
            risk_state["poll_fail"] += 1
            fails = risk_state["poll_fail"]
            guard_on = risk_state["limit"] > 0 or risk_state["floor"] is not None or bool(risk_state.get("targets"))
            notify_blind = guard_on and fails >= 6 and not risk_state["blind_notified"]
            if notify_blind:
                risk_state["blind_notified"] = True
        if notify_blind:
            notify("[Dhan] Risk monitor is BLIND",
                   "Could not read positions from Dhan 6 times in a row (token expired? IP?). "
                   "The daily loss limit / profit targets are NOT being enforced until this is fixed.")
        return
    pnl = _risk_total_pnl(positions)
    open_pos = [p for p in positions if _risk_net_qty(p) != 0]
    _risk_sync()   # pick up any limit/floor/target saved from the page while positions were being fetched
    with risk_lock:
        limit = risk_state["limit"]
        locked = risk_state["locked"]
        floor = risk_state["floor"]
        if floor is not None and not risk_state.get("floor_armed") and pnl > floor:
            risk_state["floor_armed"] = True
            log_event({"action": "RISK_FLOOR_ARMED", "floor": floor, "pnl": pnl})
        floor_armed = bool(risk_state.get("floor_armed"))
        risk_state["last_pnl"] = pnl
        risk_state["last_poll"] = now.strftime("%H:%M:%S")
        risk_state["poll_fail"] = 0
        risk_state["blind_notified"] = False
        prev_open = {str(x.get("key")): str(x.get("symbol")) for x in (risk_state.get("positions") or [])}      # ADDED 4 Nov: positions open at the previous look (to see which one just closed)
        # open positions for the page + forget targets of positions that are no longer open
        risk_state["positions"] = [
            {"key": str(p.get("securityId")), "symbol": str(p.get("tradingSymbol") or p.get("securityId")),
             "qty": _risk_net_qty(p), "pnl": round(_risk_f(p.get("unrealizedProfit")), 2),
             "cost": round(_risk_f(p.get("costPrice") or (p.get("buyAvg") if _risk_net_qty(p) > 0 else p.get("sellAvg"))), 2),
             "mult": (_risk_f(p.get("multiplier")) or 1.0)} for p in open_pos]
        live_keys = {str(p.get("securityId")) for p in open_pos}
        tg = risk_state.get("targets") or {}
        for k in list(tg.keys()):
            if k not in live_keys:
                tg.pop(k, None)
        risk_state["targets"] = tg
        auto_new = []
        if RISK_AUTO_STOCK_LOSS > 0:      # ADDED 4 Nov: default max loss for a newly seen stock position (once per open position; a loss you set or switched off yourself is never overwritten)
            seen = risk_state.get("auto_loss_seen") or {}
            for k in list(seen.keys()):
                if k not in live_keys:
                    seen.pop(k, None)     # position closed: a new position in the same stock gets the default again
            for p in open_pos:
                k = str(p.get("securityId"))
                if k in seen or str(p.get("exchangeSegment", "NSE_EQ")) != "NSE_EQ":
                    continue
                seen[k] = 1
                sym_ = str(p.get("tradingSymbol") or k)
                t = dict(tg.get(k) or {"amount": 0, "loss": 0, "lock": 0, "symbol": sym_, "fails": 0})
                if _risk_f(t.get("loss")) <= 0:
                    t["loss"] = RISK_AUTO_STOCK_LOSS
                    t["symbol"] = sym_
                    t["fails"] = 0
                    tg[k] = t
                    auto_new.append(sym_)
            risk_state["auto_loss_seen"] = seen
            risk_state["targets"] = tg
        auto_lock_new = []
        if RISK_AUTO_LOCK_TRIGGER > 0 and RISK_AUTO_LOCK_AMOUNT > 0:      # ADDED 4 Nov: automatic profit lock for every open position once it has crossed the trigger profit
            lseen = risk_state.get("auto_lock_seen") or {}
            for k in list(lseen.keys()):
                if k not in live_keys:
                    lseen.pop(k, None)
            for p in open_pos:
                k = str(p.get("securityId"))
                pnl_p = _risk_f(p.get("unrealizedProfit"))
                if k in lseen or pnl_p < RISK_AUTO_LOCK_TRIGGER:
                    continue
                lseen[k] = 1
                sym_ = str(p.get("tradingSymbol") or k)
                t = dict(tg.get(k) or {"amount": 0, "loss": 0, "lock": 0, "symbol": sym_, "fails": 0})
                if _risk_f(t.get("lock")) <= 0 and pnl_p > RISK_AUTO_LOCK_AMOUNT:
                    t["lock"] = RISK_AUTO_LOCK_AMOUNT
                    t["symbol"] = sym_
                    t["fails"] = 0
                    tg[k] = t
                    auto_lock_new.append((sym_, pnl_p))
            risk_state["auto_lock_seen"] = lseen
            risk_state["targets"] = tg
        warned = risk_state["warned"]
        _risk_save()
    for _s, _pl in auto_lock_new:
        log_event({"action": "RISK_AUTO_PROFIT_LOCK_SET", "symbol": _s, "pnl_position": _pl, "lock": RISK_AUTO_LOCK_AMOUNT})
        notify(f"[Dhan] Profit locked - {_s}", f"{_s} crossed Rs {RISK_AUTO_LOCK_TRIGGER:,.0f} profit (now about Rs {_pl:,.0f}). Its profit lock is set to Rs {RISK_AUTO_LOCK_AMOUNT:,.0f}: "
               f"the position is closed at market if its profit falls back to that. Raise the lock on the /risk page to protect more.", priority="high")
    for _s in auto_new:
        log_event({"action": "RISK_AUTO_STOCK_LOSS_SET", "symbol": _s, "loss": RISK_AUTO_STOCK_LOSS})
        notify(f"[Dhan] Default max loss set - {_s}", f"{_s} is now open. Its max loss is set to Rs {RISK_AUTO_STOCK_LOSS:,.0f} (default for stocks): "
               f"the position is closed at market if its loss reaches that. Change or switch it off on the /risk page.", priority="default")
    try:
        _risk_profit_reminder(pnl)
    except Exception as e:
        log_event({"action": "RISK_PROFIT_REMINDER_ERROR", "error": str(e)})
    try:
        if _risk_prot_closed(live_keys, pnl):
            return
        _risk_prot_sync(positions)
    except Exception as e:
        log_event({"action": "RISK_PROT_ERROR", "error": str(e)})
    if RISK_REENTRY_COOLDOWN_MIN > 0:                 # ADDED 4 Nov: a position that just closed - was it a stop-loss hit? (profit lock / max loss exits start their cooldown where they happen)
        for _k, _sym in prev_open.items():
            if _k not in live_keys and not _risk_cooldown_active(_k) and _risk_exit_was_sl(_k):
                _risk_cooldown_start(_k, _sym, "stop-loss")
        _risk_cooldown_enforce(open_pos)
    _risk_early_exit(open_pos, now)               # ADDED 4 Nov: strict rule - nothing stays open in NSE before 09:30 (works with the guard ON or OFF)
    try:                                          # ADDED 13 Nov: weekly capital-loss lock (total P&L since the start reaches the limit -> exit everything, Kill Switch, 7 days)
        if CL is not None:
            _cl_today = _risk_today()
            try:
                CL.cool_update(pnl, _cl_today)             # ADDED 15 Nov: a day ending with Rs 50,000+ profit -> next trading day is a cool-off day
            except Exception as _cu_e:
                log_event({"action": "COOLOFF_UPDATE_ERROR", "error": str(_cu_e)})
            _cl_hit = CL.on_poll(pnl, _cl_today)
            if _cl_hit or CL.is_active(_cl_today):
                if _cl_hit or not risk_state.get("locked") or open_pos:
                    _risk_flatten("capital profit floor hit" if CL.kind() == "floor" else "capital loss limit reached", pnl)
                else:
                    _risk_kill_retry()
                return
    except Exception as _cl_e:
        log_event({"action": "CAPITAL_LOCK_ERROR", "error": str(_cl_e)})
    if not risk_state.get("enabled", False):      # guard OFF: P&L display keeps updating; the per-position Profit / Lock profit / Loss exits stay active (they do not need the guard)
        _risk_process_targets(open_pos)
        return
    guard_on = limit > 0 or floor is not None
    if guard_on and locked:
        if open_pos:
            _risk_flatten("position opened while locked", pnl)
        else:
            _risk_kill_retry()
        return
    if floor is not None and floor_armed and pnl <= floor:
        _risk_flatten("profit floor hit", pnl)
        return
    if limit > 0 and pnl <= -limit:
        _risk_flatten("loss limit reached", pnl)
        return
    if limit > 0 and RISK_WARN_PCT > 0 and pnl <= -limit * RISK_WARN_PCT / 100.0 and not warned:
        with risk_lock:
            risk_state["warned"] = True
            _risk_save()
        notify("[Dhan] Loss warning",
               f"Day P&L Rs {pnl:,.0f} - already {RISK_WARN_PCT:.0f}% of your Rs {limit:,.0f} daily loss limit.",
               priority="high")
    _risk_process_targets(open_pos)


def _risk_poll_delay() -> float:
    """ADDED 4 Nov: 1 second while a position is close to the auto profit-lock trigger and has no lock yet, otherwise the normal RISK_POLL_SECONDS."""
    try:
        if RISK_AUTO_LOCK_TRIGGER > 0:
            with risk_lock:
                tg = risk_state.get("targets") or {}
                for pz in (risk_state.get("positions") or []):
                    if _risk_f(pz.get("pnl")) >= RISK_FAST_POLL_FROM and _risk_f((tg.get(pz.get("key")) or {}).get("lock")) <= 0:
                        return RISK_FAST_POLL_SECONDS
    except Exception:
        pass
    return RISK_POLL_SECONDS


def _risk_monitor_loop():
    time.sleep(12)
    _risk_sync()
    with risk_lock:
        lim = risk_state["limit"]
    log_event({"action": "RISK_MONITOR_STARTED", "build": "2026-10-30-b", "limit": lim, "poll_seconds": RISK_POLL_SECONDS,
               "pid": os.getpid(), "leader": _risk_try_become_leader()})
    if lim <= 0 and _risk_leader:
        notify("[Dhan] Daily loss limit NOT SET",
               "The risk guard is OFF. Open the /risk page and set today's maximum loss.", priority="high")
    while True:
        time.sleep(_risk_poll_delay())
        try:
            _risk_check_once()
        except Exception as e:
            log_event({"action": "RISK_MONITOR_ERROR", "error": str(e)})


def _risk_prot_page_sync() -> str:
    """After a setting is changed on the page: re-read the positions and update the Dhan order at once."""
    try:
        fresh = _risk_fetch_positions()
        msgs = _risk_prot_sync(fresh, force=True) if fresh else []
        return (" " + " ".join(msgs)) if msgs else ""
    except Exception as e:
        log_event({"action": "RISK_PROT_PAGE_ERROR", "error": str(e)})
        return ""


def risk_set_limit(new_limit: float):
    """Returns (ok, message). Enforces 'lower only' once set for the day."""
    _risk_roll_day()
    if new_limit < 0:
        return False, "Limit cannot be negative."
    today = _risk_today()
    with risk_lock:
        cur = risk_state["limit"]
        set_today = risk_state["limit_date"] == today
        if set_today and cur > 0 and (new_limit > cur or new_limit == 0) and not RISK_ALLOW_RAISE_SAME_DAY:
            return False, (f"Today's limit is already Rs {cur:,.0f}. It can only be lowered during the day "
                           f"(raise or switch off tomorrow).")
        risk_state["limit"] = new_limit
        risk_state["limit_date"] = today
        risk_state["warned"] = False
        released = False
        if risk_state["locked"] and new_limit > 0 and RISK_ALLOW_RAISE_SAME_DAY:
            pnl = risk_state.get("last_pnl")
            if pnl is not None and pnl > -new_limit:
                risk_state["locked"] = False
                risk_state["locked_at"] = ""
                released = True
        _risk_save()
    log_event({"action": "RISK_LIMIT_SET", "old": cur, "new": new_limit, "lock_released": released})
    if released:
        notify("[Dhan] Risk lock released", f"Loss limit raised to Rs {new_limit:,.0f}; trading allowed again.",
               priority="high")
    extra = _risk_prot_page_sync()
    return True, f"Daily loss limit set to Rs {new_limit:,.0f}." + (" Lock released." if released else "") + extra


def risk_set_floor(new_floor: float):
    """Profit lock: exit everything if total day P&L falls back to new_floor.
    0 (or less) = NO profit lock (guard off, nothing can trigger). A positive amount is accepted only when the current
    P&L is already ABOVE it (otherwise everything would be closed at once) and then it is ACTIVE straight away.
    Once active it can only be RAISED, never lowered or switched off. Clears tomorrow.
    Returns (ok, message)."""
    _risk_roll_day()
    with risk_lock:
        cur = risk_state["floor"]
        limit = risk_state["limit"]
        pnl = risk_state.get("last_pnl")
        if risk_state["locked"]:
            return False, "Already locked for today."
        if new_floor <= 0:
            if cur is not None:
                return False, f"Profit lock is already active at Rs {cur:,.0f}. It can only be raised."
            risk_state["floor"] = None
            risk_state["floor_armed"] = False
            _risk_save()
            log_event({"action": "RISK_FLOOR_SET", "old": cur, "new": None, "pnl": pnl, "armed": False})
            return True, "Profit lock is OFF (0). Set an amount later, once you are in profit, to protect the day."
        if limit > 0 and new_floor <= -limit:
            return False, f"Floor must be above the loss limit (-{limit:,.0f}); otherwise it does nothing."
        if cur is not None and new_floor <= cur:
            return False, f"Profit lock is already active at Rs {cur:,.0f}. It can only be raised."
        if pnl is None:
            return False, "No P&L reading yet - wait a few seconds and try again."
        if new_floor >= pnl:
            _sug = int((pnl - 300) // 100) * 100
            return False, (f"WARNING: Rs {new_floor:,.0f} is higher than your current profit of Rs {pnl:,.0f}, so the profit lock was NOT set. "
                           f"It must be BELOW your profit, otherwise everything would be closed immediately."
                           + (f" Try Rs {_sug:,.0f} or lower." if _sug > 0 else ""))
        risk_state["floor"] = new_floor
        risk_state["floor_armed"] = True
        _risk_save()
    log_event({"action": "RISK_FLOOR_SET", "old": cur, "new": new_floor, "pnl": pnl, "armed": True})
    extra = _risk_prot_page_sync()
    return True, f"Profit lock ACTIVE: everything exits if P&L falls to Rs {new_floor:,.0f}. You can raise it later as profit grows." + extra


def risk_set_target(key: str, kind: str, amount: float):
    """Independent per-position switches. kind = "profit", "loss" or "lock" (lock = exit when the position's profit falls back to this rupee amount).
    amount > 0 switches that one ON with that rupee amount, amount <= 0 switches it OFF. The other one is untouched.
    Profit must be ABOVE the position's current open profit and the max loss must be BIGGER than its current open loss
    (otherwise the position would be closed at once). Returns (ok, message)."""
    _risk_roll_day()
    key = str(key)
    if kind not in ("profit", "loss", "lock"):
        return False, "Unknown switch."
    field = "amount" if kind == "profit" else ("lock" if kind == "lock" else "loss")
    with risk_lock:
        pos = next((x for x in (risk_state.get("positions") or []) if x.get("key") == key), None)
        if pos is None:
            return False, "That position is not open any more (or not read yet). Wait a few seconds."
        tg = risk_state.get("targets") or {}
        t = dict(tg.get(key) or {"amount": 0, "loss": 0, "lock": 0, "symbol": pos["symbol"], "fails": 0})
        if amount <= 0:
            t[field] = 0
            msg = f"{pos['symbol']}: {'profit target' if kind == 'profit' else ('profit lock' if kind == 'lock' else 'max loss')} switched OFF."
        else:
            if kind == "profit" and pos["pnl"] >= amount:
                return False, (f"{pos['symbol']} is already at Rs {pos['pnl']:,.0f} profit. The target must be ABOVE it, "
                               f"otherwise it would be closed at once.")
            if kind == "loss" and pos["pnl"] <= -amount:
                return False, (f"{pos['symbol']} is already at Rs {pos['pnl']:,.0f}. The max loss must be BIGGER than the "
                               f"current loss, otherwise it would be closed at once.")
            if kind == "lock" and pos["pnl"] <= amount:
                _sug = int((pos["pnl"] - 300) // 100) * 100
                return False, (f"WARNING: Rs {amount:,.0f} is higher than {pos['symbol']}'s current profit of Rs {pos['pnl']:,.0f}, so the lock was NOT set. "
                               f"It must be BELOW the current profit, otherwise the position would be closed at once."
                               + (f" Try Rs {_sug:,.0f} or lower." if _sug > 0 else ""))
            t[field] = amount
            if kind == "lock":
                msg = f"{pos['symbol']}: profit lock ON - the whole position closes at market if its profit falls back to Rs {amount:,.0f}. Raise it any time."
            elif kind == "profit":
                msg = f"{pos['symbol']}: profit target ON - closes at market when its profit reaches Rs {amount:,.0f}."
            else:
                msg = f"{pos['symbol']}: max loss ON - closes at market when its loss reaches Rs {amount:,.0f}."
        t["symbol"] = pos["symbol"]
        t["fails"] = 0
        if _risk_f(t.get("amount")) <= 0 and _risk_f(t.get("loss")) <= 0 and _risk_f(t.get("lock")) <= 0:
            tg.pop(key, None)
        else:
            tg[key] = t
        risk_state["targets"] = tg
        _risk_save()
    log_event({"action": "RISK_TARGET_SET", "symbol": pos["symbol"], "security_id": key, "kind": kind, "amount": amount})
    if kind in ("lock", "loss"):
        msg += _risk_prot_page_sync()
    return True, msg


def risk_set_targets_on(on: bool):
    """Master switch for per-position profit targets. OFF by default each day. Switching OFF clears all targets."""
    _risk_roll_day()
    with risk_lock:
        risk_state["targets_on"] = bool(on)
        if not on:
            risk_state["targets"] = {}
        _risk_save()
    log_event({"action": "RISK_TARGETS_SWITCH", "on": bool(on)})
    if on:
        return True, "Position targets are ON. Set a profit target and/or max loss for each position you want."
    return True, "Position targets are OFF. All targets cleared."



def risk_set_enabled(on: bool):
    """Master switch of the whole risk guard (loss limit, profit lock, per-position exits and the new-trade block).
    OFF by default every day. Once it is switched ON it stays ON until the day ends (it cannot be switched off again the same day).
    ON is refused when the current P&L is already past the loss limit / profit lock (it would close everything the moment it is
    switched on). The settings are kept while it is OFF. Returns (ok, message)."""
    _risk_roll_day()
    with risk_lock:
        cur = bool(risk_state.get("enabled", False))
        pnl = risk_state.get("last_pnl")
        limit = risk_state["limit"]
        floor = risk_state["floor"]
        if not on:
            if cur:
                return False, "The guard is ON for today and stays ON until the day ends (it switches itself ON again every new day)."
            return True, "The guard is already OFF."
        else:
            if cur:
                return True, "The guard is already ON."
            if pnl is not None and limit > 0 and pnl <= -limit:
                return False, f"Cannot switch ON: P&L Rs {pnl:,.2f} is already past your loss limit of Rs {limit:,.2f} - everything would be closed at once."
            if pnl is not None and floor is not None and pnl <= floor:
                return False, f"Cannot switch ON: P&L Rs {pnl:,.2f} is already at or below your profit lock of Rs {floor:,.2f}."
            risk_state["enabled"] = True
            _risk_save()
            msg = "Risk guard switched ON. It stays ON until the day ends."
    log_event({"action": "RISK_GUARD_SWITCH", "enabled": bool(on), "was": cur, "pnl": pnl})
    if cur != bool(on):
        notify("[Dhan] Risk guard switched " + ("ON" if on else "OFF"), msg, priority="high")
    return True, msg


_risk_load()
try:                                                         # ADDED 13 Nov: weekly capital-loss lock, page /capital/<WEBHOOK_SECRET>
    import capital_lock as CL
    CL.install(app, _risk_data_dir(), WEBHOOK_SECRET, notify, log_event)
except Exception as _cl_e:
    CL = None
    log.warning(f"capital lock not started: {_cl_e}")
try:                                                         # ADDED 14 Nov: forwards the six indicator signals to the scanner /signals page + phone alert
    import signals_feed as _sfeed
    _sfeed.install(app, _risk_data_dir(), WEBHOOK_SECRET, notify, log_event)
except Exception as _sf_e0:
    log.warning(f"signals feed not started: {_sf_e0}")
threading.Thread(target=_risk_monitor_loop, daemon=True, name="risk-monitor").start()


_RISK_PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Risk guard</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,Arial,sans-serif;background:#0f1419;color:#e6e6e6;margin:0;padding:12px}
.wrap{max-width:1180px;margin:0 auto}
h1{font-size:18px;margin:0 0 10px}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;align-items:start}
@media (max-width:900px){.grid{grid-template-columns:1fr}}
.card{background:#1a2129;border-radius:12px;padding:14px}
.card h2{font-size:14px;margin:0 0 8px;color:#9fb0c0;text-transform:uppercase;letter-spacing:.04em}
.big{font-size:30px;font-weight:700;margin:2px 0 6px}
.ok{color:#3ddc84}.bad{color:#ff5252}.warn{color:#ffb300}.mut{color:#8a97a3;font-size:12px}
.row{display:flex;justify-content:space-between;gap:8px;padding:6px 0;border-bottom:1px solid #27313b;font-size:14px}
.row:last-child{border-bottom:0}
input{width:100%;padding:9px;font-size:16px;border-radius:8px;border:1px solid #3a4651;background:#0f1419;color:#fff}
button{width:100%;margin-top:6px;padding:10px;font-size:14px;border:0;border-radius:8px;background:#2f80ed;color:#fff;font-weight:600;cursor:pointer}
.sec{padding-bottom:12px;margin-bottom:12px;border-bottom:1px solid #27313b}
.sec:last-child{border-bottom:0;margin-bottom:0;padding-bottom:0}
.msg{padding:9px;border-radius:8px;background:#27313b;margin-bottom:10px;font-size:14px}
details{margin-top:4px}summary{cursor:pointer;color:#8a97a3;font-size:12px}
.prow{background:#0f1419;border-radius:10px;padding:9px;margin-bottom:8px}
.prow .top{display:flex;justify-content:space-between;font-size:14px}
.prow form{display:flex;gap:6px;margin-top:6px;align-items:center}
.prow form input{min-width:0}
.prow .lab{flex:1;font-size:13px}
.prow button.off{background:#8a3b3b}
.prow input{font-size:14px;padding:7px;width:84px;flex:none}
.prow button{flex:none;white-space:nowrap;width:92px;margin-top:0;padding:7px;background:#1e9e5a}
</style></head><body><div class="wrap">
<style>@keyframes rb{0%,49%{background:#fff176;color:#d50000}50%,100%{background:#d50000;color:#fff}}</style><div style="animation:rb 1s step-end infinite;position:sticky;top:0;z-index:99999;background:#fff176;border:3px solid #d50000;border-radius:6px;padding:10px 14px;margin:0 0 14px;color:#d50000;font-weight:900;font-size:20px;line-height:1.5;text-align:center">1. DON&#39;T RUSH &nbsp;|&nbsp; 2. DON&#39;T TRADE WITHOUT VALID SET UP &nbsp;|&nbsp; 3. 1 VALID SETUP ENOUGH TO EARN MONEY &nbsp;|&nbsp; 4. DON&#39;T BE EMOTIONAL ATTACHED</div>
<h1>Risk guard</h1>
<div class="mut" style="margin-bottom:8px"><a style="color:#9fb0c0" target="_blank" rel="noopener" href="/capital/__SECRET__">Weekly capital loss lock + profit floor &rarr;</a> &nbsp;|&nbsp; <a style="color:#9fb0c0" target="_blank" rel="noopener" href="/scanner_auto/__SECRET__">Scanner auto-trade (1 share) &rarr;</a> &nbsp;|&nbsp; <a style="color:#9fb0c0" target="_blank" rel="noopener" href="/swing/__SECRET__">Swing positions + auto-sell &rarr;</a></div>
__MSG__
<div class="grid">

<div class="card">
<h2>Status</h2>
<div id="swbox" style="margin-bottom:10px"></div>
<div class="mut">Today's total P&amp;L (all positions)</div>
<div class="big" id="pnl">__PNL__</div>
<div class="row"><span>Status</span><b id="status">__STATUS__</b></div>
<div class="row"><span>Max loss allowed</span><b id="limit">__LIMIT__</b></div>
<div class="row"><span>Profit lock</span><b id="floor">__FLOOR__</b></div>
<div class="row"><span>Last checked (IST)</span><span id="poll">__POLL__</span></div>
<div class="row"><span>Locked at</span><span id="locked">__LOCKED__</span></div>
<div class="row"><span>Last forced exit</span><span id="report">__REPORT__</span></div>
</div>

<div class="card">
<h2>Limits</h2>
<form class="sec" method="post" action="/risk/__SECRET__/set">
<div class="mut" style="margin-bottom:5px">Daily max loss, rupees (0 = off). __RULE__</div>
<input name="limit" type="number" min="0" step="100" inputmode="numeric" value="__INPUT__" required>
<button type="submit">Save limit</button>
</form>
<form class="sec" method="post" action="/risk/__SECRET__/floor">
<div class="mut" style="margin-bottom:5px">Profit lock (whole day): exit everything if total P&amp;L falls back to this amount. 0 = off.</div>
<input name="floor" type="number" step="100" inputmode="numeric" placeholder="0 = off, or e.g. 5000" required>
<button type="submit" style="background:#1e9e5a">Set profit lock</button>
<details><summary>How it works</summary><div class="mut">Enter an amount a little BELOW your current P&amp;L (about Rs 200-300 under it) once you are in profit; an amount at or above your profit is refused with a warning. It works only while the guard is ON, covers all trades of the day, can only be raised (never lowered or switched off), and clears tomorrow. A phone alert reminds you at Rs 10,000 profit and at every further Rs 5,000.</div></details>
</form>
<div class="mut">While a position is open the bridge keeps one stop-loss limit order on Dhan at the price where your loss limit or lock would be reached. When the loss limit is reached every order is cancelled, every position is closed at market and new trades are blocked until the next trading day. A fast move or price gap can make the real loss larger than the limit, so keep your size small.</div>
</div>

<div class="card">
<h2>Position exits (profit / lock / loss)</h2>
<div class="sec"><div class="mut">Each open position has three independent switches: <b>Profit</b> (close when profit reaches the amount), <b>Lock profit</b> (close when profit falls back to the amount - type the rupees you want to keep, the page shows the price; raise it yourself as profit grows) and <b>Loss</b>. All are OFF by default. Lock profit and Loss also keep a stop-loss limit order on Dhan at the calculated price; if it does not fill, the bridge closes that position at market and cancels its orders. Only that position is closed. These three work on their own - the guard does not need to be ON. They clear when the position closes.</div></div>
<div id="pos"><span class="mut">no open positions</span></div>
</div>

</div></div>
<script>
function esc(t){return String(t).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function rs(n){return 'Rs '+Math.round(n).toLocaleString('en-IN');}
function sw(p,kind){const on=kind==='profit'?p.target:p.loss;const lab=kind==='profit'?'Profit':'Loss';
 const k=esc(p.key);
 if(on){
  return '<form method="post" action="/risk/__SECRET__/target" class="sw"><input type="hidden" name="key" value="'+k+'"><input type="hidden" name="kind" value="'+kind+'"><input type="hidden" name="amount" value="0"><span class="lab">'+lab+' <b class="ok">ON</b> '+rs(on)+' <span class="mut" id="g_'+kind+'_'+k+'"></span></span><button type="submit" class="off">Turn OFF</button></form>';}
 return '<form method="post" action="/risk/__SECRET__/target" class="sw"><input type="hidden" name="key" value="'+k+'"><input type="hidden" name="kind" value="'+kind+'"><span class="lab">'+lab+' <b class="mut">OFF</b></span><input name="amount" type="number" step="any" min="1" inputmode="numeric" placeholder="Rs" required><button type="submit">Turn ON</button></form>';}
function pxh(p,a){const u=Math.abs(p.qty)*(p.mult||1);if(!p.cost||!u)return '';const x=p.qty>0?p.cost+a/u:p.cost-a/u;return 'exit price about '+x.toFixed(2);}
function lh(inp){const k=inp.dataset.k;const p=(window.PM||{})[k];const el=document.getElementById('h_lock_'+k);if(!p||!el)return;const a=parseFloat(inp.value);el.textContent=(a>0)?pxh(p,a):'';}
function lockSw(p){const k=esc(p.key);const on=p.lock;
 if(on){
  return '<form method="post" action="/risk/__SECRET__/target" class="sw"><input type="hidden" name="key" value="'+k+'"><input type="hidden" name="kind" value="lock"><span class="lab">Lock profit <b class="ok">ON</b> '+rs(on)+' <span class="mut" id="g_lock_'+k+'"></span></span><input name="amount" type="number" step="any" min="1" inputmode="numeric" placeholder="new Rs" data-k="'+k+'" oninput="lh(this)" required><button type="submit">Change</button></form>'
   +'<form method="post" action="/risk/__SECRET__/target" class="sw"><input type="hidden" name="key" value="'+k+'"><input type="hidden" name="kind" value="lock"><input type="hidden" name="amount" value="0"><span class="lab mut" id="h_lock_'+k+'">'+esc(pxh(p,on))+'</span><button type="submit" class="off">Turn OFF</button></form>';}
 return '<form method="post" action="/risk/__SECRET__/target" class="sw"><input type="hidden" name="key" value="'+k+'"><input type="hidden" name="kind" value="lock"><span class="lab">Lock profit <b class="mut">OFF</b></span><input name="amount" type="number" step="any" min="1" inputmode="numeric" placeholder="Rs to keep" data-k="'+k+'" oninput="lh(this)" required><button type="submit">Turn ON</button></form><div class="mut" id="h_lock_'+k+'" style="font-size:12px"></div>';}
function renderPos(list){const box=document.getElementById('pos');window.PM={};(list||[]).forEach(p=>{window.PM[esc(p.key)]=p;});
 if(!list||!list.length){box.dataset.sig='';box.innerHTML='<span class="mut">no open positions</span>';return;}
 const sig=list.map(p=>p.key+':'+(p.target||0)+':'+(p.loss||0)+':'+(p.lock||0)).join(',');
 if(box.dataset.sig!==sig){box.dataset.sig=sig;
  box.innerHTML=list.map(p=>'<div class="prow"><div class="top"><b>'+esc(p.symbol)+' <span class="mut" id="q_'+esc(p.key)+'"></span></b><b id="p_'+esc(p.key)+'"></b></div>'+sw(p,'profit')+lockSw(p)+sw(p,'loss')+'</div>').join('');}
 list.forEach(p=>{const k=esc(p.key);
  document.getElementById('q_'+k).textContent=(p.qty>0?'BUY ':'SELL ')+Math.abs(p.qty);
  const pe=document.getElementById('p_'+k);pe.textContent=rs(p.pnl);pe.className=p.pnl>=0?'ok':'bad';
  const gp=document.getElementById('g_profit_'+k),gl=document.getElementById('g_loss_'+k);
  if(gp&&p.target)gp.textContent='('+Math.max(0,Math.round(p.target-p.pnl)).toLocaleString('en-IN')+' to go)';
  if(gl&&p.loss)gl.textContent='('+Math.max(0,Math.round(p.loss+p.pnl)).toLocaleString('en-IN')+' left)';
  const gk=document.getElementById('g_lock_'+k);
  if(gk&&p.lock)gk.textContent='('+Math.round(p.pnl-p.lock).toLocaleString('en-IN')+' above lock)';
 });}
function renderSw(s){const b=document.getElementById('swbox');if(!b)return;const sig=s.enabled+':'+s.locked;if(b.dataset.sig===sig)return;b.dataset.sig=sig;
 if(s.enabled){b.innerHTML='<div class="mut">Risk guard is <b class="ok">ON</b> for today - it stays ON until the day ends and switches itself ON again every new day.</div>';}
 else{b.innerHTML='<div class="mut" style="color:#ffb300"><b>Guard is OFF</b> (default) - no loss limit, profit lock, position exits or trade blocking.</div><form method="post" action="/risk/__SECRET__/enable" onsubmit="return confirm(\\'Switch the risk guard ON? It cannot be switched off again today.\\')"><input type="hidden" name="on" value="1"><button type="submit" style="background:#1e9e5a;margin-top:4px">Switch guard ON (locked for the day)</button></form>';}}
async function tick(){try{const r=await fetch('/risk/__SECRET__/status');const s=await r.json();
document.getElementById('pnl').innerHTML=s.pnl_html;document.getElementById('status').innerHTML=s.status_html;
document.getElementById('limit').textContent=s.limit_text;document.getElementById('floor').textContent=s.floor_text;document.getElementById('poll').textContent=s.poll;
document.getElementById('locked').textContent=s.locked_at;document.getElementById('report').textContent=s.report;renderPos(s.positions);renderSw(s);}catch(e){}}
setInterval(tick,8000);tick();
</script></body></html>"""


def _risk_lock_is_profit(s: dict) -> bool:
    """True when today's lock came from the profit lock (the saved reason, or - for a lock made before this field existed - a positive P&L at the moment of locking)."""
    r = str(s.get("locked_reason") or "")
    if r:
        return r.startswith("profit floor")
    lp = s.get("locked_pnl")
    return s.get("floor") is not None and lp is not None and lp > 0


def _risk_view(s: dict) -> dict:
    pnl = s.get("last_pnl")
    if pnl is None:
        pnl_html = '<span class="mut">waiting for first check</span>'
    else:
        cls = "ok" if pnl >= 0 else "bad"
        pnl_html = f'<span class="{cls}">Rs {pnl:,.0f}</span>'
    limit = s.get("limit") or 0
    if not s.get("enabled", False):
        status_html = '<span class="warn">SWITCHED OFF - not protecting</span>'
    elif limit <= 0 and s.get("floor") is None:
        status_html = '<span class="warn">GUARD OFF - set a limit</span>'
    elif s.get("locked") and _risk_lock_is_profit(s):
        status_html = '<span class="ok">LOCKED - PROFIT LOCK hit, profit secured, trading blocked today</span>'
    elif s.get("locked") and str(s.get("locked_reason") or "").startswith("capital"):
        status_html = '<span class="bad">LOCKED - CAPITAL ' + ("PROFIT" if "profit" in str(s.get("locked_reason")) else "LOSS") + ' LOCK (' + str(s.get("locked_reason")) + '), trading blocked</span>'
    elif s.get("locked"):
        status_html = '<span class="bad">LOCKED - loss limit hit, trading blocked today</span>'
    else:
        status_html = '<span class="ok">ACTIVE - watching</span>'
    return {
        "pnl_html": pnl_html, "status_html": status_html,
        "limit_text": f"Rs {limit:,.0f}" if limit > 0 else "not set",
        "floor_text": ((f"Rs {s['floor']:,.0f} - HIT at {s.get('locked_at') or '-'} (P&L Rs {_risk_f(s.get('locked_pnl')):,.0f}), all positions closed" if (s.get("locked") and _risk_lock_is_profit(s)) else f"Rs {s['floor']:,.0f} - ACTIVE" if s.get("floor_armed") else f"Rs {s['floor']:,.0f} - waiting for profit") if s.get("floor") is not None else "off"),
        "poll": s.get("last_poll") or "-", "locked_at": s.get("locked_at") or "-",
        "report": s.get("report") or "-",
        "targets_on": bool(s.get("targets_on")),
        "enabled": bool(s.get("enabled", False)), "locked": bool(s.get("locked")),
        "positions": [dict(x, target=_risk_f(((s.get("targets") or {}).get(x.get("key")) or {}).get("amount")) or None,
                           loss=_risk_f(((s.get("targets") or {}).get(x.get("key")) or {}).get("loss")) or None,
                           lock=_risk_f(((s.get("targets") or {}).get(x.get("key")) or {}).get("lock")) or None)
                      for x in (s.get("positions") or [])],
    }


@app.route("/risk/<secret>", methods=["GET"])
def risk_page(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    s = risk_status()
    v = _risk_view(s)
    msg = request.args.get("m", "")
    msg_html = ""
    if msg:
        msg_html = ('<div class="msg">' + msg.replace("<", "&lt;").replace(">", "&gt;")[:200] + "</div>"
                    "<script>try{history.replaceState(null,'',location.pathname)}catch(e){}</script>")   # the message shows once: a refresh or bookmark no longer repeats an old one
    set_today = s.get("limit_date") == _risk_today() and (s.get("limit") or 0) > 0
    rule = "Already set today: it can only be lowered until tomorrow." if (set_today and not RISK_ALLOW_RAISE_SAME_DAY) else ""
    html = (_RISK_PAGE.replace("__MSG__", msg_html).replace("__PNL__", v["pnl_html"])
            .replace("__STATUS__", v["status_html"]).replace("__LIMIT__", v["limit_text"])
            .replace("__FLOOR__", v["floor_text"]).replace("__POLL__", v["poll"]).replace("__LOCKED__", v["locked_at"])
            .replace("__REPORT__", v["report"]).replace("__RULE__", rule)
            .replace("__INPUT__", str(int(s["limit"])) if (s.get("limit") or 0) > 0 else "")
            .replace("__SECRET__", secret))
    return html, 200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"}


@app.route("/risk/<secret>/status", methods=["GET"])
def risk_status_json(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    return jsonify(_risk_view(risk_status()))


@app.route("/risk/<secret>/floor", methods=["POST"])
def risk_floor(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    from urllib.parse import quote
    try:
        val = float(str(request.form.get("floor", "")).replace(",", "").strip())
    except ValueError:
        return redirect(f"/risk/{secret}?m=" + quote("Enter a number."), code=303)
    ok, message = risk_set_floor(val)
    return redirect(f"/risk/{secret}?m=" + quote(message), code=303)


@app.route("/risk/<secret>/targets_toggle", methods=["POST"])
def risk_targets_toggle(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    from urllib.parse import quote
    ok, message = risk_set_targets_on(str(request.form.get("on", "0")) == "1")
    return redirect(f"/risk/{secret}?m=" + quote(message), code=303)


@app.route("/risk/<secret>/target", methods=["POST"])
def risk_target(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    from urllib.parse import quote
    try:
        val = float(str(request.form.get("amount", "")).replace(",", "").strip() or "nan")
        if val != val:
            raise ValueError
    except ValueError:
        return redirect(f"/risk/{secret}?m=" + quote("Enter a number."), code=303)
    ok, message = risk_set_target(str(request.form.get("key", "")), str(request.form.get("kind", "")), val)
    return redirect(f"/risk/{secret}?m=" + quote(message), code=303)


@app.route("/risk/<secret>/enable", methods=["POST"])
def risk_enable(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    from urllib.parse import quote
    ok, message = risk_set_enabled(str(request.form.get("on", "1")) == "1")
    return redirect(f"/risk/{secret}?m=" + quote(message), code=303)


@app.route("/risk/<secret>/set", methods=["POST"])
def risk_set(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    from urllib.parse import quote
    try:
        val = float(str(request.form.get("limit", "")).replace(",", "").strip())
    except ValueError:
        return redirect(f"/risk/{secret}?m=" + quote("Enter a number."), code=303)
    ok, message = risk_set_limit(val)
    return redirect(f"/risk/{secret}?m=" + quote(message), code=303)


# --------------------------------------------------------------------------
# WEBHOOK
# --------------------------------------------------------------------------
def parse_num(alert: dict, key: str):
    val = alert.get(key)
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (TypeError, ValueError):
        return None


def resolve_mcx_qty(symbol: str, qty: int, fixed_lot: bool, alert_type: str,
                     entry_price=None, sl_price=None):
    """Turns an alert into the exact exchange quantity Dhan requires for an
    MCX order, handling both sizing modes:

    fixed_lot=True  -> qty IS the lot count directly (the script's "Use
    Fixed Lot Size" toggle is ON) - multiply by the real lot size and
    that's the order. Guarded by MCX_MAX_LOTS against an implausibly large
    "lot count", which usually means the toggle was actually left OFF.

    fixed_lot=False -> RISK-BASED, RECOMPUTED HERE rather than trusting the
    script's qty field. Given this alert's own entry_price/sl_price:
        stopDistance = abs(entry_price - sl_price)
        lots wanted  = RISK_RUPEES_MCX / (stopDistance * lot_size)
    Whatever that comes out to is what gets ordered - no separate lot
    ceiling on this path, since the risk amount itself already bounds it.
    If it comes out to less than 1 full lot - meaning even the smallest
    possible MCX order (1 lot) would risk more than RISK_RUPEES_MCX - the
    trade is skipped entirely rather than firing an oversized position.
    This is most likely on higher-value contracts (Gold, Silver) with a
    tight risk setting; Crude Oil/Natural Gas rarely hit this since their
    per-lot risk is much smaller. If entry/sl are missing, falls back to
    the older qty/lot_size algebra as a last resort.

    Returns (final_qty, None) on success, or (None, (action, extra_fields))
    to log and skip when this order shouldn't be placed.
    """
    lot_size = get_lot_size(symbol)
    if not lot_size:
        return None, ("SKIPPED_NO_LOT_SIZE", {
            "note": "could not find this symbol's MCX lot size in the scrip master - refusing to guess a quantity"})

    if lot_size < MCX_MIN_PLAUSIBLE_LOT_SIZE:
        return None, ("SKIPPED_IMPLAUSIBLE_LOT_SIZE", {
            "lot_size": lot_size, "min_plausible": MCX_MIN_PLAUSIBLE_LOT_SIZE,
            "note": "the scrip master reports a lot size smaller than any real MCX commodity contract "
                    "trades in - refusing to place an order off a number this suspicious (this is exactly "
                    "what produced the NATGASMINI qty=526 order Dhan's RMS rejected on 2026-09-29). "
                    "Verify this symbol's true lot size with Dhan before raising MCX_MIN_PLAUSIBLE_LOT_SIZE "
                    "or trading it again."})

    if fixed_lot:
        lots = int(round(qty))
        if lots > MCX_MAX_LOTS:
            return None, ("SKIPPED_MCX_LOTS_TOO_HIGH", {
                "lots": lots, "mcx_max_lots": MCX_MAX_LOTS,
                "note": "implausibly large lot count - check the script's Fixed Lot Size input"})
        if lots < 1:
            lots = 1
        final_qty = lot_size * lots
        return final_qty, None

    # Risk-based mode: recompute lots from this alert's own entry/sl prices
    # and RISK_RUPEES_MCX, rather than trusting the script's qty field.
    stop_dist = abs(entry_price - sl_price) if (entry_price is not None and sl_price is not None) else None
    if stop_dist and stop_dist > 0:
        lots = int(RISK_RUPEES_MCX // (stop_dist * lot_size))
        script_implied_lots = int(qty // lot_size) if qty else None
        if script_implied_lots is not None and script_implied_lots != lots:
            log_event({"action": "MISMATCH_QTY_VS_SCRIPT", "symbol": symbol,
                       "lots_from_script": script_implied_lots, "lots_recomputed": lots,
                       "entry_price": entry_price, "sl_price": sl_price, "lot_size": lot_size,
                       "risk_rupees_mcx": RISK_RUPEES_MCX,
                       "note": "using the recomputed lots, not the script's"})
    else:
        # Fallback: no entry/sl on this alert - use the old algebra against
        # whatever qty the script sent (script_qty / lot_size = lots).
        lots = int(qty // lot_size)

    if lots < 1:
        return None, ("SKIPPED_MCX_RISK_TOO_SMALL", {
            "lot_size": lot_size, "risk_rupees_mcx": RISK_RUPEES_MCX,
            "note": "even 1 lot would risk more than RISK_RUPEES_MCX at this stop distance - skipped rather than firing an oversized position"})
    final_qty = lots * lot_size
    return final_qty, None


@app.route("/webhook/<secret>", methods=["POST"])
def webhook(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403

    raw = request.get_data(as_text=True)
    try:
        alert = json.loads(raw)
    except Exception:
        log_event({"action": "BAD_PAYLOAD", "raw": raw[:500]})
        return jsonify({"error": "invalid json"}), 400

    symbol = alert.get("symbol", "")
    side = str(alert.get("side", "")).lower()
    alert_type = alert.get("type", "")
    sl_price = parse_num(alert, "sl")
    entry_price = parse_num(alert, "price")
    # ADDED 15 Nov 2026: SCANNER AUTO-TRADE. The stock scanner sends the same six signals it finds itself, with "source":"scanner". They go through exactly the same
    # checks as a TradingView alert (risk lock, 09:30 start, 14:30 cutoff, cooldowns, Super Order, SL cascade) but ALWAYS for 1 SHARE, never flip an open position,
    # and are skipped when the stock already has a position or a waiting entry (so a TradingView signal is never blocked by, or doubled with, a scanner one).
    _from_scanner = str(alert.get("source", "")).strip().lower() == "scanner"
    if _from_scanner:
        try:                                                 # remembered for the scanner CSV log (written in _scanner_csv_after, whatever the outcome)
            from flask import g as _g
            _g.scn = {"symbol": symbol, "type": alert_type, "side": side, "price": entry_price, "sl": sl_price}
        except Exception:
            pass
        if not _scanner_auto_on():
            log_event({"action": "SCANNER_SIGNAL_IGNORED_SWITCH_OFF", "symbol": symbol, "type": alert_type})
            return jsonify({"status": "scanner_auto_off"}), 200
        if alert_type not in CONFIRMED_ENTRY_TYPES or alert_type in ("CONFIRMED_BUY", "CONFIRMED_SELL"):
            log_event({"action": "SCANNER_SIGNAL_TYPE_NOT_TRADED", "symbol": symbol, "type": alert_type})
            return jsonify({"status": "scanner_type_not_traded"}), 200
    try:                                                     # ADDED 14 Nov: record the signal on /signals/<secret> + phone alert (before any risk guard; places no order)
        import signals_feed as _sf
        if not _from_scanner:                                # a scanner-sent signal is already on the scanner's own page
            _sf.record(alert)
    except Exception as _sf_e:
        log.warning(f"signals feed record failed: {_sf_e}")
    # ADDED 14 Nov 2026: the four new signals (VS_SELL, VS_BUY, SPURT_CHOCH_BUY/SELL) only send t1 and t2. Fill T3-T6 here as
    # 3R..6R of the entry-to-SL distance (same maths as manual trades) so they get the full T1->T6 SL cascade like every other trade.
    # Bridge-side only: nothing in the Pine alert changes, and any t-level the alert already carries is kept as sent.
    if (alert_type in NEW_SIGNAL_TYPES or _from_scanner) and entry_price is not None and sl_price is not None and abs(entry_price - sl_price) > 0:
        try:
            _rt = _compute_r_multiple_targets(entry_price, sl_price, "BUY" if side == "buy" else "SELL")
            for _i, _k in enumerate(("t1", "t2", "t3", "t4", "t5", "t6")):
                if parse_num(alert, _k) is None:
                    alert[_k] = _rt[_i]
        except Exception as _e:
            log_event({"action": "NEW_SIGNAL_TARGET_FILL_FAILED", "error": str(_e)})
    # UPDATED 2026-09-29 (T6 symmetry, Khagen's design, mirrors the Delta/
    # crypto bridge): both Section A and Section B confirmed alerts now
    # send a "t6" field as their real final/far target (Section A's cascade
    # goes out to T6 same as Section B's does now) - prefer it, falling
    # back to "t5" then "t4" for any older alert that doesn't carry t6 yet.
    target_price = parse_num(alert, "t6")
    if target_price is None:
        target_price = parse_num(alert, "t5")
    if target_price is None:
        target_price = parse_num(alert, "t4")
    # "t2" was the near leg's target for the old 50/50 partial-exit split
    # (removed 2026-09-29 - see open_position()). No longer used to place
    # an order, but still parsed/passed through in case future logging
    # wants it.
    near_target_price = parse_num(alert, "t2")
    # ADDED 2026-09-30 (Khagen's request, mirrors the same fix just made on
    # the Delta/crypto bridge): the FULL T1-T6 cascade, stored against the
    # position so the bridge-side cascade watchdog (_check_cascade_targets(),
    # see below) can independently watch for each T-level being touched and
    # fire the same SL move _handle_move_sl_to_cost() does - a backup path
    # that works even if the MOVE_SL_TO_COST/T1/T2/T3/T4 alert itself never
    # arrives from TradingView.
    all_targets = [parse_num(alert, "t1"), near_target_price, parse_num(alert, "t3"),
                   parse_num(alert, "t4"), parse_num(alert, "t5"), parse_num(alert, "t6")]
    qty_raw = alert.get("qty", 1)
    try:
        qty_from_script = max(1, round(float(qty_raw)))
    except (TypeError, ValueError):
        qty_from_script = 1
    # True -> qty above is a LOT COUNT (script's "Use Fixed Lot Size" is ON).
    # False -> qty above is a risk-based "share-style" count (riskRupees /
    # stop-distance, same formula stocks use) - see resolve_mcx_qty() for
    # what this means for MCX. Missing entirely (older alert, before this
    # field existed) defaults to True - the original, always-lots behavior.
    fixed_lot = str(alert.get("fixed_lot", "true")).strip().lower() == "true"
    if _from_scanner:                                        # scanner auto-trade is 1 share only (SCANNER_AUTO_QTY), whatever the alert says
        qty_from_script, fixed_lot = SCANNER_AUTO_QTY, True

    # STOCK (NSE_EQ) RISK RECHECK: in risk-based mode, recompute the share
    # count from this alert's own entry/sl prices and RISK_RUPEES_STOCKS
    # instead of trusting the script's qty field - see RISK_RUPEES_STOCKS
    # above. Fixed-lot mode (fixed_lot=true) is left as-is: the script's qty
    # IS the exact share count the chart is configured to use, not a risk
    # calculation to double check. MCX has its own version of this same
    # recheck inside resolve_mcx_qty() below.
    qty = qty_from_script
    if not fixed_lot and entry_price is not None and sl_price is not None:
        stop_dist = abs(entry_price - sl_price)
        if stop_dist > 0:
            qty = max(1, round(RISK_RUPEES_STOCKS / stop_dist))
            if qty_from_script != qty:
                log_event({"action": "MISMATCH_QTY_VS_SCRIPT", "symbol": symbol,
                           "qty_from_script": qty_from_script, "qty_recomputed": qty,
                           "entry_price": entry_price, "sl_price": sl_price,
                           "risk_rupees_stocks": RISK_RUPEES_STOCKS,
                           "note": "using the recomputed qty, not the script's"})

    log_event({"action": "ALERT_RECEIVED", "alert": alert})

    # --- CASCADE TRAILING SL: "MOVE_SL_TO_COST" (T1 hit -> shift SL to
    # breakeven), "MOVE_SL_TO_T1"/"T2"/"T3"/"T4" (each subsequent T-level
    # hit -> shift SL to the previous one) - ADDED 2026-09-29, mirrors the
    # Delta/crypto bridge. All handled the same way: _handle_move_sl_to_cost
    # already just moves the SL to whatever "price" the alert carries, so
    # no logic change was needed here beyond accepting the new alert types.
    # Not a new entry, so no cutoff-time check applies here - an open
    # trade's own SL can move any time.
    if alert_type in ("MOVE_SL_TO_COST", "MOVE_SL_TO_T1", "MOVE_SL_TO_T2", "MOVE_SL_TO_T3", "MOVE_SL_TO_T4"):
        # ADDED 2026-09-30: stage number for the cascade_stage guard in
        # _handle_move_sl_to_cost() - lets it tell whether the bridge's own
        # watchdog (_check_cascade_targets() below) already applied this
        # same move (or a later one) a few seconds earlier.
        cascade_stage = {"MOVE_SL_TO_COST": 1, "MOVE_SL_TO_T1": 2, "MOVE_SL_TO_T2": 3,
                          "MOVE_SL_TO_T3": 4, "MOVE_SL_TO_T4": 5}.get(alert_type)
        # FIX 2026-09-28 (real incident: COALINDIA's MOVE_SL_TO_COST alert
        # showed "Webhook delivery failed - request took too long and timed
        # out" on TradingView's side TWICE today, even though the bridge's
        # own logs show it eventually completed and responded 200 each time.
        # Root cause: this whole handler used to run INLINE, in the request/
        # response cycle itself - get_security_id_and_segment() (can trigger
        # a scrip-master rebuild), get_position_size(), a full GET
        # /super/orders scan for reconcile, then a PUT per leg to modify its
        # SL - several sequential real network calls to Dhan, all before
        # ever responding to TradingView. Any one of those being a bit slow
        # is enough to blow past TradingView's webhook timeout. This is the
        # exact same failure mode already fixed for fresh entries elsewhere
        # in this file (open_position runs in a background thread for
        # exactly this reason) - MOVE_SL_TO_COST just never got the same
        # treatment. Now it does: respond immediately, do the real work in
        # a background thread.
        threading.Thread(target=_handle_move_sl_to_cost, args=(symbol, alert),
                         kwargs={"stage": cascade_stage, "source": "tradingview"}, daemon=True).start()
        return jsonify({"status": "sl_move_attempted"}), 200


    # --- Re-entry: CHANGED 2026-09-25 - this bridge NEVER auto-executes a
    # re-entry anymore, regardless of the script's own "auto_execute" field
    # (its "Enable Re-Entry Auto-Execution" toggle). Khagen wants to
    # evaluate every re-entry himself and take it manually on Dhan if he
    # wants it - this still logs it and still reaches your phone/email/app
    # as a normal TradingView alert notification (nothing about the alert
    # itself changed), it just never places a real order on its own. The
    # old conditional auto-execute path (security id lookup, cutoff/start
    # time checks, MCX lot sizing, anti-stacking, open_position call) is
    # intentionally removed rather than just left unreachable, so there's
    # no dead code that could be accidentally re-enabled by a future typo.
    if is_re_entry_type(alert_type):
        log_event({"action": "RE_ENTRY_LOGGED_ONLY_MANUAL_MODE", "type": alert_type, "symbol": symbol,
                   "side": side, "sl": sl_price, "price": entry_price,
                   "note": "re-entry alerts are informational only - take it manually on Dhan if wanted"})
        return jsonify({"status": "logged_manual_reentry"}), 200

    # --- ADDED 2026-10-01: Section B (CONFIRMED_BUY/CONFIRMED_SELL) is
    # logged-only, same treatment as a re-entry alert, unless
    # SECTION_B_AUTO_EXECUTE=true - see that env var's definition above for
    # the backtest numbers behind this. Checked before the CONFIRMED_ENTRY_
    # TYPES block below so Section A (Sweep/Order Block confirmed entries)
    # is completely unaffected either way.
    if alert_type in SECTION_B_CONFIRMED_TYPES and not SECTION_B_AUTO_EXECUTE:
        log_event({"action": "SECTION_B_LOGGED_ONLY", "type": alert_type, "symbol": symbol,
                   "side": side, "sl": sl_price, "price": entry_price,
                   "note": "Section B (Unified) confirmed entries are logged-only per Khagen's backtest "
                            "(39.5% WR / PF 0.84 fresh) - take it manually on Dhan if wanted, or set "
                            "SECTION_B_AUTO_EXECUTE=true in Render env vars to restore auto-execution"})
        return jsonify({"status": "logged_section_b_disabled"}), 200

    # --- ONLY a confirmed buy/sell places a real order, and only before the
    # entry cutoff time (IST) for that symbol's segment - 2:30pm for stocks
    # (NSE_EQ), 9:30pm for commodities (MCX_COMM). Past the cutoff, it's
    # logged like any other alert - no order, whatever the signal.
    if alert_type in CONFIRMED_ENTRY_TYPES:
        if risk_is_locked():
            log_event({"action": "SKIPPED_RISK_LOCKED", "symbol": symbol, "type": alert_type,
                       "note": "daily loss limit hit today - no new entries"})
            return jsonify({"status": "logged_risk_locked"}), 200
        security_id, segment = get_security_id_and_segment(symbol)
        if not security_id:
            log_event({"action": "SKIPPED_NO_SECURITY_ID", "symbol": symbol})
            return jsonify({"status": "logged_no_security_id"}), 200

        if _from_scanner and segment != "NSE_EQ":
            log_event({"action": "SCANNER_SIGNAL_NOT_NSE_EQ", "symbol": symbol, "segment": segment})
            return jsonify({"status": "scanner_only_nse_eq"}), 200

        if past_entry_cutoff(segment):
            log_event({"action": "SKIPPED_PAST_CUTOFF", "type": alert_type, "segment": segment,
                       "cutoff_ist": cutoff_for(segment), "now_ist": datetime.now(IST).strftime("%H:%M:%S")})
            return jsonify({"status": "logged_past_cutoff"}), 200

        if before_entry_start(segment):
            log_event({"action": "SKIPPED_BEFORE_START", "type": alert_type, "segment": segment,
                       "start_ist": start_time_for(segment), "now_ist": datetime.now(IST).strftime("%H:%M:%S"),
                       "note": "too early - inside the opening-volatility window, no new orders yet"})
            return jsonify({"status": "logged_before_start"}), 200

        if _symbol_limit_blocks(security_id, segment, qty):          # ADDED 16 Nov: 4 real symbols a day (1-share tests are not counted)
            log_event({"action": "SKIPPED_DAILY_SYMBOL_LIMIT", "symbol": symbol, "type": alert_type, "limit": TRADE_SYMBOLS_PER_DAY})
            try:
                notify("[Dhan] Signal skipped - no symbols left today", f"{symbol} {alert_type} was NOT taken: all {TRADE_SYMBOLS_PER_DAY} symbols for today are already used.", priority="high")
            except Exception:
                pass
            return jsonify({"status": "logged_daily_symbol_limit"}), 200

        # --- MCX only: qty received here means one of two things depending
        # on the script's "Use Fixed Lot Size" toggle for this commodity
        # (sent as "fixed_lot" on the alert) - see resolve_mcx_qty()'s
        # docstring for the full explanation of both modes, including why
        # a signal can be skipped entirely in risk-based mode when even 1
        # lot would risk more than the configured amount.
        if segment == "MCX_COMM":
            original_qty = qty_from_script
            qty, skip = resolve_mcx_qty(symbol, qty_from_script, fixed_lot, alert_type,
                                        entry_price=entry_price, sl_price=sl_price)
            if skip:
                action, extra = skip
                log_event({"action": action, "symbol": symbol, **extra})
                return jsonify({"status": "logged_" + action.lower()}), 200
            log_event({"action": "MCX_QTY_OVERRIDDEN", "symbol": symbol,
                       "script_qty": original_qty, "fixed_lot": fixed_lot, "final_qty": qty})

        # Anti-stacking: ask Dhan directly whether this exact security
        # already has an open position before firing a fresh entry. Without
        # this, a new CONFIRMED signal on a symbol that's already open (from
        # an earlier signal, or a manual trade) would stack/net on top of it
        # instead of being skipped.
        current_size = get_position_size(security_id, segment)
        if current_size is None:
            log_event({"action": "SKIPPED_POSITION_LOOKUP_FAILED", "symbol": symbol})
            return jsonify({"status": "logged_position_lookup_failed"}), 200
        if _from_scanner:                                    # never stack on, flip or double an existing / waiting trade of this stock (e.g. the TradingView one)
            with pending_lock:
                _waiting = any(str(v.get("symbol")) == str(symbol) for v in pending_entries.values())
            if current_size != 0 or _waiting:
                log_event({"action": "SCANNER_SIGNAL_SKIPPED_STOCK_BUSY", "symbol": symbol, "type": alert_type, "position": current_size, "waiting_entry": _waiting})
                return jsonify({"status": "scanner_skipped_busy"}), 200
            log_event({"action": "SCANNER_SIGNAL_TRADING", "symbol": symbol, "type": alert_type, "side": side, "qty": qty, "price": entry_price, "sl": sl_price})

        # lot_size only matters for MCX (splitting must respect whole lots,
        # not raw units) - None for NSE_EQ, where qty is already a plain
        # share count and can split arbitrarily. Computed once here since
        # both the flip path and the normal entry path below need it.
        lot_size_for_split = get_lot_size(symbol) if segment == "MCX_COMM" else None

        if current_size != 0:
            # FLIP SUPPORT - ADDED 2026-09-27: Khagen's manual backtest counted
            # every confirmed signal, assuming each new OPPOSITE-direction
            # signal closes whatever's open and takes the new trade - the same
            # behavior the legacy flip-based approach assumed. This used to
            # unconditionally skip ANY confirmed signal while in a position.
            # Now: a signal on the SAME side as the current position is still
            # skipped (already positioned that direction - nothing to do); a
            # signal on the OPPOSITE side triggers a real flip - close the old
            # position + cancel its leg order(s), then open the new one.
            existing_side = "buy" if current_size > 0 else "sell"
            if existing_side == side:
                log_event({"action": "SKIPPED_ALREADY_IN_POSITION", "symbol": symbol, "type": alert_type,
                           "current_size": current_size,
                           "note": "a position is already open on Dhan for this security on the SAME side - "
                                    "this confirmed signal was skipped so it doesn't stack on top of it"})
                return jsonify({"status": "logged_already_in_position"}), 200

            log_event({"action": "FLIP_SIGNAL_DETECTED", "symbol": symbol, "type": alert_type,
                       "existing_side": existing_side, "new_side": side, "current_size": current_size,
                       "note": "confirmed signal is on the OPPOSITE side of the current position - closing "
                                "the old position and opening the new one (flip), matching the backtest"})
            threading.Thread(
                target=flip_and_open_position,
                args=(symbol, side, qty, sl_price, security_id, segment, entry_price, current_size),
                kwargs={"near_target_price": near_target_price, "far_target_price": target_price,
                        "lot_size": lot_size_for_split, "targets": all_targets},
                daemon=True,
            ).start()
            return jsonify({"status": "flip_attempted"}), 200

        # Runs in a background thread rather than inline: open_position()
        # now places a single atomic Super Order (near-instant), so this is
        # mostly just extra headroom against a slow Dhan API response - but
        # it's also what fixed an earlier real incident (several alerts,
        # including a GOLDM1 confirmed sell, showed "Webhook delivery
        # failed - request took too long and timed out" back when
        # open_position() did a blocking ~20s fill-poll inline). Keeping it
        # threaded means this webhook always responds in milliseconds
        # regardless of how long Dhan takes to answer.
        threading.Thread(
            target=open_position,
            args=(symbol, side, qty, sl_price, security_id, segment, entry_price),
            kwargs={"near_target_price": near_target_price, "far_target_price": target_price,
                    "lot_size": lot_size_for_split, "targets": all_targets},
            daemon=True,
        ).start()
        return jsonify({"status": "order_attempted"}), 200

    # --- DAILY_RESET_DONE: the script's own 8:30ish AM confirmation that it
    # wiped yesterday's leftover trade-tracking state and started the day
    # clean. No Dhan action, but pushed as a real notification (not just
    # logged) since Khagen specifically wants this one confirmed on his
    # phone, not just visible as a chart label. Fires once per symbol that
    # has an active alert on this script - normal, not a duplicate/bug.
    if alert_type == "DAILY_RESET_DONE":
        log_event({"action": "DAILY_RESET_CONFIRMED", "symbol": symbol})
        notify("[Dhan] Daily reset done", f"{symbol}'s indicator memory was reset for the new trading day - "
                                            "no leftover trades/re-entries carried over from yesterday.",
               priority="default")
        return jsonify({"status": "logged_daily_reset"}), 200

    # --- everything else: logged for your own alert purposes only, never
    # sent to Dhan. This covers the primary (non-CONFIRMED) Section A
    # signals, Section B's SSL_SWEEP/BSL_SWEEP raw triggers, SL_BREACHED,
    # PROFIT_TAKEN_T5, SL_TAKEN, and anything else not in
    # CONFIRMED_ENTRY_TYPES or handled by is_re_entry_type() above. A
    # RE-ENTRY alert with auto_execute=false already returned earlier, so
    # if one reaches here it means the "type" string didn't match
    # is_re_entry_type() at all - shouldn't normally happen.
    log_event({"action": "LOGGED_ONLY_NOT_SENT_TO_DHAN", "type": alert_type or side})
    return jsonify({"status": "logged"}), 200


@app.route("/health", methods=["GET"])
def health():
    with pending_lock:
        pending = dict(pending_entries)
    return jsonify({"status": "ok", "dry_run": DRY_RUN,
                     "dry_run_nse_eq": is_dry_run("NSE_EQ"), "dry_run_mcx_comm": is_dry_run("MCX_COMM"),
                     "dhan_token_status": token_status,
                     "static_ip_proxy_configured": bool(DHAN_PROXY_URL),
                     "risk": risk_status(),
                     "last_entries": last_entries, "pending_entries": pending})


# ADDED 2026-10-04 for the EOD scanner (eod_scanner.py) - serves its latest
# results as a webpage. Read-only, does not touch order placement, entry
# detection, or any other signal-generation logic anywhere in this file.
@app.route("/scanner", methods=["GET"])
def scanner_results():
    path = os.path.join(os.path.dirname(__file__), "results", "latest.html")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    with open(path) as f:
        return f.read()


# ADDED 2026-10-04 for CSV download links on the /scanner page - read-only,
# just serves files eod_scanner.py already writes to results/. Does not
# touch order placement or any entry/signal-generation logic.
@app.route("/scanner/confirmed.csv", methods=["GET"])
def scanner_confirmed_csv():
    path = os.path.join(os.path.dirname(__file__), "results", "latest.csv")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    return send_file(path, mimetype="text/csv", as_attachment=True,
                      download_name="eod_scanner_confirmed.csv")


@app.route("/scanner/watch.csv", methods=["GET"])
def scanner_watch_csv():
    path = os.path.join(os.path.dirname(__file__), "results", "latest_watch.csv")
    if not os.path.exists(path):
        return "No scan has run yet.", 404
    return send_file(path, mimetype="text/csv", as_attachment=True,
                      download_name="eod_scanner_watch.csv")


# ADDED 2026-10-04 - runs eod_scanner.py's scan once a day in a background
# thread INSIDE this same service/process (same pattern as the SL watchdog
# and manual-trade-detector threads above), so its results/ folder lands on
# the same filesystem the /scanner route just above reads from. A separate
# Render Cron Job would run as a different service with its own filesystem -
# its results would NOT show up on /scanner without extra shared-disk setup
# and extra Render cost, which this avoids entirely.
#
# Read-only and additive: this thread only imports and calls
# eod_scanner.run_scan() (itself a read-only scan, no order placement). It
# does not touch order placement, entry detection, or any other
# signal-generation logic in this file. Wrapped in its own try/except so a
# failure here (bad data, Dhan API hiccup, etc.) can never crash this
# process or affect the webhook/watchdog/manual-trade-detector threads.
#
# Runs well after the 15:10 squareoff and 15:30 NSE close (default 15:45
# IST), so there is nothing open for the SL watchdog to protect during the
# scan window. The scan's own per-symbol rate-limit sleeps naturally yield
# CPU between requests throughout its ~1-2hr runtime, so the other
# background threads still get to run rather than being starved by one
# long CPU-bound block.
EOD_SCANNER_AUTORUN = os.environ.get("EOD_SCANNER_AUTORUN", "true").strip().lower() == "true"
EOD_SCANNER_RUN_TIME = os.environ.get("EOD_SCANNER_RUN_TIME", "15:45")  # HH:MM, Asia/Kolkata


def _eod_scanner_loop():
    hh, mm = (int(x) for x in EOD_SCANNER_RUN_TIME.split(":"))
    ist = ZoneInfo("Asia/Kolkata")
    log_event({"action": "EOD_SCANNER_AUTORUN_STARTED", "run_time_ist": EOD_SCANNER_RUN_TIME})
    last_run_date = None
    while True:
        try:
            now = datetime.now(ist)
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if now >= target:
                target = target + timedelta(days=1)
            time.sleep(max((target - now).total_seconds(), 1))

            today = datetime.now(ist).date()
            if today == last_run_date:
                continue  # already ran today - guards against a short sleep-drift re-trigger
            log_event({"action": "EOD_SCANNER_AUTORUN_FIRING", "date": str(today)})
            try:
                import eod_scanner
                eod_scanner.run_scan()
                log_event({"action": "EOD_SCANNER_AUTORUN_DONE", "date": str(today)})
            except Exception as e:
                log_event({"action": "EOD_SCANNER_AUTORUN_ERROR", "error": str(e)})
            last_run_date = today
        except Exception as e:
            log.error(f"Error in EOD scanner autorun loop: {e}")
            time.sleep(60)


if EOD_SCANNER_AUTORUN:
    threading.Thread(target=_eod_scanner_loop, daemon=True, name="eod-scanner-autorun").start()


@app.route("/check_ip", methods=["GET"])
def check_ip():
    """Manual on-demand version of the same check that now runs
    automatically on a DH-905 'Invalid IP' order failure - hit this
    anytime to confirm what IP the proxy is currently presenting to the
    outside world, and compare it against what's whitelisted on Dhan and
    what StaticIP.in shows as the active IP."""
    if not DHAN_PROXY_URL:
        return jsonify({"static_ip_proxy_configured": False,
                         "note": "DHAN_PROXY_URL is not set - calls go to Dhan directly, unwhitelisted."})
    return jsonify({"static_ip_proxy_configured": True, "outbound_ip": check_outbound_ip()})


@app.route("/check_option_chain", methods=["GET"])
def check_option_chain():
    """DIAGNOSTIC - ADDED 2026-09-27, for building the new NSE options
    (Nifty/Bank Nifty) iron-condor strategy.

    Dhan's documentation (https://dhanhq.co/docs/v2/option-chain/) tells us
    the shape of the request but real accounts sometimes differ in small
    ways (exact field names, how far-out expiries are listed, etc), and we
    have no way to call Dhan's live API from inside this sandbox - the
    credentials only exist in Render's environment. So instead of guessing
    the real response shape and building the whole strategy against a
    guess, this is a one-off manual endpoint: hit it once (GET, no body),
    it resolves NIFTY/BANKNIFTY security IDs via scrip_master's
    get_index_security_id(), fetches each one's real expiry list and the
    option chain for the nearest expiry, and returns/logs the raw JSON so
    we can see the real field names before writing any strike-selection
    logic against them.

    Query params (all optional):
      ?index=NIFTY or ?index=BANKNIFTY  - check just one instead of both.

    NOTE: Dhan's option-chain APIs are rate-limited to 1 request per 3
    seconds TOTAL across expirylist + optionchain calls - this endpoint
    sleeps between its own calls to respect that, so a full run (2 indices
    x 2 calls each) takes a few seconds. Don't hit it in a tight loop.
    """
    which = request.args.get("index", "").strip().upper()
    indices = [which] if which in ("NIFTY", "BANKNIFTY") else ["NIFTY", "BANKNIFTY"]

    results = {}
    for idx_name in indices:
        sec_id = get_index_security_id(idx_name)
        entry = {"security_id": sec_id}
        if not sec_id:
            entry["error"] = "get_index_security_id() could not resolve this index - check scrip_master logs."
            results[idx_name] = entry
            log_event({"action": "CHECK_OPTION_CHAIN_INDEX_UNRESOLVED", "index": idx_name})
            continue

        try:
            expiry_resp = requests.post(
                f"{DHAN_BASE_URL}/optionchain/expirylist",
                headers=dhan_headers(),
                json={"UnderlyingScrip": sec_id, "UnderlyingSeg": "IDX_I"},
                timeout=15, proxies=DHAN_PROXIES,
            )
            entry["expirylist_status"] = expiry_resp.status_code
            expiry_data = expiry_resp.json() if expiry_resp.content else {}
            entry["expirylist_raw"] = expiry_data
            log_event({"action": "CHECK_OPTION_CHAIN_EXPIRYLIST", "index": idx_name,
                       "security_id": sec_id, "status": expiry_resp.status_code,
                       "body": expiry_resp.text[:1500]})
        except Exception as e:
            entry["expirylist_error"] = str(e)
            log_event({"action": "CHECK_OPTION_CHAIN_EXPIRYLIST_ERROR", "index": idx_name, "error": str(e)})
            results[idx_name] = entry
            continue

        expiries = (expiry_data or {}).get("data") if isinstance(expiry_data, dict) else None
        nearest_expiry = expiries[0] if isinstance(expiries, list) and expiries else None
        entry["nearest_expiry_used"] = nearest_expiry

        # respect Dhan's 1-request-per-3-seconds option-chain rate limit
        time.sleep(3)

        if nearest_expiry:
            try:
                chain_resp = requests.post(
                    f"{DHAN_BASE_URL}/optionchain",
                    headers=dhan_headers(),
                    json={"UnderlyingScrip": sec_id, "UnderlyingSeg": "IDX_I", "Expiry": nearest_expiry},
                    timeout=15, proxies=DHAN_PROXIES,
                )
                entry["optionchain_status"] = chain_resp.status_code
                chain_data = chain_resp.json() if chain_resp.content else {}
                # full chain can be large (every strike x CE/PE) - log a
                # truncated version but return the full thing in the
                # response so it can be inspected directly in the browser.
                entry["optionchain_raw"] = chain_data
                log_event({"action": "CHECK_OPTION_CHAIN_FETCHED", "index": idx_name,
                           "security_id": sec_id, "expiry": nearest_expiry,
                           "status": chain_resp.status_code, "body": chain_resp.text[:1500]})
            except Exception as e:
                entry["optionchain_error"] = str(e)
                log_event({"action": "CHECK_OPTION_CHAIN_FETCH_ERROR", "index": idx_name, "error": str(e)})

        results[idx_name] = entry
        # stay under the rate limit before the next index's calls too
        time.sleep(3)

    return jsonify(results)


# ADDED 15 Nov 2026 - SCANNER AUTO-TRADE switch. Default ON (as asked), 1 share per trade. The switch is saved on the disk and has a small page: /scanner_auto/<WEBHOOK_SECRET>
SCANNER_AUTO_QTY = 1
_SCANNER_AUTO_FILE = os.path.join(_risk_data_dir(), "scanner_auto.json")


def _scanner_auto_on() -> bool:
    try:
        with open(_SCANNER_AUTO_FILE, "r") as f:
            return bool(json.load(f).get("on", True))
    except Exception:
        return True


def _scanner_auto_set(on: bool):
    try:
        with open(_SCANNER_AUTO_FILE + ".tmp", "w") as f:
            json.dump({"on": bool(on), "at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")}, f)
        os.replace(_SCANNER_AUTO_FILE + ".tmp", _SCANNER_AUTO_FILE)
    except Exception as e:
        log_event({"action": "SCANNER_AUTO_SAVE_ERROR", "error": str(e)})


# Every scanner signal that reaches the bridge is written to scanner_auto_log.csv (on the disk) with what the bridge did with it: TRADED 1 share, or the reason it was skipped.
_SCANNER_CSV = os.path.join(_risk_data_dir(), "scanner_auto_log.csv")
_SCANNER_CSV_COLS = ["date", "time", "symbol", "type", "side", "price", "sl", "result", "reason"]
_scanner_csv_lock = threading.Lock()


@app.after_request
def _scanner_csv_after(resp):
    try:
        from flask import g as _g
        d = getattr(_g, "scn", None)
        if d is None:
            return resp
        _g.scn = None
        st = ""
        try:
            st = str((resp.get_json(silent=True) or {}).get("status", ""))
        except Exception:
            pass
        if not st:
            st = "error_http_%s" % resp.status_code
        res = "TRADED (1 share)" if st == "order_attempted" else ("ERROR" if st.startswith("error_") else "SKIPPED")
        now = datetime.now(IST)
        row = [now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"), d.get("symbol", ""), d.get("type", ""), d.get("side", ""),
               d.get("price", ""), d.get("sl", ""), res, st]
        import csv as _csv
        with _scanner_csv_lock:
            new = not os.path.exists(_SCANNER_CSV)
            with open(_SCANNER_CSV, "a", newline="") as f:
                w = _csv.writer(f)
                if new:
                    w.writerow(_SCANNER_CSV_COLS)
                w.writerow(row)
    except Exception as e:
        try:
            log_event({"action": "SCANNER_CSV_ERROR", "error": str(e)})
        except Exception:
            pass
    return resp


@app.route("/scanner_auto/<secret>/csv", methods=["GET"])
def scanner_auto_csv(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    try:
        with open(_SCANNER_CSV, "r", newline="") as f:
            data = f.read()
    except Exception:
        data = ",".join(_SCANNER_CSV_COLS) + "\n"
    return data, 200, {"Content-Type": "text/csv; charset=utf-8", "Content-Disposition": "attachment; filename=scanner_auto_log.csv", "Cache-Control": "no-store"}


@app.route("/scanner_auto/<secret>", methods=["GET"])
def scanner_auto_page(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    on = _scanner_auto_on()
    rows = ""
    try:
        import csv as _csv
        with open(_SCANNER_CSV, "r", newline="") as f:
            lines = list(_csv.DictReader(f))
        for d in reversed(lines[-15:]):
            rows += "<tr><td>%s %s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                d.get("date", ""), d.get("time", ""), d.get("symbol", ""), str(d.get("type", ""))[:26], d.get("result", ""), d.get("reason", ""))
    except Exception:
        pass
    msg = request.args.get("m", "")
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Scanner auto-trade</title>"
            "<style>body{font-family:system-ui,Arial,sans-serif;background:#0f1419;color:#e6e6e6;margin:0;padding:14px}.c{max-width:640px;margin:0 auto;background:#1a2129;border-radius:12px;padding:14px;margin-bottom:12px}"
            "button{padding:10px 18px;font-size:15px;border:0;border-radius:8px;color:#fff;font-weight:600;cursor:pointer}td,th{padding:4px 8px;text-align:left;font-size:13px;border-bottom:1px solid #27313b}</style></head><body>"
            "<div class='c'><h2>Scanner auto-trade</h2><p>The stock scanner's own ALL IN ONE PRO signals are traded by the bridge: <b>%d share per trade</b>, same risk rules as TradingView signals "
            "(daily/weekly locks, 09:30 start, 14:30 cutoff, cooldowns). Never flips or stacks on an open position, and is skipped when a TradingView signal already took the stock.</p>"
            "<p>Status: <b style='color:%s'>%s</b></p>%s"
            "<form method='post' action='/scanner_auto/%s/set'><input type='hidden' name='on' value='%s'><button style='background:%s'>%s</button></form></div>"
            "<div class='c'><b>Latest scanner signals seen</b> &nbsp; <a style='color:#9fb0c0' href='/scanner_auto/%s/csv'>Download CSV (all)</a><table><tr><th>Time</th><th>Stock</th><th>Type</th><th>Result</th><th>Reason</th></tr>%s</table></div></body></html>"
            ) % (SCANNER_AUTO_QTY, "#3ddc84" if on else "#ff5252", "ON" if on else "OFF", ("<p style='background:#27313b;padding:8px;border-radius:8px'>%s</p>" % msg.replace("<", "&lt;")[:150]) if msg else "",
                 secret, "0" if on else "1", "#c62828" if on else "#2e7d32", "Switch OFF" if on else "Switch ON", secret, rows or "<tr><td colspan=5>none yet</td></tr>"), 200, {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"}


@app.route("/scanner_auto/<secret>/set", methods=["POST"])
def scanner_auto_set(secret):
    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    from flask import redirect
    on = str(request.form.get("on", "")).strip() == "1"
    _scanner_auto_set(on)
    log_event({"action": "SCANNER_AUTO_SWITCH", "on": on})
    return redirect(f"/scanner_auto/{secret}?m=" + ("Scanner auto-trade is ON" if on else "Scanner auto-trade is OFF").replace(" ", "+"), code=303)


# ADDED 2026-11-12 - swing (delivery) holdings tracker: SL / target / profit-lock on /swing/<WEBHOOK_SECRET>. Alert only, EXCEPT for a stock whose "Auto-sell" box you tick (15 Nov): then it places a MARKET SELL (CNC).
def _swing_sell(sym, sid, seg, qty):
    """Real MARKET SELL (CNC) of a held stock for the swing page's Auto-sell. Returns (ok, text). Never raises."""
    payload = {
        "dhanClientId": DHAN_CLIENT_ID,
        "correlationId": safe_correlation_id(sym, f"SWG{int(time.time()) % 100000}"),
        "transactionType": "SELL",
        "exchangeSegment": seg,
        "productType": "CNC",
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": str(sid),
        "quantity": int(qty),
        "price": 0,
    }
    if is_dry_run(seg):
        log_event({"action": "SWING_DRY_RUN_WOULD_SELL", "payload": payload})
        return True, "DRY RUN - no real order was sent"
    try:
        resp = requests.post(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), json=payload, timeout=15, proxies=DHAN_PROXIES)
        body = safe_json(resp)
        log_event({"action": "SWING_SELL_ORDER", "symbol": sym, "payload": payload, "http_status": resp.status_code, "response": body})
        oid = body.get("orderId") if isinstance(body, dict) else None
        if resp.ok and oid:
            return True, f"Dhan order id {oid} ({body.get('orderStatus', '')})"
        return False, f"HTTP {resp.status_code} {str(body)[:180]}"
    except Exception as e:
        return False, f"error: {e}"


# ---------------------------------------------------------------------------------------------------------------------------------------------
# ADDED 16 Nov 2026 - TIME STOP (Khagen's rule): every open INTRADAY position is looked at once, after TIME_STOP_CANDLES five-minute candles
# (default 8 = 40 minutes) since the bridge first saw it open. If it is NOT in profit at that moment (price not beyond the entry in the trade's
# direction), the bridge cancels its stop / target orders and exits at market. A position that IS in profit then is left alone (SL / T1-T6 cascade
# look after it) and is not checked again. It never touches entries, signals or the SL cascade; it only adds this one extra exit.
# Defaults are built in (no Render setting needed). To switch it off set TIME_STOP_ON=false in Render (optional).
TIME_STOP_ON = os.environ.get("TIME_STOP_ON", "true").strip().lower() != "false"
TIME_STOP_CANDLES = 8                  # five-minute candles to wait (7-8 asked): 8 x 5 = 40 minutes
TIME_STOP_MIN_PROFIT_PCT = 0.0         # "in profit" = price beyond the entry by more than this %; at or below it the position is exited
TIME_STOP_START_MIN = 9 * 60 + 30      # only between 09:30 ...
TIME_STOP_END_MIN = 15 * 60 + 10       # ... and 15:10 IST (the broker's own square-off follows at about 15:20)
TIME_STOP_POLL_SECONDS = 20
_TIME_STOP_FILE = os.path.join(_risk_data_dir(), "time_stop.json")
_time_stop = {"day": "", "pos": {}}    # "segment:securityId" -> {"first": epoch seconds, "done": bool, "tries": int}
_time_stop_lock = threading.Lock()


def _time_stop_save():
    try:
        with open(_TIME_STOP_FILE + ".tmp", "w") as f:
            json.dump(_time_stop, f)
        os.replace(_TIME_STOP_FILE + ".tmp", _TIME_STOP_FILE)
    except Exception as e:
        log_event({"action": "TIME_STOP_SAVE_ERROR", "error": str(e)})


def _time_stop_load():
    try:
        with open(_TIME_STOP_FILE, "r") as f:
            d = json.load(f)
        if isinstance(d, dict) and isinstance(d.get("pos"), dict):
            _time_stop.update(day=str(d.get("day", "")), pos=d["pos"])
    except Exception:
        pass


def _time_stop_cancel_orders(security_id, segment):
    """Cancels every live stop / target order of the security so none is left to open a new position after the exit. Returns False if the lookup failed."""
    live = collect_live_sl_orders(security_id, segment)
    if live is None:
        return False
    for o in live:
        try:
            if o.get("kind") == "regular":
                cancel_regular_order(str(o["order_id"]), segment)
            else:
                cancel_super_order_leg(str(o["order_id"]), "STOP_LOSS_LEG", segment)
                cancel_super_order_leg(str(o["order_id"]), "TARGET_LEG", segment)
        except Exception as e:
            log_event({"action": "TIME_STOP_CANCEL_ERROR", "security_id": security_id, "error": str(e)})
    return True


def _time_stop_check_once():
    now = datetime.now(IST)
    mins = now.hour * 60 + now.minute
    today = now.strftime("%Y-%m-%d")
    with _time_stop_lock:
        if _time_stop.get("day") != today:
            _time_stop.update(day=today, pos={})
            _time_stop_save()
    if mins < TIME_STOP_START_MIN - 5 or mins > TIME_STOP_END_MIN + 20:
        return
    resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
    positions = safe_json(resp)
    if not resp.ok or not isinstance(positions, list):
        return
    open_keys = set()
    changed = False
    for p in positions:
        try:
            seg = p.get("exchangeSegment")
            net = _risk_net_qty(p)
            if seg not in ("NSE_EQ", "MCX_COMM") or net == 0:
                continue
            if str(p.get("productType", "")).upper() == "CNC":      # swing / delivery positions are not time-stopped
                continue
            sid = str(p.get("securityId"))
            k = f"{seg}:{sid}"
            open_keys.add(k)
            with _time_stop_lock:
                st = _time_stop["pos"].get(k)
                if st is None:
                    st = _time_stop["pos"][k] = {"first": time.time(), "done": False, "tries": 0}
                    changed = True
            if st.get("done") or mins < TIME_STOP_START_MIN or mins > TIME_STOP_END_MIN:
                continue
            if time.time() - float(st["first"]) < TIME_STOP_CANDLES * 5 * 60:
                continue
            symbol = str(p.get("tradingSymbol") or sid)
            cost = _risk_f(p.get("costPrice") or (p.get("buyAvg") if net > 0 else p.get("sellAvg")))
            ltp = get_ltp(sid, seg)
            if cost <= 0 or ltp is None:
                continue                                              # no reliable price: try again next poll, never exit blind
            pnl_pct = (float(ltp) - cost) / cost * 100.0 * (1 if net > 0 else -1)
            if pnl_pct > TIME_STOP_MIN_PROFIT_PCT:
                st["done"] = True                                     # in profit at the deadline: leave it to the SL / cascade
                changed = True
                log_event({"action": "TIME_STOP_IN_PROFIT_LEFT_ALONE", "symbol": symbol, "cost": cost, "ltp": ltp, "pnl_pct": round(pnl_pct, 3)})
                continue
            if not _time_stop_cancel_orders(sid, seg) and st.get("tries", 0) < 3:
                st["tries"] = st.get("tries", 0) + 1                  # could not read the open orders: retry (3 times) before exiting
                changed = True
                continue
            close_side = "SELL" if net > 0 else "BUY"
            r = place_order(sid, close_side, abs(net), "MARKET", exchange_segment=seg,
                            correlation_id=safe_correlation_id(symbol, f"TSTOP-{int(time.time())}"), product_type=p.get("productType"))
            ok = isinstance(r, dict) and r.get("orderId") is not None
            st["done"] = bool(ok)
            st["tries"] = st.get("tries", 0) + 1
            changed = True
            log_event({"action": "TIME_STOP_EXIT" if ok else "TIME_STOP_EXIT_FAILED", "symbol": symbol, "qty": net, "cost": cost, "ltp": ltp,
                       "pnl_pct": round(pnl_pct, 3), "candles": TIME_STOP_CANDLES, "response": r})
            if ok:
                last_entries.pop(symbol, None)
                save_state()
                notify("[Dhan] Time stop - exited " + symbol,
                       f"{symbol}: {TIME_STOP_CANDLES} five-minute candles passed and the trade was not in profit (entry {cost:.2f}, price {float(ltp):.2f}, {pnl_pct:+.2f}%). Its SL / target orders were cancelled and the position was closed at market.",
                       priority="high")
            else:
                notify("[Dhan] Time stop FAILED - " + symbol,
                       f"{symbol}: should be exited by the time rule ({TIME_STOP_CANDLES} candles, not in profit) but the market exit was refused ({r}). Its SL / target orders may already be cancelled - CHECK IT ON DHAN NOW.",
                       priority="urgent")
        except Exception as e:
            log_event({"action": "TIME_STOP_ERROR", "error": str(e)})
    with _time_stop_lock:
        for k in list(_time_stop["pos"].keys()):
            if k not in open_keys:                                    # the position is gone: forget it, so a re-entry gets a fresh clock
                _time_stop["pos"].pop(k, None)
                changed = True
        if changed:
            _time_stop_save()


def _time_stop_loop():
    _time_stop_load()
    log_event({"action": "TIME_STOP_STARTED", "enabled": TIME_STOP_ON, "candles": TIME_STOP_CANDLES})
    while True:
        time.sleep(TIME_STOP_POLL_SECONDS)
        if not TIME_STOP_ON:
            continue
        try:
            _time_stop_check_once()
        except Exception as e:
            log_event({"action": "TIME_STOP_LOOP_ERROR", "error": str(e)})


threading.Thread(target=_time_stop_loop, daemon=True, name="time-stop").start()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# ADDED 16 Nov 2026 - MAX 4 SYMBOLS A DAY (Khagen's rule). Once 4 different symbols have been traded today, no signal for a 5th symbol is taken, and when
# everything is closed and no order is pending the bridge switches Dhan's Kill Switch ON (the same routine the profit lock / loss limit use, locked for the day).
# 1-share NSE trades (the scanner auto-trade and manual tests) are NOT counted and are never blocked. A symbol already traded today can still be re-entered.
# Counted from Dhan's own /positions rows of today (so manual trades count too). Defaults are built in; TRADE_SYMBOLS_PER_DAY=0 in Render switches it off (optional).
TRADE_SYMBOLS_PER_DAY = int(os.environ.get("TRADE_SYMBOLS_PER_DAY", "4") or "4")
TEST_QTY_NOT_COUNTED = 1               # an NSE share trade of this many shares or fewer (bought and sold) is a test and is not counted
_symlim = {"day": "", "tries": 0, "last": 0.0, "told": False, "cache_t": 0.0, "cache": None}


def _symlim_real_symbols():
    """-> (set of 'segment:securityId' of the real symbols traded today, list of raw position rows) or (None, None) if Dhan could not be read."""
    resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
    rows = safe_json(resp)
    if not resp.ok or not isinstance(rows, list):
        return None, None
    real = set()
    for p in rows:
        try:
            seg = p.get("exchangeSegment")
            if seg not in ("NSE_EQ", "MCX_COMM"):
                continue
            if str(p.get("productType", "")).upper() == "CNC":
                continue
            bq = max(int(float(p.get("dayBuyQty") or 0)), int(float(p.get("buyQty") or 0)))
            sq = max(int(float(p.get("daySellQty") or 0)), int(float(p.get("sellQty") or 0)))
            tot = max(bq, sq, abs(_risk_net_qty(p)))
            if tot <= 0:
                continue
            if seg == "NSE_EQ" and tot <= TEST_QTY_NOT_COUNTED:
                continue
            real.add(f"{seg}:{p.get('securityId')}")
        except Exception:
            continue
    return real, rows


def _symbol_limit_blocks(security_id, segment, qty) -> bool:
    """True when this new entry would be a 5th real symbol today. Never raises; if Dhan cannot be read it does not block."""
    try:
        if TRADE_SYMBOLS_PER_DAY <= 0:
            return False
        if segment == "NSE_EQ" and int(qty or 0) <= TEST_QTY_NOT_COUNTED:
            return False                                   # a 1-share test trade
        real, _rows = _symlim_real_symbols()
        if real is None:
            return False
        return f"{segment}:{security_id}" not in real and len(real) >= TRADE_SYMBOLS_PER_DAY
    except Exception as e:
        log_event({"action": "SYMBOL_LIMIT_CHECK_ERROR", "error": str(e)})
        return False


def _symlim_pending_orders() -> bool:
    try:
        r1 = requests.get(f"{DHAN_BASE_URL}/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        r2 = requests.get(f"{DHAN_BASE_URL}/super/orders", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
        if not (r1.ok and r2.ok):
            return True                                    # cannot tell: treat as pending and look again later
        live = ("TRANSIT", "PENDING", "PART_TRADED", "TRIGGERED")
        for o in (safe_json(r1) or []):
            if str(o.get("orderStatus", "")).upper() in live:
                return True
        for o in (safe_json(r2) or []):
            if str(o.get("orderStatus", "")).upper() in live:
                return True
        return False
    except Exception:
        return True


def _symlim_check_once():
    now = datetime.now(IST)
    today = now.strftime("%Y-%m-%d")
    if _symlim["day"] != today:
        _symlim.update(day=today, tries=0, last=0.0, told=False, seen=0)
    if TRADE_SYMBOLS_PER_DAY <= 0 or _symlim["tries"] >= 5 or risk_state.get("kill_on"):
        return
    if time.time() - _symlim["last"] < 90:
        return
    real, rows = _symlim_real_symbols()
    if real is not None and len(real) != _symlim.get("seen", 0):     # ADDED 16 Nov: phone alert every time a new symbol is used - how many are left today
        _symlim["seen"] = len(real)
        if len(real) > 0:
            left = max(0, TRADE_SYMBOLS_PER_DAY - len(real))
            names = ", ".join(sorted(str(p.get("tradingSymbol") or p.get("securityId")) for p in rows
                                     if f"{p.get('exchangeSegment')}:{p.get('securityId')}" in real))
            notify(f"[Dhan] Symbols today: {len(real)}/{TRADE_SYMBOLS_PER_DAY} used, {left} left",
                   f"Traded today: {names}. " + (f"{left} more symbol(s) can still be traded today." if left else "That was the LAST symbol - no more entries today."),
                   priority="high")
    if real is None or len(real) < TRADE_SYMBOLS_PER_DAY:
        return
    if any(_risk_net_qty(p) != 0 for p in rows):           # something (even a 1-share test) is still open: wait until everything is flat
        if not _symlim["told"]:
            _symlim["told"] = True
            notify("[Dhan] 4 symbols traded today", f"{len(real)} different symbols have been traded today. No 5th symbol will be taken. When everything is closed the Kill Switch will be switched ON.", priority="high")
        return
    for p in rows:                                          # leftover stop / target orders of closed positions would stop Dhan accepting the Kill Switch
        try:
            if p.get("exchangeSegment") in ("NSE_EQ", "MCX_COMM"):
                _time_stop_cancel_orders(str(p.get("securityId")), p.get("exchangeSegment"))
        except Exception:
            pass
    if _symlim_pending_orders():
        return
    _symlim["last"] = time.time()
    _symlim["tries"] += 1
    log_event({"action": "SYMBOL_LIMIT_KILL_SWITCH", "symbols": sorted(real), "try": _symlim["tries"]})
    _risk_kill_switch(f"{TRADE_SYMBOLS_PER_DAY}-symbol daily limit", quiet=_symlim["tries"] < 5)


def _symlim_loop():
    while True:
        time.sleep(30)
        try:
            _symlim_check_once()
        except Exception as e:
            log_event({"action": "SYMBOL_LIMIT_LOOP_ERROR", "error": str(e)})


threading.Thread(target=_symlim_loop, daemon=True, name="symbol-limit").start()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# ADDED 16 Nov 2026 - RISK CAP PER POSITION (Khagen's rule): every open intraday position that has a stop-loss is checked every poll. Risk = quantity x
# (entry - stop) x multiplier. If it is MORE than POSITION_RISK_MAX (Rs 2000), the bridge exits the EXTRA quantity at market and keeps only the quantity whose
# risk is within Rs 2000; the stop-loss order(s) are then resized to the kept quantity. A stop already moved to cost / profit has no risk, so nothing is cut.
# A position is only looked at after it has been open 90 seconds (so a fill still in progress is not cut). A 1-share test trade is never touched in practice.
# Defaults are built in; POSITION_RISK_CAP_ON=false in Render switches it off (optional).
POSITION_RISK_CAP_ON = os.environ.get("POSITION_RISK_CAP_ON", "true").strip().lower() != "false"
POSITION_RISK_MAX = float(os.environ.get("POSITION_RISK_MAX", "2000") or "2000")
POSITION_RISK_TOLERANCE = 1.02          # up to 2% over the limit is left alone (rounding of whole shares / lots)
_riskcap = {"day": "", "seen": {}, "tries": {}, "told": set()}


def _riskcap_plan(side, cost, qty, mult, lot, live, max_risk):
    """Pure arithmetic. live = list of live SL orders {qty, trigger}. Returns (risk_now, keep_qty or None) - keep_qty is None when nothing has to be cut."""
    cov, weighted = 0, 0.0
    for o in live:
        try:
            q = min(int(float(o.get("qty") or 0)), qty - cov)
            t = float(o.get("trigger"))
        except (TypeError, ValueError):
            continue
        if q <= 0:
            continue
        d = (cost - t) if side == "BUY" else (t - cost)
        weighted += max(d, 0.0) * q
        cov += q
    if cov <= 0 or weighted <= 0:
        return 0.0, None
    dist = weighted / cov                                  # average stop distance per unit
    risk = dist * qty * mult
    if risk <= max_risk * POSITION_RISK_TOLERANCE:
        return risk, None
    keep_units = int(max_risk / (dist * mult))
    keep = (keep_units // lot) * lot
    return risk, keep


def _riskcap_check_once():
    now = datetime.now(IST)
    mins = now.hour * 60 + now.minute
    today = now.strftime("%Y-%m-%d")
    if _riskcap["day"] != today:
        _riskcap.update(day=today, seen={}, tries={}, told=set())
    if mins < 9 * 60 + 30 or mins > 15 * 60 + 15:
        return
    resp = requests.get(f"{DHAN_BASE_URL}/positions", headers=dhan_headers(), timeout=15, proxies=DHAN_PROXIES)
    positions = safe_json(resp)
    if not resp.ok or not isinstance(positions, list):
        return
    open_keys = set()
    for p in positions:
        try:
            seg = p.get("exchangeSegment")
            net = _risk_net_qty(p)
            if seg not in ("NSE_EQ", "MCX_COMM") or net == 0 or str(p.get("productType", "")).upper() == "CNC":
                continue
            sid = str(p.get("securityId"))
            key = f"{seg}:{sid}"
            open_keys.add(key)
            first = _riskcap["seen"].setdefault(key, time.time())
            if time.time() - first < 90:
                continue
            tr = _riskcap["tries"].get(key) or {"n": 0, "t": 0.0}
            if tr["n"] >= 3 or time.time() - tr["t"] < 60:
                continue
            qty = abs(net)
            side = "BUY" if net > 0 else "SELL"
            cost = _risk_f(p.get("costPrice") or (p.get("buyAvg") if net > 0 else p.get("sellAvg")))
            if cost <= 0:
                continue
            mult = _risk_f(p.get("multiplier")) or 1.0
            symbol = get_symbol_for_security_id(sid, seg) or str(p.get("tradingSymbol") or sid)
            lot = 1
            if seg == "MCX_COMM":
                try:
                    lot = int(get_lot_size(symbol) or 1)
                except Exception:
                    lot = 1
            live = collect_live_sl_orders(sid, seg)
            if not live:                                          # no stop-loss (or the lookup failed): no risk can be worked out here
                continue
            risk, keep = _riskcap_plan(side, cost, qty, mult, lot, live, POSITION_RISK_MAX)
            if keep is None:
                continue
            if keep < lot:
                if key not in _riskcap["told"]:
                    _riskcap["told"].add(key)
                    notify("[Dhan] Risk above limit - " + symbol, f"{symbol}: risk is about Rs {risk:,.0f} (limit Rs {POSITION_RISK_MAX:,.0f}) but even the smallest quantity would be above the limit, so nothing was cut. Decide yourself.", priority="high")
                continue
            exit_qty = qty - keep
            if exit_qty <= 0:
                continue
            _riskcap["tries"][key] = {"n": tr["n"] + 1, "t": time.time()}
            close_side = "SELL" if side == "BUY" else "BUY"
            entry = last_entries.get(symbol) or {"legs": [], "security_id": sid, "segment": seg, "side": side}
            entry["addsl_hold_until"] = time.time() + 90         # the exit lowers the position: the "SL for added quantity" check must not misread it
            r = place_order(sid, close_side, exit_qty, "MARKET", exchange_segment=seg,
                            correlation_id=safe_correlation_id(symbol, f"RCAP-{int(time.time())}"), product_type=p.get("productType"))
            ok = isinstance(r, dict) and r.get("orderId") is not None
            log_event({"action": "RISK_CAP_EXIT" if ok else "RISK_CAP_EXIT_FAILED", "symbol": symbol, "side": side, "position": qty, "exit_qty": exit_qty, "keep": keep,
                       "risk_before": round(risk, 2), "limit": POSITION_RISK_MAX, "response": r})
            if not ok:
                notify("[Dhan] Risk cap exit FAILED - " + symbol, f"{symbol}: risk Rs {risk:,.0f} is above Rs {POSITION_RISK_MAX:,.0f} and the bridge tried to exit {exit_qty} of {qty}, but Dhan refused it ({r}). Cut it yourself.", priority="urgent")
                continue
            time.sleep(3)
            live2 = collect_live_sl_orders(sid, seg)
            if live2 is None:
                notify("[Dhan] Risk cap: check SL - " + symbol, f"{symbol}: exited {exit_qty} of {qty} but could not read the SL orders to resize them. The SL may still be for {qty} - it must be for {keep}. Fix it NOW.", priority="urgent")
                continue
            new_legs, problems = _resize_manual_sl(symbol, entry, sid, seg, close_side, keep, live2)
            if symbol in last_entries:
                last_entries[symbol]["legs"] = new_legs
                last_entries[symbol]["qty"] = keep
                last_entries[symbol]["addsl_hold_until"] = time.time() + 60
                save_state()
            if problems:
                notify("[Dhan] Risk cap: FIX THE SL - " + symbol, f"{symbol}: exited {exit_qty} of {qty} (risk was Rs {risk:,.0f}) but: " + "; ".join(problems) + f". The position is now {keep} - make sure an SL for {keep} exists NOW.", priority="urgent")
            else:
                notify("[Dhan] Risk cap - extra quantity exited - " + symbol,
                       f"{symbol}: risk was about Rs {risk:,.0f} (limit Rs {POSITION_RISK_MAX:,.0f}). Exited {exit_qty} at market, kept {keep}; the SL now covers {keep} and is tracked. A Super Order's target leg, if any, was replaced by a plain SL.", priority="high")
        except Exception as e:
            log_event({"action": "RISK_CAP_ERROR", "error": str(e)})
    for k in list(_riskcap["seen"].keys()):
        if k not in open_keys:
            _riskcap["seen"].pop(k, None)
            _riskcap["tries"].pop(k, None)


def _riskcap_loop():
    while True:
        time.sleep(20)
        if not POSITION_RISK_CAP_ON:
            continue
        try:
            _riskcap_check_once()
        except Exception as e:
            log_event({"action": "RISK_CAP_LOOP_ERROR", "error": str(e)})


threading.Thread(target=_riskcap_loop, daemon=True, name="risk-cap").start()


try:
    import swing_tracker
    swing_tracker.install(app, DHAN_BASE_URL, dhan_headers, DHAN_PROXIES, notify, log_event, get_ltp_batch, WEBHOOK_SECRET, _swing_sell)
except Exception as _swing_e:
    log.warning(f"swing tracker not started: {_swing_e}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
