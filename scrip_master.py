"""
STANDALONE COPY (2026-10-04) - this is a separate, independent copy of
dhan-bridge/scrip_master.py, living in its own repo (the ALL IN ONE PRO
scanner) specifically so this project shares NO code or imports with the
live dhan-bridge order-placement service. Editing this file has zero effect
on dhan-bridge, and vice versa - they just happen to start from the same
logic. Only the NSE-equity lookup functions are actually used here (MCX/
index lookups are along for the ride, unused, kept only because splitting
them out risked breaking something subtle for no real benefit).

Downloads Dhan's instrument master CSV once, caches it, and looks up the
numeric Security ID Dhan's order API needs for a given trading symbol (e.g.
"DIXON", "BAJAJHLDNG" for NSE equity, or "CRUDEOIL", "GOLD" for MCX
commodities - the same text TradingView's syminfo.ticker gives, minus any
expiry suffix).

Builds TWO lookups:
  - NSE cash equity (SEM_EXM_EXCH_ID == NSE, SEM_SERIES == EQ)
  - MCX commodity futures (SEM_EXM_EXCH_ID == MCX, SEM_INSTRUMENT_NAME in
    the futures-contract instrument names)

get_security_id_and_segment() checks NSE first, then MCX, and returns
which Dhan exchangeSegment the symbol belongs to so the caller can route
the order (and pick the right cutoff time) correctly.
"""

import os
import io
import time
import logging
import threading

import requests
import pandas as pd

log = logging.getLogger("dhan-bridge.scrip_master")

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "scrip_master_cache.csv")
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # re-download once a day

# MCX commodity futures instrument names as used in Dhan's scrip master.
MCX_FUT_INSTRUMENT_NAMES = {"FUTCOM", "FUTCUR"}

# Explicit fixes for TradingView tickers whose stripped form shares no
# common substring with Dhan's own underlying name, so even the
# close-match fuzzy check in _log_close_matches() can't find it (confirmed
# 2026-09-25 via the "MCX symbol keys resolved" log: Dhan lists Natural Gas
# Mini as "NATURALGASM", but TradingView's "NATGASMINI1!" strips to
# "NATGASMINI" - two different abbreviations for the same contract). Every
# other MCX mini contract traded so far (GOLDM, SILVERM, CRUDEOILM) happens
# to share a prefix with TradingView's naming already, so this table is
# only for the genuine exceptions. Key: TradingView's stripped symbol.
# Value: Dhan's underlying key (as it appears in _mcx_lookup).
SYMBOL_ALIASES = {
    "NATGASMINI": "NATURALGASM",
}

# ADDED 2026-09-29 (real incident: every NATGASMINI order attempted tonight
# was rejected by Dhan's RMS with "quantity exceeds the maximum quantity
# that can be placed" - qty 833, 526, 526, 500, 500, all far too large to
# be real contract quantities. Root cause: whatever lot size
# get_lot_size() was reading for NATURALGASM from the scrip master CSV was
# wrong (not implausible enough to trip MCX_MIN_PLAUSIBLE_LOT_SIZE, just
# genuinely incorrect), so the bridge's risk-based sizing kept producing
# oversized quantities. The scrip master CSV is Dhan's own data and should
# be trusted by default, but for symbols where it's been PROVEN wrong by a
# real rejected order, a hand-confirmed override here is safer than
# continuing to trust a bad number. NATURALGASM (Natural Gas Mini) is
# confirmed 250 MMBtu/lot from MCX's own published contract specification,
# cross-checked against two independent broker sources (Fyers' and Sahi's
# contract-notice pages) - not just one source, in case either had a typo.
# get_lot_size() checks this table BEFORE the scrip master lookup for any
# symbol listed here. Only add a symbol here once its real lot size is
# independently confirmed - do not guess.
#
# ADDED 2026-09-30 (real incident: a GOLDM1! risk-based order was rejected
# by Dhan's RMS for insufficient funds - investigation showed get_lot_size()
# had returned lot_size=1 for GOLDM instead of its real 100 grams/lot. In
# risk-based sizing, lot_size is a DIVISOR (lots = RISK_RUPEES_MCX //
# (stopDistance * lot_size)), so a too-small lot_size doesn't just misprice
# the order like it would for a fixed-lot trade - it makes the formula think
# each lot risks far less than it really does, and it keeps handing out
# lots until a real-money position many times too large for the intended
# risk comes out the other end. This GOLDM1! order should have sized to
# under 1 lot (i.e. been skipped) for a Rs.2000 risk budget; the lot_size=1
# bug produced 5 lots (~Rs.74 lakh notional) instead. Confirms the scrip
# master's lot-size column is unreliable beyond just NATURALGASM, so the
# other MCX commodities Khagen flagged (crude, gold, silver) are added here
# too, using MCX's own published contract specifications, cross-checked
# against two independent broker sources each (Zerodha Varsity, Sahi, Angel
# One, Upstox, Poonawalla Fincorp - see chat history 2026-09-29 for exact
# citations) - BUT that first pass turned out to use the wrong kind of
# number for anything priced per-10g/per-kg rather than per raw physical
# unit. Checked directly against Dhan's own order ticket (the "Lot (Unit:
# X)" label on web.dhan.co, which is Dhan's own authoritative statement of
# how many raw API quantity units equal 1 lot for that specific contract -
# not the real-world grams/kg/barrels per lot, which is a DIFFERENT number
# whenever the exchange quotes that commodity in packaged units, e.g. gold
# priced per 10g even though its physical lot is 100g), the true values are:
#   GOLDM: Dhan unit = 10  (NOT 100 - the real 100g/lot exists, but Dhan's
#     own atomic quantity unit for gold-family contracts is "10 grams",
#     matching how gold is PRICED, not how it's weighed)
#   CRUDEOILM: Dhan unit = 10 (this one DOES match its real 10 barrels/lot -
#     crude is priced per barrel directly, no packaging factor)
#   SILVERMIC: Dhan unit = 1 (matches its real 1kg/lot - silver micro is
#     priced per kg directly)
#   NATURALGAS (regular, non-mini): Dhan unit = 1250, matching its real
#     1250 MMBtu/lot exactly - confirms gas contracts have no packaging
#     factor either.
# Bottom line: only enter a number here after reading it directly off
# Dhan's own "Lot (Unit: X)" order-ticket label for that exact symbol -
# never back-calculate it from a generic "grams/kg/barrels per lot" fact,
# since that number is only sometimes the same as Dhan's own atomic unit.
# GOLD, CRUDEOIL, SILVER, SILVERM, SILVER100, GOLDGUINEA, GOLDPETAL, and
# GOLDTEN have NOT been checked against Dhan's own ticket yet and are
# deliberately left OUT of this table for now - trading them live before
# checking risks the same oversized-order failure GOLDM just had. NATURALGASM
# (the mini, as opposed to the regular NATURALGAS confirmed above) also still
# needs its own direct "Lot (Unit: X)" check - the 250 below is only the
# real-world MMBtu figure, unverified against Dhan's own atomic unit for the
# MINI contract specifically.
VERIFIED_LOT_SIZE_OVERRIDES = {
    "NATURALGASM": 250,   # STILL UNVERIFIED against Dhan's own ticket - check before trusting
    "CRUDEOILM": 10,      # confirmed via Dhan ticket 2026-09-30
    "GOLDM": 10,          # confirmed via Dhan ticket 2026-09-30 (corrects earlier wrong guess of 100)
    "SILVERMIC": 1,       # confirmed via Dhan ticket 2026-09-30
}

_eq_lookup = None   # {SYMBOL: security_id}  - NSE_EQ
_mcx_lookup = None  # {SYMBOL: security_id}  - MCX_COMM (nearest-expiry contract per symbol)
_mcx_lot_lookup = None  # {SYMBOL: lot_size int}  - MCX_COMM, same symbols as _mcx_lookup
_index_lookup = None  # {NAME: security_id}  - NSE indices (e.g. "NIFTY", "BANKNIFTY"), see get_index_security_id()

# ADDED 2026-09-27 for the NSE options strategy build: candidate names an
# index row's SEM_TRADING_SYMBOL/SEM_CUSTOM_SYMBOL might use in Dhan's own
# scrip master, keyed by the short name this bridge will refer to the index
# by. Dhan's exact spelling for index rows isn't confirmed from documentation
# alone (their docs don't spell it out) - _build_lookup() below tries every
# candidate and logs exactly what it matched (or didn't), so a naming miss
# is immediately visible in the logs rather than silently resolving nothing.
INDEX_NAME_CANDIDATES = {
    "NIFTY": ["NIFTY 50", "NIFTY50", "NIFTY"],
    "BANKNIFTY": ["NIFTY BANK", "BANKNIFTY", "BANK NIFTY"],
}

# Dhan's scrip master has used slightly different names for this column over
# time - try each until one is found in the actual CSV.
LOT_SIZE_COLUMN_CANDIDATES = ["SEM_LOT_UNITS", "SEM_LOT_SIZE", "LOT_SIZE"]


def _download_and_cache():
    log.info("Downloading Dhan scrip master...")
    resp = requests.get(SCRIP_MASTER_URL, timeout=60)
    resp.raise_for_status()
    with open(CACHE_FILE, "wb") as f:
        f.write(resp.content)
    log.info("Scrip master downloaded and cached.")


def _ensure_fresh_cache():
    if not os.path.exists(CACHE_FILE):
        _download_and_cache()
        return
    age = time.time() - os.path.getmtime(CACHE_FILE)
    if age > CACHE_MAX_AGE_SECONDS:
        try:
            _download_and_cache()
        except Exception as e:
            # stale cache is still better than no cache if Dhan's endpoint hiccups
            log.warning(f"Could not refresh scrip master, using existing cache: {e}")


def _find_lot_size_column(df):
    for col in LOT_SIZE_COLUMN_CANDIDATES:
        if col in df.columns:
            return col
    return None


def _build_lookup():
    global _eq_lookup, _mcx_lookup, _mcx_lot_lookup, _index_lookup
    _ensure_fresh_cache()
    df = pd.read_csv(CACHE_FILE, dtype=str)

    # --- NSE cash-equity rows: exchange NSE, series EQ.
    eq_mask = (df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & (df["SEM_SERIES"].str.upper() == "EQ")
    eq = df[eq_mask]

    eq_lu = {}
    for _, row in eq.iterrows():
        symbol = str(row["SEM_TRADING_SYMBOL"]).strip().upper()
        sec_id = str(row["SEM_SMST_SECURITY_ID"]).strip()
        if symbol and sec_id:
            eq_lu[symbol] = sec_id
    _eq_lookup = eq_lu
    log.info(f"Loaded {len(_eq_lookup)} NSE equity symbols from scrip master.")

    # --- MCX commodity futures rows: exchange MCX, futures instrument types.
    mcx_lu = {}
    if "SEM_EXM_EXCH_ID" in df.columns and "SEM_INSTRUMENT_NAME" in df.columns:
        mcx_mask = (df["SEM_EXM_EXCH_ID"].str.upper() == "MCX") & (
            df["SEM_INSTRUMENT_NAME"].str.upper().isin(MCX_FUT_INSTRUMENT_NAMES)
        )
        mcx = df[mcx_mask].copy()

        if not mcx.empty:
            # Underlying commodity name: prefer SEM_CUSTOM_SYMBOL's first word
            # (e.g. "CRUDEOIL 19NOV2026" -> "CRUDEOIL"); fall back to
            # SEM_TRADING_SYMBOL's first word if that column isn't present.
            name_col = "SEM_CUSTOM_SYMBOL" if "SEM_CUSTOM_SYMBOL" in mcx.columns else "SEM_TRADING_SYMBOL"
            mcx["_underlying"] = mcx[name_col].astype(str).str.strip().str.upper().str.split().str[0]

            if "SEM_EXPIRY_DATE" in mcx.columns:
                mcx["_expiry"] = pd.to_datetime(mcx["SEM_EXPIRY_DATE"], errors="coerce")
                mcx = mcx.sort_values("_expiry")

            lot_col = _find_lot_size_column(mcx)
            mcx_lot_lu = {}

            # Nearest-expiry contract per underlying symbol (first row after sort).
            for underlying, group in mcx.groupby("_underlying"):
                symbol = str(underlying).strip().upper()
                first_row = group.iloc[0]
                sec_id = str(first_row["SEM_SMST_SECURITY_ID"]).strip()
                if symbol and sec_id and sec_id != "NAN":
                    mcx_lu[symbol] = sec_id
                    if lot_col:
                        try:
                            lot_val = int(float(first_row[lot_col]))
                            if lot_val > 0:
                                mcx_lot_lu[symbol] = lot_val
                        except (TypeError, ValueError):
                            pass
            _mcx_lot_lookup = mcx_lot_lu
            if lot_col:
                log.info(f"Loaded lot sizes for {len(mcx_lot_lu)} MCX symbols (column: {lot_col}).")
            else:
                log.warning("Could not find a lot-size column in Dhan's scrip master "
                             f"(tried {LOT_SIZE_COLUMN_CANDIDATES}) - MCX quantity override will not work "
                             "until this is fixed.")
    _mcx_lookup = mcx_lu
    if _mcx_lot_lookup is None:
        _mcx_lot_lookup = {}
    log.info(f"Loaded {len(_mcx_lookup)} MCX commodity symbols from scrip master.")
    # One-time visibility into Dhan's exact underlying names - some
    # TradingView tickers (e.g. "NATGASMINI1!") don't share a common
    # substring with Dhan's naming (e.g. if Dhan spells it "NATURAL GAS
    # MINI"), so the close-match fuzzy check in get_security_id_and_segment
    # can miss silently. Printing the full key list makes any such mismatch
    # immediately visible in the logs instead of guessing.
    log.info(f"MCX symbol keys resolved: {sorted(_mcx_lookup.keys())}")

    # --- NSE index rows (NIFTY 50 / NIFTY BANK) - ADDED 2026-09-27 for the
    # options strategy. Dhan's docs don't spell out the exact SEM_SEGMENT/
    # SEM_INSTRUMENT_NAME values used for index rows, so this searches
    # broadly (any row whose trading/custom symbol matches a candidate name
    # from INDEX_NAME_CANDIDATES, on any exchange segment) rather than
    # assuming one specific column value - safer to over-search and log what
    # was found than to under-search and silently find nothing.
    idx_lu = {}
    symbol_col_candidates = [c for c in ("SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL") if c in df.columns]
    for our_name, candidates in INDEX_NAME_CANDIDATES.items():
        found_id = None
        found_as = None
        for col in symbol_col_candidates:
            col_upper = df[col].astype(str).str.strip().str.upper()
            for cand in candidates:
                match = df[col_upper == cand]
                if not match.empty:
                    found_id = str(match.iloc[0]["SEM_SMST_SECURITY_ID"]).strip()
                    found_as = f"{col}='{cand}'"
                    break
            if found_id:
                break
        if found_id:
            idx_lu[our_name] = found_id
            log.info(f"Resolved index '{our_name}' -> security_id {found_id} (matched {found_as}).")
        else:
            log.warning(f"Could not resolve index '{our_name}' in scrip master - tried names {candidates} "
                        f"across columns {symbol_col_candidates}. The NSE options strategy cannot run for "
                        "this index until this is fixed (check the scrip master's actual column/values for "
                        "index rows and update INDEX_NAME_CANDIDATES if Dhan's naming differs).")
    _index_lookup = idx_lu


def warm_cache_async():
    """Builds the NSE/MCX lookup tables in a background thread, called once
    at app startup (see app.py). Render's disk is wiped on every redeploy,
    so without this, the very first real CONFIRMED trade signal after any
    deploy would otherwise be the one that pays for a slow, synchronous
    download+parse of Dhan's full scrip master - risking a webhook timeout
    on a real trade (this is exactly what happened once in production: a
    CONFIRMED_BUY alert hit a cold cache, the download+parse ran past
    Gunicorn's worker timeout, and the order was never placed). Warming it
    here instead means that cost is almost always paid at boot, not on a
    live signal - get_security_id_and_segment()/get_lot_size() below still
    build it synchronously as a fallback if a request somehow arrives before
    this finishes."""
    def _run():
        try:
            _build_lookup()
        except Exception as e:
            log.error(f"Background scrip master warm-up failed (will retry lazily on next real signal): {e}")
    threading.Thread(target=_run, daemon=True).start()


def get_security_id_and_segment(trading_symbol: str):
    """Looks up a trading symbol as NSE equity first, then MCX commodity.
    Returns (security_id, segment) where segment is "NSE_EQ" or "MCX_COMM",
    or (None, None) if it can't be found in either (caller should NOT place
    an order in that case - better to skip than to guess)."""
    global _eq_lookup, _mcx_lookup
    if _eq_lookup is None or _mcx_lookup is None:
        _build_lookup()

    symbol = trading_symbol.strip().upper()
    variants = [symbol, symbol.replace("-EQ", ""), symbol + "-EQ", symbol.replace(".NS", "")]
    # Also strip a trailing "1!"/"2!" continuous-contract suffix some feeds
    # (like TradingView's MCX continuous symbols) use, e.g. "CRUDEOIL1!".
    stripped = symbol.rstrip("!0123456789")
    if stripped and stripped not in variants:
        variants.append(stripped)
    # Explicit alias for known TradingView<->Dhan naming mismatches (see
    # SYMBOL_ALIASES above) - checked in addition to, not instead of, the
    # variants above, so this never overrides a direct match.
    if stripped in SYMBOL_ALIASES:
        variants.append(SYMBOL_ALIASES[stripped])

    for v in variants:
        if v in _eq_lookup:
            return _eq_lookup[v], "NSE_EQ"

    for v in variants:
        if v in _mcx_lookup:
            return _mcx_lookup[v], "MCX_COMM"

    _log_close_matches(trading_symbol, stripped)
    log.warning(f"No Dhan security id found for symbol '{trading_symbol}' in NSE or MCX lists.")
    return None, None


def _log_close_matches(trading_symbol: str, stripped: str):
    """On a lookup miss, log the nearest-looking symbol names Dhan DOES have
    (checked against both NSE and MCX lists), so the real naming mismatch is
    visible in the logs instead of just 'not found'."""
    global _eq_lookup, _mcx_lookup
    needle = stripped[:4]  # first few letters, e.g. "NATG" from "NATGASMINI"
    if not needle:
        return
    candidates = []
    for label, lookup in (("MCX", _mcx_lookup), ("NSE", _eq_lookup)):
        for sym in lookup:
            if needle in sym or sym in stripped or stripped in sym:
                candidates.append(f"{sym} ({label})")
    if candidates:
        log.warning(
            f"Closest Dhan symbol names to '{trading_symbol}' (stripped: '{stripped}'): "
            f"{', '.join(sorted(set(candidates))[:15])}"
        )
    else:
        log.warning(f"No close matches found for '{trading_symbol}' (stripped: '{stripped}') either.")


def get_lot_size(trading_symbol: str):
    """Returns the MCX exchange lot size (in Dhan's order-quantity units,
    e.g. 10 for Gold Mini's 10 grams/lot) for a commodity symbol, or None if
    it's not a known MCX symbol or the lot size column wasn't found in the
    scrip master. Not meaningful for NSE equity (no lot-size restriction
    there - any share count is valid)."""
    global _eq_lookup, _mcx_lookup, _mcx_lot_lookup
    if _eq_lookup is None or _mcx_lookup is None:
        _build_lookup()

    symbol = trading_symbol.strip().upper()
    variants = [symbol, symbol.replace("-EQ", ""), symbol + "-EQ", symbol.replace(".NS", "")]
    stripped = symbol.rstrip("!0123456789")
    if stripped and stripped not in variants:
        variants.append(stripped)
    if stripped in SYMBOL_ALIASES:
        variants.append(SYMBOL_ALIASES[stripped])

    # ADDED 2026-09-29: check the hand-confirmed override table FIRST, for
    # any symbol (or its aliased underlying key) proven wrong by a real
    # rejected order - see VERIFIED_LOT_SIZE_OVERRIDES's comment above for
    # the NATURALGASM incident this was built for.
    for v in variants:
        if v in VERIFIED_LOT_SIZE_OVERRIDES:
            return VERIFIED_LOT_SIZE_OVERRIDES[v]

    for v in variants:
        if v in _mcx_lot_lookup:
            return _mcx_lot_lookup[v]
    return None


def get_security_id(trading_symbol: str):
    """Back-compat helper: NSE-equity-only lookup, returns just the id."""
    security_id, segment = get_security_id_and_segment(trading_symbol)
    if segment == "NSE_EQ":
        return security_id
    return None


def get_index_security_id(our_name: str):
    """ADDED 2026-09-27 for the NSE options strategy. Returns the numeric
    UnderlyingScrip security id Dhan's option-chain API (POST /optionchain)
    needs for this index, or None if it couldn't be resolved from the scrip
    master (see _build_lookup()'s index-resolution block above - check the
    logs for exactly what was tried). our_name is one of the keys in
    INDEX_NAME_CANDIDATES, e.g. "NIFTY" or "BANKNIFTY" - not Dhan's own
    display name."""
    global _index_lookup
    if _index_lookup is None:
        _build_lookup()
    return _index_lookup.get(our_name.strip().upper())


# ADDED 2026-10-01 for the manual-trade detector (Khagen's request: let the
# bridge find a position he opened and protected entirely by hand on Dhan's
# own app, with no alert ever sent). The forward lookups above (symbol ->
# security_id) are no help there - the poller only has the security_id from
# Dhan's own /positions response and needs to go the other way. Built lazily
# from the same _eq_lookup/_mcx_lookup tables _build_lookup() already
# maintains, so it shares one cache and one daily refresh with every other
# lookup in this file - nothing about the existing forward lookups changes.
_reverse_lookup = None  # dict[(segment, security_id)] -> trading_symbol


def get_symbol_for_security_id(security_id: str, segment: str):
    """Reverse of get_security_id_and_segment(): given a Dhan security_id and
    exchangeSegment ("NSE_EQ" or "MCX_COMM"), returns the trading symbol
    (e.g. "RELIANCE", "CRUDEOILM") the bridge's own alert "symbol" field
    would have used, or None if this security_id isn't in either table
    (e.g. an options/futures contract the bridge doesn't otherwise trade -
    the manual-trade poller skips those rather than guessing a name)."""
    global _reverse_lookup, _eq_lookup, _mcx_lookup
    if _eq_lookup is None or _mcx_lookup is None:
        _build_lookup()
    if _reverse_lookup is None:
        rev = {}
        for sym, sid in (_eq_lookup or {}).items():
            rev[("NSE_EQ", str(sid))] = sym
        for sym, sid in (_mcx_lookup or {}).items():
            rev[("MCX_COMM", str(sid))] = sym
        _reverse_lookup = rev
    return _reverse_lookup.get((segment, str(security_id)))


# ADDED 2026-10-03 for the end-of-day scanner (eod_scanner.py). Purely
# additive read-only accessor - does not change _build_lookup() or any
# existing lookup function above, so every order-placing code path in
# app.py is untouched. Returns the SAME NSE-equity symbol list the bridge
# already trusts for order placement (the eq_mask == NSE + series == EQ
# rows built by _build_lookup()), so the scanner's universe can never
# drift from what this bridge can actually trade.
def get_all_nse_equity_symbols():
    """Returns a sorted list of every NSE cash-equity trading symbol Dhan's
    scrip master currently lists (the same set get_security_id_and_segment()
    resolves against), e.g. ["20MICRONS", "3MINDIA", ..., "ZYDUSLIFE"]."""
    global _eq_lookup
    if _eq_lookup is None:
        _build_lookup()
    return sorted(_eq_lookup.keys())
