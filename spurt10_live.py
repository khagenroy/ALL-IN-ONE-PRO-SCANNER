"""
VOLUME SPURT 10-MIN SCANNER  (page: /spurt10   csv: /spurt10.csv)

Every 10 minutes (just after each 10-minute candle closes) it ranks the whole NSE cash market by

        volume of the LATEST CLOSED 10-min candle  /  average volume of the PREVIOUS 50 ten-minute candles
        (= the "Volume Multiple vs SMA" idea, with a 50-candle volume moving average)

and shows the TOP 30. Stocks priced below MIN_PRICE (50) are left out.

HOW IT COVERS ALL ~2,700 NSE STOCKS WITHOUT HAMMERING DHAN
  Step 1  3 batched quote calls (marketfeed/quote, up to 1000 stocks each) give every stock's last price and
          today's total volume. Price < 50 is dropped. Comparing today's volume with the previous cycle's
          snapshot shows how much traded in the last 10 minutes for EVERY stock.
  Step 2  A shortlist of the SHORTLIST (default 150) busiest stocks is picked from that.
  Step 3  Only the shortlist has its 5-min candles downloaded (merged to 10-min, closed candles only) so the
          exact ratio vs the 50-candle volume average can be computed and the top 30 ranked.

It is read-only: places no orders, is not connected to Section A / Section B / the bot / RSI Flush, and any
error here is caught and logged without touching the other scans. It uses the same rate limiter as the main scan.

ENV (all optional)
  SPURT10_AUTORUN=true      SPURT10_TOP_N=30        SPURT10_MIN_PRICE=50      SPURT10_SMA_LEN=50
  SPURT10_SHORTLIST=150     SPURT10_MIN_SMA_VOL=1000  (ignore stocks whose 50-candle average is below this many shares)
  SPURT10_WORKERS=4
  SPURT10_ALERT_MULT=3.0    phone alert (ntfy) when a stock's latest 10-min candle volume is >= this many times the 50-candle average
  SPURT10_NTFY=true         switch the phone alert on/off (uses the same NTFY_TOPIC as the other scanner alerts)
  SPURT10_NTFY_MAX_PER_DAY=40   safety cap on phone notifications per day
  SPURT10_ZONE_TF=15        time frame (minutes: 10, 15, 30...) of the support / resistance zones, the zone columns and the zone alert
  SPURT10_WATCH_MULT=20     a stock that reaches this spurt multiple in the top-30 is WATCHED for the rest of the day (until SPURT10_WATCH_UNTIL),
                            even after it leaves the top-30, and you are alerted when it reaches a support / resistance zone
  SPURT10_WATCH_UNTIL=15:00 stop zone alerts after this IST time        SPURT10_WATCH_MAX=100  most stocks watched at once
  SPURT10_ZONE_ALERT=true   phone alert when a stock in the top-30 spurt table reaches a support / resistance zone (same NTFY_TOPIC)
  SPURT10_ZONE_ALERT_MAX=40 safety cap on zone notifications per day
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import live_scanner as ls
from scrip_master import get_all_nse_equity_symbols, get_security_id_and_segment

log = logging.getLogger("spurt10")
IST = ZoneInfo("Asia/Kolkata")

AUTORUN = os.environ.get("SPURT10_AUTORUN", "true").strip().lower() == "true"
TOP_N = int(os.environ.get("SPURT10_TOP_N", "30") or "30")
MIN_PRICE = float(os.environ.get("SPURT10_MIN_PRICE", "50") or "50")
SMA_LEN = int(os.environ.get("SPURT10_SMA_LEN", "50") or "50")
SHORTLIST = int(os.environ.get("SPURT10_SHORTLIST", "150") or "150")
MIN_SMA_VOL = float(os.environ.get("SPURT10_MIN_SMA_VOL", "1000") or "1000")
WORKERS = int(os.environ.get("SPURT10_WORKERS", "4") or "4")
ALERT_MULT = float(os.environ.get("SPURT10_ALERT_MULT", "3.0") or "3.0")
NTFY_ON = os.environ.get("SPURT10_NTFY", "true").strip().lower() == "true"
NTFY_MAX_PER_DAY = int(os.environ.get("SPURT10_NTFY_MAX_PER_DAY", "40") or "40")
SPURT_TF = 5                 # CHANGED 7 Nov: candle size (minutes) of the main volume spurt - 5 (was 10). The cycle runs every SPURT_TF minutes and the volume average is of the last 50 candles of this size. Set 10 to go back.
ZONE_TF = max(5, int(os.environ.get("SPURT10_ZONE_TF", "15") or "15") // 5 * 5)     # zone candles, minutes (multiple of 5)
WATCH_MULT = float(os.environ.get("SPURT10_WATCH_MULT", "20") or "20")
WATCH_MAX = int(os.environ.get("SPURT10_WATCH_MAX", "100") or "100")


def _hhmm(txt, default):
    try:
        h, m = str(txt).strip().split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return default


WATCH_UNTIL_MIN = _hhmm(os.environ.get("SPURT10_WATCH_UNTIL", "15:00"), 15 * 60)
ZONE_ALERT_ON = os.environ.get("SPURT10_ZONE_ALERT", "true").strip().lower() == "true"
ZONE_ALERT_MAX = int(os.environ.get("SPURT10_ZONE_ALERT_MAX", "40") or "40")
OFFERS_ALERT_ON = True      # ADDED: phone alert when a spurt stock has selling pressure above OFFERS_ALERT_PCT (Dhan depth: total sell qty / (buy + sell qty), the "Offers %" of the depth window)
OFFERS_ALERT_PCT = 55.0     # Offers % above this = selling pressure alert (raise it, e.g. 60, to get fewer alerts)
OFFERS_ONLY_AT_ZONE = True  # ADDED: the selling-pressure alert is sent only while the stock is AT a support / resistance zone (between zones the depth means little); False = old behaviour
OFFERS_ALERT_MAX = 30       # safety cap on these notifications per day
HISTORY_DAYS = 8            # calendar days of 5-min candles: 50 ten-minute candles need ~3 sessions

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
JSON_PATH = os.path.join(RESULTS_DIR, "spurt10.json")
CSV_PATH = os.path.join(RESULTS_DIR, "spurt10.csv")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt10_history.csv")
HISTORY_ON = os.environ.get("SPURT10_HISTORY", "true").strip().lower() == "true"       # ADDED: full-day log of every cycle
HISTORY_MIN_MULT = float(os.environ.get("SPURT10_HISTORY_MIN_MULT", "0") or "0")      # 0 = keep every row passed in (the top 30)
HTML_PATH = os.path.join(RESULTS_DIR, "spurt10.html")

_universe = None            # list of (symbol, security_id)
_snap = {"date": None, "ts": None, "vol": {}}      # previous cycle's day-volume snapshot


# ---------------------------------------------------------------- universe + quotes
import re
_ETF_SYMBOL = re.compile(r"(BEES|ETF|NIFTY|SENSEX|LIQUID|NASDAQ|MON100|MAFANG|MOM100)")
_ETF_NAME = re.compile(r"(ETF|EXCHANGE TRADED|INDEX FUND|LIQUID|BEES|\bFOF\b|MUTUAL FUND)")
_excluded = []


def _etf_symbols():
    """Symbols to leave out so only real company stocks remain. Uses Dhan's own instrument-type column when it
    has one (ES = equity share) plus a name check for ETFs / index / liquid funds."""
    import scrip_master as sm
    sm._ensure_fresh_cache()
    df = pd.read_csv(sm.CACHE_FILE, dtype=str)
    df = df[(df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & (df["SEM_SERIES"].str.upper() == "EQ")].copy()
    sym = df["SEM_TRADING_SYMBOL"].astype(str).str.strip().str.upper()
    bad = sym.str.contains(_ETF_SYMBOL)
    for col in ("SEM_CUSTOM_SYMBOL", "SEM_SYMBOL_NAME"):
        if col in df.columns:
            bad |= df[col].fillna("").astype(str).str.upper().str.contains(_ETF_NAME)
    if "SEM_EXCH_INSTRUMENT_TYPE" in df.columns:
        t = df["SEM_EXCH_INSTRUMENT_TYPE"].fillna("").astype(str).str.upper().str.strip()
        if (t == "ES").sum() > 500:                       # column is populated: anything not an equity share goes
            bad |= (t != "ES")
    if "SEM_INSTRUMENT_NAME" in df.columns:
        bad |= df["SEM_INSTRUMENT_NAME"].fillna("").astype(str).str.upper().str.contains("ETF|MUTUAL")
    return set(sym[bad])


def _load_universe():
    global _universe, _excluded
    if _universe is None:
        try:
            skip = _etf_symbols()
        except Exception as e:
            log.warning(f"Spurt10: ETF filter could not read the instrument list ({e}) - using the symbol-name check only")
            skip = set()
        out = []
        for sym in get_all_nse_equity_symbols():
            if sym in skip or _ETF_SYMBOL.search(sym):
                _excluded.append(sym)
                continue
            try:
                sid, seg = get_security_id_and_segment(sym)
                if sid and seg == "NSE_EQ":
                    out.append((sym, str(sid)))
            except Exception:
                continue
        _universe = out
        log.info(f"Spurt10 universe: {len(out)} NSE stocks; {len(_excluded)} ETFs/funds left out, e.g. {_excluded[:15]}")
    return _universe


# ---------------------------------------------------------------- bid / ask depth -> liquidity (read from the same quote call, no extra request)
LIQ_SPREAD_OK = 0.10       # best bid-ask spread (% of price) up to this = LIQUID
LIQ_REMOVE_THIN = True     # True = THIN stocks (wide bid-ask spread) are taken out of the spurt list, the tracker, the SELL signals and the alerts
THIN_LIST_MIN_MULT = 10.0  # the illiquid list at the bottom of /spurt10 shows thin stocks with a spurt of at least this multiple
THIN_LIST_MAX = 30
LIQ_MIN_DEPTH_QTY = 50000   # ADDED 4 Nov: a stock whose total waiting BID quantity or total waiting ASK quantity (shares) is below this counts as THIN too and is left out like a wide-spread stock (a stock with no depth data stays). 0 = off
CIRCUIT_NEAR_PCT = 2.0      # ADDED 4 Nov: a stock whose price is within this % of its UPPER or LOWER CIRCUIT price counts as THIN too (it can lock any moment, then you cannot get out) and is left out of every list and alert. 0 = off
LIQ_SPREAD_THIN = 0.25     # above this = THIN (wide spread: hard to get in / out at the shown price); in between = OK
_dsnap = {}                # {symbol: {px, chg, vol, spread, offers}} - the latest cycle, used by depth_html() (/depth page)
_dsnap_meta = {"run": None, "spurt": {}, "zone": {}}
DEPTH_MIN_TURNOVER = 1e7   # /depth page: leave out stocks that traded less than Rs 1 crore so far today (dead stocks show random depth)
DEPTH_LIST_N = 50          # /depth page: stocks shown in each list
DEPTH_PCT = 55.0           # /depth page: Bids % / Offers % cells turn green / red above this
DEPTH_ONLY_AT_ZONE = True  # ADDED: /depth page lists only spurt stocks that are AT a support / resistance zone AND have Bids % or Offers % above DEPTH_PCT; False = every spurt stock
_circ = {}                 # ADDED 4 Nov: {symbol: "UPPER" / "LOWER"} for stocks within CIRCUIT_NEAR_PCT % of a circuit price - filled by _fetch_quotes every cycle
_depth = {}                # {symbol: (spread_pct, buy_qty / sell_qty, offers_pct)} filled by _fetch_quotes every cycle
DEPTH_PATH = os.path.join(os.path.dirname(__file__), "results", "depth_snapshot.json")   # ADDED: the /depth page data, saved to disk every cycle so it shows whichever worker / thread serves the page and survives a restart


def _save_depth_snapshot(run, spurt, zone=None):
    """ADDED: save the /depth page data (run time, spurt multiples, depth of those stocks) to disk. Never raises."""
    try:
        snap = {sym: _dsnap[sym] for sym in spurt if sym in _dsnap}
        os.makedirs(os.path.dirname(DEPTH_PATH), exist_ok=True)
        tmp = DEPTH_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"run": run, "spurt": spurt, "snap": snap, "zone": zone or {}}, f)
        os.replace(tmp, DEPTH_PATH)
    except Exception as e:
        log.warning(f"Spurt10: could not save the /depth snapshot: {e}")


def _load_depth_snapshot():
    """ADDED: (run, spurt, snap) from the saved file, or None when there is no usable file."""
    try:
        with open(DEPTH_PATH, "r") as f:
            d = json.load(f)
        if d.get("run") and d.get("spurt"):
            return d["run"], dict(d["spurt"]), dict(d.get("snap") or {}), dict(d.get("zone") or {})
    except Exception:
        pass
    return None


def _parse_depth(q):
    """(spread %, total buy qty / total sell qty) from one marketfeed/quote row, or None when the depth fields are not there."""
    try:
        d = q.get("depth") or {}
        buy, sell = d.get("buy") or [], d.get("sell") or []
        bid = float(buy[0].get("price")) if buy else 0.0
        ask = float(sell[0].get("price")) if sell else 0.0
        spread = None
        if bid > 0 and ask > 0 and ask >= bid:
            spread = round((ask - bid) / ((ask + bid) / 2.0) * 100.0, 3)
        bq = q.get("buy_quantity")
        sq = q.get("sell_quantity")
        if bq is None:
            bq = sum(float(x.get("quantity") or 0) for x in buy) or None
        if sq is None:
            sq = sum(float(x.get("quantity") or 0) for x in sell) or None
        ratio = round(float(bq) / float(sq), 2) if bq and sq and float(sq) > 0 else None
        offers = round(float(sq) / (float(bq) + float(sq)) * 100.0, 2) if bq and sq and (float(bq) + float(sq)) > 0 else None   # ADDED: Offers % of the depth window
        if spread is None and ratio is None:
            return None
        return spread, ratio, offers, (float(bq) if bq else 0.0), (float(sq) if sq else 0.0)      # ADDED: total bid qty and total ask qty (shares)
    except Exception:
        return None


def _circuit_side(q, px):
    """ADDED 4 Nov: "UPPER" / "LOWER" when the price is within CIRCUIT_NEAR_PCT % of the stock's upper / lower circuit price (from the same quote row), else "". Never raises."""
    try:
        if CIRCUIT_NEAR_PCT <= 0:
            return ""
        u = float(q.get("upper_circuit_limit") or 0)
        l = float(q.get("lower_circuit_limit") or 0)
        f = CIRCUIT_NEAR_PCT / 100.0
        if u > 0 and px >= u * (1 - f):
            return "UPPER"
        if l > 0 and px <= l * (1 + f):
            return "LOWER"
    except Exception:
        pass
    return ""


def _liq_fields(sym):
    """spread_pct, bs_ratio and the LIQUID / OK / THIN tag of a stock ('' when the quote had no depth)."""
    v = _depth.get(sym)
    circ = _circ.get(sym, "")
    low_qty = bool(circ) or bool(v and LIQ_MIN_DEPTH_QTY > 0 and len(v) > 4 and (v[3] < LIQ_MIN_DEPTH_QTY or v[4] < LIQ_MIN_DEPTH_QTY))      # ADDED 4 Nov: too few shares waiting on the bid or the ask side
    if not v or v[0] is None:
        return {"spread_pct": None, "bs_ratio": (v[1] if v else None), "offers_pct": (v[2] if v and len(v) > 2 else None), "liq": "THIN" if low_qty else ""}
    sp = v[0]
    return {"spread_pct": sp, "bs_ratio": v[1], "offers_pct": (v[2] if len(v) > 2 else None),
            "liq": "THIN" if low_qty else "LIQUID" if sp <= LIQ_SPREAD_OK else "THIN" if sp > LIQ_SPREAD_THIN else "OK"}


def _thin_now(sym):
    """True when the stock is THIN right now (bid / ask quantity below the minimum, near a circuit, or wide spread). Followed stocks that left the table are checked with this too."""
    try:
        return bool(LIQ_REMOVE_THIN and _depth and _liq_fields(sym).get("liq") == "THIN")
    except Exception:
        return False


def _fetch_quotes(uni):
    """{symbol: (last_price, day_volume)} via marketfeed/quote, 1000 ids per call, ~1 call/s."""
    by_id = {sid: sym for sym, sid in uni}
    ids = [int(s) for s in by_id]
    out, bad = {}, 0
    _depth.clear()
    _dsnap.clear()
    _circ.clear()
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": chunk}, f"quote batch {i // 1000 + 1}")
        data = (resp.json().get("data") or {}).get("NSE_EQ") or {}
        for sid, q in data.items():
            sym = by_id.get(str(sid))
            if not sym or not isinstance(q, dict):
                continue
            try:
                px = float(q.get("last_price"))
                vol = float(q.get("volume"))
            except (TypeError, ValueError):
                bad += 1
                continue
            out[sym] = (px, vol)
            _cs = _circuit_side(q, px)
            if _cs:
                _circ[sym] = _cs
            dp = _parse_depth(q)
            if dp:
                _depth[sym] = dp
                try:                                                # ADDED: snapshot for the /depth page (Bids % / Offers % of every liquid stock)
                    nc = q.get("net_change")
                    chg = round(float(nc) / (px - float(nc)) * 100.0, 2) if nc is not None and (px - float(nc)) > 0 else None
                    _dsnap[sym] = {"px": px, "chg": chg, "vol": vol, "spread": dp[0], "offers": dp[2]}
                except Exception:
                    pass
        time.sleep(1.1)
    if not out:
        raise RuntimeError("marketfeed/quote returned no usable rows (price/volume fields missing?)")
    if bad:
        log.warning(f"Spurt10: {bad} quote rows had no price/volume")
    log.info(f"Spurt10: bid/ask depth read for {len(_depth)} of {len(out)} stocks")
    return out


def _spurt_bars(df5, as_of):
    """Closed candles of the main volume spurt (SPURT_TF minutes) built from Dhan 5-minute candles."""
    if SPURT_TF <= 5:
        return ls.drop_forming_bars(df5, 5, as_of)
    return ls.drop_forming_bars(ls.build_merged_bars(df5, SPURT_TF // 5), SPURT_TF, as_of)


def _shortlist(quotes, now_ist):
    """Pick the busiest stocks of the last 10 min (or of the day, on the first cycle)."""
    today = now_ist.strftime("%Y-%m-%d")
    if _snap["date"] != today:
        _snap.update(date=today, ts=None, vol={})
    elapsed_bars = max(1.0, ((now_ist.hour * 60 + now_ist.minute) - (9 * 60 + 15)) / float(SPURT_TF))
    cand = []
    for sym, (px, vol) in quotes.items():
        if px < MIN_PRICE or vol <= 0:
            continue
        prev = _snap["vol"].get(sym)
        if prev is not None and vol >= prev:
            delta = vol - prev
            avg10 = max(prev / max(elapsed_bars - 1.0, 1.0), 1.0)
            cand.append((sym, delta / avg10, delta * px))          # spurt vs today's pace, rupee turnover of the window
        else:
            cand.append((sym, 0.0, vol * px / elapsed_bars))      # no snapshot yet: average turnover per 10 min
    have_snap = bool(_snap["vol"])
    pick = []
    if have_snap:
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[1])[:SHORTLIST]]
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[2])[:max(SHORTLIST // 3, 30)]]
    else:
        pick += [c[0] for c in sorted(cand, key=lambda c: -c[2])[:SHORTLIST]]
    _snap["vol"] = {s: v for s, (p, v) in quotes.items()}
    _snap["ts"] = now_ist.isoformat()
    seen, out = set(), []
    for s in pick:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out, len(cand)


# ---------------------------------------------------------------- candle maths
def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


def _candle_svg(o, h, l, c):
    """Small Japanese candle (body + wick) with its O/H/L/C written beside it."""
    try:
        o, h, l, c = float(o), float(h), float(l), float(c)
    except Exception:
        return "-"
    if not all(v == v for v in (o, h, l, c)):
        return "-"
    col = "#089981" if c > o else "#F23645" if c < o else "#888"
    rng = (h - l) or 1e-9
    y = lambda v: 2 + (h - v) / rng * 32
    top, bot = min(y(o), y(c)), max(y(o), y(c))
    fm = (lambda v: f"{v:.2f}") if h >= 1 else (lambda v: f"{v:.5f}")
    svg = (f'<svg width="22" height="40" viewBox="0 0 22 36" preserveAspectRatio="none" style="vertical-align:middle">'
           f'<line x1="11" y1="{y(h):.1f}" x2="11" y2="{y(l):.1f}" stroke="{col}" stroke-width="2"/>'
           f'<rect x="5" y="{top:.1f}" width="12" height="{max(bot - top, 2):.1f}" fill="{col}" stroke="{col}"/></svg>')
    return f'<span title="O {fm(o)}  H {fm(h)}  L {fm(l)}  C {fm(c)}" style="white-space:nowrap">{svg}</span>'


_inv_cache = {"mt": None, "d": {}}


def _inv_scores():
    """{symbol: investing score} from the once-a-day investing scan; re-read only when the file changes."""
    p = os.path.join(RESULTS_DIR, "invest_scores.json")
    try:
        mt = os.path.getmtime(p)
        if _inv_cache["mt"] != mt:
            with open(p) as f:
                _inv_cache["d"] = json.load(f)
            _inv_cache["mt"] = mt
    except Exception:
        pass
    return _inv_cache["d"]


def _tvl(sym):
    """Symbol as a link that opens its chart on TradingView (NSE)."""
    import urllib.parse as _up
    import html as _h
    s = str(sym)
    u = "https://www.tradingview.com/chart/b3scGnZI/?symbol=" + _up.quote("NSE:" + s.replace("-", "_"), safe=":_")
    _nb = ""
    try:                                                    # ADDED: NEWS tag beside every stock that has an important announcement (hover shows it)
        _nt = (_news.get("syms") or {}).get(s) if (NEWS_FILTER_ON and not NEWS_LEAVE_OUT) else None
        if _nt:
            _nb = ' <span title="{t}" style="background:#fff3cd;color:#8a5a00;border:1px solid #e0b000;border-radius:3px;padding:0 3px;font-size:10px;font-weight:700">NEWS</span>'.format(t=_h.escape(str(_nt), quote=True))
    except Exception:
        _nb = ""
    try:                                                    # ADDED: INV tag beside a stock that scored well in the investing scan (results/invest_scores.json, written once a day)
        _iv = _inv_scores().get(s)
        if _iv and _iv >= 3:
            _nb += ' <a href="/invest" target="_blank" rel="noopener" title="investing scan score {n}/5" style="background:#e3f2fd;color:#0d47a1;border:1px solid #1e88e5;border-radius:3px;padding:0 3px;font-size:10px;font-weight:700;text-decoration:none">INV {n}</a>'.format(n=int(_iv))
    except Exception:
        pass
    return '<a href="{u}" target="_blank" rel="noopener" title="open chart in TradingView" style="color:inherit;text-decoration:none;border-bottom:1px dotted #888"><b>{t}</b></a>'.format(u=u, t=_h.escape(s)) + _nb


def _score_symbol(sym, sid):
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
    df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, HISTORY_DAYS)
    df = _spurt_bars(df5, as_of)
    if df is None or len(df) < SMA_LEN + 1:
        return None
    vol = df["volume"].to_numpy(dtype=float)
    base = vol[-(SMA_LEN + 1):-1].mean()                     # previous 50 candles (current one excluded)
    if base < MIN_SMA_VOL:
        return None
    last = df.iloc[-1]
    day = df.index.map(lambda t: _ist(t).date())
    today_mask = day == day[-1]
    prev_close = df["close"][~today_mask].iloc[-1] if (~today_mask).any() else np.nan
    close = float(last["close"])
    gap = {}
    try:                                                              # ADDED: gap of today's open against the previous day (gap up / gap down with spurt candle page)
        if (~today_mask).any():
            pdm = day == day[~today_mask][-1]
            op0 = float(df["open"][today_mask].iloc[0])
            pcl = float(df["close"][pdm].iloc[-1])
            gap = {"gap_open": op0, "gap_pc": pcl, "gap_ph": float(df["high"][pdm].max()), "gap_pl": float(df["low"][pdm].min()),
                   "gap_pct": round((op0 / pcl - 1) * 100, 2) if pcl else None}
    except Exception:
        gap = {}
    zdf = df                                                          # candles the support / resistance zones are built on
    if ZONE_TF != SPURT_TF:
        try:
            zdf15 = ls.drop_forming_bars(ls.build_merged_bars(df5, ZONE_TF // 5), ZONE_TF, as_of)
            if zdf15 is not None and len(zdf15) >= 30:
                zdf = zdf15
        except Exception as e:
            log.warning(f"Spurt10 {ZONE_TF}m zone candles failed for {sym}, using 10m: {e}")
    sdf = df                                                          # ADDED 5 Nov: candles the Spurt SELL rule / tracker run on (15-min, like the TradingView indicator; spurt_sell_live.BASE_TF)
    try:
        import spurt_sell_live as _ssl
        if _ssl.BASE_TF != SPURT_TF:
            _s15 = ls.drop_forming_bars(ls.build_merged_bars(df5, _ssl.BASE_TF // 5), _ssl.BASE_TF, as_of)
            if _s15 is not None and len(_s15) >= _ssl.AVG_LEN + 3:
                sdf = _s15
            else:
                sdf = None                                            # not enough 15-min candles yet: no SELL check for this stock this cycle
    except Exception as e:
        log.warning(f"Spurt10 SELL candles failed for {sym}: {e}")
    return {
        **gap,
        "_zones": _secA_zones(zdf),
        "_df5": df5, "_as_of": as_of,
        "_zone_tf": (ZONE_TF if zdf is not df else SPURT_TF),
        "_trk": _trk_check(sdf, sym, round((close / prev_close - 1) * 100, 2) if prev_close == prev_close else None),   # ADDED: spurt tracker (spurt_sell_live.py)
        "_sell": _sell_check(sdf, sym, round((close / prev_close - 1) * 100, 2) if prev_close == prev_close else None),   # ADDED: spurt -> red candle SELL rule (spurt_sell_live.py)
        "symbol": sym,
        "price": round(close, 2),
        "day_chg_pct": round((close / prev_close - 1) * 100, 2) if prev_close == prev_close else None,
        "candle": _ist(df.index[-1]).strftime("%H:%M"),
        "candle_chg_pct": round((close / float(last["open"]) - 1) * 100, 2) if last["open"] else None,
        "ohlc": (float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"])),
        "prev": ((float(df.iloc[-2]["open"]), float(df.iloc[-2]["high"]), float(df.iloc[-2]["low"]), float(df.iloc[-2]["close"]), float(vol[-2])) if len(df) >= 2 else None),   # ADDED: previous closed 10-min candle (engulfing sections)
        "candle_vol": int(vol[-1]),
        "sma_vol": int(round(base)),
        "multiple": round(float(vol[-1] / base), 2),
        "day_vol": int(vol[today_mask].sum()),
    }


def _paper_fetch5(sym):
    """ADDED: closed 5-minute candles of one stock for the paper-trading follow-up (2 days is plenty). Returns None on any problem."""
    try:
        sid = dict(_load_universe()).get(sym)
        if not sid:
            return None
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, 2)
        return ls.drop_forming_bars(df5, 5, as_of)
    except Exception:
        return None


def _trk_check(df, sym, day_chg):
    """ADDED: summary of today's volume spurt (>= 10x) of this stock for the /spurt_sell tracker table. Never raises."""
    if df is None:
        return None
    try:
        import spurt_sell_live
        return spurt_sell_live.track(df, sym, day_chg)
    except Exception:
        return None


def _sell_check(df, sym, day_chg):
    """ADDED: check the 'volume spurt then solid red candle = SELL' rule on candles already downloaded. Never raises."""
    try:
        import spurt_sell_live
        return spurt_sell_live.check(df, sym, day_chg)
    except Exception:
        return None


# ---------------------------------------------------------------- phone alert
_alerted = {"date": None, "keys": set(), "count": 0}


def _send_alerts(rows, now_ist):
    """ADDED 2026-10-09: one phone notification per cycle listing every stock whose latest closed 10-min candle volume is
    >= ALERT_MULT x its 50-candle average. A stock is announced once per candle (never twice for the same candle), and at most
    NTFY_MAX_PER_DAY notifications a day. Read-only - it only sends a notification."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ON and topic):
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _alerted["date"] != today:
        _alerted.update(date=today, keys=set(), count=0)
    if _alerted["count"] >= NTFY_MAX_PER_DAY:
        return
    hits = [r for r in rows if r["multiple"] >= ALERT_MULT and (r["symbol"], r["candle"]) not in _alerted["keys"]]
    if not hits:
        return
    hits.sort(key=lambda r: -r["multiple"])
    lines = []
    for r in hits[:12]:
        chg = r.get("candle_chg_pct")
        lines.append(f"{r['symbol']}  {r['multiple']:.1f}x  Rs {r['price']:.2f}" + (f"  candle {chg:+.2f}%" if chg is not None else ""))
    extra = f"\n+{len(hits) - 12} more on the scanner page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Volume spurt >= {ALERT_MULT:g}x ({hits[0]['candle']} candle)", "Priority": "default",
                                      "Tags": "chart_with_upwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt10 ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Spurt10 ntfy failed: {e}")
        return
    for r in hits:
        _alerted["keys"].add((r["symbol"], r["candle"]))
    _alerted["count"] += 1
    log.info(f"Spurt10 alert sent for {len(hits)} stock(s) at >= {ALERT_MULT:g}x")


_zone_alerted = {"date": None, "keys": set(), "count": 0}
_watch = {"date": None, "syms": {}}


def _watch_view():
    """Rows for the watch-list table on the /spurt10 page (latest known status of every watched stock)."""
    out = []
    for sym, w in _watch["syms"].items():
        last = w.get("last") or {}
        at = last.get("at_zone", "")
        alerted = any(k[0] == sym for k in _zone_alerted["keys"])
        out.append({"symbol": sym, "since": w.get("since", ""), "mult": w.get("mult", 0), "price": last.get("price"),
                    "support": last.get("support", ""), "resistance": last.get("resistance", ""), "at_zone": at,
                    "in_table": last.get("in_table", False), "time": last.get("time", ""), "alerted": alerted,
                    "conf_support": last.get("conf_support", ""), "conf_resistance": last.get("conf_resistance", "")})
    out.sort(key=lambda x: (0 if x["at_zone"] else 1, -x["mult"]))
    return out


def _watch_rows(top, now_ist, sid_of):
    """ADDED: every stock that spurted (>= WATCH_MULT in the top-30) stays on a watch list for the day, even after it drops out of
    the top-30 table. Each cycle the stocks that are no longer in the table get their 5-min candles downloaded (merged to the zone
    time frame) so their zones can still be checked against the live price. Read-only; returns rows for _send_zone_alerts."""
    if not ZONE_ALERT_ON:
        return []
    today = now_ist.strftime("%Y-%m-%d")
    if _watch["date"] != today:
        _watch.update(date=today, syms={})
    if now_ist.hour * 60 + now_ist.minute > WATCH_UNTIL_MIN:
        return []
    syms = _watch["syms"]
    for r in top:
        if r["multiple"] >= WATCH_MULT:
            cur = syms.get(r["symbol"])
            if cur is None and len(syms) < WATCH_MAX:
                syms[r["symbol"]] = {"since": r["candle"], "mult": r["multiple"]}
            elif cur is not None and r["multiple"] > cur["mult"]:
                cur["mult"] = r["multiple"]
    in_top = {r["symbol"] for r in top}
    for r in top:                                                    # remember the latest zone status of watched stocks that are in the table
        if r["symbol"] in syms:
            syms[r["symbol"]]["last"] = {"price": r["price"], "support": r.get("support", ""), "resistance": r.get("resistance", ""),
                                         "at_zone": r.get("at_zone", ""), "in_table": True,
                                         "conf_support": r.get("conf_support", ""), "conf_resistance": r.get("conf_resistance", ""),
                                         "time": now_ist.strftime("%H:%M")}
    todo = [x for x in syms if x not in in_top and x in sid_of and not _thin_now(x)]      # a followed stock that became thin / hit a circuit is skipped
    if not todo:
        return []
    as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))

    def one(sym):
        try:
            df5 = ls.fetch_intraday_history(sid_of[sym], "NSE_EQ", 5, HISTORY_DAYS)
            d5 = ls.drop_forming_bars(df5, 5, as_of)
            zdf = ls.drop_forming_bars(ls.build_merged_bars(df5, ZONE_TF // 5), ZONE_TF, as_of)
            if zdf is None or len(zdf) < 30 or d5 is None or len(d5) == 0:
                return None
            price = float(d5["close"].iloc[-1])
            zs_w = _secA_zones(zdf)
            f = _zone_fields(zs_w, price, None)
            f.update(_conf_fields(zs_w, _conf_zones(df5, as_of), price))
            f.update(symbol=sym, price=round(price, 2), multiple=syms[sym]["mult"], candle_chg_pct=None,
                     note=f"spurt {syms[sym]['since']}")
            syms[sym]["last"] = {"price": round(price, 2), "support": f.get("support", ""), "resistance": f.get("resistance", ""),
                                 "at_zone": f.get("at_zone", ""), "in_table": False, "time": now_ist.strftime("%H:%M"),
                                 "conf_support": f.get("conf_support", ""), "conf_resistance": f.get("conf_resistance", "")}
            return f
        except Exception as e:
            log.warning(f"Spurt10 watch check failed for {sym}: {e}")
            return None
    out = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for f in ex.map(one, todo):
            if f:
                out.append(f)
    log.info(f"Spurt10 watch list: {len(syms)} stock(s), {len(todo)} checked outside the table")
    return out


def _send_zone_alerts(top, now_ist):
    """ADDED: one phone notification per cycle for every stock of the top-30 spurt table that is AT a support or resistance zone
    (the same cells that are highlighted on the page). A stock is announced once per day per zone (it is announced again only if
    its zone changes). Read-only - it only sends a notification and does not touch the spurt ranking or any signal."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (ZONE_ALERT_ON and topic):
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _zone_alerted["date"] != today:
        _zone_alerted.update(date=today, keys=set(), count=0)
    if _zone_alerted["count"] >= ZONE_ALERT_MAX:
        return
    if now_ist.hour * 60 + now_ist.minute > WATCH_UNTIL_MIN:        # tracking stops at 15:00 (SPURT10_WATCH_UNTIL)
        return
    hits = []
    for r in top:
        at = r.get("at_zone")
        if at not in ("SUPPORT", "RESISTANCE"):
            continue
        zone = r.get("support") if at == "SUPPORT" else r.get("resistance")
        key = (r["symbol"], at, zone)
        if key in _zone_alerted["keys"]:
            continue
        hits.append((r, at, zone, key))
    if not hits:
        return
    hits.sort(key=lambda h: -h[0]["multiple"])
    lines = []
    for r, at, zone, _k in hits[:12]:
        chg = r.get("candle_chg_pct")
        lines.append(f"{'RESISTANCE' if at == 'RESISTANCE' else 'SUPPORT'}  {r['symbol']}  Rs {r['price']:.2f}  zone {zone}  spurt {r['multiple']:.1f}x"
                     + (f"  candle {chg:+.2f}%" if chg is not None else "") + (f"  ({r['note']})" if r.get("note") else "")
                     + (f"  [{r['zone_view']}]" if r.get("zone_view") in ("ALIGNED", "AGAINST") else "")
                     + (f"  [15m+{CONF_TF}m zone]" if _is_conf(r) else ""))
    extra = f"\n+{len(hits) - 12} more on the scanner page" if len(hits) > 12 else ""
    kinds = {h[1] for h in hits}
    n_conf = sum(1 for h in hits if _is_conf(h[0]))
    title = "Spurt stock at " + (" / ".join(sorted(k.lower() for k in kinds))) + f" ({len(hits)})" + (f" - {n_conf} with {ZONE_TF}m+{CONF_TF}m zone" if n_conf else "")
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": title, "Priority": "default", "Tags": "bell"})
        if resp.status_code >= 300:
            log.warning(f"Spurt10 zone ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Spurt10 zone ntfy failed: {e}")
        return
    for _r, _a, _z, k in hits:
        _zone_alerted["keys"].add(k)
    _zone_alerted["count"] += 1
    log.info(f"Spurt10 zone alert sent for {len(hits)} stock(s)")


# ---------------------------------------------------------------- run + output
_remind_last = {"k": None}


def _candle_reminder(now, step_min, label):
    """ADDED: phone alert (same NTFY_TOPIC, loud priority) at every candle close with the 3 trading rules. Read-only, never raises."""
    try:
        if os.environ.get("CANDLE_REMINDER", "true").strip().lower() != "true":
            return
        topic = os.environ.get("NTFY_TOPIC", "").strip()
        if not topic:
            return
        k = (now.strftime("%Y-%m-%d"), now.hour, now.minute // step_min)
        if _remind_last["k"] == k:
            return
        _remind_last["k"] = k
        import requests
        requests.post(f"https://ntfy.sh/{topic}",
                      data="1. DON'T RUSH\n2. DON'T TRADE WITHOUT VALID SET UP\n3. 1 VALID SETUP ENOUGH TO EARN MONEY\n4. DON'T BE EMOTIONAL ATTACHED".encode("utf-8"),
                      timeout=8, headers={"Title": f"{label} candle closed - read before you trade", "Priority": "urgent", "Tags": "rotating_light"})
    except Exception as e:
        log.warning(f"Candle reminder failed: {e}")


_offers_alerted = {"date": None, "keys": set(), "count": 0}


def _send_offers_alerts(top, now_ist):
    """ADDED: phone notification when a stock of the top-30 spurt table has selling pressure above OFFERS_ALERT_PCT
    (Offers % = total sell qty / (total buy qty + total sell qty) in the Dhan depth window). A stock is announced once per day.
    Read-only - it only sends a notification and does not touch the spurt ranking or any signal."""
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (OFFERS_ALERT_ON and NTFY_ON and topic):
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _offers_alerted["date"] != today:
        _offers_alerted.update(date=today, keys=set(), count=0)
    if _offers_alerted["count"] >= OFFERS_ALERT_MAX:
        return
    hits = [r for r in top if (r.get("offers_pct") or 0) > OFFERS_ALERT_PCT and r["symbol"] not in _offers_alerted["keys"]
            and (not OFFERS_ONLY_AT_ZONE or r.get("at_zone") in ("SUPPORT", "RESISTANCE"))]   # ADDED: only at a zone; a stock that is not at one yet is not used up for the day
    if not hits:
        return
    hits.sort(key=lambda r: -(r.get("offers_pct") or 0))
    lines = []
    for r in hits[:12]:
        chg = r.get("candle_chg_pct")
        lines.append(f"{r['symbol']}  Offers {r['offers_pct']:.1f}%  Bids {100 - r['offers_pct']:.1f}%  Rs {r['price']:.2f}  spurt {r['multiple']:.1f}x"
                     + (f"  candle {chg:+.2f}%" if chg is not None else "")
                     + (f"  AT {r['at_zone']} {r.get('support') if r.get('at_zone') == 'SUPPORT' else r.get('resistance')}" if r.get("at_zone") in ("SUPPORT", "RESISTANCE") else ""))
    extra = f"\n+{len(hits) - 12} more on the scanner page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Selling pressure above {OFFERS_ALERT_PCT:g}% at a zone (spurt stocks)" if OFFERS_ONLY_AT_ZONE else f"Selling pressure above {OFFERS_ALERT_PCT:g}% (spurt stocks)", "Priority": "default",
                                      "Tags": "chart_with_downwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt10 offers ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Spurt10 offers ntfy failed: {e}")
        return
    for r in hits:
        _offers_alerted["keys"].add(r["symbol"])
    _offers_alerted["count"] += 1


# ---------------------------------------------------------------- spurt stock rejected at resistance (long upper wick)
CAND_TF = 15                 # ADDED 4 Nov: candle size (minutes, 10 or 15) used by the rejected-at-resistance, clean rejection and engulfing sections (15 = closed 15-min candles, 10 = as before)
WICK_ON = True               # ADDED: phone alert + table when a spurt stock's closed 15 (CAND_TF)-min candle is rejected at a resistance zone with a long upper wick
WICK_SPURT_MULT = 10.0       # a "spurt stock" = reached this multiple (x the 50-candle volume average) at any time today
WICK_PCT = 60.0              # upper wick of the CAND_TF-min candle >= this % of its high-low range
WICK_UNTIL_MIN = 15 * 60 + 15  # no wick alerts after 15:15 IST
WICK_WATCH_MAX = 100         # most spurt stocks followed at once
WICK_ALERT_MAX = 40          # safety cap on these notifications per day
_wick = {"date": None, "syms": {}, "keys": set(), "count": 0, "hits": []}
# ADDED: state of the clean-rejection and engulfing sections (defined here because the saved state is loaded just below)
_clean = {"date": None, "keys": set(), "count": {"R": 0, "S": 0}, "hits": []}
_engulf = {"date": None, "keys": set(), "count": {"B": 0, "E": 0}, "hits": []}


# ADDED: today's watch list, wick rejections and "already alerted" memory are saved to the results folder (the Render disk), so a restart or an
# update of the app no longer empties them or repeats a phone alert. A saved state from an earlier day is ignored by the daily reset below.
STATE_PATH = os.path.join(RESULTS_DIR, "spurt10_state.json")


def _state_save():
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        d = {"watch": _watch,
             "wick": {**_wick, "keys": [list(k) for k in _wick["keys"]]},
             "zone": {**_zone_alerted, "keys": [list(k) for k in _zone_alerted["keys"]]},
             "clean": {**_clean, "keys": [list(k) for k in _clean["keys"]]},
             "engulf": {**_engulf, "keys": [list(k) for k in _engulf["keys"]]}}
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, default=str)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        log.warning(f"Spurt10 state save failed: {e}")


def _state_load():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        _watch.clear()
        _watch.update(d.get("watch") or {"date": None, "syms": {}})
        w = d.get("wick") or {}
        _wick.update(date=w.get("date"), syms=w.get("syms") or {}, keys={tuple(k) for k in (w.get("keys") or [])},
                     count=w.get("count", 0), hits=w.get("hits") or [])
        z = d.get("zone") or {}
        _zone_alerted.update(date=z.get("date"), keys={tuple(k) for k in (z.get("keys") or [])}, count=z.get("count", 0))
        cl = d.get("clean") or {}
        _clean.update(date=cl.get("date"), keys={tuple(k) for k in (cl.get("keys") or [])}, count=cl.get("count", {"R": 0, "S": 0}) if isinstance(cl.get("count"), dict) else {"R": 0, "S": 0}, hits=cl.get("hits") or [])
        eg = d.get("engulf") or {}
        _engulf.update(date=eg.get("date"), keys={tuple(k) for k in (eg.get("keys") or [])}, count=eg.get("count") if isinstance(eg.get("count"), dict) else {"B": 0, "E": 0}, hits=eg.get("hits") or [])
        log.info(f"Spurt10 state restored: {len((_watch.get('syms') or {}))} watched, {len(_wick['hits'])} wick hits, {len(_zone_alerted['keys'])} zone alerts")
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"Spurt10 state load failed: {e}")


_state_load()


def _wick_eval(o, h, l, c, res):
    """Wick % of the candle when it touched the resistance zone `res` ("lo-hi"), has an upper wick >= WICK_PCT and closed at / below the zone top; else None."""
    try:
        rng = h - l
        if rng <= 0 or not res:
            return None
        lo, hi = (float(x) for x in str(res).split("-"))
        wick = (h - max(o, c)) / rng * 100.0
        near = ZONE_NEAR_PCT / 100.0
        if wick >= WICK_PCT and h >= lo * (1 - near) and c <= hi:
            return round(wick, 1)
    except Exception:
        pass
    return None


_cands_last = []            # (symbol, candle, o, h, l, c, price, resistance, support) of the followed spurt stocks, filled by _wick_alerts each cycle
_cands_last_meta = {"syms": {}}


def _wick_alerts(top, now_ist, sid_of):
    """ADDED: stocks that reached WICK_SPURT_MULT x today are followed until 15:15. When the latest CLOSED 10-min candle of such a stock touched its
    resistance zone (the same zones as the zone columns), has an upper wick of WICK_PCT % or more of its range and closed at / below the zone top,
    one phone alert is sent (once per stock per candle) and the stock is listed in the table on /spurt10. Read-only - it changes no signal."""
    _cands_last.clear()
    if not WICK_ON:
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _wick["date"] != today:
        _wick.update(date=today, syms={}, keys=set(), count=0, hits=[])
    if now_ist.hour * 60 + now_ist.minute > WICK_UNTIL_MIN:
        return
    syms = _wick["syms"]
    for r in top:
        if r["multiple"] >= WICK_SPURT_MULT:
            cur = syms.get(r["symbol"])
            if cur is None and len(syms) < WICK_WATCH_MAX:
                syms[r["symbol"]] = {"since": r["candle"], "mult": r["multiple"]}
            elif cur is not None and r["multiple"] > cur["mult"]:
                cur["mult"] = r["multiple"]
    if not syms:
        return
    in_top = {r["symbol"]: r for r in top}
    cands, todo = [], []
    for sym in syms:
        r = in_top.get(sym)
        if r is not None:
            oh = r.get("ohlc")
            c15 = r.get("c15")
            if CAND_TF == 15 and (r.get("resistance") or r.get("support")):
                if c15:
                    cands.append((sym, c15["t"], c15["o"], c15["h"], c15["l"], c15["c"], r["price"], r["resistance"], r.get("support", ""),
                                  {"prev": (c15["po"], c15["ph"], c15["pl"], c15["pc"], c15["pv"]), "vol": c15["v"]}))
            elif oh and (r.get("resistance") or r.get("support")):
                cands.append((sym, r["candle"], oh[0], oh[1], oh[2], oh[3], r["price"], r["resistance"], r.get("support", ""), {"prev": r.get("prev"), "vol": float(r.get("candle_vol") or 0)}))
        elif sym in sid_of and not _thin_now(sym):      # a followed stock that became thin / hit a circuit gets no wick / rejection / engulfing alert
            todo.append(sym)
    if todo:
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))

        def one(sym):
            try:
                df5 = ls.fetch_intraday_history(sid_of[sym], "NSE_EQ", 5, HISTORY_DAYS)
                d10 = ls.drop_forming_bars(ls.build_merged_bars(df5, CAND_TF // 5), CAND_TF, as_of)
                zdf = ls.drop_forming_bars(ls.build_merged_bars(df5, ZONE_TF // 5), ZONE_TF, as_of)
                if zdf is None or len(zdf) < 30 or d10 is None or len(d10) == 0:
                    return None
                last = d10.iloc[-1]
                px = float(last["close"])
                f = _zone_fields(_secA_zones(zdf), px, None)
                return (sym, _ist(d10.index[-1]).strftime("%H:%M"), float(last["open"]), float(last["high"]), float(last["low"]), px, round(px, 2), f.get("resistance", ""), f.get("support", ""),
                        {"prev": ((float(d10.iloc[-2]["open"]), float(d10.iloc[-2]["high"]), float(d10.iloc[-2]["low"]), float(d10.iloc[-2]["close"]), float(d10.iloc[-2]["volume"])) if len(d10) >= 2 else None), "vol": float(last["volume"])})
            except Exception as e:
                log.warning(f"Spurt10 wick check failed for {sym}: {e}")
                return None
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for c_ in ex.map(one, todo):
                if c_:
                    cands.append(c_)
    _cands_last[:] = list(cands)   # shared with the clean-rejection sections below
    _cands_last_meta["syms"] = syms
    hits = []
    for sym, cndl, o, h, l, c, price, res, _sup, _x in cands:
        if not res:
            continue
        key = (sym, cndl)
        if key in _wick["keys"]:
            continue
        w = _wick_eval(o, h, l, c, res)
        if w is None:
            continue
        _wick["keys"].add(key)
        hit = {"candle": cndl, "symbol": sym, "price": price, "wick": w, "resistance": res, "mult": syms[sym]["mult"], "since": syms[sym]["since"],
               "alert": now_ist.strftime("%H:%M")}
        hits.append(hit)
        _wick["hits"].append(hit)
    if not hits:
        return
    log.info(f"Spurt10 wick: {len(hits)} spurt stock(s) rejected at resistance: {[h['symbol'] for h in hits]}")
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ON and topic) or _wick["count"] >= WICK_ALERT_MAX:
        return
    hits.sort(key=lambda x: -x["mult"])
    lines = [f"{x['symbol']}  Rs {x['price']:.2f}  wick {x['wick']:.0f}%  resistance {x['resistance']}  spurt {x['mult']:.1f}x (since {x['since']})  candle {x['candle']}" for x in hits[:12]]
    extra = f"\n+{len(hits) - 12} more on the scanner page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"Spurt stock rejected at resistance - wick >= {WICK_PCT:g}% ({len(hits)})", "Priority": "urgent",
                                      "Tags": "rotating_light,chart_with_downwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"Spurt10 wick ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"Spurt10 wick ntfy failed: {e}")
        return
    _wick["count"] += 1


def _wick_html():
    """Table of today's wick rejections (kept in memory; empty after a restart). Never raises."""
    try:
        rows = list(_wick["hits"])
        head = (f'<h2>Spurt stocks rejected at resistance &mdash; upper wick &ge; {WICK_PCT:g}% &middot; spurt &ge; {WICK_SPURT_MULT:g}x ({len(rows)})</h2>'
                f'<div class="m">A stock that reached {WICK_SPURT_MULT:g}x today, whose closed {CAND_TF}-min candle touched its resistance zone ({ZONE_TF}m zones), has an upper wick of '
                f'{WICK_PCT:g}% or more of the candle and closed at / below the zone top. A phone alert is sent for each (until 15:15 IST). Information only.</div>')
        if not rows:
            return head + '<div class="m">None yet today.</div>'
        body = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td><b>{x['wick']:.0f}%</b></td><td>{x['resistance']}</td>"
                       f"<td>{x['mult']:.1f}x</td><td>{x['since']}</td><td>{x['alert']}</td></tr>" for x in reversed(rows))
        return (head + '<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Upper wick</th><th>Resistance zone</th>'
                '<th>Best spurt</th><th>Spurt at</th><th>Alert</th></tr>' + body + '</table></div>')
    except Exception:
        return ""


# ---------------------------------------------------------------- ADDED: clean rejection candles at resistance / support (spurt stocks)
CLEAN_ON = True              # phone alert + table: a followed spurt stock's closed 10-min candle touches a zone and shows a clean rejection. False = off
CLEAN_WICK_PCT = 60.0        # the rejection wick must be MORE than this % of the candle's high-low range
CLEAN_OPP_MAX_PCT = 5.0      # the opposite wick must be at most this % of the range ("no wick on the other side"; 0 = strictly none)
CLEAN_VOL_X = 1.0           # the rejection candle's volume must be MORE than this x the previous candle's volume (1.0 = any increase)
CLEAN_ALERT_MAX = 40         # safety cap on these notifications per day (each kind)


def _clean_eval(o, h, l, c, zone, kind, vol=None, prev=None):
    """kind "R": touched the resistance zone, upper wick > CLEAN_WICK_PCT, lower wick <= CLEAN_OPP_MAX_PCT, closed at / below the zone top.
    kind "S": touched the support zone, lower wick > CLEAN_WICK_PCT, upper wick <= CLEAN_OPP_MAX_PCT, closed at / above the zone bottom.
    Also the candle's volume must be more than CLEAN_VOL_X x the previous candle's volume.
    Returns (wick %, opposite wick %, volume ratio) or None."""
    try:
        rng = h - l
        if rng <= 0 or not zone or not vol or not prev or not prev[4] or vol <= prev[4] * CLEAN_VOL_X:
            return None
        vr = round(vol / prev[4], 2)
        lo, hi = (float(x) for x in str(zone).split("-"))
        up = (h - max(o, c)) / rng * 100.0
        dn = (min(o, c) - l) / rng * 100.0
        near = ZONE_NEAR_PCT / 100.0
        if kind == "R" and up > CLEAN_WICK_PCT and dn <= CLEAN_OPP_MAX_PCT and h >= lo * (1 - near) and c <= hi:
            return round(up, 1), round(dn, 1), vr
        if kind == "S" and dn > CLEAN_WICK_PCT and up <= CLEAN_OPP_MAX_PCT and l <= hi * (1 + near) and c >= lo:
            return round(dn, 1), round(up, 1), vr
    except Exception:
        pass
    return None


def _clean_alerts(now_ist):
    """ADDED: uses the closed 10-min candles of the spurt stocks already read by _wick_alerts (no extra Dhan call). One phone alert per stock per
    candle and kind. Read-only - it changes no signal."""
    if not CLEAN_ON or not _cands_last:
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _clean["date"] != today:
        _clean.update(date=today, keys=set(), count={"R": 0, "S": 0}, hits=[])
    syms = _cands_last_meta.get("syms") or {}
    hits = {"R": [], "S": []}
    for sym, cndl, o, h, l, c, price, res, sup, _x in _cands_last:
        for kind, zone in (("R", res), ("S", sup)):
            key = (sym, cndl, kind)
            if key in _clean["keys"]:
                continue
            e = _clean_eval(o, h, l, c, zone, kind, (_x or {}).get("vol"), (_x or {}).get("prev"))
            if e is None:
                continue
            _clean["keys"].add(key)
            hit = {"kind": kind, "candle": cndl, "symbol": sym, "price": price, "wick": e[0], "opp": e[1], "ratio": e[2], "zone": zone,
                   "mult": (syms.get(sym) or {}).get("mult", 0.0), "since": (syms.get(sym) or {}).get("since", ""), "alert": now_ist.strftime("%H:%M")}
            hits[kind].append(hit)
            _clean["hits"].append(hit)
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    for kind, title, tag in (("R", "Clean rejection at RESISTANCE (upper wick, no lower wick)", "chart_with_downwards_trend"),
                             ("S", "Clean rejection at SUPPORT (lower wick, no upper wick)", "chart_with_upwards_trend")):
        hs = hits[kind]
        if not hs:
            continue
        log.info(f"Spurt10 clean rejection {kind}: {[x['symbol'] for x in hs]}")
        if not (NTFY_ON and topic) or _clean["count"].get(kind, 0) >= CLEAN_ALERT_MAX:
            continue
        hs.sort(key=lambda x: -x["mult"])
        lines = [f"{x['symbol']}  Rs {x['price']:.2f}  wick {x['wick']:.0f}% (other side {x['opp']:.0f}%)  volume {x['ratio']:.1f}x previous  zone {x['zone']}  spurt {x['mult']:.1f}x  candle {x['candle']}" for x in hs[:12]]
        extra = f"\n+{len(hs) - 12} more on the scanner page" if len(hs) > 12 else ""
        try:
            import requests
            resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                                 headers={"Title": f"{title} ({len(hs)})", "Priority": "urgent", "Tags": "rotating_light," + tag})
            if resp.status_code >= 300:
                log.warning(f"Spurt10 clean ntfy refused: {resp.status_code} {resp.text[:150]}")
                continue
        except Exception as ex:
            log.warning(f"Spurt10 clean ntfy failed: {ex}")
            continue
        _clean["count"][kind] = _clean["count"].get(kind, 0) + 1


def _clean_html():
    """Two tables: clean rejections at resistance and at support today (kept on the results folder). Never raises."""
    try:
        out = ""
        for kind, head, wl, zl, note in (
                ("R", "Clean top rejection at resistance", "Upper wick", "Resistance zone",
                 f"candle touched the resistance zone, upper wick more than {CLEAN_WICK_PCT:g}% of the candle and almost no lower wick (at most {CLEAN_OPP_MAX_PCT:g}%), and volume more than {CLEAN_VOL_X:g}x the previous candle"),
                ("S", "Clean bottom rejection at support", "Lower wick", "Support zone",
                 f"candle touched the support zone, lower wick more than {CLEAN_WICK_PCT:g}% of the candle and almost no upper wick (at most {CLEAN_OPP_MAX_PCT:g}%), and volume more than {CLEAN_VOL_X:g}x the previous candle")):
            rows = [x for x in _clean["hits"] if x.get("kind") == kind]
            out += (f'<h2>{head} &mdash; spurt stocks &ge; {WICK_SPURT_MULT:g}x ({len(rows)})</h2>'
                    f'<div class="m">Closed {CAND_TF}-min candle of a stock that reached {WICK_SPURT_MULT:g}x today: {note}. Zones are the {ZONE_TF}m order-block zones. '
                    f'A phone alert is sent for each (until 15:15 IST). Information only. <a href="/spurt10_patterns.csv">CSV with follow-up (all days)</a></div>')
            if not rows:
                out += '<div class="m">None yet today.</div>'
                continue
            body = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td><b>{x['wick']:.0f}%</b></td><td>{x['opp']:.0f}%</td><td><b>{x.get('ratio', 0):.1f}x</b></td><td>{x['zone']}</td>"
                           f"<td>{x['mult']:.1f}x</td><td>{x['since']}</td><td>{x['alert']}</td></tr>" for x in reversed(rows))
            other = "Lower wick" if kind == "R" else "Upper wick"
            out += (f'<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>{wl}</th><th>{other}</th><th>Volume vs previous</th><th>{zl}</th>'
                    '<th>Best spurt</th><th>Spurt at</th><th>Alert</th></tr>' + body + '</table></div>')
        return out
    except Exception:
        return ""


# ---------------------------------------------------------------- ADDED: engulfing candles at support / resistance with rising volume (spurt stocks)
ENGULF_ON = True             # phone alert + table: bullish engulfing at support / bearish engulfing at resistance on a followed spurt stock. False = off
ENGULF_VOL_X = 1.0           # the engulfing candle's volume must be MORE than this x the previous candle's volume (1.0 = any increase)
ENGULF_ALERT_MAX = 40        # safety cap on these notifications per day (each kind)


def _engulf_eval(o, h, l, c, vol, prev, support, resistance):
    """Returns ("B" bullish at support / "E" bearish at resistance, volume ratio) or None.
    Bullish: previous candle red, this one green, its body covers the previous body (open <= prev close, close >= prev open), either candle touched the
    support zone, it closed at / above the zone bottom, volume > ENGULF_VOL_X x previous volume. Bearish: the mirror image at the resistance zone."""
    try:
        if not prev or not vol or not prev[4] or vol <= prev[4] * ENGULF_VOL_X:
            return None
        po, ph, pl, pc, pv = prev
        near = ZONE_NEAR_PCT / 100.0
        ratio = round(vol / pv, 2)
        if pc < po and c > o and o <= pc and c >= po and support:
            lo, hi = (float(x) for x in str(support).split("-"))
            if min(l, pl) <= hi * (1 + near) and c >= lo:
                return "B", ratio
        if pc > po and c < o and o >= pc and c <= po and resistance:
            lo, hi = (float(x) for x in str(resistance).split("-"))
            if max(h, ph) >= lo * (1 - near) and c <= hi:
                return "E", ratio
    except Exception:
        pass
    return None


def _engulf_alerts(now_ist):
    """ADDED: uses the closed 10-min candles of the spurt stocks already read by _wick_alerts (no extra Dhan call). One phone alert per stock per candle and kind."""
    if not ENGULF_ON or not _cands_last:
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _engulf["date"] != today:
        _engulf.update(date=today, keys=set(), count={"B": 0, "E": 0}, hits=[])
    syms = _cands_last_meta.get("syms") or {}
    hits = {"B": [], "E": []}
    for sym, cndl, o, h, l, c, price, res, sup, x in _cands_last:
        e = _engulf_eval(o, h, l, c, (x or {}).get("vol"), (x or {}).get("prev"), sup, res)
        if e is None or (sym, cndl, e[0]) in _engulf["keys"]:
            continue
        _engulf["keys"].add((sym, cndl, e[0]))
        hit = {"kind": e[0], "candle": cndl, "symbol": sym, "price": price, "ratio": e[1], "zone": sup if e[0] == "B" else res,
               "mult": (syms.get(sym) or {}).get("mult", 0.0), "since": (syms.get(sym) or {}).get("since", ""), "alert": now_ist.strftime("%H:%M")}
        hits[e[0]].append(hit)
        _engulf["hits"].append(hit)
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    for kind, title, tag in (("B", "Bullish ENGULFING at SUPPORT + volume up", "chart_with_upwards_trend"),
                             ("E", "Bearish ENGULFING at RESISTANCE + volume up", "chart_with_downwards_trend")):
        hs = hits[kind]
        if not hs:
            continue
        log.info(f"Spurt10 engulfing {kind}: {[y['symbol'] for y in hs]}")
        if not (NTFY_ON and topic) or _engulf["count"].get(kind, 0) >= ENGULF_ALERT_MAX:
            continue
        hs.sort(key=lambda y: -y["mult"])
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  volume {y['ratio']:.1f}x previous candle  zone {y['zone']}  spurt {y['mult']:.1f}x  candle {y['candle']}" for y in hs[:12]]
        extra = f"\n+{len(hs) - 12} more on the scanner page" if len(hs) > 12 else ""
        try:
            import requests
            resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                                 headers={"Title": f"{title} ({len(hs)})", "Priority": "urgent", "Tags": tag})
            if resp.status_code >= 300:
                log.warning(f"Spurt10 engulfing ntfy refused: {resp.status_code} {resp.text[:150]}")
                continue
        except Exception as ex:
            log.warning(f"Spurt10 engulfing ntfy failed: {ex}")
            continue
        _engulf["count"][kind] = _engulf["count"].get(kind, 0) + 1


def _engulf_html():
    """Two tables: bullish engulfing at support and bearish engulfing at resistance today. Never raises."""
    try:
        out = ""
        for kind, head, zl, note in (
                ("B", "Bullish engulfing at support", "Support zone",
                 "previous candle red, this candle green and its body covers the previous body, touching the support zone"),
                ("E", "Bearish engulfing at resistance", "Resistance zone",
                 "previous candle green, this candle red and its body covers the previous body, touching the resistance zone")):
            rows = [x for x in _engulf["hits"] if x.get("kind") == kind]
            out += (f'<h2>{head} &mdash; with rising volume &middot; spurt stocks &ge; {WICK_SPURT_MULT:g}x ({len(rows)})</h2>'
                    f'<div class="m">Closed {CAND_TF}-min candle of a stock that reached {WICK_SPURT_MULT:g}x today: {note}, and its volume is more than {ENGULF_VOL_X:g}x the previous candle. '
                    f'Zones are the {ZONE_TF}m order-block zones. A phone alert is sent for each (until 15:15 IST). Information only. <a href="/spurt10_patterns.csv">CSV with follow-up (all days)</a></div>')
            if not rows:
                out += '<div class="m">None yet today.</div>'
                continue
            body = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td><b>{x['ratio']:.1f}x</b></td><td>{x['zone']}</td>"
                           f"<td>{x['mult']:.1f}x</td><td>{x['since']}</td><td>{x['alert']}</td></tr>" for x in reversed(rows))
            out += (f'<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Volume vs previous</th><th>{zl}</th>'
                    '<th>Best spurt</th><th>Spurt at</th><th>Alert</th></tr>' + body + '</table></div>')
        return out
    except Exception:
        return ""



# ---------------------------------------------------------------- ADDED: csv of the clean-rejection / engulfing alerts with the price follow-up
# results/spurt10_patterns.csv (all days). One row per alert; the follow-up columns are the move AFTER the alert counted in the direction the signal
# expected (+ = it worked: price fell after a top rejection / bearish engulfing, rose after a bottom rejection / bullish engulfing). Read-only.
PAT_CSV = os.path.join(RESULTS_DIR, "spurt10_patterns.csv")
_PAT_NAME = {"R": "TOP REJECTION", "S": "BOTTOM REJECTION", "B": "BULLISH ENGULFING", "E": "BEARISH ENGULFING"}
_PAT_SIGN = {"S": 1, "B": 1, "R": -1, "E": -1}
_PAT_COLS = ["date", "pattern", "symbol", "candle", "alert_time", "price", "zone", "wick_pct", "other_wick_pct", "vol_vs_prev", "spurt_mult", "spurt_since",
             "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "result"]


def _pat_mins(hm):
    try:
        return int(str(hm)[:2]) * 60 + int(str(hm)[3:5])
    except Exception:
        return None


def _pat_row(day, x):
    last = x.get("last")
    res = "open" if (last is None or x.get("r60") is None) else ("WORKED" if last >= 0.3 else "FAILED" if last <= -0.3 else "FLAT")
    g = lambda k: "" if x.get(k) is None else x[k]
    return [day, _PAT_NAME[x["kind"]], x["symbol"], x["candle"], x["alert"], x["price"], x["zone"], g("wick"), g("opp"), g("ratio"), x["mult"], x["since"],
            g("r10"), g("r30"), g("r60"), g("best"), g("worst"), g("last"), g("last_t"), res]


def _pat_write(day, hits):
    """Rows of other days stay as they are, rows of `day` are replaced by the current hits (so the follow-up columns keep updating)."""
    try:
        import csv
        old = []
        if os.path.exists(PAT_CSV):
            with open(PAT_CSV, newline="", encoding="utf-8") as f:
                rd = csv.reader(f)
                next(rd, None)
                old = [r for r in rd if r and r[0] != day]
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = PAT_CSV + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(_PAT_COLS)
            w.writerows(old)
            w.writerows(_pat_row(day, x) for x in hits)
        os.replace(tmp, PAT_CSV)
    except Exception as e:
        log.warning(f"Spurt10 pattern csv write failed: {e}")


def _pat_roll(now_ist):
    """Called BEFORE the two sections run: on a new day, yesterday's alerts are saved to the csv before the lists are cleared."""
    try:
        today = now_ist.strftime("%Y-%m-%d")
        old = _clean.get("date") or _engulf.get("date")
        if old and old != today:
            _pat_write(_clean.get("date") or old, list(_clean["hits"]) + list(_engulf["hits"]))
    except Exception as e:
        log.warning(f"Spurt10 pattern roll failed: {e}")


def _pat_follow(now_ist):
    """Called AFTER the two sections: move every alert of today forward with the latest price of the stock, then rewrite today's rows."""
    try:
        hm = now_ist.strftime("%H:%M")
        now_m = _pat_mins(hm)
        px = {c[0]: float(c[6]) for c in _cands_last if c[6]}
        hits = list(_clean["hits"]) + list(_engulf["hits"])
        for x in hits:
            if not x.get("p0"):
                x["p0"] = x["price"]
            p, a = px.get(x["symbol"]), _pat_mins(x["alert"])
            if p is None or a is None or now_m is None or not x["p0"]:
                continue
            mv = round((p / x["p0"] - 1.0) * 100.0 * _PAT_SIGN[x["kind"]], 2)
            x["best"] = mv if x.get("best") is None else max(x["best"], mv)
            x["worst"] = mv if x.get("worst") is None else min(x["worst"], mv)
            x["last"], x["last_t"] = mv, hm
            for lab, mm in (("r10", 10), ("r30", 30), ("r60", 60)):
                if x.get(lab) is None and now_m - a >= mm:
                    x[lab] = mv
        _pat_write(now_ist.strftime("%Y-%m-%d"), hits)
    except Exception as e:
        log.warning(f"Spurt10 pattern follow-up failed: {e}")


# ---------------------------------------------------------------- ADDED: breakout / breakdown WITH ROOM (BUY + SELL phone alerts, own page, own csv)
# A spurt stock whose price has just moved ABOVE the resistance zone it was under in the previous cycle (breakout) with the NEXT resistance still far away
# (or none), or just moved BELOW the support zone it was above (breakdown) with the NEXT support still far away. Zones = the same Section A zones as the table.
# Information only. Follow-up (10/30/60 min, best/worst/last, NIFTY baseline) is written to results/spurt10_room.csv (all days).
ROOM_ON = True
ROOM_MIN_MULT = 3.0           # candle volume at least this many times the average
ROOM_MIN_PCT = 2.0            # the next zone must be at least this % away (or there is no next zone)
ROOM_BREAK_PCT = 0.1          # price must be this % beyond the broken zone
ROOM_ALERT_MAX = 40           # phone alerts per kind per day
ROOM_CSV = os.path.join(RESULTS_DIR, "spurt10_room.csv")
ROOM_COLS = ["date", "side", "symbol", "candle", "alert_time", "price", "broken_zone", "next_zone", "room_pct", "candle_pct", "multiple",
             "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "result",
             "nifty_at_alert", "nifty_10m", "nifty_30m", "nifty_60m", "nifty_last", "rel_last", "gap_filled"]
_room = {"date": "", "mem": {}, "keys": set(), "hits": [], "count": {"BUY": 0, "SELL": 0, "ZSELL": 0, "UREJ": 0, "GUP": 0, "GDN": 0, "ORBUP": 0, "ORBDN": 0, "ORBIN": 0}}

ORB_ON = True                # ADDED 5 Nov: opening range = the first 15-min candle of the day (09:15-09:30). ORBUP / ORBDN = a closed 15-min candle of a spurt stock CLOSES above the range high / below the range low (alert once per stock per day)
ORB_BREAK_UNTIL = 15 * 60    # breakout alerts are given until this time (minutes after midnight, IST)
ORB_INSIDE_UNTIL = 13 * 60   # ORBIN: a spurt stock (>= ALERT_MULT) that is still INSIDE the opening range - alert once per stock per day until 13:00
GAP_ON = True                # gap up / gap down page: today's open is more than GAP_MIN_PCT % away from the previous close AND the stock shows a spurt candle (more than GAP_MIN_MULT x)
GAP_MIN_PCT = 3.0
GAP_MIN_MULT = 10.0
UREJ_ON = True               # rejected at the UPPER part of a resistance zone: closed CAND_TF-min candle of a followed spurt stock reaches the upper part of the zone and is rejected
UREJ_FROM = 0.5              # the "upper part" starts this far up the zone (0.5 = the top half of the zone, 0.75 = the top quarter); the high must reach it
ZSELL_ON = True              # SELL alert: spurt stock (>= ALERT_MULT) with a RED candle at a resistance zone that is also a 30m zone (15m+30m), candle volume above the previous candle


def _room_zone(s):
    try:
        a, b = str(s).split("-")
        return float(a), float(b)
    except Exception:
        return None


def _room_load(today):
    """After a restart: take today's rows back from the csv so nothing is lost or alerted twice."""
    try:
        import csv
        if not os.path.exists(ROOM_CSV):
            return
        with open(ROOM_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("date") != today:
                    continue
                x = {"side": r["side"], "symbol": r["symbol"], "candle": r["candle"], "alert": r["alert_time"], "price": float(r["price"] or 0),
                     "broken": r["broken_zone"], "next": r["next_zone"], "room": r["room_pct"], "cpct": r["candle_pct"], "mult": r["multiple"], "p0": float(r["price"] or 0)}
                for k, c in (("r10", "ret_10m"), ("r30", "ret_30m"), ("r60", "ret_60m"), ("best", "best_pct"), ("worst", "worst_pct"), ("last", "last_pct"),
                             ("n0", "nifty_at_alert"), ("n10", "nifty_10m"), ("n30", "nifty_30m"), ("n60", "nifty_60m"), ("nlast", "nifty_last")):
                    try:
                        x[k] = float(r[c]) if r.get(c) not in (None, "") else None
                    except Exception:
                        x[k] = None
                x["last_t"] = r.get("last_time", "")
                x["filled"] = r.get("gap_filled", "") or ""
                try:
                    _lo, _hi = (float(v) for v in str(x["broken"]).split("-"))
                    x["pc"] = _lo if x["side"] == "GUP" else _hi
                except Exception:
                    pass
                _room["hits"].append(x)
                _room["keys"].add((x["symbol"], x["side"]))
    except Exception as e:
        log.warning(f"Spurt10 room load failed: {e}")


def _room_row(day, x):
    g = lambda k: "" if x.get(k) is None else x[k]
    last = x.get("last")
    res = "open" if (last is None or x.get("r60") is None) else ("WORKED" if last >= 0.3 else "FAILED" if last <= -0.3 else "FLAT")
    rel = "" if (x.get("last") is None or x.get("nlast") is None) else round(x["last"] - x["nlast"], 2)
    return [day, x["side"], x["symbol"], x["candle"], x["alert"], x["price"], x["broken"], x["next"], x["room"], x["cpct"], x["mult"],
            g("r10"), g("r30"), g("r60"), g("best"), g("worst"), g("last"), g("last_t"), res, g("n0"), g("n10"), g("n30"), g("n60"), g("nlast"), rel, x.get("filled", "")]


def _room_write(day, hits):
    try:
        import csv
        old = []
        if os.path.exists(ROOM_CSV):
            with open(ROOM_CSV, newline="", encoding="utf-8") as f:
                rd = csv.reader(f)
                next(rd, None)
                old = [r for r in rd if r and r[0] != day]
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = ROOM_CSV + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(ROOM_COLS)
            w.writerows(old)
            w.writerows(_room_row(day, x) for x in hits)
        os.replace(tmp, ROOM_CSV)
    except Exception as e:
        log.warning(f"Spurt10 room csv write failed: {e}")


def _room_send(side, hs):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not hs or not (NTFY_ON and topic) or _room["count"].get(side, 0) >= ROOM_ALERT_MAX:
        return
    title, tag = {"BUY": ("BREAKOUT with room (BUY side)", "rocket"), "SELL": ("BREAKDOWN with room (SELL side)", "small_red_triangle_down"),
                  "ZSELL": ("SELL at 15m+30m zone + volume up", "chart_with_downwards_trend"),
                  "UREJ": ("REJECTED at UPPER part of resistance zone", "chart_with_downwards_trend"),
                  "GUP": ("GAP UP + volume spurt candle", "chart_with_upwards_trend"), "GDN": ("GAP DOWN + volume spurt candle", "chart_with_downwards_trend"),
                  "ORBUP": ("ORB BREAKOUT UP (closed above the opening range)", "rocket"), "ORBDN": ("ORB BREAKDOWN (closed below the opening range)", "small_red_triangle_down"),
                  "ORBIN": ("Spurt stocks INSIDE the opening range", "hourglass_flowing_sand")}[side]
    hs.sort(key=lambda y: -float(y["mult"] or 0))
    if side in ("GUP", "GDN"):
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  gap {'+' if side == 'GUP' else ''}{y['room']}%  ({y['broken']}, {y['next']})  spurt {float(y['mult']):.1f}x  candle {y['candle']}" for y in hs[:12]]
    elif side in ("ORBUP", "ORBDN"):
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  15m candle {y['candle']} closed {'above' if side == 'ORBUP' else 'below'} the opening range {y['broken']}  spurt {float(y['mult']):.1f}x" for y in hs[:12]]
    elif side == "ORBIN":
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  inside the opening range {y['broken']}  spurt {float(y['mult']):.1f}x" for y in hs[:12]]
    elif side == "UREJ":
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  high reached the upper part of resistance {y['broken']}  upper wick {y['room']}%  spurt {float(y['mult']):.1f}x  candle {y['candle']}" for y in hs[:12]]
    elif side == "ZSELL":
        lines = [f"{y['symbol']}  Rs {y['price']:.2f}  red 15m candle at resistance {y['broken']} (15m+30m {y['next']})  volume {y['room']}x previous  spurt {float(y['mult']):.1f}x" for y in hs[:12]]
    else:
      lines = [f"{y['symbol']}  Rs {y['price']:.2f}  {'above' if side == 'BUY' else 'below'} {y['broken']}  next zone {y['next'] or 'none'}  room {y['room']}{'' if y['room'] == 'no zone' else '%'}  volume {float(y['mult']):.1f}x" for y in hs[:12]]
    extra = f"\n+{len(hs) - 12} more on the scanner page" if len(hs) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"{title} ({len(hs)})", "Priority": ("urgent" if side == "UREJ" else "default"), "Tags": ("rotating_light," + tag if side == "UREJ" else tag)})
        if resp.status_code < 300:
            _room["count"][side] = _room["count"].get(side, 0) + 1
        else:
            log.warning(f"Spurt10 room ntfy refused: {resp.status_code}")
    except Exception as ex:
        log.warning(f"Spurt10 room ntfy failed: {ex}")


def _room_update(top, quotes, now_ist):
    """Called once per cycle with the table rows and the live quotes. Never raises."""
    if not ROOM_ON:
        return
    try:
        today = now_ist.strftime("%Y-%m-%d")
        if _room["date"] != today:
            if _room["date"] and _room["hits"]:
                _room_write(_room["date"], _room["hits"])
            _room.update(date=today, mem={}, keys=set(), hits=[], count={"BUY": 0, "SELL": 0, "ZSELL": 0, "UREJ": 0, "GUP": 0, "GDN": 0, "ORBUP": 0, "ORBDN": 0, "ORBIN": 0})
            _room_load(today)
        hm = now_ist.strftime("%H:%M")
        new = {"BUY": [], "SELL": [], "ZSELL": [], "UREJ": [], "GUP": [], "GDN": [], "ORBUP": [], "ORBDN": [], "ORBIN": []}
        for r in top:
            sym, px = r.get("symbol"), r.get("price")
            if not sym or not px:
                continue
            prev = _room["mem"].get(sym)
            res, sup = _room_zone(r.get("resistance")), _room_zone(r.get("support"))
            mult, cc = float(r.get("multiple") or 0), r.get("candle_chg_pct")
            if prev and mult >= ROOM_MIN_MULT and cc is not None:
                for side, key in (("BUY", "res"), ("SELL", "sup")):
                    z = prev.get(key)
                    if not z or (sym, side) in _room["keys"]:
                        continue
                    if side == "BUY":
                        ok = px > z[1] * (1 + ROOM_BREAK_PCT / 100.0) and cc > 0
                        nxt = [t for t in [res] if t and t[0] > z[1]]
                        room = (res[0] - px) / px * 100.0 if (res and res[0] > px) else (None if res else 99.0)
                    else:
                        ok = px < z[0] * (1 - ROOM_BREAK_PCT / 100.0) and cc < 0
                        room = (px - sup[1]) / px * 100.0 if (sup and sup[1] < px) else (None if sup else 99.0)
                    if not ok or room is None or room < ROOM_MIN_PCT:
                        continue
                    n0 = _nifty_px()[0]
                    nz = r.get("resistance") if side == "BUY" else r.get("support")
                    x = {"side": side, "symbol": sym, "candle": r.get("candle", ""), "alert": hm, "price": float(px), "p0": float(px),
                         "broken": f"{z[0]:g}-{z[1]:g}", "next": nz or "", "room": round(min(room, 99.0), 2) if room < 99 else "no zone",
                         "cpct": round(float(cc), 2), "mult": round(mult, 1), "n0": n0}
                    _room["keys"].add((sym, side))
                    _room["hits"].append(x)
                    new[side].append(x)
            try:                                                       # SELL at 15m+30m resistance zone, red candle, volume up
                c15, cz, zr = r.get("c15"), _room_zone(r.get("conf_resistance")), _room_zone(r.get("resistance"))
                if ZSELL_ON and c15 and cz and zr and mult >= ALERT_MULT and (sym, "ZSELL", c15["t"]) not in _room["keys"]:
                    vr = c15["v"] / c15["pv"] if c15["pv"] else 0.0
                    if c15["c"] < c15["o"] and vr > 1.0 and c15["h"] >= zr[0] * (1 - ZONE_NEAR_PCT / 100.0):
                        _room["keys"].add((sym, "ZSELL", c15["t"]))
                        x = {"side": "ZSELL", "symbol": sym, "candle": c15["t"], "alert": hm, "price": float(px), "p0": float(px),
                             "broken": f"{zr[0]:g}-{zr[1]:g}", "next": f"{cz[0]:g}-{cz[1]:g}", "room": round(vr, 1), "cpct": round((c15["c"] / c15["o"] - 1) * 100, 2), "mult": round(mult, 1), "n0": _nifty_px()[0]}
                        _room["hits"].append(x)
                        new["ZSELL"].append(x)
            except Exception:
                pass
            try:                                                       # gap up / gap down with a spurt candle
                gp = r.get("gap_pct")
                if GAP_ON and gp is not None and abs(gp) > GAP_MIN_PCT and mult > GAP_MIN_MULT:
                    gs = "GUP" if gp > 0 else "GDN"
                    if (sym, gs) not in _room["keys"]:
                        op0, pc0, ph0, pl0 = r["gap_open"], r["gap_pc"], r["gap_ph"], r["gap_pl"]
                        where = ("above previous day's high" if op0 > ph0 else "inside previous day's range") if gs == "GUP" else ("below previous day's low" if op0 < pl0 else "inside previous day's range")
                        x = {"side": gs, "symbol": sym, "candle": r.get("candle", ""), "alert": hm, "price": float(px), "p0": float(px), "pc": pc0,
                             "broken": f"{min(op0, pc0):g}-{max(op0, pc0):g}", "next": where, "room": round(gp, 2), "cpct": round(float(cc), 2) if cc is not None else 0.0,
                             "mult": round(mult, 1), "n0": _nifty_px()[0], "filled": ""}
                        _room["keys"].add((sym, gs))
                        _room["hits"].append(x)
                        new[gs].append(x)
            except Exception:
                pass
            try:                                                       # opening range (first 15-min candle): breakout up / down on a closed 15-min candle, and still inside the range until 13:00
                orb, c15 = r.get("orb"), r.get("c15")
                if ORB_ON and orb and mult >= ALERT_MULT:
                    ohi, olo = orb
                    nowm0 = now_ist.hour * 60 + now_ist.minute
                    rng_s = f"{olo:g}-{ohi:g}"
                    if c15 and c15["t"] != "09:15" and nowm0 <= ORB_BREAK_UNTIL:
                        for os_, okk in (("ORBUP", c15["c"] > ohi), ("ORBDN", c15["c"] < olo)):
                            if okk and (sym, os_) not in _room["keys"]:
                                x = {"side": os_, "symbol": sym, "candle": c15["t"], "alert": hm, "price": float(px), "p0": float(px), "broken": rng_s, "next": "",
                                     "room": round((c15["c"] / (ohi if os_ == "ORBUP" else olo) - 1) * 100, 2), "cpct": round((c15["c"] / c15["o"] - 1) * 100, 2) if c15["o"] else 0.0,
                                     "mult": round(mult, 1), "n0": _nifty_px()[0]}
                                _room["keys"].add((sym, os_))
                                _room["hits"].append(x)
                                new[os_].append(x)
                    if 9 * 60 + 30 <= nowm0 <= ORB_INSIDE_UNTIL and olo <= px <= ohi and (sym, "ORBIN") not in _room["keys"]:
                        x = {"side": "ORBIN", "symbol": sym, "candle": (c15 or {}).get("t", ""), "alert": hm, "price": float(px), "p0": float(px), "broken": rng_s, "next": "",
                             "room": round((px - olo) / (ohi - olo) * 100, 0) if ohi > olo else 0, "cpct": round(float(cc), 2) if cc is not None else 0.0, "mult": round(mult, 1), "n0": _nifty_px()[0]}
                        _room["keys"].add((sym, "ORBIN"))
                        _room["hits"].append(x)
                        new["ORBIN"].append(x)
            except Exception:
                pass
            _room["mem"][sym] = {"res": res, "sup": sup}
        try:                                                           # rejected at the UPPER part of the resistance zone (followed spurt stocks, CAND_TF-min candles)
            symsm = _cands_last_meta.get("syms") or {}
            for sym, cndl, o, h, l, c, price, res_z, _sp, _x in (_cands_last if UREJ_ON else []):
                z = _room_zone(res_z)
                if not z or (sym, "UREJ", cndl) in _room["keys"] or h - l <= 0:
                    continue
                wick = (h - max(o, c)) / (h - l) * 100.0
                if wick >= WICK_PCT and h >= z[0] + UREJ_FROM * (z[1] - z[0]) and c <= z[1]:
                    _room["keys"].add((sym, "UREJ", cndl))
                    x = {"side": "UREJ", "symbol": sym, "candle": cndl, "alert": hm, "price": float(price), "p0": float(price), "broken": f"{z[0]:g}-{z[1]:g}", "next": "",
                         "room": round(wick, 1), "cpct": round((c / o - 1) * 100, 2) if o else 0.0, "mult": round(float((symsm.get(sym) or {}).get("mult", 0.0)), 1), "n0": _nifty_px()[0]}
                    _room["hits"].append(x)
                    new["UREJ"].append(x)
        except Exception as e:
            log.warning(f"Spurt10 upper-zone step failed: {e}")
        # follow-up of every hit of today with the live quote
        now_m = now_ist.hour * 60 + now_ist.minute
        nf = _nifty_px()[0]
        for x in _room["hits"]:
            q = (quotes or {}).get(x["symbol"])
            p = q[0] if q else None
            a = _pat_mins(x["alert"])
            if not p or not x.get("p0") or a is None:
                continue
            sg = 1 if x["side"] in ("BUY", "GUP", "ORBUP", "ORBIN") else -1       # SELL, ZSELL, UREJ: a fall counts as +; gaps: a move in the gap direction counts as +
            mv = round((p / x["p0"] - 1.0) * 100.0 * sg, 2)
            x["best"] = mv if x.get("best") is None else max(x["best"], mv)
            x["worst"] = mv if x.get("worst") is None else min(x["worst"], mv)
            x["last"], x["last_t"] = mv, hm
            if x["side"] in ("GUP", "GDN") and x.get("pc") and not x.get("filled"):
                if (x["side"] == "GUP" and p <= x["pc"]) or (x["side"] == "GDN" and p >= x["pc"]):
                    x["filled"] = "YES " + hm                      # price is back at the previous close = the gap is filled
            nm = round((nf / x["n0"] - 1.0) * 100.0, 2) if (nf and x.get("n0")) else None
            if nm is not None:
                x["nlast"] = nm
            for lab, nlab, mm in (("r10", "n10", 10), ("r30", "n30", 30), ("r60", "n60", 60)):
                if x.get(lab) is None and now_m - a >= mm:
                    x[lab] = mv
                    x[nlab] = nm
        if _room["hits"]:
            _room_write(today, _room["hits"])
        for side in ("BUY", "SELL", "ZSELL", "UREJ", "GUP", "GDN", "ORBUP", "ORBDN", "ORBIN"):
            if new[side]:
                log.info(f"Spurt10 room {side}: {[y['symbol'] for y in new[side]]}")
                _room_send(side, new[side])
    except Exception as e:
        log.warning(f"Spurt10 room step failed: {e}")


def _room_html():
    try:
        out = ""
        for side, head, word in (("BUY", "Breakout with room", "above the resistance zone it was under"), ("SELL", "Breakdown with room", "below the support zone it was above")):
            rows = [x for x in _room["hits"] if x["side"] == side]
            out += (f'<h2>{head} &mdash; {"BUY" if side == "BUY" else "SELL"} side &middot; volume &ge; {ROOM_MIN_MULT:g}x &middot; next zone &ge; {ROOM_MIN_PCT:g}% away ({len(rows)})</h2>'
                    f'<div class="m">Spurt stock whose price moved {word} in the previous cycle, with the next zone still far away. Only liquid stocks: buy-side and sell-side waiting quantity each at least {LIQ_MIN_DEPTH_QTY:,} shares, not near a circuit, no news. Phone alert for each. Information only. '
                    f'<a href="/{"spurt10_room" if side == "BUY" else "spurt10_roomdown"}.csv">CSV with follow-up and NIFTY baseline (all days)</a></div>')
            if not rows:
                out += '<div class="m">None yet today.</div>'
                continue
            body = "".join(
                f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td>{x['broken']}</td><td>{x['next'] or 'none'}</td><td>{x['room']}</td>"
                f"<td>{x['mult']}x</td><td>{x['alert']}</td><td>{'' if x.get('last') is None else x['last']}</td><td>{'' if x.get('best') is None else x['best']}</td></tr>" for x in reversed(rows))
            out += ('<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Broken zone</th><th>Next zone</th><th>Room %</th>'
                    '<th>Volume</th><th>Alert</th><th>Move now %</th><th>Best %</th></tr>' + body + '</table></div>')
        rows = [x for x in _room["hits"] if x["side"] == "ZSELL"]
        out += (f'<h2>SELL at {ZONE_TF}m+{CONF_TF}m resistance zone &mdash; red 15-min candle with rising volume &middot; spurt &ge; {ALERT_MULT:g}x ({len(rows)})</h2>'
                f'<div class="m">Closed 15-min candle of a stock with a 10-min volume spurt: red, its high touched a resistance zone that also exists on the {CONF_TF}-min chart, and its volume is above the previous 15-min candle. '
                'Phone alert for each. Information only. <a href="/spurt10_zsell.csv">CSV with follow-up (all days)</a></div>')
        if rows:
            body = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td>{x['broken']}</td><td>{x['next']}</td><td>{x['room']}x</td>"
                            f"<td>{x['mult']}x</td><td>{x['alert']}</td><td>{'' if x.get('last') is None else x['last']}</td><td>{'' if x.get('best') is None else x['best']}</td></tr>" for x in reversed(rows))
            out += ('<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Resistance zone</th><th>30m zone</th><th>Volume vs previous</th>'
                    '<th>Spurt</th><th>Alert</th><th>Move now % (fall = +)</th><th>Best %</th></tr>' + body + '</table></div>')
        else:
            out += '<div class="m">None yet today.</div>'
        for gs, ghead, gnote in (("GUP", "Gap up with a volume spurt candle", "opened ABOVE"), ("GDN", "Gap down with a volume spurt candle", "opened BELOW")):
            grows = [x for x in _room["hits"] if x["side"] == gs]
            out += (f'<h2>{ghead} &mdash; gap more than {GAP_MIN_PCT:g}% &middot; spurt more than {GAP_MIN_MULT:g}x ({len(grows)})</h2>'
                    f'<div class="m">The stock {gnote} the previous close by more than {GAP_MIN_PCT:g}% and shows a spurt candle of more than {GAP_MIN_MULT:g}x. The first time it qualifies it is listed here with a phone alert. '
                    'Move now = % move from the price at the alert, counted in the gap direction (+ = the gap is continuing). Gap filled = price has come back to the previous close. '
                    'Information only. <a href="/spurt10_gap.csv">CSV with follow-up (all days)</a></div>')
            if grows:
                gb = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td><b>{x['room']:+.2f}%</b></td><td>{x['broken']}</td><td>{x['next']}</td>"
                             f"<td>{x['mult']}x</td><td>{x['alert']}</td><td>{'' if x.get('last') is None else x['last']}</td><td>{x.get('filled') or ''}</td></tr>" for x in reversed(grows))
                out += ('<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Gap %</th><th>Gap (previous close to open)</th><th>Against previous day</th>'
                        '<th>Spurt</th><th>Alert</th><th>Move now % (gap direction)</th><th>Gap filled</th></tr>' + gb + '</table></div>')
            else:
                out += '<div class="m">None yet today.</div>'
        rows = [x for x in _room["hits"] if x["side"] == "UREJ"]
        out += (f'<h2>Rejected at the UPPER part of the resistance zone &mdash; {CAND_TF}-min candle, upper wick &ge; {WICK_PCT:g}% ({len(rows)})</h2>'
                f'<div class="m">A stock that reached {WICK_SPURT_MULT:g}x today whose closed {CAND_TF}-min candle pushed into the upper part of its resistance zone (the high reached at least {UREJ_FROM * 100:.0f}% of the way up the zone), '
                f'has an upper wick of {WICK_PCT:g}% or more and closed at / below the zone top. Price went deep into the zone and was still pushed back. Phone alert for each. Information only. '
                '<a href="/spurt10_upper.csv">CSV with follow-up (all days)</a></div>')
        if rows:
            body = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td>{x['broken']}</td><td><b>{x['room']}%</b></td>"
                            f"<td>{x['mult']}x</td><td>{x['alert']}</td><td>{'' if x.get('last') is None else x['last']}</td><td>{'' if x.get('best') is None else x['best']}</td></tr>" for x in reversed(rows))
            out += ('<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Resistance zone</th><th>Upper wick</th>'
                    '<th>Best spurt</th><th>Alert</th><th>Move now % (fall = +)</th><th>Best %</th></tr>' + body + '</table></div>')
        else:
            out += '<div class="m">None yet today.</div>'
        for os_, ohead, onote in (("ORBUP", "Opening range breakout UP", "CLOSED above the high"), ("ORBDN", "Opening range breakdown DOWN", "CLOSED below the low")):
            orows = [x for x in _room["hits"] if x["side"] == os_]
            out += (f'<h2>{ohead} &mdash; 15-min candle {onote} of the first 15 minutes &middot; spurt &ge; {ALERT_MULT:g}x ({len(orows)})</h2>'
                    f'<div class="m">Opening range = the high and low of the 09:15-09:30 candle. A closed 15-min candle of a spurt stock {onote} of that range (the opening candle itself does not count). '
                    'First time per stock per day, phone alert for each. Move now = % move from the price at the alert in the breakout direction. Information only. '
                    '<a href="/spurt10_orb.csv">CSV with follow-up (all days)</a></div>')
            if orows:
                ob = "".join(f"<tr><td>{x['candle']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td>{x['broken']}</td><td>{x['room']:+.2f}%</td>"
                             f"<td>{x['mult']}x</td><td>{x['alert']}</td><td>{'' if x.get('last') is None else x['last']}</td><td>{'' if x.get('best') is None else x['best']}</td></tr>" for x in reversed(orows))
                out += ('<div class="w"><table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Opening range</th><th>Close vs range edge</th>'
                        '<th>Spurt</th><th>Alert</th><th>Move now %</th><th>Best %</th></tr>' + ob + '</table></div>')
            else:
                out += '<div class="m">None yet today.</div>'
        orows = [x for x in _room["hits"] if x["side"] == "ORBIN"]
        out += (f'<h2>Spurt stocks INSIDE the opening range &mdash; until {ORB_INSIDE_UNTIL // 60}:00 &middot; spurt &ge; {ALERT_MULT:g}x ({len(orows)})</h2>'
                '<div class="m">A spurt stock whose price is still between the high and low of the 09:15-09:30 candle. Listed the first time it is seen inside the range after 09:30, until 13:00, with a phone alert. '
                'Position in range: 0% = at the range low, 100% = at the range high. Information only. <a href="/spurt10_orb.csv">CSV with follow-up (all days)</a></div>')
        if orows:
            ob = "".join(f"<tr><td>{x['alert']}</td><td>{_tvl(x['symbol'])}</td><td>{x['price']:.2f}</td><td>{x['broken']}</td><td>{x['room']:g}%</td><td>{x['mult']}x</td></tr>" for x in reversed(orows))
            out += '<div class="w"><table class="sp"><tr><th>Alert</th><th>Symbol</th><th>Price</th><th>Opening range</th><th>Position in range</th><th>Spurt</th></tr>' + ob + '</table></div>'
        else:
            out += '<div class="m">None yet today.</div>'
        return out
    except Exception:
        return ""


# ---------------------------------------------------------------- stocks with news (NSE corporate announcements) are left out
NEWS_FILTER_ON = True        # ADDED: spurt stocks with an important NSE corporate announcement since the previous close are taken out of the list / alerts / /depth and shown in a list at the bottom. False = off
NEWS_LEAVE_OUT = False       # CHANGED 6 Nov: news stocks stay IN the normal list / alerts / sell scans (tagged NEWS beside the symbol) instead of being removed; the news list + news phone alert keep working as before. True = (old behaviour) take them out
NEWS_REFRESH_SEC = 600       # the announcements are read again after this many seconds (one request per refresh)
NEWS_KEEP_WORDS = ("result", "outcome of board", "board meeting", "acquisition", "acquire", "order", "contract", "award", "bagging", "fund raising", "fundraising",
                   "preferential", "qip", "rights issue", "bonus", "split", "dividend", "buy back", "buyback", "merger", "demerger", "amalgamation",
                   "scheme of arrangement", "rating", "press release", "joint venture", "agreement", "usfda", "patent", "capacity", "expansion",
                   "default", "fraud", "raid", "penalty", "insolvency", "takeover", "open offer", "delisting", "stake")
NEWS_LIST_MIN_MULT = 5.0     # the "left out because of news" list at the bottom shows only stocks with a volume spurt of at least this multiple (they are all removed from the table either way)
NEWS_SKIP_WORDS = ("monitoring agency", "trading window", "loss of share", "duplicate share", "certificate under regulation 74", "shareholding pattern", "investor complaints",
                   "newspaper", "voting results", "scrutinizer", "record date for", "analysts", "institutional investor", "change in registrar")
_news = {"at": 0.0, "syms": {}, "err": "", "ok_at": "", "n_all": 0}


def _news_cutoff(now_ist):
    """Previous trading day 15:30 IST (yesterday after the close; Monday looks back to Friday)."""
    d = now_ist - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.replace(hour=15, minute=30, second=0, microsecond=0)


def _news_fetch(now_ist):
    """{symbol: title} of important announcements since the previous close. Raises on any problem (the caller keeps the old result)."""
    import requests
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                         "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"})
    sess.get("https://www.nseindia.com/", timeout=12)                               # cookies
    cut = _news_cutoff(now_ist)
    fd, td = cut.strftime("%d-%m-%Y"), now_ist.strftime("%d-%m-%Y")
    resp = sess.get(f"https://www.nseindia.com/api/corporate-announcements?index=equities&from_date={fd}&to_date={td}", timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"NSE answered {resp.status_code}")
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("NSE answer was not a list")
    out = {}
    for a in data:
        try:
            sym = str(a.get("symbol") or "").strip().upper()
            if not sym:
                continue
            txt = f"{a.get('desc') or ''} {a.get('attchmntText') or ''}".lower()
            if any(w in txt for w in NEWS_SKIP_WORDS) or not any(w in txt for w in NEWS_KEEP_WORDS):
                continue
            dt = datetime.strptime(str(a.get("an_dt") or "")[:20].strip(), "%d-%b-%Y %H:%M:%S").replace(tzinfo=IST) if a.get("an_dt") else None
            if dt is not None and dt < cut:
                continue
            title = str(a.get("desc") or a.get("attchmntText") or "announcement").strip()[:90]
            if sym not in out:
                out[sym] = title
        except Exception:
            continue
    _news["n_all"] = len(data)
    return out


def _news_get(now_ist):
    """The {symbol: title} set to leave out. Refreshed every NEWS_REFRESH_SEC; if NSE cannot be read the last good set is kept (or nothing is left out). Never raises."""
    if not NEWS_FILTER_ON:
        return {}
    try:
        if time.time() - _news["at"] >= NEWS_REFRESH_SEC:
            _news["at"] = time.time()
            try:
                _news["syms"] = _news_fetch(now_ist)
                _news["err"] = ""
                _news["ok_at"] = now_ist.strftime("%H:%M")
                log.info(f"Spurt10 news: {len(_news['syms'])} stocks with an important announcement ({_news['n_all']} announcements read)")
            except Exception as e:
                _news["err"] = str(e)[:120]
                log.warning(f"Spurt10 news feed failed (nothing is left out for news): {e}")
        return dict(_news["syms"])
    except Exception:
        return {}


NEWS_DAY_JSON = os.path.join(RESULTS_DIR, "news_day.json")      # ADDED: every stock with news and a spurt of NEWS_LIST_MIN_MULT or more, kept for the whole day
NEWS_DAY_CSV = os.path.join(RESULTS_DIR, "news_day.csv")
_news_day = {"date": None, "rows": {}}


_NEWS_DAY_COLS = ["date", "first_spurt_candle", "symbol", "best_spurt_x", "last_price", "announcement",
                  "first_seen", "first_price", "spurt_dir", "spurt_candle_chg_pct", "day_chg_at_first",
                  "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "result",
                  "nifty_at_first", "nifty_10m", "nifty_30m", "nifty_60m", "nifty_last", "rel_last", "nifty_day_pct"]   # the first six are the old columns, same order; the last six = NIFTY baseline


def _nifty_px():
    """ADDED: (NIFTY last price, day % change) from the indices strip (it asks Dhan every 20 s in this same app - no extra call), or (None, None) when it is not fresh. Never raises."""
    try:
        import indices_strip as _ix
        with _ix._lock:
            rows, asof = list(_ix._data.get("rows") or []), str(_ix._data.get("asof") or "")
        t = datetime.strptime(asof, "%d-%b %H:%M:%S").replace(year=datetime.now(IST).year, tzinfo=IST)
        if abs((datetime.now(IST) - t).total_seconds()) > 180:
            return None, None
        for r in rows:
            if r.get("name") == "NIFTY" and r.get("price"):
                return float(r["price"]), r.get("pct")
    except Exception:
        pass
    return None, None


def _news_follow(rows, quotes, now_ist):
    """ADDED: whole-day follow-up of every stock in the news day list. Price = live quote of the stock (the quotes of the whole market are read every cycle anyway),
    so a stock keeps being followed after its spurt has ended. Moves are in % from the price when it was first seen (+ = price went up). Read-only. Never raises."""
    try:
        hm = now_ist.strftime("%H:%M")
        now_m = now_ist.hour * 60 + now_ist.minute
        nf, _np = _nifty_px()
        for r in rows.values():
            if not r.get("p0"):
                r["p0"] = r.get("price")
            if not r.get("seen"):
                r["seen"] = r.get("first") or hm
            q = (quotes or {}).get(r["symbol"])
            p = float(q[0]) if q and q[0] else None
            if p:
                r["price"] = p
            p = r.get("price")
            a = _pat_mins(r["seen"])
            if not p or not r.get("p0") or a is None:
                continue
            mv = round((float(p) / float(r["p0"]) - 1.0) * 100.0, 2)
            r["best_pct"] = mv if r.get("best_pct") is None else max(r["best_pct"], mv)
            r["worst_pct"] = mv if r.get("worst_pct") is None else min(r["worst_pct"], mv)
            r["last_pct"], r["last_time"] = mv, hm
            for lab, mm in (("r10", 10), ("r30", 30), ("r60", 60)):
                if r.get(lab) is None and now_m - a >= mm:
                    r[lab] = mv
            if nf and r.get("n0"):                                   # NIFTY baseline: its % move from the same moment
                nm = round((nf / r["n0"] - 1.0) * 100.0, 2)
                r["nifty_last"] = nm
                for lab, mm in (("n10", 10), ("n30", 30), ("n60", 60)):
                    if r.get(lab) is None and now_m - a >= mm:
                        r[lab] = nm
    except Exception as e:
        log.warning(f"Spurt10 news follow-up failed: {e}")


def _news_row(day, r, now_m):
    """One csv row of the news day list. result = did the price keep going the way the spurt candle went (CONTINUED), come back (FADED) or stay (FLAT);
    'open' until 60 min after the stock was first seen, or until 15:25 (end of the day) whichever is first."""
    g = lambda k: "" if r.get(k) is None else r[k]
    up = (r.get("chg0") is None) or (r["chg0"] >= 0)
    last = r.get("last_pct")
    rel = None if (last is None or r.get("nifty_last") is None) else round(last - r["nifty_last"], 2)      # move of the stock minus the move of NIFTY (market-adjusted)
    if rel is not None:
        last = rel
    done = r.get("r60") is not None or now_m >= 15 * 60 + 25
    res = "open"
    if last is not None and done:
        m = last if up else -last
        res = "CONTINUED" if m >= 0.3 else "FADED" if m <= -0.3 else "FLAT"
    return [day, r["first"], r["symbol"], r["best"], r["price"], r["title"],
            g("seen"), g("p0"), "UP" if up else "DOWN", g("chg0"), g("dchg0"),
            g("r10"), g("r30"), g("r60"), g("best_pct"), g("worst_pct"), g("last_pct"), g("last_time"), res,
            g("n0"), g("n10"), g("n30"), g("n60"), g("nifty_last"), "" if rel is None else rel, g("nd0")]


def _news_day_update(items, now_ist, quotes=None):
    """Adds this cycle's news stocks (spurt >= NEWS_LIST_MIN_MULT) to the day list (first spurt time stays, best multiple / last price update), saves it to disk
    (json for a restart, csv for download; older days stay in the csv) and returns the day list, biggest best-spurt first. Never raises.
    ADDED: every stock in the list is also followed for the whole day with the live quotes (see _news_follow) and the csv carries the follow-up columns."""
    today = now_ist.strftime("%Y-%m-%d")
    try:
        if _news_day["date"] != today:
            _news_day.update(date=today, rows={})
            try:
                with open(NEWS_DAY_JSON) as fh:
                    d = json.load(fh)
                if d.get("date") == today:
                    _news_day["rows"] = dict(d.get("rows") or {})
            except Exception:
                pass
        rows = _news_day["rows"]
        for it in items:
            cur = rows.get(it["symbol"])
            if cur is None:
                rows[it["symbol"]] = {"symbol": it["symbol"], "first": it["candle"], "best": it["multiple"], "price": it["price"], "title": it["title"],
                                      "seen": now_ist.strftime("%H:%M"), "p0": it["price"], "chg0": it.get("candle_chg_pct"), "dchg0": it.get("day_chg_pct"),
                                      "n0": _nifty_px()[0], "nd0": _nifty_px()[1]}
            else:
                cur["best"] = max(cur["best"], it["multiple"])
                cur["price"] = it["price"]
                cur["title"] = it["title"]
        _news_follow(rows, quotes, now_ist)
        if rows:
            os.makedirs(RESULTS_DIR, exist_ok=True)
            with open(NEWS_DAY_JSON + ".tmp", "w") as fh:
                json.dump({"date": today, "rows": rows}, fh)
            os.replace(NEWS_DAY_JSON + ".tmp", NEWS_DAY_JSON)
            _nm = now_ist.hour * 60 + now_ist.minute
            new = pd.DataFrame([_news_row(today, r, _nm) for r in rows.values()], columns=_NEWS_DAY_COLS)
            if os.path.exists(NEWS_DAY_CSV):
                try:
                    old = pd.read_csv(NEWS_DAY_CSV)
                    new = pd.concat([old[old["date"].astype(str) != today], new], ignore_index=True)
                except Exception:
                    pass
            new.to_csv(NEWS_DAY_CSV + ".tmp", index=False)
            os.replace(NEWS_DAY_CSV + ".tmp", NEWS_DAY_CSV)
        return sorted(rows.values(), key=lambda t: -t["best"])[:100]
    except Exception as e:
        log.warning(f"Spurt10 news day list failed: {e}")
        return sorted(items, key=lambda t: -t["multiple"])


# ---------------------------------------------------------------- ADDED: phone alert for stocks that spurt AND have important news
NEWS_ALERT_ON = True         # one phone alert per stock per day, the first time a stock with an important NSE announcement reaches NEWS_LIST_MIN_MULT x volume. False = off
NEWS_ALERT_MAX = 40          # safety cap on these notifications per day
NEWS_ALERT_STATE = os.path.join(RESULTS_DIR, "news_alert_state.json")   # on the disk: a restart / update does not repeat the alerts
_news_alerted = {"date": None, "syms": [], "count": 0}


def _news_alerts(news_list, now_ist):
    """Tells you which spurt stocks are in the news (these are taken out of the normal list and alerts, so without this you would not hear about them).
    Read-only. Never raises."""
    try:
        if not NEWS_ALERT_ON or not news_list:
            return
        today = now_ist.strftime("%Y-%m-%d")
        if _news_alerted["date"] != today:
            _news_alerted.update(date=today, syms=[], count=0)
            try:
                with open(NEWS_ALERT_STATE) as fh:
                    d = json.load(fh)
                if d.get("date") == today:
                    _news_alerted.update(syms=list(d.get("syms") or []), count=int(d.get("count") or 0))
            except Exception:
                pass
        if now_ist.hour * 60 + now_ist.minute > 15 * 60 + 30:      # the news alert runs the whole market day
            return
        done = set(_news_alerted["syms"])
        new = [r for r in news_list if r["symbol"] not in done]
        if not new:
            return
        for r in new:
            _news_alerted["syms"].append(r["symbol"])
        topic = os.environ.get("NTFY_TOPIC", "").strip()
        sent = bool(NTFY_ON and topic and _news_alerted["count"] < NEWS_ALERT_MAX)
        if sent:
            new.sort(key=lambda r: -r["best"])
            lines = [f"{r['symbol']}  Rs {r['price']:g}  spurt {r['best']:.1f}x (first {r['first']})  NEWS: {str(r['title'])[:110]}" for r in new[:12]]
            extra = f"\n+{len(new) - 12} more on the spurt10_news page" if len(new) > 12 else ""
            try:
                import requests
                resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                                     headers={"Title": f"Spurt stock WITH NEWS ({len(new)})", "Priority": "default", "Tags": "newspaper"})
                if resp.status_code >= 300:
                    log.warning(f"Spurt10 news alert refused: {resp.status_code} {resp.text[:150]}")
                    for r in new:
                        _news_alerted["syms"].remove(r["symbol"])
                    return
                _news_alerted["count"] += 1
            except Exception as ex:
                log.warning(f"Spurt10 news alert failed: {ex}")
                for r in new:
                    _news_alerted["syms"].remove(r["symbol"])
                return
        log.info(f"Spurt10 news alert: {[r['symbol'] for r in new]}")
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(NEWS_ALERT_STATE + ".tmp", "w") as fh:
            json.dump(_news_alerted, fh)
        os.replace(NEWS_ALERT_STATE + ".tmp", NEWS_ALERT_STATE)
    except Exception as e:
        log.warning(f"Spurt10 news alert step failed: {e}")


def news_symbols():
    """{symbol: announcement title} of the stocks with important news (for the other alert modules). {} when the filter is off or the feed is not readable. Never raises."""
    try:
        return _news_get(datetime.now(IST)) if (NEWS_FILTER_ON and NEWS_LEAVE_OUT) else {}
    except Exception:
        return {}


def _news_purge_tracker(nsyms):
    """Take stocks with news out of the Spurt SELL tracker table (spurt_track.json keeps every stock that spurted today). Never raises."""
    try:
        if not nsyms:
            return
        import spurt_sell_live
        path = spurt_sell_live.TRACK_PATH
        with open(path) as fh:
            data = json.load(fh)
        rows = data.get("rows") or {}
        gone = [k for k in rows if k in nsyms]
        if gone:
            for k in gone:
                rows.pop(k, None)
            with open(path + ".tmp", "w") as fh:
                json.dump(data, fh)
            os.replace(path + ".tmp", path)
            log.info(f"Spurt10 news: {len(gone)} stock(s) taken out of the Spurt SELL tracker")
    except Exception:
        pass


def _news_html(meta):
    """List of the spurt stocks left out because of news, plus the status of the news feed. Never raises."""
    try:
        if not NEWS_FILTER_ON:
            return ""
        nl = meta.get("news_list") or []
        status = meta.get("news_status") or ""
        head = (f'<h3 style="margin:22px 0 4px">Spurt stocks with news today &mdash; {len(nl)}{" (left out of the list above)" if NEWS_LEAVE_OUT else " (kept in the list above, tagged NEWS)"}</h3>'
                f'<div class="m">Important NSE corporate announcement since the previous close (results, board meeting, orders, deals, fund raising, dividend, rating ...). Whole day: every stock that reached a volume spurt of {NEWS_LIST_MIN_MULT:g}x or more, kept until the day ends &middot; <a href="/news_day.csv">CSV</a>. {status}</div>')
        if not nl:
            return head + '<div class="m">None right now.</div>'
        rows = "".join(f"<tr><td>{i}</td><td>{_tvl(t['symbol'])}</td><td>{t['price']:.2f}</td><td>{t.get('first', t.get('candle', ''))}</td><td>{t.get('best', t.get('multiple', 0)):.2f}x</td><td style='text-align:left'>{t['title']}</td></tr>"
                       for i, t in enumerate(nl, 1))
        return (head + '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Last price</th><th>First spurt (candle)</th><th>Best spurt</th><th>Announcement</th></tr>' + rows + '</table></div>')
    except Exception:
        return ""


def run_once():
    _candle_reminder(datetime.now(IST), SPURT_TF, f"Stock {SPURT_TF}-min")
    t0 = time.monotonic()
    now_ist = datetime.now(IST)
    uni = _load_universe()
    quotes = _fetch_quotes(uni)
    _early_snap(quotes, now_ist)                                    # ADDED 5 Nov: feeds the early volume spurt check
    names, n_pass = _shortlist(quotes, now_ist)
    sid_of = dict(uni)
    rows, failed = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_score_symbol, s, sid_of[s]) for s in names]
        for f in futs:
            try:
                r = f.result()
                if r:
                    rows.append(r)
            except Exception as e:
                failed += 1
                if failed <= 3:
                    log.warning(f"Spurt10 candle fetch failed: {e}")
    try:
        _early_note_bases(rows)                                     # ADDED 5 Nov: average 10-min volume of the stocks just scored (used by the early volume spurt check)
    except Exception:
        pass
    thin_removed = 0
    thin_list = []
    if LIQ_REMOVE_THIN and _depth:                                  # ADDED: leave out thin stocks (wide bid-ask spread); a stock with no depth data stays
        for _r in rows:
            _r.update(_liq_fields(_r["symbol"]))
        thin_removed = sum(1 for _r in rows if _r.get("liq") == "THIN")
        thin_list = sorted(({"symbol": _r["symbol"], "price": _r["price"], "candle": _r["candle"], "multiple": _r["multiple"], "spread_pct": _r["spread_pct"],
                            "bid_qty": (_depth.get(_r["symbol"]) or (0, 0, 0, None, None))[3], "ask_qty": (_depth.get(_r["symbol"]) or (0, 0, 0, None, None))[4],
                            "why": (("Near " + _circ[_r["symbol"]].lower() + " circuit") if _r["symbol"] in _circ else
                                    "Low buy / sell qty" if (_depth.get(_r["symbol"]) and len(_depth[_r["symbol"]]) > 4 and min(_depth[_r["symbol"]][3], _depth[_r["symbol"]][4]) < LIQ_MIN_DEPTH_QTY) else "Wide spread")}
                            for _r in rows if _r.get("liq") == "THIN" and _r["multiple"] >= THIN_LIST_MIN_MULT), key=lambda t: -t["multiple"])[:THIN_LIST_MAX]
        rows = [_r for _r in rows if _r.get("liq") != "THIN"]
        if thin_removed:
            log.info(f"Spurt10: {thin_removed} thin stock(s) left out (spread above {LIQ_SPREAD_THIN}% or bid / ask quantity below {LIQ_MIN_DEPTH_QTY:,})")
    news_list, news_status = [], ""
    if NEWS_FILTER_ON:                                              # ADDED: leave out stocks with an important announcement (fails open: nothing is left out when the news feed cannot be read)
        _nsyms = _news_get(now_ist)
        if _news["err"]:
            news_status = (f'<span style="color:#c62828">News feed problem: {_news["err"]} &mdash; ' + ("using the last good read." if _nsyms else "nothing is being left out for news.") + '</span>')
        elif _news["ok_at"]:
            news_status = f'News feed OK (read {_news["ok_at"]} IST, {len(_nsyms)} stocks with news).'
        if _nsyms:
            news_list = _news_day_update([{"symbol": _r["symbol"], "price": _r["price"], "candle": _r["candle"], "multiple": _r["multiple"], "title": _nsyms[_r["symbol"]],
                                           "candle_chg_pct": _r.get("candle_chg_pct"), "day_chg_pct": _r.get("day_chg_pct")}
                                          for _r in rows if _r["symbol"] in _nsyms and _r["multiple"] >= NEWS_LIST_MIN_MULT], now_ist, quotes)
            _news_alerts(news_list, now_ist)
            if NEWS_LEAVE_OUT:
                rows = [_r for _r in rows if _r["symbol"] not in _nsyms]
            try:                                                    # also stop following stocks that spurted earlier today and now have news (zone / wick alerts keep checking those)
                for _d in ((_watch.get("syms"), _wick.get("syms")) if NEWS_LEAVE_OUT else ()):
                    for _k in [k for k in (_d or {}) if k in _nsyms]:
                        _d.pop(_k, None)
            except Exception:
                pass
    try:                                                            # ADDED 8 Nov: RSI Flush on the stocks that spurted today (page /spurt_flush)
        import spurt_flush
        spurt_flush.update(rows, now_ist)
    except Exception as _e:
        log.warning(f"spurt_flush failed: {_e}")
    rows.sort(key=lambda r: -r["multiple"])
    top = rows[:TOP_N]
    top_ids = {id(x) for x in top}
    sell_hits = []
    trk_items = []
    for r in rows:                                                  # ADDED: Section A support / resistance zones of each stock
        _lq = _liq_fields(r["symbol"])
        r.update(_lq)
        tk = r.pop("_trk", None)
        if tk:
            tk.update(_lq)
            trk_items.append(tk)
        sg = r.pop("_sell", None)
        if sg:
            sg.update(_lq)
            sell_hits.append(sg)
        zs = r.pop("_zones", None)
        ztf = r.pop("_zone_tf", 10)
        d5, asof = r.pop("_df5", None), r.pop("_as_of", None)
        if id(r) in top_ids:
            r.update(_zone_fields(zs, r["price"], r.get("candle_chg_pct")))
            r.update(_conf_fields(zs, _conf_zones(d5, asof), r["price"]))     # ADDED: 15m + 30m confluence
            try:                                                           # ADDED: last two CLOSED 15-min candles (SELL at 15m+30m zone alert)
                d15 = ls.drop_forming_bars(ls.build_merged_bars(d5, 3), 15, asof)
                try:                                                       # ADDED: opening range = today's first 15-min candle (09:15), only once it has closed
                    _td = now_ist.date()
                    _ix = [_ist(t) for t in d15.index] if d15 is not None else []
                    _k = [i for i, t in enumerate(_ix) if t.date() == _td]
                    if _k and _ix[_k[0]].strftime("%H:%M") == "09:15":
                        _f = d15.iloc[_k[0]]
                        r["orb"] = (float(_f["high"]), float(_f["low"]))
                except Exception:
                    pass
                if d15 is not None and len(d15) >= 2:
                    a_, b_ = d15.iloc[-1], d15.iloc[-2]
                    r["c15"] = {"t": _ist(d15.index[-1]).strftime("%H:%M"), "o": float(a_["open"]), "h": float(a_["high"]), "l": float(a_["low"]), "c": float(a_["close"]),
                                "v": float(a_["volume"]), "pv": float(b_["volume"]),
                                "po": float(b_["open"]), "ph": float(b_["high"]), "pl": float(b_["low"]), "pc": float(b_["close"])}
            except Exception:
                pass
            if r.get("zone_src"):
                r["zone_src"] = f"Section A {ztf}m"
    try:
        _send_alerts(rows, now_ist)
    except Exception as e:
        log.warning(f"Spurt10 alert step failed: {e}")
    try:
        _dsnap_meta["run"] = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
        _dsnap_meta["spurt"] = {r["symbol"]: r["multiple"] for r in top}
        _dsnap_meta["zone"] = {r["symbol"]: {"at": r.get("at_zone") or "", "sup": r.get("support") or "", "res": r.get("resistance") or ""} for r in top}
        _save_depth_snapshot(_dsnap_meta["run"], _dsnap_meta["spurt"], _dsnap_meta["zone"])
    except Exception:
        pass
    try:
        _send_offers_alerts(top, now_ist)
    except Exception as e:
        log.warning(f"Spurt10 offers alert step failed: {e}")
    watch_view = []
    try:                                                            # ADDED: phone alert when a top-30 spurt stock is at a support / resistance zone
        try:
            wrows = _watch_rows(top, now_ist, sid_of)
        except Exception as e:
            log.warning(f"Spurt10 watch step failed: {e}")
            wrows = []
        _send_zone_alerts(top + wrows, now_ist)
        watch_view = _watch_view()
    except Exception as e:
        log.warning(f"Spurt10 zone alert step failed: {e}")
    try:
        _wick_alerts(top, now_ist, sid_of)
    except Exception as e:
        log.warning(f"Spurt10 wick step failed: {e}")
    _pat_roll(now_ist)
    try:
        _clean_alerts(now_ist)
    except Exception as e:
        log.warning(f"Spurt10 clean-rejection step failed: {e}")
    try:
        _engulf_alerts(now_ist)
    except Exception as e:
        log.warning(f"Spurt10 engulfing step failed: {e}")
    _pat_follow(now_ist)
    _room_update(top, quotes, now_ist)
    _state_save()
    try:                                                            # ADDED: link Section A OB_BUY / OB_SELL signals with spurt stocks (spurt_link.py)
        import spurt_link
        spurt_link.note_spurts(top, now_ist)
        spurt_link.process(now_ist)
    except Exception as e:
        log.warning(f"Spurt link step failed: {e}")
    try:                                                            # ADDED: spurt -> red candle SELL signals (own page, own ntfy topic)
        import spurt_sell_live
        _news_purge_tracker(_news.get("syms") if (NEWS_FILTER_ON and NEWS_LEAVE_OUT) else None)
        spurt_sell_live.update_tracker(trk_items, now_ist)
        spurt_sell_live.process(sell_hits, now_ist)
    except Exception as e:
        log.warning(f"Spurt SELL step failed: {e}")
    try:                                                            # ADDED: move the open paper trades forward (spurt_paper.py)
        import spurt_paper
        spurt_paper.update(now_ist, _paper_fetch5)
    except Exception as e:
        log.warning(f"Spurt paper step failed: {e}")
    meta = {
        "run": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"), "took_s": round(time.monotonic() - t0),
        "universe": len(uni), "quoted": len(quotes), "price_ok": n_pass, "shortlisted": len(names),
        "scored": len(rows), "failed": failed, "etf_excluded": len(_excluded), "thin_removed": thin_removed, "thin_list": thin_list, "news_list": news_list, "news_status": news_status, "min_price": MIN_PRICE, "sma_len": SMA_LEN, "alert_mult": ALERT_MULT,
        "watch": watch_view, "watch_until": f"{WATCH_UNTIL_MIN // 60:02d}:{WATCH_UNTIL_MIN % 60:02d}", "watch_mult": WATCH_MULT,
    }
    _write(top, meta)
    _append_history(top, now_ist)      # top 30 of the cycle only
    try:                                                            # ADDED: every spurt stock, every candle (price + zones) and what happened after each zone touch
        _track_update(top, now_ist, sid_of)
    except Exception as e:
        log.warning(f"Spurt10 track step failed: {e}")
    log.info(f"Spurt10 done: {meta}")
    return top, meta


ZONE_NEAR_PCT = float(os.environ.get("SPURT10_ZONE_NEAR_PCT", "0.5") or "0.5")      # "at" a zone = inside it or within this % of it
CONF_TF = 30                    # the second, higher time frame (minutes) whose zones are compared with the main zone time frame
CONF_PCT = 0.3                  # two zones count as "the same zone" when they overlap or are within this % of the price of each other
ZONE_AGAINST_PCT = 1.0                                                                # opposite zone this close ahead = AGAINST


def _secA_zones(df):
    """Live order-block zones from Section A on this stock's 10-min candles: the same zones the ALL IN ONE PRO indicator draws
    (bullish OB = support, bearish OB = resistance). Runs on candles already downloaded - no extra Dhan call."""
    try:                                   # FIXED: zones now come from spurt_zones.py (the old with_zones option no longer exists in all_in_one_scanner.py)
        import spurt_zones
        import math
        return [z for z in (spurt_zones.live_zones(df) or [])
                if all(isinstance(z.get(k), (int, float)) and math.isfinite(z[k]) for k in ("lo", "hi"))]      # a zone with a missing edge (nan) is dropped
    except Exception as e:
        log.warning(f"Spurt10 zone calculation failed: {e}")
        return []


def _conf_zones(df5, as_of):
    """Zones on the CONF_TF-minute candles (built from the 5-min candles already downloaded). [] when not available."""
    try:
        if CONF_TF == ZONE_TF or df5 is None:
            return []
        d = ls.drop_forming_bars(ls.build_merged_bars(df5, CONF_TF // 5), CONF_TF, as_of)
        if d is None or len(d) < 30:
            return []
        return _secA_zones(d)
    except Exception as e:
        log.warning(f"Spurt10 {CONF_TF}m zone candles failed: {e}")
        return []


def _conf_fields(zs, zs2, price):
    """CONFLUENCE: the nearest support / resistance of the main zone time frame (zs) is the same zone as a zone of the
    CONF_TF time frame (zs2) when they overlap or are within CONF_PCT % of the price. Returns the CONF_TF zone range (else "")."""
    out = {"conf_support": "", "conf_resistance": ""}
    try:
        if not zs or not zs2 or not price:
            return out
        gap = CONF_PCT / 100.0 * price
        for kind, key in (("SUPPORT", "conf_support"), ("RESISTANCE", "conf_resistance")):
            a = [z for z in zs if z["type"] == kind]
            b = [z for z in zs2 if z["type"] == kind]
            if not a or not b:
                continue
            z1 = max(a, key=lambda z: z["hi"]) if kind == "SUPPORT" else min(a, key=lambda z: z["lo"])
            best = None
            for z2 in b:
                d = max(z2["lo"] - z1["hi"], z1["lo"] - z2["hi"], 0.0)          # 0 = overlapping
                if d <= gap and (best is None or d < best[0]):
                    best = (d, z2)
            if best:
                out[key] = f"{best[1]['lo']:g}-{best[1]['hi']:g}"
    except Exception:
        pass
    return out


def _is_conf(r):
    """True when the zone the price is AT right now is a confluence zone (15m + 30m)."""
    at = r.get("at_zone")
    return bool(at and r.get("conf_support" if at == "SUPPORT" else "conf_resistance"))


def _zone_fields(zs, price, candle_chg):
    """Nearest support below / resistance above the price and whether the spurt candle's direction agrees with it."""
    out = {"support": "", "support_dist_pct": None, "resistance": "", "resistance_dist_pct": None, "zone_view": "no zone", "zone_src": "", "at_zone": ""}
    try:
        if not zs or not price:
            return out
        out["zone_src"] = "Section A 10m"
        near, ag = ZONE_NEAR_PCT / 100.0, ZONE_AGAINST_PCT / 100.0
        sup = [z for z in zs if z["type"] == "SUPPORT"]
        res = [z for z in zs if z["type"] == "RESISTANCE"]
        zsup = max(sup, key=lambda z: z["hi"]) if sup else None          # nearest support (highest one)
        zres = min(res, key=lambda z: z["lo"]) if res else None          # nearest resistance (lowest one)
        if zsup:
            out["support"] = f"{zsup['lo']:g}-{zsup['hi']:g}"
            out["support_dist_pct"] = round(max(price - zsup["hi"], 0) / price * 100, 2)
        if zres:
            out["resistance"] = f"{zres['lo']:g}-{zres['hi']:g}"
            out["resistance_dist_pct"] = round(max(zres["lo"] - price, 0) / price * 100, 2)
        at_s = zsup is not None and zsup["lo"] * (1 - near) <= price <= zsup["hi"] * (1 + near)
        at_r = zres is not None and zres["lo"] * (1 - near) <= price <= zres["hi"] * (1 + near)
        out["zone_view"] = "between zones" if (zsup or zres) else "no zone"
        if at_s and at_r:                                                 # inside both (very narrow range): the nearer one
            at_s, at_r = (out["support_dist_pct"] <= out["resistance_dist_pct"]), (out["support_dist_pct"] > out["resistance_dist_pct"])
        out["at_zone"] = "SUPPORT" if at_s else "RESISTANCE" if at_r else ""
        if candle_chg is not None and abs(candle_chg) >= 0.05:
            up = candle_chg > 0
            if (up and at_s) or (not up and at_r):
                out["zone_view"] = "ALIGNED"                              # green at support / red at resistance
            elif (up and zres and (at_r or 0 <= (zres["lo"] - price) / price <= ag)) or \
                 (not up and zsup and (at_s or 0 <= (price - zsup["hi"]) / price <= ag)):
                out["zone_view"] = "AGAINST"                              # green into resistance / red onto support
            elif at_s:
                out["zone_view"] = "at support"
            elif at_r:
                out["zone_view"] = "at resistance"
        else:
            out["zone_view"] = "at support" if at_s else "at resistance" if at_r else out["zone_view"]
    except Exception:
        pass
    return out


_hist_seen = {"date": None, "keys": set()}
HIST_COLS = ["date", "run", "candle", "symbol", "price", "day_chg_pct", "candle_chg_pct", "candle_vol", "sma_vol", "multiple", "day_vol",
             "support", "support_dist_pct", "resistance", "resistance_dist_pct", "zone_view", "zone_src", "at_zone",
             "conf_support", "conf_resistance", "spread_pct", "liq"]      # spread_pct / liq = best bid-ask spread % and LIQUID / OK / THIN;  conf_* = the 30-min zone that coincides with the nearest 15-min zone ("" = none)


def _append_history(rows, now_ist):
    """ADDED: append the top-30 stocks of this cycle to results/spurt10_history.csv, one row per
    (date, 10-min candle, symbol). Read-only logging: never raises, never touches the scan itself."""
    if not HISTORY_ON or not rows:
        return
    try:
        today = now_ist.strftime("%Y-%m-%d")
        if _hist_seen["date"] != today:
            _hist_seen.update(date=today, keys=set())
            if os.path.exists(HIST_PATH):                      # restart in the middle of the day: do not write the same candle twice
                old = pd.read_csv(HIST_PATH, usecols=["date", "candle", "symbol"], dtype=str)
                for d, c, sy in old[old["date"] == today].itertuples(index=False):
                    _hist_seen["keys"].add((c, sy))
        run = now_ist.strftime("%H:%M:%S")
        out = []
        for r in rows:
            k = (r["candle"], r["symbol"])
            if k in _hist_seen["keys"] or r["multiple"] < HISTORY_MIN_MULT:
                continue
            _hist_seen["keys"].add(k)
            out.append({"date": today, "run": run, **{c: r.get(c) for c in HIST_COLS[2:]}})
        if not out:
            return
        os.makedirs(RESULTS_DIR, exist_ok=True)
        new_file = not os.path.exists(HIST_PATH)
        if not new_file:                                   # file written by an older version (fewer columns): upgrade it in place
            with open(HIST_PATH) as fh:
                first = fh.readline().strip().split(",")
            if first != HIST_COLS:
                pd.read_csv(HIST_PATH).reindex(columns=HIST_COLS).to_csv(HIST_PATH + ".tmp", index=False)
                os.replace(HIST_PATH + ".tmp", HIST_PATH)
        pd.DataFrame(out, columns=HIST_COLS).sort_values(["candle", "multiple"], ascending=[True, False]).to_csv(
            HIST_PATH, mode="a", header=new_file, index=False)
    except Exception as e:
        log.warning(f"Spurt10 history write failed: {e}")


# ------------------------------------------------------------------ ADDED: every-candle track + zone-touch follow-up
# Every stock that reached the top 30 today is followed for the rest of the day (even after it drops out of the table): one row per
# 10-min candle with price, candle and support / resistance zone  ->  results/spurt10_track.csv
# Each time a followed stock arrives AT a support / resistance zone, the price 10 / 30 / 60 minutes later and the best / worst move
# are recorded  ->  results/zone_touch_followup.csv   (answers: did it reverse or break through?)
TRACK_ON = os.environ.get("SPURT10_TRACK", "true").strip().lower() == "true"
TRACK_MAX = int(os.environ.get("SPURT10_TRACK_MAX", "150") or "150")          # most stocks followed in one day
TRACK_PATH = os.path.join(RESULTS_DIR, "spurt10_track.csv")
TOUCH_PATH = os.path.join(RESULTS_DIR, "zone_touch_followup.csv")
TRACK_STATE = os.path.join(RESULTS_DIR, "spurt10_track_state.json")
TRACK_COLS = ["date", "run", "candle", "symbol", "in_top30", "price", "day_chg_pct", "candle_chg_pct", "open", "high", "low", "close",
              "candle_vol", "sma_vol", "multiple", "day_vol", "support", "support_dist_pct", "resistance", "resistance_dist_pct",
              "zone_view", "zone_src", "at_zone", "conf_support", "conf_resistance"]
TOUCH_COLS = ["date", "symbol", "zone_type", "touch_candle", "touch_run", "touch_price", "zone", "multiple", "candle_chg_pct", "day_chg_pct",
              "ret_10m", "ret_30m", "ret_60m", "best_pct", "worst_pct", "last_pct", "last_time", "broke_zone", "outcome"]
_trk = {"date": None, "syms": [], "seen": [], "prev_at": {}, "events": []}


def _trk_load():
    try:
        with open(TRACK_STATE, "r", encoding="utf-8") as f:
            d = json.load(f)
        _trk.update(date=d.get("date"), syms=d.get("syms") or [], seen=d.get("seen") or [], prev_at=d.get("prev_at") or {}, events=d.get("events") or [])
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"Spurt10 track state load failed: {e}")


def _trk_save():
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = TRACK_STATE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_trk, f, default=str)
        os.replace(tmp, TRACK_STATE)
    except Exception as e:
        log.warning(f"Spurt10 track state save failed: {e}")


def _trk_row(sid_sym, sid, now_ist):
    """Fresh candle + zone data of one followed stock that is no longer in the top-30 table (None when it cannot be read)."""
    r = _score_symbol(sid_sym, sid)
    if not r:
        return None
    zs = r.pop("_zones", None)
    ztf = r.pop("_zone_tf", 10)
    d5, asof = r.pop("_df5", None), r.pop("_as_of", None)
    for k in ("_trk", "_sell"):
        r.pop(k, None)
    r.update(_zone_fields(zs, r["price"], r.get("candle_chg_pct")))
    r.update(_conf_fields(zs, _conf_zones(d5, asof), r["price"]))
    if r.get("zone_src"):
        r["zone_src"] = f"Section A {ztf}m"
    return r


def _trk_event_update(ev, price, now_ist):
    """Move one zone-touch event forward with the newest price of its stock."""
    p0 = ev["touch_price"]
    if not p0:
        return
    pct = round((price / p0 - 1) * 100, 2)
    t0 = datetime.strptime(ev["date"] + " " + ev["touch_run"], "%Y-%m-%d %H:%M:%S")
    mins = (now_ist.replace(tzinfo=None) - t0).total_seconds() / 60.0
    for key, lim in (("ret_10m", 9), ("ret_30m", 29), ("ret_60m", 59)):
        if ev.get(key) in (None, "") and mins >= lim:
            ev[key] = pct
    ev["best_pct"] = pct if ev.get("best_pct") in (None, "") else max(ev["best_pct"], pct)
    ev["worst_pct"] = pct if ev.get("worst_pct") in (None, "") else min(ev["worst_pct"], pct)
    ev["last_pct"], ev["last_time"] = pct, now_ist.strftime("%H:%M")
    try:
        lo, hi = [float(x) for x in str(ev["zone"]).split("-")]
        if ev["zone_type"] == "RESISTANCE" and price > hi * 1.002:
            ev["broke_zone"] = "YES"
        elif ev["zone_type"] == "SUPPORT" and price < lo * 0.998:
            ev["broke_zone"] = "YES"
    except Exception:
        pass
    res = ev.get("ret_60m")
    if ev.get("broke_zone") == "YES":
        ev["outcome"] = "BROKE THROUGH"
    elif res in (None, ""):
        ev["outcome"] = "open"
    else:
        away = -res if ev["zone_type"] == "RESISTANCE" else res          # move away from the zone = reversal
        ev["outcome"] = "REVERSED" if away >= 0.5 else "STALLED"


def _track_update(top, now_ist, sid_of):
    if not TRACK_ON:
        return
    today = now_ist.strftime("%Y-%m-%d")
    if _trk["date"] != today:
        _trk.update(date=today, syms=[], seen=[], prev_at={}, events=[])
    run = now_ist.strftime("%H:%M:%S")
    in_top = {r["symbol"] for r in top}
    for r in top:                                                   # every stock that reaches the top 30 is followed from now on
        if r["symbol"] not in _trk["syms"] and len(_trk["syms"]) < TRACK_MAX:
            _trk["syms"].append(r["symbol"])
    todo = [x for x in _trk["syms"] if x not in in_top and x in sid_of]
    rows = [(dict(r), 1) for r in top if r["symbol"] in _trk["syms"]]
    if todo:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {x: ex.submit(_trk_row, x, sid_of[x], now_ist) for x in todo}
            for x, fu in futs.items():
                try:
                    rr = fu.result()
                    if rr:
                        rows.append((rr, 0))
                except Exception as e:
                    log.warning(f"Spurt10 track fetch failed for {x}: {e}")
    seen = {tuple(k) for k in _trk["seen"]}
    out = []
    for r, intop in rows:
        k = (r["candle"], r["symbol"])
        if k in seen:
            continue
        seen.add(k)
        o = r.get("ohlc") or (None, None, None, None)
        row = {"date": today, "run": run, "in_top30": intop, "open": o[0], "high": o[1], "low": o[2], "close": o[3]}
        row.update({c: r.get(c) for c in TRACK_COLS if c not in row})
        out.append(row)
        # --- zone touch events
        sym, at, price = r["symbol"], r.get("at_zone") or "", r["price"]
        for ev in _trk["events"]:                                   # earlier touches of this stock: move them forward
            if ev["symbol"] == sym and ev["touch_run"] != run:
                _trk_event_update(ev, price, now_ist)
        if at in ("SUPPORT", "RESISTANCE") and _trk["prev_at"].get(sym, "") != at:
            ev = {"date": today, "symbol": sym, "zone_type": at, "touch_candle": r["candle"], "touch_run": run, "touch_price": price,
                  "zone": r.get("resistance") if at == "RESISTANCE" else r.get("support"), "multiple": r.get("multiple"),
                  "candle_chg_pct": r.get("candle_chg_pct"), "day_chg_pct": r.get("day_chg_pct"), "broke_zone": "", "outcome": "open"}
            _trk["events"].append(ev)
        _trk["prev_at"][sym] = at
    _trk["seen"] = [list(k) for k in seen]
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        if out:
            new_file = not os.path.exists(TRACK_PATH)
            if not new_file:
                with open(TRACK_PATH) as fh:
                    first = fh.readline().strip().split(",")
                if first != TRACK_COLS:
                    pd.read_csv(TRACK_PATH).reindex(columns=TRACK_COLS).to_csv(TRACK_PATH + ".tmp", index=False)
                    os.replace(TRACK_PATH + ".tmp", TRACK_PATH)
            pd.DataFrame(out, columns=TRACK_COLS).sort_values(["candle", "multiple"], ascending=[True, False]).to_csv(
                TRACK_PATH, mode="a", header=new_file, index=False)
        # touch file = earlier days kept as they are + today's events rewritten
        old = pd.DataFrame(columns=TOUCH_COLS)
        if os.path.exists(TOUCH_PATH):
            old = pd.read_csv(TOUCH_PATH, dtype=str)
            old = old[old["date"] != today]
        new = pd.DataFrame(_trk["events"]).reindex(columns=TOUCH_COLS)
        pd.concat([old.reindex(columns=TOUCH_COLS), new], ignore_index=True).to_csv(TOUCH_PATH + ".tmp", index=False)
        os.replace(TOUCH_PATH + ".tmp", TOUCH_PATH)
    except Exception as e:
        log.warning(f"Spurt10 track write failed: {e}")
    _trk_save()
    log.info(f"Spurt10 track: {len(_trk['syms'])} stock(s) followed, {len(out)} new row(s), {len(_trk['events'])} zone touch(es) today")


_trk_load()


def _write(top, meta):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    pd.DataFrame(top).to_csv(CSV_PATH + ".tmp", index=False)
    os.replace(CSV_PATH + ".tmp", CSV_PATH)
    with open(JSON_PATH, "w") as f:
        json.dump({"meta": meta, "rows": top}, f)
    html = _html(top, meta)
    with open(HTML_PATH + ".tmp", "w") as f:
        f.write(html)
    os.replace(HTML_PATH + ".tmp", HTML_PATH)


ROW_BG = {"SUPPORT": "#d4f1dc", "RESISTANCE": "#fadad6"}          # row colour: price is AT a support zone (green) / resistance zone (red)
LEGEND = ('<span style="background:#d4f1dc;padding:2px 8px;border:1px solid #9ccfae">green row = price AT a support zone</span> '
          '<span style="background:#fadad6;padding:2px 8px;border:1px solid #e0a39c">red row = price AT a resistance zone</span> '
          '<span style="background:#ffe066;padding:2px 8px;border:1px solid #d9b800">yellow multiple = phone-alert strength</span> '
          '<span style="background:#6fd68a;padding:2px 8px;border:1px solid #3ea85c">green cell = price at support zone</span> '
          '<span style="background:#f08d84;padding:2px 8px;border:1px solid #c9564c">red cell = price at resistance zone</span> '
          '<span style="background:#f08d84;padding:2px 8px;border:1px solid #c9564c">Offers % red = selling pressure above the limit (sell qty share of the depth window)</span>')


def _rowstyle(at):
    return f' style="background:{ROW_BG[at]}"' if at in ROW_BG else ""


def _zc(r, k):
    z = r.get(k) or ""
    d = r.get(k + "_dist_pct")
    if not z:
        return "-"
    cf = r.get("conf_" + k)
    tag = f"<br><small style='color:#6a1b9a'><b>{ZONE_TF}m+{CONF_TF}m zone</b> ({cf})</small>" if cf else ""
    return f"{z}<br><small>{'inside/at' if (d or 0) == 0 else f'{d:.2f}% away'}</small>{tag}"


def _zh(z, d):
    if not isinstance(z, str) or not z:
        return "-"
    return f"{z}<br><small>{'inside/at' if (d or 0) == 0 else f'{d:.2f}% away'}</small>"


def _zbg(at, kind):
    """Cell highlight: the support cell turns green when price is AT a support zone, the resistance cell red when AT a resistance zone."""
    if at == kind:
        return ' style="background:#6fd68a;font-weight:bold"' if kind == "SUPPORT" else ' style="background:#f08d84;font-weight:bold"'
    return ""


def _zv(v, src=None):
    c = {"ALIGNED": "#0a7d33", "AGAINST": "#c62828", "at support": "#0a7d33", "at resistance": "#c62828"}.get(v or "", "#777")
    lab = f'<br><small>{src} zones</small>' if isinstance(src, str) and src else ""
    return f'<span style="color:{c}"><b>{v or "-"}</b></span>{lab}'


_SORT_JS = r"""<script>
document.querySelectorAll('table.sp').forEach(function(t){Array.prototype.forEach.call(t.rows[0].cells,function(th){
th.style.cursor='pointer';th.title='click to sort (high to low / low to high)';
th.addEventListener('click',function(e){if(e.target&&e.target.tagName==='A')return;var i=th.cellIndex,b=t.tBodies[0],rows=Array.prototype.slice.call(t.rows,1),
asc=th.dataset.asc!=='1';th.dataset.asc=asc?'1':'0';
Array.prototype.forEach.call(t.rows[0].cells,function(c){var q=c.querySelector('.ar');if(q)q.remove();});
var ar=document.createElement('span');ar.className='ar';ar.textContent=asc?' \u25B2':' \u25BC';th.appendChild(ar);
function val(r){var x=r.cells[i].innerText.trim();var m=x.match(/^(\d\d):(\d\d)$/);if(m)return parseInt(m[1])*60+parseInt(m[2]);
var n=parseFloat(x.replace(/[,%x+]/g,''));return isNaN(n)?x.toLowerCase():n;}
rows.sort(function(a,c){var p=val(a),q=val(c);if(p<q)return asc?-1:1;if(p>q)return asc?1:-1;return 0;});
rows.forEach(function(r){b.appendChild(r);});});});});
</script>"""


def _link_html():
    try:
        import spurt_link
        return spurt_link.section_html()
    except Exception:
        return ""


def _wconf(w, k):
    cf = w.get("conf_" + k)
    return f"<br><small style='color:#6a1b9a'><b>+{CONF_TF}m</b> ({cf})</small>" if cf else ""


def _watch_html(meta):
    """Watch-list table: every stock that spurted >= watch multiple today, with its latest zone status (read from meta)."""
    wl = meta.get("watch") or []
    until = meta.get("watch_until", "15:00")
    head = (f'<h3 style="margin:22px 0 4px">Watch list &mdash; {len(wl)} spurt stock(s) tracked for support / resistance until {until} IST</h3>'
            f'<div class="m">A stock joins when it reaches {meta.get("watch_mult", 20):g}x or more in the top-30 and stays all day, even after it leaves the table. '
            f'You get a phone alert the first time it is at a zone. Red/green row = at a zone now. Updated every {SPURT_TF} min.</div>')
    if not wl:
        return head + '<div class="m">Nothing on the watch list yet.</div>'
    rows = ""
    for i, w in enumerate(wl, 1):
        at = w.get("at_zone", "")
        status = f"AT {at}" if at else "between zones"
        price = "-" if w.get("price") is None else f"{w['price']:.2f}"
        rows += (f"<tr{_rowstyle(at)}><td>{i}</td><td>{_tvl(w['symbol'])}</td><td>{w.get('since', '')}</td><td>{w.get('mult', 0):.1f}x</td><td>{price}</td>"
                 f"<td{_zbg(at, 'SUPPORT')}>{w.get('support') or '-'}{_wconf(w, 'support')}</td><td{_zbg(at, 'RESISTANCE')}>{w.get('resistance') or '-'}{_wconf(w, 'resistance')}</td>"
                 f"<td>{status}</td><td>{'in table' if w.get('in_table') else 'outside table'}</td><td>{'yes' if w.get('alerted') else '-'}</td><td>{w.get('time', '')}</td></tr>")
    return (head + '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Spurt at</th><th>Best spurt</th><th>Price</th>'
            f'<th>Support zone ({ZONE_TF}m)</th><th>Resistance zone ({ZONE_TF}m)</th><th>Status now</th><th>Where</th><th>Alert sent</th><th>Checked</th></tr>{rows}</table></div>')


def _thin_html(meta):
    """ADDED: stocks with a volume spurt that were left out of the list because their bid-ask spread is wide (not liquid)."""
    tl = meta.get("thin_list") or []
    head = (f'<h3 style="margin:22px 0 4px">Illiquid stocks with a volume spurt (left out of the list above) &mdash; {len(tl)}</h3>'
            f'<div class="m">Spread above {LIQ_SPREAD_THIN}% of the price (best bid vs best ask), or fewer than {LIQ_MIN_DEPTH_QTY:,} shares waiting on the bid or the ask side, or the price is within {CIRCUIT_NEAR_PCT:g}% of its upper / lower circuit, so the spurt is hard to trade at the shown price. Spurts of {THIN_LIST_MIN_MULT:g}x or more, biggest first.</div>')
    if not tl:
        return head + '<div class="m">None right now.</div>'
    rows = "".join(f"<tr><td>{i}</td><td>{_tvl(t['symbol'])}</td><td>{t['price']:.2f}</td><td>{t['candle']}</td><td>{t['multiple']:.2f}x</td>"
                   f"<td{' style=color:#c62828' if (t.get('spread_pct') or 0) > LIQ_SPREAD_THIN else ''}>{'-' if t.get('spread_pct') is None else format(t['spread_pct'], '.2f') + '%'}</td>"
                   f"<td{' style=color:#c62828' if (t.get('bid_qty') is not None and t['bid_qty'] < LIQ_MIN_DEPTH_QTY) else ''}>{'-' if t.get('bid_qty') is None else format(int(t['bid_qty']), ',')}</td>"
                   f"<td{' style=color:#c62828' if (t.get('ask_qty') is not None and t['ask_qty'] < LIQ_MIN_DEPTH_QTY) else ''}>{'-' if t.get('ask_qty') is None else format(int(t['ask_qty']), ',')}</td><td>{t.get('why', '')}</td></tr>"
                   for i, t in enumerate(tl, 1))
    return (head + '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Candle</th><th>Multiple</th><th>Bid-ask spread</th><th>Buy side qty</th><th>Sell side qty</th><th>Why left out</th></tr>' + rows + '</table></div>')


def _html(top, meta):
    def col(v):
        return "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"

    def pct(v):
        return "-" if v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'

    def hl(r):
        return _rowstyle(r.get("at_zone"))

    def ctype(r):
        v = r.get("candle_chg_pct")
        if v is None:
            return "<td>-</td>"
        oh = r.get("ohlc")
        svg = _candle_svg(*oh) if oh else ""
        if v > 0:
            return '<td style="white-space:nowrap">' + svg + ' <b style="color:#089981">GREEN</b></td>'
        if v < 0:
            return '<td style="white-space:nowrap">' + svg + ' <b style="color:#F23645">RED</b></td>'
        return '<td style="white-space:nowrap">' + svg + ' DOJI</td>'

    def mc(r):
        return ' style="background:#ffe066"' if r["multiple"] >= meta.get("alert_mult", 3.0) else ""

    def offc(r):
        v = r.get("offers_pct")
        if v is None:
            return "<td>-</td><td>-</td>"
        lk = lambda t: f'<a href="/depth/{r["symbol"]}" target="_blank" rel="noopener" title="open the Bids / Offers depth of this stock" style="color:inherit;text-decoration:none;border-bottom:1px dotted #888">{t}</a>'
        bid = f'<td style="color:#0a7d33">{lk(format(100.0 - v, ".1f") + "%")}</td>'
        if v > OFFERS_ALERT_PCT:
            return bid + f'<td style="background:#f08d84"><b>{lk(format(v, ".1f") + "%")}</b></td>'
        return bid + f'<td style="color:#c62828">{lk(format(v, ".1f") + "%")}</td>'


    body = "".join(
        f"<tr{hl(r)}><td>{i}</td><td>{_tvl(r['symbol'])}</td><td>{r['price']:.2f}</td><td>{pct(r['day_chg_pct'])}</td>"
        f"<td>{r['candle']}</td><td>{pct(r['candle_chg_pct'])}</td>{ctype(r)}<td>{r['candle_vol']:,}</td>"
        f"<td>{r['sma_vol']:,}</td><td{mc(r)}><b>{r['multiple']:.2f}x</b></td>{offc(r)}<td>{r['day_vol']:,}</td>"
        f"<td{_zbg(r.get('at_zone'), 'SUPPORT')}>{_zc(r, 'support')}</td><td{_zbg(r.get('at_zone'), 'RESISTANCE')}>{_zc(r, 'resistance')}</td><td>{_zv(r.get('zone_view'), r.get('zone_src'))}</td></tr>"
        for i, r in enumerate(top, 1)
    ) or '<tr><td colspan="16">No stocks qualified in this cycle.</td></tr>'
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60"><title>Volume Spurt {SPURT_TF}-min</title>
<style>body{{font-family:Arial,sans-serif;margin:12px;background:#fafafa}}table{{border-collapse:collapse;width:100%;background:#fff}}
th,td{{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}}th{{background:#222;color:#fff}}td:nth-child(2){{text-align:left}}
.m{{color:#555;font-size:13px;margin:6px 0 12px}}.w{{overflow-x:auto}}</style></head><body>
{_RULES_BANNER}
<h2>Volume Spurt &mdash; latest {SPURT_TF}-min candle vs {meta['sma_len']}-candle volume average (top {len(top)})</h2>
<div class="m">Run: {meta['run']} (took {meta['took_s']}s) &middot; {meta['universe']} NSE stocks (ETFs left out: {meta.get('etf_excluded', 0)}; thin stocks left out: {meta.get('thin_removed', 0)}), {meta['price_ok']} priced &ge; {meta['min_price']:.0f},
{meta['shortlisted']} shortlisted, {meta['scored']} scored, {meta['failed']} errors &middot; refreshes every {SPURT_TF} min &middot; {LEGEND} &middot; phone alert at {meta.get('alert_mult', 3.0):g}x or more &middot; zones = ALL IN ONE PRO order-block zones on the {ZONE_TF}-min chart (same as the indicator on a {ZONE_TF}-min chart); purple "{ZONE_TF}m+{CONF_TF}m zone" = the same zone also exists on the {CONF_TF}-min chart (stronger); volume spurt = 10-min candles &middot;
<a href="/spurt10.csv">CSV</a> (this cycle) &middot; <a href="/spurt10_history">full-day history page</a> &middot; <a href="/spurt10_history.csv">history CSV</a> &middot; <a href="/spurt_sell">Spurt SELL signals</a> &middot; <a href="/spurt_daily">Daily volume spurt</a> &middot; <a href="/scanner">Scanner</a></div>
<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Candle</th><th>Candle %</th><th>Candle type</th>
<th>Candle volume</th><th>{meta['sma_len']}-candle avg vol</th><th>Multiple</th><th><a href="/depth" target="_blank" rel="noopener" title="open the Bids / Offers page" style="color:#fff;text-decoration:underline">Bids %</a></th><th><a href="/depth" target="_blank" rel="noopener" title="open the Bids / Offers page" style="color:#fff;text-decoration:underline">Offers %</a></th><th>Day volume</th><th>Support zone ({ZONE_TF}m)</th><th>Resistance zone ({ZONE_TF}m)</th><th>Zone view</th></tr>{body}</table></div>
{_link_html()}
{_wick_html()}
{_clean_html()}
{_engulf_html()}
{_room_html()}
{_watch_html(meta)}
{_news_html(meta)}
{_thin_html(meta)}
{_SORT_JS}</body></html>"""


def _inr(n):
    """Indian digit grouping (18,55,355)."""
    try:
        n = int(round(float(n)))
    except Exception:
        return "-"
    s_ = str(abs(n))
    if len(s_) > 3:
        s_ = (",".join([s_[:-3][::-1][i:i + 2][::-1] for i in range(0, len(s_[:-3]), 2)][::-1]) + "," + s_[-3:])
    return ("-" if n < 0 else "") + s_


def stock_depth_html(sym):
    """ADDED: /depth/<SYMBOL> page - the 5-level bid / ask window of ONE stock (orders, quantity, price, totals, Bids % / Offers %),
    read live from Dhan marketfeed/quote (one small request per page load). Never raises."""
    import html as _h
    sym = str(sym or "").strip().upper()
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="10"><title>' + _h.escape(sym) + ' depth</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa;max-width:760px}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{padding:6px 8px;text-align:right;font-size:14px;border-bottom:1px solid #eee}th{color:#555;font-weight:600}.g{color:#0a7d33}.r{color:#c62828}'
            '.h{display:flex;gap:16px}.h>div{flex:1}.m{color:#555;font-size:13px;margin:6px 0 12px}'
            '.bar{display:flex;height:9px;border-radius:5px;overflow:hidden;margin-top:4px}</style></head><body>')
    try:
        import re as _re
        if not _re.fullmatch(r"[A-Z0-9&_\-]{1,20}", sym):
            return head + "<p>Unknown stock symbol.</p></body></html>"
        sid = None
        for s_, i_ in (_universe or []):
            if s_ == sym:
                sid = str(i_)
                break
        if sid is None:
            sid_seg = get_security_id_and_segment(sym)
            if sid_seg and sid_seg[0] and sid_seg[1] == "NSE_EQ":
                sid = str(sid_seg[0])
        if sid is None:
            return head + f"<p>{_h.escape(sym)} was not found in the NSE equity list.</p></body></html>"
        resp = ls._dhan_post("marketfeed/quote", {"NSE_EQ": [int(sid)]}, f"depth {sym}")
        q = ((resp.json().get("data") or {}).get("NSE_EQ") or {}).get(str(sid)) or {}
        d = q.get("depth") or {}
        buy, sell = (d.get("buy") or [])[:5], (d.get("sell") or [])[:5]
        if not buy and not sell:
            return head + f"<h3>{_tvl(sym)}</h3><p>Dhan sent no bid / ask depth for this stock right now (market closed or no quote).</p><p><a href='/depth'>Back to the Bids / Offers list</a></p></body></html>"
        bq = q.get("buy_quantity")
        sq = q.get("sell_quantity")
        if bq is None:
            bq = sum(float(x.get("quantity") or 0) for x in buy)
        if sq is None:
            sq = sum(float(x.get("quantity") or 0) for x in sell)
        bq, sq = float(bq or 0), float(sq or 0)
        tot = bq + sq
        bp = (bq / tot * 100.0) if tot > 0 else 0.0
        op = 100.0 - bp if tot > 0 else 0.0
        px = q.get("last_price")
        nc = q.get("net_change")
        try:
            chg = float(nc) / (float(px) - float(nc)) * 100.0
            chg_s = f' <span class="{"g" if chg >= 0 else "r"}">{chg:+.2f}%</span>'
        except Exception:
            chg_s = ""
        mq = max([float(x.get("quantity") or 0) for x in buy + sell] + [1.0])

        def cell_bar(qty, col, to="left"):
            w = min(100.0, float(qty or 0) / mq * 100.0)
            return f'<td style="background:linear-gradient(to {to},{col} {w:.0f}%,transparent {w:.0f}%)">{_inr(qty)}</td>'

        n = max(len(buy), len(sell))
        brow, srow = "", ""
        for i in range(n):
            b = buy[i] if i < len(buy) else {}
            a = sell[i] if i < len(sell) else {}
            brow += (f"<tr><td>{_inr(b.get('orders'))}</td>{cell_bar(b.get('quantity'), '#dff3e4')}<td class='g'>{float(b['price']):.2f}</td></tr>" if b else "<tr><td colspan='3'>&nbsp;</td></tr>")
            srow += (f"<tr><td class='r'>{float(a['price']):.2f}</td>{cell_bar(a.get('quantity'), '#fbe1de', 'right')}<td>{_inr(a.get('orders'))}</td></tr>" if a else "<tr><td colspan='3'>&nbsp;</td></tr>")
        return (head + f'<h2>{_tvl(sym)} &mdash; Bids / Offers</h2>'
                f'<div class="m">Price {float(px):.2f}{chg_s} &middot; read live from Dhan (5 best bids / offers) &middot; refreshes every 10 s &middot; <a href="/depth">Bids / Offers list</a> &middot; <a href="/spurt10">Volume spurt</a></div>'
                f'<div class="h"><div><b>Buy Orders</b><table><tr><th>Orders</th><th>Quantity</th><th>Bid</th></tr>{brow}'
                f'<tr><td colspan="3" style="text-align:center"><b>{_inr(bq)}</b> Total</td></tr></table></div>'
                f'<div><b>Sell Orders</b><table><tr><th>Ask</th><th>Quantity</th><th>Orders</th></tr>{srow}'
                f'<tr><td colspan="3" style="text-align:center">Total <b>{_inr(sq)}</b></td></tr></table></div></div>'
                f'<div style="display:flex;justify-content:space-between;margin-top:14px"><b class="g">Bids: {bp:.2f}%</b><b class="r">Offers: {op:.2f}%</b></div>'
                f'<div class="bar"><div style="width:{bp:.2f}%;background:#2e9e4f"></div><div style="width:{op:.2f}%;background:#d9534f"></div></div>'
                '</body></html>')
    except Exception as e:
        log.warning(f"Spurt10 /depth/{sym} failed: {e}")
        return head + f"<p>Could not read the depth of {_h.escape(sym)} right now. Please refresh in a few seconds.</p><p><a href='/depth'>Back to the Bids / Offers list</a></p></body></html>"


def depth_html():
    """ADDED: /depth page - Bids % and Offers % (Dhan depth window: total buy / sell quantity) of the stocks that HAVE A VOLUME SPURT now
    (the top-30 spurt table of the latest 10-min cycle), biggest spurt first. Click a Bids % / Offers % value for that stock's 5-level depth.
    In memory, refreshed every 10-min cycle. Never raises."""
    try:
        snap = dict(_dsnap)
        run = _dsnap_meta.get("run")
        spurt = dict(_dsnap_meta.get("spurt") or {})
        _saved = _load_depth_snapshot()                              # ADDED: the saved copy is complete and the same for every worker, so it wins when present
        zone = dict(_dsnap_meta.get("zone") or {})
        if _saved:
            run, spurt, snap, zone = _saved
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<meta http-equiv="refresh" content="60"><title>Spurt stocks - Bids / Offers</title>'
                '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff;margin-bottom:18px}'
                'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}td:nth-child(2){text-align:left}'
                '.m{color:#555;font-size:13px;margin:6px 0 12px}.w{overflow-x:auto}</style></head><body>')
        if not run or not spurt:
            return head + "<h2>Volume-spurt stocks &mdash; Bids / Offers</h2><p>No scan has run yet. It fills at the first 10-min cycle after 09:25 IST on a trading day.</p></body></html>"
        rows = sorted(((sym, mult) for sym, mult in spurt.items()), key=lambda x: -x[1])
        total_spurt = len(rows)
        if DEPTH_ONLY_AT_ZONE:                                          # ADDED: only stocks at a zone with one side above DEPTH_PCT
            def _keep(sym):
                d_ = snap.get(sym)
                if not d_ or d_.get("offers") is None:
                    return False
                at_ = (zone.get(sym) or {}).get("at")
                return at_ in ("SUPPORT", "RESISTANCE") and (d_["offers"] > DEPTH_PCT or (100.0 - d_["offers"]) > DEPTH_PCT)
            rows = [(sym, mult) for sym, mult in rows if _keep(sym)]
        body = ""
        for i, (sym, mult) in enumerate(rows, 1):
            d = snap.get(sym)
            zi = zone.get(sym) or {}                                   # ADDED: Zone column - AT SUPPORT / AT RESISTANCE (green / red) or the nearest zones when between them
            if zi.get("at") == "SUPPORT":
                zc = f'<td style="background:#d4f0dc;text-align:left"><b>AT SUPPORT</b> {zi.get("sup", "")}</td>'
            elif zi.get("at") == "RESISTANCE":
                zc = f'<td style="background:#f8d9d6;text-align:left"><b>AT RESISTANCE</b> {zi.get("res", "")}</td>'
            elif zi.get("sup") or zi.get("res"):
                zc = f'<td style="text-align:left;color:#666">between &middot; S {zi.get("sup") or "-"} &middot; R {zi.get("res") or "-"}</td>'
            else:
                zc = '<td style="text-align:left;color:#888">no zone</td>'
            if not d or d.get("offers") is None:
                body += f"<tr><td>{i}</td><td>{_tvl(sym)}</td><td>-</td><td>-</td><td><b>{mult:.2f}x</b></td><td>-</td><td>-</td><td>-</td>{zc}</tr>"
                continue
            off = d["offers"]
            chg = d["chg"]
            chg_s = "-" if chg is None else f'<span style="color:{"#0a7d33" if chg > 0 else "#c62828" if chg < 0 else "#555"}">{chg:+.2f}%</span>'
            bg_b = ' style="background:#6fd68a"' if (100.0 - off) > DEPTH_PCT else ""
            bg_o = ' style="background:#f08d84"' if off > DEPTH_PCT else ""
            dl = f'<a href="/depth/{sym}" target="_blank" rel="noopener" title="open the Bids / Offers depth of this stock" style="color:inherit;text-decoration:none;border-bottom:1px dotted #888">'
            body += (f"<tr><td>{i}</td><td>{_tvl(sym)}</td><td>{d['px']:.2f}</td><td>{chg_s}</td><td><b>{mult:.2f}x</b></td>"
                     f"<td{bg_b}><b>{dl}{100.0 - off:.1f}%</a></b></td><td{bg_o}><b>{dl}{off:.1f}%</a></b></td>"
                     f"<td>{'-' if d['spread'] is None else format(d['spread'], '.2f') + '%'}</td>{zc}</tr>")
        return (head + '<h2>Volume-spurt stocks &mdash; Bids / Offers</h2>'
                f'<div class="m">Run: {run} &middot; ' + (f'{len(rows)} of the {total_spurt} volume-spurt stocks: only those AT a support / resistance zone with Bids % or Offers % above {DEPTH_PCT:g}%' if DEPTH_ONLY_AT_ZONE else f'the {len(rows)} stocks of the 10-min volume-spurt table') + f', biggest spurt first &middot; '
                f'Bids % / Offers % = total buy / sell quantity in the Dhan depth window (green / red cell = above {DEPTH_PCT:g}%) &middot; click a value for that stock\'s 5-level depth &middot; '
                'refreshes every 10 min &middot; <a href="/spurt10">Volume spurt</a> &middot; <a href="/spurt_sell">Spurt SELL signals</a></div>'
                '<div class="w"><table class="sp"><tr><th>#</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Spurt (' + str(SPURT_TF) + '-min)</th><th>Bids %</th><th>Offers %</th><th>Bid-ask spread</th><th>Zone</th></tr>'
                + body + '</table></div>'
                + (f'<p class="m">No spurt stock is at a support / resistance zone with Bids % or Offers % above {DEPTH_PCT:g}% right now. It is checked every 10 minutes.</p>' if (DEPTH_ONLY_AT_ZONE and not rows) else '')
                + _SORT_JS + "</body></html>")
    except Exception as e:
        log.warning(f"Spurt10 /depth page failed: {e}")
        return "<p>Bids / Offers page could not be built. Please try again after the next cycle.</p>"


def history_html(date=None, min_mult=0.0, sym=None, only=None):
    """ADDED: /spurt10_history page - every logged top-30 cycle of one day (default: latest day), with a day picker,
    a minimum-multiple filter and a per-stock summary. Reads results/spurt10_history.csv; never raises."""
    import html as _h
    try:
        if not os.path.exists(HIST_PATH):
            return "<p>No history yet. It starts filling at the first cycle after 09:25 IST on a trading day.</p>"
        df = pd.read_csv(HIST_PATH)
        if df.empty:
            return "<p>No history yet.</p>"
        dates = sorted(df["date"].unique(), reverse=True)
        d = date if date in dates else dates[0]
        for c_ in ("support", "support_dist_pct", "resistance", "resistance_dist_pct", "zone_view", "zone_src", "at_zone"):
            if c_ not in df.columns:                           # rows logged before zones were added
                df[c_] = np.nan
        x = df[df["date"] == d].copy()
        x = x[x["multiple"] >= min_mult]
        if sym:
            x = x[x["symbol"].str.upper() == sym.upper()]
        x["at_zone"] = x["at_zone"].fillna("")
        if only == "zone":
            x = x[x["at_zone"] != ""]
        col = lambda v: "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"
        pct = lambda v: "-" if v != v or v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'
        summ = (x.groupby("symbol").agg(times=("candle", "count"), atz=("at_zone", lambda v: int((v != "").sum())), first=("candle", "min"), last=("candle", "max"),
                                        best=("multiple", "max"), price=("price", "last"), day=("day_chg_pct", "last"))
                .sort_values(["times", "best"], ascending=False))
        srows = "".join(f"<tr><td><b>{_h.escape(str(i))}</b></td><td>{int(r.times)}</td><td>{int(r.atz)}</td><td>{r['first']}</td><td>{r['last']}</td>"
                        f"<td>{r.best:.2f}x</td><td>{r.price:.2f}</td><td>{pct(r.day)}</td></tr>" for i, r in summ.iterrows())
        x = x.sort_values(["candle", "multiple"], ascending=[False, False])
        rows = "".join(f"<tr{_rowstyle(r.at_zone)}><td>{r.candle}</td><td>{_tvl(r.symbol)}</td><td>{r.price:.2f}</td><td>{pct(r.day_chg_pct)}</td>"
                       f"<td>{pct(r.candle_chg_pct)}</td><td>{int(r.candle_vol):,}</td><td>{int(r.sma_vol):,}</td><td><b>{r.multiple:.2f}x</b></td><td>{int(r.day_vol):,}</td>"
                       f"<td{_zbg(r.at_zone, 'SUPPORT')}>{_zh(r.support, r.support_dist_pct)}</td><td{_zbg(r.at_zone, 'RESISTANCE')}>{_zh(r.resistance, r.resistance_dist_pct)}</td><td>{_zv(None if r.zone_view != r.zone_view else r.zone_view, None if r.zone_src != r.zone_src else r.zone_src)}</td></tr>"
                       for r in x.itertuples())
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Volume Spurt history</title>
<style>body{{font-family:Arial,sans-serif;margin:12px;background:#fafafa}}table{{border-collapse:collapse;background:#fff;margin-bottom:24px}}
th,td{{border:1px solid #ddd;padding:5px 8px;text-align:right;font-size:14px}}th{{background:#222;color:#fff}}td:nth-child(2){{text-align:left}}.m{{color:#555;font-size:13px;margin:6px 0 12px}}</style></head><body>
<h2>Volume Spurt history &mdash; {_h.escape(d)}</h2>
<form method="get" class="m">Day <select name="date">{opts}</select> &nbsp; min multiple <input name="min" value="{min_mult:g}" size="4"> &nbsp; stock <input name="sym" value="{_h.escape(sym or '')}" size="10"> <label><input type="checkbox" name="only" value="zone"{" checked" if only == "zone" else ""}> only rows at a zone</label> <button>Show</button>
 &middot; {len(x)} rows, {len(summ)} stocks &middot; <a href="/spurt10_history.csv">download CSV (all days)</a> &middot; <a href="/spurt10">live /spurt10</a></form>
<div class="m">{LEGEND}</div>
<h3>Stocks of the day (how many cycles each was in the top 30) &mdash; click any heading to sort</h3>
<table class="sp"><tr><th>Stock</th><th>Times in top 30</th><th>Times at a zone</th><th>First</th><th>Last</th><th>Best multiple</th><th>Last price</th><th>Day %</th></tr>{srows}</table>
<h3>Every cycle (newest first) &mdash; click any heading to sort</h3>
<table class="sp"><tr><th>Candle</th><th>Symbol</th><th>Price</th><th>Day %</th><th>Candle %</th><th>Candle vol</th><th>50-candle avg</th><th>Multiple</th><th>Day volume</th><th>Support zone ({ZONE_TF}m)</th><th>Resistance zone ({ZONE_TF}m)</th><th>Zone view</th></tr>{rows}</table>
{_SORT_JS}</body></html>"""
    except Exception as e:
        return f"<p>History page error: {_h.escape(str(e))}</p>"


_SECTION_CSS = """<style>
.spwrap{background:#fafafa;color:#222;border-radius:10px;padding:12px;margin:0 0 28px}
.spwrap h2,.spwrap h3{color:#111}
.spwrap table{border-collapse:collapse;width:100%;background:#fff;margin-bottom:8px}
.spwrap th,.spwrap td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px;color:#222}
.spwrap th{background:#222;color:#fff}
.spwrap td:nth-child(2){text-align:left}
.spwrap tr:hover{background:#eef3ff}
.spwrap .m{color:#555;font-size:13px;margin:6px 0 12px}
.spwrap .w{overflow-x:auto}
.spwrap a{color:#0645ad}
</style>"""


_RULES_BANNER = ('<style>@keyframes rb{0%,49%{background:#fff176;color:#d50000}50%,100%{background:#d50000;color:#fff}}</style>'
                 '<div style="animation:rb 1s step-end infinite;position:sticky;top:0;z-index:99999;background:#fff176;border:3px solid #d50000;border-radius:6px;'
                 'padding:10px 14px;margin:0 0 14px;color:#d50000;font-weight:900;font-size:22px;line-height:1.5;text-align:center">'
                 '1. DON\'T RUSH &nbsp;&nbsp;|&nbsp;&nbsp; 2. DON\'T TRADE WITHOUT VALID SET UP &nbsp;&nbsp;|&nbsp;&nbsp; 3. 1 VALID SETUP ENOUGH TO EARN MONEY&nbsp;&nbsp;|&nbsp;&nbsp; 4. DON&#39;T BE EMOTIONAL ATTACHED</div>')



def section_html():
    """Table block for the /scanner page (read from the last saved result; '' if none yet)."""
    try:
        with open(JSON_PATH) as f:
            d = json.load(f)
        full = _html(d["rows"], d["meta"])
        a = full.index("<h2>")
        b = full.index("</body>")
        return _SECTION_CSS + '<div class="spwrap">' + full[a:b] + "</div>"
    except Exception:
        return ""


# ---------------------------------------------------------------- loop
def _seconds_to_next_run(now):
    nxt = now.replace(second=20, microsecond=0)
    nxt += timedelta(minutes=(SPURT_TF - now.minute % SPURT_TF) % SPURT_TF)
    if nxt <= now:
        nxt += timedelta(minutes=SPURT_TF)
    return max((nxt - now).total_seconds(), 1)


def _in_session(now):
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return 9 * 60 + 25 <= mins <= 15 * 60 + 40


# ---------------------------------------------------------------- EARLY volume spurt (the 10-min candle is still forming)  - ADDED 5 Nov
# Volume only grows during a candle, so a spurt is a spurt before the candle closes. Every 2 minutes one quote call (all stocks) gives the day volume; the volume of the LAST up to 10 minutes
# (today's volume now minus the volume ~10 minutes ago) is compared with the stock's average 10-min candle volume (50 closed candles). At EARLY_MULT x or more = EARLY alert (once per stock per
# EARLY_COOLDOWN_MIN minutes). The same leave-outs apply as everywhere else: thin / near-circuit stocks and stocks with news. The candle colour is not final yet - the alert shows the price move so far.
EARLY_ON = True
EARLY_MULT = 20.0
EARLY_MIN_AGE = 2.0           # minutes: the window must be at least this long
EARLY_MAX_AGE = SPURT_TF + 0.5          # minutes: the window is never longer than one 10-min candle
EARLY_FETCH_MAX = 25          # stocks per tick whose average volume must still be read from candles
EARLY_PRETEST = 3.0           # a stock whose average is not known yet is looked up only when its window volume is at least this many times today's pace (cheap first filter)
EARLY_COOLDOWN_MIN = 20
EARLY_ALERT_MAX = 60          # phone alerts per day
EARLY_FROM = 9 * 60 + 14
EARLY_TO = 15 * 60 + 25
EARLY_CSV = os.path.join(RESULTS_DIR, "spurt10_early.csv")
EARLY_COLS = ["date", "time", "symbol", "price", "multiple", "window_min", "move_pct", "volume_in_window", "avg_10m_volume"]
_early = {"date": "", "snaps": [], "base": {}, "hits": [], "last": {}, "count": 0}


def _early_reset(today):
    _early.update(date=today, snaps=[], base={}, hits=[], last={}, count=0)
    try:
        import csv
        if os.path.exists(EARLY_CSV):
            with open(EARLY_CSV, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if r.get("date") == today:
                        _early["hits"].append({"time": r["time"], "symbol": r["symbol"], "price": float(r["price"]), "mult": float(r["multiple"]), "window": float(r["window_min"]),
                                               "move": float(r["move_pct"]), "vol": int(float(r["volume_in_window"])), "base": int(float(r["avg_10m_volume"]))})
                        _early["last"][r["symbol"]] = time.time()
    except Exception as e:
        log.warning(f"Spurt10 early load failed: {e}")


def _early_write(today):
    try:
        import csv
        old = []
        if os.path.exists(EARLY_CSV):
            with open(EARLY_CSV, newline="", encoding="utf-8") as f:
                rd = csv.reader(f)
                next(rd, None)
                old = [r for r in rd if r and r[0] != today]
        os.makedirs(RESULTS_DIR, exist_ok=True)
        tmp = EARLY_CSV + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(EARLY_COLS)
            w.writerows(old)
            w.writerows([today, h["time"], h["symbol"], h["price"], h["mult"], h["window"], h["move"], h["vol"], h["base"]] for h in _early["hits"])
        os.replace(tmp, EARLY_CSV)
    except Exception as e:
        log.warning(f"Spurt10 early csv write failed: {e}")


def _early_snap(quotes, now_ist):
    """Remember this quote read (price, day volume of every stock) for the next ~12 minutes. Never raises."""
    try:
        today = now_ist.strftime("%Y-%m-%d")
        if _early["date"] != today:
            _early_reset(today)
        t = now_ist.timestamp()
        _early["snaps"].append((t, dict(quotes)))
        _early["snaps"] = [x for x in _early["snaps"] if t - x[0] <= 12 * 60]
    except Exception:
        pass


def _early_note_bases(rows):
    """Average 10-min candle volume of the stocks the normal cycle just scored (no extra candle call needed for them later)."""
    for r in rows:
        if r.get("sma_vol"):
            _early["base"][r["symbol"]] = float(r["sma_vol"])


def _early_fetch_base(sym, sid):
    """Average volume of the last SMA_LEN closed 10-min candles of one stock, or 0.0. Never raises."""
    try:
        as_of = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        df5 = ls.fetch_intraday_history(sid, "NSE_EQ", 5, HISTORY_DAYS)
        df = _spurt_bars(df5, as_of)
        if df is None or len(df) < SMA_LEN + 1:
            return 0.0
        return float(df["volume"].to_numpy(dtype=float)[-SMA_LEN:].mean())
    except Exception:
        return 0.0


def _early_send(hits):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not hits or not (NTFY_ON and topic) or _early["count"] >= EARLY_ALERT_MAX:
        return
    hits = sorted(hits, key=lambda h: -h["mult"])
    lines = [f"{h['symbol']}  Rs {h['price']:.2f}  {h['mult']:.1f}x in {h['window']:.0f} min  price {h['move']:+.2f}%" for h in hits[:12]]
    extra = f"\n+{len(hits) - 12} more on the Early spurt page" if len(hits) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"EARLY volume spurt - candle still forming ({len(hits)})", "Priority": "default", "Tags": "zap"})
        if resp.status_code < 300:
            _early["count"] += 1
        else:
            log.warning(f"Spurt10 early ntfy refused: {resp.status_code}")
    except Exception as ex:
        log.warning(f"Spurt10 early ntfy failed: {ex}")


def _early_once(now_ist):
    """One early check: reads all quotes, compares the volume of the last <= 10 minutes with the average 10-min candle volume. Never raises."""
    try:
        mins = now_ist.hour * 60 + now_ist.minute
        if not EARLY_ON or now_ist.weekday() >= 5 or not (EARLY_FROM <= mins <= EARLY_TO):
            return
        uni = _load_universe()
        quotes = _fetch_quotes(uni)
        _early_snap(quotes, now_ist)
        now_ts = now_ist.timestamp()
        snaps = [x for x in _early["snaps"] if EARLY_MIN_AGE * 60 <= now_ts - x[0] <= EARLY_MAX_AGE * 60]
        if not snaps:
            return
        t0, old = min(snaps, key=lambda x: x[0])                       # the oldest read inside the window: the longest window
        age = (now_ts - t0) / 60.0
        slot0 = max(((datetime.fromtimestamp(t0, IST).hour * 60 + datetime.fromtimestamp(t0, IST).minute) - (9 * 60 + 15)) / float(SPURT_TF), 3.0)
        cand, need = [], []
        for sym, (px, vol) in quotes.items():
            o = old.get(sym)
            if not o or px < MIN_PRICE:
                continue
            w = vol - o[1]
            if w <= 0:
                continue
            base = _early["base"].get(sym)
            if base is None:                                           # not known yet: pace of the day so far gives a first guess; only an obvious spurt is checked on candles
                est = max(o[1] / slot0, 1000.0)
                if w / est >= EARLY_PRETEST:
                    need.append((sym, w / est))
            elif base >= MIN_SMA_VOL and w / base >= EARLY_MULT:
                cand.append((sym, w, base))
        if need:
            need.sort(key=lambda t: -t[1])
            sid_of = dict(uni)
            todo = [sy for sy, _r in need[:EARLY_FETCH_MAX]]
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                for sy, b in zip(todo, ex.map(lambda z: _early_fetch_base(z, sid_of[z]), todo)):
                    _early["base"][sy] = b
            for sy in todo:
                b = _early["base"].get(sy) or 0.0
                w = quotes[sy][1] - old[sy][1]
                if b >= MIN_SMA_VOL and w / b >= EARLY_MULT:
                    cand.append((sy, w, b))
        if not cand:
            return
        nsyms = {}
        if NEWS_FILTER_ON and NEWS_LEAVE_OUT:
            try:
                nsyms = _news_get(now_ist) or {}
            except Exception:
                nsyms = {}
        new = []
        for sym, w, base in cand:
            if time.time() - _early["last"].get(sym, 0) < EARLY_COOLDOWN_MIN * 60 or sym in nsyms or _thin_now(sym):
                continue
            px = quotes[sym][0]
            h = {"time": now_ist.strftime("%H:%M"), "symbol": sym, "price": float(px), "mult": round(w / base, 1), "window": round(age, 1),
                 "move": round((px / old[sym][0] - 1) * 100, 2) if old[sym][0] else 0.0, "vol": int(w), "base": int(base)}
            _early["last"][sym] = time.time()
            _early["hits"].append(h)
            new.append(h)
        if new:
            log.info(f"Spurt10 EARLY spurt: {[h['symbol'] for h in new]}")
            _early_write(_early["date"])
            _early_send(new)
    except Exception as e:
        log.warning(f"Spurt10 early check failed: {e}")


def early_html():
    """Body of the Early spurt page (shown inside the Spurt10 tabs)."""
    try:
        hits = list(reversed(_early["hits"]))
        out = (f'<h2>Early volume spurt &mdash; candle still forming &middot; {EARLY_MULT:g}x or more ({len(hits)})</h2>'
               f'<div class="m">Checked every 2 minutes. Volume of the last {EARLY_MIN_AGE:g}-{EARLY_MAX_AGE - 0.5:g} minutes (today\'s volume now minus ~{SPURT_TF} minutes ago) &ge; {EARLY_MULT:g}x the stock\'s average 10-min candle volume (50 closed candles). '
               'Volume cannot fall back, so it stays a spurt when the candle closes; the candle colour is not final yet, so look at the price move. '
               f'A stock is listed again only after {EARLY_COOLDOWN_MIN} minutes. Thin / near-circuit stocks and stocks with news are left out. Phone alert for each check. Information only. '
               '<a href="/spurt10_early.csv">CSV (all days)</a></div>')
        if not hits:
            return out + '<div class="m">None yet today.</div>'
        body = "".join(f"<tr><td>{h['time']}</td><td>{_tvl(h['symbol'])}</td><td>{h['price']:.2f}</td><td><b>{h['mult']}x</b></td><td>{h['window']:g}</td>"
                       f"<td style=\"color:{'#0a7d33' if h['move'] > 0 else '#c62828' if h['move'] < 0 else '#555'}\">{h['move']:+.2f}%</td><td>{h['vol']:,}</td><td>{h['base']:,}</td></tr>" for h in hits)
        return out + ('<div class="w"><table class="sp"><tr><th>Alert time</th><th>Symbol</th><th>Price</th><th>Early spurt</th><th>Window (min)</th><th>Price move in window</th>'
                      '<th>Volume in window</th><th>Avg ' + str(SPURT_TF) + '-min volume</th></tr>' + body + '</table></div>')
    except Exception:
        return '<div class="m">The early spurt page could not be built.</div>'


def _early_next(now):
    nxt = now.replace(second=50, microsecond=0)
    if now.minute % 2 == 1:
        nxt += timedelta(minutes=1)
    elif nxt <= now:
        nxt += timedelta(minutes=2)
    return nxt



def loop():
    log.info("Spurt10 autorun started - every 10 min at candle close (09:25-15:40 IST, Mon-Fri); early volume spurt check every 2 min (09:14-15:25).")
    while True:
        try:
            now = datetime.now(IST)
            t_wake, kind = now + timedelta(seconds=_seconds_to_next_run(now)), "run"
            if EARLY_ON and now.weekday() < 5:
                mins = now.hour * 60 + now.minute
                t_e = now.replace(hour=9, minute=14, second=50, microsecond=0) if mins < EARLY_FROM else (_early_next(now) if mins <= EARLY_TO else None)
                if t_e is not None and t_e < t_wake - timedelta(seconds=15):
                    t_wake, kind = t_e, "early"
            time.sleep(max((t_wake - now).total_seconds(), 1))
            if kind == "early":
                _early_once(datetime.now(IST))
            elif _in_session(datetime.now(IST)):
                run_once()
        except Exception as e:
            log.error(f"Spurt10 cycle failed: {e}")
            time.sleep(30)
