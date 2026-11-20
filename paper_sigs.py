"""PAPER CHART - signals of ALL IN ONE PRO SWING / CONSOL BOX / GAP RETRACE and the Nifty candles for RS vs NIFTY (ADDED 20 Nov 2026).

The paper chart (paper_chart.js) cannot run Pine. It asks this module for the signals instead: the SAME Python detectors the swing page and the daily scan use
(swing_page.detect, daily_scan.strong_buy / pullback_buy / gap_retrace_buy) are run over the candles the chart is showing, and every signal comes back with its
entry, SL, T1 / T2 / T6 and the room to resistance. Nothing here places an order and nothing here is written to disk.

  groups: "aio" = ALL IN ONE PRO (VS BUY, SPURT BUY), "sw" = ALL IN ONE PRO SWING (VOLUME SPURT + SMA), "cbox" = CONSOL BOX, "gap" = GAP RETRACE (daily chart only). BUY side only.
  signals_json(symbol, tf) -> (dict, error)   {"signals": [{t, group, name, entry, sl, t1, t2, t6, risk_pct, mult, room_r, room}], "note": "..."}
  nifty_json(tf)           -> (list, error)   [{"t": candle open (UTC seconds, same as the stock candles), "c": close}]
"""
import time
import logging
import threading
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import paper_engine as pe
import paper_dhan as dl

log = logging.getLogger("paper")

MAX_SIGNALS = 80                      # newest signals returned
ROOM_FOR_LAST = 40                    # the room text (resistance search) is worked out for the newest signals only
NIFTY_ID, NIFTY_SEG, NIFTY_INS = "13", "IDX_I", "INDEX"
_cache = {}
_lock = threading.Lock()


def _epoch(idx):
    return int((idx - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1))


def _get(key, ttl):
    c = _cache.get(key)
    return c[1] if c and time.time() - c[0] < ttl else None


def _put(key, val):
    with _lock:
        _cache[key] = (time.time(), val)
        if len(_cache) > 120:
            for k, _ in sorted(list(_cache.items()), key=lambda kv: kv[1][0])[:40]:
                _cache.pop(k, None)
    return val


# ------------------------------------------------------------------------------------------------------- signals
def signals_json(symbol, tf):
    symbol = (symbol or "").strip().upper()
    ent = pe._ent(symbol)
    if not ent:
        return None, "Unknown symbol (NSE equities only)."
    tf = pe._tf(tf, "15")
    ck = ("sig", symbol, tf)
    c = _get(ck, 45)
    if c is not None:
        return c, ""
    try:
        import swing_page as sw
        import daily_scan as ds
        d = pe._closed_bars(ent[0], tf, dl.utc_now())          # closed candles only: a signal is final
        if d is None or len(d) < sw.MIN_BARS:
            return {"signals": [], "note": "Not enough candles on this timeframe for the signal rules (at least %d needed)." % sw.MIN_BARS}, ""
        d = d.tail(700)
        n = len(d)
        o, h, l, c_, v = (d[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume"))
        win = np.ones(n, dtype=bool)
        raw = []                                                # (i, group, name, entry, sl, mult, extra)
        try:
            for i, _sec, nm, entry, sl, mult, crossed in sw.detect(d, win):
                raw.append((i, "sw" if nm == "VOLUME SPURT + SMA" else "aio", nm, entry, sl, mult, ("crossed SMA " + crossed.replace(",", " ")) if crossed and crossed != "-" else ""))
        except Exception as e:
            log.warning("paper sigs: ALL IN ONE PRO failed for %s: %s", symbol, e)
        try:
            for i, entry, sl, vx, bn in ds.strong_buy(d, win):
                raw.append((i, "cbox", "STRONG BUY", entry, sl, vx, "box of %s candles" % bn))
            for i, entry, sl in ds.pullback_buy(o, h, l, c_, v, win):
                raw.append((i, "cbox", "PULLBACK BUY", entry, sl, None, ""))
        except Exception as e:
            log.warning("paper sigs: CONSOL BOX failed for %s: %s", symbol, e)
        note = ""
        if tf == "D":
            try:
                for i, entry, sl in ds.gap_retrace_buy(o, h, l, c_, win):
                    raw.append((i, "gap", "GAP UP PULLBACK BUY", entry, sl, None, ""))
            except Exception as e:
                log.warning("paper sigs: GAP RETRACE failed for %s: %s", symbol, e)
        else:
            note = "GAP RETRACE signals are shown on the daily (D) chart only."
        raw = [r for r in raw if r[3] > r[4] > 0]
        raw.sort(key=lambda r: (r[0], r[1]))
        raw = raw[-MAX_SIGNALS:]
        out = []
        for k, (i, grp, nm, entry, sl, mult, extra) in enumerate(raw):
            t1, t2, t6 = sw._levels(entry, sl)
            room_r, room = None, ""
            if k >= len(raw) - ROOM_FOR_LAST:
                try:
                    lv = sw._res_levels(d, i, entry)
                    room_r, room = sw._room_text(entry, sl, lv[0][0] if lv else None)
                except Exception:
                    pass
            out.append({"t": _epoch(d.index[i]), "group": grp, "name": nm, "entry": round(entry, 2), "sl": round(sl, 2),
                        "t1": round(t1, 2), "t2": round(t2, 2), "t6": round(t6, 2), "risk_pct": round((entry - sl) / entry * 100.0, 2),
                        "mult": (round(float(mult), 1) if mult else None), "extra": extra, "room_r": room_r, "room": room})
        return _put(ck, {"signals": out, "note": note}), ""
    except Exception as ex:
        log.warning("paper sigs: failed for %s %s: %s", symbol, tf, ex)
        return None, "Could not work out the signals (%s)" % ex


# ------------------------------------------------------------------------------------------------------- NIFTY candles
def _nifty_m5(days):
    return pe._cached(("nifty5", days), 8, lambda: dl.candles(NIFTY_ID, NIFTY_SEG, NIFTY_INS, days, with_oi=False))


def _nifty_daily():
    def fetch():
        now = datetime.now(pe.IST)
        payload = {"securityId": NIFTY_ID, "exchangeSegment": NIFTY_SEG, "instrument": NIFTY_INS, "expiryCode": 0, "oi": False,
                   "fromDate": (now - timedelta(days=1825)).strftime("%Y-%m-%d"), "toDate": now.strftime("%Y-%m-%d")}
        d = dl.parse_intraday(dl.post("charts/historical", payload, "NIFTY daily").json())
        dates = (d.index + pe._IST_OFF).normalize()
        d = d[["open", "high", "low", "close", "volume"]].copy()
        d.index = dates
        return d[~d.index.duplicated(keep="last")]
    d = pe._cached(("nifty_d",), 1800, fetch).copy()
    try:                                                         # today's candle from the 5-minute index candles (Dhan's daily list ends yesterday)
        m5 = _nifty_m5(8)
        ist = m5.index + pe._IST_OFF
        today = pd.Timestamp(pe.now_ist().date())
        t = m5[ist.normalize() == today]
        if len(t) and (not len(d) or d.index[-1] < today):
            d.loc[today] = [float(t["open"].iloc[0]), float(t["high"].max()), float(t["low"].min()), float(t["close"].iloc[-1]), float(t["volume"].sum())]
    except Exception:
        pass
    return d.sort_index()


def nifty_json(tf):
    tf = pe._tf(tf, "15")
    ck = ("nif", tf)
    c = _get(ck, 20)
    if c is not None:
        return c, ""
    try:
        if tf in pe.TF_HTF:
            d = _nifty_daily()
            if tf != "D":
                d = pe._period(d, tf)
            d = d.copy()
            d.index = d.index - pe._IST_OFF                      # IST midnight -> UTC, like the stock candles
        else:
            t = int(tf)
            days = min(88, pe.TF_INTRA[t][1])
            d = _nifty_m5(days)
            if t != 5:
                d = pe._resample_intra(d, t)
        out = [{"t": _epoch(ix), "c": round(float(cl), 2)} for ix, cl in zip(d.index, d["close"])]
        return _put(ck, out), ""
    except Exception as ex:
        log.warning("paper sigs: NIFTY candles failed (%s): %s", tf, ex)
        return None, "Could not read the Nifty candles (%s)" % ex
