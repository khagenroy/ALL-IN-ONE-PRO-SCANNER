"""
RSI FLUSH ON SPURT STOCKS  (page: /spurt_flush   csv: /spurt_flush.csv)                                  ADDED 8 Nov

Connects the volume spurt scan with the RSI Flush rule. For every stock that had a volume spurt today (a closed spurt candle of
FLUSH_MIN_MULT x or more, same candles as the Spurt10 page) it runs the RSI Flush rule on that stock's 10-minute candles and lists the
setups of today: signal candle -> break -> ACTIVE / STOPPED / T6 DONE, with entry, stop and T1..T6.

  * The RULE is not new: it is rsi_flush_live.live_setups(), the same Python port of the TradingView "RSI Flush" Section C that the
    /scanner page uses (RSI 14 at 30 / 70 then back within 4 candles, volume 1.5x, break of the signal candle, cascade stop).
  * It uses the candles the spurt scan has ALREADY downloaded: NO extra Dhan call, no orders. Any error is caught and logged.
  * The old RSI Flush table on /scanner is not changed. This page is only the RSI Flush signals of spurt stocks.
  * "After spurt" = YES when the RSI Flush signal candle came at / after the stock's first spurt candle of the day.
  * Stocks that stop being scanned (their volume fades out of the shortlist) keep their last known setups for the day.
"""
import os
import json
import logging
from datetime import datetime

import pandas as pd

import spurt10_live as sp

log = logging.getLogger("spurt_flush")
IST = sp.IST

ON = True
FLUSH_MIN_MULT = 3.0          # a stock counts as a "spurt stock" for the day from its first closed spurt candle of this many x (same as the main alert level)
NTFY_ALERT = True             # phone alert when a spurt stock gets a NEW RSI Flush signal (set False to switch it off)

PATH = os.path.join(sp.RESULTS_DIR, "spurt_flush.csv")
STATE = os.path.join(sp.RESULTS_DIR, "spurt_flush_state.json")
COLS = ["date", "symbol", "spurt_x", "spurt_time", "side", "status", "detail", "after_spurt", "signal_ts", "confirm_ts", "close", "entry", "sl", "sl_now",
        "T1", "T2", "T3", "T4", "T5", "T6", "risk_pct", "rsi", "updated"]
_st = {"date": "", "spurt": {}, "setups": {}, "alerted": []}
_warned = {"imp": False}


def _load(today_s):
    try:
        with open(STATE) as f:
            s_ = json.load(f)
        if s_.get("date") == today_s:
            _st.update(date=today_s, spurt=dict(s_.get("spurt") or {}), setups=dict(s_.get("setups") or {}), alerted=list(s_.get("alerted") or []))
            return
    except Exception:
        pass
    _st.update(date=today_s, spurt={}, setups={}, alerted=[])


def _hm(ts):
    """'2026-11-09 04:55:00' (candle open, UTC) -> '10:25' IST."""
    try:
        return (pd.Timestamp(ts) + pd.Timedelta(minutes=330)).strftime("%H:%M")
    except Exception:
        return str(ts)


def _flat_rows(today_s, stamp):
    out = []
    for sym, lst in sorted(_st["setups"].items()):
        sp_ = _st["spurt"].get(sym) or {}
        for s_ in lst:
            sig, first = _hm(s_.get("signal_ts", "")), sp_.get("time", "")
            out.append({"date": today_s, "symbol": sym, "spurt_x": sp_.get("peak", ""), "spurt_time": first, "side": s_["side"], "status": s_["status"],
                        "detail": s_.get("detail", ""), "after_spurt": ("YES" if (first and sig >= first) else "NO"),
                        "signal_ts": sig, "confirm_ts": _hm(s_["confirm_ts"]) if s_.get("confirm_ts") else "",
                        **{k: s_.get(k, "") for k in ("close", "entry", "sl", "sl_now", "T1", "T2", "T3", "T4", "T5", "T6", "risk_pct", "rsi")}, "updated": stamp})
    return out


def _write(today_s, stamp):
    try:
        os.makedirs(sp.RESULTS_DIR, exist_ok=True)
        df = pd.DataFrame(_flat_rows(today_s, stamp), columns=COLS)
        if os.path.exists(PATH):
            try:
                old = pd.read_csv(PATH)
                df = pd.concat([old[old["date"].astype(str) != today_s], df], ignore_index=True)
            except Exception as e:
                log.warning(f"spurt_flush csv could not be read, writing today only: {e}")
        df.to_csv(PATH + ".tmp", index=False)
        os.replace(PATH + ".tmp", PATH)
        with open(STATE + ".tmp", "w") as f:
            json.dump(_st, f)
        os.replace(STATE + ".tmp", STATE)
    except Exception as e:
        log.warning(f"spurt_flush write failed: {e}")


def _notify(items):
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not (NTFY_ALERT and sp.NTFY_ON and topic and items):
        return
    try:
        import requests
        lines = [f"{sym}  {s_['side']}  {s_['status']}  entry {s_.get('entry')}  SL {s_.get('sl')}  (spurt {(_st['spurt'].get(sym) or {}).get('peak', '')}x)"
                 for sym, s_ in items[:12]]
        requests.post(f"https://ntfy.sh/{topic}", data="\n".join(lines).encode("utf-8"), timeout=8,
                      headers={"Title": f"RSI Flush on a spurt stock ({len(items)})", "Priority": "high", "Tags": "chart_with_upwards_trend"})
    except Exception as e:
        log.warning(f"spurt_flush ntfy failed: {e}")


def update(rows, now_ist):
    """Called by spurt10_live.run_once with the scored rows (each still holds its 5-minute candles in '_df5'). Never raises."""
    if not ON:
        return
    try:
        import live_scanner as ls
        try:
            import rsi_flush_live as rf
        except Exception as e:
            if not _warned["imp"]:
                _warned["imp"] = True
                log.warning(f"spurt_flush: rsi_flush_live could not be imported ({e}) - nothing is done")
            return
        today_s = now_ist.strftime("%Y-%m-%d")
        if _st["date"] != today_s:
            _load(today_s)
        for r in rows:                                          # stocks that spurted in this cycle join today's spurt list
            try:
                if r["multiple"] >= FLUSH_MIN_MULT:
                    e = _st["spurt"].setdefault(r["symbol"], {"time": r["candle"], "peak": r["multiple"]})
                    if r["multiple"] > e["peak"]:
                        e["peak"] = r["multiple"]
            except Exception:
                continue
        new = []
        for r in rows:
            sym = r["symbol"]
            if sym not in _st["spurt"] or r.get("_df5") is None:
                continue
            try:
                asof = r.get("_as_of")
                d10 = ls.drop_forming_bars(ls.build_merged_bars(r["_df5"], 2), 10, asof)
                lst = rf.live_setups(d10, "10m", sym)
            except Exception as e:
                log.warning(f"spurt_flush: {sym} failed: {e}")
                continue
            _st["setups"][sym] = lst                            # fresh recompute (stateless), replaces the stock's earlier list
            if not lst:
                _st["setups"].pop(sym, None)
            for s_ in lst:
                key = f"{sym}|{s_['side']}|{s_['signal_ts']}"
                if key not in _st["alerted"] and s_["status"] in ("PENDING", "ACTIVE"):
                    _st["alerted"].append(key)
                    new.append((sym, s_))
        _write(today_s, now_ist.strftime("%H:%M"))
        if new:
            _notify(new)
            log.info(f"spurt_flush: new RSI Flush setups on spurt stocks: {[(a, b['side']) for a, b in new]}")
    except Exception as e:
        log.warning(f"spurt_flush step failed: {e}")


def page_html():
    """The /spurt_flush page: today's RSI Flush setups of the stocks that had a volume spurt today. Never raises."""
    head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta http-equiv="refresh" content="60"><title>RSI Flush on Spurt Stocks</title>'
            '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;width:100%;background:#fff}'
            'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff;position:sticky;top:0}'
            'td:nth-child(1){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>')
    try:
        today_s = datetime.now(IST).strftime("%Y-%m-%d")
        if _st["date"] != today_s and os.path.exists(STATE):
            _load(today_s)
        rows = _flat_rows(today_s, "")
        n_sp = len(_st["spurt"]) if _st["date"] == today_s else 0
        col = {"ACTIVE": "#0a7d33", "PENDING": "#b26a00", "STOPPED": "#c62828", "T6 DONE": "#0a7d33"}

        def side(v):
            return f'<b style="color:{"#0a7d33" if v == "BUY" else "#c62828"}">{v}</b>'
        body = "".join(
            f"<tr><td>{sp._tvl(r['symbol'])}</td><td>{r['spurt_x']}x</td><td>{r['spurt_time']}</td><td>{side(r['side'])}</td>"
            f"<td><b style=\"color:{col.get(r['status'], '#555')}\">{r['status']}</b><br><span style=\"font-size:12px;color:#555\">{r['detail']}</span></td>"
            f"<td>{r['after_spurt']}</td><td>{r['signal_ts']}</td><td>{r['confirm_ts'] or '-'}</td><td>{r['close']}</td><td>{r['entry']}</td><td>{r['sl']}</td>"
            f"<td>{r['sl_now']}</td><td>{r['T1']}</td><td>{r['T2']}</td><td>{r['T3']}</td><td>{r['T6']}</td><td>{r['risk_pct']}%</td><td>{r['rsi']}</td></tr>"
            for r in rows) or '<tr><td colspan="18" style="text-align:center">No RSI Flush setup on a spurt stock yet today.</td></tr>'
        return (head + f'<h2>RSI Flush on volume-spurt stocks &mdash; today ({len(rows)} setups, {n_sp} spurt stocks followed)</h2>'
                f'<div class="m">Stocks that had a closed volume-spurt candle of {FLUSH_MIN_MULT:g}x or more today, and the RSI Flush signals on their 10-minute candles '
                f'(RSI 14 at 30 / 70 then back within 4 candles, volume 1.5x, then the break of the signal candle). Same rule as the RSI Flush table on /scanner. '
                f'"After spurt" = the RSI Flush signal candle came at / after the stock\'s first spurt candle. PENDING = waiting for the break. Information only. '
                f'Times are IST candle open. <a href="/spurt_flush.csv" target="_blank" rel="noopener">CSV</a> &middot; '
                f'<a href="/spurt10" target="_blank" rel="noopener">Spurt10</a> &middot; <a href="/scanner" target="_blank" rel="noopener">Scanner (all RSI Flush)</a></div>'
                '<table><tr><th>Symbol</th><th>Peak spurt</th><th>1st spurt</th><th>Side</th><th>Status</th><th>After spurt</th><th>Signal</th><th>Break</th>'
                '<th>Price</th><th>Entry</th><th>SL</th><th>SL now</th><th>T1</th><th>T2</th><th>T3</th><th>T6</th><th>Risk</th><th>RSI</th></tr>' + body + '</table></body></html>')
    except Exception as e:
        log.warning(f"spurt_flush page failed: {e}")
        return head + "<p>The page could not be built - see the logs.</p></body></html>"
