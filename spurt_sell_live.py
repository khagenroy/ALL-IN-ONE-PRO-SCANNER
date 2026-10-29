"""
SPURT SELL  (page: /spurt_sell   csv: /spurt_sell.csv)

Same rule as the TradingView indicator "VOLUME SPURT RED CANDLE SELL", on the 10-minute candles the volume-spurt
scanner already downloads (no extra Dhan calls):

  1. SPURT      a green 10-min candle whose volume >= SPURT_SELL_MULT (50) x the average volume of the previous
                SPURT_SELL_AVG_LEN (50) candles. The 09:15 opening candle counts too (set SPURT_SELL_SKIP_OPEN=true to ignore it).
  2. RED CANDLE within SPURT_SELL_WAIT (10) candles after the spurt, a SOLID red candle (body >= SPURT_SELL_BODY_PCT
                (60) % of its high-low range) whose volume is higher than the candle before it.
  3. SELL       entry = close of that red candle, stop loss = its high, targets = 1x and 2x the risk below the entry.
                One signal per spurt.
  4. ZONE       (required, same as the indicator checklist point 2) price touched a RESISTANCE order-block zone (ALL IN ONE PRO
                logic, 10-min chart, see spurt_zones.py) on the spurt candle or on any candle between the spurt and the red candle.
                Set SPURT_SELL_NEED_ZONE=false to switch this off and get the old 3-point signal.

Read-only: it places no orders, is not connected to Section A / Section B / the bot, and any error here is caught
and logged without touching any other scan. Signals are logged to results/spurt_sell_history.csv.

Only stocks the volume-spurt scan scores in a cycle (the 150 busiest) can be checked in that cycle.

ENV (all optional)
  SPURT_SELL_ENABLED=true
  SPURT_SELL_MULT=50   SPURT_SELL_AVG_LEN=50   SPURT_SELL_WAIT=10   SPURT_SELL_BODY_PCT=60
  SPURT_SELL_SKIP_OPEN=false   (true = ignore the 09:15 candle as a spurt)
  SPURT_SELL_NEED_ZONE=true    (false = do not require the resistance zone)   SPURT_SELL_ZONE_NEAR_PCT=0.5  ('at zone' tolerance %)
  SPURT_SELL_R1=1      SPURT_SELL_R2=2
  SPURT_SELL_NTFY_TOPIC=<a NEW ntfy topic>     phone alert goes ONLY to this topic (blank = no phone alert)
  SPURT_SELL_NTFY_MAX_PER_DAY=40
"""

import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger("spurt_sell")
IST = ZoneInfo("Asia/Kolkata")


def _f(name, default):
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return float(default)


ENABLED = os.environ.get("SPURT_SELL_ENABLED", "true").strip().lower() == "true"
MULT = _f("SPURT_SELL_MULT", 50)
AVG_LEN = int(_f("SPURT_SELL_AVG_LEN", 50))
WAIT = int(_f("SPURT_SELL_WAIT", 10))
BODY_PCT = _f("SPURT_SELL_BODY_PCT", 60)
R1 = _f("SPURT_SELL_R1", 1)
R2 = _f("SPURT_SELL_R2", 2)
MAX_PER_DAY = int(_f("SPURT_SELL_NTFY_MAX_PER_DAY", 40))
NEED_ZONE = os.environ.get("SPURT_SELL_NEED_ZONE", "true").strip().lower() == "true"
ZONE_NEAR_PCT = _f("SPURT_SELL_ZONE_NEAR_PCT", 0.5)
NEED_RSI = os.environ.get("SPURT_SELL_NEED_RSI", "true").strip().lower() == "true"       # checklist point 6: RSI above RSI_MIN since the spurt
RSI_LEN = int(_f("SPURT_SELL_RSI_LEN", 14))
RSI_MIN = _f("SPURT_SELL_RSI_MIN", 70)
HTF_MIN = int(_f("SPURT_SELL_HTF_MIN", 30))                                               # checklist point 5 (information, not required)
SKIP_OPEN = os.environ.get("SPURT_SELL_SKIP_OPEN", "false").strip().lower() == "true"     # default: the 09:15 candle COUNTS as a spurt

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
HIST_PATH = os.path.join(RESULTS_DIR, "spurt_sell_history.csv")
COLS = ["date", "run", "candle", "symbol", "entry", "sl", "t1", "t2", "risk_pct", "spurt_candle", "spurt_mult",
        "candles_after_spurt", "red_vol_vs_prev", "day_chg_pct", "res_zone", "rsi_peak", "htf_zone", "score", "sig_ohlc"]

_seen = {"date": None, "keys": set(), "count": 0}


def _ist(ts):
    return pd.Timestamp(ts) + pd.Timedelta(minutes=330)


# ---------------------------------------------------------------- candle drawing (setup candles on the page)
def _candle_svg(o, h, l, c, ref=None):
    """Small Japanese candle (body + wick) with its O/H/L/C written beside it."""
    try:
        o, h, l, c = float(o), float(h), float(l), float(c)
    except Exception:
        return "-"
    if not all(v == v for v in (o, h, l, c)):
        return "-"
    col = "#089981" if c > o else "#F23645" if c < o else "#888"
    rng = (h - l) or 1e-9
    # real size: a candle whose high-low range is 3x the stock's usual candle range (or more) fills the whole box; a normal one is about a third of it
    try:
        frac = min(1.0, max(0.12, (h - l) / (3.0 * float(ref)))) if ref and float(ref) > 0 else 1.0
    except Exception:
        frac = 1.0
    hh = 32.0 * frac
    y0 = 2 + (32.0 - hh) / 2                                  # centred vertically
    y = lambda v: y0 + (h - v) / rng * hh
    top, bot = min(y(o), y(c)), max(y(o), y(c))
    fm = (lambda v: f"{v:.2f}") if h >= 1 else (lambda v: f"{v:.5f}")
    svg = (f'<svg width="22" height="40" viewBox="0 0 22 36" preserveAspectRatio="none" style="vertical-align:middle">'
           f'<line x1="11" y1="{y(h):.1f}" x2="11" y2="{y(l):.1f}" stroke="{col}" stroke-width="2"/>'
           f'<rect x="5" y="{top:.1f}" width="12" height="{max(bot - top, 2):.1f}" fill="{col}" stroke="{col}"/></svg>')
    return f'<span title="O {fm(o)}  H {fm(h)}  L {fm(l)}  C {fm(c)}" style="white-space:nowrap">{svg}</span>'


def _pack(o, h, l, c, i, avg_n=50):
    """'open|high|low|close|usual range' of candle i, stored in the history CSV so the page can draw it later."""
    rngs = (h[max(i - avg_n, 0):i] - l[max(i - avg_n, 0):i])
    ref = float(rngs.mean()) if len(rngs) else 0.0
    return f"{o[i]:.4f}|{h[i]:.4f}|{l[i]:.4f}|{c[i]:.4f}|{ref:.4f}"


def _draw(txt):
    try:
        p = [float(x) for x in str(txt).split("|")]
        return _candle_svg(p[0], p[1], p[2], p[3])
    except Exception:
        return "-"


# ---------------------------------------------------------------- spurt tracker (every spurt stock of the day, not only the ones with a signal)
TRACK_MULT = _f("SPURT_SELL_TRACK_MULT", 10)                      # a stock is tracked once any candle today has volume >= this x its average
TRACK_PATH = os.path.join(RESULTS_DIR, "spurt_track.json")


def track(df, sym, day_chg=None, price=None):
    """Summary of today's latest volume spurt (>= TRACK_MULT x) of one stock and what the candles after it did. None when no spurt today. Never raises."""
    if not ENABLED:
        return None
    try:
        n = len(df)
        if n < AVG_LEN + 2:
            return None
        o = df["open"].to_numpy(dtype=float)
        h = df["high"].to_numpy(dtype=float)
        l = df["low"].to_numpy(dtype=float)
        c = df["close"].to_numpy(dtype=float)
        v = df["volume"].to_numpy(dtype=float)
        L = n - 1
        ist = [_ist(t) for t in df.index]
        today = ist[L].date()
        avg = pd.Series(v).rolling(AVG_LEN).mean().shift(1).to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            mult = np.where(avg > 0, v / avg, 0.0)
        idx = [i for i in range(AVG_LEN, n) if ist[i].date() == today and mult[i] >= TRACK_MULT
               and not (SKIP_OPEN and ist[i].hour == 9 and ist[i].minute == 15)]
        if not idx:
            return None
        j = idx[-1]                                                    # latest spurt of the day
        best = float(max(mult[i] for i in idx))
        colour = lambda i: "GREEN" if c[i] > o[i] else "RED" if c[i] < o[i] else "DOJI"
        red_solid_vol = False
        for i in range(j + 1, L + 1):
            rng = h[i] - l[i]
            if c[i] < o[i] and rng > 0 and (o[i] - c[i]) / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]:
                red_solid_vol = True
        red_any = any(c[i] < o[i] for i in range(j + 1, L + 1))
        res_zone = htf_zone = None                                    # zone checks only while the sell window is open (they are slow); older rows keep their last value
        if colour(j) == "GREEN" and L - j <= WAIT:
            try:
                import spurt_zones
                res_zone = spurt_zones.resistance_touch(df, j, L, ZONE_NEAR_PCT) or ""
            except Exception:
                res_zone = None
            htf_zone = _htf_touch(df, j, L, ZONE_NEAR_PCT)
        return {
            "res_zone": res_zone, "htf_zone": htf_zone, "red_any": bool(red_any),
            "move_pct": round(float((c[L] / c[j] - 1) * 100), 2), "off_high_pct": round(float((c[L] / h[j:L + 1].max() - 1) * 100), 2),
            "vol_vs_spurt": round(float(v[L] / v[j]), 2) if v[j] > 0 else None,
            "strip": [f"{o[i]:.4f}|{h[i]:.4f}|{l[i]:.4f}|{c[i]:.4f}" for i in range(max(j - PRE_CANDLES, 0), min(L, j + WAIT) + 1)],
            "pre": int(j - max(j - PRE_CANDLES, 0)),
            "symbol": sym, "price": round(float(c[L]), 2), "day_chg_pct": day_chg,
            "spurt_at": ist[j].strftime("%H:%M"), "spurt_mult": round(float(mult[j]), 1), "best_mult": round(best, 1),
            "spurt_colour": colour(j), "since": int(L - j),
            "first_after": colour(j + 1) if j + 1 <= L else "-",
            "latest_candle": ist[L].strftime("%H:%M"), "latest_colour": colour(L),
            "ohlc": f"{o[L]:.4f}|{h[L]:.4f}|{l[L]:.4f}|{c[L]:.4f}",
            "red_solid_vol": bool(red_solid_vol),
            "rsi_peak": round(float(np.nanmax(_rsi(c, RSI_LEN)[j:L + 1])), 1),
        }
    except Exception:
        return None


def update_tracker(items, now_ist):
    """Called every volume-spurt cycle with the track() result of each scored stock. Keeps every stock that spurted today (stocks not scored
    in a later cycle keep their last known row, marked with their last-seen time). Saved to results/spurt_track.json. Never raises."""
    try:
        import json
        today = now_ist.strftime("%Y-%m-%d")
        data = {"date": today, "rows": {}}
        try:
            with open(TRACK_PATH) as fh:
                old = json.load(fh)
            if old.get("date") == today:
                data = old
        except Exception:
            pass
        stamp = now_ist.strftime("%H:%M")
        for it in items:
            if it:
                it["seen"] = stamp
                prev = data["rows"].get(it["symbol"])
                if prev and prev.get("spurt_at") == it.get("spurt_at"):      # same spurt: keep the zone results found while the window was open
                    for k in ("res_zone", "htf_zone"):
                        if it.get(k) is None and prev.get(k) is not None:
                            it[k] = prev[k]
                data["rows"][it["symbol"]] = it
        data["updated"] = stamp
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(TRACK_PATH + ".tmp", "w") as fh:
            json.dump(data, fh)
        os.replace(TRACK_PATH + ".tmp", TRACK_PATH)
    except Exception as e:
        log.warning(f"SpurtSell tracker failed: {e}")


def _tv_link(sym):
    import html as _h
    import urllib.parse as _up
    u = "https://www.tradingview.com/chart/b3scGnZI/?symbol=" + _up.quote("NSE:" + str(sym).replace("-", "_"), safe=":_")
    return f'<a href="{u}" target="_blank" rel="noopener" title="open chart in TradingView" style="color:inherit;text-decoration:none;border-bottom:1px dotted #888"><b>{_h.escape(str(sym))}</b></a>'


PRE_CANDLES = 10      # candles BEFORE the spurt candle shown in the strip (dimmed), so the strip is 10 before + the spurt + up to 10 after


def _strip_svg(strip, tag=0, pre=0):
    """The 10 candles before the spurt (dimmed), the spurt candle and the candles after it (up to 10) side by side on one shared price scale, so sizes can be compared. A dot marks the spurt candle."""
    try:
        cs = [[float(x) for x in str(t).split("|")[:4]] for t in strip]
        if not cs:
            return "-"
        hi = max(c[1] for c in cs)
        lo = min(c[2] for c in cs)
        rng = (hi - lo) or 1e-9
        y = lambda v: 3 + (hi - v) / rng * 58
        w = 13
        parts = []
        for i, (o, h, l, c) in enumerate(cs):
            col = "#089981" if c > o else "#F23645" if c < o else "#888"
            x = i * w + w / 2
            op = ' opacity="0.55"' if i < pre else ""
            top, bot = min(y(o), y(c)), max(y(o), y(c))
            parts.append(f'<line x1="{x:.1f}" y1="{y(h):.1f}" x2="{x:.1f}" y2="{y(l):.1f}" stroke="{col}" stroke-width="1.5"{op}/>'
                         f'<rect x="{x - 4:.1f}" y="{top:.1f}" width="8" height="{max(bot - top, 1.5):.1f}" fill="{col}"{op}/>')
        parts.append(f'<circle cx="{pre * w + w / 2:.1f}" cy="68" r="2.4" fill="#d29922"/>')
        title = "10 candles before the spurt (dim), the spurt candle (dot) and the candles after it"
        return f'<svg width="{len(cs) * w}" height="72" viewBox="0 0 {len(cs) * w} 72" style="vertical-align:middle"><title>{title}</title>' + "".join(parts) + "</svg>"
    except Exception:
        return "-"


def _live_c(lp, sym, close):
    v = lp.get(sym)
    if v is None:
        return "-"
    c = "#089981" if v > close else "#F23645" if v < close else "#888"
    return f"<b style='color:{c}'>{v:g}</b>"


def _trk_score(r):
    """Checklist points met so far: 1 green spurt (>= the SELL multiple), 2 resistance zone, 3+4 solid red candle with higher volume, 5 higher-time-frame zone, 6 RSI above the level."""
    return (1 if (r.get("spurt_colour") == "GREEN" and r.get("spurt_mult", 0) >= MULT) else 0) + (1 if r.get("res_zone") else 0) \
        + (2 if r.get("red_solid_vol") else 0) + (1 if r.get("htf_zone") else 0) + (1 if r.get("rsi_peak", 0) > RSI_MIN else 0)


def _status(r, need_mult, is_signal, red_col):
    """Plain-words status of a tracked spurt. 'Red candle' in the rule means a SOLID red candle (body >= 60% of its range) with higher volume than the candle before it."""
    if is_signal:
        return f"<b style='color:{red_col}'>SELL SIGNAL (below)</b>"
    if r["spurt_colour"] != "GREEN":
        return "red spurt - no sell setup"
    if r["since"] == 0:
        return "spurt just happened"
    if r.get("red_solid_vol"):
        miss = []
        if r.get("spurt_mult", 0) < need_mult:
            miss.append(f"spurt below {need_mult:g}x")
        if not r.get("res_zone"):
            miss.append("resistance zone")
        if not r.get("rsi_peak", 0) > RSI_MIN:
            miss.append(f"RSI above {RSI_MIN:g}")
        return "solid red + volume up seen" + (" - missing: " + ", ".join(miss) if miss else "")
    if r["since"] > WAIT:
        return "window over - no solid red candle with higher volume came"
    if r.get("red_any"):
        return f"red candle seen but not solid / volume not higher ({r['since']}/{WAIT})"
    return f"waiting for red candle ({r['since']}/{WAIT})"


_NO_TRK = (f'<h3 style="margin:18px 0 4px">All volume-spurt stocks today (&ge; {TRACK_MULT:g}x)</h3>'
           f'<div class="m">No stock has had a 10-min candle of {TRACK_MULT:g}x volume or more yet today. Every stock that reaches it is listed here and followed for {WAIT} candles. '
           f'The {MULT:g}x in the rule above is only the level needed for a SELL signal.</div>')


def tracker_html(signal_syms=()):
    """The 'all spurt stocks of today' table (from the saved tracker file). '' when there is nothing yet."""
    try:
        import json
        with open(TRACK_PATH) as fh:
            data = json.load(fh)
        rows = list(data.get("rows", {}).values())
        if not rows:
            return _NO_TRK
        rows.sort(key=lambda r: r["spurt_at"], reverse=True)
        try:
            import spurt_sell_x
            lp = spurt_sell_x.live_prices("stock", [r["symbol"] for r in rows])
        except Exception:
            lp = {}
        cc = lambda t: "#089981" if t == "GREEN" else "#F23645" if t == "RED" else "#555"
        out = ""
        import html as _h
        zt = lambda z: "-" if z in (None, "") else _h.escape(str(z))
        pc = lambda v: "-" if v is None else f'<span style="color:{cc("GREEN") if v > 0 else cc("RED") if v < 0 else "#555"}">{v:+.2f}%</span>'
        for i, r in enumerate(rows, 1):
            status = _status(r, MULT, r["symbol"] in signal_syms, "#c62828")
            rsi_style = " style='color:#0a7d33'" if r["rsi_peak"] > RSI_MIN else ""
            out += (f"<tr><td>{i}</td><td>{_tv_link(r['symbol'])}</td><td>{r['spurt_at']}</td><td><b>{r['spurt_mult']:g}x</b></td><td>{r['best_mult']:g}x</td>"
                    f"<td style='color:{cc(r['spurt_colour'])}'><b>{r['spurt_colour']}</b></td><td>{r['since']}</td>"
                    f"<td style='color:{cc(r['first_after'])}'><b>{r['first_after']}</b></td>"
                    f"<td style='white-space:nowrap;text-align:center'>{_draw(r['ohlc'])} <b style='color:{cc(r['latest_colour'])}'>{r['latest_colour']}</b></td>"
                    f"<td class='st' style='white-space:nowrap'>{_strip_svg(r.get('strip') or [], 0, int(r.get('pre') or 0))}</td>"
                    f"<td>{pc(r.get('move_pct'))}</td><td>{pc(r.get('off_high_pct'))}</td><td>{'-' if r.get('vol_vs_spurt') is None else format(r['vol_vs_spurt'], 'g') + 'x'}</td>"
                    f"<td>{'yes' if r['red_solid_vol'] else '-'}</td><td>{zt(r.get('res_zone'))}</td><td>{zt(r.get('htf_zone'))}</td>"
                    f"<td{rsi_style}>{r['rsi_peak']:g}{' &#10004;' if r['rsi_peak'] > RSI_MIN else ''}</td>"
                    f"<td><b>{_trk_score(r)}/6</b></td><td>{r['price']:.2f}</td><td>{_live_c(lp, r['symbol'], r['price'])}</td><td>{status}</td><td>{r.get('seen', '')}</td></tr>")
        return (f'<h3 style="margin:18px 0 4px">All volume-spurt stocks today (&ge; {TRACK_MULT:g}x) &mdash; {len(rows)} stock(s) &middot; updated {data.get("updated", "")}</h3>'
                '<div class="m">Every stock with a 10-min candle of at least that volume multiple today, newest spurt first. "First candle after" and the latest candle show what the price did after the spurt. '
                '"Red solid + volume up" = a solid red candle with higher volume than the one before it has come since the spurt (checklist points 3 and 4). Stocks leave nothing out: '
                'a stock that is no longer in the live top list keeps its last row (see "Seen").</div>'
                '<style>table.trk th,table.trk td{padding:4px 5px;font-size:12px;line-height:1.25}table.trk td.st{min-width:152px;padding:2px 4px;text-align:left}table.trk th{font-size:11.5px}</style><table class="s trk"><tr><th>#</th><th>Symbol</th><th>Spurt at</th><th title="volume multiple of the spurt candle">Spurt x</th><th title="best multiple today">Best x</th><th>Spurt candle</th><th title="candles since the spurt">Since</th>'
                '<th>1st candle after</th><th>Latest</th><th title="10 candles before the spurt (dim), the spurt candle (dot) and the candles after it">10 before + spurt (dot) + next</th><th title="change since the spurt candle closed">Move</th><th title="distance from the highest high since the spurt">From high</th><th title="latest candle volume vs the spurt candle volume">Vol vs spurt</th><th title="solid red candle with higher volume has come">Red + vol up</th><th title="resistance zone touched since the spurt">Res. zone</th><th>' + str(HTF_MIN) + 'm zone</th><th>RSI</th><th>Checklist</th><th title="close of the latest closed 10-min candle">Close</th><th title="last traded price now (refreshed when the page is opened)">Live</th><th>Status</th><th>Seen</th></tr>'
                f'{out}</table><br>')
    except FileNotFoundError:
        return _NO_TRK
    except Exception:
        return ""


# ---------------------------------------------------------------- checklist helpers (points 5 and 6)
def _rsi(close, n):
    """Wilder RSI (same as TradingView ta.rsi)."""
    d = pd.Series(close, dtype=float).diff()
    up = d.clip(lower=0).ewm(alpha=1.0 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1.0 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(100.0).to_numpy()


def _htf_touch(df, sj, L, tol_pct):
    """Point 5: did price touch a live HTF_MIN-minute RESISTANCE zone between the spurt and the red candle? Returns 'lo-hi' or ''.
    The HTF candles are built from the 10-min candles (only complete ones). Information only - never blocks a signal."""
    try:
        import spurt_zones
        step = HTF_MIN // 10
        if step < 2:
            return ""
        ist = df.index.to_series().map(_ist)
        key = ist.dt.date.astype(str) + "_" + (((ist.dt.hour * 60 + ist.dt.minute) - 555) // HTF_MIN).astype(str)
        g = df.groupby(key.to_numpy(), sort=False)
        d = g.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        d = d[g.size().to_numpy() >= step]
        idx = g.apply(lambda x: x.index[0])
        d.index = [idx[k] for k in d.index]
        zs = [z for z in spurt_zones.live_zones(d) if z["type"] == "RESISTANCE"]
        if not zs:
            return ""
        tol = tol_pct / 100.0
        hi_, lo_ = df["high"].to_numpy(dtype=float), df["low"].to_numpy(dtype=float)
        hit = ""
        for k in range(sj, L + 1):
            for z in zs:
                if lo_[k] <= z["hi"] * (1 + tol) and hi_[k] >= z["lo"] * (1 - tol):
                    hit = f"{z['lo']:g}-{z['hi']:g}"
        return hit
    except Exception:
        return ""


# ---------------------------------------------------------------- the rule
def check(df, sym, day_chg=None):
    """df = 10-min candles, last row = latest CLOSED candle. Returns a signal dict or None. Never raises."""
    if not ENABLED:
        return None
    try:
        n = len(df)
        if n < AVG_LEN + 3:
            return None
        o = df["open"].to_numpy(dtype=float)
        h = df["high"].to_numpy(dtype=float)
        l = df["low"].to_numpy(dtype=float)
        c = df["close"].to_numpy(dtype=float)
        v = df["volume"].to_numpy(dtype=float)
        L = n - 1

        def red(i):
            rng = h[i] - l[i]
            return c[i] < o[i] and rng > 0 and (o[i] - c[i]) / rng * 100.0 >= BODY_PCT and v[i] > v[i - 1]

        if not red(L):
            return None
        sj, smult = None, None
        for j in range(L - 1, max(L - WAIT, AVG_LEN) - 1, -1):          # latest spurt first
            avg = v[j - AVG_LEN:j].mean()
            if avg <= 0:
                continue
            m = v[j] / avg
            t = _ist(df.index[j])
            if m >= MULT and c[j] > o[j] and not (SKIP_OPEN and t.hour == 9 and t.minute == 15):
                sj, smult = j, m
                break
        if sj is None:
            return None
        for m_ in range(sj + 1, L):                                      # one signal per spurt
            if red(m_):
                return None
        entry, sl = float(c[L]), float(h[L])
        risk = sl - entry
        if risk <= 0 or entry <= 0:
            return None
        zone_txt = ""
        try:                                                             # point 4: resistance zone touched since the spurt
            import spurt_zones
            zone_txt = spurt_zones.resistance_touch(df, sj, L, ZONE_NEAR_PCT) or ""
        except Exception:
            zone_txt = ""
        if NEED_ZONE and not zone_txt:
            return None
        rsi_peak = float(np.nanmax(_rsi(c, RSI_LEN)[sj:L + 1]))                  # point 6: highest RSI from the spurt candle to the red candle
        if NEED_RSI and not rsi_peak > RSI_MIN:
            return None
        htf_txt = _htf_touch(df, sj, L, ZONE_NEAR_PCT)                           # point 5 (information)
        score = 3 + (1 if zone_txt else 0) + (1 if rsi_peak > RSI_MIN else 0) + (1 if htf_txt else 0)   # points 1, 3, 4 are met by construction
        return {
            "symbol": sym,
            "candle": _ist(df.index[L]).strftime("%H:%M"),
            "entry": round(entry, 2), "sl": round(sl, 2),
            "t1": round(entry - risk * R1, 2), "t2": round(entry - risk * R2, 2),
            "risk_pct": round(risk / entry * 100, 2),
            "spurt_candle": _ist(df.index[sj]).strftime("%H:%M"),
            "spurt_mult": round(float(smult), 1),
            "candles_after_spurt": int(L - sj),
            "red_vol_vs_prev": round(float(v[L] / v[L - 1]), 2) if v[L - 1] > 0 else None,
            "day_chg_pct": day_chg,
            "res_zone": zone_txt,
            "rsi_peak": round(rsi_peak, 1), "htf_zone": htf_txt, "score": f"{score}/6",
            "sig_ohlc": _pack(o, h, l, c, L),
        }
    except Exception:
        return None


# ---------------------------------------------------------------- log + phone alert
def _load_seen(today):
    _seen.update(date=today, keys=set(), count=0)
    try:
        if os.path.exists(HIST_PATH):
            old = pd.read_csv(HIST_PATH, usecols=["date", "candle", "symbol"], dtype=str)
            for d, c_, s in old[old["date"] == today].itertuples(index=False):
                _seen["keys"].add((c_, s))
    except Exception as e:
        log.warning(f"SpurtSell: could not read history ({e})")


def _alert(new, now_ist):
    topic = os.environ.get("SPURT_SELL_NTFY_TOPIC", "").strip()
    if not topic or os.environ.get("SPURT_SELL_NTFY", "true").strip().lower() != "true":
        return
    if _seen["count"] >= MAX_PER_DAY:
        return
    lines = [f"{r['symbol']}  SELL {r['entry']:.2f}  SL {r['sl']:.2f}  T1 {r['t1']:.2f}  T2 {r['t2']:.2f}  (spurt {r['spurt_mult']:g}x at {r['spurt_candle']}; resistance {r.get('res_zone') or '-'}; RSI {r.get('rsi_peak', '-')}; {HTF_MIN}m zone {r.get('htf_zone') or 'no'}; checklist {r.get('score', '-')})"
             for r in new[:12]]
    extra = f"\n+{len(new) - 12} more on /spurt_sell" if len(new) > 12 else ""
    try:
        import requests
        resp = requests.post(f"https://ntfy.sh/{topic}", data=("\n".join(lines) + extra).encode("utf-8"), timeout=8,
                             headers={"Title": f"SELL after volume spurt ({new[0]['candle']} candle closed)", "Priority": "high",
                                      "Tags": "chart_with_downwards_trend"})
        if resp.status_code >= 300:
            log.warning(f"SpurtSell ntfy refused: {resp.status_code} {resp.text[:150]}")
            return
    except Exception as e:
        log.warning(f"SpurtSell ntfy failed: {e}")
        return
    _seen["count"] += 1
    log.info(f"SpurtSell alert sent for {len(new)} stock(s)")


def process(hits, now_ist):
    """Called once per volume-spurt cycle with every signal found. Logs the new ones and sends the phone alert. Never raises."""
    try:
        if not ENABLED or not hits:
            return
        today = now_ist.strftime("%Y-%m-%d")
        if _seen["date"] != today:
            _load_seen(today)
        new = []
        for r in sorted(hits, key=lambda r: -r["spurt_mult"]):
            k = (r["candle"], r["symbol"])
            if k in _seen["keys"]:
                continue
            _seen["keys"].add(k)
            new.append(r)
        if not new:
            return
        run = now_ist.strftime("%H:%M:%S")
        os.makedirs(RESULTS_DIR, exist_ok=True)
        new_file = not os.path.exists(HIST_PATH)
        if not new_file:                                                # file written before the zone column existed: upgrade in place
            with open(HIST_PATH) as fh:
                first = fh.readline().strip().split(",")
            if first != COLS:
                pd.read_csv(HIST_PATH).reindex(columns=COLS).to_csv(HIST_PATH + ".tmp", index=False)
                os.replace(HIST_PATH + ".tmp", HIST_PATH)
        pd.DataFrame([{"date": today, "run": run, **{c_: r.get(c_) for c_ in COLS[2:]}} for r in new], columns=COLS).to_csv(
            HIST_PATH, mode="a", header=new_file, index=False)
        log.info(f"SpurtSell: {len(new)} new SELL signal(s): {[r['symbol'] for r in new]}")
        _alert(new, now_ist)
        try:                                                            # ADDED: virtual (paper) trades for every new signal - spurt_paper.py
            import spurt_paper
            spurt_paper.open_trades(new, now_ist)
        except Exception as e:
            log.warning(f"Spurt paper open failed: {e}")
    except Exception as e:
        log.warning(f"SpurtSell process failed: {e}")


# ---------------------------------------------------------------- page
_SORT_JS = """<script>
document.querySelectorAll('table.s th').forEach(function(th,i){th.style.cursor='pointer';th.title='click to sort';
th.addEventListener('click',function(){var t=th.closest('table'),b=t.tBodies[0],rows=Array.prototype.slice.call(b.rows),
asc=th.dataset.asc!=='1';th.dataset.asc=asc?'1':'0';
function val(r){var x=r.cells[i].innerText.replace(/[,%x]/g,'').trim();var n=parseFloat(x);return isNaN(n)?x.toLowerCase():n;}
rows.sort(function(a,b2){var p=val(a),q=val(b2);if(p<q)return asc?-1:1;if(p>q)return asc?1:-1;return 0;});
rows.forEach(function(r){b.appendChild(r);});});});
(function(){function fx(){var bn=document.querySelector('div[style*="rb 1s"]');var h=bn?bn.offsetHeight:0;document.querySelectorAll('table.s th').forEach(function(th){th.style.position='sticky';th.style.top=(h-3)+'px';th.style.zIndex=5;});}fx();window.addEventListener('resize',fx);})();
</script>"""


_RULES_BANNER = ('<style>@keyframes rb{0%,49%{background:#fff176;color:#d50000}50%,100%{background:#d50000;color:#fff}}</style>'
                 '<div style="animation:rb 1s step-end infinite;position:sticky;top:0;z-index:99999;background:#fff176;border:3px solid #d50000;border-radius:6px;'
                 'padding:10px 14px;margin:0 0 14px;color:#d50000;font-weight:900;font-size:22px;line-height:1.5;text-align:center">'
                 '1. DON\'T RUSH &nbsp;&nbsp;|&nbsp;&nbsp; 2. DON\'T TRADE WITHOUT VALID SET UP &nbsp;&nbsp;|&nbsp;&nbsp; 3. 1 VALID SETUP ENOUGH TO EARN MONEY&nbsp;&nbsp;|&nbsp;&nbsp; 4. DON&#39;T BE EMOTIONAL ATTACHED</div>')



def page_html(date=None):
    import html as _h
    try:
        head = ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<meta http-equiv="refresh" content="60"><title>Spurt SELL</title>'
                '<style>body{font-family:Arial,sans-serif;margin:12px;background:#fafafa}table{border-collapse:collapse;background:#fff}'
                'th,td{border:1px solid #ddd;padding:6px 8px;text-align:right;font-size:14px}th{background:#222;color:#fff}'
                'td:nth-child(2){text-align:left}.m{color:#555;font-size:13px;margin:6px 0 12px}</style></head><body>' + _RULES_BANNER)
        rule = (f"Rule: a green 10-min candle with volume &ge; {MULT:g}x the average of the previous {AVG_LEN} candles (09:15 candle {'ignored' if SKIP_OPEN else 'included'}), then within "
                f"{WAIT} candles a solid red candle (body &ge; {BODY_PCT:g}% of its range) with higher volume than the candle before it. "
                f"SELL at that red candle's close, stop loss = its high, targets = {R1:g}x and {R2:g}x the risk. One signal per spurt. "
                f"Checklist: points 1-4 above plus RSI above {RSI_MIN:g} since the spurt are required; the {HTF_MIN}m resistance zone is shown as a bonus (score 5/6 or 6/6). "
                f"{'The price must also have touched a resistance order-block zone since the spurt (10-min chart, ALL IN ONE PRO logic). ' if NEED_ZONE else ''}"
                "Paper/information only - the scanner places no orders.")
        sig_today = set()
        try:
            if os.path.exists(HIST_PATH):
                _t = pd.read_csv(HIST_PATH, usecols=["date", "symbol"], dtype=str)
                sig_today = set(_t[_t["date"] == datetime.now(IST).strftime("%Y-%m-%d")]["symbol"])
        except Exception:
            pass
        trk = tracker_html(sig_today) if date in (None, "", datetime.now(IST).strftime("%Y-%m-%d")) else ""
        if not os.path.exists(HIST_PATH):
            return head + f'<h2>Spurt SELL</h2><div class="m">{rule}</div>{trk}<p>No signal yet. It fills when a stock matches the rule during market hours.</p>{_SORT_JS}</body></html>'
        df = pd.read_csv(HIST_PATH)
        if df.empty:
            return head + f'<h2>Spurt SELL</h2><div class="m">{rule}</div>{trk}<p>No signal yet.</p>{_SORT_JS}</body></html>'
        for _c in ("rsi_peak", "htf_zone", "score", "sig_ohlc"):                   # rows logged before the checklist columns existed
            if _c not in df.columns:
                df[_c] = ""
        if "res_zone" not in df.columns:                              # rows logged before the zone condition existed
            df["res_zone"] = ""
        dates = sorted(df["date"].unique(), reverse=True)
        d = date if date in dates else dates[0]
        x = df[df["date"] == d].sort_values(["candle", "spurt_mult"], ascending=[False, False])
        x = x.assign(_sc=x["score"].astype(str).str[:1]).sort_values("_sc", ascending=False, kind="stable")
        col = lambda v: "#0a7d33" if (v or 0) > 0 else "#c62828" if (v or 0) < 0 else "#555"
        pct = lambda v: "-" if v != v or v is None else f'<span style="color:{col(v)}">{v:+.2f}%</span>'
        rows = "".join(
            f"<tr><td>{r.candle}</td><td><b>{_h.escape(str(r.symbol))}</b></td><td>{r.entry:.2f}</td><td style='color:#c62828'>{r.sl:.2f}</td>"
            f"<td style='color:#0a7d33'>{r.t1:.2f}</td><td style='color:#0a7d33'>{r.t2:.2f}</td><td>{r.risk_pct:.2f}%</td>"
            f"<td>{r.spurt_candle}</td><td><b>{r.spurt_mult:g}x</b></td><td>{int(r.candles_after_spurt)}</td><td>{pct(r.day_chg_pct)}</td><td>{_h.escape(str(r.res_zone)) if r.res_zone == r.res_zone and r.res_zone else '-'}</td>"
            f"<td style='white-space:nowrap;text-align:center'>{_draw(r.sig_ohlc)}</td>"
            f"<td>{'-' if r.rsi_peak != r.rsi_peak or r.rsi_peak == '' else r.rsi_peak}</td><td>{_h.escape(str(r.htf_zone)) if r.htf_zone == r.htf_zone and r.htf_zone else '-'}</td><td><b>{_h.escape(str(r.score)) if r.score == r.score and r.score else '-'}</b></td></tr>"
            for r in x.itertuples())
        opts = "".join(f'<option value="{_h.escape(v)}"{" selected" if v == d else ""}>{_h.escape(v)}</option>' for v in dates)
        return (head + f'<h2>Spurt SELL &mdash; {_h.escape(d)}</h2>'
                f'<form method="get" class="m">Day <select name="date">{opts}</select> <button>Show</button> &middot; {len(x)} signals &middot; '
                f'click any heading to sort &middot; <a href="/spurt_sell.csv">download CSV (all days)</a> &middot; <a href="/spurt_paper">paper trading results</a> &middot; <a href="/spurt10">live /spurt10</a></form>'
                f'<div class="m">{rule}</div>{trk}'
                '<h3 style="margin:18px 0 4px">SELL signals (all checklist points met)</h3>'
                '<table class="s"><tr><th>Signal candle</th><th>Symbol</th><th>SELL entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th>'
                '<th>Risk %</th><th>Spurt candle</th><th>Spurt multiple</th><th>Candles after spurt</th><th>Day %</th><th>Resistance zone touched</th><th>Signal<br>candle</th><th>RSI peak</th><th>30m resistance zone</th><th>Checklist</th></tr>'
                f'{rows}</table>{_SORT_JS}</body></html>')
    except Exception as e:
        return f"<p>Spurt SELL page error: {_h.escape(str(e))}</p>"
