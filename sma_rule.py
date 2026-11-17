"""SMA 9/21/60 DISTANCE RULE for every scanner signal (ADDED 17 Nov 2026) - the same rule as the TradingView indicator ALL IN ONE PRO.

A signal that fires while price is still close to its 9-SMA usually just bounces off the average; the ones that fire far from it work.
Checked on the SIGNAL candle (close vs the SMA 9 / 21 / 60 of the closes of the same candles the scanner reads):
    SELL : allowed if the close is below ALL THREE SMAs (with the downtrend),  or at least PCT % ABOVE the NEAREST of the 9/21/60 SMAs.
    BUY  : allowed if the close is above ALL THREE SMAs (with the uptrend),  or at least PCT % BELOW the NEAREST of the 9/21/60 SMAs.
    (CHANGED 17 Nov: only the nearest SMA counts; no separate 9-SMA test.)
While an SMA is not yet known (first candles) the signal is allowed. Defaults need no settings. Optional:
  SMA_RULE=true|false   SMA_RULE_PCT=2   SMA_RULE_TREND=true|false (false = the with-the-trend exception is off)
Read-only: it only filters signals the scanners already find. Never raises.
"""
import os

import numpy as np
import pandas as pd

ON = os.environ.get("SMA_RULE", "true").strip().lower() != "false"
TREND_OK = os.environ.get("SMA_RULE_TREND", "true").strip().lower() != "false"
try:
    PCT = float(os.environ.get("SMA_RULE_PCT", "2") or "2")
except ValueError:
    PCT = 2.0


def _sma(c, n):
    return pd.Series(c).rolling(n, min_periods=n).mean().to_numpy()


def ok_arrays(close):
    """-> (ok_buy, ok_sell): boolean arrays, one value per candle of `close`."""
    try:
        c = np.asarray(close, dtype=float)
        n = len(c)
        if not ON or n == 0:
            return np.ones(n, dtype=bool), np.ones(n, dtype=bool)
        s9, s21, s60 = _sma(c, 9), _sma(c, 21), _sma(c, 60)
        known = ~np.isnan(s60)
        with np.errstate(invalid="ignore"):
            d = np.vstack([c / s9 - 1.0, c / s21 - 1.0, c / s60 - 1.0]) * 100.0
            near = d[np.nanargmin(np.where(np.isnan(d), np.inf, np.abs(d)), axis=0), np.arange(n)]     # signed % to the NEAREST SMA (+ = above it)
            above = (c > s9) & (c > s21) & (c > s60)
            below = (c < s9) & (c < s21) & (c < s60)
        ok_sell = ~known | (near >= PCT) | (TREND_OK & below)
        ok_buy = ~known | (near <= -PCT) | (TREND_OK & above)
        return ok_buy, ok_sell
    except Exception:
        n = len(close) if close is not None else 0
        return np.ones(n, dtype=bool), np.ones(n, dtype=bool)


def gap_pct(close, i):
    """% the close of candle i is above (+) / below (-) its 9-SMA, or None."""
    try:
        c = np.asarray(close, dtype=float)
        s = _sma(c, 9)[i]
        return None if np.isnan(s) or s <= 0 else round((c[i] / s - 1.0) * 100.0, 2)
    except Exception:
        return None
