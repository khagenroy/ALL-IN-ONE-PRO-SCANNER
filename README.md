# ALL IN ONE PRO - Live Scanner (Section A + Section B)

A separate project from dhan-bridge - its own repo, its own Render service,
shares no code or imports with the live trading bridge. Scans your 1000-
stock universe on 10-minute candles and reports signals from BOTH strategy
engines inside `ALL_IN_ONE_PRO_STOCK_COMMODITY_LIVE_NO_SCANNER_WITH_OB_ALERT.pine`:

- **Section A** ("PA Toolkit Dual Strategy") - liquidity sweep + order block.
- **Section B** ("UNI+PA+RSI+Pattern2") - Keltner Channel/SMC liquidity sweep
  (SSL/BSL) + RSI/candlestick pattern checklist.

Updates every 10 minutes during market hours (9:15-15:30 IST, Mon-Fri).

## What this does NOT do

- Does not place orders, does not touch dhan-bridge in any way.
- Does not track open positions, SL trailing, or T1-T6 target cascades -
  those only matter for a live trade. This only reports "would this
  strategy's entry condition fire right now", same as glancing at the chart.
- Entry/SL/T1-T6 shown alongside each signal are REFERENCE values (same
  formulas as the Pine script), not an active trade being managed.
- Does not port the "Force Signal [TEST MODE]" toggle or the RSI-divergence
  calculation from Section B - both are dead/unused in the real script (the
  divergence calc is never read by any signal or alert there either;
  confirmed by searching every reference to it).

## Files

- `app.py` - Flask app, `/scanner` page, 10-min autorun scheduler (market hours only).
- `live_scanner.py` - fetches 5-min Dhan intraday data, builds 10-min candles, runs both sections, writes results.
- `all_in_one_scanner.py` - Section A port (ZigZag/CHoCH, order blocks, liquidity sweeps).
- `section_b.py` - Section B port (Keltner Channel, SSL/BSL sweep-and-reclaim, RSI + candlestick checklist).
- `scrip_master.py` - standalone copy of dhan-bridge's symbol resolver (read-only, independent copy).
- `market_cap_universe.csv` - same cleaned 1000-stock list used by the EOD scanner (copy here if you update it there).

## IMPORTANT - not run against the live Dhan API yet

Same disclosure as the other two scanners in this project: this was built
from Dhan's documented `/charts/intraday` endpoint, not a live test call
(this sandbox has no network access to api.dhan.co). Both sections were
validated against synthetic price data (checked signals fire on the right
bars with correct entry/SL/target math), not against real market data.
Dry-run first:

```bash
TEST_SYMBOL_LIMIT=5 DHAN_CLIENT_ID=... DHAN_ACCESS_TOKEN=... python live_scanner.py
```

Check `results/latest.html` looks sane (real prices, plausible signals)
before trusting a full run or turning on autorun.

## Deploying

1. New GitHub repo, push these files.
2. New Render **Web Service** pointed at it.
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn app:app --workers 1 --threads 4 --timeout 90`
5. Same `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` env vars as dhan-bridge (read-only Data API scope is enough).
6. Once live, `/scanner` shows the latest results; a background thread runs the scan automatically every 10 minutes during market hours - no Cron Job needed, same zero-extra-cost pattern as the EOD scanner's autorun.

## Env vars

- `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` - required.
- `LIVE_SCANNER_AUTORUN` - default `true`.
- `LIVE_SCANNER_INTERVAL_SECONDS` - default `600` (10 min).
- `MAX_SYMBOLS` - default `1000`.
- `TEST_SYMBOL_LIMIT` - default `0` (off); set for a dry run.

## Reading the output

Three tables on `/scanner`:
- **Live Signals (Section A)** - a sweep or OB-mitigation entry just fired on the most recent closed 10-min bar (SWEEP_BUY / SWEEP_SELL / OB_BUY / OB_SELL), with reference entry/SL/T1-T3.
- **OB Zone Watch** - price is currently sitting inside a live Section-A order-block zone (wicked in) but hasn't closed back through to confirm a mitigation signal yet - a heads-up, not a signal.
- **Live Signals (Section B)** - a BUY/SELL from Section B's own engine, with a "source" column telling you which of its two mechanisms fired: `SSL_SWEEP`/`BSL_SWEEP` (liquidity-level sweep-and-reclaim against the Keltner Channel) or `RSI_CHECKLIST` (RSI oversold/overbought + a clean reversal candle + local price extreme), plus that bar's RSI value and reference entry/SL/T1-T3.

CSV downloads: `/scanner/signals.csv` (Section A), `/scanner/signals_b.csv` (Section B), `/scanner/watch.csv` (OB zone watch).
