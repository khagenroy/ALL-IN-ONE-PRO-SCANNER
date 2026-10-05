# ALL IN ONE PRO - Live Scanner (Intraday + Swing)

A separate project from dhan-bridge - its own repo, its own Render service,
shares no code or imports with the live trading bridge. Scans your 1000-stock
universe with both strategy engines from
`ALL_IN_ONE_PRO_STOCK_COMMODITY_LIVE_NO_SCANNER_WITH_OB_ALERT.pine`
and reports **confirmed signals only**:

- **Section A** ("PA Toolkit Dual Strategy") - liquidity sweep + order block
  mitigation: SWEEP_BUY / SWEEP_SELL / OB_BUY / OB_SELL.
- **Section B** ("UNI+PA+RSI+Pattern2") - Keltner Channel/SMC liquidity sweep
  (SSL/BSL) + RSI/candlestick pattern checklist.

It runs as **two scans** in one service, split by how often their data changes:

| | Intraday | Swing / Positional |
|---|---|---|
| Timeframes | 10m, 1H, 4H | 1D, 1W, 1M |
| Runs | every 10 min, 9:15-15:30 IST, Mon-Fri (fixed schedule, never overlaps) | once a day after 16:00 IST, Mon-Fri; plus one seed run on a fresh deploy |
| Page | `/scanner` | `/swing` |
| CSVs | `/scanner/signals.csv`, `/scanner/signals_b.csv` | `/swing/signals.csv`, `/swing/signals_b.csv` |
| Dhan calls | ~2 per symbol per cycle | 1 per symbol per day |

Each page links to the other. Run times on the page are shown in IST.

## Morning-gainers strength study (`/gainers`)

A third, separate job (`gainers_study.py`): read-only research, no signals, no orders. It answers "which stocks in the morning top-gainers list keep their strength all day, and how does volume relate to that?".

- Pulls ~30 calendar days of 5-minute candles per stock (1 Dhan call per stock), then for **every** trading day takes the top 20 by % change vs previous close at **09:30 and 10:00 IST** (price >= 20, >= Rs 0.5 cr traded by then) and follows each through the day.
- Each pick gets a verdict: **STRONG** (closed at/above its pick price, above VWAP, >=70% of later bars above VWAP), **HELD** (above VWAP, kept >= half its gain), **FADED** (green but gave most back), **REVERSED** (closed at/below previous close).
- Also reports RVOL (morning volume vs the stock's own 10-day average for the same window), % at 11:00/12:00/13:00/14:00/close, further high and worst dip after the pick, volume after vs before the pick.
- Page: `/gainers` (CSV links on it: all picks, latest day, per-stock summary). Runs once a day after 16:30 IST, plus a seed run on a fresh deploy (only outside market hours). Manual run: `python gainers_study.py`.
- It rebuilds everything from Dhan's history each run, so nothing depends on files surviving a redeploy.
- Env (all optional): `GAINERS_STUDY_AUTORUN` (default true), `GAINER_TOP_N` (20), `GAINER_PICK_TIMES` ("09:30,10:00"), `GAINER_HISTORY_DAYS` (30), `GAINER_MIN_PRICE` (20), `GAINER_MIN_TURNOVER_CR` (0.5).

## What this does NOT do

- Does not place orders, does not touch dhan-bridge in any way.
- Does not track open positions, SL trailing, or T1-T6 target cascades -
  those only matter for a live trade. This only reports "would this
  strategy's entry condition fire right now", same as glancing at the chart.
- Entry/SL/T1-T6 shown with each signal are REFERENCE values (same formulas
  as the Pine script), not an active trade being managed.
- No OB Zone Watch / support-resistance zone columns (removed 2026-10-05 at
  Khagen's request - confirmed signals only).
- Does not port the "Force Signal [TEST MODE]" toggle or the RSI-divergence
  calculation from Section B - both are dead/unused in the real script.

## Files

- `app.py` - Flask app, `/scanner` and `/swing` pages, the two background scan loops.
- `live_scanner.py` - Dhan fetching (rate-limited, threaded), bar building, runs both sections, writes results. `python live_scanner.py [intraday|swing|both]`.
- `all_in_one_scanner.py` - Section A port (ZigZag/CHoCH, order block mitigation, liquidity sweeps). Timeframe-agnostic.
- `section_b.py` - Section B port. Timeframe-agnostic.
- `scrip_master.py` - standalone copy of dhan-bridge's symbol resolver (read-only, independent copy).
- `market_cap_universe.csv` - same cleaned 1000-stock list used by the EOD scanner.
- `cache/daily/<SYMBOL>.csv` - daily-history cache, refreshed once per calendar day. **Don't delete it** - if Dhan rejects a refresh it is the fallback.
- `cache/h60_old/<SYMBOL>.csv` - older half of the 60-minute history (historical, re-fetched only when >3 days old).

## Deploying

1. Push these files to your GitHub repo (same filenames overwrite the old ones).
2. Render **Web Service**: build `pip install -r requirements.txt`, start `gunicorn app:app --workers 1 --threads 4 --timeout 90` (keep **1 worker** - the scan loops live in the web process).
3. Env vars: `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` (read-only Data API scope is enough).
4. On a fresh deploy the swing scan seeds itself (a few minutes); the intraday scan starts at the next 9:15.

## Env vars

- `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` - required.
- `LIVE_SCANNER_AUTORUN` - default `true` (intraday loop).
- `SWING_SCANNER_AUTORUN` - default `true` (swing loop).
- `LIVE_SCANNER_INTERVAL_SECONDS` - default `600` (intraday cycle length).
- `MAX_REQUESTS_PER_SECOND` - default `4`, the STARTING rate. One self-tuning limiter covers every Dhan call in the process: each 429 from Dhan slows it down (floor 1.5/s, widened once per burst), and a long run of successes speeds it back up toward this value. Seeing a few "Dhan rate limit hit - slowing request rate" lines in the logs is normal; if it stays at the floor, lower this value.
- `SCAN_WORKERS` - default `4` (threads overlapping network waits; the limiter still caps the rate).
- `MAX_SYMBOLS` - default `1000`.
- `TEST_SYMBOL_LIMIT` - default `0` (off); set for a dry run. **Don't leave it set on the service.**

## Dry run / manual commands (Render Shell)

```bash
TEST_SYMBOL_LIMIT=10 python live_scanner.py            # intraday dry run
TEST_SYMBOL_LIMIT=10 python live_scanner.py swing      # swing dry run
python live_scanner.py swing                           # full swing scan now
```

Avoid manual FULL scans during market hours while the service autorun is
going: a Shell run is a separate process with its own rate limiter, so the
two would add up against Dhan's request limit.

## Timing

- Intraday cycle: ~2 calls/symbol x 1000 symbols at 4 req/s = ~8.5 min, so it fits a 10-minute cycle - IF Dhan allows 4 req/s. The limiter slows itself down when Dhan returns 429s (2026-10-05: the daily endpoint did at a fixed 4/s), and then a cycle takes longer (at 2.5 req/s: ~13 min); the next cycle just starts as soon as one ends. The first cycle after a fresh deploy (or every ~3 days) also refetches the older 60-minute chunk (~3 calls/symbol, ~12.5 min) and simply starts the next cycle right after.
- Swing scan: ~1000 daily calls = ~4-5 min once a day.
- Compute (both sections) is only ~1 minute per 1000 symbols - the request rate is the limit.

## Closed bars only

The intraday scan only evaluates **closed** candles, matching the Pine script (alerts fire on `barstate.isconfirmed`). The candle still forming is dropped, so a signal appears one bar later than a live chart might flicker it, but it will not disappear when the bar closes. A 10m signal shows up in the first scan after that candle closes; 1H and 4H likewise. The Bar Time column is the candle's START time in UTC (03:45 = 09:15 IST).

## Reading the output

Each page has two tables (Section A, Section B) with a **TF** column. A signal means the entry condition fired on the most recent bar of that timeframe. Section B's "source" column shows which mechanism fired: `SSL_SWEEP`/`BSL_SWEEP` (liquidity sweep-and-reclaim) or `RSI_CHECKLIST` (RSI + reversal candle + local extreme), plus that bar's RSI.

## Dhan notes

- `compute_section_a` needs 90 bars on whatever timeframe it is given. That is why the daily pull is ~10 years (Monthly) and the 60-minute history is ~165 days (4H). Confirmed against live Dhan.
- **2026-10-05:** Dhan's `/charts/historical` started returning `400 DH-905` ("Missing required fields, bad values for parameters") for ~17% of symbols (167 of 999 - e.g. RELIANCE, INFY, SUNPHARMA, LICI) on a request identical to ones that succeed for other symbols, and that worked for these same symbols the day before. Cause unknown, on Dhan's side. The swing scan uses that symbol's older cached daily history if it has one, otherwise skips it (shown as "skipped (Dhan returned no daily data)" on `/swing`). The intraday scan never calls the daily endpoint, so it is unaffected. If it persists, raise it with Dhan support.
- **429 (rate limited):** retried up to 6 times with growing waits and does not count as a failed attempt; a symbol is only skipped if Dhan keeps returning 429 through all of them (the log says "rate-limited (429)", not a data problem). 400 errors are never retried; other HTTP errors are retried 3 times.
- The intraday/swing split (this version) was tested against mocked Dhan responses only; the first live run is the real test.
- Dhan's per-request date range cap on `/charts/intraday` is assumed ~90 days (unverified), which is why each 60-minute chunk is <=85 days.

## Volume-spurt backtest (`/volspurt`)

Research only - places no orders and does not touch the Section A / B signal logic.

A "spurt" is a 5-minute candle (starting 09:30-14:25 IST) whose volume is far above that stock's
average for the **same time of day over the previous 10 days**, on a candle that actually moved.
The study replays every spurt in the last ~60 calendar days for the whole stock list and reports which
rules would have made money after stop-loss, targets and costs.

- Rules tested: FOLLOW (trade the candle's direction) and FADE (trade against it) x volume 3/4/5/6/8/10x x candle move 0.3/0.6/1.0% x target 1R/2R/3R.
- Entry next candle's open; SL at the signal candle's far end; exit at target, SL or 15:25; stop wins ties; 0.05% round-trip cost; one trade per stock per day per rule.
- For each rule: trades, win rate, average net R, profit factor, total R, and average R in the first vs second half of the period. "Positive in both halves" is the quick luck filter.
- The page also splits the three best rules by time of day, VWAP side and up/down candle.

Runs once a day after 17:30 IST (Mon-Fri), and once as a seed when there are no results yet (outside market hours, after the gainers study has produced its file). Results live in `results/` and are wiped on every Render redeploy, so the seed run repeats after a redeploy.

Pages / downloads: `/volspurt`, `/volspurt/grid.csv` (every rule), `/volspurt/signals.csv` (every spurt with the R each rule would have made).

Environment variables (all optional): `VOLSPURT_STUDY_AUTORUN` (true), `VOLSPURT_HISTORY_DAYS` (60), `VOLSPURT_COST_PCT` (0.05), `VOLSPURT_MIN_PRICE` (20), `VOLSPURT_MIN_BAR_TURNOVER_CR` (0.02), `VOLSPURT_MIN_TRADES` (150, minimum trades for a rule to be ranked).

Run by hand: `python volume_spurt_study.py` (or `TEST_SYMBOL_LIMIT=20 python volume_spurt_study.py` for a quick dry run).

Treat ~40 trading days as one market regime: the best rules are candidates to watch live, not proof.
