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

## Spurt-pullback backtest (`/spurtpb`)

Research only - places no orders and does not touch the Section A / B signal logic.

Tests the idea "pick the volume-spurt stocks at 10:00, then trade the retracement":
- **Watchlist at 10:00, two definitions:** (1) NSE-style, matching NSE's "Volume Spurts" report: volume traded so far today (09:15-10:00) at least 1x/2x/3x/5x the stock's average full-day volume over the previous 5 sessions; (2) a single 5-minute candle between 09:15 and 10:00 with volume at least 3x/5x/8x that stock's same-time-of-day 10-day average and body at least 0.5%. Both need a move of at least 1% from the day's extreme to the furthest point by 10:00.
- **Both sides:** an UP spurt is bought on the pullback (LONG), a DOWN spurt is sold on the bounce (SHORT). Reported separately and combined.
- **Entry (10:00-14:00):** the first time price retraces 38.2% / 50% / 61.8% of the move. SL at the move's start. Targets 1R, 2R, or RETEST (back to the extreme before the pullback). Exit at target, SL or 15:25. Optional VWAP filter. 0.05% round-trip cost.
- Page shows each rule's trades, win rate, average net R, profit factor, total R, and average R in the first vs second half of the period.

Runs once a day after 18:30 IST (Mon-Fri) and once as a seed when no results exist (outside market hours, after the volume-spurt backtest). Pages / downloads: `/spurtpb`, `/spurtpb/grid.csv`, `/spurtpb/trades.csv`.

Env vars (optional): `SPURTPB_STUDY_AUTORUN` (true), `SPURTPB_HISTORY_DAYS` (60), `SPURTPB_COST_PCT` (0.05), `SPURTPB_MIN_PRICE` (20), `SPURTPB_MIN_SPURT_MOVE` (0.5), `SPURTPB_MIN_IMPULSE_PCT` (1.0), `SPURTPB_MIN_TRADES` (150).

Run by hand: `python spurt_pullback_study.py` (or `TEST_SYMBOL_LIMIT=20 python spurt_pullback_study.py`).

## SMA200 support / rejection (`/sma200`)

Research and setup list only - places no orders and does not touch the Section A / B signal logic.

- **LONG (support):** SMA200 rising, price above it for the previous 5 closes, a bar dips to the SMA200 zone (within 0.25 ATR, no more than 1 ATR through it) and closes back above it on a bullish candle.
- **SHORT (rejection):** the mirror - SMA200 falling, price below it, a bar rallies to the zone and closes back below it on a bearish candle.
- **Volume support:** the touch bar's volume as a multiple of the previous 20 bars' average (tested at any / 1x / 1.5x / 2x). One signal per side per 10 bars per stock.
- **Timeframes:** 1D, 1W, 1H, 10m are backtested; 4H is setups-only (not enough history). 1M is impossible (only ~120 monthly bars exist).
- **Backtest trade:** entry next bar's open, stop beyond the touch bar (+0.1 ATR), target 1R/2R/3R, exit at target/stop/hold limit (1D 20 bars, 1W 12, 1H 30, 10m 40); stop wins ties; costs 0.15% (1D/1W) or 0.05% (1H/10m) per round trip.
- **Page:** setups right now on every timeframe (last closed bar), then the ranked rules (win rate, average net R, profit factor, first/second half, years positive).
- Daily/weekly SHORTs need futures. 1H covers ~5 months and 10m ~2 months, so intraday results are thin.

Runs once a day after 19:30 IST (Mon-Fri), and once as a seed when no results exist (outside market hours, after the spurt-pullback study). Pages / downloads: `/sma200`, `/sma200/live.csv`, `/sma200/grid.csv`, `/sma200/trades.csv`.

Env vars (optional): `SMA200_STUDY_AUTORUN` (true), `SMA200_MIN_TRADES` (150), `SMA200_COST_DAILY` (0.15), `SMA200_COST_INTRADAY` (0.05), `SMA200_INTRADAY_HISTORY_DAYS` (60).

Run by hand: `python sma200_study.py` (or `TEST_SYMBOL_LIMIT=20 python sma200_study.py`).

## SMA200 live setups (`/sma200now`)

The same SMA200 support / rejection rule, but live: it rides on the existing intraday scan (10m, 1H, 4H, every 10 minutes during market hours) and on the swing scan (1D, 1W, daily after 16:00), using the bars those scans already download - **no extra Dhan calls**. Each cycle it checks the last 3 closed 10m candles, the last 2 for 1H/4H and the last candle for 1D/1W, and adds any setups to one running list for the day (with the time first seen, the SMA slope, the volume multiple, the stop and the risk). A star marks setups with volume at least 1.5x its 20-candle average and an SMA slope at least 1x that timeframe's normal. Page `/sma200now` (refreshes itself every minute), download `/sma200now.csv`.

It is wrapped so it can never change Section A / B signals; if it fails, the scans carry on and a warning is logged. The rule is untested live - the backtest at `/sma200` shows how it did historically.

## Trendline setups (`/trendnow`)

Trendlines exactly as the **Price Action Toolkit Lite [UAlgo]** draws them (sensitivity 20): the **falling** line passes through the last two pivot highs (the later one lower), the **rising** line through the last two pivot lows (the later one higher); a pivot is the highest / lowest of 20 candles on each side and is confirmed 20 candles later. The scanner rides on the same scans as the SMA200 setups (no extra Dhan calls, closed candles only) and flags: a close **above a falling line** (LONG), a close **below a rising line** (SHORT), and a first **touch that holds / is rejected** at a line (including retests of a line that was just broken). Each row shows the two pivot prices and dates the line passes through, so it can be found on the chart. Page `/trendnow` (refreshes every minute), download `/trendnow.csv`. These setups are not backtested yet.

## Best setups (`/bestnow`, backtest `/best`)

The SMA200 and trendline setups are *lagging* (they fire after price has moved), so each one is also checked against five clues that tend to show up earlier, all measured on closed candles with no look-ahead: **V** volume now (>= 1.5x the previous 20 candles), **B** volume build-up (the 3 candles before already averaged >= 1.2x), **S** squeeze (the last 10 candles' range <= 0.8x the 50 before), **H** the higher timeframe agrees (10m/1H: the previous day's daily SMA200 on the right side with the right slope; 1D: the previous week's weekly SMA200), **C** an SMA200 setup and a trendline setup together. The score is how many are true (0-5) and is shown on `/sma200now` and `/trendnow`.

`/bestnow` lists only the setups with 3 or more clues, or that match a *tested rule*. `confluence_study.py` (page `/best`, runs by itself every weekday evening after 20:30 IST, after the SMA200 study) trades every historical setup on 1D / 1W / 1H / 10m, measures what each clue is worth with and without it (in both halves of the history), and writes the combinations that were positive in both halves to `results/confluence_rules.json`. Live setups that match one get a tick with its tested average R and trade count. Until that first backtest has run there are no tested rules, so the page shows the checklist score only. Not tested (not in the downloaded bars): open interest, delivery %, relative strength. Downloads: `/bestnow.csv`, `/best/effects.csv`, `/best/grid.csv`, `/best/trades.csv`. Set `CONFLUENCE_STUDY_AUTORUN=false` to switch the evening run off.

## Section A bot (`section_a_bot.py`, log at `/botlog`)
Trades only Section A (Strategy 1 sweep, Strategy 2 order block) from the 10m scan, using the scanner's own volume rule.
After a signal it waits for the break of the signal candle (up to 15 candles), then sends the same `..._CONFIRMED` message TradingView would send to dhan-bridge.
Render env vars: `BOT_ENABLED` (default false), `BOT_LIVE` (default false = paper, log only), `BRIDGE_WEBHOOK_URL`, optional `NTFY_TOPIC`, `BOT_MAX_TRADES_PER_DAY` (10), `BOT_CUTOFF` (14:30).
Record: `/botlog` and `/botlog.csv` (cleared on redeploy - download it).
