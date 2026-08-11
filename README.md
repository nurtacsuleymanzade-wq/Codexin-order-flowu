# Codexin Order Flow

Market-data-first BTCUSDT USD-M Futures research terminal.

## Current scope

- One venue: Binance
- One market: USD-M Futures
- One instrument: BTCUSDT
- Read-only: no API keys, no order execution
- Live Futures trade tape, diff-depth local book and book ticker
- 1m Futures candles refreshed from REST with a visible freshness SLA
- Live Binance Futures `forceOrder` liquidation stream
- OI, funding, mark/index price and public positioning context
- CVD, session VWAP, trade count, footprint buckets and displayed-book imbalance
- Execution, liquidity, derivatives, context and data-health workspaces

The frontend intentionally has no Spot fallback. If a critical feed is missing or its sequence is invalid, the decision gate stays locked to `NO TRADE`.

## Run locally

```bash
python3 -m http.server 4173
```

Then open <http://localhost:4173>.

## GitHub Pages

This is a static, no-build site. Enable **Settings → Pages → Deploy from a branch**, choose `main` and the `/ (root)` folder. GitHub will then serve the terminal from the repository Pages URL.

## Data integrity rules

1. The displayed market is always BTCUSDT USD-M Futures.
2. The order book is built from Futures REST snapshot + buffered Futures diff-depth events.
3. `pu`/`U`/`u` continuity is checked. A gap invalidates the local book and starts a resync.
4. Displayed liquidity is never labeled as executed flow or observed liquidation.
5. Observed `forceOrder` events and estimated OI/leverage zones are separate data products.
6. Historical calibration is explicitly unavailable until a verified replayable history is connected.
7. This project does not place trades.

## Production roadmap

The browser release is deliberately read-only and now includes a verified live-data vertical slice. A production deployment should still move raw ingestion, event validation, ClickHouse history, Redis snapshots, monitoring, authentication and model calibration into separate backend services before adding live execution.
