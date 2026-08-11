# Codexin Order Flow

Market-data-first BTCUSDT USD-M Futures research terminal.

## Current scope

- One venue: Binance
- One market: USD-M Futures
- One instrument: BTCUSDT
- Read-only: no API keys, no order execution
- Live Futures aggTrade, diff-depth local book, book ticker and 1m klines
- OI, funding, mark/index price and public positioning context
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
5. Liquidation and historical calibration are explicitly unavailable until verified sources are connected.
6. This project does not place trades.

## Production roadmap

The browser prototype is deliberately read-only. A production deployment should move raw ingestion, event validation, ClickHouse history, Redis snapshots, monitoring and authentication into separate backend services before adding live execution.
