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

## Backend

GitHub Pages cannot run collectors, Redis or ClickHouse. The backend in `backend/` is the live data plane.

### Local backend with Docker

```bash
docker compose up --build
curl http://localhost:8000/healthz
```

The API exposes the canonical contracts at:

- `GET /api/v1/health`
- `GET /api/v1/live/BTCUSDT/futures/snapshot`
- `GET /api/v1/orderbook/live`
- `GET /api/v1/flow/summary?tf=5m`
- `GET /api/v1/liquidations/recent`
- `GET /api/v1/probability-map/summary?tf=5m`
- `GET /metrics`

To point the static frontend at a deployed backend, append its versioned API base:

```text
https://nurtacsuleymanzade-wq.github.io/Codexin-order-flowu/?api=https://api.example.com/api/v1
```

The published site now defaults to the production API gateway at
`https://nce-api.78.46.134.148.sslip.io/api/v2`. The gateway preserves the
backend's canonical `/api/v1` contract internally. Supplying `api=` overrides
the gateway for local or staging environments. The frontend never falls back
from Futures to Spot.

## Data integrity rules

1. The displayed market is always BTCUSDT USD-M Futures.
2. The order book is built from Futures REST snapshot + buffered Futures diff-depth events.
3. `pu`/`U`/`u` continuity is checked. A gap invalidates the local book and starts a resync.
4. Displayed liquidity is never labeled as executed flow or observed liquidation.
5. Observed `forceOrder` events and estimated OI/leverage zones are separate data products.
6. Historical calibration is explicitly unavailable until a verified replayable history is connected.
7. This project does not place trades.

## Production status

The VPS deployment runs the collector as a restartable systemd service behind
Nginx, with Redis snapshot publishing and an append-only hash-chained raw-event
archive. The decision gate remains read-only and locked until calibration data
is verified. ClickHouse history, macro/news ingestion and calibration are
intentionally marked unavailable rather than fabricated.
