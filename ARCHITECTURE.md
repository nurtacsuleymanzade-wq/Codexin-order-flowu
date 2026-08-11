# Codexin Order Flow architecture

## Current release: browser terminal v0.1.0

The repository is intentionally narrow: one venue, one market and one instrument.

```text
Binance USD-M REST + WebSocket
          ↓
Browser-side event validation
          ↓
Local Futures order book + tape aggregation
          ↓
Read-only workspaces
          ↓
NO TRADE gate when evidence is missing, stale or invalid
```

### Contract boundaries

- `aggTrade` is executed Futures flow. It is not a reconstructed order book.
- `depth@100ms` is applied only after a Futures REST snapshot and sequence continuity checks.
- `bookTicker` is a best-bid/best-ask reference. It does not validate the full L2 book.
- `kline` is used for chart context; the open candle is not treated as a closed structure candle.
- Open interest, funding and public long/short ratios are REST context feeds.
- Resting liquidity is observed displayed intent, not proof of execution or absorption.
- Liquidation zones are estimated from an OI/leverage prior and are never labeled as observed force orders.

## Production target

The browser implementation is a safe read-only vertical slice. A production terminal should move ingestion and history behind services:

```text
Binance streams
  → collector workers
  → raw immutable archive (object storage)
  → sequence / dedup / gap validator
  → event bus
  → feature engine
  → Redis live snapshots + ClickHouse history
  → versioned API
  → terminal UI
```

Required production controls:

- A common event envelope: `venue`, `market`, `symbol`, `event_time`, `received_time`, `sequence`, `source`, `status`.
- Independent health states for source connectivity, freshness, sequence validity and persistence.
- No Spot fallback for a Futures market.
- No execution authorization if a required feed is stale, invalid or unavailable.
- Immutable raw events and replayable order-book history.
- Prometheus metrics, structured logs and restart alerts.
- Paper trading and fee/slippage-aware out-of-sample evaluation before any execution path.

## Deliberate non-features

This release does not place orders, expose API keys, claim observed liquidations, or display uncalibrated scores as probabilities.
