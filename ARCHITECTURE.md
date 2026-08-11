# Codexin Order Flow architecture

## Current release: browser terminal + backend data plane v0.3.0

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

- `trade` is executed Futures flow. It is not a reconstructed order book.
- `depth@100ms` is applied only after a Futures REST snapshot and sequence continuity checks.
- `bookTicker` is a best-bid/best-ask reference. It does not validate the full L2 book.
- `kline` is used for chart context; the open candle is not treated as a closed structure candle.
- `forceOrder` is the observed liquidation tape. A quiet stream is not converted into synthetic liquidation events.
- Open interest, funding and public long/short ratios are REST context feeds.
- CVD and VWAP are session measurements derived from the received Futures trade stream.
- Resting liquidity is observed displayed intent, not proof of execution or absorption.
- Liquidation zones are estimated from an OI/leverage prior and are never labeled as observed force orders.

## Implemented backend

`backend/app/collector.py` owns the Binance USD-M Futures data plane. It consumes the verified Futures `@trade`, `@depth@100ms`, `@bookTicker` and `@forceOrder` streams, polls klines/derivatives through Futures REST, validates the local book and publishes one canonical health contract.

Raw events are appended to an immutable hash-chained JSONL archive. Redis is used when configured for short-lived snapshots; ClickHouse is used when configured for raw event history. The service still runs with the local archive and in-memory state when those services are unavailable, and reports the degraded state instead of manufacturing data.

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
