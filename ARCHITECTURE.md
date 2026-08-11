# Codexin Order Flow architecture

## Current release: browser terminal + Liquidity Intent Engine v0.4.0

The repository is intentionally narrow: one venue, one market and one instrument.

```text
Binance USD-M REST + WebSocket
          ↓
Collector: trade/depth/bookTicker/mark/forceOrder
          ↓
REST snapshot + buffered diff-depth sequence validator
          ↓
Base book (all levels) + tracked wall lifecycle (significant levels only)
          ↓
Trade/depth reconciliation → aggression → absorption/replenishment/pull/iceberg
           ↓
Microprice/OBI/vacuum/LRI → target touch-score + conditional break-score
           ↓
Versioned API + chart workspaces; stale/invalid data suppresses intelligence
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
- Depth quantity decreases are reconciled against the Futures trade tape. Unmatched
  decreases remain `unknown_removed_qty`; they are never silently called executions.
- A `LiquidityWall` is opened only for dynamically significant levels and keeps a
  bounded lifecycle: first/last seen, persistence, approach pull, replenishment,
  execution/cancel estimates, absorption and iceberg-like turnover.
- `P_TOUCH` and `P_BREAK_GIVEN_TOUCH` are separate fields. Until labelled replay
  history passes out-of-sample calibration, both are `null` and the API exposes
  `probability_status=UNCALIBRATED`; visible `TOUCH_SCORE`/`BREAK_SCORE` values are
  heuristic evidence ranks.
- Clusters use adaptive ATR-relative price bins. Near/medium/far target regimes
  weight microstructure, depth path and broader structure differently.
- Migration is explicitly a pattern-match inference (`NO ORDER_ID`); spoof-like
  output is a pull-before-touch behaviour score, not a legal manipulation claim.
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

This release does not place orders, expose API keys, claim observed liquidations, or display uncalibrated scores as probabilities. The event archive is
also the source for future target labels (`touch_10s` … `touch_5m`, break after
touch, pull and replenishment), but no calibration claim is made until that
dataset is replayable and validated.
