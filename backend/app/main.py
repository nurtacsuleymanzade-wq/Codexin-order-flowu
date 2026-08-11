from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

from .collector import BinanceCollector
from .config import settings
from .contracts import unavailable_probability
from .store import EventStore


store = EventStore(settings.raw_dir, settings.redis_url, settings.clickhouse_url)
collector = BinanceCollector(settings, store)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await collector.start()
    yield
    await collector.stop()


app = FastAPI(title="Codexin Order Flow API", version="0.3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=list(settings.cors_origins), allow_methods=["GET"], allow_headers=["*"])


@app.get("/")
async def root() -> dict[str, Any]:
    return {"service": "codexin-order-flow-api", "status": "ok", "market": settings.symbol, "docs": "/docs"}


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    health = collector.health()
    return {"status": "ok" if health["overall"] == "LIVE" else "degraded", "health": health}


@app.get("/readyz")
async def readyz(response: Response) -> dict[str, Any]:
    health = collector.health()
    if health["feeds"]["trades"]["status"] != "LIVE" or health["feeds"]["orderbook"]["status"] != "LIVE":
        response.status_code = 503
    return health


@app.get("/api/v1/health")
@app.get("/api/v1/data-health")
async def data_health() -> dict[str, Any]:
    return collector.health()


@app.get("/api/v1/live/{symbol}/futures/snapshot")
async def snapshot(symbol: str) -> dict[str, Any]:
    if symbol.upper() != settings.symbol:
        raise HTTPException(status_code=404, detail="Only configured symbol is available")
    payload = collector.state.snapshot(); payload["health"] = collector.health(); return payload


@app.get("/api/v1/orderbook/live")
async def live_orderbook() -> dict[str, Any]:
    state = collector.state; return {"status": collector.health()["feeds"]["orderbook"]["status"], "symbol": settings.symbol, "market": "USD_M_FUTURES", "book": state.orderbook.metrics(), "received_at": state.orderbook.last_event_at}


@app.get("/api/v1/flow/summary")
async def flow_summary(tf: str = Query("5m", pattern=r"^(1m|5m|15m|1h)$")) -> dict[str, Any]:
    state = collector.state; return {"status": "LIVE" if state.last_trade_at else "STALE", "timeframe": tf, "symbol": settings.symbol, "source": "BINANCE_FUTURES_TRADE", "cvd": state.cvd, "vwap": state.vwap_pv / state.vwap_notional if state.vwap_notional else None, "trade_count": state.trade_count, "buckets": list(state.buckets.values())[-30:], "data_age_ms": state.age(state.last_trade_at)}


@app.get("/api/v1/capital-flow/summary")
async def capital_flow(tf: str = Query("5m")) -> dict[str, Any]:
    state = collector.state; metrics = state.orderbook.metrics()
    return {"status": "LIVE" if state.last_trade_at else "STALE", "timeframe": tf, "symbol": settings.symbol, "measurement_only": True, "source": "BINANCE_FUTURES_TRADE", "buy_notional": sum(row["buy"] for row in state.buckets.values()), "sell_notional": sum(row["sell"] for row in state.buckets.values()), "cvd": state.cvd, "open_interest": state.open_interest, "funding_rate": state.funding_rate, "book_imbalance": metrics.get("imbalance"), "unsupported": ["wallet_attribution", "exchange_inflow_outflow", "ETF_institutional_flow"]}


@app.get("/api/v1/liquidations/recent")
@app.get("/api/v1/liquidation/heatmap")
async def liquidations() -> dict[str, Any]:
    state = collector.state; observed = list(state.liquidations); price = state.mark_price or state.price
    estimated = []
    if price and state.open_interest:
        for distance in (-.025, -.018, -.012, -.008, .008, .012, .018, .025):
            estimated.append({"price": price * (1 + distance), "size_btc": max(25, state.open_interest * .00035 * (1 - abs(distance) * 8)), "method": "OI_LEVERAGE_PRIOR", "status": "ESTIMATED"})
    return {"symbol": settings.symbol, "observed_status": collector.health()["feeds"]["liquidations"]["status"], "observed": observed, "estimated": estimated, "historical": [], "methodology": {"observed": "Binance forceOrder", "estimated": "OI cohort + leverage prior", "historical": "UNAVAILABLE"}}


@app.get("/api/v1/probability-map/summary")
async def probability_map(tf: str = Query("5m")) -> dict[str, Any]:
    return {"symbol": settings.symbol, "timeframe": tf, "status": "UNAVAILABLE", "decision_authorized": False, "components": {"attraction": None, "accessibility": None, "friction": None, "flow_alignment": None, "historical_calibration": None}, "probability": unavailable_probability("Replayable out-of-sample calibration is not connected")}


@app.get("/api/v1/research/calibration")
async def calibration() -> dict[str, Any]:
    return {"status": "UNAVAILABLE", "calibration_status": "NOT_VERIFIED", "sample_size": 0, "oos_status": "NOT_RUN", "brier_score": None, "log_loss": None, "reason": "Immutable history, labels, fees and slippage model are required"}


@app.get("/metrics")
async def metrics() -> Response:
    state = collector.state; health = collector.health(); values = {"codexin_trade_count": state.trade_count, "codexin_orderbook_resync_total": state.orderbook.resync_count, "codexin_raw_events_dropped_total": store.dropped, "codexin_ws_reconnect_total": collector.reconnects}
    body = "\n".join(f"{key} {value}" for key, value in values.items()) + "\n" + f'codexin_decision_authorized {1 if health["decision_authorized"] else 0}\n'
    return Response(content=body, media_type="text/plain; version=0.0.4")
