from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    symbol: str = os.getenv("CODEXIN_SYMBOL", "BTCUSDT").upper()
    rest_url: str = os.getenv("CODEXIN_BINANCE_REST", "https://fapi.binance.com/fapi/v1")
    ws_url: str = os.getenv("CODEXIN_BINANCE_WS", "wss://fstream.binance.com/stream?streams=")
    cors_origins: tuple[str, ...] = tuple(filter(None, os.getenv("CODEXIN_CORS_ORIGINS", "*").split(",")))
    raw_dir: str = os.getenv("CODEXIN_RAW_DIR", "./data/raw-events")
    redis_url: str | None = os.getenv("CODEXIN_REDIS_URL")
    clickhouse_url: str | None = os.getenv("CODEXIN_CLICKHOUSE_URL")
    trade_sla_ms: int = int(os.getenv("CODEXIN_TRADE_SLA_MS", "3000"))
    book_sla_ms: int = int(os.getenv("CODEXIN_BOOK_SLA_MS", "1500"))
    kline_sla_ms: int = int(os.getenv("CODEXIN_KLINE_SLA_MS", "5000"))
    oi_sla_ms: int = int(os.getenv("CODEXIN_OI_SLA_MS", "60000"))
    funding_sla_ms: int = int(os.getenv("CODEXIN_FUNDING_SLA_MS", "7200000"))
    liquidation_sla_ms: int = int(os.getenv("CODEXIN_LIQUIDATION_SLA_MS", "5000"))


settings = Settings()
