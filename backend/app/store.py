"""Append-only raw archive and optional Redis/ClickHouse adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class EventStore:
    def __init__(self, raw_dir: str, redis_url: str | None = None, clickhouse_url: str | None = None) -> None:
        self.raw_dir = Path(raw_dir)
        self.redis_url = redis_url
        self.clickhouse_url = clickhouse_url
        self._lock = asyncio.Lock()
        self._previous_hash = "GENESIS"
        self.redis = None
        self.clickhouse = None
        self.queue: asyncio.Queue[tuple[str, dict[str, Any], str]] = asyncio.Queue(maxsize=20000)
        self.worker: asyncio.Task[Any] | None = None
        self.dropped = 0

    async def start(self) -> None:
        if self.redis_url:
            try:
                from redis.asyncio import Redis
                self.redis = Redis.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
            except Exception:
                self.redis = None
        if self.clickhouse_url:
            try:
                import clickhouse_connect
                self.clickhouse = await asyncio.to_thread(clickhouse_connect.get_client, dsn=self.clickhouse_url)
                await asyncio.to_thread(self.clickhouse.command, "CREATE TABLE IF NOT EXISTS codexin_raw_events (event_time DateTime64(3), received_time DateTime64(3), symbol LowCardinality(String), event_type LowCardinality(String), payload String, event_hash String) ENGINE = MergeTree ORDER BY (symbol, event_time, event_hash)")
            except Exception:
                self.clickhouse = None
        self.worker = asyncio.create_task(self._worker_loop())

    async def close(self) -> None:
        if self.worker:
            try:
                await asyncio.wait_for(self.queue.join(), timeout=5)
            except asyncio.TimeoutError:
                pass
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        if self.redis:
            await self.redis.aclose()
        if self.clickhouse:
            await asyncio.to_thread(self.clickhouse.close)

    async def enqueue(self, event_type: str, payload: dict[str, Any], symbol: str) -> None:
        try:
            self.queue.put_nowait((event_type, payload, symbol))
        except asyncio.QueueFull:
            self.dropped += 1

    async def _worker_loop(self) -> None:
        while True:
            event_type, payload, symbol = await self.queue.get()
            try:
                await self.append(event_type, payload, symbol)
            finally:
                self.queue.task_done()

    async def append(self, event_type: str, payload: dict[str, Any], symbol: str) -> None:
        received = datetime.now(timezone.utc)
        envelope = {"event_type": event_type, "symbol": symbol, "received_time": received.isoformat(), "payload": payload, "previous_hash": self._previous_hash}
        encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":"), default=str)
        event_hash = hashlib.sha256(encoded.encode()).hexdigest()
        envelope["event_hash"] = event_hash
        self._previous_hash = event_hash
        day = received.strftime("%Y-%m-%d")
        async with self._lock:
            path = self.raw_dir / day / "events.jsonl"
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            line = json.dumps(envelope, separators=(",", ":"), default=str) + "\n"
            await asyncio.to_thread(self._append_line, path, line)
            if self.clickhouse:
                event_time = datetime.fromtimestamp(float(payload.get("T", payload.get("E", received.timestamp() * 1000))) / 1000, timezone.utc)
                await asyncio.to_thread(self.clickhouse.insert, "codexin_raw_events", [[event_time, received, symbol, event_type, json.dumps(payload, separators=(",", ":")), event_hash]], column_names=["event_time", "received_time", "symbol", "event_type", "payload", "event_hash"])

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()

    async def publish_snapshot(self, key: str, snapshot: dict[str, Any], ttl: int = 10) -> None:
        if self.redis:
            await self.redis.set(key, json.dumps(snapshot, separators=(",", ":"), default=str), ex=ttl)
