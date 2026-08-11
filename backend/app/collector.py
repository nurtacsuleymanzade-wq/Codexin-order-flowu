"""Binance USD-M Futures collector with one canonical market state."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import websockets

from .config import Settings
from .contracts import HealthContract
from .core import MarketState, SequenceGap, iso_ms, now_ms
from .store import EventStore
from .telegram import TelegramReporter


class BinanceCollector:
    def __init__(self, settings: Settings, store: EventStore) -> None:
        self.settings = settings
        self.state = MarketState(symbol=settings.symbol)
        self.state.decision_brain.min_rr = settings.min_rr
        self.telegram = TelegramReporter(settings.telegram_bot_token, settings.telegram_chat_id, settings.telegram_enabled)
        self.store = store
        self.http: httpx.AsyncClient | None = None
        self.tasks: list[asyncio.Task[Any]] = []
        self.stop_event = asyncio.Event()
        self.book_sync_lock = asyncio.Lock()
        self.connected = False
        self.last_error: str | None = None
        self.reconnects = 0
        self.started_at = now_ms()

    @property
    def streams(self) -> str:
        symbol = self.settings.symbol.lower()
        return "/".join((f"{symbol}@trade", f"{symbol}@depth@100ms", f"{symbol}@bookTicker", f"{symbol}@forceOrder", f"{symbol}@markPrice@1s"))

    async def start(self) -> None:
        self.http = httpx.AsyncClient(timeout=8, headers={"User-Agent": "codexin-order-flow/0.4"})
        await self.store.start()
        self.tasks = [asyncio.create_task(self.websocket_loop()), asyncio.create_task(self.rest_loop())]

    async def stop(self) -> None:
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.http:
            await self.http.aclose()
        await self.store.close()

    async def websocket_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                url = self.settings.ws_url + self.streams
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, max_queue=5000) as socket:
                    self.connected = True; self.reconnects += 1; self.state.ws_connected_at = now_ms(); self.state.liquidation_connected_at = now_ms(); self.last_error = None
                    asyncio.create_task(self.sync_orderbook())
                    async for raw in socket:
                        if self.stop_event.is_set(): break
                        message = json.loads(raw); data = message.get("data", message); stream = message.get("stream", "")
                        self.state.last_ws_at = now_ms()
                        if "@trade" in stream or data.get("e") in ("trade", "aggTrade"):
                            self.state.trade(data); await self.store.enqueue("trade", data, self.settings.symbol); await self.archive_intelligence_events()
                        elif "@depth" in stream or data.get("e") == "depthUpdate":
                            await self.on_depth(data)
                        elif "bookTicker" in stream or data.get("e") == "bookTicker":
                            self.state.best_bid = float(data["b"]); self.state.best_ask = float(data["a"]); self.state.price = (self.state.best_bid + self.state.best_ask) / 2; self.state.last_ticker_at = now_ms()
                            self.state.intelligence.on_price(self.state.price, self.state.last_ticker_at)
                        elif "markPrice" in stream or data.get("e") == "markPriceUpdate":
                            self.state.mark_price = float(data.get("p") or self.state.mark_price or 0); self.state.index_price = float(data.get("i") or self.state.index_price or 0); self.state.last_ws_at = now_ms()
                        elif "forceOrder" in stream or data.get("e") == "forceOrder":
                            self.state.liquidation(data); await self.store.enqueue("force_order", data, self.settings.symbol)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.connected = False; self.last_error = str(error); await asyncio.sleep(min(30, 1.5 + self.reconnects * 0.25))
            finally:
                self.connected = False

    async def on_depth(self, event: dict[str, Any]) -> None:
        book = self.state.orderbook
        await self.store.enqueue("depth", event, self.settings.symbol)
        if not book.valid:
            book.buffer(event); return
        try:
            changes = book.apply(event); received = now_ms(); self.state.last_book_at = received
            self.state.intelligence.on_depth(changes, received, self.state.price, self.state.intelligence.last_atr)
            await self.archive_intelligence_events()
        except (SequenceGap, KeyError, TypeError, ValueError) as error:
            book.invalidate(); book.buffer(event); self.last_error = f"orderbook gap: {error}"
            if not self.book_sync_lock.locked(): asyncio.create_task(self.sync_orderbook())

    async def archive_intelligence_events(self) -> None:
        for event in self.state.intelligence.drain_archive_events():
            await self.store.enqueue(str(event.get("event_type", "intelligence_event")).lower(), event, self.settings.symbol)

    async def sync_orderbook(self) -> None:
        if not self.http:
            return
        async with self.book_sync_lock:
            # A websocket gap and the periodic REST loop can request a
            # snapshot at the same time.  Once the first request has bridged
            # the buffered stream, the second request must not rebuild the
            # book again (or inflate resync counters).
            if self.state.orderbook.valid:
                return
            try:
                response = await self.http.get(f"{self.settings.rest_url}/depth", params={"symbol": self.settings.symbol, "limit": 1000})
                response.raise_for_status(); snapshot = response.json(); self.state.orderbook.apply_buffered(snapshot); self.state.last_book_at = now_ms(); self.state.intelligence.reset_from_book(self.state.orderbook, self.state.last_book_at, self.state.price); await self.archive_intelligence_events()
            except SequenceGap:
                self.state.orderbook.invalidate()
            except Exception as error:
                self.state.orderbook.invalidate(); self.last_error = f"orderbook snapshot: {error}"

    async def rest_loop(self) -> None:
        derivative_due = 0
        while not self.stop_event.is_set():
            try:
                if not self.state.orderbook.valid:
                    await self.sync_orderbook()
                await self.poll_klines()
                if now_ms() >= derivative_due:
                    await self.poll_derivatives(); derivative_due = now_ms() + 30000
                snapshot = self.state.snapshot()
                await self.telegram.consider(snapshot.get("decision_brain", {}))
                await self.store.publish_snapshot(f"codexin:snapshot:{self.settings.symbol}", snapshot)
                intelligence = snapshot.get("intelligence", {})
                if intelligence.get("status") == "LIVE":
                    await self.store.enqueue("liquidity_intelligence", {"generated_at": intelligence.get("generated_at"), "direction": intelligence.get("direction"), "targets": intelligence.get("targets", []), "detectors": intelligence.get("detectors", {}), "probability_status": intelligence.get("probability_status")}, self.settings.symbol)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = str(error)
            await asyncio.sleep(5)

    async def poll_klines(self) -> None:
        if not self.http: return
        async def fetch(timeframe: str) -> tuple[str, Any]:
            response = await self.http.get(f"{self.settings.rest_url}/klines", params={"symbol": self.settings.symbol, "interval": timeframe, "limit": 500})
            response.raise_for_status()
            return timeframe, response.json()
        results = await asyncio.gather(*(fetch(timeframe) for timeframe in ("1m", "5m", "15m")), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                self.last_error = f"kline poll: {result}"
                continue
            timeframe, rows = result
            for row in rows:
                self.state.update_kline({"time": int(row[0]) // 1000, "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]), "volume": float(row[7]), "closed": int(row[6]) < now_ms()}, timeframe=timeframe)
        self.state.intelligence.last_atr = self.state.intelligence.indicators(list(self.state.klines), list(self.state.buckets.values()), "1m").get("atr")

    async def poll_derivatives(self) -> None:
        if not self.http: return
        symbol = self.settings.symbol
        results = await asyncio.gather(
            self.http.get(f"{self.settings.rest_url}/openInterest", params={"symbol": symbol}),
            self.http.get(f"{self.settings.rest_url}/fundingRate", params={"symbol": symbol, "limit": 1}),
            self.http.get(f"{self.settings.rest_url}/premiumIndex", params={"symbol": symbol}),
            self.http.get("https://fapi.binance.com/futures/data/globalLongShortAccountRatio", params={"symbol": symbol, "period": "5m", "limit": 1}),
        )
        for result in results: result.raise_for_status()
        oi, funding, premium, ratio = [result.json() for result in results]; received = now_ms()
        next_oi = float(oi["openInterest"])
        self.state.oi_delta = next_oi - self.state.open_interest if self.state.open_interest is not None else None
        self.state.previous_open_interest = self.state.open_interest
        self.state.open_interest = next_oi; self.state.funding_rate = float((funding[0].get("fundingRate") if funding else premium.get("lastFundingRate")) or 0); self.state.mark_price = float(premium.get("markPrice") or 0); self.state.index_price = float(premium.get("indexPrice") or 0); self.state.ratio = ratio[0] if ratio else None
        self.state.last_oi_at = self.state.last_funding_at = self.state.last_ratio_at = received

    def health(self) -> HealthContract:
        now = now_ms(); s = self.state; book = s.orderbook
        def feed(status: str, timestamp: int | None, source: str, detail: str, methodology: str = "OBSERVED") -> dict[str, Any]: return {"status": status, "age_ms": s.age(timestamp, now), "timestamp": iso_ms(timestamp), "received_timestamp": iso_ms(timestamp), "source": source, "methodology": methodology, "confidence": "REAL" if status.startswith("LIVE") else "STALE_OR_UNAVAILABLE", "detail": detail}
        trade_status = "LIVE" if s.last_trade_at and now - s.last_trade_at < self.settings.trade_sla_ms else "STALE"
        book_status = "LIVE" if book.valid and s.last_book_at and now - s.last_book_at < self.settings.book_sla_ms else "INVALID" if not book.valid else "STALE"
        kline_status = "LIVE" if s.last_kline_at and now - s.last_kline_at < self.settings.kline_sla_ms else "STALE"
        kline_5_status = "LIVE" if s.last_timeframe_kline_at.get("5m") and len(s.timeframe_klines.get("5m", ())) >= 7 and now - s.last_timeframe_kline_at["5m"] < self.settings.kline_sla_ms * 2 else "STALE"
        kline_15_status = "LIVE" if s.last_timeframe_kline_at.get("15m") and len(s.timeframe_klines.get("15m", ())) >= 7 and now - s.last_timeframe_kline_at["15m"] < self.settings.kline_sla_ms * 2 else "STALE"
        oi_status = "LIVE" if s.last_oi_at and now - s.last_oi_at < self.settings.oi_sla_ms else "STALE" if s.open_interest is not None else "UNAVAILABLE"
        funding_status = "LIVE" if s.last_funding_at and now - s.last_funding_at < self.settings.funding_sla_ms else "STALE" if s.funding_rate is not None else "UNAVAILABLE"
        liquidation_status = "LIVE_QUIET" if self.connected and s.last_ws_at and now - s.last_ws_at < 5000 else "UNAVAILABLE"
        intelligence_status = "LIVE" if book_status == "LIVE" and trade_status == "LIVE" else "SUPPRESSED"
        feeds = {"trades": feed(trade_status, s.last_trade_at, "BINANCE_FUTURES_TRADE", "@trade · isBuyerMaker side rule"), "orderbook": feed(book_status, s.last_book_at, "BINANCE_FUTURES_DEPTH", f"sequence={book.last_update_id} resync={book.resync_count} · aggregated price levels", "OBSERVED_AGGREGATED_L2"), "klines": feed(kline_status, s.last_kline_at, "BINANCE_FUTURES_REST", "1m closed/active candles"), "klines_5m": feed(kline_5_status, s.last_timeframe_kline_at.get("5m"), "BINANCE_FUTURES_REST", f"5m closed candles · {len(s.timeframe_klines.get('5m', ())) } rows"), "klines_15m": feed(kline_15_status, s.last_timeframe_kline_at.get("15m"), "BINANCE_FUTURES_REST", f"15m closed candles · {len(s.timeframe_klines.get('15m', ())) } rows"), "open_interest": feed(oi_status, s.last_oi_at, "BINANCE_FUTURES_REST", "SLA <60s"), "funding": feed(funding_status, s.last_funding_at, "BINANCE_FUTURES_REST", "SLA <2h"), "liquidations": feed(liquidation_status, s.last_liquidation_at, "BINANCE_FUTURES_FORCE_ORDER", "LIVE_QUIET means connected with no recent event"), "intelligence": feed(intelligence_status, s.last_book_at, "LOCAL_LIQUIDITY_INTELLIGENCE", f"tracked_walls={len(s.intelligence.walls)} base_levels={len(s.intelligence.levels)} detector_latency_ms={s.intelligence.last_latency_ms:.2f} reconciliation_error={s.intelligence.reconciliation_error():.4f}", "HEURISTIC_SCORES_UNCALIBRATED"), "macro": feed("UNAVAILABLE", None, "NOT_CONNECTED", "Not used for decisions", "UNAVAILABLE")}
        critical = ("trades", "orderbook", "klines", "klines_5m", "klines_15m", "open_interest", "funding", "liquidations"); missing = [key for key in critical if not feeds[key]["status"].startswith("LIVE")]
        return {"overall": "LIVE" if not missing else "DEGRADED", "market": s.symbol, "venue": s.venue, "market_type": s.market, "decision_authorized": False, "feeds": feeds, "missing_data": missing + ["calibration"], "telegram": self.telegram.status(), "last_error": self.last_error, "generated_at": iso_ms(now) or ""}
