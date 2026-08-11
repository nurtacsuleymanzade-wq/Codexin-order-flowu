"""Framework-independent market state and local order-book validation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import exp, isfinite
from typing import Any


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def iso_ms(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


class SequenceGap(RuntimeError):
    """Raised when the Binance local-book update chain is not continuous."""


class LocalOrderBook:
    """Binance USD-M local order book using snapshot + U/u/pu continuity."""

    def __init__(self, max_queue: int = 5000) -> None:
        self.bids: dict[str, float] = {}
        self.asks: dict[str, float] = {}
        self.level_last_update: dict[str, int] = {}
        self.last_update_id: int | None = None
        self.last_event_id: int | None = None
        self.valid = False
        self.resync_count = 0
        self.max_queue = max_queue
        self.queue: deque[dict[str, Any]] = deque(maxlen=max_queue)
        self.last_event_at: int | None = None

    def buffer(self, event: dict[str, Any]) -> None:
        self.queue.append(event)

    def reset(self, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        self.bids = {str(price): float(qty) for price, qty in snapshot.get("bids", []) if float(qty) > 0}
        self.asks = {str(price): float(qty) for price, qty in snapshot.get("asks", []) if float(qty) > 0}
        received = now_ms()
        self.level_last_update = {f"BID:{price}": received for price in self.bids} | {f"ASK:{price}": received for price in self.asks}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.last_event_id = None
        self.valid = False
        buffered = list(self.queue)
        self.queue.clear()
        return buffered

    def find_snapshot_start(self, events: list[dict[str, Any]]) -> int:
        if self.last_update_id is None:
            return -1
        for index, event in enumerate(events):
            # The first usable event must cover lastUpdateId + 1. An event
            # ending exactly at the REST snapshot is stale and must be
            # discarded; accepting it leaves the book invalid forever when
            # the next event starts at snapshot + 1.
            if int(event.get("U", 0)) <= self.last_update_id + 1 and int(event.get("u", 0)) >= self.last_update_id + 1:
                return index
        return -1

    def apply(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        if self.last_update_id is None:
            raise SequenceGap("book has no REST snapshot")
        final_id = int(event["u"])
        first_id = int(event.get("U", final_id))
        if self.last_event_id is not None and final_id <= self.last_event_id:
            return []
        if self.last_event_id is None and final_id <= self.last_update_id:
            return []
        if self.last_event_id is None:
            if first_id > self.last_update_id + 1 or final_id < self.last_update_id + 1:
                raise SequenceGap(f"initial U/u={first_id}/{final_id} does not bridge snapshot={self.last_update_id}")
        elif event.get("pu") is not None and int(event["pu"]) != self.last_event_id:
            raise SequenceGap(f"pu={event.get('pu')} expected={self.last_event_id}")
        elif event.get("pu") is None and first_id > self.last_event_id + 1:
            raise SequenceGap(f"U={first_id} after previous u={self.last_event_id}")
        received = now_ms()
        changes: list[dict[str, Any]] = []
        for price, quantity in event.get("b", []):
            key, value = str(price), float(quantity)
            old_value = self.bids.get(key, 0.0)
            if value == 0:
                self.bids.pop(key, None)
            else:
                self.bids[key] = value
            self.level_last_update[f"BID:{key}"] = received
            if old_value != value:
                changes.append({"side": "BID", "price": float(price), "old_qty": old_value, "new_qty": value, "event_id": int(event["u"]), "event_time": int(event.get("E") or event.get("T") or received)})
        for price, quantity in event.get("a", []):
            key, value = str(price), float(quantity)
            old_value = self.asks.get(key, 0.0)
            if value == 0:
                self.asks.pop(key, None)
            else:
                self.asks[key] = value
            self.level_last_update[f"ASK:{key}"] = received
            if old_value != value:
                changes.append({"side": "ASK", "price": float(price), "old_qty": old_value, "new_qty": value, "event_id": int(event["u"]), "event_time": int(event.get("E") or event.get("T") or received)})
        self.last_update_id = final_id
        self.last_event_id = final_id
        self.last_event_at = now_ms()
        self.valid = True
        return changes

    def apply_buffered(self, snapshot: dict[str, Any]) -> None:
        events = self.reset(snapshot)
        start = self.find_snapshot_start(events)
        if start < 0:
            self.resync_count += 1
            raise SequenceGap("no buffered event bridges REST snapshot")
        try:
            for event in events[start:]:
                if int(event["u"]) <= int(self.last_update_id or 0):
                    continue
                self.apply(event)
        except (KeyError, TypeError, ValueError, SequenceGap):
            self.valid = False
            self.resync_count += 1
            raise

    def invalidate(self) -> None:
        self.valid = False
        self.resync_count += 1

    def metrics(self) -> dict[str, Any]:
        bids = sorted(({"price": float(p), "quantity": q} for p, q in self.bids.items()), key=lambda row: row["price"], reverse=True)
        asks = sorted(({"price": float(p), "quantity": q} for p, q in self.asks.items()), key=lambda row: row["price"])
        if not self.valid or not bids or not asks or bids[0]["price"] >= asks[0]["price"]:
            return {"valid": False, "sequence": self.last_update_id, "resync_count": self.resync_count}
        mid = (bids[0]["price"] + asks[0]["price"]) / 2
        range_10bps = mid * 0.001
        bid_notional = sum(row["price"] * row["quantity"] for row in bids if row["price"] >= mid - range_10bps)
        ask_notional = sum(row["price"] * row["quantity"] for row in asks if row["price"] <= mid + range_10bps)
        bid_btc = sum(row["quantity"] for row in bids); ask_btc = sum(row["quantity"] for row in asks)
        total_notional = sum(row["price"] * row["quantity"] for row in bids + asks)
        weighted_bid = sum(row["quantity"] * exp(-abs(row["price"] - mid) / (mid * 0.002)) for row in bids)
        weighted_ask = sum(row["quantity"] * exp(-abs(row["price"] - mid) / (mid * 0.002)) for row in asks)
        weighted_total = weighted_bid + weighted_ask
        total = bid_notional + ask_notional
        return {
            "valid": True,
            "sequence": self.last_update_id,
            "resync_count": self.resync_count,
            "mid": mid,
            "spread": asks[0]["price"] - bids[0]["price"],
            "spread_bps": (asks[0]["price"] - bids[0]["price"]) / mid * 10000,
            "bid_notional_10bps": bid_notional,
            "ask_notional_10bps": ask_notional,
            "bid_btc": bid_btc,
            "ask_btc": ask_btc,
            "bid_notional": sum(row["price"] * row["quantity"] for row in bids),
            "ask_notional": sum(row["price"] * row["quantity"] for row in asks),
            "bid_levels": len(bids),
            "ask_levels": len(asks),
            "microprice": (asks[0]["quantity"] * bids[0]["price"] + bids[0]["quantity"] * asks[0]["price"]) / (bids[0]["quantity"] + asks[0]["quantity"]),
            "weighted_imbalance": (weighted_bid - weighted_ask) / weighted_total * 100 if weighted_total else 0,
            "visible_notional": total_notional,
            "bid_share": bid_notional / total * 100 if total else 50,
            "ask_share": ask_notional / total * 100 if total else 50,
            "imbalance": (bid_notional - ask_notional) / total * 100 if total else 0,
            "bids": bids,
            "asks": asks,
        }


@dataclass
class MarketState:
    symbol: str = "BTCUSDT"
    venue: str = "BINANCE"
    market: str = "USD_M_FUTURES"
    orderbook: LocalOrderBook = field(default_factory=LocalOrderBook)
    price: float | None = None
    mark_price: float | None = None
    index_price: float | None = None
    open_interest: float | None = None
    funding_rate: float | None = None
    ratio: dict[str, Any] | None = None
    last_trade_at: int | None = None
    last_book_at: int | None = None
    last_kline_at: int | None = None
    last_oi_at: int | None = None
    last_funding_at: int | None = None
    last_ratio_at: int | None = None
    ws_connected_at: int | None = None
    last_ws_at: int | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    last_ticker_at: int | None = None
    liquidation_connected_at: int | None = None
    last_liquidation_at: int | None = None
    cvd: float = 0
    vwap_pv: float = 0
    vwap_notional: float = 0
    trade_count: int = 0
    trades: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=1000))
    liquidations: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=500))
    buckets: dict[int, dict[str, Any]] = field(default_factory=dict)
    klines: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=500))
    intelligence: Any = field(default=None, repr=False)

    def __post_init__(self) -> None:
        from .intelligence import LiquidityIntelligenceEngine
        self.intelligence = LiquidityIntelligenceEngine(self.symbol)

    def trade(self, event: dict[str, Any], received_at: int | None = None) -> dict[str, Any]:
        received = received_at or now_ms()
        price, quantity = float(event["p"]), float(event["q"])
        notional, buy = price * quantity, not bool(event.get("m", False))
        event_time = int(event.get("T", received))
        item = {"event_time": event_time, "received_time": received, "price": price, "quantity": quantity, "notional": notional, "side": "BUY" if buy else "SELL", "trade_id": event.get("t") or event.get("a"), "source": "BINANCE_FUTURES_TRADE"}
        self.price = price; self.last_trade_at = received; self.last_ws_at = received; self.trade_count += 1
        self.cvd += notional if buy else -notional
        self.vwap_pv += price * notional; self.vwap_notional += notional
        self.trades.appendleft(item)
        self.intelligence.on_trade(item, received)
        bucket_time = event_time // 60000 * 60000
        bucket = self.buckets.setdefault(bucket_time, {"time": bucket_time, "buy": 0.0, "sell": 0.0, "count": 0})
        bucket["buy" if buy else "sell"] += notional; bucket["count"] += 1
        if len(self.buckets) > 120:
            del self.buckets[min(self.buckets)]
        return item

    def liquidation(self, event: dict[str, Any], received_at: int | None = None) -> dict[str, Any]:
        received = received_at or now_ms(); order = event.get("o", event)
        price, quantity = float(order.get("ap") or order.get("p") or 0), float(order.get("z") or order.get("q") or 0)
        item = {"event_time": int(order.get("T", received)), "received_time": received, "price": price, "quantity": quantity, "notional": price * quantity, "side": order.get("S", "UNKNOWN"), "position_side": order.get("ps", "UNKNOWN"), "status": order.get("X", "FILLED"), "source": "BINANCE_FUTURES_FORCE_ORDER"}
        self.last_liquidation_at = received; self.last_ws_at = received; self.liquidations.appendleft(item)
        return item

    def update_kline(self, row: dict[str, Any], received_at: int | None = None) -> None:
        self.last_kline_at = received_at or now_ms()
        existing = next((index for index, item in enumerate(self.klines) if item["time"] == row["time"]), None)
        if existing is None:
            self.klines.append(row)
        else:
            items = list(self.klines); items[existing] = row; self.klines.clear(); self.klines.extend(items)

    def age(self, timestamp: int | None, current: int | None = None) -> int | None:
        if timestamp is None:
            return None
        return max(0, (current or now_ms()) - timestamp)

    def snapshot(self, intelligence_trusted: bool | None = None) -> dict[str, Any]:
        current = now_ms(); metrics = self.orderbook.metrics()
        book_fresh = self.last_book_at is not None and self.age(self.last_book_at, current) <= 1500
        trade_fresh = self.last_trade_at is not None and self.age(self.last_trade_at, current) <= 3000
        trusted = book_fresh and trade_fresh if intelligence_trusted is None else intelligence_trusted
        intelligence = self.intelligence.snapshot(self.orderbook, list(self.klines), list(self.buckets.values()), self.price, current, data_integrity_ok=trusted) if self.intelligence else {"status": "UNAVAILABLE"}
        return {
            "market": {"venue": self.venue, "market": self.market, "symbol": self.symbol, "market_type": "PERPETUAL"},
            "price": {"last": self.price, "mark": self.mark_price, "index": self.index_price, "best_bid": self.best_bid, "best_ask": self.best_ask},
            "flow": {"cvd": self.cvd, "vwap": self.vwap_pv / self.vwap_notional if self.vwap_notional else None, "trade_count": self.trade_count, "buckets": list(self.buckets.values())[-500:]},
            "derivatives": {"open_interest": self.open_interest, "funding_rate": self.funding_rate, "positioning": self.ratio},
            "orderbook": {**metrics, "age_ms": self.age(self.last_book_at)},
            "liquidations": {"recent": list(self.liquidations)[:50], "age_ms": self.age(self.last_liquidation_at)},
            "candles": {"rows": list(self.klines)[-200:], "age_ms": self.age(self.last_kline_at)},
            "freshness": {"trade_age_ms": self.age(self.last_trade_at), "book_age_ms": self.age(self.last_book_at), "oi_age_ms": self.age(self.last_oi_at), "funding_age_ms": self.age(self.last_funding_at)},
            "intelligence": intelligence,
            "received_at": iso_ms(current),
        }
