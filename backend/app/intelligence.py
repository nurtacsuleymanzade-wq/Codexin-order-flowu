"""Lifecycle-aware liquidity intent engine for Binance USD-M Futures.

Observed L2, executed trade flow and heuristic inferences stay separate.  Every
``*_score`` is an uncalibrated 0-100 evidence rank.  Probability fields remain
``None`` until the archived outcome labels pass out-of-sample calibration.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from math import exp, isfinite
from statistics import median
from time import perf_counter
from typing import Any, Iterable

from .calibration import TargetOutcomeTracker


EPS = 1e-9
WINDOWS = (100, 250, 500, 1000, 3000, 5000)
TOUCH_HORIZONS = ("10s", "30s", "1m", "3m", "5m")


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if isfinite(result) else default
    except (TypeError, ValueError):
        return default


def percentile(values: Iterable[float], value: float) -> float:
    rows = sorted(safe_float(item) for item in values if safe_float(item) > 0)
    return percentile_sorted(rows, value)


def percentile_sorted(rows: list[float], value: float) -> float:
    if not rows:
        return 50.0
    return bisect_right(rows, value) / len(rows) * 100.0


def quantile(rows: list[float], fraction: float, default: float = 0.0) -> float:
    if not rows:
        return default
    ordered = sorted(rows)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    output = [values[0]]
    for value in values[1:]:
        output.append(value * alpha + output[-1] * (1 - alpha))
    return output


def aggregate_bars(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    if minutes <= 1:
        return list(rows)
    grouped: dict[int, dict[str, Any]] = {}
    bucket_seconds = minutes * 60
    for row in rows:
        timestamp = int(safe_float(row.get("time")))
        key = timestamp // bucket_seconds * bucket_seconds
        if key not in grouped:
            grouped[key] = {
                "time": key,
                "open": safe_float(row.get("open")),
                "high": safe_float(row.get("high")),
                "low": safe_float(row.get("low")),
                "close": safe_float(row.get("close")),
                "volume": 0.0,
                "closed": bool(row.get("closed", True)),
            }
        item = grouped[key]
        item["high"] = max(item["high"], safe_float(row.get("high")))
        item["low"] = min(item["low"], safe_float(row.get("low")))
        item["close"] = safe_float(row.get("close"))
        item["volume"] += safe_float(row.get("volume"))
        item["closed"] = item["closed"] and bool(row.get("closed", True))
    return [grouped[key] for key in sorted(grouped)]


def rsi_series(closes: list[float], period: int = 14) -> list[float | None]:
    output: list[float | None] = [None] * len(closes)
    if len(closes) < period + 1:
        return output
    gains = [max(closes[index] - closes[index - 1], 0.0) for index in range(1, len(closes))]
    losses = [max(closes[index - 1] - closes[index], 0.0) for index in range(1, len(closes))]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period
    output[period] = 100.0 if average_loss <= EPS and average_gain > EPS else 50.0 if average_loss <= EPS else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
    for index in range(period + 1, len(closes)):
        average_gain = (average_gain * (period - 1) + gains[index - 1]) / period
        average_loss = (average_loss * (period - 1) + losses[index - 1]) / period
        output[index] = 100.0 if average_loss <= EPS and average_gain > EPS else 50.0 if average_loss <= EPS else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
    return output


def atr(rows: list[dict[str, Any]], period: int = 14) -> float | None:
    if len(rows) < 2:
        return None
    ranges: list[float] = []
    previous_close = safe_float(rows[0].get("close"))
    for row in rows[1:]:
        high, low, close = safe_float(row.get("high")), safe_float(row.get("low")), safe_float(row.get("close"))
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        previous_close = close
    return sum(ranges[-period:]) / min(period, len(ranges)) if ranges else None


@dataclass
class LiquidityWall:
    symbol: str
    side: str
    price: float
    first_seen_ts: int
    last_seen_ts: int
    initial_qty: float
    current_qty: float
    max_qty: float
    min_qty: float
    total_added_qty: float = 0.0
    total_removed_qty: float = 0.0
    total_replenished_qty: float = 0.0
    estimated_executed_qty: float = 0.0
    estimated_cancelled_qty: float = 0.0
    unknown_removed_qty: float = 0.0
    distance_usd: float = 0.0
    distance_pct: float = 0.0
    distance_atr: float | None = None
    tracking_zone: str = "STRUCTURAL"
    importance_score: float = 0.0
    size_percentile: float = 50.0
    relative_depth_percentile: float = 50.0
    pull_count: int = 0
    repeat_pull_count: int = 0
    replenishment_count: int = 0
    depletion_count: int = 0
    touch_count: int = 0
    reaction_count: int = 0
    appearance_count: int = 1
    max_depletion_pct: float = 0.0
    max_replenishment_pct: float = 0.0
    last_touch_ts: int | None = None
    last_pull_ts: int | None = None
    last_replenishment_ts: int | None = None
    last_depletion_ts: int | None = None
    last_reaction_ts: int | None = None
    price_approach_velocity: float = 0.0
    approach_sensitivity: float = 0.0
    liquidity_elasticity: float | None = None
    elasticity_state: str = "STABLE"
    persistence_score: float = 0.0
    pull_score: float = 0.0
    replenishment_score: float = 0.0
    absorption_score: float = 0.0
    iceberg_score: float = 0.0
    real_liquidity_score: float = 0.0
    touch_score: float = 0.0
    break_score: float = 0.0
    reaction_strength: float = 0.0
    reconciliation_confidence: float = 0.0
    effective_liquidity_estimate: float = 0.0
    effective_liquidity_confidence: float = 0.0
    hidden_liquidity_estimate: float = 0.0
    migration_direction: str = "NONE"
    migration_velocity: float | None = None
    migration_distance: float = 0.0
    migration_confidence: float = 0.0
    visible_time_ms: int = 0
    time_near_max_size_ms: int = 0
    time_above_significant_threshold_ms: int = 0
    last_sample_ts: int | None = None
    pending_depletion_qty: float = 0.0
    visible: bool = True
    role: str = "UNCERTAIN"
    confidence: float = 0.0

    @property
    def lifetime_ms(self) -> int:
        return max(0, self.last_seen_ts - self.first_seen_ts)

    @property
    def survival_ratio(self) -> float:
        return self.visible_time_ms / max(self.lifetime_ms, 1)

    @property
    def pull_ratio(self) -> float:
        return self.estimated_cancelled_qty / max(self.initial_qty + self.total_added_qty, EPS)

    @property
    def replenishment_ratio(self) -> float:
        return self.total_replenished_qty / max(self.estimated_executed_qty + self.estimated_cancelled_qty + self.unknown_removed_qty, EPS)

    @property
    def displayed_turnover_ratio(self) -> float:
        return self.estimated_executed_qty / max(self.max_qty, EPS)

    def observe_time(self, timestamp: int, significant_threshold: float) -> None:
        if self.last_sample_ts is None:
            self.last_sample_ts = timestamp
            return
        elapsed = max(0, min(timestamp - self.last_sample_ts, 30_000))
        if self.visible and self.current_qty > 0:
            self.visible_time_ms += elapsed
            if self.current_qty >= self.max_qty * 0.8:
                self.time_near_max_size_ms += elapsed
            if self.current_qty >= significant_threshold:
                self.time_above_significant_threshold_ms += elapsed
        self.last_sample_ts = timestamp

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result.update(
            {
                "lifetime_ms": self.lifetime_ms,
                "lifetime_sec": round(self.lifetime_ms / 1000, 3),
                "survival_ratio": round(self.survival_ratio, 5),
                "pull_ratio": round(self.pull_ratio, 5),
                "pull_before_touch_score": round(self.pull_score, 2),
                "replenishment_ratio": round(self.replenishment_ratio, 5),
                "displayed_turnover_ratio": round(self.displayed_turnover_ratio, 5),
                "visible_peak_qty": self.max_qty,
                "executed_at_level": self.estimated_executed_qty,
                "executed_qty": self.estimated_executed_qty,
                "cancelled_qty": self.estimated_cancelled_qty,
                "displayed_qty": self.current_qty,
                "effective_qty": self.effective_liquidity_estimate,
                "probability_status": "UNCALIBRATED",
                "p_touch": {horizon: None for horizon in TOUCH_HORIZONS},
                "p_break_given_touch": None,
                "median_ettt_sec": None,
            }
        )
        return result


class LiquidityIntelligenceEngine:
    """Bounded state machine fed only after local-book sequence validation."""

    def __init__(self, symbol: str = "BTCUSDT") -> None:
        self.symbol = symbol
        self.levels: dict[str, dict[str, Any]] = {}
        self.walls: dict[str, LiquidityWall] = {}
        self.trades: deque[dict[str, Any]] = deque(maxlen=12_000)
        self.book_history: deque[dict[str, Any]] = deque(maxlen=1_200)
        self.price_history: deque[tuple[int, float]] = deque(maxlen=2_500)
        self.events: deque[dict[str, Any]] = deque(maxlen=500)
        self.archive_events: deque[dict[str, Any]] = deque()
        self.recent_pulls: deque[dict[str, Any]] = deque(maxlen=200)
        self.migration_events: deque[dict[str, Any]] = deque(maxlen=100)
        self.depth_event_times: deque[int] = deque(maxlen=5_000)
        self.outcomes = TargetOutcomeTracker(max_records=600)
        self.current_price: float | None = None
        self.last_trade_at: int | None = None
        self.last_depth_at: int | None = None
        self.last_touch_scan_at: int = 0
        self.last_outcome_scan_at: int = 0
        self.last_atr: float | None = None
        self.last_latency_ms: float = 0.0
        self.last_snapshot_latency_ms: float = 0.0
        self.reset_count = 0
        self.depth_removed_qty = 0.0
        self.unknown_removed_qty = 0.0
        self.effective_policy = {"pull_discount": 0.75, "replenishment_boost": 0.80, "persistence_floor": 0.55}

    @staticmethod
    def _key(side: str, price: float) -> str:
        return f"{side}:{price:.8f}"

    def _emit(self, event_type: str, timestamp: int, **detail: Any) -> None:
        event = {"event_type": event_type, "event_time": timestamp, "symbol": self.symbol, "probability_status": "UNCALIBRATED", **detail}
        self.events.appendleft(event)
        self.archive_events.append(event)

    def drain_archive_events(self) -> list[dict[str, Any]]:
        rows = list(self.archive_events)
        self.archive_events.clear()
        rows.extend(self.outcomes.drain_archive())
        return rows

    def reset_from_book(self, book: Any, timestamp: int, price: float | None = None) -> None:
        self.levels.clear()
        self.walls.clear()
        self.trades.clear()
        self.book_history.clear()
        self.current_price = price
        atr_value = self.last_atr or (price or 1) * 0.001
        side_values = {
            "BID": sorted(float(quantity) for quantity in book.bids.values() if float(quantity) > 0),
            "ASK": sorted(float(quantity) for quantity in book.asks.values() if float(quantity) > 0),
        }
        for side, mapping in (("BID", book.bids), ("ASK", book.asks)):
            threshold = quantile(side_values[side], 0.85, 0.0)
            for raw_price, quantity in mapping.items():
                level_price, level_qty = float(raw_price), float(quantity)
                key = self._key(side, level_price)
                self.levels[key] = {"side": side, "price": level_price, "current_qty": level_qty, "last_update_ts": timestamp}
                size_pct = percentile_sorted(side_values[side], level_qty)
                importance, zone, distance_atr = self._importance(side, level_price, level_qty, price, atr_value, size_pct, 0.0)
                if self._should_track(size_pct, distance_atr, importance):
                    wall = LiquidityWall(self.symbol, side, level_price, timestamp, timestamp, level_qty, level_qty, level_qty, level_qty)
                    wall.size_percentile = wall.relative_depth_percentile = size_pct
                    wall.importance_score, wall.tracking_zone = importance, zone
                    wall.last_sample_ts = timestamp
                    self.walls[key] = wall
            for wall in (item for item in self.walls.values() if item.side == side):
                self._update_wall_scores(wall, price, atr_value, timestamp, threshold)
        self.reset_count += 1
        self._emit("INTELLIGENCE_RESET_AFTER_SNAPSHOT", timestamp, sequence=getattr(book, "last_update_id", None), tracked_walls=len(self.walls), base_levels=len(self.levels))

    def on_price(self, price: float | None, timestamp: int) -> None:
        if price is None or not isfinite(price) or price <= 0:
            return
        self.current_price = price
        if not self.price_history or self.price_history[-1] != (timestamp, price):
            self.price_history.append((timestamp, price))
        if timestamp - self.last_outcome_scan_at >= 100:
            self.outcomes.on_price(price, timestamp)
            self.last_outcome_scan_at = timestamp
        if timestamp - self.last_touch_scan_at >= 100:
            self._touch_walls(timestamp)
            self.last_touch_scan_at = timestamp

    def on_trade(self, item: dict[str, Any], timestamp: int) -> None:
        record = dict(item)
        record["received_time"] = timestamp
        record["reconciled_qty"] = 0.0
        record["buy"] = record.get("side") == "BUY"
        self.trades.append(record)
        self.last_trade_at = timestamp
        self.on_price(safe_float(record.get("price")), timestamp)

    def _significance_context(self) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for side in ("BID", "ASK"):
            values = sorted(item["current_qty"] for item in self.levels.values() if item["side"] == side and item["current_qty"] > 0)
            output[side] = {"values": values, "threshold": quantile(values, 0.85, 0.0)}
        return output

    def _importance(self, side: str, price: float, quantity: float, current: float | None, atr_value: float | None, size_pct: float, behaviour: float) -> tuple[float, str, float]:
        fallback_atr = atr_value or abs(current or price) * 0.001
        distance_atr = abs(price - current) / max(fallback_atr, EPS) if current else 99.0
        zone = "HOT" if distance_atr <= 0.5 else "ACTIVE" if distance_atr <= 2 else "STRUCTURAL"
        proximity = clamp(100 - distance_atr * (55 if zone == "HOT" else 25 if zone == "ACTIVE" else 8))
        relative = size_pct
        persistence = 0.0
        importance = 0.35 * size_pct + 0.25 * proximity + 0.15 * relative + 0.15 * persistence + 0.10 * behaviour
        return clamp(importance), zone, distance_atr

    @staticmethod
    def _should_track(size_pct: float, distance_atr: float, importance: float) -> bool:
        return importance >= 58 or size_pct >= 92 or (distance_atr <= 0.5 and size_pct >= 35) or (distance_atr <= 2 and size_pct >= 72)

    def on_depth(self, changes: list[dict[str, Any]], timestamp: int, price: float | None, atr_value: float | None = None) -> None:
        started = perf_counter()
        self.last_depth_at = timestamp
        self.depth_event_times.append(timestamp)
        if atr_value and atr_value > 0:
            self.last_atr = atr_value
        atr_value = self.last_atr or (price or self.current_price or 1) * 0.001
        self.on_price(price, timestamp)
        context = self._significance_context()
        dynamics = self._price_dynamics(timestamp)
        for change in changes:
            side = str(change["side"]).upper()
            level_price = safe_float(change["price"])
            old_qty, new_qty = safe_float(change.get("old_qty")), safe_float(change.get("new_qty"))
            key = self._key(side, level_price)
            if new_qty > 0:
                self.levels[key] = {"side": side, "price": level_price, "current_qty": new_qty, "last_update_ts": timestamp}
            else:
                self.levels.pop(key, None)
            size_pct = percentile_sorted(context[side]["values"], new_qty)
            existing = self.walls.get(key)
            behaviour = max(existing.pull_score, existing.replenishment_score, existing.absorption_score) if existing else 0.0
            importance, zone, distance_atr = self._importance(side, level_price, new_qty, price, atr_value, size_pct, behaviour)
            wall = existing
            if wall is None and new_qty > 0 and self._should_track(size_pct, distance_atr, importance):
                wall = LiquidityWall(self.symbol, side, level_price, timestamp, timestamp, new_qty, new_qty, new_qty, new_qty)
                wall.last_sample_ts = timestamp
                self.walls[key] = wall
                self._emit("WALL_TRACKED", timestamp, side=side, price=level_price, displayed_qty=new_qty, importance_score=importance, tracking_zone=zone)
                if old_qty <= 0:
                    self._match_migration(wall, timestamp, atr_value)
            if wall is None:
                continue

            significant_threshold = context[side]["threshold"]
            wall.observe_time(timestamp, significant_threshold)
            previous_distance = abs(wall.price - (self.current_price or wall.price))
            wall.last_seen_ts = timestamp
            wall.size_percentile = size_pct if new_qty > 0 else wall.size_percentile
            wall.relative_depth_percentile = wall.size_percentile
            wall.importance_score, wall.tracking_zone, wall.distance_atr = importance, zone, distance_atr
            wall.current_qty = new_qty
            wall.visible = new_qty > 0

            if new_qty > old_qty:
                added = new_qty - old_qty
                wall.total_added_qty += added
                wall.max_qty = max(wall.max_qty, new_qty)
                if old_qty <= 0:
                    wall.appearance_count += 1
                if wall.pending_depletion_qty > 0 and wall.last_depletion_ts and timestamp - wall.last_depletion_ts <= 2_500:
                    replenished = min(added, wall.pending_depletion_qty)
                    recovery = replenished / max(wall.pending_depletion_qty, EPS)
                    if replenished >= max(wall.max_qty * 0.01, 0.01):
                        wall.replenishment_count += 1
                        wall.total_replenished_qty += replenished
                        wall.last_replenishment_ts = timestamp
                        wall.max_replenishment_pct = max(wall.max_replenishment_pct, added / max(old_qty, EPS))
                        self._emit(
                            "REPLENISHMENT_EVENT",
                            timestamp,
                            side=side,
                            price=level_price,
                            before_qty=old_qty,
                            depleted_qty=wall.pending_depletion_qty,
                            replenished_qty=replenished,
                            recovery_pct=recovery * 100,
                            time_to_replenish_ms=timestamp - wall.last_depletion_ts,
                            repeat_index=wall.replenishment_count,
                        )
                        self.outcomes.note_replenishment(side, level_price, timestamp)
                    wall.pending_depletion_qty = max(0.0, wall.pending_depletion_qty - replenished)
            elif new_qty < old_qty:
                removed = old_qty - new_qty
                self.depth_removed_qty += removed
                wall.total_removed_qty += removed
                wall.depletion_count += 1
                wall.pending_depletion_qty += removed
                wall.last_depletion_ts = timestamp
                wall.max_depletion_pct = max(wall.max_depletion_pct, removed / max(old_qty, EPS))
                executed, coverage_confidence = self._reconcile(level_price, side, removed, timestamp, atr_value)
                remaining = max(0.0, removed - executed)
                cancelled = remaining * coverage_confidence
                unknown = remaining - cancelled
                wall.estimated_executed_qty += executed
                wall.estimated_cancelled_qty += cancelled
                wall.unknown_removed_qty += unknown
                self.unknown_removed_qty += unknown
                event_confidence = (executed + cancelled) / max(removed, EPS)
                wall.reconciliation_confidence = 0.7 * wall.reconciliation_confidence + 0.3 * event_confidence if wall.depletion_count > 1 else event_confidence
                removal_ratio = removed / max(old_qty, EPS)
                recent_touch = wall.last_touch_ts is not None and timestamp - wall.last_touch_ts <= 1_500
                pull_evidence = cancelled / max(removed, EPS)
                if pull_evidence >= 0.5 and (new_qty <= 0 or removal_ratio >= 0.35) and (not recent_touch or pull_evidence >= 0.80):
                    wall.pull_count += 1
                    wall.repeat_pull_count = max(wall.repeat_pull_count, wall.pull_count)
                    wall.last_pull_ts = timestamp
                    self.recent_pulls.append({"side": side, "price": level_price, "qty": max(old_qty, cancelled), "timestamp": timestamp})
                    if not recent_touch:
                        self.outcomes.note_pull_before_touch(side, level_price, timestamp)
                    self._emit("PULL_BEFORE_TOUCH_EVENT" if not recent_touch else "LIQUIDITY_PULL_AT_TOUCH_EVENT", timestamp, side=side, price=level_price, removed_qty=removed, cancelled_qty_estimate=cancelled, executed_qty_estimate=executed, reconciliation_confidence=event_confidence, before_touch=not recent_touch)
                if removed >= max(wall.max_qty * 0.08, 0.01):
                    self._emit("DEPTH_REMOVAL_RECONCILED", timestamp, side=side, price=level_price, removed_qty=removed, executed_qty_estimate=executed, cancelled_qty_estimate=cancelled, unknown_removed_qty=unknown, reconciliation_confidence=event_confidence)

            wall.min_qty = min(wall.min_qty, new_qty)
            new_distance = abs(wall.price - (self.current_price or wall.price))
            distance_delta = new_distance - previous_distance
            if abs(distance_delta) > EPS:
                wall.liquidity_elasticity = (new_qty - old_qty) / distance_delta
                approaching = distance_delta < 0
                wall.elasticity_state = "BUILDING" if approaching and new_qty > old_qty else "RETREATING" if approaching and new_qty < old_qty else "STABLE"
                if approaching:
                    wall.approach_sensitivity = 0.75 * wall.approach_sensitivity + 0.25 * clamp(abs(new_qty - old_qty) / max(old_qty, 0.01) * 100)
            self._update_wall_scores(wall, price, atr_value, timestamp, significant_threshold, dynamics)

        self._touch_walls(timestamp)
        self._prune_walls(timestamp)
        self.last_latency_ms = (perf_counter() - started) * 1000

    def _reconcile(self, level_price: float, side: str, removed: float, timestamp: int, atr_value: float | None) -> tuple[float, float]:
        if removed <= 0:
            return 0.0, 0.0
        tolerance = max(0.2, (atr_value or level_price * 0.0005) * 0.015)
        wanted_buy = side == "ASK"
        remaining, matched = removed, 0.0
        for trade in reversed(self.trades):
            age = timestamp - int(trade.get("received_time", timestamp))
            if age > 1_200:
                break
            if age < -200 or bool(trade.get("buy")) != wanted_buy or abs(safe_float(trade.get("price")) - level_price) > tolerance:
                continue
            available = max(0.0, safe_float(trade.get("quantity")) - safe_float(trade.get("reconciled_qty")))
            used = min(remaining, available)
            if used <= 0:
                continue
            trade["reconciled_qty"] = safe_float(trade.get("reconciled_qty")) + used
            matched += used
            remaining -= used
            if remaining <= EPS:
                break
        trade_coverage = 0.92 if self.last_trade_at is not None and abs(timestamp - self.last_trade_at) <= 1_500 else 0.0
        return matched, trade_coverage

    def _match_migration(self, wall: LiquidityWall, timestamp: int, atr_value: float) -> None:
        while self.recent_pulls and timestamp - self.recent_pulls[0]["timestamp"] > 4_000:
            self.recent_pulls.popleft()
        matches = []
        for item in self.recent_pulls:
            if item["side"] != wall.side:
                continue
            qty_ratio = wall.current_qty / max(item["qty"], EPS)
            distance = wall.price - item["price"]
            # Re-adding at the same price is replenishment, not migration.
            # Migration is only an inferred cross-price pattern because
            # Binance depth updates do not carry a stable order id.
            if abs(distance) < 0.1:
                continue
            if 0.45 <= qty_ratio <= 2.2 and abs(distance) <= max(atr_value * 0.6, 2.0):
                similarity = 1 - min(abs(1 - qty_ratio), 1)
                recency = 1 - min((timestamp - item["timestamp"]) / 4_000, 1)
                matches.append((0.65 * similarity + 0.35 * recency, item, distance))
        if not matches:
            return
        score, origin, distance = max(matches, key=lambda row: row[0])
        elapsed = max((timestamp - origin["timestamp"]) / 1000, 0.001)
        wall.migration_distance = distance
        wall.migration_velocity = distance / elapsed
        wall.migration_confidence = clamp(score * 100) / 100
        toward = abs(wall.price - (self.current_price or wall.price)) < abs(origin["price"] - (self.current_price or origin["price"]))
        wall.migration_direction = "TOWARD_PRICE" if toward else "AWAY_FROM_PRICE"
        event = {"side": wall.side, "from_price": origin["price"], "to_price": wall.price, "distance": distance, "velocity": wall.migration_velocity, "direction": wall.migration_direction, "confidence": wall.migration_confidence, "inference": "PATTERN_MATCH; NO ORDER_ID"}
        self.migration_events.appendleft({"event_time": timestamp, **event})
        self._emit("LIQUIDITY_MIGRATION", timestamp, **event)

    def _touch_walls(self, timestamp: int) -> None:
        if self.current_price is None:
            return
        threshold = max(0.5, (self.last_atr or self.current_price * 0.001) * 0.025)
        for wall in self.walls.values():
            distance = abs(wall.price - self.current_price)
            if distance <= threshold:
                if wall.last_touch_ts is None or timestamp - wall.last_touch_ts > 750:
                    wall.touch_count += 1
                    wall.last_touch_ts = timestamp
            elif wall.last_touch_ts and timestamp - wall.last_touch_ts < 5_000 and distance > threshold * 2:
                if wall.last_reaction_ts is None or timestamp - wall.last_reaction_ts > 2_000:
                    wall.reaction_count += 1
                    wall.last_reaction_ts = timestamp
                    wall.reaction_strength = max(wall.reaction_strength, distance / max(self.last_atr or self.current_price * 0.001, EPS) * 100)

    def _price_dynamics(self, timestamp: int) -> dict[str, float]:
        if not self.price_history:
            return {"velocity_250ms": 0.0, "velocity_1s": 0.0, "acceleration": 0.0, "change_1s": 0.0, "change_3s": 0.0}
        latest_time, latest_price = self.price_history[-1]

        def at_or_before(age_ms: int) -> tuple[int, float]:
            target = timestamp - age_ms
            for row in reversed(self.price_history):
                if row[0] <= target:
                    return row
            return self.price_history[0]

        row_250, row_1s, row_2s, row_3s = at_or_before(250), at_or_before(1_000), at_or_before(2_000), at_or_before(3_000)
        velocity_250 = (latest_price - row_250[1]) / max((latest_time - row_250[0]) / 1000, 0.001)
        velocity_1s = (latest_price - row_1s[1]) / max((latest_time - row_1s[0]) / 1000, 0.001)
        prior_velocity = (row_1s[1] - row_2s[1]) / max((row_1s[0] - row_2s[0]) / 1000, 0.001)
        return {"velocity_250ms": velocity_250, "velocity_1s": velocity_1s, "acceleration": velocity_1s - prior_velocity, "change_1s": latest_price - row_1s[1], "change_3s": latest_price - row_3s[1]}

    def _update_wall_scores(self, wall: LiquidityWall, current: float | None, atr_value: float | None, timestamp: int, significant_threshold: float = 0.0, dynamics: dict[str, float] | None = None) -> None:
        wall.observe_time(timestamp, significant_threshold)
        if wall.visible:
            wall.last_seen_ts = timestamp
        if current:
            wall.distance_usd = wall.price - current
            wall.distance_pct = wall.distance_usd / current * 100
            wall.distance_atr = abs(wall.distance_usd) / max(atr_value or current * 0.001, EPS)
            wall.tracking_zone = "HOT" if wall.distance_atr <= 0.5 else "ACTIVE" if wall.distance_atr <= 2 else "STRUCTURAL"
        dynamics = dynamics or self._price_dynamics(timestamp)
        velocity = dynamics.get("velocity_1s", 0.0)
        wall.price_approach_velocity = velocity if wall.side == "ASK" else -velocity
        lifetime_seconds = wall.lifetime_ms / 1000
        near_max_ratio = wall.time_near_max_size_ms / max(wall.lifetime_ms, 1)
        significant_ratio = wall.time_above_significant_threshold_ms / max(wall.lifetime_ms, 1)
        wall.persistence_score = clamp(min(lifetime_seconds / 90, 1) * 35 + wall.survival_ratio * 25 + near_max_ratio * 20 + significant_ratio * 20)
        wall.pull_score = clamp(wall.pull_ratio * 65 + min(wall.repeat_pull_count / 3, 1) * 20 + (10 if wall.pull_count else 0) + wall.approach_sensitivity * 0.15)
        wall.replenishment_score = clamp(min(wall.replenishment_count / 4, 1) * 55 + min(wall.replenishment_ratio, 1.5) / 1.5 * 45)
        wall.iceberg_score = clamp(max(0, wall.displayed_turnover_ratio - 1) * 38 + wall.replenishment_score * 0.52 + min(wall.replenishment_count, 5) * 2)
        wall.hidden_liquidity_estimate = max(0.0, wall.estimated_executed_qty - wall.max_qty) if wall.visible else max(0.0, wall.estimated_executed_qty - wall.total_removed_qty)
        execution_ratio = wall.estimated_executed_qty / max(wall.max_qty, EPS)
        wall.absorption_score = clamp(min(execution_ratio, 2) / 2 * 35 + wall.replenishment_score * 0.35 + min(wall.reaction_count / 3, 1) * 20 + (100 - wall.pull_score) * 0.10)
        wall.real_liquidity_score = clamp(wall.persistence_score * 0.30 + min(execution_ratio, 1) * 25 + (100 - wall.pull_score) * 0.25 + wall.absorption_score * 0.20)
        wall.break_score = clamp(55 + wall.max_depletion_pct * 25 - wall.replenishment_score * 0.25 - wall.iceberg_score * 0.20 - wall.persistence_score * 0.15)
        pull_adjustment = 1 - min(wall.pull_score / 100 * self.effective_policy["pull_discount"], self.effective_policy["pull_discount"])
        replenishment_adjustment = 1 + wall.replenishment_score / 100 * self.effective_policy["replenishment_boost"]
        persistence_adjustment = self.effective_policy["persistence_floor"] + (1 - self.effective_policy["persistence_floor"]) * wall.persistence_score / 100
        wall.effective_liquidity_estimate = wall.current_qty * pull_adjustment * replenishment_adjustment * persistence_adjustment
        wall.effective_liquidity_confidence = clamp(30 + wall.reconciliation_confidence * 25 + wall.persistence_score * 0.25 + min(wall.lifetime_ms / 60_000, 1) * 20) / 100
        wall.confidence = clamp(20 + wall.persistence_score * 0.30 + (100 - wall.pull_score) * 0.20 + wall.reconciliation_confidence * 25 + min(wall.depletion_count / 5, 1) * 15) / 100
        if wall.migration_confidence >= 0.65:
            wall.role = "MOVING_RESISTANCE" if wall.side == "ASK" else "MOVING_SUPPORT"
        elif wall.pull_score >= 70:
            wall.role = "SPOOF_LIKE"
        elif wall.absorption_score >= 65:
            wall.role = "ABSORPTION_ZONE"
        elif wall.break_score >= 68 and wall.replenishment_score < 35:
            wall.role = "BREAKOUT_GATE"
        elif wall.real_liquidity_score >= 55:
            wall.role = "RESISTANCE" if wall.side == "ASK" else "SUPPORT"
        else:
            wall.role = "UNCERTAIN"

    def _prune_walls(self, timestamp: int) -> None:
        stale = [key for key, wall in self.walls.items() if not wall.visible and timestamp - wall.last_seen_ts > 300_000]
        for key in stale:
            self.walls.pop(key, None)
        if len(self.walls) > 650:
            ordered = sorted(self.walls.items(), key=lambda item: (item[1].visible, item[1].importance_score, item[1].persistence_score), reverse=True)
            self.walls = dict(ordered[:650])

    def _book_metrics(self, book: Any, atr_value: float, timestamp: int) -> dict[str, Any]:
        if not book.valid:
            return {"valid": False}
        bids = sorted(((float(price), float(qty)) for price, qty in book.bids.items()), reverse=True)
        asks = sorted(((float(price), float(qty)) for price, qty in book.asks.items()))
        if not bids or not asks or bids[0][0] >= asks[0][0]:
            return {"valid": False}
        mid = (bids[0][0] + asks[0][0]) / 2
        microprice = (asks[0][0] * bids[0][1] + bids[0][0] * asks[0][1]) / max(bids[0][1] + asks[0][1], EPS)
        ranges: dict[str, float] = {}
        depth: dict[str, dict[str, float]] = {}
        for label, count in (("top5", 5), ("top10", 10), ("top25", 25), ("top50", 50)):
            bid_qty, ask_qty = sum(qty for _, qty in bids[:count]), sum(qty for _, qty in asks[:count])
            ranges[label] = (bid_qty - ask_qty) / max(bid_qty + ask_qty, EPS)
            depth[label] = {"bid_qty": bid_qty, "ask_qty": ask_qty}
        for label, multiplier in (("0.1atr", 0.1), ("0.25atr", 0.25), ("0.5atr", 0.5), ("1atr", 1.0)):
            distance = atr_value * multiplier
            bid_qty = sum(qty for price, qty in bids if price >= mid - distance)
            ask_qty = sum(qty for price, qty in asks if price <= mid + distance)
            ranges[label] = (bid_qty - ask_qty) / max(bid_qty + ask_qty, EPS)
            depth[label] = {"bid_qty": bid_qty, "ask_qty": ask_qty}

        if not self.book_history or self.book_history[-1]["timestamp"] != self.last_depth_at:
            previous = self.book_history[-1] if self.book_history else None
            elapsed = max(((self.last_depth_at or timestamp) - previous["timestamp"]) / 1000, 0.001) if previous else 1.0
            velocity = (ranges["top10"] - previous["top10"]) / elapsed if previous else 0.0
            previous_velocity = previous.get("velocity", 0.0) if previous else 0.0
            self.book_history.append({"timestamp": self.last_depth_at or timestamp, "top10": ranges["top10"], "velocity": velocity, "acceleration": (velocity - previous_velocity) / elapsed})
        history = self.book_history[-1] if self.book_history else {"velocity": 0.0, "acceleration": 0.0}
        return {
            "valid": True,
            "mid_price": mid,
            "microprice": microprice,
            "microprice_bias_usd": microprice - mid,
            "microprice_bias_bps": (microprice - mid) / mid * 10_000,
            "spread": asks[0][0] - bids[0][0],
            "spread_bps": (asks[0][0] - bids[0][0]) / mid * 10_000,
            "obi": ranges,
            "depth_bands": depth,
            "obi_velocity": history["velocity"],
            "obi_acceleration": history["acceleration"],
            "bid": bids,
            "ask": asks,
        }

    def aggression(self, timestamp: int) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for window in WINDOWS:
            rows = []
            for trade in reversed(self.trades):
                age = timestamp - int(trade.get("received_time", timestamp))
                if age > window:
                    break
                if age >= -200:
                    rows.append(trade)
            buy_rows = [item for item in rows if item.get("buy")]
            sell_rows = [item for item in rows if not item.get("buy")]
            buy_qty = sum(safe_float(item.get("quantity")) for item in buy_rows)
            sell_qty = sum(safe_float(item.get("quantity")) for item in sell_rows)
            buy_notional = sum(safe_float(item.get("notional")) for item in buy_rows)
            sell_notional = sum(safe_float(item.get("notional")) for item in sell_rows)
            seconds = max(window / 1000, 0.001)
            result[f"{window}ms"] = {
                "buy": buy_qty,
                "sell": sell_qty,
                "buy_volume": buy_qty,
                "sell_volume": sell_qty,
                "net": buy_qty - sell_qty,
                "net_aggression": buy_qty - sell_qty,
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "net_notional": buy_notional - sell_notional,
                "imbalance": (buy_qty - sell_qty) / max(buy_qty + sell_qty, EPS),
                "buy_trade_count": len(buy_rows),
                "sell_trade_count": len(sell_rows),
                "avg_buy_trade_size": buy_qty / max(len(buy_rows), 1),
                "avg_sell_trade_size": sell_qty / max(len(sell_rows), 1),
                "max_buy_trade": max((safe_float(item.get("quantity")) for item in buy_rows), default=0.0),
                "max_sell_trade": max((safe_float(item.get("quantity")) for item in sell_rows), default=0.0),
                "trade_velocity": len(rows) / seconds,
                "notional_per_second": (buy_notional + sell_notional) / seconds,
            }
        prior = [item for item in self.trades if 1_000 < timestamp - int(item.get("received_time", timestamp)) <= 2_000]
        prior_buy = sum(safe_float(item.get("quantity")) for item in prior if item.get("buy"))
        prior_sell = sum(safe_float(item.get("quantity")) for item in prior if not item.get("buy"))
        prior_imbalance = (prior_buy - prior_sell) / max(prior_buy + prior_sell, EPS)
        result["acceleration"] = result["1000ms"]["imbalance"] - prior_imbalance
        result["net_acceleration"] = result["1000ms"]["net"] - (prior_buy - prior_sell)
        return result

    def _profiles(self, bars: list[dict[str, Any]]) -> dict[str, Any]:
        atr_value = atr(bars) or (self.current_price or 1) * 0.001
        bin_size = max(1.0, atr_value * 0.06)
        volume: defaultdict[float, float] = defaultdict(float)
        tpo: defaultdict[float, int] = defaultdict(int)
        for trade in list(self.trades)[-8_000:]:
            price = safe_float(trade.get("price"))
            key = round(price / bin_size) * bin_size
            volume[key] += safe_float(trade.get("notional"))
        if not volume:
            for row in bars[-240:]:
                typical = (safe_float(row.get("high")) + safe_float(row.get("low")) + safe_float(row.get("close"))) / 3
                volume[round(typical / bin_size) * bin_size] += safe_float(row.get("volume"))
        for row in bars[-240:]:
            low = round(safe_float(row.get("low")) / bin_size) * bin_size
            high = round(safe_float(row.get("high")) / bin_size) * bin_size
            steps = min(500, max(0, round((high - low) / bin_size)))
            for step in range(steps + 1):
                tpo[low + step * bin_size] += 1
        if not volume:
            return {"status": "UNAVAILABLE", "rows": [], "tpo_rows": []}
        rows = sorted(({"price": price, "volume": quantity, "tpo": tpo.get(price, 0)} for price, quantity in volume.items()), key=lambda row: row["price"])
        poc = max(rows, key=lambda row: row["volume"])
        total, target = sum(row["volume"] for row in rows), sum(row["volume"] for row in rows) * 0.70
        included, low_index, high_index = poc["volume"], rows.index(poc), rows.index(poc)
        while included < target and (low_index > 0 or high_index < len(rows) - 1):
            left = rows[low_index - 1]["volume"] if low_index > 0 else -1
            right = rows[high_index + 1]["volume"] if high_index < len(rows) - 1 else -1
            if right >= left and high_index < len(rows) - 1:
                high_index += 1
                included += rows[high_index]["volume"]
            elif low_index > 0:
                low_index -= 1
                included += rows[low_index]["volume"]
            else:
                break
        hvn = sorted(rows, key=lambda row: row["volume"], reverse=True)[:5]
        lvn = sorted(rows, key=lambda row: row["volume"])[:5]
        tpo_rows = sorted(({"price": price, "tpo": count} for price, count in tpo.items()), key=lambda row: row["price"])
        return {
            "status": "OBSERVED_DERIVED",
            "bin_size": bin_size,
            "rows": rows[-160:],
            "tpo_rows": tpo_rows[-160:],
            "poc": poc["price"],
            "hvn": hvn,
            "lvn": lvn,
            "vah": rows[high_index]["price"],
            "val": rows[low_index]["price"],
            "value_area": {"low": rows[low_index]["price"], "high": rows[high_index]["price"], "coverage": 0.70},
        }

    def _structure(self, bars: list[dict[str, Any]]) -> dict[str, Any]:
        if len(bars) < 7:
            return {"status": "INSUFFICIENT_SAMPLE", "events": [], "label": "UNAVAILABLE", "dealing_range": None}
        swing_highs: list[tuple[int, float]] = []
        swing_lows: list[tuple[int, float]] = []
        for index in range(2, len(bars) - 2):
            highs = [safe_float(bars[item].get("high")) for item in range(index - 2, index + 3)]
            lows = [safe_float(bars[item].get("low")) for item in range(index - 2, index + 3)]
            if highs[2] == max(highs):
                swing_highs.append((index, highs[2]))
            if lows[2] == min(lows):
                swing_lows.append((index, lows[2]))
        events: list[dict[str, Any]] = []
        high_label = low_label = None
        if len(swing_highs) >= 2:
            high_label = "HH" if swing_highs[-1][1] > swing_highs[-2][1] else "LH"
            events.append({"type": high_label, "price": swing_highs[-1][1], "time": bars[swing_highs[-1][0]].get("time")})
        if len(swing_lows) >= 2:
            low_label = "HL" if swing_lows[-1][1] > swing_lows[-2][1] else "LL"
            events.append({"type": low_label, "price": swing_lows[-1][1], "time": bars[swing_lows[-1][0]].get("time")})
        trend = "BULLISH" if high_label == "HH" and low_label == "HL" else "BEARISH" if high_label == "LH" and low_label == "LL" else "RANGE"
        last_close = safe_float(bars[-1].get("close"))
        broken_direction = None
        if swing_highs and last_close > swing_highs[-1][1]:
            broken_direction = "UP"
        elif swing_lows and last_close < swing_lows[-1][1]:
            broken_direction = "DOWN"
        if broken_direction:
            is_change = (trend == "BEARISH" and broken_direction == "UP") or (trend == "BULLISH" and broken_direction == "DOWN")
            event_type = "CHOCH" if is_change else "BOS"
            broken_price = swing_highs[-1][1] if broken_direction == "UP" else swing_lows[-1][1]
            events.append({"type": event_type, "alias": "MSB" if is_change else None, "direction": broken_direction, "price": broken_price, "time": bars[-1].get("time")})
        range_high = swing_highs[-1][1] if swing_highs else max(safe_float(row.get("high")) for row in bars[-20:])
        range_low = swing_lows[-1][1] if swing_lows else min(safe_float(row.get("low")) for row in bars[-20:])
        equilibrium = (range_high + range_low) / 2
        return {
            "status": "OBSERVED_DERIVED",
            "label": trend,
            "events": events[-12:],
            "swing_highs": [{"price": price, "time": bars[index].get("time")} for index, price in swing_highs[-8:]],
            "swing_lows": [{"price": price, "time": bars[index].get("time")} for index, price in swing_lows[-8:]],
            "dealing_range": {"low": range_low, "high": range_high, "equilibrium": equilibrium, "discount": {"low": range_low, "high": equilibrium}, "premium": {"low": equilibrium, "high": range_high}},
        }

    def _vacuum(self, book: dict[str, Any], atr_value: float) -> dict[str, Any]:
        if not book.get("valid") or self.current_price is None:
            return {"up": 0.0, "down": 0.0, "strength": 0.0, "vacuum_up": False, "vacuum_down": False}
        current = self.current_price

        def side_metrics(rows: list[tuple[float, float]], direction: str) -> dict[str, float | None]:
            selected = [(price, qty) for price, qty in rows if (price > current if direction == "UP" else price < current) and abs(price - current) <= atr_value]
            near = [(price, qty) for price, qty in selected if abs(price - current) <= atr_value * 0.5]
            total = sum(qty for _, qty in near)
            prices = sorted(price for price, _ in near)
            gaps = [after - before for before, after in zip(prices, prices[1:])]
            return {
                "depth_per_dollar": total / max(atr_value * 0.5, EPS),
                "depth_per_atr": sum(qty for _, qty in selected),
                "gap_size": max(gaps, default=0.0),
                "endpoint": (max(prices) if direction == "UP" else min(prices)) if prices else None,
            }

        up_metrics = side_metrics(book["ask"], "UP")
        down_metrics = side_metrics(book["bid"], "DOWN")
        up_density, down_density = safe_float(up_metrics["depth_per_dollar"]), safe_float(down_metrics["depth_per_dollar"])
        baseline = max((up_density + down_density) / 2, EPS)
        gap_baseline = max(atr_value * 0.02, EPS)
        up_score = clamp((1 - up_density / baseline) * 45 + 45 + min(safe_float(up_metrics["gap_size"]) / gap_baseline, 1) * 10)
        down_score = clamp((1 - down_density / baseline) * 45 + 45 + min(safe_float(down_metrics["gap_size"]) / gap_baseline, 1) * 10)
        return {
            "up": up_score,
            "down": down_score,
            "vacuum_up": up_score >= 65,
            "vacuum_down": down_score >= 65,
            "strength": max(up_score, down_score),
            "up_metrics": {**up_metrics, "low_depth_percentile": up_score},
            "down_metrics": {**down_metrics, "low_depth_percentile": down_score},
        }

    def _effective_liquidity(self) -> list[dict[str, Any]]:
        output = []
        for wall in self.walls.values():
            if not wall.visible:
                continue
            output.append(
                {
                    "side": wall.side,
                    "price": wall.price,
                    "displayed_liquidity": wall.current_qty,
                    "effective_liquidity_estimate": wall.effective_liquidity_estimate,
                    "confidence": wall.effective_liquidity_confidence,
                    "estimate_status": "HEURISTIC_UNCALIBRATED",
                    "features": {"pull_score": wall.pull_score, "replenishment_score": wall.replenishment_score, "persistence_score": wall.persistence_score},
                }
            )
        return sorted(output, key=lambda item: item["effective_liquidity_estimate"], reverse=True)[:120]

    def _migration(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for side in ("BID", "ASK"):
            event = next((item for item in self.migration_events if item["side"] == side), None)
            result[side.lower()] = event or {"direction": "UNAVAILABLE", "velocity": None, "distance": None, "confidence": 0.0, "inference": "PATTERN_MATCH; NO ORDER_ID"}
        return result

    def _clusters(self, book: Any, current: float | None, atr_value: float) -> list[dict[str, Any]]:
        if not book.valid:
            return []
        bin_size = max(1.0, atr_value * 0.06)
        clusters: list[dict[str, Any]] = []
        for side, mapping in (("BID", book.bids), ("ASK", book.asks)):
            grouped: dict[int, list[tuple[float, float]]] = defaultdict(list)
            for raw_price, raw_qty in mapping.items():
                grouped[round(float(raw_price) / bin_size)].append((float(raw_price), float(raw_qty)))
            quantities = [sum(quantity for _, quantity in rows) for rows in grouped.values()]
            for rows in grouped.values():
                quantity = sum(qty for _, qty in rows)
                low, high = min(price for price, _ in rows), max(price for price, _ in rows)
                center = sum(price * qty for price, qty in rows) / max(quantity, EPS)
                members = [self.walls.get(self._key(side, price)) for price, _ in rows]
                tracked = [wall for wall in members if wall]
                persistence = sum(wall.persistence_score for wall in tracked) / max(len(tracked), 1)
                pull = max((wall.pull_score for wall in tracked), default=0.0)
                replenish = max((wall.replenishment_score for wall in tracked), default=0.0)
                absorption = max((wall.absorption_score for wall in tracked), default=0.0)
                iceberg = max((wall.iceberg_score for wall in tracked), default=0.0)
                effective = sum(wall.effective_liquidity_estimate if wall else qty * 0.65 for wall, (_, qty) in zip(members, rows))
                distance = center - (current or center)
                distance_atr = abs(distance) / max(atr_value, EPS)
                clusters.append(
                    {
                        "side": side,
                        "cluster_low": low,
                        "cluster_high": high,
                        "cluster_center": center,
                        "peak_price": max(rows, key=lambda row: row[1])[0],
                        "peak_qty": max(qty for _, qty in rows),
                        "total_qty": quantity,
                        "effective_qty_estimate": effective,
                        "distance": distance,
                        "distance_atr": distance_atr,
                        "tracking_zone": "HOT" if distance_atr <= 0.5 else "ACTIVE" if distance_atr <= 2 else "STRUCTURAL",
                        "size_percentile": percentile(quantities, quantity),
                        "persistence": persistence,
                        "pull_score": pull,
                        "replenishment_score": replenish,
                        "absorption_score": absorption,
                        "iceberg_score": iceberg,
                        "status": "SPOOF-LIKE BEHAVIOUR" if pull >= 70 else "REPLENISHING" if replenish >= 60 else "OBSERVED_CLUSTER",
                    }
                )
        return sorted(clusters, key=lambda row: abs(row["distance"]))[:100]

    def _liquidity_path(self, target: float, side: str, book: dict[str, Any], aggression: dict[str, Any], atr_value: float) -> dict[str, Any]:
        current = self.current_price or target
        rows = book["ask"] if side == "ASK" else book["bid"]
        between = [(price, qty) for price, qty in rows if current <= price <= target] if side == "ASK" else [(price, qty) for price, qty in rows if target <= price <= current]
        weighted = 0.0
        displayed = 0.0
        for price, quantity in between:
            wall = self.walls.get(self._key(side, price))
            effective = wall.effective_liquidity_estimate if wall else quantity * 0.65
            progress = abs(price - current) / max(abs(target - current), EPS)
            distance_weight = 1.0 - 0.45 * progress
            weighted += effective * distance_weight
            displayed += quantity
        one, three = aggression.get("1000ms", {}), aggression.get("3000ms", {})
        field = "buy_volume" if side == "ASK" else "sell_volume"
        aggressive_power = safe_float(one.get(field)) + safe_float(three.get(field)) / 3 * 0.35
        lri = weighted / max(aggressive_power, 0.01)
        lri_score = 100 * lri / (1 + lri)
        path = "EASY" if lri_score < 35 else "MODERATE" if lri_score < 65 else "HARD"
        return {"weighted_liquidity_path": weighted, "displayed_liquidity_path": displayed, "aggressive_power": aggressive_power, "lri": lri, "lri_score": lri_score, "path": path, "threshold_status": "HEURISTIC_UNCALIBRATED"}

    def _targets(self, clusters: list[dict[str, Any]], book: dict[str, Any], aggression: dict[str, Any], profiles: dict[str, Any], structure: dict[str, Any], vacuum: dict[str, Any], atr_value: float, timestamp: int) -> list[dict[str, Any]]:
        if self.current_price is None:
            return []
        current = self.current_price
        raw_candidates: list[dict[str, Any]] = []
        for cluster in clusters:
            if cluster["side"] == "ASK" and cluster["cluster_center"] <= current:
                continue
            if cluster["side"] == "BID" and cluster["cluster_center"] >= current:
                continue
            if cluster["size_percentile"] >= 70 or cluster["distance_atr"] <= 0.5 or cluster["replenishment_score"] >= 55:
                source = "REPLENISHING_WALL" if cluster["replenishment_score"] >= 55 else "LIQUIDITY_CLUSTER"
                raw_candidates.append({"price": cluster["cluster_center"], "side": cluster["side"], "source": source, "cluster": cluster})
        for side, direction, score_key, metrics_key in (("ASK", "UP", "up", "up_metrics"), ("BID", "DOWN", "down", "down_metrics")):
            metrics = vacuum.get(metrics_key, {})
            endpoint = metrics.get("endpoint")
            if safe_float(vacuum.get(score_key)) >= 60 and endpoint:
                raw_candidates.append({"price": endpoint, "side": side, "source": "LIQUIDITY_VACUUM_ENDPOINT", "cluster": {"size_percentile": 50.0, "persistence": 0.0, "pull_score": 0.0, "replenishment_score": 0.0, "absorption_score": 0.0, "iceberg_score": 0.0, "distance_atr": abs(endpoint - current) / max(atr_value, EPS), "direction": direction}})
        if profiles.get("status") == "OBSERVED_DERIVED":
            profile_prices = [(profiles.get("poc"), "POC")] + [(row.get("price"), "HVN") for row in profiles.get("hvn", [])[:3]]
            for profile_price, source in profile_prices:
                if not profile_price or profile_price == current or abs(profile_price - current) > atr_value * 4:
                    continue
                side = "ASK" if profile_price > current else "BID"
                raw_candidates.append({"price": profile_price, "side": side, "source": source, "cluster": {"size_percentile": 70.0, "persistence": 40.0, "pull_score": 0.0, "replenishment_score": 0.0, "absorption_score": 0.0, "iceberg_score": 0.0, "distance_atr": abs(profile_price - current) / max(atr_value, EPS)}})

        deduplicated: dict[str, dict[str, Any]] = {}
        for item in raw_candidates:
            key = f"{item['side']}:{round(item['price'] / max(atr_value * 0.03, 0.5))}"
            previous = deduplicated.get(key)
            if previous is None or item["cluster"].get("size_percentile", 0) > previous["cluster"].get("size_percentile", 0):
                deduplicated[key] = item

        one = aggression.get("1000ms", {})
        dynamics = self._price_dynamics(timestamp)
        targets: list[dict[str, Any]] = []
        for item in deduplicated.values():
            target, side, evidence = safe_float(item["price"]), item["side"], item["cluster"]
            # Ignore levels effectively at the current price; they are BBO
            # churn, not actionable targets.
            if abs(target - current) < max(0.5, atr_value * 0.02):
                continue
            distance_atr = abs(target - current) / max(atr_value, EPS)
            regime = "NEAR" if distance_atr <= 0.5 else "MEDIUM" if distance_atr <= 2 else "FAR"
            aligned = safe_float(one.get("imbalance")) if side == "ASK" else -safe_float(one.get("imbalance"))
            micro_bias = safe_float(book.get("microprice_bias_bps")) * (1 if side == "ASK" else -1)
            vacuum_score = safe_float(vacuum.get("up" if side == "ASK" else "down"))
            path = self._liquidity_path(target, side, book, aggression, atr_value)
            size_pct = safe_float(evidence.get("size_percentile"), 50)
            persistence = safe_float(evidence.get("persistence"))
            pull = safe_float(evidence.get("pull_score"))
            replenish = safe_float(evidence.get("replenishment_score"))
            absorption = safe_float(evidence.get("absorption_score"))
            iceberg = safe_float(evidence.get("iceberg_score"))
            if regime == "NEAR":
                touch_score = 76 - distance_atr * 42 + aligned * 15 + clamp(micro_bias * 8, -12, 12) + vacuum_score * 0.10 + size_pct * 0.08 + persistence * 0.05 - pull * 0.18 - path["lri_score"] * 0.18
            elif regime == "MEDIUM":
                touch_score = 68 - distance_atr * 14 + aligned * 12 + vacuum_score * 0.07 + size_pct * 0.12 + persistence * 0.07 - pull * 0.16 - path["lri_score"] * 0.18
            else:
                structure_alignment = 8 if (structure.get("label") == "BULLISH" and side == "ASK") or (structure.get("label") == "BEARISH" and side == "BID") else -4
                touch_score = 52 - distance_atr * 8 + structure_alignment + size_pct * 0.08 + persistence * 0.05 - pull * 0.14 - path["lri_score"] * 0.10
            break_score = 50 + aligned * 24 + clamp(micro_bias * 7, -10, 10) + safe_float(evidence.get("max_depletion_pct")) * 20 - persistence * 0.20 - replenish * 0.28 - absorption * 0.22 - iceberg * 0.24 - path["lri_score"] * 0.12
            touch_score, break_score = clamp(touch_score), clamp(break_score)
            speed_toward = dynamics["velocity_1s"] if side == "ASK" else -dynamics["velocity_1s"]
            conservative_speed = max(speed_toward, atr_value / 180, 0.01)
            estimated_seconds = abs(target - current) / conservative_speed
            bucket = "<10 sec" if estimated_seconds < 10 else "10–30 sec" if estimated_seconds < 30 else "30–60 sec" if estimated_seconds < 60 else "1–3 min" if estimated_seconds < 180 else "3–5 min" if estimated_seconds < 300 else ">5 min"
            horizon_seconds = {"10s": 10, "30s": 30, "1m": 60, "3m": 180, "5m": 300}
            horizon_scores = {label: round(clamp(touch_score - max(0, estimated_seconds / seconds - 1) * 12), 2) for label, seconds in horizon_seconds.items()}
            role = "POTENTIAL_TARGET"
            if touch_score >= 55 and break_score < 42:
                role = "LIKELY_TARGET_STRONG_RESISTANCE" if side == "ASK" else "LIKELY_TARGET_STRONG_SUPPORT"
            elif touch_score >= 55 and break_score >= 58:
                role = "TRANSITION_CONTINUATION_CANDIDATE"
            elif item["source"] == "LIQUIDITY_VACUUM_ENDPOINT":
                role = "LIQUIDITY_VACUUM_ENDPOINT"
            features = {
                "distance_usd": target - current,
                "distance_pct": (target - current) / current * 100,
                "distance_atr": distance_atr,
                "target_regime": regime,
                "aggression_alignment": aligned,
                "microprice_bias_bps_aligned": micro_bias,
                "vacuum_score": vacuum_score,
                "target_persistence": persistence,
                "target_pull_score": pull,
                "target_replenishment_score": replenish,
                "lri": path["lri"],
                "lri_score": path["lri_score"],
                "liquidity_path": path["path"],
            }
            targets.append(
                {
                    "price": target,
                    "side": side,
                    "source": item["source"],
                    "role": role,
                    "target_regime": regime,
                    "touch_score": round(touch_score, 2),
                    "touch_scores_by_horizon": horizon_scores,
                    "break_score": round(break_score, 2),
                    "p_touch": {horizon: None for horizon in TOUCH_HORIZONS},
                    "p_break_given_touch": None,
                    "probability_status": "UNCALIBRATED",
                    "median_ettt_sec": None,
                    "p25_ettt_sec": None,
                    "p75_ettt_sec": None,
                    "estimated_time_bucket": bucket,
                    "lri": round(path["lri"], 4),
                    "lri_score": round(path["lri_score"], 2),
                    "liquidity_path": path["path"],
                    "confidence": round(clamp(25 + persistence * 0.25 + (100 - pull) * 0.20 + size_pct * 0.20) / 100, 3),
                    "features": features,
                    "evidence": evidence,
                }
            )
        targets = sorted(targets, key=lambda item: (item["touch_score"], -abs(item["price"] - current)), reverse=True)[:16]
        self.outcomes.register(targets, timestamp, current)
        return targets

    def _detectors(self, aggression: dict[str, Any], book: dict[str, Any], profiles: dict[str, Any], vacuum: dict[str, Any], atr_value: float, timestamp: int) -> dict[str, Any]:
        one, three = aggression.get("1000ms", {}), aggression.get("3000ms", {})
        dynamics = self._price_dynamics(timestamp)
        ask_walls = [wall for wall in self.walls.values() if wall.side == "ASK" and wall.visible]
        bid_walls = [wall for wall in self.walls.values() if wall.side == "BID" and wall.visible]
        best_ask = max(ask_walls, key=lambda wall: wall.absorption_score, default=None)
        best_bid = max(bid_walls, key=lambda wall: wall.absorption_score, default=None)
        buy_volume, sell_volume = safe_float(one.get("buy_volume")), safe_float(one.get("sell_volume"))
        price_change = dynamics["change_1s"]
        normalized_impact = abs(price_change) / max(atr_value, EPS)
        buy_impact_per_btc = abs(price_change) / max(buy_volume, EPS)
        sell_impact_per_btc = abs(price_change) / max(sell_volume, EPS)
        sell_absorption = safe_float(best_ask.absorption_score if best_ask else 0)
        buy_absorption = safe_float(best_bid.absorption_score if best_bid else 0)
        if buy_volume > sell_volume:
            sell_absorption = clamp(sell_absorption * 0.55 + max(one.get("imbalance", 0), 0) * 25 + clamp((0.20 - normalized_impact) / 0.20 * 20) + safe_float(best_ask.replenishment_score if best_ask else 0) * 0.20)
        if sell_volume > buy_volume:
            buy_absorption = clamp(buy_absorption * 0.55 + max(-one.get("imbalance", 0), 0) * 25 + clamp((0.20 - normalized_impact) / 0.20 * 20) + safe_float(best_bid.replenishment_score if best_bid else 0) * 0.20)
        exhaustion = clamp(abs(three.get("imbalance", 0)) * 45 + max(0, -abs(aggression.get("acceleration", 0))) * 15 + (30 if normalized_impact < 0.08 else 0))
        trapped = clamp(abs(one.get("imbalance", 0)) * 50 + (40 if (one.get("imbalance", 0) > 0 and price_change < 0) or (one.get("imbalance", 0) < 0 and price_change > 0) else 0))
        spoof_like = max((wall.pull_score for wall in self.walls.values()), default=0.0)
        max_ask_replenishment = max((wall.replenishment_score for wall in ask_walls), default=0.0)
        max_bid_replenishment = max((wall.replenishment_score for wall in bid_walls), default=0.0)
        ask_depletion = max((wall.max_depletion_pct * 100 for wall in ask_walls), default=0.0)
        bid_depletion = max((wall.max_depletion_pct * 100 for wall in bid_walls), default=0.0)
        interaction = "BALANCED_MIXED"
        if one.get("imbalance", 0) >= 0.25 and normalized_impact < 0.10 and max_ask_replenishment >= 55:
            interaction = "SELL_ABSORPTION"
        elif one.get("imbalance", 0) >= 0.25 and ask_depletion >= 45 and max_ask_replenishment < 35 and book.get("microprice_bias_bps", 0) > 0:
            interaction = "BULLISH_BREAKOUT_PRESSURE"
        elif one.get("imbalance", 0) <= -0.25 and normalized_impact < 0.10 and max_bid_replenishment >= 55:
            interaction = "BUY_ABSORPTION"
        elif one.get("imbalance", 0) <= -0.25 and bid_depletion >= 45 and max_bid_replenishment < 35 and book.get("microprice_bias_bps", 0) < 0:
            interaction = "BEARISH_BREAKOUT_PRESSURE"
        elasticity = sorted(({"side": wall.side, "price": wall.price, "state": wall.elasticity_state, "elasticity": wall.liquidity_elasticity, "approach_sensitivity": wall.approach_sensitivity} for wall in self.walls.values() if wall.visible), key=lambda item: item["approach_sensitivity"], reverse=True)[:20]
        buy_efficiency = buy_volume / max(normalized_impact, 0.01) if buy_volume > EPS else 0.0
        sell_efficiency = sell_volume / max(normalized_impact, 0.01) if sell_volume > EPS else 0.0
        return {
            "interaction_state": interaction,
            "absorption": {
                "buy_absorption_score": round(buy_absorption, 2),
                "sell_absorption_score": round(sell_absorption, 2),
                "price_impact_per_btc_buy": buy_impact_per_btc,
                "price_impact_per_btc_sell": sell_impact_per_btc,
                "absorption_efficiency_buy": round(buy_efficiency, 4),
                "absorption_efficiency_sell": round(sell_efficiency, 4),
                "evidence": {
                    "aggressive_buy_volume": buy_volume,
                    "aggressive_sell_volume": sell_volume,
                    "price_change": price_change,
                    "price_change_atr": price_change / max(atr_value, EPS),
                    "ask_executed_at_level": best_ask.estimated_executed_qty if best_ask else 0,
                    "ask_replenished_qty": best_ask.total_replenished_qty if best_ask else 0,
                    "ask_wall_survival": best_ask.survival_ratio if best_ask else 0,
                    "bid_executed_at_level": best_bid.estimated_executed_qty if best_bid else 0,
                    "bid_replenished_qty": best_bid.total_replenished_qty if best_bid else 0,
                    "bid_wall_survival": best_bid.survival_ratio if best_bid else 0,
                },
            },
            "exhaustion": {"score": round(exhaustion, 2), "status": "HIGH" if exhaustion >= 70 else "WATCH" if exhaustion >= 45 else "LOW", "evidence": {"aggression_3s": three.get("imbalance", 0), "aggression_acceleration": aggression.get("acceleration", 0), "price_change_atr": normalized_impact}},
            "trapped_traders": {"score": round(trapped, 2), "status": "WATCH" if trapped >= 55 else "LOW", "evidence": {"aggression_imbalance": one.get("imbalance", 0), "price_change": price_change}},
            "spoof_like": {"score": round(spoof_like, 2), "label": "HIGH PULL-BEFORE-TOUCH RISK" if spoof_like >= 70 else "SPOOF-LIKE WATCH" if spoof_like >= 45 else "LOW EVIDENCE", "legal_status": "BEHAVIOUR SCORE ONLY; NOT PROOF OF MANIPULATION"},
            "manipulation_confirmation": {"status": "UNAVAILABLE", "reason": "Aggregated L2 has no order identity or trader attribution"},
            "replenishment": {"ask_score": max_ask_replenishment, "bid_score": max_bid_replenishment},
            "migration": self._migration(),
            "elasticity": elasticity,
            "vacuum": vacuum,
            "effective_liquidity": self._effective_liquidity(),
            "profiles": profiles,
        }

    def indicators(self, bars: list[dict[str, Any]], buckets: list[dict[str, Any]], timeframe: str = "1m") -> dict[str, Any]:
        minutes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}.get(timeframe, 1)
        selected = aggregate_bars(bars, minutes)
        closes = [safe_float(row.get("close")) for row in selected]
        volumes = [safe_float(row.get("volume")) for row in selected]
        typical_prices = [(safe_float(row.get("high")) + safe_float(row.get("low")) + safe_float(row.get("close"))) / 3 for row in selected]
        vwap_value = sum(price * volume for price, volume in zip(typical_prices, volumes)) / max(sum(volumes), EPS) if selected else None

        period_ms = minutes * 60_000
        grouped_delta: dict[int, dict[str, float]] = {}
        for bucket in buckets:
            timestamp = int(safe_float(bucket.get("time")))
            key = timestamp // period_ms * period_ms
            item = grouped_delta.setdefault(key, {"time": key, "buy": 0.0, "sell": 0.0, "count": 0.0})
            item["buy"] += safe_float(bucket.get("buy"))
            item["sell"] += safe_float(bucket.get("sell"))
            item["count"] += safe_float(bucket.get("count"))
        delta_rows = [grouped_delta[key] for key in sorted(grouped_delta)]
        running_cvd = 0.0
        delta_series = []
        for item in delta_rows:
            delta_value = item["buy"] - item["sell"]
            running_cvd += delta_value
            delta_series.append({**item, "delta": delta_value, "cvd": running_cvd})
        current_delta = delta_series[-1]["delta"] if delta_series else 0.0

        fast, slow = ema(closes, 12), ema(closes, 26)
        macd_values = [fast[index] - slow[index] for index in range(min(len(fast), len(slow)))] if closes else []
        signal_values = ema(macd_values, 9)
        rsi_values = rsi_series(closes)
        indicator_series = []
        for index, row in enumerate(selected):
            indicator_series.append(
                {
                    "time": row.get("time"),
                    "rsi": rsi_values[index] if index < len(rsi_values) else None,
                    "macd": macd_values[index] if index < len(macd_values) else None,
                    "macd_signal": signal_values[index] if index < len(signal_values) else None,
                    "macd_histogram": macd_values[index] - signal_values[index] if index < len(macd_values) and index < len(signal_values) else None,
                }
            )
        macd_line = macd_values[-1] if macd_values else None
        signal = signal_values[-1] if signal_values else None
        return {
            "timeframe": timeframe,
            "bars": selected[-500:],
            "vwap": vwap_value,
            "volume": volumes[-1] if volumes else 0.0,
            "volume_total": sum(volumes),
            "delta": current_delta,
            "cvd": running_cvd,
            "delta_series": delta_series[-500:],
            "rsi": next((value for value in reversed(rsi_values) if value is not None), None),
            "macd": {"line": macd_line, "signal": signal, "histogram": macd_line - signal if macd_line is not None and signal is not None else None},
            "indicator_series": indicator_series[-500:],
            "atr": atr(selected),
        }

    def _interpretation(self, direction: str, detectors: dict[str, Any], targets: list[dict[str, Any]], book: dict[str, Any]) -> dict[str, Any]:
        target = targets[0] if targets else None
        interaction = detectors.get("interaction_state", "BALANCED_MIXED")
        if not target:
            return {"state": interaction, "headline": direction, "nearest_target": None, "message": "Validated flow exists, but no sufficiently significant liquidity target is available.", "potential_path": [], "invalidation": ["sequence invalid", "trade feed stale"]}
        side_word = "upside" if target["side"] == "ASK" else "downside"
        message = f"{interaction}: the highest-ranked {side_word} target is {target['price']:.1f}. Touch evidence score is {target['touch_score']:.1f}; conditional break evidence score is {target['break_score']:.1f}. These are not probabilities."
        path = [self.current_price, target["price"]]
        if target.get("liquidity_path") == "EASY":
            message += " Displayed effective liquidity is relatively thin along the path."
        elif target.get("liquidity_path") == "HARD":
            message += " Opposing effective liquidity makes the path difficult."
        invalidation = ["aggression alignment reverses", "microprice crosses against target", "supporting liquidity pulls", "local-book integrity degrades"]
        return {"state": interaction, "headline": direction, "nearest_target": target["price"], "target_side": target["side"], "message": message, "potential_path": path, "invalidation": invalidation, "probability_status": "UNCALIBRATED"}

    def reconciliation_error(self) -> float:
        return self.unknown_removed_qty / max(self.depth_removed_qty, EPS)

    def snapshot(self, book: Any, bars: list[dict[str, Any]], buckets: list[dict[str, Any]], price: float | None, timestamp: int, timeframe: str = "1m", data_integrity_ok: bool = True) -> dict[str, Any]:
        started = perf_counter()
        if price is not None:
            self.on_price(price, timestamp)
        indicators = self.indicators(bars, buckets, timeframe)
        atr_value = indicators.get("atr") or self.last_atr or (price or 1) * 0.001
        self.last_atr = atr_value
        if not book.valid or not data_integrity_ok:
            self.last_snapshot_latency_ms = (perf_counter() - started) * 1000
            return {
                "status": "SUPPRESSED",
                "health_state": "DEGRADED",
                "trusted": False,
                "probability_status": "UNAVAILABLE",
                "reason": "Local order-book sequence is invalid/stale or trade execution feed is stale; detector output suppressed",
                "indicators": indicators,
                "targets": [],
                "walls": [],
                "clusters": [],
                "events": list(self.events)[:100],
                "calibration": self.outcomes.summary(),
            }

        book_metrics = self._book_metrics(book, atr_value, timestamp)
        if not book_metrics.get("valid"):
            return {"status": "SUPPRESSED", "health_state": "DEGRADED", "trusted": False, "probability_status": "UNAVAILABLE", "reason": "Crossed or empty local order book", "indicators": indicators, "targets": [], "walls": [], "clusters": []}
        context = self._significance_context()
        dynamics = self._price_dynamics(timestamp)
        for wall in self.walls.values():
            self._update_wall_scores(wall, price, atr_value, timestamp, context.get(wall.side, {}).get("threshold", 0.0), dynamics)
        profiles = self._profiles(bars)
        structure = self._structure(indicators["bars"])
        clusters = self._clusters(book, price, atr_value)
        aggression = self.aggression(timestamp)
        vacuum = self._vacuum(book_metrics, atr_value)
        targets = self._targets(clusters, book_metrics, aggression, profiles, structure, vacuum, atr_value, timestamp)
        detectors = self._detectors(aggression, book_metrics, profiles, vacuum, atr_value, timestamp)
        direction = "BULLISH PRESSURE" if aggression.get("1000ms", {}).get("imbalance", 0) > 0.2 and book_metrics.get("obi", {}).get("top10", 0) > 0 else "BEARISH PRESSURE" if aggression.get("1000ms", {}).get("imbalance", 0) < -0.2 and book_metrics.get("obi", {}).get("top10", 0) < 0 else "BALANCED / MIXED"
        interpretation = self._interpretation(direction, detectors, targets, book_metrics)
        tracking_zones = {zone: sum(1 for wall in self.walls.values() if wall.visible and wall.tracking_zone == zone) for zone in ("HOT", "ACTIVE", "STRUCTURAL")}
        while self.depth_event_times and timestamp - self.depth_event_times[0] > 1_000:
            self.depth_event_times.popleft()
        self.last_snapshot_latency_ms = (perf_counter() - started) * 1000
        return {
            "status": "LIVE",
            "health_state": "OK",
            "trusted": True,
            "generated_at": timestamp,
            "symbol": self.symbol,
            "probability_status": "UNCALIBRATED",
            "model_contract": {"scores": "HEURISTIC_0_100", "probabilities": "NOT_PUBLISHED", "touch_break_separated": True, "spoof_label": "BEHAVIOUR_INFERENCE_ONLY"},
            "direction": direction,
            "current_price": price,
            "interpretation": interpretation,
            "microstructure": book_metrics,
            "aggression": aggression,
            "indicators": indicators,
            "profiles": profiles,
            "structure": structure,
            "detectors": detectors,
            "clusters": clusters,
            "walls": [wall.as_dict() for wall in sorted(self.walls.values(), key=lambda item: (not item.visible, abs(item.distance_usd), -item.importance_score))[:160]],
            "targets": targets,
            "tracking_zones": tracking_zones,
            "events": list(self.events)[:120],
            "calibration": self.outcomes.summary(),
            "performance": {
                "depth_events_sec": len(self.depth_event_times),
                "detector_latency_ms": round(self.last_latency_ms, 3),
                "snapshot_latency_ms": round(self.last_snapshot_latency_ms, 3),
                "tracked_walls": len(self.walls),
                "base_levels": len(self.levels),
                "sequence_resets": self.reset_count,
                "trade_depth_reconciliation_error": round(self.reconciliation_error(), 6),
            },
        }
