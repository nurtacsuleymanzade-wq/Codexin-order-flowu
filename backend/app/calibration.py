"""Outcome labelling for future empirical probability calibration.

This module does not estimate probabilities.  It records target forecasts and
closes observable labels at fixed horizons so a separate, out-of-sample model
can eventually be trained and calibrated without contaminating the live
heuristic scores.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


HORIZONS_MS: dict[str, int] = {
    "10s": 10_000,
    "30s": 30_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
}


@dataclass
class TargetRecord:
    target_id: str
    side: str
    price: float
    source: str
    created_at: int
    created_price: float
    touch_score: float
    break_score: float
    features: dict[str, Any]
    touched_at: int | None = None
    broken_at: int | None = None
    reacted_at: int | None = None
    labels: dict[str, bool] = field(default_factory=dict)


class TargetOutcomeTracker:
    """Bounded live label ledger; explicitly not a probability model."""

    def __init__(self, max_records: int = 2_000) -> None:
        self.max_records = max_records
        self.records: dict[str, TargetRecord] = {}
        self.archive: deque[dict[str, Any]] = deque()
        self.created_total = 0
        self.closed_label_total = 0

    @staticmethod
    def _key(side: str, price: float) -> str:
        return f"{side}:{price:.1f}"

    def register(self, candidates: list[dict[str, Any]], timestamp: int, current_price: float) -> None:
        for candidate in candidates:
            side = str(candidate.get("side", "")).upper()
            price = float(candidate.get("price", 0))
            if side not in {"ASK", "BID"} or price <= 0:
                continue
            key = self._key(side, price)
            existing = self.records.get(key)
            if existing and timestamp - existing.created_at < HORIZONS_MS["5m"]:
                existing.touch_score = float(candidate.get("touch_score", existing.touch_score))
                existing.break_score = float(candidate.get("break_score", existing.break_score))
                continue
            target_id = f"{timestamp}:{key}"
            record = TargetRecord(
                target_id=target_id,
                side=side,
                price=price,
                source=str(candidate.get("source", "UNKNOWN")),
                created_at=timestamp,
                created_price=current_price,
                touch_score=float(candidate.get("touch_score", 0)),
                break_score=float(candidate.get("break_score", 0)),
                features=dict(candidate.get("features") or candidate.get("evidence") or {}),
            )
            self.records[key] = record
            self.created_total += 1
            self._emit("TARGET_CREATED", timestamp, record, {})
        self._prune(timestamp)

    def on_price(self, price: float, timestamp: int) -> None:
        for record in list(self.records.values()):
            tolerance = max(record.price * 0.00002, 0.1)
            elapsed = timestamp - record.created_at
            touched_now = price >= record.price - tolerance if record.side == "ASK" else price <= record.price + tolerance
            if record.touched_at is None and touched_now:
                record.touched_at = timestamp
                self._emit("TARGET_TOUCHED", timestamp, record, {"elapsed_ms": elapsed, "observed_price": price})

            if record.touched_at is not None and record.broken_at is None:
                break_distance = max(record.price * 0.0002, 1.0)
                broken_now = price >= record.price + break_distance if record.side == "ASK" else price <= record.price - break_distance
                if broken_now:
                    record.broken_at = timestamp
                    self._emit("TARGET_BROKEN", timestamp, record, {"after_touch_ms": timestamp - record.touched_at, "observed_price": price})
                reaction_now = price <= record.price - break_distance if record.side == "ASK" else price >= record.price + break_distance
                if record.reacted_at is None and reaction_now:
                    record.reacted_at = timestamp
                    self._emit("TARGET_REACTION", timestamp, record, {"after_touch_ms": timestamp - record.touched_at, "observed_price": price})

            for label, horizon in HORIZONS_MS.items():
                label_key = f"touch_{label}"
                if elapsed >= horizon and label_key not in record.labels:
                    hit = record.touched_at is not None and record.touched_at <= record.created_at + horizon
                    record.labels[label_key] = hit
                    self.closed_label_total += 1
                    self._emit("TARGET_HORIZON_LABEL", timestamp, record, {"label": label_key, "value": hit, "horizon_ms": horizon})

            if record.touched_at is not None and timestamp - record.touched_at >= 60_000 and "break_after_touch" not in record.labels:
                broke = record.broken_at is not None and record.broken_at <= record.touched_at + 60_000
                record.labels["break_after_touch"] = broke
                record.labels["reaction_after_touch"] = record.reacted_at is not None and record.reacted_at <= record.touched_at + 60_000
                self.closed_label_total += 2
                self._emit(
                    "TARGET_TOUCH_OUTCOME_LABEL",
                    timestamp,
                    record,
                    {"break_after_touch": broke, "reaction_after_touch": record.labels["reaction_after_touch"]},
                )
        self._prune(timestamp)

    def note_pull_before_touch(self, side: str, price: float, timestamp: int) -> None:
        record = self.records.get(self._key(side, price))
        if record and record.touched_at is None and "pull_before_touch" not in record.labels:
            record.labels["pull_before_touch"] = True
            self.closed_label_total += 1
            self._emit("TARGET_PULL_LABEL", timestamp, record, {"pull_before_touch": True})

    def note_replenishment(self, side: str, price: float, timestamp: int) -> None:
        record = self.records.get(self._key(side, price))
        if record and "replenish_after_depletion" not in record.labels:
            record.labels["replenish_after_depletion"] = True
            self.closed_label_total += 1
            self._emit("TARGET_REPLENISHMENT_LABEL", timestamp, record, {"replenish_after_depletion": True})

    def summary(self) -> dict[str, Any]:
        horizons: dict[str, dict[str, int]] = {}
        for horizon in HORIZONS_MS:
            key = f"touch_{horizon}"
            values = [record.labels[key] for record in self.records.values() if key in record.labels]
            horizons[horizon] = {"realized": sum(values), "missed": len(values) - sum(values), "sample_size": len(values)}
        break_values = [record.labels["break_after_touch"] for record in self.records.values() if "break_after_touch" in record.labels]
        return {
            "probability_status": "UNCALIBRATED",
            "dataset_status": "COLLECTING_LABELS",
            "predictions_registered": self.created_total,
            "labels_closed": self.closed_label_total,
            "touch_outcomes": horizons,
            "break_given_touch_outcomes": {"realized": sum(break_values), "missed": len(break_values) - sum(break_values), "sample_size": len(break_values)},
            "brier_score": None,
            "log_loss": None,
            "expected_calibration_error": None,
            "oos_status": "NOT_RUN",
        }

    def drain_archive(self) -> list[dict[str, Any]]:
        rows = list(self.archive)
        self.archive.clear()
        return rows

    def _emit(self, event_type: str, timestamp: int, record: TargetRecord, detail: dict[str, Any]) -> None:
        self.archive.append(
            {
                "event_type": event_type,
                "event_time": timestamp,
                "target_id": record.target_id,
                "side": record.side,
                "price": record.price,
                "source": record.source,
                "created_at": record.created_at,
                "created_price": record.created_price,
                "touch_score": record.touch_score,
                "break_score": record.break_score,
                "probability_status": "UNCALIBRATED",
                "features": record.features,
                **detail,
            }
        )

    def _prune(self, timestamp: int) -> None:
        expiry = HORIZONS_MS["5m"] + 120_000
        stale = [key for key, record in self.records.items() if timestamp - record.created_at > expiry]
        for key in stale:
            self.records.pop(key, None)
        if len(self.records) > self.max_records:
            oldest = sorted(self.records, key=lambda key: self.records[key].created_at)[: len(self.records) - self.max_records]
            for key in oldest:
                self.records.pop(key, None)
