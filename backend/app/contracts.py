from __future__ import annotations

from typing import Any, Literal

from typing_extensions import TypedDict


Status = Literal["LIVE", "LIVE_QUIET", "STALE", "INVALID", "UNAVAILABLE", "ERROR", "WAITING"]


class FeedHealth(TypedDict):
    status: Status
    age_ms: int | None
    timestamp: str | None
    received_timestamp: str | None
    source: str
    methodology: str
    confidence: str
    detail: str


class HealthContract(TypedDict):
    overall: Literal["LIVE", "DEGRADED", "DOWN"]
    market: str
    venue: str
    market_type: str
    decision_authorized: bool
    feeds: dict[str, FeedHealth]
    missing_data: list[str]
    generated_at: str


def unavailable_probability(reason: str = "No verified calibration dataset") -> dict[str, Any]:
    return {"status": "UNAVAILABLE", "probability": None, "score": None, "calibration_status": "NOT_VERIFIED", "reason": reason, "model_version": None}
