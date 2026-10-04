"""Canonical data model (spec §7, §10, §11).

Observations are immutable. Conflicting observations about the same entity
coexist; nothing is overwritten (spec §3.4, §11).
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

EARTH_RADIUS_M = 6_371_008.8


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float

    def __post_init__(self) -> None:
        if not (math.isfinite(self.lat) and math.isfinite(self.lon)):
            raise ValueError("coordinates must be finite")
        if not -90.0 <= self.lat <= 90.0:
            raise ValueError(f"latitude out of range: {self.lat}")
        if not -180.0 <= self.lon <= 180.0:
            raise ValueError(f"longitude out of range: {self.lon}")


def haversine_m(a: Point, b: Point) -> float:
    """Great-circle distance in metres."""
    p1, p2 = math.radians(a.lat), math.radians(b.lat)
    dp, dl = p2 - p1, math.radians(b.lon - a.lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(h)))


@dataclass(frozen=True)
class Provenance:
    """Where an observation came from (spec §10)."""
    source_id: str
    processing_model: str | None = None
    inputs: tuple[str, ...] = ()
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Observation:
    entity_id: str
    event_type: str
    location: Point
    timestamp: datetime
    provenance: Provenance
    confidence: float = 1.0
    spatial_uncertainty_m: float = 0.0
    duration: timedelta = timedelta(0)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    parent_observation: str | None = None
    observation_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0, 1]: {self.confidence}")
        if self.spatial_uncertainty_m < 0:
            raise ValueError("spatial_uncertainty_m must be >= 0")
        if self.duration < timedelta(0):
            raise ValueError("duration must be >= 0")

    @property
    def source_id(self) -> str:
        return self.provenance.source_id

    @property
    def end_time(self) -> datetime:
        return self.timestamp + self.duration
