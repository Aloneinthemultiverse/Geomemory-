"""In-memory observation store with spatial, temporal and provenance queries
(spec §12, Phase 1 of §26). Single-node reference implementation; later
phases swap in distributed backends behind the same query surface.
"""
from __future__ import annotations

import heapq
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from .index import GridIndex, SpatialIndex, TemporalIndex, bbox_for_radius
from .model import Observation, Point, haversine_m


def _point_in_polygon(p: Point, ring: list[Point]) -> bool:
    inside = False
    n = len(ring)
    for i in range(n):
        a, b = ring[i], ring[(i + 1) % n]
        if (a.lat > p.lat) != (b.lat > p.lat):
            x = a.lon + (p.lat - a.lat) * (b.lon - a.lon) / (b.lat - a.lat)
            if p.lon < x:
                inside = not inside
    return inside


class GeoMemory:
    def __init__(self, spatial_index: SpatialIndex | None = None) -> None:
        self._obs: dict[str, Observation] = {}
        self._spatial = spatial_index or GridIndex()
        self._temporal = TemporalIndex()
        self._by_entity: dict[str, list[str]] = defaultdict(list)
        self._by_source: dict[str, list[str]] = defaultdict(list)

    def __len__(self) -> int:
        return len(self._obs)

    def ingest(self, obs: Observation) -> bool:
        """Store an observation. Duplicate IDs are ignored (idempotent, §18.11)."""
        if obs.observation_id in self._obs:
            return False
        oid = obs.observation_id
        self._obs[oid] = obs
        self._spatial.insert(oid, obs.location)
        self._temporal.insert(oid, obs.timestamp)
        self._by_entity[obs.entity_id].append(oid)
        self._by_source[obs.source_id].append(oid)
        return True

    def ingest_many(self, observations: Iterable[Observation]) -> int:
        return sum(self.ingest(o) for o in observations)

    def get(self, observation_id: str) -> Observation:
        return self._obs[observation_id]

    def entity_ids(self) -> list[str]:
        return sorted(self._by_entity)

    # --- spatial (§12.1-12.3) -------------------------------------------

    def radius(self, center: Point, radius_m: float) -> list[Observation]:
        if radius_m < 0:
            raise ValueError("radius_m must be >= 0")
        seen: set[str] = set()
        out = []
        for box in bbox_for_radius(center, radius_m):
            for k in self._spatial.candidates_in_bbox(*box):
                if k in seen:
                    continue
                seen.add(k)
                o = self._obs[k]
                if haversine_m(center, o.location) <= radius_m:
                    out.append(o)
        return out

    def nearest(self, center: Point, k: int = 10) -> list[Observation]:
        if k <= 0 or not self._obs:
            return []
        # Expand the search radius until k hits are found, then rank exactly.
        r = 100.0
        while r < 2.1e7:
            hits = self.radius(center, r)
            if len(hits) >= k:
                return heapq.nsmallest(
                    k, hits, key=lambda o: (haversine_m(center, o.location), o.observation_id))
            r *= 4
        return heapq.nsmallest(
            k, self._obs.values(),
            key=lambda o: (haversine_m(center, o.location), o.observation_id))

    def within_polygon(self, ring: list[Point]) -> list[Observation]:
        if len(ring) < 3:
            raise ValueError("polygon needs at least 3 vertices")
        lats, lons = [p.lat for p in ring], [p.lon for p in ring]
        cands = self._spatial.candidates_in_bbox(min(lats), max(lats), min(lons), max(lons))
        return [o for o in (self._obs[k] for k in cands) if _point_in_polygon(o.location, ring)]

    # --- temporal (§12.6-12.8) ------------------------------------------

    def between(self, start: datetime, end: datetime) -> list[Observation]:
        if end < start:
            return []
        return [self._obs[k] for k in self._temporal.range(start, end)]

    def radius_between(
        self, center: Point, radius_m: float, start: datetime, end: datetime
    ) -> list[Observation]:
        """Spatio-temporal query (§12.7): runs the more selective filter first.

        The time window's size is known exactly and cheaply (two bisections).
        Spatial candidates are walked lazily and abandoned as soon as they
        outnumber it, so the cost is about min(spatial, temporal), never both.
        """
        if radius_m < 0:
            raise ValueError("radius_m must be >= 0")
        if end < start:
            return []
        n_time = self._temporal.count(start, end)
        seen: set[str] = set()
        spatial_cheaper = True
        for box in bbox_for_radius(center, radius_m):
            for k in self._spatial.candidates_in_bbox(*box):
                seen.add(k)
                if len(seen) > n_time:
                    spatial_cheaper = False
                    break
            if not spatial_cheaper:
                break
        if spatial_cheaper:
            hits = (self._obs[k] for k in seen)
            return sorted((o for o in hits if start <= o.timestamp <= end
                           and haversine_m(center, o.location) <= radius_m),
                          key=lambda o: o.timestamp)
        return [o for o in (self._obs[k] for k in self._temporal.range(start, end))
                if haversine_m(center, o.location) <= radius_m]

    def entity_history(self, entity_id: str) -> list[Observation]:
        return sorted((self._obs[k] for k in self._by_entity.get(entity_id, ())),
                      key=lambda o: o.timestamp)

    def latest(self, entity_id: str) -> Observation | None:
        hist = self.entity_history(entity_id)
        return hist[-1] if hist else None

    def before(self, entity_id: str, t: datetime, window=None) -> list[Observation]:
        """What happened to an entity before time t (spec §22)."""
        return [o for o in self.entity_history(entity_id)
                if o.timestamp < t and (window is None or o.timestamp >= t - window)]

    # --- provenance (§12.10) --------------------------------------------

    def by_source(self, source_id: str) -> list[Observation]:
        return [self._obs[k] for k in self._by_source.get(source_id, ())]

    def lineage(self, observation_id: str) -> list[Observation]:
        """Chain from an observation back through its parents."""
        chain, seen, cur = [], set(), observation_id
        while cur is not None and cur in self._obs and cur not in seen:
            seen.add(cur)
            o = self._obs[cur]
            chain.append(o)
            cur = o.parent_observation
        return chain

    def evidence(self, observations: Iterable[Observation]) -> dict:
        """Structured evidence bundle for agents (spec §13)."""
        obs = list(observations)
        return {
            "count": len(obs),
            "sources": sorted({o.source_id for o in obs}),
            "observations": [
                {
                    "observation_id": o.observation_id,
                    "entity_id": o.entity_id,
                    "event_type": o.event_type,
                    "location": {"lat": o.location.lat, "lon": o.location.lon,
                                 "uncertainty_m": o.spatial_uncertainty_m},
                    "timestamp": o.timestamp.isoformat(),
                    "source_id": o.source_id,
                    "processing_model": o.provenance.processing_model,
                    "confidence": o.confidence,
                }
                for o in sorted(obs, key=lambda o: o.timestamp)
            ],
        }
