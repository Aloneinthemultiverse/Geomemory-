"""Trust layer (spec §11, §18.10, Phase 4): uncertainty fusion, historical
reasoning and pattern detection. Nothing here mutates stored observations;
every derived answer carries the observation ids that support it.
Functions taking `mem` accept a GeoMemory or a Cluster.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Sequence

from .model import EARTH_RADIUS_M, Observation, Point, haversine_m
from .store import GeoMemory

MIN_SIGMA_M = 1.0  # floor so a source claiming 0 m error cannot dominate infinitely


def _to_xyz(p: Point) -> tuple[float, float, float]:
    la, lo = math.radians(p.lat), math.radians(p.lon)
    return math.cos(la) * math.cos(lo), math.cos(la) * math.sin(lo), math.sin(la)


def _from_xyz(x: float, y: float, z: float) -> Point:
    n = math.sqrt(x * x + y * y + z * z)
    lat = math.degrees(math.asin(max(-1.0, min(1.0, z / n))))
    lon = math.degrees(math.atan2(y, x)) if abs(lat) < 90 else 0.0
    return Point(lat, lon)


def observation_variance_m2(o: Observation) -> float:
    """Positional variance: reported uncertainty inflated by low confidence."""
    sigma = max(o.spatial_uncertainty_m, MIN_SIGMA_M)
    return sigma * sigma / max(o.confidence, 1e-3)


@dataclass
class FusedEstimate:
    location: Point
    sigma_m: float
    support: list[str]
    outliers: list[str]
    confidence: float

    def as_dict(self) -> dict:
        return {"lat": self.location.lat, "lon": self.location.lon,
                "uncertainty_m": self.sigma_m, "confidence": self.confidence,
                "support": self.support, "outliers": self.outliers}


def _weighted_mean(obs: Sequence[Observation]) -> tuple[Point, float]:
    sx = sy = sz = sw = 0.0
    for o in obs:
        w = 1.0 / observation_variance_m2(o)
        x, y, z = _to_xyz(o.location)
        sx, sy, sz, sw = sx + w * x, sy + w * y, sz + w * z, sw + w
    if math.sqrt(sx * sx + sy * sy + sz * sz) < 1e-12 * sw:
        raise ValueError("observations are antipodal; location is undefined")
    return _from_xyz(sx, sy, sz), math.sqrt(1.0 / sw)


def _medoid(obs: Sequence[Observation]) -> Observation:
    """Observation minimising the weighted sum of distances to all others.
    Robust starting point: a single far outlier cannot drag it."""
    best, best_cost = obs[0], math.inf
    for c in obs:
        cost = sum(haversine_m(c.location, o.location) / math.sqrt(observation_variance_m2(o))
                   for o in obs)
        if cost < best_cost:
            best, best_cost = c, cost
    return best


def fuse_locations(observations: Iterable[Observation], k_sigma: float = 3.0,
                   max_rounds: int = 10) -> FusedEstimate:
    """Inverse-variance fusion on the sphere with robust outlier rejection.

    Starts from the weighted medoid, then alternates between classifying every
    observation against the current centre and re-fusing the inliers. An
    outlier set is only accepted when it is a strict minority. Outliers are
    excluded from the estimate but reported, never deleted.
    """
    obs = sorted(observations, key=lambda o: o.observation_id)
    if not obs:
        raise ValueError("nothing to fuse")
    bad: set[str] = set()
    if len(obs) > 2:
        # The medoid is itself a noisy observation: carry its uncertainty.
        m = _medoid(obs)
        center, sigma = m.location, math.sqrt(observation_variance_m2(m))
        for _ in range(max_rounds):
            new_bad = {o.observation_id for o in obs
                       if haversine_m(center, o.location)
                       > k_sigma * math.sqrt(observation_variance_m2(o) + sigma * sigma)}
            if len(new_bad) * 2 >= len(obs):
                new_bad = set()  # no clear majority to trust: keep everything
            if new_bad == bad and _ > 0:
                break
            bad = new_bad
            center, sigma = _weighted_mean([o for o in obs if o.observation_id not in bad])
    inliers = [o for o in obs if o.observation_id not in bad]
    outliers = [o for o in obs if o.observation_id in bad]
    center, sigma = _weighted_mean(inliers)
    # Independent sources: probability that at least one is right.
    conf = 1.0 - math.prod(1.0 - o.confidence for o in inliers)
    return FusedEstimate(center, sigma, sorted(o.observation_id for o in inliers),
                         sorted(o.observation_id for o in outliers), conf)


def associate(observations: Iterable[Observation], radius_m: float,
              window: timedelta) -> list[list[Observation]]:
    """Group observations that plausibly describe the same physical event:
    connected components of the (distance <= radius, |dt| <= window) graph."""
    obs = list(observations)
    mem = GeoMemory()
    mem.ingest_many(obs)
    parent = {o.observation_id: o.observation_id for o in obs}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for o in obs:
        for other in mem.radius_between(o.location, radius_m, o.timestamp - window,
                                        o.timestamp + window):
            ra, rb = find(o.observation_id), find(other.observation_id)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)
    groups: dict[str, list[Observation]] = defaultdict(list)
    for o in obs:
        groups[find(o.observation_id)].append(o)
    return sorted((sorted(g, key=lambda o: o.observation_id) for g in groups.values()),
                  key=lambda g: g[0].observation_id)


# --- historical reasoning (§12.8, §21.3) -----------------------------------

@dataclass
class Snapshot:
    """Latest known observation per entity at time t, within a radius."""
    at: datetime
    latest: dict[str, Observation] = field(default_factory=dict)


def snapshot(mem, center: Point, radius_m: float, at: datetime,
             horizon: timedelta = timedelta(days=36500)) -> Snapshot:
    snap = Snapshot(at)
    for o in mem.radius_between(center, radius_m, at - horizon, at):
        cur = snap.latest.get(o.entity_id)
        if cur is None or (o.timestamp, o.observation_id) > (cur.timestamp, cur.observation_id):
            snap.latest[o.entity_id] = o
    return snap


def diff(mem, center: Point, radius_m: float, t1: datetime, t2: datetime,
         horizon: timedelta = timedelta(days=36500)) -> dict:
    """What changed in an area between t1 and t2 (spec §12.8, §21.3)."""
    if t2 < t1:
        raise ValueError("t2 must be >= t1")
    a = snapshot(mem, center, radius_m, t1, horizon)
    b = snapshot(mem, center, radius_m, t2, horizon)
    appeared = sorted(set(b.latest) - set(a.latest))
    changed = []
    for e in sorted(set(a.latest) & set(b.latest)):
        before, after = a.latest[e], b.latest[e]
        if before.observation_id == after.observation_id:
            continue
        moved = haversine_m(before.location, after.location)
        if before.event_type != after.event_type or moved > max(
                before.spatial_uncertainty_m + after.spatial_uncertainty_m, MIN_SIGMA_M):
            changed.append({"entity_id": e, "from": before.event_type, "to": after.event_type,
                            "moved_m": moved,
                            "evidence": [before.observation_id, after.observation_id]})
    return {"appeared": appeared, "changed": changed,
            "unchanged": sorted(set(a.latest) & set(b.latest) - {c["entity_id"] for c in changed})}


# --- pattern detection (§12.11, §21.2, §21.6) -------------------------------

def _cell(p: Point, cell_m: float) -> tuple[int, int]:
    """Equal-area-ish cell: rows by latitude, columns scaled by cos(lat)."""
    row = math.floor(math.radians(p.lat + 90) * EARTH_RADIUS_M / cell_m)
    lat_c = math.radians(-90 + (row + 0.5) * cell_m / EARTH_RADIUS_M * 180 / math.pi)
    circ = 2 * math.pi * EARTH_RADIUS_M * max(math.cos(lat_c), 1e-9)
    ncols = max(1, math.floor(circ / cell_m))
    col = math.floor((p.lon + 180) / 360 * ncols) % ncols
    return row, col


def hotspots(observations: Iterable[Observation], event_type: str, cell_m: float,
             min_count: int) -> list[dict]:
    """Cells where `event_type` repeats at least `min_count` times."""
    cells: dict[tuple[int, int], list[Observation]] = defaultdict(list)
    for o in observations:
        if o.event_type == event_type:
            cells[_cell(o.location, cell_m)].append(o)
    out = [{"cell": c, "count": len(v),
            "entities": sorted({o.entity_id for o in v}),
            "evidence": sorted(o.observation_id for o in v)}
           for c, v in cells.items() if len(v) >= min_count]
    return sorted(out, key=lambda h: (-h["count"], h["cell"]))


def deteriorating(mem, entity_ids: Iterable[str],
                  severity: Sequence[str]) -> list[dict]:
    """Entities whose condition never improves and gets strictly worse
    over their history (spec §21.6: Normal -> Hotspot -> Crack)."""
    rank = {s: i for i, s in enumerate(severity)}
    out = []
    for e in entity_ids:
        hist = [o for o in mem.entity_history(e) if o.event_type in rank]
        levels = [rank[o.event_type] for o in hist]
        if len(levels) >= 2 and levels[-1] > levels[0] and \
                all(b >= a for a, b in zip(levels, levels[1:])):
            out.append({"entity_id": e, "trajectory": [o.event_type for o in hist],
                        "evidence": [o.observation_id for o in hist]})
    return out


def precursors(mem, target_event: str, lookback: timedelta,
               entity_ids: Iterable[str] | None = None) -> list[tuple[str, float]]:
    """For every `target_event`, which event types occurred on the same entity
    in the preceding `lookback`? Returns (event_type, fraction of targets)."""
    ids = list(entity_ids) if entity_ids is not None else mem.entity_ids()
    seen: Counter = Counter()
    n_targets = 0
    for e in ids:
        hist = mem.entity_history(e)
        for i, o in enumerate(hist):
            if o.event_type != target_event:
                continue
            n_targets += 1
            before = {p.event_type for p in hist[:i]
                      if o.timestamp - lookback <= p.timestamp < o.timestamp
                      and p.event_type != target_event}
            seen.update(before)
    if not n_targets:
        return []
    return sorted(((k, v / n_targets) for k, v in seen.items()), key=lambda kv: (-kv[1], kv[0]))


def event_sequence(mem, center: Point, radius_m: float,
                   start: datetime, end: datetime) -> list[str]:
    return [o.event_type for o in mem.radius_between(center, radius_m, start, end)]


def sequence_similarity(a: Sequence[str], b: Sequence[str], n: int = 2) -> float:
    """Jaccard similarity of event n-grams (spec §12.11). 1.0 = same pattern."""
    def grams(s):
        return {tuple(s[i:i + n]) for i in range(len(s) - n + 1)} or ({tuple(s)} if s else set())
    ga, gb = grams(list(a)), grams(list(b))
    if not ga and not gb:
        return 1.0
    return len(ga & gb) / len(ga | gb)
