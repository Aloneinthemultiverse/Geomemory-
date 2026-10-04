"""Spatial and temporal indexes (spec §16, §18.2).

`SpatialIndex` is the interface every strategy (grid, geohash, H3, R-tree, ...)
implements so they can be benchmarked against each other in E2.
"""
from __future__ import annotations

import bisect
import math
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Iterator

from .model import EARTH_RADIUS_M, Point

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(p: Point, precision: int = 7) -> str:
    lat_lo, lat_hi, lon_lo, lon_hi = -90.0, 90.0, -180.0, 180.0
    out, bits, ch, even = [], 0, 0, True
    while len(out) < precision:
        if even:
            mid = (lon_lo + lon_hi) / 2
            if p.lon >= mid:
                ch, lon_lo = (ch << 1) | 1, mid
            else:
                ch, lon_hi = ch << 1, mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if p.lat >= mid:
                ch, lat_lo = (ch << 1) | 1, mid
            else:
                ch, lat_hi = ch << 1, mid
        even = not even
        bits += 1
        if bits == 5:
            out.append(_BASE32[ch])
            bits, ch = 0, 0
    return "".join(out)


class SpatialIndex(ABC):
    @abstractmethod
    def insert(self, key: str, p: Point) -> None: ...

    @abstractmethod
    def candidates_in_bbox(
        self, lat_lo: float, lat_hi: float, lon_lo: float, lon_hi: float
    ) -> Iterable[str]:
        """Superset of keys whose point may fall in the box (filter step)."""


class GridIndex(SpatialIndex):
    """Uniform lat/lon grid. Simple baseline for the index comparison."""

    def __init__(self, cell_deg: float = 0.01) -> None:
        self.cell_deg = cell_deg
        self._cells: dict[tuple[int, int], list[str]] = defaultdict(list)

    def _cell(self, lat: float, lon: float) -> tuple[int, int]:
        return math.floor(lat / self.cell_deg), math.floor(lon / self.cell_deg)

    def insert(self, key: str, p: Point) -> None:
        self._cells[self._cell(p.lat, p.lon)].append(key)

    def candidates_in_bbox(self, lat_lo, lat_hi, lon_lo, lon_hi) -> Iterator[str]:
        r0, c0 = self._cell(lat_lo, lon_lo)
        r1, c1 = self._cell(lat_hi, lon_hi)
        if (r1 - r0 + 1) * (c1 - c0 + 1) > len(self._cells):
            # Box spans more cells than are populated: scan populated cells.
            for (r, c), keys in self._cells.items():
                if r0 <= r <= r1 and c0 <= c <= c1:
                    yield from keys
            return
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                yield from self._cells.get((r, c), ())


class TemporalIndex:
    """Sorted timestamps supporting O(log n + k) range lookups."""

    def __init__(self) -> None:
        self._times: list[datetime] = []
        self._keys: list[str] = []

    def insert(self, key: str, t: datetime) -> None:
        i = bisect.bisect_right(self._times, t)
        self._times.insert(i, t)
        self._keys.insert(i, key)

    def range(self, start: datetime, end: datetime) -> list[str]:
        """Keys with start <= t <= end."""
        lo = bisect.bisect_left(self._times, start)
        hi = bisect.bisect_right(self._times, end)
        return self._keys[lo:hi]


def bbox_for_radius(center: Point, radius_m: float) -> tuple[float, float, float, float]:
    dlat = math.degrees(radius_m / EARTH_RADIUS_M)
    lat_lo, lat_hi = max(-90.0, center.lat - dlat), min(90.0, center.lat + dlat)
    cos_lat = math.cos(math.radians(max(abs(lat_lo), abs(lat_hi))))
    if cos_lat < 1e-9 or radius_m / EARTH_RADIUS_M / cos_lat >= math.pi:
        return lat_lo, lat_hi, -180.0, 180.0
    dlon = math.degrees(radius_m / (EARTH_RADIUS_M * cos_lat))
    lon_lo, lon_hi = center.lon - dlon, center.lon + dlon
    if lon_lo < -180.0 or lon_hi > 180.0:  # crosses antimeridian; keep it simple
        return lat_lo, lat_hi, -180.0, 180.0
    return lat_lo, lat_hi, lon_lo, lon_hi
