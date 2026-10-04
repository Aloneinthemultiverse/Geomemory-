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


def geohash_bounds(code: str) -> tuple[float, float, float, float]:
    """(lat_lo, lat_hi, lon_lo, lon_hi) of a geohash cell."""
    lat_lo, lat_hi, lon_lo, lon_hi = -90.0, 90.0, -180.0, 180.0
    even = True
    for ch in code:
        v = _BASE32.index(ch)
        for bit in (16, 8, 4, 2, 1):
            if even:
                mid = (lon_lo + lon_hi) / 2
                lon_lo, lon_hi = (mid, lon_hi) if v & bit else (lon_lo, mid)
            else:
                mid = (lat_lo + lat_hi) / 2
                lat_lo, lat_hi = (mid, lat_hi) if v & bit else (lat_lo, mid)
            even = not even
    return lat_lo, lat_hi, lon_lo, lon_hi


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


class ScanIndex(SpatialIndex):
    """No index: every query scans everything. Lower bound for comparisons."""

    def __init__(self) -> None:
        self._pts: list[tuple[str, float, float]] = []

    def insert(self, key: str, p: Point) -> None:
        self._pts.append((key, p.lat, p.lon))

    def candidates_in_bbox(self, lat_lo, lat_hi, lon_lo, lon_hi) -> Iterator[str]:
        for k, la, lo in self._pts:
            if lat_lo <= la <= lat_hi and lon_lo <= lo <= lon_hi:
                yield k


class GeohashIndex(SpatialIndex):
    """Buckets by geohash; queries descend only populated prefixes."""

    def __init__(self, precision: int = 6) -> None:
        self.precision = precision
        self._buckets: dict[str, list[str]] = defaultdict(list)
        self._prefixes: set[str] = {""}

    def insert(self, key: str, p: Point) -> None:
        code = geohash_encode(p, self.precision)
        if code not in self._buckets:
            for i in range(1, self.precision):
                self._prefixes.add(code[:i])
        self._buckets[code].append(key)

    def candidates_in_bbox(self, lat_lo, lat_hi, lon_lo, lon_hi) -> Iterator[str]:
        stack = [""]
        while stack:
            prefix = stack.pop()
            for ch in _BASE32:
                code = prefix + ch
                last = len(code) == self.precision
                if not (code in self._buckets if last else code in self._prefixes):
                    continue
                a, b, c, d = geohash_bounds(code)
                if b < lat_lo or a > lat_hi or d < lon_lo or c > lon_hi:
                    continue
                if last:
                    yield from self._buckets[code]
                else:
                    stack.append(code)


class QuadTreeIndex(SpatialIndex):
    """Adaptive point quadtree: leaves split when they exceed `capacity`,
    so dense hotspots get fine cells and empty ocean stays coarse."""

    def __init__(self, capacity: int = 32, max_depth: int = 30) -> None:
        self.capacity, self.max_depth = capacity, max_depth
        # node: [lat_lo, lat_hi, lon_lo, lon_hi, depth, points or None, children or None]
        self._root = [-90.0, 90.0, -180.0, 180.0, 0, [], None]

    def insert(self, key: str, p: Point) -> None:
        node = self._root
        while node[6] is not None:
            node = node[6][self._child(node, p.lat, p.lon)]
        node[5].append((key, p.lat, p.lon))
        if len(node[5]) > self.capacity and node[4] < self.max_depth:
            self._split(node)

    @staticmethod
    def _child(node, lat, lon) -> int:
        mlat, mlon = (node[0] + node[1]) / 2, (node[2] + node[3]) / 2
        return (2 if lat >= mlat else 0) + (1 if lon >= mlon else 0)

    def _split(self, node) -> None:
        a, b, c, d, depth = node[:5]
        mlat, mlon = (a + b) / 2, (c + d) / 2
        node[6] = [[a, mlat, c, mlon, depth + 1, [], None], [a, mlat, mlon, d, depth + 1, [], None],
                   [mlat, b, c, mlon, depth + 1, [], None], [mlat, b, mlon, d, depth + 1, [], None]]
        pts, node[5] = node[5], None
        for k, la, lo in pts:
            node[6][self._child(node, la, lo)][5].append((k, la, lo))
        for ch in node[6]:
            if len(ch[5]) > self.capacity and ch[4] < self.max_depth:
                self._split(ch)

    def candidates_in_bbox(self, lat_lo, lat_hi, lon_lo, lon_hi) -> Iterator[str]:
        stack = [self._root]
        while stack:
            n = stack.pop()
            if n[1] < lat_lo or n[0] > lat_hi or n[3] < lon_lo or n[2] > lon_hi:
                continue
            if n[6] is None:
                inside = lat_lo <= n[0] and n[1] <= lat_hi and lon_lo <= n[2] and n[3] <= lon_hi
                for k, la, lo in n[5]:
                    if inside or (lat_lo <= la <= lat_hi and lon_lo <= lo <= lon_hi):
                        yield k
            else:
                stack.extend(n[6])


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


def bbox_for_radius(center: Point, radius_m: float) -> list[tuple[float, float, float, float]]:
    """Bounding boxes covering a radius. Split in two across the antimeridian."""
    dlat = math.degrees(radius_m / EARTH_RADIUS_M)
    lat_lo, lat_hi = max(-90.0, center.lat - dlat), min(90.0, center.lat + dlat)
    cos_lat = math.cos(math.radians(max(abs(lat_lo), abs(lat_hi))))
    if lat_hi >= 90.0 or lat_lo <= -90.0 or cos_lat < 1e-9 \
            or radius_m / (EARTH_RADIUS_M * cos_lat) >= math.pi:
        return [(lat_lo, lat_hi, -180.0, 180.0)]
    dlon = math.degrees(math.asin(min(1.0, math.sin(radius_m / EARTH_RADIUS_M) / cos_lat)))
    dlon = max(dlon, math.degrees(radius_m / (EARTH_RADIUS_M * cos_lat)))
    lon_lo, lon_hi = center.lon - dlon, center.lon + dlon
    if lon_lo < -180.0:
        return [(lat_lo, lat_hi, -180.0, lon_hi), (lat_lo, lat_hi, lon_lo + 360.0, 180.0)]
    if lon_hi > 180.0:
        return [(lat_lo, lat_hi, lon_lo, 180.0), (lat_lo, lat_hi, -180.0, lon_hi - 360.0)]
    return [(lat_lo, lat_hi, lon_lo, lon_hi)]
