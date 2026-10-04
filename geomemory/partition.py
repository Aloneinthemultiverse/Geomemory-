"""Spatial partitioning strategies (spec §16, experiments E3/E5).

A partitioner maps a point to a partition id and a bounding box to the set of
partitions that may hold points inside it (used to route queries).
"""
from __future__ import annotations

import math
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence

from .model import Point

BBox = tuple[float, float, float, float]  # lat_lo, lat_hi, lon_lo, lon_hi


class Partitioner(ABC):
    n_partitions: int

    @abstractmethod
    def partition_of(self, p: Point) -> int: ...

    @abstractmethod
    def partitions_for_bbox(self, box: BBox) -> set[int]: ...


class GridPartitioner(Partitioner):
    """Fixed lat/lon grid; cells hashed onto partitions. Skew-oblivious baseline."""

    def __init__(self, n_partitions: int, cell_deg: float = 10.0) -> None:
        if n_partitions < 1:
            raise ValueError("n_partitions must be >= 1")
        self.n_partitions = n_partitions
        self.cell_deg = cell_deg
        self._rows = math.ceil(180 / cell_deg)
        self._cols = math.ceil(360 / cell_deg)

    def _cell(self, lat: float, lon: float) -> tuple[int, int]:
        r = min(self._rows - 1, int((lat + 90) // self.cell_deg))
        c = min(self._cols - 1, int((lon + 180) // self.cell_deg))
        return r, c

    def _cell_partition(self, r: int, c: int) -> int:
        return zlib.crc32(f"{r}:{c}".encode()) % self.n_partitions

    def partition_of(self, p: Point) -> int:
        return self._cell_partition(*self._cell(p.lat, p.lon))

    def partitions_for_bbox(self, box: BBox) -> set[int]:
        r0, c0 = self._cell(box[0], box[2])
        r1, c1 = self._cell(box[1], box[3])
        out: set[int] = set()
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                out.add(self._cell_partition(r, c))
                if len(out) == self.n_partitions:
                    return out
        return out


@dataclass(frozen=True)
class _Leaf:
    box: BBox
    pid: int


class KDPartitioner(Partitioner):
    """Adaptive partitioner: recursive median splits over a data sample, so a
    geographic hotspot is split across many partitions (spec §18.8)."""

    def __init__(self, n_partitions: int, sample: Sequence[Point]) -> None:
        if n_partitions < 1:
            raise ValueError("n_partitions must be >= 1")
        self.n_partitions = n_partitions
        self._leaves: list[_Leaf] = []
        self._tree = self._build(list(sample), (-90.0, 90.0, -180.0, 180.0), n_partitions, depth=0)

    def _build(self, pts: list[Point], box: BBox, n: int, depth: int):
        if n == 1:
            leaf = _Leaf(box, len(self._leaves))
            self._leaves.append(leaf)
            return leaf
        n_left = n // 2
        split_lat = depth % 2 == 0
        key = (lambda p: p.lat) if split_lat else (lambda p: p.lon)
        lo, hi = (box[0], box[1]) if split_lat else (box[2], box[3])
        if pts:
            pts.sort(key=key)
            idx = max(0, min(len(pts) - 1, len(pts) * n_left // n))
            split = key(pts[idx])
        else:
            split = lo + (hi - lo) * n_left / n
        if not lo < split < hi:  # degenerate sample (all identical): fall back to midpoint
            split = (lo + hi) / 2
        left = [p for p in pts if key(p) < split]
        right = [p for p in pts if key(p) >= split]
        if split_lat:
            lbox, rbox = (box[0], split, box[2], box[3]), (split, box[1], box[2], box[3])
        else:
            lbox, rbox = (box[0], box[1], box[2], split), (box[0], box[1], split, box[3])
        return (split_lat, split,
                self._build(left, lbox, n_left, depth + 1),
                self._build(right, rbox, n - n_left, depth + 1))

    def partition_of(self, p: Point) -> int:
        node = self._tree
        while not isinstance(node, _Leaf):
            split_lat, split, left, right = node
            v = p.lat if split_lat else p.lon
            node = left if v < split else right
        return node.pid

    def partitions_for_bbox(self, box: BBox) -> set[int]:
        out: set[int] = set()
        stack = [self._tree]
        while stack:
            node = stack.pop()
            if isinstance(node, _Leaf):
                out.add(node.pid)
                continue
            split_lat, split, left, right = node
            lo, hi = (box[0], box[1]) if split_lat else (box[2], box[3])
            if lo < split:
                stack.append(left)
            if hi >= split:
                stack.append(right)
        return out

    def leaf_boxes(self) -> list[BBox]:
        return [leaf.box for leaf in self._leaves]
