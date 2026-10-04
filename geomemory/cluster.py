"""Simulated distributed cluster (spec §16, §18.7, §18.11).

Partitions are placed on nodes with a replication factor. Queries are
scatter-gather: routed to the partitions a query can touch, served by any
live replica, and merged. A node can fail and later recover by re-syncing
its partitions from live replicas.
"""
from __future__ import annotations

import heapq
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Iterable

from .index import bbox_for_radius
from .model import Observation, Point, haversine_m
from .partition import Partitioner
from .store import GeoMemory


class PartitionUnavailable(RuntimeError):
    """No live replica holds a partition the query needs."""


class Node:
    def __init__(self, node_id: int, index_factory: Callable[[], object] | None = None) -> None:
        self.node_id = node_id
        self.alive = True
        self._index_factory = index_factory
        self.partitions: dict[int, GeoMemory] = {}

    def store(self, pid: int) -> GeoMemory:
        if pid not in self.partitions:
            idx = self._index_factory() if self._index_factory else None
            self.partitions[pid] = GeoMemory(idx)
        return self.partitions[pid]

    def record_count(self) -> int:
        return sum(len(s) for s in self.partitions.values())


class Cluster:
    def __init__(self, partitioner: Partitioner, n_nodes: int, replication: int = 1,
                 workers: int | None = None, index_factory=None) -> None:
        if not 1 <= replication <= n_nodes:
            raise ValueError("replication must be in [1, n_nodes]")
        self.partitioner = partitioner
        self.replication = replication
        self.nodes = [Node(i, index_factory) for i in range(n_nodes)]
        self._pool = ThreadPoolExecutor(workers) if workers and workers > 1 else None
        self.messages = 0  # network-message counter for experiments

    # --- placement ------------------------------------------------------

    def replicas(self, pid: int) -> list[Node]:
        n = len(self.nodes)
        return [self.nodes[(pid + i) % n] for i in range(self.replication)]

    def _live_replica(self, pid: int) -> Node:
        for node in self.replicas(pid):
            if node.alive:
                return node
        raise PartitionUnavailable(f"partition {pid} has no live replica")

    # --- ingest ---------------------------------------------------------

    def ingest(self, obs: Observation) -> bool:
        """Write to every live replica. Fails if none is live (no silent loss)."""
        pid = self.partitioner.partition_of(obs.location)
        live = [n for n in self.replicas(pid) if n.alive]
        if not live:
            raise PartitionUnavailable(f"partition {pid} has no live replica")
        new = False
        for node in live:
            self.messages += 1
            new = node.store(pid).ingest(obs) or new
        return new

    def ingest_many(self, observations: Iterable[Observation]) -> int:
        return sum(self.ingest(o) for o in observations)

    # --- failure handling ----------------------------------------------

    def fail(self, node_id: int) -> None:
        node = self.nodes[node_id]
        node.alive = False
        node.partitions.clear()  # simulate total loss of local state

    def recover(self, node_id: int) -> int:
        """Bring a node back and re-sync its partitions from live peers."""
        node = self.nodes[node_id]
        node.alive = True
        copied = 0
        for pid in range(self.partitioner.n_partitions):
            if node not in self.replicas(pid):
                continue
            for peer in self.replicas(pid):
                if peer is node or not peer.alive or pid not in peer.partitions:
                    continue
                for o in peer.partitions[pid]._obs.values():
                    copied += node.store(pid).ingest(o)
                    self.messages += 1
        return copied

    # --- scatter / gather ----------------------------------------------

    def _scatter(self, pids: Iterable[int], fn: Callable[[GeoMemory], list]) -> list:
        targets = []
        for pid in sorted(set(pids)):
            node = self._live_replica(pid)
            if pid in node.partitions:
                targets.append(node.partitions[pid])
        self.messages += 2 * len(targets)
        if self._pool:
            parts = list(self._pool.map(fn, targets))
        else:
            parts = [fn(t) for t in targets]
        return [x for part in parts for x in part]

    def _all_pids(self) -> range:
        return range(self.partitioner.n_partitions)

    def radius(self, center: Point, radius_m: float) -> list[Observation]:
        pids = set()
        for box in bbox_for_radius(center, radius_m):
            pids |= self.partitioner.partitions_for_bbox(box)
        return self._scatter(pids, lambda s: s.radius(center, radius_m))

    def within_polygon(self, ring: list[Point]) -> list[Observation]:
        lats, lons = [p.lat for p in ring], [p.lon for p in ring]
        pids = self.partitioner.partitions_for_bbox((min(lats), max(lats), min(lons), max(lons)))
        return self._scatter(pids, lambda s: s.within_polygon(ring))

    def nearest(self, center: Point, k: int = 10) -> list[Observation]:
        hits = self._scatter(self._all_pids(), lambda s: s.nearest(center, k))
        return heapq.nsmallest(
            k, hits, key=lambda o: (haversine_m(center, o.location), o.observation_id))

    def between(self, start: datetime, end: datetime) -> list[Observation]:
        hits = self._scatter(self._all_pids(), lambda s: s.between(start, end))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    def radius_between(self, center, radius_m, start, end) -> list[Observation]:
        pids = set()
        for box in bbox_for_radius(center, radius_m):
            pids |= self.partitioner.partitions_for_bbox(box)
        hits = self._scatter(pids, lambda s: s.radius_between(center, radius_m, start, end))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    def entity_history(self, entity_id: str) -> list[Observation]:
        hits = self._scatter(self._all_pids(), lambda s: s.entity_history(entity_id))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    # --- metrics (E5) ---------------------------------------------------

    def partition_sizes(self) -> Counter:
        sizes: Counter = Counter()
        for pid in self._all_pids():
            node = self._live_replica(pid)
            sizes[pid] = len(node.partitions.get(pid, ()))
        return sizes

    def records_per_node(self) -> list[int]:
        return [n.record_count() for n in self.nodes]

    def imbalance(self) -> float:
        """max/mean records per partition; 1.0 is perfectly balanced."""
        sizes = list(self.partition_sizes().values()) or [0]
        mean = sum(sizes) / len(sizes)
        return max(sizes) / mean if mean else 1.0

    def close(self) -> None:
        if self._pool:
            self._pool.shutdown()
