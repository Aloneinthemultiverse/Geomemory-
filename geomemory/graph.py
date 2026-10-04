"""Relationship layer (spec §8, §9, §12.9, Phase 3).

Nodes are entity ids, source ids, region names, or observations ("obs:<id>").
Each typed edge keeps when it held (first/last seen) and the observation ids
that support it, so every relationship is traceable to evidence.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Mapping

from .model import Observation, Point
from .store import GeoMemory

SYMMETRIC = {"NEAR", "CONNECTED_TO", "ADJACENT_TO", "INTERSECTS", "OVERLAPS"}


def obs_node(o: Observation | str) -> str:
    return f"obs:{o if isinstance(o, str) else o.observation_id}"


@dataclass
class Edge:
    src: str
    rel: str
    dst: str
    first_seen: datetime
    last_seen: datetime
    evidence: set[str] = field(default_factory=set)

    def active(self, t: datetime, slack: timedelta = timedelta(0)) -> bool:
        return self.first_seen - slack <= t <= self.last_seen + slack


class RelationshipGraph:
    def __init__(self) -> None:
        self._edges: dict[tuple[str, str, str], Edge] = {}
        self._out: dict[str, set[tuple[str, str, str]]] = defaultdict(set)

    def __len__(self) -> int:
        return len(self._edges)

    def add_edge(self, src: str, rel: str, dst: str, t: datetime,
                 evidence: Iterable[str] = ()) -> Edge:
        if src == dst:
            raise ValueError("self-loops are not allowed")
        if rel in SYMMETRIC and dst < src:
            src, dst = dst, src  # store symmetric edges once, canonically
        key = (src, rel, dst)
        e = self._edges.get(key)
        if e is None:
            e = self._edges[key] = Edge(src, rel, dst, t, t)
            self._out[src].add(key)
            self._out[dst].add(key)
        else:
            e.first_seen, e.last_seen = min(e.first_seen, t), max(e.last_seen, t)
        e.evidence.update(evidence)
        return e

    def edge(self, src: str, rel: str, dst: str) -> Edge | None:
        if rel in SYMMETRIC and dst < src:
            src, dst = dst, src
        return self._edges.get((src, rel, dst))

    def edges_of(self, node: str, rel: str | None = None) -> list[Edge]:
        return [self._edges[k] for k in self._out.get(node, ())
                if rel is None or k[1] == rel]

    def neighbors(self, node: str, rel: str | None = None, at: datetime | None = None,
                  slack: timedelta = timedelta(0), direction: str = "out") -> set[str]:
        """Adjacent nodes. Symmetric relations ignore direction."""
        out = set()
        for e in self.edges_of(node, rel):
            if at is not None and not e.active(at, slack):
                continue
            if e.rel in SYMMETRIC:
                out.add(e.dst if e.src == node else e.src)
            elif direction == "out" and e.src == node:
                out.add(e.dst)
            elif direction == "in" and e.dst == node:
                out.add(e.src)
        return out

    def path(self, src: str, dst: str, max_depth: int = 6) -> list[str] | None:
        """Shortest undirected path between two nodes (BFS)."""
        if src == dst:
            return [src]
        prev = {src: None}
        q = deque([(src, 0)])
        while q:
            node, d = q.popleft()
            if d == max_depth:
                continue
            for k in self._out.get(node, ()):
                nxt = k[2] if k[0] == node else k[0]
                if nxt in prev:
                    continue
                prev[nxt] = node
                if nxt == dst:
                    out = [dst]
                    while prev[out[-1]] is not None:
                        out.append(prev[out[-1]])
                    return out[::-1]
                q.append((nxt, d + 1))
        return None


def derive_relationships(
    observations: Iterable[Observation],
    near_m: float = 50.0,
    near_window: timedelta = timedelta(minutes=5),
    regions: Mapping[str, list[Point]] | None = None,
    graph: RelationshipGraph | None = None,
) -> RelationshipGraph:
    """Build spatial, temporal and provenance edges from observations.

    - OBSERVED_BY: entity -> source
    - NEAR: entities observed within `near_m` metres and `near_window` of each other
    - INSIDE: entity -> region polygon containing one of its observations
    - BEFORE / CHANGED_FROM: consecutive observations of the same entity
    """
    obs = list(observations)
    g = graph or RelationshipGraph()
    mem = GeoMemory()
    mem.ingest_many(obs)

    for o in obs:
        g.add_edge(o.entity_id, "OBSERVED_BY", o.source_id, o.timestamp, [o.observation_id])

    for o in obs:
        for other in mem.radius_between(o.location, near_m, o.timestamp - near_window,
                                        o.timestamp + near_window):
            if other.entity_id != o.entity_id and o.observation_id < other.observation_id:
                g.add_edge(o.entity_id, "NEAR", other.entity_id,
                           max(o.timestamp, other.timestamp),
                           [o.observation_id, other.observation_id])
                g.add_edge(o.entity_id, "NEAR", other.entity_id,
                           min(o.timestamp, other.timestamp))

    for name, ring in (regions or {}).items():
        for o in mem.within_polygon(ring):
            g.add_edge(o.entity_id, "INSIDE", name, o.timestamp, [o.observation_id])

    by_entity = defaultdict(list)
    for o in obs:
        by_entity[o.entity_id].append(o)
    for hist in by_entity.values():
        hist.sort(key=lambda o: (o.timestamp, o.observation_id))
        for a, b in zip(hist, hist[1:]):
            g.add_edge(obs_node(a), "BEFORE", obs_node(b), b.timestamp,
                       [a.observation_id, b.observation_id])
            if a.event_type != b.event_type:
                g.add_edge(obs_node(b), "CHANGED_FROM", obs_node(a), b.timestamp,
                           [a.observation_id, b.observation_id])
    return g


def connected_when(graph: RelationshipGraph, entity_id: str, event: Observation,
                   rel: str = "NEAR", slack: timedelta = timedelta(0)) -> set[str]:
    """Spec §12.9: entities related to `entity_id` when `event` occurred."""
    return graph.neighbors(entity_id, rel, at=event.timestamp, slack=slack)
