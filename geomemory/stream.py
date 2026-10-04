"""Streaming ingestion (spec §14, §18.6, §18.11).

`Broker` is an in-process stand-in for Kafka: partitioned append-only logs
with consumer-committed offsets and a bounded backlog. `StreamProcessor`
validates and normalizes raw records, dead-letters bad ones, and writes to a
sink with at-least-once delivery; the sink's idempotent ingest turns that
into effectively-once storage.
"""
from __future__ import annotations

import hashlib
import json
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Protocol

from .model import Observation, Point, Provenance


class Sink(Protocol):
    def ingest(self, obs: Observation) -> bool: ...


class BacklogFull(RuntimeError):
    pass


class Broker:
    def __init__(self, n_partitions: int = 4, max_backlog: int | None = None) -> None:
        self.logs: list[list[tuple[float, dict]]] = [[] for _ in range(n_partitions)]
        self.committed: dict[tuple[str, int], int] = {}
        self.max_backlog = max_backlog
        self.produced = 0
        self.dropped = 0

    def backlog(self, group: str) -> int:
        return sum(len(log) - self.committed.get((group, i), 0)
                   for i, log in enumerate(self.logs))

    def produce(self, record: dict, key: str | None = None, group: str = "geomemory") -> None:
        if self.max_backlog is not None and self.backlog(group) >= self.max_backlog:
            self.dropped += 1
            raise BacklogFull("backlog full")
        k = key if key is not None else str(record.get("entity_id", ""))
        self.logs[zlib.crc32(k.encode()) % len(self.logs)].append((time.monotonic(), record))
        self.produced += 1

    def fetch(self, group: str, partition: int, max_records: int) -> list[tuple[int, float, dict]]:
        start = self.committed.get((group, partition), 0)
        log = self.logs[partition]
        return [(i, *log[i]) for i in range(start, min(len(log), start + max_records))]

    def commit(self, group: str, partition: int, offset: int) -> None:
        self.committed[(group, partition)] = max(self.committed.get((group, partition), 0), offset)


class InvalidRecord(ValueError):
    pass


def _parse_time(v: Any) -> datetime:
    try:
        if isinstance(v, datetime):
            t = v
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            t = datetime.fromtimestamp(float(v), tz=timezone.utc)
        elif isinstance(v, str):
            t = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            raise InvalidRecord("missing or unparseable timestamp")
    except (ValueError, OverflowError, OSError) as e:
        if isinstance(e, InvalidRecord):
            raise
        raise InvalidRecord(f"unparseable timestamp: {e}") from None
    if t.tzinfo is None:
        raise InvalidRecord("timestamp must include a timezone")
    return t.astimezone(timezone.utc)


def _require(rec: Mapping, key: str) -> Any:
    v = rec.get(key)
    if v is None or v == "":
        raise InvalidRecord(f"missing {key}")
    return v


def normalize(rec: Mapping[str, Any]) -> Observation:
    """Validate a raw record and turn it into a canonical Observation."""
    if not isinstance(rec, Mapping):
        raise InvalidRecord("record is not an object")
    try:
        lat, lon = float(_require(rec, "lat")), float(_require(rec, "lon"))
        loc = Point(lat, lon)
    except (TypeError, ValueError) as e:
        raise InvalidRecord(f"bad coordinates: {e}") from None
    ts = _parse_time(rec.get("timestamp"))
    entity, event, source = (str(_require(rec, k)) for k in ("entity_id", "event_type", "source_id"))
    oid = rec.get("observation_id")
    if not oid:
        # Deterministic ID so a re-sent record without an ID is still deduplicated.
        basis = json.dumps([entity, event, lat, lon, ts.isoformat(), source], sort_keys=True)
        oid = hashlib.sha1(basis.encode()).hexdigest()
    try:
        return Observation(
            entity_id=entity, event_type=event, location=loc, timestamp=ts,
            provenance=Provenance(source, rec.get("processing_model"),
                                  tuple(rec.get("inputs", ()))),
            confidence=float(rec.get("confidence", 1.0)),
            spatial_uncertainty_m=float(rec.get("uncertainty_m", 0.0)),
            duration=timedelta(seconds=float(rec.get("duration_s", 0.0))),
            attributes=dict(rec.get("attributes", {})),
            parent_observation=rec.get("parent_observation"),
            observation_id=str(oid),
        )
    except (TypeError, ValueError) as e:
        raise InvalidRecord(str(e)) from None


@dataclass
class StreamMetrics:
    processed: int = 0
    stored: int = 0
    duplicates: int = 0
    dead_lettered: int = 0
    out_of_order: int = 0
    latencies_s: list[float] = field(default_factory=list)

    def p(self, q: float) -> float:
        if not self.latencies_s:
            return 0.0
        xs = sorted(self.latencies_s)
        return xs[min(len(xs) - 1, int(q * len(xs)))]


class StreamProcessor:
    def __init__(self, broker: Broker, sink: Sink, group: str = "geomemory",
                 batch_size: int = 100,
                 crash_hook: Callable[[int], bool] | None = None) -> None:
        self.broker, self.sink, self.group = broker, sink, group
        self.batch_size = batch_size
        self.metrics = StreamMetrics()
        self.dead_letters: list[tuple[dict, str]] = []
        self._crash_hook = crash_hook  # test hook: return True to crash mid-batch
        self._max_seen: dict[str, datetime] = {}

    def poll_once(self) -> int:
        """Process one batch from every partition. Returns records handled."""
        handled = 0
        for pid in range(len(self.broker.logs)):
            batch = self.broker.fetch(self.group, pid, self.batch_size)
            for offset, enq, rec in batch:
                if self._crash_hook and self._crash_hook(self.metrics.processed):
                    raise RuntimeError("injected crash before commit")
                self._handle(rec, enq)
                handled += 1
            if batch:
                self.broker.commit(self.group, pid, batch[-1][0] + 1)
        return handled

    def run_until_idle(self) -> None:
        while self.poll_once():
            pass

    def _handle(self, rec: dict, enqueued: float) -> None:
        self.metrics.processed += 1
        try:
            obs = normalize(rec)
        except InvalidRecord as e:
            self.metrics.dead_lettered += 1
            self.dead_letters.append((rec, str(e)))
            return
        prev = self._max_seen.get(obs.entity_id)
        if prev is not None and obs.timestamp < prev:
            self.metrics.out_of_order += 1
        else:
            self._max_seen[obs.entity_id] = obs.timestamp
        if self.sink.ingest(obs):
            self.metrics.stored += 1
        else:
            self.metrics.duplicates += 1
        self.metrics.latencies_s.append(time.monotonic() - enqueued)
