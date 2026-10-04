"""Kafka ingestion (spec §14, §24: Apache Kafka).

Producers publish raw JSON records keyed by entity, so one entity's records
stay ordered within a partition. `KafkaStreamProcessor` consumes with manual
commits: a batch's offsets are committed only after every record in it has
been stored. A crash before the commit replays the batch (at-least-once);
the sink's idempotent ingest makes the stored result exactly-once. Records
that fail validation go to a dead-letter topic with the reason.
"""
from __future__ import annotations

import json
import time
from typing import Callable, Iterable

from ..stream import InvalidRecord, Sink, StreamMetrics, normalize
from . import DEFAULT_KAFKA


def ensure_topic(topic: str, partitions: int = 4, bootstrap: str = DEFAULT_KAFKA) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                        replication_factor=1)])[topic]
    try:
        fut.result(timeout=30)
    except Exception as e:  # created concurrently is fine
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def produce(records: Iterable, topic: str, bootstrap: str = DEFAULT_KAFKA) -> int:
    """Publish raw records (dicts are JSON-encoded; bytes/str sent as-is so
    tests can inject garbage). Returns the number acknowledged by Kafka."""
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                  "linger.ms": 20, "compression.type": "lz4"})
    acked = [0]
    errors: list[str] = []

    def done(err, _msg):
        if err is None:
            acked[0] += 1
        else:
            errors.append(str(err))

    for r in records:
        if isinstance(r, dict):
            key = str(r.get("entity_id", "")).encode()
            value = json.dumps(r).encode()
        else:
            key, value = None, r if isinstance(r, bytes) else str(r).encode()
        while True:
            try:
                p.produce(topic, value=value, key=key, on_delivery=done)
                break
            except BufferError:  # local queue full: let it drain
                p.poll(0.05)
        p.poll(0)
    p.flush(60)
    if errors:
        raise RuntimeError(f"{len(errors)} delivery errors, e.g. {errors[0]}")
    return acked[0]


class KafkaStreamProcessor:
    def __init__(self, sink: Sink, topic: str, group: str = "geomemory",
                 bootstrap: str = DEFAULT_KAFKA, dlq_topic: str | None = None,
                 batch_size: int = 500,
                 crash_hook: Callable[[int], bool] | None = None) -> None:
        from confluent_kafka import Consumer, Producer
        self.sink, self.topic, self.batch_size = sink, topic, batch_size
        self.dlq_topic = dlq_topic or f"{topic}.dlq"
        self.metrics = StreamMetrics()
        self._crash_hook = crash_hook
        self._consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                                   "enable.auto.commit": False,
                                   "auto.offset.reset": "earliest"})
        self._consumer.subscribe([topic])
        self._dlq = Producer({"bootstrap.servers": bootstrap})
        self.dead_lettered: list[tuple[bytes, str]] = []

    def close(self) -> None:
        self._dlq.flush(10)
        self._consumer.close()  # no commit: auto-commit is off

    def run(self, idle_timeout_s: float = 5.0, max_records: int | None = None,
            startup_timeout_s: float = 30.0) -> StreamMetrics:
        """Consume until no message arrives for `idle_timeout_s`. The idle clock
        starts at the first message: joining the group and getting partitions
        assigned can take several seconds on its own."""
        started = time.monotonic()
        idle_since = None
        while True:
            now = time.monotonic()
            if idle_since is None and now - started > startup_timeout_s:
                break
            if idle_since is not None and now - idle_since > idle_timeout_s:
                break
            msgs = self._consumer.consume(num_messages=self.batch_size, timeout=0.5)
            msgs = [m for m in msgs if m.error() is None]
            if not msgs:
                continue
            idle_since = time.monotonic()
            for m in msgs:
                if self._crash_hook and self._crash_hook(self.metrics.processed):
                    raise RuntimeError("injected crash before commit")
                self._handle(m)
            self._dlq.flush(10)
            self._consumer.commit(asynchronous=False)
            if max_records is not None and self.metrics.processed >= max_records:
                break
        return self.metrics

    def _handle(self, m) -> None:
        self.metrics.processed += 1
        raw = m.value()
        try:
            rec = json.loads(raw)
            obs = normalize(rec)
        except (ValueError, UnicodeDecodeError, InvalidRecord) as e:
            reason = str(e) if isinstance(e, InvalidRecord) else f"not JSON: {e}"
            self.metrics.dead_lettered += 1
            self.dead_lettered.append((raw, reason))
            self._dlq.produce(self.dlq_topic, value=raw,
                              headers=[("reason", reason.encode()[:500])])
            return
        if self.sink.ingest(obs):
            self.metrics.stored += 1
        else:
            self.metrics.duplicates += 1
        ts_type, ts_ms = m.timestamp()
        if ts_ms > 0:
            self.metrics.latencies_s.append(max(0.0, time.time() - ts_ms / 1000))
