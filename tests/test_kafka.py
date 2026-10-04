"""Real Kafka: chaos stream (duplicates, garbage, shuffling, crashes) must
store every valid record exactly once, in memory and in PostGIS."""
import random
import unittest
import uuid

from geomemory import GeoMemory
from geomemory.backends import DEFAULT_KAFKA

from helpers import T0, ids, uniform_world
from test_distributed import raw
from test_postgis import HAVE_PG


def _kafka_available():
    try:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": DEFAULT_KAFKA, "socket.timeout.ms": 3000}) \
            .list_topics(timeout=5)
        return True
    except Exception:
        return False


HAVE_KAFKA = _kafka_available()


def chaos_records(seed, n=1500):
    rng = random.Random(seed)
    good = uniform_world(rng, n)
    recs = [raw(o) for o in good] + [raw(rng.choice(good)) for _ in range(300)]
    recs += [raw(rng.choice(good), lat=999), raw(rng.choice(good), timestamp="soon")] * 20
    recs += [b"{not json", b"\xff\xfe", b"[]"] * 5
    rng.shuffle(recs)
    return good, recs


@unittest.skipUnless(HAVE_KAFKA, "Kafka not reachable (scripts/kafka-dev.sh)")
class KafkaTests(unittest.TestCase):
    def _run_with_crashes(self, sink, topic, seed):
        from geomemory.backends.kafka import KafkaStreamProcessor
        rng = random.Random(seed)
        group = f"g-{uuid.uuid4().hex[:8]}"
        crashes, stored, dlq = 0, 0, 0
        while True:
            crash_at = rng.randrange(50, 900)
            p = KafkaStreamProcessor(sink, topic, group, batch_size=rng.randrange(20, 300),
                                     crash_hook=lambda n, k=crash_at: n == k)
            try:
                m = p.run(idle_timeout_s=4)
                stored += m.stored
                dlq += m.dead_lettered
                p.close()
                return crashes, stored, dlq
            except RuntimeError:
                crashes += 1
                stored += p.metrics.stored
                p.close()

    def test_exactly_once_in_memory(self):
        from geomemory.backends.kafka import ensure_topic, produce
        topic = f"obs-{uuid.uuid4().hex[:8]}"
        ensure_topic(topic, partitions=4)
        good, recs = chaos_records(1)
        self.assertEqual(produce(recs, topic), len(recs))
        sink = GeoMemory()
        crashes, stored, dlq = self._run_with_crashes(sink, topic, 1)
        self.assertGreater(crashes, 0)
        self.assertEqual(len(sink), len(good))
        self.assertEqual(stored, len(good))
        self.assertEqual(set(sink._obs), ids(good))

    def test_dead_letter_topic(self):
        from confluent_kafka import Consumer
        from geomemory.backends.kafka import KafkaStreamProcessor, ensure_topic, produce
        topic = f"obs-{uuid.uuid4().hex[:8]}"
        ensure_topic(topic, partitions=2)
        good = uniform_world(random.Random(2), 50)
        produce([raw(o) for o in good] + [b"garbage", raw(good[0], lat=-200)], topic)
        p = KafkaStreamProcessor(GeoMemory(), topic, f"g-{uuid.uuid4().hex[:8]}")
        m = p.run(idle_timeout_s=3)
        p.close()
        self.assertEqual((m.stored, m.dead_lettered), (50, 2))
        c = Consumer({"bootstrap.servers": DEFAULT_KAFKA, "group.id": uuid.uuid4().hex,
                      "auto.offset.reset": "earliest"})
        c.subscribe([f"{topic}.dlq"])
        got = []
        for _ in range(40):
            got += [x for x in c.consume(10, timeout=0.5) if x.error() is None]
            if len(got) >= 2:
                break
        c.close()
        reasons = sorted(dict(x.headers())["reason"].decode() for x in got)
        self.assertEqual(len(reasons), 2)
        self.assertTrue(any("coordinates" in r for r in reasons))
        self.assertTrue(any("not JSON" in r for r in reasons))

    @unittest.skipUnless(HAVE_PG, "PostGIS not reachable")
    def test_kafka_to_postgis_end_to_end(self):
        from geomemory.backends.kafka import ensure_topic, produce
        from geomemory.backends.postgis import PostGISStore
        topic = f"obs-{uuid.uuid4().hex[:8]}"
        ensure_topic(topic, partitions=4)
        good, recs = chaos_records(3, n=800)
        produce(recs, topic)
        with PostGISStore(table="test_kafka_sink", reset=True) as pg:
            crashes, stored, _ = self._run_with_crashes(pg, topic, 3)
            self.assertGreater(crashes, 0)
            self.assertEqual(len(pg), len(good))
            self.assertEqual(ids(pg.between(T0.replace(year=2000), T0.replace(year=2100))),
                             ids(good))


if __name__ == "__main__":
    unittest.main()
