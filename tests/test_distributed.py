"""Phase 2: partitioning, cluster scatter-gather, failures, streaming."""
import random
import unittest
from datetime import timedelta

from geomemory import GeoMemory, GridIndex, Point, haversine_m
from geomemory.cluster import Cluster, PartitionUnavailable
from geomemory.partition import GridPartitioner, KDPartitioner
from geomemory.stream import BacklogFull, Broker, StreamProcessor, normalize, InvalidRecord

from helpers import T0, brute_between, brute_radius, hotspot_world, ids, obs, uniform_world


def make_cluster(pts, kind, n_parts, n_nodes, rf=1, workers=None):
    if kind == "grid":
        part = GridPartitioner(n_parts, cell_deg=15)
    else:
        rng = random.Random(0)
        part = KDPartitioner(n_parts, [o.location for o in rng.sample(pts, min(500, len(pts)))])
    c = Cluster(part, n_nodes, rf, workers=workers, index_factory=lambda: GridIndex(1.0))
    c.ingest_many(pts)
    return c


class PartitionerTests(unittest.TestCase):
    def test_bbox_routing_is_complete(self):
        """Every point inside a box must live in a partition the box routes to."""
        rng = random.Random(11)
        pts = [Point(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(3000)]
        parts = [GridPartitioner(7, 13), GridPartitioner(1),
                 KDPartitioner(8, pts[:300]), KDPartitioner(13, pts[:50]), KDPartitioner(5, [])]
        for part in parts:
            for _ in range(200):
                a, b = sorted(rng.uniform(-90, 90) for _ in range(2))
                c, d = sorted(rng.uniform(-180, 180) for _ in range(2))
                routed = part.partitions_for_bbox((a, b, c, d))
                for p in pts:
                    if a <= p.lat <= b and c <= p.lon <= d:
                        self.assertIn(part.partition_of(p), routed)
                self.assertTrue(all(0 <= x < part.n_partitions for x in routed))

    def test_kd_degenerate_sample(self):
        same = [Point(5, 5)] * 100
        part = KDPartitioner(8, same)
        self.assertEqual(len(part.leaf_boxes()), 8)
        self.assertTrue(0 <= part.partition_of(Point(5, 5)) < 8)
        self.assertEqual(part.partition_of(Point(90, 180)), part.partition_of(Point(90, 180)))

    def test_kd_handles_skew_better_than_grid(self):  # E5
        rng = random.Random(12)
        pts = hotspot_world(rng, 6000, spread=0.5)
        grid = make_cluster(pts, "grid", 8, 8)
        kd = make_cluster(pts, "kd", 8, 8)
        self.assertLess(kd.imbalance(), 1.3)
        self.assertGreater(grid.imbalance(), 3 * kd.imbalance())


class ClusterEquivalence(unittest.TestCase):
    """A cluster of any shape must answer exactly like one node."""

    def test_matches_single_node(self):
        for seed in range(4):
            rng = random.Random(seed)
            pts = uniform_world(rng, 1200) + hotspot_world(rng, 1200)
            single = GeoMemory()
            single.ingest_many(pts)
            for kind in ("grid", "kd"):
                for n_parts, n_nodes, rf in ((1, 1, 1), (4, 2, 2), (8, 4, 3), (16, 8, 2)):
                    c = make_cluster(pts, kind, n_parts, n_nodes, rf, workers=4)
                    self.assertEqual(sum(c.partition_sizes().values()), len(pts))
                    for _ in range(5):
                        ctr = Point(rng.uniform(-90, 90), rng.uniform(-180, 180))
                        r = 10 ** rng.uniform(3, 7)
                        self.assertEqual(ids(c.radius(ctr, r)), brute_radius(pts, ctr, r))
                        self.assertEqual([o.observation_id for o in c.nearest(ctr, 9)],
                                         [o.observation_id for o in single.nearest(ctr, 9)])
                        a = T0 + timedelta(minutes=rng.uniform(0, 5000))
                        b = a + timedelta(minutes=rng.uniform(0, 2000))
                        self.assertEqual(ids(c.between(a, b)), brute_between(pts, a, b))
                        self.assertEqual(ids(c.radius_between(ctr, r, a, b)),
                                         brute_radius(pts, ctr, r) & brute_between(pts, a, b))
                    ring = [Point(10, 75), Point(10, 77), Point(12, 77), Point(12, 75)]
                    self.assertEqual(ids(c.within_polygon(ring)), ids(single.within_polygon(ring)))
                    self.assertEqual([o.observation_id for o in c.entity_history("e3")],
                                     [o.observation_id for o in
                                      sorted(single.entity_history("e3"),
                                             key=lambda o: (o.timestamp, o.observation_id))])
                    c.close()


class FailureTests(unittest.TestCase):  # spec §18.11
    def setUp(self):
        rng = random.Random(21)
        self.pts = uniform_world(rng, 2000)
        self.c = make_cluster(self.pts, "kd", 8, 4, rf=2)
        self.q = (Point(0, 0), 8_000_000)

    def test_any_single_failure_is_invisible(self):
        want = brute_radius(self.pts, *self.q)
        for nid in range(4):
            c = make_cluster(self.pts, "kd", 8, 4, rf=2)
            c.fail(nid)
            self.assertEqual(ids(c.radius(*self.q)), want)
            self.assertEqual(len(c.between(T0, T0 + timedelta(days=30))), len(self.pts))

    def test_losing_all_replicas_is_loud(self):
        self.c.fail(0)
        self.c.fail(1)  # partitions on {0,1} have no replica left
        with self.assertRaises(PartitionUnavailable):
            self.c.radius(Point(0, 0), 2e7)
        with self.assertRaises(PartitionUnavailable):
            self.c.ingest_many(uniform_world(random.Random(1), 200))

    def test_writes_during_failure_survive_recovery(self):
        self.c.fail(2)
        extra = uniform_world(random.Random(99), 500)
        self.c.ingest_many(extra)
        copied = self.c.recover(2)
        self.assertGreater(copied, 0)
        # Now kill the *other* replica of each partition node 2 holds.
        self.c.fail(1)
        self.c.fail(3)
        everything = self.pts + extra
        for pid in range(8):
            if self.c.nodes[2] in self.c.replicas(pid):
                node2 = self.c.nodes[2].partitions.get(pid)
                want = {o.observation_id for o in everything
                        if self.c.partitioner.partition_of(o.location) == pid}
                self.assertEqual(set(node2._obs) if node2 else set(), want)

    def test_replicas_identical_and_no_duplication(self):
        self.c.ingest_many(self.pts)  # replay everything
        for pid in range(8):
            stores = [n.partitions.get(pid) for n in self.c.replicas(pid)]
            keys = [set(s._obs) if s else set() for s in stores]
            self.assertTrue(all(k == keys[0] for k in keys))
        self.assertEqual(sum(self.c.partition_sizes().values()), len(self.pts))


def raw(o, **over):
    d = {"observation_id": o.observation_id, "entity_id": o.entity_id,
         "event_type": o.event_type, "lat": o.location.lat, "lon": o.location.lon,
         "timestamp": o.timestamp.isoformat(), "source_id": o.source_id,
         "confidence": o.confidence}
    d.update(over)
    return d


class StreamTests(unittest.TestCase):
    def test_normalize_rejects_garbage(self):
        base = raw(obs("e", "x", 1, 2))
        bad = [None, [], {}, {**base, "lat": "abc"}, {**base, "lat": 91}, {**base, "lon": None},
               {**base, "timestamp": None}, {**base, "timestamp": "2026-10-04T09:00:00"},
               {**base, "timestamp": "yesterday"}, {**base, "entity_id": ""},
               {**base, "source_id": None}, {**base, "confidence": 2},
               {**base, "lat": float("nan")}, {**base, "timestamp": True},
               {**base, "duration_s": -5}, {**base, "uncertainty_m": -1},
               {**base, "timestamp": 1e20}, {**base, "timestamp": float("nan")},
               {**base, "confidence": "high"}, {**base, "attributes": 5}]
        for rec in bad:
            with self.assertRaises(InvalidRecord, msg=repr(rec)):
                normalize(rec)
        o = normalize({**base, "timestamp": "2026-10-04T14:30:00+05:30"})
        self.assertEqual(o.timestamp, T0)
        self.assertEqual(normalize({**base, "timestamp": T0.timestamp()}).timestamp, T0)

    def test_id_less_records_dedup_deterministically(self):
        r = raw(obs("e", "x", 1, 2))
        del r["observation_id"]
        self.assertEqual(normalize(r).observation_id, normalize(dict(r)).observation_id)

    def test_chaos_stream_is_exactly_once(self):
        """Duplicates, shuffled order, invalid records and random crashes."""
        for seed in range(6):
            rng = random.Random(seed)
            good = uniform_world(rng, 1500)
            records = [raw(o) for o in good]
            records += [raw(rng.choice(good)) for _ in range(400)]  # duplicates
            invalid = [raw(rng.choice(good), lat=999), raw(rng.choice(good), timestamp=None),
                       {"junk": True}] * 30
            records += invalid
            rng.shuffle(records)  # out-of-order arrival
            broker = Broker(n_partitions=5)
            for rec in records:
                broker.produce(rec, key=str(rng.random()))
            sink = make_cluster([], "grid", 6, 3, rf=2)
            crashes = 0
            while broker.backlog("geomemory"):
                crash_at = rng.randrange(1, 400)
                proc = StreamProcessor(broker, sink, batch_size=rng.randrange(1, 64),
                                       crash_hook=lambda n, k=crash_at: n == k)
                try:
                    proc.run_until_idle()
                except RuntimeError:
                    crashes += 1
            self.assertGreater(crashes, 0)
            self.assertEqual(sum(sink.partition_sizes().values()), len(good))
            self.assertEqual(ids(sink.between(T0 - timedelta(1), T0 + timedelta(30))), ids(good))

    def test_metrics_and_dead_letters(self):
        rng = random.Random(3)
        good = uniform_world(rng, 300)
        broker = Broker(2)
        for o in good:
            broker.produce(raw(o))
        broker.produce(raw(good[0]))
        broker.produce({**raw(good[1]), "lat": -100})
        proc = StreamProcessor(broker, GeoMemory(), batch_size=17)
        proc.run_until_idle()
        m = proc.metrics
        self.assertEqual((m.processed, m.stored, m.duplicates, m.dead_lettered),
                         (302, 300, 1, 1))
        self.assertIn("coordinates", proc.dead_letters[0][1])
        self.assertGreater(m.out_of_order, 0)  # uniform_world times are random
        self.assertEqual(broker.backlog("geomemory"), 0)
        self.assertGreaterEqual(m.p(0.99), m.p(0.5))

    def test_backpressure_accounting(self):
        broker = Broker(3, max_backlog=100)
        accepted = 0
        for o in uniform_world(random.Random(4), 250):
            try:
                broker.produce(raw(o))
                accepted += 1
            except BacklogFull:
                pass
        self.assertEqual((accepted, broker.dropped), (100, 150))
        proc = StreamProcessor(broker, GeoMemory())
        proc.run_until_idle()
        self.assertEqual(proc.metrics.stored, 100)
        broker.produce(raw(obs("late", "x", 0, 0)))  # room again after draining
        self.assertEqual(broker.backlog("geomemory"), 1)

    def test_independent_consumer_groups(self):
        broker = Broker(2)
        for o in uniform_world(random.Random(5), 100):
            broker.produce(raw(o))
        a, b = GeoMemory(), GeoMemory()
        StreamProcessor(broker, a, group="a").run_until_idle()
        self.assertEqual(broker.backlog("b"), 100)
        StreamProcessor(broker, b, group="b").run_until_idle()
        self.assertEqual(len(a), len(b))
        self.assertEqual(len(a), 100)


if __name__ == "__main__":
    unittest.main()
