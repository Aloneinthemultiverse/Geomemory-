"""Real multi-process cluster: equivalence, failover after SIGKILL, errors."""
import random
import unittest
from datetime import timedelta

from geomemory import GeoMemory, Point
from geomemory.cluster import PartitionUnavailable
from geomemory.partition import GridPartitioner, KDPartitioner
from geomemory.procluster import ProcessCluster, WorkerError

from helpers import T0, brute_between, brute_radius, hotspot_world, ids, uniform_world


def kd(pts, n):
    return KDPartitioner(n, [o.location for o in pts[:500]])


class ProcessClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rng = random.Random(5)
        cls.pts = uniform_world(rng, 1500) + hotspot_world(rng, 1500)
        cls.single = GeoMemory()
        cls.single.ingest_many(cls.pts)

    def test_matches_single_node(self):
        rng = random.Random(1)
        for workers, rf in ((1, 1), (3, 2), (4, 1)):
            with ProcessCluster(kd(self.pts, workers * 3), workers, rf) as pc:
                self.assertEqual(pc.ingest_many(self.pts, batch=700), len(self.pts))
                self.assertEqual(pc.ingest_many(self.pts[:100]), 0)  # idempotent
                self.assertEqual(sum(pc.partition_sizes().values()), len(self.pts))
                qs = [(Point(rng.uniform(-90, 90), rng.uniform(-180, 180)),
                       10 ** rng.uniform(3, 7)) for _ in range(8)]
                qs += [(Point(11 + rng.gauss(0, .05), 76 + rng.gauss(0, .05)), 5000)
                       for _ in range(8)]
                for c, r in qs:
                    self.assertEqual(ids(pc.radius(c, r)), brute_radius(self.pts, c, r))
                    self.assertEqual([o.observation_id for o in pc.nearest(c, 7)],
                                     [o.observation_id for o in self.single.nearest(c, 7)])
                    a = T0 + timedelta(minutes=rng.uniform(0, 5000))
                    b = a + timedelta(minutes=1000)
                    self.assertEqual(ids(pc.radius_between(c, r, a, b)),
                                     brute_radius(self.pts, c, r) & brute_between(self.pts, a, b))
                batch = pc.radius_batch(qs)
                self.assertEqual(batch, [brute_radius(self.pts, c, r) for c, r in qs])
                self.assertEqual(ids(pc.between(T0, T0 + timedelta(minutes=777))),
                                 brute_between(self.pts, T0, T0 + timedelta(minutes=777)))
                ring = [Point(10, 75), Point(10, 77), Point(12, 77), Point(12, 75)]
                self.assertEqual(ids(pc.within_polygon(ring)), ids(self.single.within_polygon(ring)))
                self.assertEqual(ids(pc.entity_history("e3")), ids(self.single.entity_history("e3")))

    def test_sigkill_failover(self):
        """Kill worker processes for real: replicas must serve identical answers."""
        q = (Point(0, 0), 2e7)
        want = brute_radius(self.pts, *q)
        for victim in range(4):
            with ProcessCluster(kd(self.pts, 8), 4, replication=2) as pc:
                pc.ingest_many(self.pts)
                pc.kill(victim)
                self.assertEqual(ids(pc.radius(*q)), want)
                self.assertEqual(pc.radius_batch([q, q]), [want, want])
                self.assertFalse(pc.alive(victim))

    def test_kill_detected_mid_query(self):
        """Process dies after routing but before answering: query still completes."""
        with ProcessCluster(kd(self.pts, 8), 4, replication=2) as pc:
            pc.ingest_many(self.pts)
            pc.alive = lambda w: w not in pc._dead  # stale health view: only pipes tell
            pc._procs[2].kill()
            pc._procs[2].join()
            self.assertIn(2, pc._assign(range(8)))  # still routed to the dead worker
            self.assertEqual(ids(pc.radius(Point(0, 0), 2e7)), {o.observation_id for o in self.pts})
            self.assertIn(2, pc._dead)  # detected from the broken pipe

    def test_losing_all_replicas_is_loud(self):
        with ProcessCluster(GridPartitioner(6, 30), 3, replication=2) as pc:
            pc.ingest_many(self.pts)
            pc.kill(0)
            pc.kill(1)
            with self.assertRaises(PartitionUnavailable):
                pc.radius(Point(0, 0), 2e7)

    def test_worker_errors_are_reported_not_hidden(self):
        with ProcessCluster(GridPartitioner(2, 30), 2) as pc:
            pc.ingest_many(self.pts[:50])
            with self.assertRaises(WorkerError):
                pc.within_polygon([Point(0, 0), Point(1, 1), Point(1, 1)][:2] + [Point(0, 0)][:0])
            # The worker keeps serving after an error.
            self.assertEqual(ids(pc.radius(Point(0, 0), 2e7)),
                             {o.observation_id for o in self.pts[:50]})


if __name__ == "__main__":
    unittest.main()
