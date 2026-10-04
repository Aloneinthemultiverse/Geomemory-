"""PostGIS backend must answer exactly like the in-memory engine.
Skipped when no PostgreSQL/PostGIS is reachable at GEOMEMORY_PG_DSN."""
import random
import unittest
from datetime import timedelta

from geomemory import GeoMemory, Point, haversine_m
from geomemory.agent import AgentInterface
from geomemory.backends import DEFAULT_DSN

from helpers import T0, brute_between, brute_radius, hotspot_world, ids, obs, uniform_world


def _pg_available():
    try:
        import psycopg
        with psycopg.connect(DEFAULT_DSN, connect_timeout=3) as c:
            c.execute("SELECT postgis_version()")
        return True
    except Exception:
        return False


HAVE_PG = _pg_available()


@unittest.skipUnless(HAVE_PG, "PostGIS not reachable (set GEOMEMORY_PG_DSN)")
class PostGISTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from geomemory.backends.postgis import PostGISStore
        rng = random.Random(8)
        cls.pts = uniform_world(rng, 1500) + hotspot_world(rng, 1500)
        cls.pts += [obs("a", "x", 0, 180), obs("b", "x", 0, -180), obs("n", "x", 90, 0),
                    obs("s", "x", -90, 10)]
        cls.pg = PostGISStore(table="test_observations", reset=True)
        cls.inserted = cls.pg.ingest_many(cls.pts, batch=700)
        cls.mem = GeoMemory()
        cls.mem.ingest_many(cls.pts)

    @classmethod
    def tearDownClass(cls):
        cls.pg.close()

    def test_ingest_idempotent(self):
        self.assertEqual(self.inserted, len(self.pts))
        self.assertEqual(self.pg.ingest_many(self.pts[:500]), 0)
        self.assertFalse(self.pg.ingest(self.pts[0]))
        self.assertEqual(len(self.pg), len(self.pts))

    def test_roundtrip_fidelity(self):
        for o in random.Random(1).sample(self.pts, 50):
            self.assertEqual(self.pg.get(o.observation_id), o)
        with self.assertRaises(KeyError):
            self.pg.get("missing")

    def test_queries_match_brute_force(self):
        rng = random.Random(2)
        qs = [(Point(rng.uniform(-90, 90), rng.uniform(-180, 180)), 10 ** rng.uniform(3, 7.3))
              for _ in range(25)]
        qs += [(Point(11 + rng.gauss(0, .05), 76 + rng.gauss(0, .05)), 10 ** rng.uniform(1, 4.5))
               for _ in range(25)]
        qs += [(Point(0, 179.999), 5000), (Point(89.99, 0), 5000), (Point(-90, 0), 1)]
        for c, r in qs:
            self.assertEqual(ids(self.pg.radius(c, r)), brute_radius(self.pts, c, r), (c, r))
            a = T0 + timedelta(minutes=rng.uniform(0, 8000))
            b = a + timedelta(minutes=rng.uniform(0, 3000))
            got = self.pg.radius_between(c, r, a, b)
            self.assertEqual(ids(got), brute_radius(self.pts, c, r) & brute_between(self.pts, a, b))
            # Same k distances to within 1 mm (near-ties may order differently
            # because PostGIS and Python round the sphere distance differently).
            dp = [haversine_m(c, o.location) for o in self.pg.nearest(c, 7)]
            dm = [haversine_m(c, o.location) for o in self.mem.nearest(c, 7)]
            self.assertEqual(len(dp), 7)
            for x, y in zip(dp, dm):
                self.assertAlmostEqual(x, y, delta=1e-3)

    def test_time_entity_polygon_lineage(self):
        a, b = T0 + timedelta(minutes=100), T0 + timedelta(minutes=900)
        self.assertEqual(ids(self.pg.between(a, b)), brute_between(self.pts, a, b))
        self.assertEqual([o.observation_id for o in self.pg.entity_history("e7")],
                         [o.observation_id for o in sorted(self.mem.entity_history("e7"),
                                                           key=lambda o: (o.timestamp, o.observation_id))])
        ring = [Point(0, 0), Point(0, 3), Point(3, 3), Point(3, 2), Point(1, 2),
                Point(1, 1), Point(3, 1), Point(3, 0)]
        ring2 = [Point(10.9, 75.9), Point(10.9, 76.1), Point(11.1, 76.1), Point(11.1, 75.9)]
        for rg in (ring, ring2):
            self.assertEqual(ids(self.pg.within_polygon(rg)), ids(self.mem.within_polygon(rg)))
        self.assertEqual(self.pg.entity_ids(), self.mem.entity_ids())

    def test_lineage_and_agent_tools(self):
        from geomemory.backends.postgis import PostGISStore
        with PostGISStore(table="test_lineage", reset=True) as pg:
            a = obs("M47", "temperature_anomaly", 11, 76, 0, "S_A", 0.91, observation_id="la")
            b = obs("M47", "vibration_anomaly", 11, 76, 5, "S_B", 0.88, observation_id="lb",
                    parent_observation="la")
            f = obs("M47", "failure", 11, 76, 10, "S_B", observation_id="lf",
                    parent_observation="lb")
            loop = obs("X", "x", 0, 0, observation_id="l1", parent_observation="l2")
            loop2 = obs("X", "x", 0, 0, observation_id="l2", parent_observation="l1")
            pg.ingest_many([f, a, b, loop, loop2])
            self.assertEqual([o.observation_id for o in pg.lineage("lf")], ["lf", "lb", "la"])
            self.assertEqual([o.observation_id for o in pg.lineage("l1")], ["l1", "l2"])
            ag = AgentInterface(pg)
            r = ag.dispatch("history_before", {"entity_id": "M47", "event_type": "failure",
                                               "lookback_minutes": 30})
            self.assertEqual([t["event_type"] for t in r["timeline"]],
                             ["temperature_anomaly", "vibration_anomaly"])
            r = ag.dispatch("evidence", {"observation_id": "lf"})
            self.assertEqual(len(r["lineage"]), 3)
            self.assertFalse(ag.dispatch("evidence", {"observation_id": "nope"})["ok"])

    def test_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            self.pg.radius(Point(0, 0), -1)
        with self.assertRaises(ValueError):
            self.pg.within_polygon([Point(0, 0), Point(1, 1)])
        self.assertEqual(self.pg.between(T0 + timedelta(1), T0), [])
        self.assertEqual(self.pg.nearest(Point(0, 0), 0), [])


if __name__ == "__main__":
    unittest.main()
