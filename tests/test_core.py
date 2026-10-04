import random
import unittest
from datetime import datetime, timedelta, timezone

from geomemory import GeoMemory, Observation, Point, Provenance, geohash_encode, haversine_m

T0 = datetime(2026, 10, 4, 9, 0, tzinfo=timezone.utc)


def obs(entity, event, lat, lon, minutes=0, source="sensor_1", conf=1.0, **kw):
    return Observation(entity, event, Point(lat, lon), T0 + timedelta(minutes=minutes),
                       Provenance(source), confidence=conf, **kw)


class ModelTests(unittest.TestCase):
    def test_validation(self):
        with self.assertRaises(ValueError):
            Point(91, 0)
        with self.assertRaises(ValueError):
            obs("e", "x", 0, 0, conf=1.5)
        with self.assertRaises(ValueError):
            Observation("e", "x", Point(0, 0), datetime(2026, 1, 1), Provenance("s"))

    def test_haversine_and_geohash(self):
        self.assertAlmostEqual(haversine_m(Point(0, 0), Point(0, 1)), 111_195, delta=10)
        self.assertEqual(geohash_encode(Point(57.64911, 10.40744), 11), "u4pruydqqvj")


class QueryTests(unittest.TestCase):
    def setUp(self):
        rng = random.Random(42)
        self.points = [obs(f"e{i}", "ping", 11 + rng.uniform(-0.1, 0.1),
                           76 + rng.uniform(-0.1, 0.1), minutes=i) for i in range(2000)]
        self.gm = GeoMemory()
        self.gm.ingest_many(self.points)
        self.c = Point(11.0, 76.0)

    def test_radius_matches_brute_force(self):
        got = {o.observation_id for o in self.gm.radius(self.c, 3000)}
        want = {o.observation_id for o in self.points if haversine_m(self.c, o.location) <= 3000}
        self.assertEqual(got, want)
        self.assertTrue(want)

    def test_nearest(self):
        got = [o.observation_id for o in self.gm.nearest(self.c, 10)]
        want = [o.observation_id for o in
                sorted(self.points, key=lambda o: haversine_m(self.c, o.location))[:10]]
        self.assertEqual(got, want)

    def test_polygon(self):
        ring = [Point(10.95, 75.95), Point(10.95, 76.05), Point(11.05, 76.05), Point(11.05, 75.95)]
        got = {o.observation_id for o in self.gm.within_polygon(ring)}
        want = {o.observation_id for o in self.points
                if 10.95 < o.location.lat < 11.05 and 75.95 < o.location.lon < 76.05}
        self.assertEqual(got, want)

    def test_temporal_and_spatiotemporal(self):
        start, end = T0 + timedelta(minutes=100), T0 + timedelta(minutes=900)
        self.assertEqual(len(self.gm.between(start, end)), 801)
        got = {o.observation_id for o in self.gm.radius_between(self.c, 5000, start, end)}
        want = {o.observation_id for o in self.points
                if start <= o.timestamp <= end and haversine_m(self.c, o.location) <= 5000}
        self.assertEqual(got, want)

    def test_duplicate_ingest_is_idempotent(self):
        self.assertFalse(self.gm.ingest(self.points[0]))
        self.assertEqual(len(self.gm), 2000)


class ProvenanceTests(unittest.TestCase):
    def test_conflicting_observations_coexist(self):  # spec §18.9
        gm = GeoMemory()
        gm.ingest(obs("obj", "detect", 11.0, 76.0, source="sensor_A", conf=0.91))
        gm.ingest(obs("obj", "detect", 11.00027, 76.0, source="satellite_B", conf=0.82))
        gm.ingest(obs("obj", "detect", 11.00045, 76.0, source="drone_C", conf=0.74))
        hist = gm.entity_history("obj")
        self.assertEqual(len(hist), 3)
        ev = gm.evidence(hist)
        self.assertEqual(ev["sources"], ["drone_C", "satellite_B", "sensor_A"])
        self.assertEqual(sorted(o["confidence"] for o in ev["observations"]), [0.74, 0.82, 0.91])

    def test_what_happened_before_failure(self):  # spec §22
        gm = GeoMemory()
        a = obs("Machine_47", "temperature_anomaly", 11, 76, 0, "sensor_A")
        b = obs("Machine_47", "vibration_anomaly", 11, 76, 5, "sensor_B",
                parent_observation=a.observation_id)
        f = obs("Machine_47", "failure", 11, 76, 10, "sensor_B",
                parent_observation=b.observation_id)
        gm.ingest_many([f, a, b])  # out of order on purpose
        prior = gm.before("Machine_47", f.timestamp)
        self.assertEqual([o.event_type for o in prior],
                         ["temperature_anomaly", "vibration_anomaly"])
        self.assertEqual([o.observation_id for o in gm.lineage(f.observation_id)],
                         [f.observation_id, b.observation_id, a.observation_id])


if __name__ == "__main__":
    unittest.main()
