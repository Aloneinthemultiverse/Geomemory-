"""Adversarial / randomized tests for the Phase 1 core."""
import math
import random
import unittest
from datetime import timedelta

from geomemory import GeoMemory, GridIndex, Point, geohash_encode, haversine_m
from geomemory.store import _point_in_polygon

from helpers import T0, brute_between, brute_radius, hotspot_world, ids, obs, uniform_world


class RandomizedOracle(unittest.TestCase):
    """Every query must equal a brute-force scan, over many seeds and cell sizes."""

    def test_radius_global_many_seeds(self):
        for seed in range(15):
            rng = random.Random(seed)
            pts = uniform_world(rng, 1500)
            for cell in (0.5, 5.0, 45.0):
                gm = GeoMemory(GridIndex(cell))
                gm.ingest_many(pts)
                for _ in range(10):
                    c = Point(rng.uniform(-90, 90), rng.uniform(-180, 180))
                    r = 10 ** rng.uniform(3, 7.3)  # 1 km .. 20,000 km
                    self.assertEqual(ids(gm.radius(c, r)), brute_radius(pts, c, r),
                                     f"seed={seed} cell={cell} c={c} r={r}")

    def test_radius_hotspot(self):
        rng = random.Random(7)
        pts = hotspot_world(rng, 5000)
        gm = GeoMemory(GridIndex(0.001))
        gm.ingest_many(pts)
        for r in (0, 1, 50, 500, 5_000, 50_000, 5_000_000):
            c = Point(11.0, 76.0)
            self.assertEqual(ids(gm.radius(c, r)), brute_radius(pts, c, r), r)

    def test_nearest_many_seeds(self):
        for seed in range(10):
            rng = random.Random(100 + seed)
            pts = uniform_world(rng, 800)
            gm = GeoMemory(GridIndex(2.0))
            gm.ingest_many(pts)
            for k in (1, 7, 50, 800, 5000):
                c = Point(rng.uniform(-90, 90), rng.uniform(-180, 180))
                want = sorted(pts, key=lambda o: (haversine_m(c, o.location), o.observation_id))
                got = gm.nearest(c, k)
                self.assertEqual([o.observation_id for o in got],
                                 [o.observation_id for o in want[:k]])

    def test_spatiotemporal_both_plans(self):
        """Exercise both the spatial-first and temporal-first plans."""
        rng = random.Random(3)
        pts = hotspot_world(rng, 4000)
        gm = GeoMemory()
        gm.ingest_many(pts)
        c = Point(11.0, 76.0)
        for r, span in ((100, 1000), (1e6, 1), (5000, 50), (1e7, 900)):
            a = T0 + timedelta(minutes=rng.uniform(0, 500))
            b = a + timedelta(minutes=span)
            got = gm.radius_between(c, r, a, b)
            self.assertEqual(ids(got), brute_radius(pts, c, r) & brute_between(pts, a, b))
            ts = [o.timestamp for o in got]
            self.assertEqual(ts, sorted(ts))

    def test_planner_switches_without_changing_answers(self):
        """Huge area + tiny window, tiny area + huge window, and in between."""
        rng = random.Random(77)
        pts = hotspot_world(rng, 6000, spread=0.01)
        gm = GeoMemory()
        gm.ingest_many(pts)
        c = Point(11.0, 76.0)
        for r, a_min, span in ((2e7, 500, 0.5), (5, 0, 10_000), (2000, 100, 300), (2e7, 0, 10_000)):
            a, b = T0 + timedelta(minutes=a_min), T0 + timedelta(minutes=a_min + span)
            got = gm.radius_between(c, r, a, b)
            self.assertEqual(ids(got), brute_radius(pts, c, r) & brute_between(pts, a, b), r)
            self.assertEqual([o.timestamp for o in got], sorted(o.timestamp for o in got))
        self.assertEqual(gm.radius_between(c, 100, T0 + timedelta(1), T0), [])
        with self.assertRaises(ValueError):
            gm.radius_between(c, -1, T0, T0)

    def test_between_many_windows(self):
        rng = random.Random(9)
        pts = uniform_world(rng, 3000)
        gm = GeoMemory()
        gm.ingest_many(pts)
        for _ in range(200):
            a = T0 + timedelta(minutes=rng.uniform(-100, 10_100))
            b = a + timedelta(minutes=rng.uniform(0, 3000))
            self.assertEqual(ids(gm.between(a, b)), brute_between(pts, a, b))
        self.assertEqual(gm.between(T0 + timedelta(1), T0), [])


class EdgeCases(unittest.TestCase):
    def test_antimeridian(self):
        pts = [obs("w", "x", 0, 179.999), obs("e", "x", 0, -179.999),
               obs("far", "x", 0, 170), obs("edge", "x", 0, 180), obs("edge2", "x", 0, -180)]
        gm = GeoMemory(GridIndex(0.01))
        gm.ingest_many(pts)
        for c in (Point(0, 180), Point(0, -180), Point(0, 179.9995), Point(0, -179.9995)):
            for r in (1, 500, 50_000, 2_000_000):
                self.assertEqual(ids(gm.radius(c, r)), brute_radius(pts, c, r), (c, r))

    def test_poles(self):
        rng = random.Random(1)
        pts = [obs(f"p{i}", "x", rng.uniform(89, 90), rng.uniform(-180, 180)) for i in range(300)]
        pts += [obs(f"q{i}", "x", rng.uniform(-90, -89), rng.uniform(-180, 180)) for i in range(300)]
        gm = GeoMemory(GridIndex(0.1))
        gm.ingest_many(pts)
        for c in (Point(90, 0), Point(-90, 0), Point(89.5, 123), Point(-89.9, -45)):
            for r in (1000, 50_000, 120_000):
                self.assertEqual(ids(gm.radius(c, r)), brute_radius(pts, c, r), (c, r))

    def test_identical_points_and_timestamps(self):
        pts = [obs(f"e{i}", "x", 1, 1, minutes=5) for i in range(500)]
        gm = GeoMemory()
        gm.ingest_many(pts)
        self.assertEqual(len(gm.radius(Point(1, 1), 0)), 500)
        self.assertEqual(len(gm.between(T0 + timedelta(minutes=5), T0 + timedelta(minutes=5))), 500)
        self.assertEqual(len(gm.nearest(Point(1, 1), 10)), 10)

    def test_empty_store(self):
        gm = GeoMemory()
        self.assertEqual(gm.radius(Point(0, 0), 1e7), [])
        self.assertEqual(gm.nearest(Point(0, 0), 5), [])
        self.assertEqual(gm.nearest(Point(0, 0), 0), [])
        self.assertIsNone(gm.latest("nobody"))
        self.assertEqual(gm.lineage("missing"), [])
        self.assertEqual(gm.evidence([])["count"], 0)

    def test_invalid_inputs_rejected(self):
        for bad in ((float("nan"), 0), (0, float("inf")), (-90.0001, 0), (0, 180.0001)):
            with self.assertRaises(ValueError):
                Point(*bad)
        with self.assertRaises(ValueError):
            obs("e", "x", 0, 0, conf=-0.01)
        with self.assertRaises(ValueError):
            obs("e", "x", 0, 0, spatial_uncertainty_m=-1)
        with self.assertRaises(ValueError):
            obs("e", "x", 0, 0, duration=timedelta(seconds=-1))
        gm = GeoMemory()
        with self.assertRaises(ValueError):
            gm.radius(Point(0, 0), -1)
        with self.assertRaises(ValueError):
            gm.within_polygon([Point(0, 0), Point(1, 1)])

    def test_concave_polygon(self):
        # U shape: the notch must be excluded.
        ring = [Point(0, 0), Point(0, 3), Point(3, 3), Point(3, 2), Point(1, 2),
                Point(1, 1), Point(3, 1), Point(3, 0)]
        rng = random.Random(5)
        pts = [obs(f"e{i}", "x", rng.uniform(-1, 4), rng.uniform(-1, 4)) for i in range(3000)]
        gm = GeoMemory(GridIndex(0.25))
        gm.ingest_many(pts)
        got = ids(gm.within_polygon(ring))
        want = {o.observation_id for o in pts if _point_in_polygon(o.location, ring)}
        self.assertEqual(got, want)
        notch = {o.observation_id for o in pts
                 if 1 < o.location.lat < 3 and 1 < o.location.lon < 2}
        self.assertFalse(got & notch)

    def test_lineage_cycle_terminates(self):
        a = obs("e", "x", 0, 0, observation_id="a", parent_observation="b")
        b = obs("e", "x", 0, 0, observation_id="b", parent_observation="a")
        gm = GeoMemory()
        gm.ingest_many([a, b])
        self.assertEqual([o.observation_id for o in gm.lineage("a")], ["a", "b"])

    def test_geohash_prefix_property(self):
        rng = random.Random(2)
        for _ in range(500):
            p = Point(rng.uniform(-90, 90), rng.uniform(-180, 180))
            h12 = geohash_encode(p, 12)
            for k in range(1, 12):
                self.assertEqual(geohash_encode(p, k), h12[:k])

    def test_haversine_symmetry_and_triangle(self):
        rng = random.Random(4)
        for _ in range(500):
            a, b, c = (Point(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(3))
            self.assertAlmostEqual(haversine_m(a, b), haversine_m(b, a), places=6)
            self.assertLessEqual(haversine_m(a, c), haversine_m(a, b) + haversine_m(b, c) + 1e-6)
            self.assertLessEqual(haversine_m(a, b), math.pi * 6_371_008.8 + 1e-6)


if __name__ == "__main__":
    unittest.main()
