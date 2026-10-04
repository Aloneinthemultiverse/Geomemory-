"""Every SpatialIndex must give identical answers (spec §18.2 contenders)."""
import random
import unittest

from geomemory import (GeoMemory, GeohashIndex, GridIndex, Point, QuadTreeIndex, ScanIndex,
                       geohash_encode)
from geomemory.index import geohash_bounds

from helpers import brute_radius, hotspot_world, ids, obs, uniform_world

FACTORIES = {
    "scan": ScanIndex,
    "grid": lambda: GridIndex(1.0),
    "geohash4": lambda: GeohashIndex(4),
    "geohash7": lambda: GeohashIndex(7),
    "quadtree": lambda: QuadTreeIndex(16),
    "quadtree1": lambda: QuadTreeIndex(1, max_depth=12),
}


class IndexOracle(unittest.TestCase):
    def check(self, pts, queries):
        for name, f in FACTORIES.items():
            gm = GeoMemory(f())
            gm.ingest_many(pts)
            for c, r in queries:
                self.assertEqual(ids(gm.radius(c, r)), brute_radius(pts, c, r), (name, c, r))

    def test_random_worlds(self):
        for seed in range(4):
            rng = random.Random(seed)
            pts = uniform_world(rng, 1500) + hotspot_world(rng, 1500)
            qs = [(Point(rng.uniform(-90, 90), rng.uniform(-180, 180)), 10 ** rng.uniform(2, 7.3))
                  for _ in range(15)]
            qs += [(Point(11 + rng.gauss(0, .05), 76 + rng.gauss(0, .05)), 10 ** rng.uniform(1, 5))
                   for _ in range(15)]
            self.check(pts, qs)

    def test_edges(self):
        pts = [obs(f"d{i}", "x", 5, 5) for i in range(200)]  # identical points
        pts += [obs("a", "x", 0, 180), obs("b", "x", 0, -180), obs("n", "x", 90, 0),
                obs("s", "x", -90, 77), obs("c", "x", 0, 0)]
        qs = [(Point(5, 5), 0), (Point(0, 180), 1000), (Point(0, -179.99), 5000),
              (Point(89.99, 120), 5000), (Point(-90, 0), 1), (Point(0, 0), 2e7)]
        self.check(pts, qs)

    def test_geohash_bounds_contain_point(self):
        rng = random.Random(1)
        for _ in range(1000):
            p = Point(rng.uniform(-90, 90), rng.uniform(-180, 180))
            for k in (1, 5, 9):
                a, b, c, d = geohash_bounds(geohash_encode(p, k))
                self.assertTrue(a <= p.lat <= b and c <= p.lon <= d)


if __name__ == "__main__":
    unittest.main()
