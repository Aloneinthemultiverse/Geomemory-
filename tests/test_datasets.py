"""Real datasets: loaders parse correctly. Skipped if data/raw is absent."""
import unittest
from datetime import timezone

from geomemory import GeoMemory, Point
from geomemory.datasets import DATA_DIR, UBER_BASES, load_quakes, load_uber

HAVE_UBER = (DATA_DIR / "uber-raw-data-apr14.csv").exists()
HAVE_QUAKES = (DATA_DIR / "earthquakes-23k.csv").exists()


@unittest.skipUnless(HAVE_UBER, "run: python -m geomemory.datasets download")
class UberTests(unittest.TestCase):
    def test_first_rows(self):
        rows = list(load_uber(limit=2000))
        self.assertEqual(len(rows), 2000)
        first = rows[0]
        # "4/1/2014 0:11:00" New York time (EDT, UTC-4) -> 04:11 UTC
        self.assertEqual(first.timestamp.isoformat(), "2014-04-01T04:11:00+00:00")
        self.assertEqual((first.location.lat, first.location.lon), (40.769, -73.9549))
        self.assertEqual(first.source_id, "B02512")
        for o in rows:
            self.assertIn(o.source_id, UBER_BASES)
            self.assertTrue(39.5 < o.location.lat < 42 and -75 < o.location.lon < -72)
        self.assertEqual(len({o.observation_id for o in rows}), 2000)

    def test_queryable(self):
        gm = GeoMemory()
        gm.ingest_many(load_uber(limit=20_000))
        midtown = gm.radius(Point(40.7549, -73.9840), 500)
        self.assertGreater(len(midtown), 100)  # Times Square area is busy


@unittest.skipUnless(HAVE_QUAKES, "run: python -m geomemory.datasets download")
class QuakeTests(unittest.TestCase):
    def test_all_rows_parse(self):
        rows = list(load_quakes())
        self.assertEqual(len(rows), 23_412)
        self.assertTrue(all(o.timestamp.tzinfo == timezone.utc for o in rows))
        self.assertEqual(min(o.timestamp.year for o in rows), 1965)
        self.assertTrue(any(abs(o.location.lon) > 179 for o in rows))  # antimeridian
        self.assertTrue(all(o.attributes["magnitude"] >= 5.5 for o in rows))


if __name__ == "__main__":
    unittest.main()
