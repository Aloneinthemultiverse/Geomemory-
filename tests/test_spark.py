"""Spark + Sedona pipeline: the distributed spatial join must count exactly
what a plain Python pass counts. Skipped without pyspark/sedona/data."""
import collections
import math
import unittest

from geomemory.datasets import DATA_DIR, load_uber

try:
    import pyspark  # noqa: F401
    import sedona  # noqa: F401
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False

HAVE_DATA = (DATA_DIR / "uber-raw-data-apr14.csv").exists()


@unittest.skipUnless(HAVE_SPARK and HAVE_DATA, "needs pyspark, apache-sedona and Uber data")
class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from geomemory.backends.spark import run_uber_pipeline, spark_session
        cls.spark = spark_session(cores=2, memory="2g")
        cls.res = run_uber_pipeline(cls.spark, DATA_DIR, ["apr14"], None, cell_deg=0.01)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_counts_match_python(self):
        st = self.res["stats"]
        g = st["grid"]
        py = collections.Counter()
        n = 0
        for o in load_uber(months=("apr14",)):
            n += 1
            r = math.floor((o.location.lat - g["lat0"]) / 0.01)
            c = math.floor((o.location.lon - g["lon0"]) / 0.01)
            py[r * g["cols"] + c] += 1
        self.assertEqual(st["rows_in"], n)
        self.assertEqual((st["valid"], st["invalid"]), (n, 0))
        self.assertEqual(st["joined"], n)  # nothing lost on zone edges
        top = {z["zone_id"]: z["pickups"] for z in st["top_zones"]}
        for zid, cnt in top.items():
            self.assertEqual(cnt, py[zid], zid)
        self.assertEqual(sorted(top.values(), reverse=True),
                         [v for _, v in py.most_common(len(top))])

    def test_hourly_by_base_matches(self):
        from pyspark.sql import functions as F
        h = self.res["hourly"]
        per_base = {r.base: r.n for r in h.groupBy("base").agg(F.sum("count").alias("n")).collect()}
        py = collections.Counter(o.source_id for o in load_uber(months=("apr14",)))
        self.assertEqual(per_base, dict(py))
        # Hour truncation is in UTC: the first pickup (00:11 EDT) lands in 04:00 UTC.
        first = h.orderBy("hour").first().hour
        self.assertEqual(first.strftime("%Y-%m-%d %H:%M"), "2014-04-01 04:00")

    def test_used_a_spatial_join(self):
        self.assertIn(self.res["stats"]["spatial_join_operator"],
                      ("RangeJoin", "BroadcastIndexJoin"))


if __name__ == "__main__":
    unittest.main()
