"""The benchmark harness itself: runs end to end and reports correct results."""
import json
import tempfile
import unittest
from pathlib import Path

from geomemory import bench


class BenchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.res = bench.run("tiny", seed=1, out=cls.dir, log=lambda *_: None)

    def test_all_experiments_correct(self):
        e = self.res["experiments"]
        self.assertEqual(sorted(e), ["E1", "E2", "E3", "E4", "E5", "E6"])
        for name in ("E1", "E2", "E3", "E4"):
            for row in e[name]["rows"]:
                self.assertTrue(row["correct"], (name, row))

    def test_expected_shapes(self):
        e = self.res["experiments"]
        self.assertEqual([r["n"] for r in e["E1"]["rows"]], list(bench.SCALES["tiny"].e1_sizes))
        self.assertEqual(len(e["E2"]["rows"]), len(bench.INDEXES))
        self.assertAlmostEqual(e["E3"]["rows"][0]["speedup"], 1.0)
        for r in e["E3"]["rows"]:
            self.assertGreaterEqual(r["messages_per_query"], 2)
        geo, kw = e["E6"]["rows"]
        self.assertEqual((geo["precision"], geo["recall"]), (1.0, 1.0))
        self.assertLess(kw["precision"], geo["precision"])
        skew = {(r["distribution"], r["partitioner"]): r for r in e["E5"]["rows"]}
        self.assertLess(skew[("hotspot", "KD (adaptive)")]["imbalance_max_over_mean"],
                        skew[("hotspot", "Grid 10°")]["imbalance_max_over_mean"])

    def test_files_written(self):
        d = Path(self.dir)
        self.assertEqual(json.loads((d / "results.json").read_text())["scale"], "tiny")
        md = (d / "RESULTS.md").read_text()
        for h in ("E1", "E2", "E3", "E4", "E5", "E6"):
            self.assertIn(f"## {h}", md)
        self.assertNotIn("**NO**", md)

    def test_datasets_deterministic(self):
        a = bench.make_dataset(100, 7, "hotspot")
        b = bench.make_dataset(100, 7, "hotspot")
        self.assertEqual([(o.location, o.timestamp) for o in a],
                         [(o.location, o.timestamp) for o in b])

    def test_cli_rejects_unknown_experiment(self):
        with self.assertRaises(SystemExit):
            bench.main(["--experiments", "E9", "--out", self.dir])


if __name__ == "__main__":
    unittest.main()
