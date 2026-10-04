"""Phase 4: uncertainty fusion, association, history, patterns."""
import dataclasses
import math
import random
import statistics
import unittest
from datetime import timedelta

from geomemory import GeoMemory, Point, haversine_m
from geomemory.cluster import Cluster
from geomemory.partition import GridPartitioner
from geomemory import trust

from helpers import T0, obs


def jitter(rng, p, sigma_m):
    """Gaussian noise of sigma_m metres around p (tangent-plane approximation)."""
    dn, de = rng.gauss(0, sigma_m), rng.gauss(0, sigma_m)
    lat = p.lat + math.degrees(dn / 6_371_008.8)
    lon = p.lon + math.degrees(de / (6_371_008.8 * math.cos(math.radians(p.lat))))
    lon = (lon + 180) % 360 - 180
    return max(-90, min(90, lat)), lon


class FusionTests(unittest.TestCase):
    def test_fusion_beats_individual_sources(self):  # §18.10
        errs_fused, errs_best_single = [], []
        for seed in range(200):
            rng = random.Random(seed)
            truth = Point(rng.uniform(-80, 80), rng.uniform(-180, 180))
            ob = []
            for i in range(rng.randrange(3, 12)):
                sigma = rng.choice([5, 10, 25, 50])
                ob.append(obs("x", "detect", *jitter(rng, truth, sigma), source=f"s{i}",
                              conf=rng.uniform(0.5, 1), spatial_uncertainty_m=sigma))
            est = trust.fuse_locations(ob)
            errs_fused.append(haversine_m(est.location, truth))
            errs_best_single.append(statistics.median(haversine_m(o.location, truth) for o in ob))
        self.assertLess(statistics.median(errs_fused), 0.6 * statistics.median(errs_best_single))
        self.assertLess(statistics.median(errs_fused), 15)

    def test_reported_sigma_is_calibrated(self):
        """~95% of fused estimates must fall within 2.5 sigma of the truth."""
        inside, n = 0, 400
        for seed in range(n):
            rng = random.Random(1000 + seed)
            truth = Point(rng.uniform(-60, 60), rng.uniform(-180, 180))
            ob = [obs("x", "d", *jitter(rng, truth, 20), source=f"s{i}",
                      spatial_uncertainty_m=20) for i in range(5)]
            est = trust.fuse_locations(ob, k_sigma=10)
            inside += haversine_m(est.location, truth) <= 2.5 * est.sigma_m * math.sqrt(2)
        self.assertGreater(inside / n, 0.93)

    def test_outlier_flagged_not_deleted(self):  # §18.9
        good = [obs("x", "d", 11 + i * 1e-6, 76, source=f"s{i}", conf=0.9,
                    spatial_uncertainty_m=5) for i in range(6)]
        bad = obs("x", "d", 11.01, 76, source="liar", conf=0.95, spatial_uncertainty_m=5)
        est = trust.fuse_locations(good + [bad])
        self.assertEqual(est.outliers, [bad.observation_id])
        self.assertLess(haversine_m(est.location, Point(11, 76)), 2)
        self.assertEqual(len(est.support), 6)

    def test_multiple_outliers_random(self):
        """Up to 40% gross outliers, placed anywhere: all flagged, estimate unharmed."""
        for seed in range(150):
            rng = random.Random(seed)
            truth = Point(rng.uniform(-70, 70), rng.uniform(-180, 180))
            n_good = rng.randrange(3, 10)
            n_bad = rng.randrange(0, max(1, (2 * n_good) // 3))
            good = [obs("x", "d", *jitter(rng, truth, 5), source=f"g{i}",
                        spatial_uncertainty_m=5, conf=rng.uniform(0.6, 1))
                    for i in range(n_good)]
            bad = [obs("x", "d", *jitter(rng, truth, rng.choice([500, 5000, 50000])),
                       source=f"b{i}", spatial_uncertainty_m=5, conf=rng.uniform(0.6, 1))
                   for i in range(n_bad)]
            est = trust.fuse_locations(good + bad)
            self.assertEqual(set(est.outliers), {o.observation_id for o in bad}, seed)
            self.assertLess(haversine_m(est.location, truth), 15, seed)

    def test_no_majority_means_no_rejection(self):
        a = obs("x", "d", 0, 0, spatial_uncertainty_m=1)
        b = obs("x", "d", 0, 0.1, spatial_uncertainty_m=1)
        est = trust.fuse_locations([a, b])
        self.assertEqual(est.outliers, [])
        self.assertAlmostEqual(est.location.lon, 0.05, places=6)

    def test_antimeridian_and_pole(self):
        a = obs("x", "d", 10, 179.9999, spatial_uncertainty_m=5)
        b = obs("x", "d", 10, -179.9999, spatial_uncertainty_m=5)
        est = trust.fuse_locations([a, b])
        self.assertAlmostEqual(abs(est.location.lon), 180, places=3)  # not 0!
        self.assertLess(haversine_m(est.location, Point(10, 180)), 1)
        polar = [obs("x", "d", 89.9999, lon, spatial_uncertainty_m=5) for lon in (0, 90, 180, -90)]
        est = trust.fuse_locations(polar)
        self.assertGreater(est.location.lat, 89.9999)
        with self.assertRaises(ValueError):
            trust.fuse_locations([obs("x", "d", 0, 0), obs("x", "d", 0, 180)])
        with self.assertRaises(ValueError):
            trust.fuse_locations([])

    def test_confidence_weighting_and_monotonicity(self):
        hi = obs("x", "d", 0, 0, conf=0.99, spatial_uncertainty_m=10)
        lo = obs("x", "d", 0, 0.001, conf=0.1, spatial_uncertainty_m=10)
        est = trust.fuse_locations([hi, lo])
        self.assertLess(haversine_m(est.location, hi.location),
                        haversine_m(est.location, lo.location) / 5)
        c1 = trust.fuse_locations([hi]).confidence
        c2 = trust.fuse_locations([hi, lo]).confidence
        self.assertGreaterEqual(c2, c1)
        self.assertLessEqual(c2, 1.0)
        twin = dataclasses.replace(hi, observation_id="twin")
        self.assertLess(trust.fuse_locations([hi, twin]).sigma_m,
                        trust.fuse_locations([hi]).sigma_m)


class AssociationTests(unittest.TestCase):
    def test_recovers_true_events(self):
        """Noisy multi-source sightings of separate events must regroup exactly."""
        for seed in range(10):
            rng = random.Random(seed)
            truth_of = {}
            all_obs = []
            for ev in range(40):
                p = Point(rng.uniform(-60, 60), rng.uniform(-170, 170))
                t = rng.uniform(0, 10_000)
                for s in range(rng.randrange(1, 6)):
                    o = obs(f"ev{ev}", "d", *jitter(rng, p, 10), minutes=t + rng.uniform(-1, 1),
                            source=f"s{s}")
                    truth_of[o.observation_id] = ev
                    all_obs.append(o)
            groups = trust.associate(all_obs, radius_m=100, window=timedelta(minutes=3))
            got = sorted(sorted(o.observation_id for o in g) for g in groups)
            want = {}
            for oid, ev in truth_of.items():
                want.setdefault(ev, []).append(oid)
            self.assertEqual(got, sorted(sorted(v) for v in want.values()))

    def test_transitive_chain_and_time_separation(self):
        chain = [obs("c", "d", 0, i * 0.0005, observation_id=f"c{i}") for i in range(10)]
        later = obs("c", "d", 0, 0, minutes=60, observation_id="late")
        groups = trust.associate(chain + [later], radius_m=60, window=timedelta(minutes=5))
        self.assertEqual(sorted(len(g) for g in groups), [1, 10])


class HistoryAndPatternTests(unittest.TestCase):
    def setUp(self):
        self.gm = GeoMemory()
        site = Point(11, 76)
        self.site = site
        rows = [("crane", "parked", 0, 0), ("crane", "parked", 0, 60 * 24 * 10),
                ("truck", "parked", 0.0001, 0), ("truck", "parked", 0.003, 60 * 24 * 20),
                ("tank", "normal", 0.0002, 0), ("tank", "leak", 0.0002, 60 * 24 * 15),
                ("drone", "flying", 0.0003, 60 * 24 * 25), ("far", "x", 1.0, 60 * 24 * 25)]
        for e, ev, dlat, m in rows:
            self.gm.ingest(obs(e, ev, 11 + dlat, 76, minutes=m))

    def test_diff_between_snapshots(self):
        d = trust.diff(self.gm, self.site, 1000, T0 + timedelta(hours=1), T0 + timedelta(days=30))
        self.assertEqual(d["appeared"], ["drone"])
        self.assertEqual(sorted(c["entity_id"] for c in d["changed"]), ["tank", "truck"])
        self.assertEqual(d["unchanged"], ["crane"])
        truck = next(c for c in d["changed"] if c["entity_id"] == "truck")
        self.assertGreater(truck["moved_m"], 300)
        with self.assertRaises(ValueError):
            trust.diff(self.gm, self.site, 10, T0 + timedelta(1), T0)

    def test_deteriorating_panels(self):  # §21.6
        sev = ["normal", "hotspot", "crack"]
        gm = GeoMemory()
        seqs = {"p172": ["normal", "hotspot", "crack"], "p1": ["normal", "normal"],
                "p2": ["crack", "normal"], "p3": ["normal", "crack", "hotspot"],
                "p4": ["hotspot", "hotspot", "crack", "crack"], "p5": ["crack"]}
        for e, seq in seqs.items():
            for i, s in enumerate(seq):
                gm.ingest(obs(e, s, 0, 0, minutes=i * 60 * 24 * 30))
                gm.ingest(obs(e, "ping", 0, 0, minutes=i * 60 * 24 * 30 + 1))
        got = {d["entity_id"] for d in trust.deteriorating(gm, seqs, sev)}
        self.assertEqual(got, {"p172", "p4"})

    def test_precursors(self):  # §21.2 / §22
        gm = GeoMemory()
        rng = random.Random(0)
        for m in range(30):
            base = rng.uniform(0, 1e5)
            gm.ingest(obs(f"m{m}", "temperature_anomaly", 0, 0, minutes=base - 10))
            if m % 3 == 0:
                gm.ingest(obs(f"m{m}", "vibration_anomaly", 0, 0, minutes=base - 5))
            gm.ingest(obs(f"m{m}", "maintenance", 0, 0, minutes=base - 500))  # too early
            gm.ingest(obs(f"m{m}", "failure", 0, 0, minutes=base))
        p = dict(trust.precursors(gm, "failure", timedelta(minutes=30)))
        self.assertEqual(p["temperature_anomaly"], 1.0)
        self.assertAlmostEqual(p["vibration_anomaly"], 1 / 3)
        self.assertNotIn("maintenance", p)
        self.assertEqual(trust.precursors(gm, "nonexistent", timedelta(1)), [])

    def test_hotspots_match_brute_force(self):
        rng = random.Random(4)
        pts = [obs(f"e{i}", rng.choice(["failure", "ok"]),
                   11 + rng.gauss(0, 0.001) if i % 2 else rng.uniform(-80, 80),
                   76 + rng.gauss(0, 0.001) if i % 2 else rng.uniform(-180, 180))
               for i in range(3000)]
        hs = trust.hotspots(pts, "failure", cell_m=500, min_count=20)
        self.assertTrue(hs)
        self.assertGreater(hs[0]["count"], 100)
        total = sum(h["count"] for h in hs)
        cells = {}
        for o in pts:
            if o.event_type == "failure":
                cells.setdefault(trust._cell(o.location, 500), []).append(o)
        self.assertEqual(total, sum(len(v) for v in cells.values() if len(v) >= 20))
        for h in hs:
            locs = [o.location for o in pts if o.observation_id in set(h["evidence"])]
            span = max(haversine_m(a, b) for a in locs[:50] for b in locs[:50])
            self.assertLess(span, 500 * 3)  # cells really are ~cell_m wide

    def test_cell_size_is_uniform_across_latitudes(self):
        rng = random.Random(6)
        for lat in (0, 45, 70, 85):
            p = Point(lat, rng.uniform(-170, 170))
            # Points 600 m apart east-west must land in different 500 m cells.
            q = Point(lat, p.lon + math.degrees(600 / (6_371_008.8 * math.cos(math.radians(lat)))))
            self.assertNotEqual(trust._cell(p, 500), trust._cell(q, 500), lat)

    def test_sequence_similarity(self):
        a = ["normal", "hotspot", "crack"]
        self.assertEqual(trust.sequence_similarity(a, a), 1.0)
        self.assertEqual(trust.sequence_similarity(a, ["x", "y", "z"]), 0.0)
        self.assertEqual(trust.sequence_similarity([], []), 1.0)
        self.assertEqual(trust.sequence_similarity(["a"], ["a"]), 1.0)
        mid = trust.sequence_similarity(a, ["normal", "hotspot", "fire"])
        self.assertTrue(0 < mid < 1)
        for seed in range(50):
            rng = random.Random(seed)
            x = [rng.choice("abcd") for _ in range(rng.randrange(0, 10))]
            y = [rng.choice("abcd") for _ in range(rng.randrange(0, 10))]
            self.assertEqual(trust.sequence_similarity(x, y), trust.sequence_similarity(y, x))

    def test_works_on_cluster(self):
        c = Cluster(GridPartitioner(4, 0.5), 2, 2)
        c.ingest_many(self.gm._obs.values())
        self.assertEqual(trust.diff(c, self.site, 1000, T0 + timedelta(hours=1),
                                    T0 + timedelta(days=30)),
                         trust.diff(self.gm, self.site, 1000, T0 + timedelta(hours=1),
                                    T0 + timedelta(days=30)))
        self.assertEqual(c.entity_ids(), self.gm.entity_ids())


if __name__ == "__main__":
    unittest.main()
