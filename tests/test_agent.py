"""Phase 5: agent interface, HTTP API and retrieval evaluation (§18.12, E6)."""
import json
import random
import unittest
import urllib.error
import urllib.request
from datetime import timedelta

from geomemory import GeoMemory, Point, Provenance, haversine_m
from geomemory.agent import TOOLS, AgentInterface, keyword_baseline, precision_recall, serve
from geomemory.cluster import Cluster
from geomemory.partition import KDPartitioner

from helpers import T0, obs, uniform_world


def iso(minutes):
    return (T0 + timedelta(minutes=minutes)).isoformat()


def factory_world():
    a = obs("Machine_47", "temperature_anomaly", 11, 76, 0, "Sensor_A", 0.91)
    b = obs("Machine_47", "vibration_anomaly", 11, 76, 5, "Sensor_B", 0.88,
            parent_observation=a.observation_id)
    f = obs("Machine_47", "failure", 11, 76, 10, "Sensor_B", 1.0,
            parent_observation=b.observation_id)
    nb = obs("Machine_48", "ping", 11.0002, 76, 9, "Sensor_C")
    old = obs("Machine_47", "maintenance", 11, 76, -600, "Tech_1")
    return [a, b, f, nb, old]


class AgentToolTests(unittest.TestCase):
    def setUp(self):
        self.world = factory_world()
        self.single = GeoMemory()
        self.single.ingest_many(self.world)
        self.cluster = Cluster(KDPartitioner(4, [o.location for o in self.world]), 3, 2)
        self.cluster.ingest_many(self.world)

    def each(self):
        return [AgentInterface(self.single), AgentInterface(self.cluster)]

    def test_what_happened_before_failure(self):  # spec §22
        for ag in self.each():
            r = ag.dispatch("history_before", {"entity_id": "Machine_47",
                                               "event_type": "failure", "lookback_minutes": 30})
            self.assertTrue(r["ok"], r)
            self.assertEqual([(t["offset_minutes"], t["event_type"]) for t in r["timeline"]],
                             [(-10.0, "temperature_anomaly"), (-5.0, "vibration_anomaly")])
            self.assertEqual(r["sources"], ["Sensor_A", "Sensor_B"])
            self.assertEqual(r["anchor"]["event_type"], "failure")
            r2 = ag.dispatch("history_before", {"entity_id": "Machine_47", "at": iso(10),
                                                "lookback_minutes": 1000})
            self.assertEqual(r2["count"], 3)

    def test_evidence_lineage(self):
        f = self.world[2]
        for ag in self.each():
            r = ag.dispatch("evidence", {"observation_id": f.observation_id})
            self.assertEqual([x["event_type"] for x in r["lineage"]],
                             ["failure", "vibration_anomaly", "temperature_anomaly"])

    def test_nearby_and_events(self):
        for ag in self.each():
            r = ag.dispatch("nearby_entities", {"entity_id": "Machine_47", "at": iso(10),
                                                "radius_m": 50, "window_minutes": 2})
            self.assertEqual([e["entity_id"] for e in r["entities"]], ["Machine_48"])
            self.assertAlmostEqual(r["entities"][0]["distance_m"], 22.2, delta=0.5)
            r = ag.dispatch("events_near", {"lat": 11, "lon": 76, "radius_m": 100,
                                            "start": iso(0), "end": iso(60),
                                            "min_confidence": 0.9})
            self.assertEqual({o["event_type"] for o in r["observations"]},
                             {"temperature_anomaly", "failure", "ping"})
            r = ag.dispatch("events_near", {"lat": 11, "lon": 76, "radius_m": 100,
                                            "start": iso(-1000), "end": iso(60), "limit": 2})
            self.assertEqual((r["count"], len(r["observations"]), r["truncated"]), (5, 2, True))

    def test_what_changed(self):
        for ag in self.each():
            r = ag.dispatch("what_changed", {"lat": 11, "lon": 76, "radius_m": 100,
                                             "start": iso(-1), "end": iso(60)})
            self.assertEqual(r["appeared"], ["Machine_48"])
            self.assertEqual(r["changed"][0]["from"], "maintenance")
            self.assertEqual(r["changed"][0]["to"], "failure")
            self.assertEqual(r["count"], 4)

    def test_fused_location(self):
        gm = GeoMemory()
        gm.ingest(obs("obj", "d", 11.0, 76.0, source="sensor_A", conf=0.91, spatial_uncertainty_m=5))
        gm.ingest(obs("obj", "d", 11.00002, 76.0, source="sat_B", conf=0.82, spatial_uncertainty_m=10))
        gm.ingest(obs("obj", "d", 11.00001, 76.00001, source="drone_C", conf=0.74,
                      spatial_uncertainty_m=8))
        gm.ingest(obs("obj", "d", 11.5, 76.0, source="bad_D", conf=0.9, spatial_uncertainty_m=5))
        r = AgentInterface(gm).dispatch("fused_location", {"entity_id": "obj", "start": iso(-1),
                                                           "end": iso(1)})
        self.assertTrue(r["ok"])
        self.assertEqual(len(r["outliers"]), 1)
        self.assertLess(haversine_m(Point(r["lat"], r["lon"]), Point(11, 76)), 5)

    def test_garbage_never_raises(self):
        """Fuzz every tool with hostile arguments: always a dict, never an exception."""
        ag = AgentInterface(self.single)
        rng = random.Random(0)
        junk = [None, True, -1, 0, 1e309, float("nan"), "", "x", "2026-10-04", [], {}, 91, -181,
                "2026-10-04T09:00:00+00:00", "Machine_47", 10 ** 30, -1e9]
        names = [t["name"] for t in TOOLS] + ["nope", None, 5]
        keys = sorted({k for t in TOOLS for k in t["parameters"]["properties"]})
        for _ in range(3000):
            args = {k: rng.choice(junk) for k in rng.sample(keys, rng.randrange(0, len(keys)))}
            r = ag.dispatch(rng.choice(names), rng.choice([args, args, None, [1]]))
            self.assertIsInstance(r, dict)
            self.assertIn("ok", r)
            json.dumps(r)  # always serializable

    def test_cluster_outage_reported(self):
        self.cluster.fail(0)
        self.cluster.fail(1)
        r = AgentInterface(self.cluster).dispatch(
            "events_near", {"lat": 11, "lon": 76, "radius_m": 1e7, "start": iso(0), "end": iso(9)})
        self.assertFalse(r["ok"])
        self.assertIn("unavailable", r["error"])


class ActivityAndHotspotTests(unittest.TestCase):
    def setUp(self):
        rng = random.Random(3)
        self.pts = [obs(f"e{i}", rng.choice(["a", "b"]), 40.75 + rng.gauss(0, 0.01),
                        -73.98 + rng.gauss(0, 0.01), minutes=rng.uniform(0, 600),
                        source=rng.choice(["B1", "B2", "B3"])) for i in range(4000)]
        # a dense cluster that must come out on top
        self.pts += [obs(f"h{i}", "a", 40.76 + rng.gauss(0, 0.0003), -73.97 + rng.gauss(0, 0.0003),
                         minutes=rng.uniform(0, 600), source="B1") for i in range(300)]
        gm = GeoMemory()
        gm.ingest_many(self.pts)
        self.ag = AgentInterface(gm)

    def test_activity_matches_brute_force(self):
        args = {"lat": 40.75, "lon": -73.98, "radius_m": 1500, "start": iso(30), "end": iso(500),
                "bucket_minutes": 45, "group_by": "source_id"}
        r = self.ag.dispatch("activity", args)
        c = Point(40.75, -73.98)
        a, b = T0 + timedelta(minutes=30), T0 + timedelta(minutes=500)
        inside = [o for o in self.pts if a <= o.timestamp <= b and haversine_m(c, o.location) <= 1500]
        self.assertEqual(r["count"], len(inside))
        self.assertEqual(sum(x["count"] for x in r["buckets"]), len(inside))
        for i, bk in enumerate(r["buckets"]):
            lo = a + timedelta(minutes=45 * i)
            want = sum(1 for o in inside if lo <= o.timestamp < lo + timedelta(minutes=45))
            self.assertEqual(bk["count"], want, i)
        self.assertEqual(sum(map(sum, r["groups"].values())), len(inside))
        self.assertEqual([x["count"] for x in r["buckets"]],
                         [sum(v[i] for v in r["groups"].values()) for i in range(len(r["buckets"]))])

    def test_activity_rejects_bad_buckets(self):
        base = {"lat": 0, "lon": 0, "radius_m": 1, "start": iso(0), "end": iso(10 ** 6)}
        self.assertFalse(self.ag.dispatch("activity", {**base, "bucket_minutes": 1})["ok"])
        self.assertFalse(self.ag.dispatch("activity", {**base, "bucket_minutes": 60,
                                                       "group_by": "entity_id"})["ok"])

    def test_hotspots_find_the_cluster(self):
        r = self.ag.dispatch("hotspots", {"lat": 40.75, "lon": -73.98, "radius_m": 5000,
                                          "start": iso(0), "end": iso(600), "cell_m": 150, "top": 5})
        top = r["hotspots"][0]
        self.assertLess(haversine_m(Point(top["lat"], top["lon"]), Point(40.76, -73.97)), 100)
        self.assertGreater(top["sources"]["B1"], 100)
        self.assertEqual(sum(top["sources"].values()), top["count"])
        counts = [h["count"] for h in r["hotspots"]]
        self.assertEqual(counts, sorted(counts, reverse=True))
        empty = self.ag.dispatch("hotspots", {"lat": 0, "lon": 0, "radius_m": 10, "start": iso(0),
                                              "end": iso(1), "cell_m": 100})
        self.assertEqual(empty["hotspots"], [])


class RetrievalEvaluation(unittest.TestCase):  # §18.12 / E6
    def test_spatiotemporal_beats_keyword_baseline(self):
        rng = random.Random(42)
        site = Point(11, 76)
        world = uniform_world(rng, 3000)
        # Same event types near the site but outside September, and elsewhere in September:
        for i in range(300):
            world.append(obs(f"near{i}", "fail", 11 + rng.uniform(-0.005, 0.005),
                             76 + rng.uniform(-0.005, 0.005),
                             minutes=rng.uniform(0, 60 * 24 * 60)))
        gm = GeoMemory()
        gm.ingest_many(world)
        start, end = T0 + timedelta(days=10), T0 + timedelta(days=40)
        relevant = {o.observation_id for o in world
                    if haversine_m(site, o.location) <= 1000 and start <= o.timestamp <= end}
        self.assertGreater(len(relevant), 50)
        r = AgentInterface(gm).dispatch("events_near", {
            "lat": 11, "lon": 76, "radius_m": 1000, "start": start.isoformat(),
            "end": end.isoformat(), "limit": 1000})
        p_geo, r_geo = precision_recall([o["observation_id"] for o in r["observations"]], relevant)
        p_kw, r_kw = precision_recall(keyword_baseline(world, ["fail"], 1000), relevant)
        self.assertEqual((p_geo, r_geo), (1.0, 1.0))
        self.assertLess(p_kw, 0.5)
        for o in r["observations"]:  # spatial, temporal and provenance accuracy
            self.assertLessEqual(haversine_m(site, Point(o["lat"], o["lon"])), 1000)
            self.assertTrue(start.isoformat() <= o["timestamp"] <= end.isoformat())
            self.assertEqual(gm.get(o["observation_id"]).source_id, o["source_id"])

    def test_precision_recall_edges(self):
        self.assertEqual(precision_recall([], []), (1.0, 1.0))
        self.assertEqual(precision_recall([], ["a"]), (0.0, 0.0))
        self.assertEqual(precision_recall(["a", "b"], ["a"]), (0.5, 1.0))


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        gm = GeoMemory()
        gm.ingest_many(factory_world())
        cls.srv, _ = serve(AgentInterface(gm), max_body=10_000)
        cls.base = f"http://127.0.0.1:{cls.srv.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def call(self, path, body=None, raw=None):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(self.base + path, data=data,
                                     method="POST" if data is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_roundtrip(self):
        code, body = self.call("/tools")
        self.assertEqual(code, 200)
        self.assertEqual({t["name"] for t in body["tools"]},
                         {"events_near", "what_changed", "history_before", "nearby_entities",
                          "evidence", "fused_location", "activity", "hotspots"})
        code, body = self.call("/tools/history_before", {"entity_id": "Machine_47",
                                                         "event_type": "failure",
                                                         "lookback_minutes": 30})
        self.assertEqual((code, body["count"]), (200, 2))

    def test_bad_requests(self):
        self.assertEqual(self.call("/tools/history_before", raw=b"{not json")[0], 400)
        self.assertEqual(self.call("/tools/history_before", raw=b"\xff\xfe")[0], 400)
        self.assertEqual(self.call("/tools/nope", {})[0], 400)
        self.assertEqual(self.call("/elsewhere", {})[0], 404)
        self.assertEqual(self.call("/tools/evidence", raw=b" " * 20_000)[0], 413)
        self.assertEqual(self.call("/tools/evidence", {"observation_id": "missing"})[0], 400)

    def test_ui_and_overview(self):
        req = urllib.request.urlopen(self.base + "/", timeout=5)
        html = req.read().decode()
        self.assertEqual(req.status, 200)
        self.assertIn("<title>GeoMemory</title>", html)
        for tool in ("events_near", "what_changed", "history_before", "fused_location"):
            self.assertIn(tool, html)
        code, body = self.call("/api/overview")
        self.assertEqual((code, body["count"], body["entities"]), (200, 5, 2))
        self.assertIn("failure", body["event_types"])
        code, body = self.call("/api/results")
        self.assertEqual((code, body["runs"]), (200, {}))

    def test_concurrent_requests(self):
        from concurrent.futures import ThreadPoolExecutor
        def one(_):
            return self.call("/tools/events_near", {"lat": 11, "lon": 76, "radius_m": 100,
                                                    "start": iso(-1000), "end": iso(100)})
        with ThreadPoolExecutor(16) as ex:
            results = list(ex.map(one, range(64)))
        self.assertTrue(all(code == 200 and body["count"] == 5 for code, body in results))


if __name__ == "__main__":
    unittest.main()
