"""Phase 3: relationship graph."""
import random
import unittest
from datetime import timedelta

from geomemory import Point, haversine_m
from geomemory.graph import RelationshipGraph, connected_when, derive_relationships, obs_node
from geomemory.store import _point_in_polygon

from helpers import T0, obs


class GraphTests(unittest.TestCase):
    def test_near_matches_brute_force(self):
        for seed in range(5):
            rng = random.Random(seed)
            pts = [obs(f"e{rng.randrange(40)}", "ping", 11 + rng.gauss(0, 0.002),
                       76 + rng.gauss(0, 0.002), minutes=rng.uniform(0, 120),
                       observation_id=f"o{i}") for i in range(600)]
            near_m, win = 60.0, timedelta(minutes=3)
            g = derive_relationships(pts, near_m, win)
            want = set()
            for i, a in enumerate(pts):
                for b in pts[i + 1:]:
                    if (a.entity_id != b.entity_id
                            and haversine_m(a.location, b.location) <= near_m
                            and abs(a.timestamp - b.timestamp) <= win):
                        want.add(tuple(sorted((a.entity_id, b.entity_id))))
            got = {(e.src, e.dst) for e in g._edges.values() if e.rel == "NEAR"}
            self.assertEqual(got, want)
            self.assertTrue(want)
            for e in g._edges.values():
                if e.rel == "NEAR":
                    self.assertGreaterEqual(len(e.evidence), 2)
                    self.assertLessEqual(e.first_seen, e.last_seen)

    def test_inside_regions(self):
        rng = random.Random(8)
        zone = [Point(0, 0), Point(0, 1), Point(1, 1), Point(1, 0)]
        pts = [obs(f"e{i}", "x", rng.uniform(-1, 2), rng.uniform(-1, 2)) for i in range(500)]
        g = derive_relationships(pts, near_m=1, regions={"zone": zone})
        got = g.neighbors("zone", "INSIDE", direction="in")
        want = {o.entity_id for o in pts if _point_in_polygon(o.location, zone)}
        self.assertEqual(got, want)

    def test_machine_47_scenario(self):  # spec §2, §22
        temp = obs("Machine_47", "temperature_anomaly", 11, 76, 0, "Sensor_A", 0.91)
        vib = obs("Machine_47", "vibration_anomaly", 11, 76, 5, "Sensor_B", 0.88)
        fail = obs("Machine_47", "failure", 11, 76, 10, "Sensor_B")
        nb = obs("Machine_48", "ping", 11.0001, 76.0001, 9, "Sensor_C")
        far = obs("Machine_99", "ping", 11.01, 76.01, 10, "Sensor_C")
        early = obs("Machine_50", "ping", 11, 76, -60, "Sensor_C")
        g = derive_relationships([fail, vib, temp, nb, far, early], near_m=30,
                                 near_window=timedelta(minutes=2),
                                 regions={"Zone_3": [Point(10.99, 75.99), Point(10.99, 76.01),
                                                     Point(11.01, 76.01), Point(11.01, 75.99)]})
        self.assertEqual(connected_when(g, "Machine_47", fail), {"Machine_48"})
        self.assertEqual(g.neighbors("Machine_47", "OBSERVED_BY"), {"Sensor_A", "Sensor_B"})
        self.assertIn("Zone_3", g.neighbors("Machine_47", "INSIDE"))
        self.assertNotIn("Zone_3", g.neighbors("Machine_99", "INSIDE"))
        self.assertEqual(g.neighbors(obs_node(temp), "BEFORE"), {obs_node(vib)})
        self.assertEqual(g.neighbors(obs_node(fail), "CHANGED_FROM"), {obs_node(vib)})
        self.assertEqual(g.path("Machine_48", "Sensor_A"),
                         ["Machine_48", "Machine_47", "Sensor_A"])
        self.assertIsNone(g.path("Machine_99", "Machine_47", max_depth=1))

    def test_edge_semantics(self):
        g = RelationshipGraph()
        g.add_edge("b", "NEAR", "a", T0, ["o1"])
        g.add_edge("a", "NEAR", "b", T0 + timedelta(hours=1), ["o2"])
        self.assertEqual(len(g), 1)
        e = g.edge("b", "NEAR", "a")
        self.assertEqual(e.evidence, {"o1", "o2"})
        self.assertTrue(e.active(T0 + timedelta(minutes=30)))
        self.assertFalse(e.active(T0 + timedelta(hours=2)))
        self.assertEqual(g.neighbors("a", "NEAR", at=T0 + timedelta(hours=2),
                                     slack=timedelta(hours=1)), {"b"})
        g.add_edge("a", "INSIDE", "z", T0)
        self.assertEqual(g.neighbors("z", "INSIDE"), set())
        self.assertEqual(g.neighbors("z", "INSIDE", direction="in"), {"a"})
        with self.assertRaises(ValueError):
            g.add_edge("a", "NEAR", "a", T0)


if __name__ == "__main__":
    unittest.main()
