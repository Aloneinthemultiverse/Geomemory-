"""The demo world must actually contain the stories the UI scenarios show."""
import unittest
from datetime import timedelta

from geomemory import GeoMemory, Point, haversine_m
from geomemory.agent import AgentInterface
from geomemory.demo import FLOOD, NOW, build_world


class DemoWorldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gm = GeoMemory()
        cls.gm.ingest_many(build_world())
        cls.ag = AgentInterface(cls.gm)

    def test_deterministic(self):
        a, b = build_world(), build_world()
        self.assertEqual([o.observation_id for o in a], [o.observation_id for o in b])

    def test_machine_47_story(self):
        r = self.ag.dispatch("history_before", {"entity_id": "Machine_47", "event_type": "failure",
                                                "lookback_minutes": 60})
        self.assertEqual([t["event_type"] for t in r["timeline"]][-2:],
                         ["temperature_anomaly", "vibration_anomaly"])
        ev = self.ag.dispatch("evidence", {"observation_id": "obs_m47_fail"})
        self.assertEqual(len(ev["lineage"]), 3)

    def test_flood_fusion_flags_only_citizen_report(self):
        r = self.ag.dispatch("fused_location", {
            "entity_id": "Flood_Event_1", "start": (NOW - timedelta(days=7)).isoformat(),
            "end": NOW.isoformat()})
        self.assertEqual([self.gm.get(o).source_id for o in r["outliers"]], ["Citizen_Report"])
        self.assertLess(haversine_m(Point(r["lat"], r["lon"]), Point(*FLOOD)), 20)

    def test_deteriorating_panels_present(self):
        from geomemory.trust import deteriorating
        panels = [e for e in self.gm.entity_ids() if e.startswith("Panel_")]
        got = {d["entity_id"] for d in deteriorating(self.gm, panels, ["normal", "hotspot", "crack"])}
        self.assertTrue({"Panel_008", "Panel_014", "Panel_043", "Panel_056"} <= got)


if __name__ == "__main__":
    unittest.main()
