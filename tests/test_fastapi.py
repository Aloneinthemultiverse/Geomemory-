"""FastAPI service: same behaviour as the stdlib server, plus typed bodies."""
import json
import random
import unittest

from geomemory import GeoMemory

try:
    from fastapi.testclient import TestClient
    from geomemory.api import create_app
    HAVE_FASTAPI = True
except ImportError:
    HAVE_FASTAPI = False

from geomemory.agent import TOOLS, AgentInterface
from test_agent import factory_world, iso


@unittest.skipUnless(HAVE_FASTAPI, "pip install fastapi httpx")
class FastAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        gm = GeoMemory()
        gm.ingest_many(factory_world())
        cls.c = TestClient(create_app(AgentInterface(gm), max_body=10_000))

    def test_ui_docs_and_schema(self):
        self.assertIn("<title>GeoMemory</title>", self.c.get("/").text)
        self.assertEqual(self.c.get("/docs").status_code, 200)
        paths = self.c.get("/openapi.json").json()["paths"]
        for t in TOOLS:
            self.assertIn(f"/tools/{t['name']}", paths)
        self.assertEqual(self.c.get("/api/overview").json()["count"], 5)

    def test_tool_calls(self):
        r = self.c.post("/tools/history_before", json={"entity_id": "Machine_47",
                                                      "event_type": "failure",
                                                      "lookback_minutes": 30})
        self.assertEqual(r.status_code, 200)
        self.assertEqual([t["event_type"] for t in r.json()["timeline"]],
                         ["temperature_anomaly", "vibration_anomaly"])
        r = self.c.post("/tools/events_near", json={"lat": 11, "lon": 76, "radius_m": 100,
                                                   "start": iso(-1000), "end": iso(100)})
        self.assertEqual(r.json()["count"], 5)

    def test_validation_layers(self):
        # Type errors -> 422 from pydantic; semantic errors -> 400 from the agent.
        self.assertEqual(self.c.post("/tools/events_near", json={"lat": "x"}).status_code, 422)
        self.assertEqual(self.c.post("/tools/events_near", json={
            "lat": 11, "lon": 76, "radius_m": 1, "start": iso(0), "end": iso(1),
            "bogus": 1}).status_code, 422)
        r = self.c.post("/tools/events_near", json={"lat": 95, "lon": 76, "radius_m": 1,
                                                   "start": iso(0), "end": iso(1)})
        self.assertEqual(r.status_code, 400)
        self.assertFalse(r.json()["ok"])
        self.assertEqual(self.c.post("/tools/nope", json={}).status_code, 404)
        self.assertEqual(self.c.post("/tools/evidence", content=b" " * 20_000,
                                     headers={"content-type": "application/json"}).status_code, 413)

    def test_fuzz_never_500(self):
        rng = random.Random(0)
        junk = [None, True, -1, 0, 1e300, "", "x", "2026-10-04", [], {}, 91,
                "2026-10-04T09:00:00+00:00", "Machine_47"]
        keys = sorted({k for t in TOOLS for k in t["parameters"]["properties"]})
        for _ in range(400):
            t = rng.choice(TOOLS)["name"]
            body = {k: rng.choice(junk) for k in rng.sample(keys, rng.randrange(0, 6))}
            r = self.c.post(f"/tools/{t}", content=json.dumps(body),
                            headers={"content-type": "application/json"})
            self.assertIn(r.status_code, (200, 400, 422), (t, body, r.text))


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(HAVE_FASTAPI, "pip install fastapi httpx")
class FastAPIShowcaseTests(unittest.TestCase):
    def test_showcase_routes(self):
        from geomemory.showcase import build_memory
        c = TestClient(create_app(AgentInterface(build_memory("showcase"), "showcase")))
        self.assertEqual(len(c.get("/api/replay").json()["citywide"]), 168)
        r = c.post("/api/ask", json={"question": "How busy was Times Square on July 4th?"})
        self.assertEqual((r.status_code, r.json()["tool"]), (200, "activity"))
        self.assertEqual(c.post("/api/ask", content=b"not json").status_code, 400)
        r = c.post("/api/crash", json={"node": 0})
        self.assertTrue(r.json()["after"]["identical"])
        self.assertEqual(c.post("/api/crash", json={"node": 7}).status_code, 400)

