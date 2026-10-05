"""Showcase: the committed NYC pack, plain-English questions, the crash test
and the HTTP routes the web UI uses for them."""
import json
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone

from geomemory import showcase
from geomemory.agent import TOOLS, AgentInterface, serve
from geomemory.showcase import CrashTest, ask, build_memory, load_week, parse_question


class PackTests(unittest.TestCase):
    def test_week_sample_is_real_and_in_range(self):
        obs = list(load_week())
        self.assertGreater(len(obs), 20_000)
        self.assertEqual(len({o.observation_id for o in obs}), len(obs))
        lo = datetime(2014, 7, 1, 4, tzinfo=timezone.utc)   # Jul 1 00:00 New York
        hi = datetime(2014, 7, 8, 4, tzinfo=timezone.utc)
        for o in obs:
            self.assertTrue(lo <= o.timestamp < hi, o)
            self.assertTrue(39.0 < o.location.lat < 42.0 and -75.0 < o.location.lon < -72.0, o)
            self.assertTrue(o.observation_id.startswith("uber_jul14_"))
            self.assertIn(o.source_id, showcase._BASES)
        self.assertEqual([o.timestamp for o in obs], sorted(o.timestamp for o in obs))

    def test_week_sample_matches_raw_rows(self):
        from geomemory.datasets import DATA_DIR, load_uber
        if not (DATA_DIR / "uber-raw-data-jul14.csv").exists():
            self.skipTest("raw Uber data not downloaded")
        sample = {o.observation_id: o for o in load_week()}
        hits = 0
        for o in load_uber(months=("jul14",)):
            s = sample.get(o.observation_id)
            if s:
                hits += 1
                self.assertEqual((s.location, s.timestamp, s.source_id),
                                 (o.location, o.timestamp, o.source_id))
        self.assertEqual(hits, len(sample))

    def test_replay_grid_shape(self):
        r = showcase.replay()
        self.assertEqual(len(r["cells"]), len(r["values"]))
        self.assertTrue(all(len(v) == 168 for v in r["values"]))
        self.assertEqual(len(r["citywide"]), 168)
        self.assertGreater(r["pickups"], 4_000_000)
        # Weekday rush hour is busier than 4 am on Sunday, citywide.
        self.assertGreater(r["citywide"][2 * 24 + 18], 3 * r["citywide"][6 * 24 + 4])


class QuestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent = AgentInterface(build_memory("showcase"), "showcase")

    def test_parser_routes_to_the_right_tool(self):
        cases = {
            "How busy was Times Square on July 4th?": ("activity", None),
            "Where were the busiest spots in Manhattan on Saturday night?": ("hotspots", None),
            "Which companies picked people up at JFK over the weekend?": ("activity", "source_id"),
            "Show pickups near Times Square on Friday night": ("events_near", None),
            "Why did machine 47 fail?": ("history_before", None),
            "Where exactly is the flood?": ("fused_location", None),
            "what changed at the solar farm": ("what_changed", None),
            "what happened at the factory": ("events_near", None),
            "tell me something": ("activity", None),
        }
        for q, (tool, group) in cases.items():
            p = parse_question(q)
            self.assertEqual(p["tool"], tool, q)
            self.assertEqual(p["args"].get("group_by"), group, q)
            self.assertTrue(p["understood"], q)
            self.assertEqual(TOOLS[[t["name"] for t in TOOLS].index(tool)]["name"], tool)

    def test_every_parsed_question_runs(self):
        for q in ["How busy was Times Square on July 4th?", "busiest spots in brooklyn",
                  "JFK on monday morning", "LaGuardia tuesday afternoon by company",
                  "Show pickups near Grand Central on Wednesday", "Why did machine 47 fail?",
                  "Where exactly is the flood?", "", "!!!", "x" * 400]:
            r = ask(self.agent, q, key="")
            if not q.strip():
                self.assertFalse(r["ok"])
                continue
            self.assertTrue(r["ok"], q)
            self.assertTrue(r["result"]["ok"], (q, r["result"]))
            self.assertEqual(r["mode"], "rules")
            self.assertIn("took_ms", r["result"])

    def test_times_square_on_july_4th_matches_a_direct_query(self):
        r = ask(self.agent, "How busy was Times Square on July 4th?", key="")
        direct = self.agent.dispatch("activity", r["args"])
        self.assertEqual(r["result"]["count"], direct["count"])
        self.assertGreater(direct["count"], 0)
        self.assertEqual(r["args"]["start"], "2014-07-04T04:00:00+00:00")

    def test_too_long_question(self):
        self.assertFalse(ask(self.agent, "x" * 501, key="")["ok"])

    def test_llm_path_runs_tools_and_returns_its_answer(self):
        calls = []

        def fake_llm(body, key):
            calls.append(body)
            self.assertEqual(key, "k")
            self.assertEqual({t["name"] for t in body["tools"]}, {t["name"] for t in TOOLS})
            if len(calls) == 1:
                return {"content": [{"type": "tool_use", "id": "t1", "name": "history_before",
                                     "input": {"entity_id": "Machine_47", "event_type": "failure",
                                               "lookback_minutes": 60}}]}
            # The tool result went back to the model.
            res = json.loads(body["messages"][-1]["content"][0]["content"])
            self.assertTrue(res["ok"])
            return {"content": [{"type": "text", "text": f"It overheated first ({res['count']} signs)."}]}

        r = ask(self.agent, "why did machine 47 break", key="k", llm=fake_llm)
        self.assertEqual(r["mode"], "llm")
        self.assertIn("overheated", r["answer"])
        self.assertEqual(r["tool"], "history_before")
        self.assertTrue(r["result"]["ok"])
        self.assertEqual(len(calls), 2)

    def test_llm_failure_falls_back_to_rules(self):
        def broken(body, key):
            raise urllib.error.URLError("offline")
        r = ask(self.agent, "Where exactly is the flood?", key="k", llm=broken)
        self.assertEqual(r["mode"], "rules")
        self.assertIn("unavailable", r["note"])
        self.assertTrue(r["result"]["ok"])


class ChatTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent = AgentInterface(build_memory("showcase"), "showcase")

    def test_history_is_validated(self):
        bad = [None, [], "hi", [{"role": "assistant", "content": "x"}],
               [{"role": "user", "content": "q"}, {"role": "assistant", "content": "a"}],
               [{"role": "system", "content": "q"}], [{"role": "user", "content": ""}],
               [{"role": "user", "content": "x" * 501}], [{"role": "user", "content": 5}],
               [{"role": "user", "content": "q"}] * 41]
        for h in bad:
            self.assertFalse(showcase.chat(self.agent, h, key="")["ok"], h)

    def test_follow_up_sends_whole_conversation_and_reports_steps(self):
        seen = []

        def fake_llm(body, key):
            seen.append([m for m in body["messages"]])
            last = body["messages"][-1]
            if isinstance(last["content"], str):
                return {"stop_reason": "tool_use", "content": [
                    {"type": "tool_use", "id": "t1", "name": "hotspots",
                     "input": {"lat": 40.758, "lon": -73.9855, "radius_m": 7000,
                               "start": "2014-07-05T22:00:00Z", "end": "2014-07-06T06:00:00Z",
                               "cell_m": 300}},
                    {"type": "tool_use", "id": "t2", "name": "nope", "input": {}}]}
            results = last["content"]
            self.assertEqual([r["tool_use_id"] for r in results], ["t1", "t2"])
            self.assertTrue(results[1]["is_error"])
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Go to the East Village."}]}

        h = [{"role": "user", "content": "Where on Friday night?"},
             {"role": "assistant", "content": "Mostly downtown."},
             {"role": "user", "content": "and Saturday?"}]
        r = showcase.chat(self.agent, h, key="k", llm=fake_llm)
        self.assertEqual((r["mode"], r["answer"]), ("llm", "Go to the East Village."))
        self.assertEqual(seen[0][:3], h)                      # history went to the model
        self.assertEqual([s["tool"] for s in r["steps"]], ["hotspots", "nope"])
        self.assertTrue(r["steps"][0]["ok"])
        self.assertGreater(r["steps"][0]["count"], 0)
        self.assertGreaterEqual(r["steps"][0]["took_ms"], 0)
        self.assertFalse(r["steps"][1]["ok"])
        self.assertEqual(r["tool"], "hotspots")

    def test_refusal_falls_back(self):
        r = showcase.chat(self.agent, [{"role": "user", "content": "Where exactly is the flood?"}], key="k",
                          llm=lambda b, k: {"stop_reason": "refusal", "content": []})
        self.assertEqual(r["mode"], "rules")
        self.assertIn("unavailable", r["note"])

    def test_without_key_uses_reader_with_steps(self):
        r = showcase.chat(self.agent, [{"role": "user", "content": "How busy was Times Square on July 4th?"}], key="")
        self.assertEqual((r["mode"], r["steps"][0]["tool"]), ("rules", "activity"))


class CrashTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mem = build_memory("showcase")
        cls.ct = CrashTest(cls.mem)

    def test_every_server_can_die_without_changing_the_answer(self):
        truth = len(self.mem.radius_between(
            showcase.Point(*showcase.TIMES_SQUARE), 1500,
            datetime(2014, 1, 1, tzinfo=timezone.utc), datetime(2015, 1, 1, tzinfo=timezone.utc)))
        lost_somewhere = False
        for node in range(4):
            r = self.ct.run(node)
            self.assertEqual(r["killed"], node)
            self.assertEqual(r["before"]["count"], truth)
            self.assertEqual(r["after"]["count"], truth)
            self.assertTrue(r["after"]["identical"])
            self.assertGreater(r["recovered"]["records"], 0)
            lost_somewhere |= r["without_copies"]["lost"] > 0
            # The server is back with all its data for the next run.
            self.assertTrue(all(n.alive for n in self.ct.cluster.nodes))
        self.assertTrue(lost_somewhere)
        self.assertEqual(sum(n.record_count() for n in self.ct.cluster.nodes), 2 * self.ct.n)

    def test_bad_server(self):
        with self.assertRaises(ValueError):
            self.ct.run(9)


class RouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv, _ = serve(AgentInterface(build_memory("showcase"), "showcase"))
        cls.base = f"http://127.0.0.1:{cls.srv.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def call(self, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method="POST" if data else "GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_routes(self):
        code, r = self.call("/api/replay")
        self.assertEqual((code, len(r["citywide"])), (200, 168))
        code, r = self.call("/api/ask", {"question": "Where exactly is the flood?"})
        self.assertEqual((code, r["tool"]), (200, "fused_location"))
        code, r = self.call("/api/ask", {"question": 5})
        self.assertEqual(code, 400)
        code, r = self.call("/api/ask", [1, 2])
        self.assertEqual(code, 400)
        code, r = self.call("/api/chat", {"messages": [{"role": "user", "content": "Why did machine 47 fail?"}]})
        self.assertEqual((code, r["tool"]), (200, "history_before"))
        code, r = self.call("/api/chat", {"messages": "nope"})
        self.assertEqual(code, 400)
        code, r = self.call("/api/crash", {"node": 1})
        self.assertEqual((code, r["killed"], r["after"]["identical"]), (200, 1, True))
        code, r = self.call("/api/crash", {"node": "x"})
        self.assertEqual(code, 400)
        code, r = self.call("/api/nope", {})
        self.assertEqual(code, 404)
        code, r = self.call("/api/overview")
        self.assertEqual(r["dataset"], "showcase")
        self.assertFalse(r["entity_names"][0].count("_trip_"))  # demo entities listed first
        code, r = self.call("/tools/events_near", {"lat": 40.758, "lon": -73.9855, "radius_m": 300,
                                                   "start": "2014-07-04T00:00:00Z",
                                                   "end": "2014-07-05T00:00:00Z"})
        self.assertEqual(code, 200)
        self.assertGreater(r["searched"], 40_000)
        self.assertGreaterEqual(r["took_ms"], 0)


if __name__ == "__main__":
    unittest.main()
