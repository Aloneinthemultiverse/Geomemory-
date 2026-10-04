"""Apache AGE graph must agree with the in-memory RelationshipGraph."""
import random
import unittest
from datetime import timedelta

from geomemory import Point
from geomemory.graph import RelationshipGraph, derive_relationships, obs_node

from helpers import T0, obs
from test_postgis import HAVE_PG


def _have_age():
    if not HAVE_PG:
        return False
    try:
        from geomemory.backends.graph import AgeGraph
        AgeGraph(graph="probe_graph", reset=True).close()
        return True
    except Exception:
        return False


HAVE_AGE = _have_age()


@unittest.skipUnless(HAVE_AGE, "Apache AGE not available")
class AgeGraphTests(unittest.TestCase):
    def test_bulk_load_matches_memory(self):
        from geomemory.backends.graph import AgeGraph
        rng = random.Random(4)
        pts = [obs(f"e{rng.randrange(25)}", rng.choice(["ping", "fail"]),
                   11 + rng.gauss(0, 0.002), 76 + rng.gauss(0, 0.002),
                   minutes=rng.uniform(0, 120), source=f"s{rng.randrange(4)}",
                   observation_id=f"o{i}") for i in range(250)]
        zone = [Point(10.999, 75.999), Point(10.999, 76.001), Point(11.001, 76.001),
                Point(11.001, 75.999)]
        rg = derive_relationships(pts, 60, timedelta(minutes=3), regions={"Zone": zone})
        with AgeGraph(graph="test_bulk", reset=True) as g:
            self.assertEqual(g.load(rg), len(rg))
            nodes = {e.src for e in rg._edges.values()} | {e.dst for e in rg._edges.values()}
            self.assertEqual(g.count(), (len(nodes), len(rg)))
            entities = sorted(n for n in nodes if not n.startswith("obs:"))
            for n in entities:
                for rel in ("NEAR", "OBSERVED_BY", "INSIDE", None):
                    for d in ("out", "in"):
                        self.assertEqual(g.neighbors(n, rel, direction=d),
                                         rg.neighbors(n, rel, direction=d), (n, rel, d))
            for _ in range(15):
                a, b = rng.sample(entities, 2)
                pm, pa = rg.path(a, b, 4), g.path(a, b, 4)
                self.assertEqual(pm is None, pa is None, (a, b))
                if pm:
                    self.assertEqual(len(pa), len(pm))
            e = next(e for e in rg._edges.values() if e.rel == "NEAR")
            got = g.edge(e.dst, "NEAR", e.src)  # symmetric lookup either way round
            self.assertEqual((got["first_seen"], got["last_seen"], got["evidence"]),
                             (e.first_seen, e.last_seen, e.evidence))

    def test_incremental_edges_and_time_filter(self):
        from geomemory.backends.graph import AgeGraph
        with AgeGraph(graph="test_incr", reset=True) as g:
            g.add_edge("b", "NEAR", "a", T0, ["o1"])
            g.add_edge("a", "NEAR", "b", T0 + timedelta(hours=1), ["o2"])
            g.add_edge("a", "INSIDE", "zone", T0)
            self.assertEqual(g.count(), (3, 2))
            e = g.edge("a", "NEAR", "b")
            self.assertEqual((e["first_seen"], e["last_seen"], e["evidence"]),
                             (T0, T0 + timedelta(hours=1), {"o1", "o2"}))
            self.assertEqual(g.neighbors("a", "NEAR", at=T0 + timedelta(minutes=30)), {"b"})
            self.assertEqual(g.neighbors("a", "NEAR", at=T0 + timedelta(hours=2)), set())
            self.assertEqual(g.neighbors("zone", "INSIDE"), set())
            self.assertEqual(g.neighbors("zone", "INSIDE", direction="in"), {"a"})
            self.assertEqual(g.path("b", "zone"), ["b", "a", "zone"])
            with self.assertRaises(ValueError):
                g.add_edge("a", "NEAR", "a", T0)
            with self.assertRaises(ValueError):
                g.add_edge("a", "BAD REL", "b", T0)

    def test_injection_is_inert(self):
        from geomemory.backends.graph import AgeGraph
        with AgeGraph(graph="test_inj", reset=True) as g:
            evil = 'x"}) DETACH DELETE (n) //'
            g.add_edge(evil, "NEAR", "y'; DROP TABLE observations; --", T0)
            self.assertEqual(g.count(), (2, 1))
            self.assertEqual(g.neighbors(evil, "NEAR"), {"y'; DROP TABLE observations; --"})
            with self.assertRaises(ValueError):
                g.add_edge("a$cy$b", "NEAR", "c", T0)


if __name__ == "__main__":
    unittest.main()
