"""Relationship graph in Apache AGE: openCypher (Neo4j's query language)
inside PostgreSQL, next to the PostGIS observations (spec §24: "Neo4j or an
equivalent graph layer integrated into the storage architecture").

`AgeGraph` mirrors `RelationshipGraph`: typed edges carrying first/last seen
times and supporting observation ids; symmetric relations stored once.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from ..graph import SYMMETRIC, RelationshipGraph
from . import DEFAULT_DSN

_TAG = "$cy$"


def _lit(v) -> str:
    """Cypher literal. Strings via JSON quoting; dollar-tag injection refused."""
    if isinstance(v, str):
        if _TAG in v:
            raise ValueError("string contains reserved sequence")
        return json.dumps(v)
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_lit(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {_lit(x)}" for k, x in v.items()) + "}"
    if isinstance(v, bool) or v is None:
        return {True: "true", False: "false", None: "null"}[v]
    return repr(v)


def _iso(t: datetime) -> str:
    return t.isoformat()


class AgeGraph:
    def __init__(self, dsn: str = DEFAULT_DSN, graph: str = "geomemory",
                 reset: bool = False) -> None:
        import psycopg
        if not graph.isidentifier():
            raise ValueError("graph must be a plain identifier")
        self.graph = graph
        self.conn = psycopg.connect(dsn, autocommit=True)
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS age")
            cur.execute("LOAD 'age'")
            cur.execute('SET search_path = ag_catalog, "$user", public')
            cur.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (graph,))
            exists = cur.fetchone()[0] == 1
            if exists and reset:
                cur.execute("SELECT drop_graph(%s, true)", (graph,))
                exists = False
            if not exists:
                cur.execute("SELECT create_graph(%s)", (graph,))
                self._cypher("CREATE (:Node {id: '__init__'})")
                self._cypher("MATCH (n:Node {id: '__init__'}) DELETE n")
                cur.execute(f'CREATE INDEX IF NOT EXISTS {graph}_node_props '
                            f'ON {graph}."Node" USING gin (properties)')

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _cypher(self, query: str, cols: str = "v agtype") -> list[tuple]:
        if _TAG in query:
            raise ValueError("query contains reserved sequence")
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM cypher('{self.graph}', {_TAG}{query}{_TAG}) AS ({cols})")
            return cur.fetchall() if cur.description else []

    @staticmethod
    def _val(agtype_text):
        """agtype values come back as text; scalars and lists are JSON."""
        if agtype_text is None:
            return None
        s = str(agtype_text)
        for suffix in ("::vertex", "::edge", "::path"):
            if s.endswith(suffix):
                s = s[: -len(suffix)]
        return json.loads(s)

    @staticmethod
    def _rel(rel: str) -> str:
        if not rel.isidentifier():
            raise ValueError(f"relation must be an identifier: {rel!r}")
        return rel

    # --- writes ---------------------------------------------------------------

    def add_edge(self, src: str, rel: str, dst: str, t: datetime, evidence=()) -> None:
        if src == dst:
            raise ValueError("self-loops are not allowed")
        rel = self._rel(rel)
        if rel in SYMMETRIC and dst < src:
            src, dst = dst, src
        for n in (src, dst):
            self._cypher(f"MERGE (:Node {{id: {_lit(n)}}})")
        rows = self._cypher(
            f"MATCH (a:Node {{id: {_lit(src)}}})-[e:{rel}]->(b:Node {{id: {_lit(dst)}}}) "
            f"RETURN [e.first_seen, e.last_seen, e.evidence]")
        if rows:
            first, last, ev = self._val(rows[0][0])
            first, last = min(first, _iso(t)), max(last, _iso(t))
            ev = sorted(set(ev) | set(evidence))
            self._cypher(
                f"MATCH (a:Node {{id: {_lit(src)}}})-[e:{rel}]->(b:Node {{id: {_lit(dst)}}}) "
                f"SET e.first_seen = {_lit(first)}, e.last_seen = {_lit(last)}, "
                f"e.evidence = {_lit(ev)}")
        else:
            self._cypher(
                f"MATCH (a:Node {{id: {_lit(src)}}}), (b:Node {{id: {_lit(dst)}}}) "
                f"CREATE (a)-[:{rel} {{first_seen: {_lit(_iso(t))}, "
                f"last_seen: {_lit(_iso(t))}, evidence: {_lit(sorted(set(evidence)))}}}]->(b)")

    def load(self, rg: RelationshipGraph, batch: int = 500) -> int:
        """Bulk-copy an in-memory RelationshipGraph (e.g. from
        `derive_relationships`) into an empty AGE graph."""
        edges = list(rg._edges.values())
        nodes = sorted({e.src for e in edges} | {e.dst for e in edges})
        for i in range(0, len(nodes), batch):
            self._cypher(f"UNWIND {_lit(nodes[i:i + batch])} AS id CREATE (:Node {{id: id}})")
        by_rel: dict[str, list] = {}
        for e in edges:
            by_rel.setdefault(self._rel(e.rel), []).append(
                {"s": e.src, "d": e.dst, "f": _iso(e.first_seen), "l": _iso(e.last_seen),
                 "ev": sorted(e.evidence)})
        for rel, rows in by_rel.items():
            for i in range(0, len(rows), batch):
                self._cypher(
                    f"UNWIND {_lit(rows[i:i + batch])} AS r "
                    f"MATCH (a:Node {{id: r.s}}), (b:Node {{id: r.d}}) "
                    f"CREATE (a)-[:{rel} {{first_seen: r.f, last_seen: r.l, evidence: r.ev}}]->(b)")
        return len(edges)

    # --- reads ---------------------------------------------------------------------

    def edge(self, src: str, rel: str, dst: str) -> dict | None:
        rel = self._rel(rel)
        if rel in SYMMETRIC and dst < src:
            src, dst = dst, src
        rows = self._cypher(
            f"MATCH (a:Node {{id: {_lit(src)}}})-[e:{rel}]->(b:Node {{id: {_lit(dst)}}}) "
            f"RETURN [e.first_seen, e.last_seen, e.evidence]")
        if not rows:
            return None
        f, l, ev = self._val(rows[0][0])
        return {"src": src, "rel": rel, "dst": dst, "first_seen": datetime.fromisoformat(f),
                "last_seen": datetime.fromisoformat(l), "evidence": set(ev)}

    def neighbors(self, node: str, rel: str | None = None, at: datetime | None = None,
                  slack: timedelta = timedelta(0), direction: str = "out") -> set[str]:
        typ = f":{self._rel(rel)}" if rel else ""
        rows = self._cypher(
            f"MATCH (a:Node {{id: {_lit(node)}}})-[e{typ}]-(b:Node) "
            f"RETURN [b.id, label(e), startNode(e).id, e.first_seen, e.last_seen]")
        out = set()
        for (r,) in rows:
            other, label, start, first, last = self._val(r)
            if at is not None and not (datetime.fromisoformat(first) - slack <= at
                                       <= datetime.fromisoformat(last) + slack):
                continue
            outgoing = start == node
            if label in SYMMETRIC or (direction == "out") == outgoing:
                out.add(other)
        return out

    def path(self, src: str, dst: str, max_depth: int = 6) -> list[str] | None:
        """Shortest undirected path, by trying lengths 1..max_depth in Cypher."""
        if src == dst:
            return [src]
        for k in range(1, max_depth + 1):
            rows = self._cypher(
                f"MATCH p = (a:Node {{id: {_lit(src)}}})-[*{k}..{k}]-(b:Node {{id: {_lit(dst)}}}) "
                f"WITH p LIMIT 1 UNWIND nodes(p) AS n RETURN n.id")
            if rows:  # AGE 1.5 has no list comprehensions; UNWIND keeps path order
                return [self._val(r[0]) for r in rows]
        return None

    def count(self) -> tuple[int, int]:
        n = self._val(self._cypher("MATCH (n:Node) RETURN count(n)")[0][0])
        e = self._val(self._cypher("MATCH ()-[e]->() RETURN count(e)")[0][0])
        return n, e
