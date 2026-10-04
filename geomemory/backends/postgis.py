"""PostGIS storage backend (spec §24: PostgreSQL + PostGIS).

`PostGISStore` has the same query surface as `GeoMemory`, so the agent
interface, the UI, `trust` functions and the benchmark run on it unchanged.

Semantics are matched to the in-memory engine:
- distances are great-circle on a sphere (ST_DWithin/ST_Distance with
  use_spheroid = false; PostGIS's sphere radius equals our 6,371,008.8 m),
- polygon containment is planar in lon/lat, like `_point_in_polygon`,
- duplicate observation ids are ignored (idempotent ingest).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Iterable

from ..model import Observation, Point, Provenance
from . import DEFAULT_DSN

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS {t} (
    observation_id     text PRIMARY KEY,
    entity_id          text NOT NULL,
    event_type         text NOT NULL,
    geog               geography(Point, 4326) NOT NULL,
    ts                 timestamptz NOT NULL,
    duration_s         double precision NOT NULL DEFAULT 0,
    source_id          text NOT NULL,
    processing_model   text,
    inputs             text[] NOT NULL DEFAULT '{{}}',
    ingested_at        timestamptz NOT NULL,
    confidence         double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    uncertainty_m      double precision NOT NULL CHECK (uncertainty_m >= 0),
    attributes         jsonb NOT NULL DEFAULT '{{}}',
    parent_observation text
);
CREATE INDEX IF NOT EXISTS {t}_geog_gist ON {t} USING gist (geog);
CREATE INDEX IF NOT EXISTS {t}_ts ON {t} (ts);
CREATE INDEX IF NOT EXISTS {t}_entity_ts ON {t} (entity_id, ts);
CREATE INDEX IF NOT EXISTS {t}_source ON {t} (source_id);
"""

_COLS = ("observation_id, entity_id, event_type, ST_Y(geog::geometry), ST_X(geog::geometry), ts, "
         "duration_s, source_id, processing_model, inputs, ingested_at, confidence, "
         "uncertainty_m, attributes, parent_observation")
_PT = "ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography"


def _row_to_obs(r) -> Observation:
    (oid, ent, ev, lat, lon, ts, dur, src, model, inputs, ing, conf, unc, attrs, parent) = r
    return Observation(
        entity_id=ent, event_type=ev, location=Point(lat, lon),
        timestamp=ts.astimezone(timezone.utc),
        provenance=Provenance(src, model, tuple(inputs or ()), ing.astimezone(timezone.utc)),
        confidence=float(conf), spatial_uncertainty_m=float(unc),
        duration=timedelta(seconds=dur), attributes=attrs or {},
        parent_observation=parent, observation_id=oid)


class PostGISStore:
    def __init__(self, dsn: str = DEFAULT_DSN, table: str = "observations",
                 reset: bool = False) -> None:
        import psycopg
        if not table.isidentifier():
            raise ValueError("table must be a plain identifier")
        self.table = table
        self.conn = psycopg.connect(dsn, autocommit=True)
        with self.conn.cursor() as cur:
            if reset:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(SCHEMA.format(t=table))

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _q(self, sql: str, params=None) -> list[Observation]:
        with self.conn.cursor() as cur:
            cur.execute(sql.format(t=self.table, cols=_COLS, pt=_PT), params or {})
            return [_row_to_obs(r) for r in cur.fetchall()]

    def __len__(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.table}")
            return cur.fetchone()[0]

    # --- ingest -------------------------------------------------------------

    @staticmethod
    def _values(o: Observation) -> tuple:
        return (o.observation_id, o.entity_id, o.event_type,
                f"SRID=4326;POINT({o.location.lon!r} {o.location.lat!r})", o.timestamp,
                o.duration.total_seconds(), o.source_id, o.provenance.processing_model,
                list(o.provenance.inputs), o.provenance.ingested_at, o.confidence,
                o.spatial_uncertainty_m, json.dumps(dict(o.attributes)), o.parent_observation)

    def ingest(self, obs: Observation) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {self.table} VALUES
                            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (observation_id) DO NOTHING""", self._values(obs))
            return cur.rowcount == 1

    def ingest_many(self, observations: Iterable[Observation], batch: int = 50_000) -> int:
        """Bulk load with COPY into a staging table, then one idempotent insert."""
        new = 0
        buf: list[Observation] = []

        def flush():
            nonlocal new
            if not buf:
                return
            with self.conn.transaction(), self.conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS _stage (LIKE {self.table}) "
                            "ON COMMIT DELETE ROWS")
                with cur.copy("COPY _stage FROM STDIN") as cp:
                    for o in buf:
                        cp.write_row(self._values(o))
                cur.execute(f"INSERT INTO {self.table} SELECT DISTINCT ON (observation_id) * "
                            f"FROM _stage ON CONFLICT (observation_id) DO NOTHING")
                new += cur.rowcount
            buf.clear()

        for o in observations:
            buf.append(o)
            if len(buf) >= batch:
                flush()
        flush()
        with self.conn.cursor() as cur:
            cur.execute(f"ANALYZE {self.table}")
        return new

    # --- queries --------------------------------------------------------------

    def get(self, observation_id: str) -> Observation:
        rows = self._q("SELECT {cols} FROM {t} WHERE observation_id = %(id)s",
                       {"id": observation_id})
        if not rows:
            raise KeyError(observation_id)
        return rows[0]

    def entity_ids(self) -> list[str]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT entity_id FROM {self.table} ORDER BY 1")
            return [r[0] for r in cur.fetchall()]

    def radius(self, center: Point, radius_m: float) -> list[Observation]:
        if radius_m < 0:
            raise ValueError("radius_m must be >= 0")
        return self._q("SELECT {cols} FROM {t} WHERE ST_DWithin(geog, {pt}, %(r)s, false)",
                       {"lat": center.lat, "lon": center.lon, "r": radius_m})

    def radius_between(self, center: Point, radius_m: float, start: datetime,
                       end: datetime) -> list[Observation]:
        if radius_m < 0:
            raise ValueError("radius_m must be >= 0")
        return self._q("SELECT {cols} FROM {t} WHERE ST_DWithin(geog, {pt}, %(r)s, false) "
                       "AND ts BETWEEN %(a)s AND %(b)s ORDER BY ts, observation_id",
                       {"lat": center.lat, "lon": center.lon, "r": radius_m,
                        "a": start, "b": end})

    def nearest(self, center: Point, k: int = 10) -> list[Observation]:
        if k <= 0:
            return []
        # KNN index scan for candidates, then exact sphere distance + id tie-break.
        return self._q(
            "SELECT {cols} FROM (SELECT * FROM {t} ORDER BY geog <-> {pt} LIMIT %(c)s) s "
            "ORDER BY ST_Distance(geog, {pt}, false), observation_id LIMIT %(k)s",
            {"lat": center.lat, "lon": center.lon, "k": k, "c": 2 * k + 32})

    def within_polygon(self, ring: list[Point]) -> list[Observation]:
        if len(ring) < 3:
            raise ValueError("polygon needs at least 3 vertices")
        pts = ring + [ring[0]]
        wkt = "POLYGON((" + ",".join(f"{p.lon!r} {p.lat!r}" for p in pts) + "))"
        return self._q("SELECT {cols} FROM {t} "
                       "WHERE ST_Contains(ST_GeomFromText(%(w)s, 4326), geog::geometry)",
                       {"w": wkt})

    def between(self, start: datetime, end: datetime) -> list[Observation]:
        if end < start:
            return []
        return self._q("SELECT {cols} FROM {t} WHERE ts BETWEEN %(a)s AND %(b)s "
                       "ORDER BY ts, observation_id", {"a": start, "b": end})

    def entity_history(self, entity_id: str) -> list[Observation]:
        return self._q("SELECT {cols} FROM {t} WHERE entity_id = %(e)s "
                       "ORDER BY ts, observation_id", {"e": entity_id})

    def latest(self, entity_id: str) -> Observation | None:
        h = self.entity_history(entity_id)
        return h[-1] if h else None

    def by_source(self, source_id: str) -> list[Observation]:
        return self._q("SELECT {cols} FROM {t} WHERE source_id = %(s)s", {"s": source_id})

    def lineage(self, observation_id: str) -> list[Observation]:
        """Parent chain via a recursive CTE (cycle-safe)."""
        return self._q("""
            WITH RECURSIVE chain(id, depth, path) AS (
                SELECT %(id)s::text, 0, ARRAY[%(id)s::text]
                UNION ALL
                SELECT o.parent_observation, c.depth + 1, c.path || o.parent_observation
                FROM chain c JOIN {t} o ON o.observation_id = c.id
                WHERE o.parent_observation IS NOT NULL
                  AND NOT o.parent_observation = ANY(c.path))
            SELECT {cols} FROM chain c JOIN {t} ON observation_id = c.id
            ORDER BY c.depth""", {"id": observation_id})
