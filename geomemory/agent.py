"""Agent interface (spec §13, §18.12, Phase 5).

Agents call a small set of typed tools rather than touching storage. Every
tool returns JSON-serializable evidence (observation ids, sources,
confidence) and never raises on bad input: it returns {"ok": false, ...}.
`serve()` exposes the same tools over HTTP using only the standard library.
"""
from __future__ import annotations

import json
import math
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Iterable

from .model import Observation, Point, haversine_m
from . import trust

MAX_LIMIT = 1000


class ToolError(ValueError):
    pass


# --- argument parsing --------------------------------------------------------

def _num(args, key, lo=None, hi=None, default=None):
    v = args.get(key, default)
    if v is None:
        raise ToolError(f"missing required argument: {key}")
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise ToolError(f"{key} must be a finite number")
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        raise ToolError(f"{key} must be in [{lo}, {hi}]")
    return float(v)


def _str(args, key, default=None, required=True):
    v = args.get(key, default)
    if v is None:
        if required:
            raise ToolError(f"missing required argument: {key}")
        return None
    if not isinstance(v, str) or not v:
        raise ToolError(f"{key} must be a non-empty string")
    return v


def _time(args, key, default=None):
    v = args.get(key, default)
    if v is None:
        raise ToolError(f"missing required argument: {key}")
    if isinstance(v, datetime):
        t = v
    elif isinstance(v, str):
        try:
            t = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ToolError(f"{key} must be an ISO-8601 timestamp") from None
    else:
        raise ToolError(f"{key} must be an ISO-8601 timestamp")
    if t.tzinfo is None:
        raise ToolError(f"{key} must include a timezone")
    return t


def _point(args) -> Point:
    return Point(_num(args, "lat", -90, 90), _num(args, "lon", -180, 180))


def _window(args) -> tuple[datetime, datetime]:
    start, end = _time(args, "start"), _time(args, "end")
    if end < start:
        raise ToolError("end must be >= start")
    return start, end


def _limit(args) -> int:
    return int(_num(args, "limit", 1, MAX_LIMIT, default=100))


def _obs_json(o: Observation) -> dict:
    return {"observation_id": o.observation_id, "entity_id": o.entity_id,
            "event_type": o.event_type, "lat": o.location.lat, "lon": o.location.lon,
            "uncertainty_m": o.spatial_uncertainty_m, "timestamp": o.timestamp.isoformat(),
            "source_id": o.source_id, "processing_model": o.provenance.processing_model,
            "confidence": o.confidence, "parent_observation": o.parent_observation}


def _bundle(obs: list[Observation], limit: int, **extra) -> dict:
    obs = sorted(obs, key=lambda o: (o.timestamp, o.observation_id))
    return {"ok": True, **extra, "count": len(obs), "truncated": len(obs) > limit,
            "sources": sorted({o.source_id for o in obs}),
            "observations": [_obs_json(o) for o in obs[:limit]]}


# --- tools ---------------------------------------------------------------------

TOOLS: list[dict] = [
    {"name": "events_near",
     "description": "Observations within radius_m of a point between start and end.",
     "parameters": {"type": "object", "required": ["lat", "lon", "radius_m", "start", "end"],
                    "properties": {"lat": {"type": "number"}, "lon": {"type": "number"},
                                   "radius_m": {"type": "number"},
                                   "start": {"type": "string", "format": "date-time"},
                                   "end": {"type": "string", "format": "date-time"},
                                   "event_type": {"type": "string"},
                                   "source_id": {"type": "string"},
                                   "min_confidence": {"type": "number"},
                                   "limit": {"type": "integer"}}}},
    {"name": "what_changed",
     "description": "What appeared or changed around a point between start and end.",
     "parameters": {"type": "object", "required": ["lat", "lon", "radius_m", "start", "end"],
                    "properties": {"lat": {"type": "number"}, "lon": {"type": "number"},
                                   "radius_m": {"type": "number"},
                                   "start": {"type": "string"}, "end": {"type": "string"}}}},
    {"name": "history_before",
     "description": "What happened to an entity in the lookback window before an event "
                    "(its latest occurrence of event_type, or the time `at`).",
     "parameters": {"type": "object", "required": ["entity_id", "lookback_minutes"],
                    "properties": {"entity_id": {"type": "string"},
                                   "event_type": {"type": "string"},
                                   "at": {"type": "string"},
                                   "lookback_minutes": {"type": "number"}}}},
    {"name": "nearby_entities",
     "description": "Other entities observed within radius_m of an entity around time `at`.",
     "parameters": {"type": "object",
                    "required": ["entity_id", "at", "radius_m", "window_minutes"],
                    "properties": {"entity_id": {"type": "string"}, "at": {"type": "string"},
                                   "radius_m": {"type": "number"},
                                   "window_minutes": {"type": "number"}}}},
    {"name": "evidence",
     "description": "An observation with its full provenance lineage.",
     "parameters": {"type": "object", "required": ["observation_id"],
                    "properties": {"observation_id": {"type": "string"}}}},
    {"name": "fused_location",
     "description": "Best location estimate for an entity from all sources in a window, "
                    "with uncertainty and conflicting (outlier) observations.",
     "parameters": {"type": "object", "required": ["entity_id", "start", "end"],
                    "properties": {"entity_id": {"type": "string"},
                                   "start": {"type": "string"}, "end": {"type": "string"}}}},
]


class AgentInterface:
    def __init__(self, mem) -> None:
        """`mem` is a GeoMemory or a Cluster."""
        self.mem = mem
        self._tools: dict[str, Callable[[dict], dict]] = {
            "events_near": self.events_near, "what_changed": self.what_changed,
            "history_before": self.history_before, "nearby_entities": self.nearby_entities,
            "evidence": self.evidence, "fused_location": self.fused_location,
        }

    def dispatch(self, name: Any, args: Any) -> dict:
        if not isinstance(name, str) or name not in self._tools:
            return {"ok": False, "error": f"unknown tool: {name!r}"}
        if not isinstance(args, dict):
            return {"ok": False, "error": "arguments must be an object"}
        try:
            return self._tools[name](args)
        except (ToolError, ValueError) as e:
            return {"ok": False, "error": str(e)}
        except KeyError as e:
            return {"ok": False, "error": f"not found: {e.args[0]}"}
        except RuntimeError as e:  # e.g. PartitionUnavailable
            return {"ok": False, "error": f"unavailable: {e}"}

    def events_near(self, args: dict) -> dict:
        c, r = _point(args), _num(args, "radius_m", 0, 2.1e7)
        start, end = _window(args)
        etype = _str(args, "event_type", required=False)
        source = _str(args, "source_id", required=False)
        minc = _num(args, "min_confidence", 0, 1, default=0.0)
        hits = [o for o in self.mem.radius_between(c, r, start, end)
                if (etype is None or o.event_type == etype)
                and (source is None or o.source_id == source) and o.confidence >= minc]
        return _bundle(hits, _limit(args))

    def what_changed(self, args: dict) -> dict:
        c, r = _point(args), _num(args, "radius_m", 0, 2.1e7)
        start, end = _window(args)
        d = trust.diff(self.mem, c, r, start, end)
        events = self.mem.radius_between(c, r, start, end)
        # Only events strictly after start are "changes" in the window.
        events = [o for o in events if o.timestamp > start]
        return _bundle(events, _limit(args), appeared=d["appeared"], changed=d["changed"])

    def history_before(self, args: dict) -> dict:
        entity = _str(args, "entity_id")
        lookback = timedelta(minutes=_num(args, "lookback_minutes", 0, 1e8))
        hist = self.mem.entity_history(entity)
        if not hist:
            raise KeyError(entity)
        etype = _str(args, "event_type", required=False)
        if etype is not None:
            anchors = [o for o in hist if o.event_type == etype]
            if not anchors:
                return {"ok": False, "error": f"{entity} has no {etype!r} event"}
            anchor, at = anchors[-1], anchors[-1].timestamp
        else:
            anchor, at = None, _time(args, "at")
        prior = [o for o in hist if at - lookback <= o.timestamp < at]
        out = _bundle(prior, _limit(args), entity_id=entity, at=at.isoformat())
        out["anchor"] = _obs_json(anchor) if anchor else None
        out["timeline"] = [{"offset_minutes": (o.timestamp - at).total_seconds() / 60,
                            "event_type": o.event_type, "observation_id": o.observation_id,
                            "source_id": o.source_id, "confidence": o.confidence}
                           for o in sorted(prior, key=lambda o: (o.timestamp, o.observation_id))]
        return out

    def nearby_entities(self, args: dict) -> dict:
        entity = _str(args, "entity_id")
        at = _time(args, "at")
        r = _num(args, "radius_m", 0, 2.1e7)
        w = timedelta(minutes=_num(args, "window_minutes", 0, 1e8))
        mine = [o for o in self.mem.entity_history(entity) if at - w <= o.timestamp <= at + w]
        if not mine:
            return {"ok": False, "error": f"no observation of {entity} near that time"}
        found: dict[str, dict] = {}
        for o in mine:
            for other in self.mem.radius_between(o.location, r, o.timestamp - w, o.timestamp + w):
                if other.entity_id == entity:
                    continue
                d = haversine_m(o.location, other.location)
                cur = found.get(other.entity_id)
                if cur is None or d < cur["distance_m"]:
                    found[other.entity_id] = {"entity_id": other.entity_id, "distance_m": d,
                                              "evidence": [o.observation_id,
                                                           other.observation_id]}
        return {"ok": True, "entity_id": entity,
                "entities": sorted(found.values(), key=lambda x: (x["distance_m"], x["entity_id"]))}

    def overview(self, limit: int = 5000) -> dict:
        """Dataset summary plus a sample for the UI's initial map."""
        far_past = datetime(1, 1, 2, tzinfo=timezone.utc)
        far_future = datetime(9999, 12, 30, tzinfo=timezone.utc)
        obs = self.mem.between(far_past, far_future)
        if not obs:
            return {"ok": True, "count": 0, "observations": [], "event_types": [],
                    "sources": [], "entities": 0}
        step = max(1, len(obs) // limit)
        return {"ok": True, "count": len(obs), "entities": len({o.entity_id for o in obs}),
                "sources": sorted({o.source_id for o in obs}),
                "event_types": sorted({o.event_type for o in obs}),
                "start": obs[0].timestamp.isoformat(), "end": obs[-1].timestamp.isoformat(),
                "sampled": step > 1,
                "observations": [_obs_json(o) for o in obs[::step][:limit]]}

    def evidence(self, args: dict) -> dict:
        oid = _str(args, "observation_id")
        root = self.mem.get(oid)
        chain, seen, cur = [], set(), root
        while cur is not None and cur.observation_id not in seen:
            seen.add(cur.observation_id)
            chain.append(cur)
            try:
                cur = self.mem.get(cur.parent_observation) if cur.parent_observation else None
            except KeyError:
                cur = None
        return {"ok": True, "observation": _obs_json(root),
                "lineage": [_obs_json(o) for o in chain],
                "inputs": list(root.provenance.inputs)}

    def fused_location(self, args: dict) -> dict:
        entity = _str(args, "entity_id")
        start, end = _window(args)
        obs = [o for o in self.mem.entity_history(entity) if start <= o.timestamp <= end]
        if not obs:
            return {"ok": False, "error": f"no observations of {entity} in window"}
        est = trust.fuse_locations(obs)
        return {"ok": True, "entity_id": entity, **est.as_dict(),
                "sources": sorted({o.source_id for o in obs})}


# --- retrieval evaluation (§18.12, E6) -----------------------------------------

def precision_recall(retrieved: Iterable[str], relevant: Iterable[str]) -> tuple[float, float]:
    got, want = set(retrieved), set(relevant)
    p = len(got & want) / len(got) if got else (1.0 if not want else 0.0)
    r = len(got & want) / len(want) if want else 1.0
    return p, r


def keyword_baseline(observations: Iterable[Observation], query_terms: Iterable[str],
                     limit: int) -> list[str]:
    """Conventional text retrieval: match terms against entity/event/source,
    ignoring where and when. The comparison point for experiment E6."""
    terms = {t.lower() for t in query_terms}
    hits = [o for o in observations
            if terms & {o.entity_id.lower(), o.event_type.lower(), o.source_id.lower()}]
    return [o.observation_id for o in hits[:limit]]


# --- HTTP ------------------------------------------------------------------------

def serve(agent: AgentInterface, host: str = "127.0.0.1", port: int = 0,
          max_body: int = 1 << 20, results_dir: str | None = None,
          ) -> tuple[ThreadingHTTPServer, threading.Thread]:
    """GET / serves the web UI, GET /api/overview a dataset summary,
    GET /api/results the latest benchmark results, GET /tools the tool
    schemas; POST /tools/<name> with a JSON body calls one tool."""
    from pathlib import Path
    ui_html = (Path(__file__).parent / "ui" / "index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # keep test output quiet
            pass

        def _send(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(ui_html)))
                self.end_headers()
                self.wfile.write(ui_html)
            elif path == "/tools":
                self._send(200, {"tools": TOOLS})
            elif path == "/api/overview":
                self._send(200, agent.overview())
            elif path == "/api/results":
                runs = {}
                if results_dir:
                    for f in sorted(Path(results_dir).glob("**/results.json")):
                        try:
                            data = json.loads(f.read_text())
                        except (OSError, ValueError):
                            continue
                        ds = data.get("dataset", "synthetic")
                        key = data.get("scale", f.parent.name)
                        runs[key if ds == "synthetic" else f"{key} · {ds}"] = data
                self._send(200, {"ok": True, "runs": runs})
            else:
                self._send(404, {"ok": False, "error": "not found"})

        def do_POST(self):
            if not self.path.startswith("/tools/"):
                return self._send(404, {"ok": False, "error": "not found"})
            try:
                n = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                return self._send(400, {"ok": False, "error": "bad Content-Length"})
            if n < 0 or n > max_body:
                if 0 < n <= 16 * max_body:  # drain so the client sees the 413, not a reset
                    self.rfile.read(n)
                self.close_connection = True
                return self._send(413, {"ok": False, "error": "body too large"})
            try:
                args = json.loads(self.rfile.read(n) or b"{}")
            except (ValueError, UnicodeDecodeError):
                return self._send(400, {"ok": False, "error": "body must be JSON"})
            res = agent.dispatch(self.path[len("/tools/"):], args)
            self._send(200 if res.get("ok") else 400, res)

    class Server(ThreadingHTTPServer):
        request_queue_size = 256  # stdlib default of 5 drops bursts of agent calls
        daemon_threads = True

    srv = Server((host, port), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, t
