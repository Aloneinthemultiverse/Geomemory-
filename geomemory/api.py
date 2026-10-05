"""FastAPI service (spec §24: Python + FastAPI).

    python -m geomemory.api                         # synthetic demo world, in memory
    python -m geomemory.api --backend postgis       # serve what is stored in PostGIS
    python -m geomemory.api --dataset uber          # 1.8M real pickups, in memory

Same paths as the stdlib server, so the web UI works unchanged, plus typed
request models generated from the tool schemas and OpenAPI docs at /docs.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import ConfigDict, create_model

from .agent import TOOLS, AgentInterface, showcase_call

_TYPES = {"number": float, "integer": int, "string": str}
UI = Path(__file__).parent / "ui" / "index.html"


def _model(tool: dict):
    props = tool["parameters"]["properties"]
    req = set(tool["parameters"].get("required", ()))
    fields = {}
    for name, spec in props.items():
        t = _TYPES.get(spec.get("type"), Any)
        # Types are checked here; ranges and semantics by AgentInterface.
        fields[name] = (t, ...) if name in req else (Optional[t], None)
    return create_model(f"{tool['name'].title().replace('_', '')}Args",
                        __config__=ConfigDict(extra="forbid"), **fields)


def create_app(agent: AgentInterface, results_dir: str | None = None,
               max_body: int = 1 << 20) -> FastAPI:
    app = FastAPI(title="GeoMemory", version="1.0",
                  description="Spatial-temporal memory and provenance engine for AI agents. "
                              "Every answer carries the observation ids, sources and "
                              "confidence that support it.")
    overview_cache: dict = {}
    cache: dict = {}

    async def _body(request: Request):
        try:
            return json.loads(await request.body() or b"{}")
        except ValueError:
            return None

    def _json(res: dict):
        return JSONResponse(res, status_code=200 if res.get("ok") else 400)

    @app.middleware("http")
    async def limit_body(request: Request, call_next):
        try:
            n = int(request.headers.get("content-length", "0"))
        except ValueError:
            return JSONResponse({"ok": False, "error": "bad Content-Length"}, 400)
        if n > max_body:
            return JSONResponse({"ok": False, "error": "body too large"}, 413)
        return await call_next(request)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index():
        return UI.read_text()

    @app.get("/tools", summary="Tool schemas (for LLM function calling)")
    def tools():
        return {"tools": TOOLS}

    @app.get("/api/overview", summary="Dataset summary and a map sample")
    def overview():
        if "v" not in overview_cache:
            overview_cache["v"] = agent.overview()
        return overview_cache["v"]

    @app.get("/api/replay", summary="NYC time-lapse: pickups per cell and hour of the week")
    def replay_data():
        from .showcase import REPLAY_FILE
        return Response(REPLAY_FILE.read_bytes(), media_type="application/json",
                        headers={"Cache-Control": "public, max-age=86400"})

    @app.post("/api/ask", summary="Answer a plain-English question (LLM if ANTHROPIC_API_KEY is set)")
    async def ask_route(request: Request):
        return _json(showcase_call(agent, "/api/ask", await _body(request), cache))

    @app.post("/api/chat", summary="Chat with the GeoMemory agent (Claude when ANTHROPIC_API_KEY is set)")
    async def chat_route(request: Request):
        return _json(showcase_call(agent, "/api/chat", await _body(request), cache))

    @app.post("/api/crash", summary="Crash test: kill one of 4 servers mid-question")
    async def crash_route(request: Request):
        return _json(showcase_call(agent, "/api/crash", await _body(request), cache))

    @app.get("/api/results", summary="Benchmark results")
    def results():
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
        return {"ok": True, "runs": runs}

    def add_tool(tool: dict):
        Model = _model(tool)
        name = tool["name"]

        def endpoint(body):
            res = agent.dispatch(name, body.model_dump(exclude_none=True))
            return JSONResponse(res, status_code=200 if res.get("ok") else 400)

        # The model is built at runtime, so attach it as a real annotation
        # (string annotations from `from __future__` cannot be resolved here).
        endpoint.__annotations__ = {"body": Model}

        app.post(f"/tools/{name}", summary=tool["description"], name=name)(endpoint)

    for t in TOOLS:
        add_tool(t)
    return app


def main(argv=None) -> None:
    import uvicorn
    ap = argparse.ArgumentParser(description="GeoMemory FastAPI service")
    ap.add_argument("--backend", choices=["memory", "postgis"], default="memory")
    ap.add_argument("--dataset", choices=["showcase", "synthetic", "uber"], default="showcase")
    ap.add_argument("--months", default="jul14,sep14")
    ap.add_argument("--table", default="observations", help="PostGIS table")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--results", default="results")
    a = ap.parse_args(argv)
    if a.backend == "postgis":
        from .backends.postgis import PostGISStore
        mem = PostGISStore(table=a.table)
    else:
        from .showcase import build_memory
        mem = build_memory(a.dataset, a.months)
    app = create_app(AgentInterface(mem, a.dataset), a.results)
    print(f"GeoMemory API on http://{a.host}:{a.port}  (docs at /docs)")
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    main()
