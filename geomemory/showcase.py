"""The showcase: what the web UI demonstrates beyond single queries.

* A committed NYC data pack, so the demo works anywhere (laptop, Vercel)
  without the 200 MB download:
    - ``nyc_week.txt.xz``: a 20% sample of real Uber pickups during
      Independence Day week 2014 (Jul 1-7), with their original record ids.
    - ``nyc_replay.json``: all 4.5M pickups (Apr-Sep 2014) counted per
      ~650 m cell and hour of the week, for the time-lapse replay.
  Rebuild both from the raw data with ``python -m geomemory.showcase build``.
* ``crash_test``: run a question on a 4-server cluster, destroy one server,
  ask again, and compare the answers.
* ``ask``: answer a question typed in plain English, with an LLM when
  ``ANTHROPIC_API_KEY`` is set and with a small rule-based reader otherwise.
"""
from __future__ import annotations

import argparse
import csv
import json
import lzma
import os
import random
import re
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

from .model import Observation, Point, Provenance

PACK_DIR = Path(__file__).parent / "data"
WEEK_FILE = PACK_DIR / "nyc_week.txt.xz"
REPLAY_FILE = PACK_DIR / "nyc_replay.json"
_NYC = ZoneInfo("America/New_York")
_BASES = ("B02512", "B02598", "B02617", "B02682", "B02764")
_WEEK0 = datetime(2014, 7, 1)  # local time
_LAT0, _LON0 = 40.0, -75.0
CELL_LAT, CELL_LON = 0.006, 0.008  # ~650 m x ~670 m in NYC


# --- data pack ------------------------------------------------------------------

def build_pack(raw_dir: Path, out_dir: Path = PACK_DIR, frac: float = 0.2, seed: int = 7,
               log=print) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    build_week(raw_dir / "uber-raw-data-jul14.csv", out_dir / WEEK_FILE.name, frac, seed, log)

    # Replay grid: every pickup, counted per cell and local hour of the week.
    grid: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0] * 168)
    total = 0
    weeks = set()
    for m in ("apr14", "may14", "jun14", "jul14", "aug14", "sep14"):
        with open(raw_dir / f"uber-raw-data-{m}.csv", newline="") as f:
            for r in csv.DictReader(f):
                d = datetime.strptime(r["Date/Time"], "%m/%d/%Y %H:%M:%S")
                la, lo = float(r["Lat"]), float(r["Lon"])
                grid[(int((la - _LAT0) // CELL_LAT), int((lo - _LON0) // CELL_LON))][
                    d.weekday() * 24 + d.hour] += 1
                weeks.add(d.isocalendar()[:2])
                total += 1
        log(f"  counted {m}")
    n_weeks = len(weeks)
    cells = sorted(((k, v) for k, v in grid.items() if sum(v) >= 100),
                   key=lambda kv: -sum(kv[1]))
    out = {"source": "NYC TLC Uber pickups, Apr-Sep 2014 (FiveThirtyEight FOIL release)",
           "pickups": total, "weeks": n_weeks, "cell_m": 650,
           "note": "values: average pickups per cell in that hour of a typical week (x10)",
           "hours": "168 local hours, Monday 00:00 first",
           "cells": [[round(_LAT0 + (a + .5) * CELL_LAT, 4), round(_LON0 + (b + .5) * CELL_LON, 4)]
                     for (a, b), _ in cells],
           "values": [[round(c * 10 / n_weeks) for c in v] for _, v in cells],
           "citywide": [round(sum(v[h] for v in grid.values()) / n_weeks) for h in range(168)]}
    REPLAY = out_dir / REPLAY_FILE.name
    REPLAY.write_text(json.dumps(out, separators=(",", ":")))
    log(f"{REPLAY.name}: {len(cells):,} cells, {REPLAY.stat().st_size:,} bytes")


def build_week(raw_csv: Path, out: Path, frac: float = 0.2, seed: int = 7, log=print) -> None:
    """Sample July 1-7 2014 from the raw July CSV: "minute,row,lat,lon,base"
    lines, delta-coded so xz packs them. Deterministic for a given seed."""
    rng = random.Random(seed)
    days = tuple(f'"7/{d}/2014 ' for d in range(1, 8))
    rows = []
    with open(raw_csv, newline="") as f:
        next(f)
        for i, line in enumerate(f):
            if not line.startswith(days) or rng.random() >= frac:
                continue
            ts, lat, lon, base = next(csv.reader([line]))
            d = datetime.strptime(ts, "%m/%d/%Y %H:%M:%S")
            rows.append((int((d - _WEEK0).total_seconds() // 60), i,
                         round((float(lat) - _LAT0) * 1e4), round((float(lon) - _LON0) * 1e4),
                         _BASES.index(base)))
    rows.sort()
    lines, pm, pi = [], 0, 0
    for m, i, la, lo, b in rows:
        lines.append(f"{m - pm},{i - pi},{la},{lo},{b}")
        pm, pi = m, i
    out.write_bytes(lzma.compress("\n".join(lines).encode(), preset=9 | lzma.PRESET_EXTREME))
    log(f"{out.name}: {len(rows):,} pickups, {out.stat().st_size:,} bytes")


def fetch_week(out: Path = WEEK_FILE, log=print) -> None:
    """Rebuild the week sample from the public July 2014 CSV (37 MB). Used
    where the pack is not shipped, e.g. the size-limited Vercel bundle."""
    import tempfile
    from .datasets import SOURCES
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as d:
        raw = Path(d) / "jul14.csv"
        log("downloading the July 2014 Uber pickups ...")
        urllib.request.urlretrieve(SOURCES["uber-raw-data-jul14.csv"], raw)
        build_week(raw, out, log=log)


def load_week(path: Path = WEEK_FILE) -> Iterator[Observation]:
    """The committed Independence Day week sample, as observations with the
    same ids as the full dataset (``uber_jul14_<row>``)."""
    from .datasets import UBER_BASES
    if not path.exists():
        fetch_week(path)
    m = i = 0
    for line in lzma.decompress(path.read_bytes()).decode().splitlines():
        dm, di, la, lo, b = map(int, line.split(","))
        m, i = m + dm, i + di
        local = _WEEK0 + timedelta(minutes=m)
        base = _BASES[b]
        yield Observation(
            entity_id=f"{base}_trip_jul14_{i}", event_type="pickup",
            location=Point(round(_LAT0 + la / 1e4, 4), round(_LON0 + lo / 1e4, 4)),
            timestamp=local.replace(tzinfo=_NYC).astimezone(timezone.utc),
            provenance=Provenance(base, "TLC FOIL 2014"), spatial_uncertainty_m=11.0,
            observation_id=f"uber_jul14_{i}", attributes={"base_name": UBER_BASES[base]})


def build_memory(dataset: str = "showcase", months: str = "jul14,sep14"):
    """showcase: the demo world plus the committed NYC week (the default);
    synthetic: the demo world; uber: real pickups from data/raw."""
    from .demo import build_world
    from .index import QuadTreeIndex
    from .store import GeoMemory
    gm = GeoMemory(QuadTreeIndex())
    if dataset == "uber":
        from .datasets import load_uber
        ms = tuple(m.strip() for m in months.split(",") if m.strip())
        print(f"loading Uber pickups for {', '.join(ms)} ...", flush=True)
        gm.ingest_many(load_uber(months=ms))
    else:
        gm.ingest_many(build_world())
        if dataset == "showcase":
            gm.ingest_many(load_week())
    return gm


def replay() -> dict:
    return json.loads(REPLAY_FILE.read_text())


# --- crash test -----------------------------------------------------------------

TIMES_SQUARE = (40.7580, -73.9855)


class CrashTest:
    """A 4-server cluster holding the NYC pickups, every partition on 2 servers.

    ``run`` asks a question, destroys one server (its data is wiped), asks
    again, then brings the server back and re-syncs it from its peers. It also
    reports what a cluster *without* copies would have lost."""

    def __init__(self, mem, n_nodes: int = 4, n_partitions: int = 8) -> None:
        from .cluster import Cluster
        from .index import QuadTreeIndex
        from .partition import KDPartitioner
        far = (datetime(1, 1, 2, tzinfo=timezone.utc), datetime(9999, 1, 1, tzinfo=timezone.utc))
        obs = [o for o in mem.between(*far) if o.event_type == "pickup"]
        self.n = len(obs)
        part = KDPartitioner(n_partitions, [o.location for o in obs[::20]])
        self.cluster = Cluster(part, n_nodes, replication=2, index_factory=QuadTreeIndex)
        self.cluster.ingest_many(obs)

    def run(self, node: int | None = None, lat: float = TIMES_SQUARE[0],
            lon: float = TIMES_SQUARE[1], radius_m: float = 1500.0) -> dict:
        cl = self.cluster
        n_nodes = len(cl.nodes)
        node = random.randrange(n_nodes) if node is None else node
        if not 0 <= node < n_nodes:
            raise ValueError("no such server")
        c = Point(lat, lon)
        servers = [{"id": n.node_id, "records": n.record_count(),
                    "partitions": sorted(n.partitions)} for n in cl.nodes]
        t0 = time.perf_counter()
        before = cl.radius(c, radius_m)
        t1 = time.perf_counter()
        cl.fail(node)
        try:
            t2 = time.perf_counter()
            after = {o.observation_id for o in cl.radius(c, radius_m)}
            t3 = time.perf_counter()
        finally:
            t4 = time.perf_counter()
            copied = cl.recover(node)
            t5 = time.perf_counter()
        # Without copies, partition p lives only on server p % n_nodes.
        lost = sum(1 for o in before
                   if cl.partitioner.partition_of(o.location) % n_nodes == node)
        ids = {o.observation_id for o in before}
        return {"ok": True, "records": self.n, "servers": servers, "killed": node,
                "question": {"lat": lat, "lon": lon, "radius_m": radius_m},
                "before": {"count": len(ids), "ms": (t1 - t0) * 1e3},
                "after": {"count": len(after), "ms": (t3 - t2) * 1e3, "identical": ids == after},
                "recovered": {"records": copied, "ms": (t5 - t4) * 1e3},
                "without_copies": {"lost": lost, "share": lost / len(ids) if ids else 0.0}}


# --- plain-English questions ----------------------------------------------------

PLACES = {
    "times square": (40.7580, -73.9855, 400), "jfk": (40.6446, -73.7797, 2500),
    "laguardia": (40.7769, -73.8740, 1500), "la guardia": (40.7769, -73.8740, 1500),
    "brooklyn bridge": (40.7061, -73.9969, 800), "fireworks": (40.7061, -73.9969, 1500),
    "central park": (40.7812, -73.9665, 1500), "wall street": (40.7060, -74.0088, 500),
    "grand central": (40.7527, -73.9772, 400), "penn station": (40.7506, -73.9935, 400),
    "williamsburg": (40.7081, -73.9571, 1200), "soho": (40.7233, -74.0030, 600),
    "east village": (40.7265, -73.9815, 700), "west village": (40.7358, -74.0036, 700),
    "chelsea": (40.7465, -74.0014, 700), "harlem": (40.8116, -73.9465, 1500),
    "midtown": (40.7549, -73.9840, 1500), "lower east side": (40.7150, -73.9843, 700),
    "upper east side": (40.7736, -73.9566, 1200), "upper west side": (40.7870, -73.9754, 1200),
    "meatpacking": (40.7406, -74.0080, 400), "empire state": (40.7484, -73.9857, 400),
    "madison square garden": (40.7505, -73.9934, 400), "yankee stadium": (40.8296, -73.9262, 600),
    "citi field": (40.7571, -73.8458, 600), "barclays": (40.6826, -73.9754, 500),
    "manhattan": (40.7580, -73.9855, 7000), "brooklyn": (40.6782, -73.9442, 6000),
    "queens": (40.7282, -73.7949, 9000), "new york": (40.7300, -73.9500, 15000),
    "nyc": (40.7300, -73.9500, 15000), "city": (40.7300, -73.9500, 15000),
}
_NICE = {"jfk": "JFK Airport", "laguardia": "LaGuardia Airport", "la guardia": "LaGuardia Airport",
         "new york": "New York", "nyc": "New York", "city": "New York", "soho": "SoHo",
         "fireworks": "the Brooklyn Bridge fireworks", "empire state": "the Empire State Building"}
_DAYS = {"tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5,
         "sunday": 6, "monday": 7}  # day of July 2014 (Jul 1 was a Tuesday)
_UTC = timezone.utc


def _local(day: int, hour: int) -> datetime:
    return (datetime(2014, 7, 1) + timedelta(days=day - 1, hours=hour)) \
        .replace(tzinfo=_NYC).astimezone(_UTC)


def parse_question(q: str, now: datetime | None = None) -> dict:
    """Turn a plain-English question into one tool call. Returns
    {tool, args, understood} or {error}. Covers the questions the demo data
    can answer; anything fancier needs the LLM."""
    s = " " + re.sub(r"[^a-z0-9: ]+", " ", q.lower()) + " "
    now = now or datetime.now(_UTC)

    # The trust demo (synthetic world near Coimbatore).
    if "machine" in s or "fail" in s or "broke" in s:
        n = re.search(r"machine\s*(\d+)", s)
        m = f"Machine_{n.group(1) if n else 47}"
        return {"tool": "history_before", "args": {"entity_id": m, "event_type": "failure",
                                                     "lookback_minutes": 60},
                "understood": f"what happened to {m.replace('_', ' ')} in the hour before it failed"}
    if "flood" in s:
        return {"tool": "fused_location", "args": {"entity_id": "Flood_Event_1",
                                                     "start": (now - timedelta(days=7)).isoformat(),
                                                     "end": now.isoformat()},
                "understood": "where the flood really is, combining every source"}
    if "solar" in s:
        return {"tool": "what_changed", "args": {"lat": 10.9618, "lon": 77.0518, "radius_m": 600,
                                                   "start": (now - timedelta(days=45)).isoformat(),
                                                   "end": now.isoformat(), "limit": 500},
                "understood": "what changed at the solar farm in the last 6 weeks"}
    if "factory" in s:
        return {"tool": "events_near", "args": {"lat": 11.0172, "lon": 76.9566, "radius_m": 300,
                                                  "start": (now - timedelta(days=1)).isoformat(),
                                                  "end": now.isoformat(), "limit": 500},
                "understood": "everything recorded at the factory in the last 24 hours"}

    # NYC pickups, Independence Day week 2014.
    place = next(((k, v) for k, v in sorted(PLACES.items(), key=lambda kv: -len(kv[0]))
                  if f" {k} " in s), None)
    if place is None and ("4th" in s or "july 4" in s or "fourth" in s):
        place = ("fireworks", PLACES["fireworks"])
    if place is None:
        place = ("new york", PLACES["new york"])
    name, (lat, lon, r) = place
    day = next((d for k, d in _DAYS.items() if k in s), None)
    if "4th" in s or "july 4" in s or "fourth" in s or "fireworks" in s or "independence" in s:
        day = 4
    if "weekend" in s:
        start, end, when = _local(5, 0), _local(7, 0), "the weekend of July 5-6, 2014"
    elif day:
        h0, h1 = 0, 24
        if "night" in s or "evening" in s:
            h0, h1 = 18, 30
        elif "morning" in s or "rush" in s:
            h0, h1 = 6, 11
        elif "afternoon" in s:
            h0, h1 = 12, 18
        start, end = _local(day, h0), _local(day, h1)
        label = {6: " night", 18: " evening and night", 12: " afternoon"}.get(h0, "")
        label = " morning" if h0 == 6 else label
        when = f"{datetime(2014, 7, day):%A, July} {day}{label if h0 else ''}"
    else:
        start, end, when = _local(1, 0), _local(8, 0), "the week of July 1-7, 2014"
    span_h = (end - start).total_seconds() / 3600
    w = {"lat": lat, "lon": lon, "radius_m": r, "start": start.isoformat(), "end": end.isoformat()}
    where = _NICE.get(name, name.title())
    if re.search(r" (busiest|hotspot|hot spot|where|popular|most) ", s):
        big = max(r, 5000) if name in ("manhattan", "brooklyn", "queens", "new york", "nyc", "city") else r
        return {"tool": "hotspots", "args": {**w, "radius_m": big, "cell_m": 300 if big > 2000 else 120,
                                             "top": 15},
                "understood": f"the busiest pickup spots in {where} during {when}"}
    if re.search(r" (which|who|company|companies|base|bases|source|sources) ", s):
        return {"tool": "activity", "args": {**w, "bucket_minutes": 60 if span_h <= 48 else 180,
                                             "group_by": "source_id"},
                "understood": f"pickups near {where} during {when}, split by dispatch company"}
    if re.search(r" (show|list|events|pickups near|records) ", s) and span_h <= 24:
        return {"tool": "events_near", "args": {**w, "limit": 300},
                "understood": f"every pickup near {where} during {when}"}
    return {"tool": "activity", "args": {**w, "bucket_minutes": 30 if span_h <= 24 else 60 if span_h <= 48 else 180},
            "understood": f"how busy {where} was, hour by hour, during {when}"}


LLM_MODEL = os.environ.get("GEOMEMORY_LLM_MODEL", "claude-opus-5-5")
_SYSTEM = """You are GeoMemory's analyst: a live assistant for a city operations team, answering from a spatial-temporal memory of real events. Use the tools for every number; never guess.
Data available:
1. Real NYC Uber pickups, Independence Day week 2014: July 1-7 2014 local time (America/New_York, UTC-4). Tools take UTC ISO times, so 6 pm local on July 4 is 2014-07-04T22:00:00Z. Each pickup's source is its dispatch base (B02512, B02598, B02617, B02682, B02764). This is a 20% sample, so say "in the sample" when giving counts.
   Useful places: Times Square 40.7580,-73.9855; JFK 40.6446,-73.7797; LaGuardia 40.7769,-73.8740; Brooklyn Bridge 40.7061,-73.9969; Grand Central 40.7527,-73.9772; Williamsburg 40.7081,-73.9571; Manhattan centre 40.758,-73.9855 (radius ~7 km).
2. A small sensor world near Coimbatore, India, with recent timestamps: Machine_47 fails (use history_before); Flood_Event_1 is reported by sources that disagree (use fused_location); a factory at 11.0172,76.9566; a solar farm at 10.9618,77.0518.
Approach: plan briefly, call several tools when a comparison helps (e.g. two places or two time windows), then answer.
Answer format: 2-5 short sentences for a non-expert, lead with the conclusion, include the key numbers, and end with one concrete recommendation when the user is making a decision. Mention which sources the answer came from. Use plain text, no markdown headings."""


def _llm_call(body: dict, key: str) -> dict:
    """One Messages API call through the official SDK (pip install anthropic)."""
    import anthropic
    client = anthropic.Anthropic(api_key=key, max_retries=2, timeout=90.0)
    msg = client.beta.messages.create(betas=["server-side-fallback-2026-07-01"],
                                      fallbacks="default", **body)
    return msg.to_dict()


OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.5")


def _openrouter_call(body: dict, key: str) -> dict:
    """The same request through OpenRouter (OpenAI-style chat completions),
    translated to and from the Messages shape the agent loop uses."""
    msgs = [{"role": "system", "content": body["system"]}]
    for m in body["messages"]:
        c = m["content"]
        if isinstance(c, str):
            msgs.append({"role": m["role"], "content": c})
        elif m["role"] == "assistant":
            text = "".join(b.get("text", "") for b in c if b.get("type") == "text")
            calls = [{"id": b["id"], "type": "function",
                      "function": {"name": b["name"], "arguments": json.dumps(b.get("input") or {})}}
                     for b in c if b.get("type") == "tool_use"]
            msgs.append({"role": "assistant", "content": text or None, **({"tool_calls": calls} if calls else {})})
        else:
            msgs += [{"role": "tool", "tool_call_id": b["tool_use_id"], "content": b["content"]}
                     for b in c if b.get("type") == "tool_result"]
    req = {"model": OPENROUTER_MODEL, "max_tokens": 4000, "messages": msgs,
           "tools": [{"type": "function", "function": {"name": t["name"], "description": t["description"],
                                                        "parameters": t["input_schema"]}} for t in body["tools"]]}
    r = urllib.request.Request("https://openrouter.ai/api/v1/chat/completions", data=json.dumps(req).encode(),
                               headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json",
                                        "X-Title": "GeoMemory"})
    try:
        with urllib.request.urlopen(r, timeout=90) as f:
            out = json.load(f)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"OpenRouter {e.code}: {e.read().decode(errors='replace')[:300]}") from None
    if "error" in out:
        raise RuntimeError(f"OpenRouter: {out['error'].get('message', out['error'])}")
    msg = out["choices"][0]["message"]
    content = [{"type": "text", "text": msg["content"]}] if msg.get("content") else []
    for tc in msg.get("tool_calls") or []:
        try:
            args = json.loads(tc["function"].get("arguments") or "{}")
        except ValueError:
            args = {}
        content.append({"type": "tool_use", "id": tc["id"], "name": tc["function"]["name"], "input": args})
    return {"stop_reason": "tool_use" if any(b["type"] == "tool_use" for b in content) else "end_turn",
            "content": content}


def _provider(key: str | None):
    """(key, call) for the configured LLM: Anthropic first, then OpenRouter."""
    if key is not None:
        return key, None
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"], None
    if os.environ.get("OPENROUTER_API_KEY"):
        return os.environ["OPENROUTER_API_KEY"], _openrouter_call
    return None, None


def _short(res: dict) -> dict:
    """Trim a tool result before showing it to the LLM."""
    out = dict(res)
    if "observations" in out:
        out["observations"] = out["observations"][:15]
    if "buckets" in out:
        out["buckets"] = [b for b in out["buckets"] if b["count"]][:80]
    if "hotspots" in out:
        out["hotspots"] = [{k: h[k] for k in ("lat", "lon", "count", "sources")} for h in out["hotspots"][:10]]
    return out


def _clean_history(messages) -> list[dict] | None:
    """Chat history from the browser: alternating user/assistant plain-text turns."""
    if not isinstance(messages, list) or not messages or len(messages) > 40:
        return None
    out = []
    for m in messages:
        if not isinstance(m, dict) or m.get("role") not in ("user", "assistant"):
            return None
        text = m.get("content")
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            return None
        out.append({"role": m["role"], "content": text.strip()})
    if out[0]["role"] != "user" or out[-1]["role"] != "user" or len(out[-1]["content"]) > 500:
        return None
    return out


def chat(agent, messages, key: str | None = None, llm=None) -> dict:
    """A multi-turn conversation: Claude plans, calls GeoMemory tools, answers.
    Without a key (or if the API fails) the built-in reader answers the last message."""
    history = _clean_history(messages)
    if history is None:
        return {"ok": False, "error": "Send a conversation that ends with a question (max 500 characters)."}
    key, call = _provider(key)
    question = history[-1]["content"]
    if key:
        try:
            return _chat_llm(agent, history, key, llm or call or _llm_call)
        except Exception as e:  # network, quota, bad key: fall back, but say so
            out = _ask_rules(agent, question)
            out["note"] = (f"The AI model was unavailable ({type(e).__name__}: {str(e)[:200]}); "
                           "answered with the built-in reader.")
            return out
    return _ask_rules(agent, question)


def ask(agent, question: str, key: str | None = None, llm=None) -> dict:
    question = (question or "").strip()
    if not question:
        return {"ok": False, "error": "Type a question first."}
    if len(question) > 500:
        return {"ok": False, "error": "Please keep the question under 500 characters."}
    return chat(agent, [{"role": "user", "content": question}], key, llm)


def _step(name: str, args: dict, res: dict) -> dict:
    """What the UI shows for one tool call (and draws on the map)."""
    return {"tool": name, "args": args, "ok": bool(res.get("ok")),
            "count": res.get("count"), "took_ms": res.get("took_ms"),
            "searched": res.get("searched"), "error": res.get("error"), "result": res}


def _ask_rules(agent, question: str) -> dict:
    p = parse_question(question)
    res = agent.dispatch(p["tool"], p["args"])
    return {"ok": True, "mode": "rules", "question": question, "understood": p["understood"],
            "tool": p["tool"], "args": p["args"], "result": res,
            "steps": [_step(p["tool"], p["args"], res)]}


def _chat_llm(agent, history: list[dict], key: str, llm) -> dict:
    from .agent import TOOLS
    tools = [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]}
             for t in TOOLS]
    msgs = list(history)
    steps, last = [], None
    for _ in range(8):
        r = llm({"model": LLM_MODEL, "max_tokens": 16000, "system": _SYSTEM,
                 "output_config": {"effort": "low"}, "tools": tools, "messages": msgs}, key)
        if r.get("stop_reason") == "refusal":
            raise RuntimeError("the model declined this request")
        msgs.append({"role": "assistant", "content": r["content"]})
        calls = [c for c in r["content"] if c.get("type") == "tool_use"]
        if not calls:
            text = "\n\n".join(c.get("text", "") for c in r["content"]
                                if c.get("type") == "text").strip()
            out = {"ok": True, "mode": "llm", "question": history[-1]["content"],
                   "answer": text, "steps": steps}
            if last:
                out.update(tool=last["tool"], args=last["args"], result=last["result"])
            return out
        results = []
        for c in calls:
            args = c.get("input") if isinstance(c.get("input"), dict) else {}
            res = agent.dispatch(c["name"], args)
            step = _step(c["name"], args, res)
            steps.append(step)
            if c["name"] != "evidence" and res.get("ok"):
                last = step
            results.append({"type": "tool_result", "tool_use_id": c["id"],
                            "content": json.dumps(_short(res), default=str)[:20000],
                            **({} if res.get("ok") else {"is_error": True})})
        msgs.append({"role": "user", "content": results})
    raise RuntimeError("too many tool calls")


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="GeoMemory showcase data")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="rebuild the committed NYC pack from data/raw")
    b.add_argument("--raw", default=str(Path(__file__).resolve().parent.parent / "data" / "raw"))
    a = ap.parse_args(argv)
    if a.cmd == "build":
        build_pack(Path(a.raw))


if __name__ == "__main__":
    main()
