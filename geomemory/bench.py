"""Benchmark harness for the core experiments E1–E6 (spec §18–20).

    python -m geomemory.bench --scale S --out results/

Writes results.json and a Markdown report with one table per experiment.
Every experiment also checks correctness against a brute-force answer, so a
fast-but-wrong configuration is reported as wrong, never as a win.
"""
from __future__ import annotations

import argparse
import gc
import json
import math
import platform
import random
import statistics
import threading
import time
import tracemalloc
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .agent import AgentInterface, keyword_baseline, precision_recall
from .cluster import Cluster
from .index import GeohashIndex, GridIndex, QuadTreeIndex, ScanIndex
from .model import Observation, Point, Provenance, haversine_m
from .partition import GridPartitioner, KDPartitioner
from .store import GeoMemory
from .stream import BacklogFull, Broker, StreamProcessor

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
EVENTS = ["ping", "move", "detect", "failure", "inspection", "change"]


@dataclass(frozen=True)
class Scale:
    name: str
    e1_sizes: tuple[int, ...]
    e2_size: int
    e3_size: int
    e3_nodes: tuple[int, ...]
    e4_rates: tuple[int, ...]
    e4_seconds: float
    e5_size: int
    e6_size: int
    queries: int
    e3_batch: int = 2000


SCALES = {
    "tiny": Scale("tiny", (500, 2_000), 2_000, 2_000, (1, 2, 4), (100, 1_000), 0.3, 2_000, 2_000, 10,
                 e3_batch=100),
    "S": Scale("S", (10_000, 100_000), 100_000, 50_000, (1, 2, 4, 8), (100, 1_000, 10_000), 2.0,
               50_000, 20_000, 50),
    "M": Scale("M", (10_000, 100_000, 1_000_000), 1_000_000, 200_000, (1, 2, 4, 8),
               (100, 1_000, 10_000), 5.0, 200_000, 100_000, 100, e3_batch=5000),
}


# --- synthetic datasets (spec §18.1, §18.8) ------------------------------------

HOTSPOTS = [(11.0, 76.0), (40.7, -74.0), (51.5, -0.1), (35.7, 139.7), (-23.5, -46.6)]


def make_dataset(n: int, seed: int, dist: str = "mixed", days: float = 365) -> list[Observation]:
    """uniform: anywhere on Earth. hotspot: 90% in five city-sized clusters.
    mixed: half and half (the default; closest to real sensor data)."""
    rng = random.Random(seed)
    p_hot = {"uniform": 0.0, "hotspot": 0.9, "mixed": 0.5}[dist]
    out = []
    for i in range(n):
        if rng.random() < p_hot:
            clat, clon = rng.choice(HOTSPOTS)
            lat = min(90.0, max(-90.0, rng.gauss(clat, 0.05)))
            lon = min(180.0, max(-180.0, rng.gauss(clon, 0.05)))
        else:
            lat = math.degrees(math.asin(rng.uniform(-1, 1)))  # uniform on the sphere
            lon = rng.uniform(-180, 180)
        out.append(Observation(
            entity_id=f"e{rng.randrange(max(1, n // 20))}", event_type=rng.choice(EVENTS),
            location=Point(lat, lon), timestamp=T0 + timedelta(seconds=rng.uniform(0, days * 86400)),
            provenance=Provenance(f"s{rng.randrange(50)}"), confidence=rng.uniform(0.5, 1.0),
            spatial_uncertainty_m=rng.choice([1.0, 5.0, 25.0]), observation_id=f"o{seed}_{i}"))
    return out


def query_points(n: int, seed: int) -> list[Point]:
    """Half the queries hit hotspots, half land anywhere."""
    rng = random.Random(seed)
    pts = []
    for i in range(n):
        if i % 2 == 0:
            clat, clon = rng.choice(HOTSPOTS)
            pts.append(Point(clat + rng.gauss(0, 0.03), clon + rng.gauss(0, 0.03)))
        else:
            pts.append(Point(math.degrees(math.asin(rng.uniform(-1, 1))), rng.uniform(-180, 180)))
    return pts


# --- measurement helpers ---------------------------------------------------------

def timed(fn: Callable[[], object]) -> tuple[float, object]:
    t = time.perf_counter()
    r = fn()
    return time.perf_counter() - t, r


def latency_stats(samples_s: list[float]) -> dict:
    xs = sorted(samples_s)
    if not xs:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "mean_ms": 0.0}
    return {"p50_ms": 1e3 * xs[len(xs) // 2],
            "p95_ms": 1e3 * xs[min(len(xs) - 1, int(0.95 * len(xs)))],
            "mean_ms": 1e3 * statistics.fmean(xs)}


def peak_memory_mb(fn: Callable[[], object]) -> float:
    gc.collect()
    tracemalloc.start()
    obj = fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del obj
    return peak / 2**20


def brute(points: list[Observation], c: Point, r: float) -> set[str]:
    return {o.observation_id for o in points if haversine_m(c, o.location) <= r}


# --- E1: dataset scaling -----------------------------------------------------------

def e1_scaling(sc: Scale, seed: int) -> dict:
    rows = []
    for n in sc.e1_sizes:
        data = make_dataset(n, seed)
        gm = GeoMemory(QuadTreeIndex())
        ingest_s, _ = timed(lambda: gm.ingest_many(data))
        qs = query_points(sc.queries, seed + 1)
        radius = [timed(lambda c=c: gm.radius(c, 2_000))[0] for c in qs]
        window = [timed(lambda i=i: gm.between(T0 + timedelta(days=i % 300),
                                              T0 + timedelta(days=i % 300 + 1)))[0]
                  for i in range(sc.queries)]
        st = [timed(lambda c=c: gm.radius_between(c, 2_000, T0 + timedelta(days=100),
                                                 T0 + timedelta(days=130)))[0] for c in qs]
        mem = peak_memory_mb(lambda: GeoMemory(QuadTreeIndex()).ingest_many(data)) \
            if n <= 200_000 else None
        ok = all({o.observation_id for o in gm.radius(c, 2_000)} == brute(data, c, 2_000)
                 for c in qs[:3])
        rows.append({"n": n, "ingest_per_s": n / ingest_s,
                     "radius_2km": latency_stats(radius), "time_1day": latency_stats(window),
                     "spatiotemporal": latency_stats(st),
                     "memory_mb": mem, "bytes_per_obs": mem * 2**20 / n if mem else None,
                     "correct": ok})
    return {"index": "quadtree", "rows": rows}


# --- E2: spatial index comparison ------------------------------------------------

INDEXES = {
    "Scan (no index)": ScanIndex,
    "Grid 0.01°": lambda: GridIndex(0.01),
    "Grid 1°": lambda: GridIndex(1.0),
    "Geohash p6": lambda: GeohashIndex(6),
    "QuadTree": lambda: QuadTreeIndex(32),
}


def e2_indexes(sc: Scale, seed: int) -> dict:
    data = make_dataset(sc.e2_size, seed)
    qs = query_points(sc.queries, seed + 2)
    truth = {r: [brute(data, c, r) for c in qs[:5]] for r in (500, 50_000)}
    ring = [Point(10.9, 75.9), Point(10.9, 76.1), Point(11.1, 76.1), Point(11.1, 75.9)]
    rows = []
    for name, factory in INDEXES.items():
        gm = GeoMemory(factory())
        build_s, _ = timed(lambda: gm.ingest_many(data))
        lat = {r: latency_stats([timed(lambda c=c: gm.radius(c, r))[0] for c in qs])
               for r in (500, 50_000)}
        knn = latency_stats([timed(lambda c=c: gm.nearest(c, 10))[0] for c in qs[:20]])
        poly = latency_stats([timed(lambda: gm.within_polygon(ring))[0] for _ in range(5)])
        ok = all({o.observation_id for o in gm.radius(c, r)} == t
                 for r, ts in truth.items() for c, t in zip(qs, ts))
        mem = peak_memory_mb(lambda: _build_index(factory, data))
        rows.append({"index": name, "build_s": build_s, "index_memory_mb": mem,
                     "radius_500m": lat[500], "radius_50km": lat[50_000], "knn_10": knn,
                     "polygon": poly, "correct": ok})
    return {"n": len(data), "rows": rows}


def _build_index(factory, data):
    idx = factory()
    for o in data:
        idx.insert(o.observation_id, o.location)
    return idx


# --- E3: distributed scaling ----------------------------------------------------

def e3_processes(sc: Scale, seed: int) -> dict:
    """Real parallelism: one OS process per worker, a batch of radius queries
    fanned out to all of them, wall-clock timed (spec §18.7)."""
    import os
    from .procluster import ProcessCluster
    data = make_dataset(sc.e3_size, seed)
    rng = random.Random(seed + 33)
    qs = [(c, rng.choice([20_000, 100_000, 300_000])) for c in query_points(sc.e3_batch, seed + 3)]
    sample = [o.location for o in random.Random(seed).sample(data, min(2000, len(data)))]
    truth = [brute(data, c, r) for c, r in qs[:5]]
    rows, t1 = [], None
    for n in sc.e3_nodes:
        with ProcessCluster(KDPartitioner(n * 4, sample), n) as pc:
            ingest_s, _ = timed(lambda: pc.ingest_many(data))
            pc.radius_batch(qs[:10])  # warm-up
            runs = [timed(lambda: pc.radius_batch(qs))[0] for _ in range(3)]
            wall = min(runs)
            ok = pc.radius_batch(qs[:5]) == truth
            sizes = list(pc.partition_sizes().values())
        t1 = t1 or wall
        rows.append({"workers": n, "wall_s": wall, "queries_per_s": len(qs) / wall,
                     "speedup": t1 / wall, "efficiency": t1 / wall / n,
                     "ingest_per_s": len(data) / ingest_s,
                     "imbalance": max(sizes) / (sum(sizes) / len(sizes)), "correct": ok})
    return {"n": len(data), "queries": len(qs), "cpus": os.cpu_count(), "rows": rows}


def e3_distributed(sc: Scale, seed: int) -> dict:
    """The cluster is simulated in one process, so wall-clock speedup is not
    meaningful. We instead time each partition's share of every query and
    model the parallel time as the slowest node (critical path)."""
    data = make_dataset(sc.e3_size, seed)
    qs = query_points(sc.queries, seed + 3)
    sample = [o.location for o in random.Random(seed).sample(data, min(2000, len(data)))]
    rows = []
    t1 = None
    for n in sc.e3_nodes:
        c = Cluster(KDPartitioner(n * 4, sample), n, replication=min(2, n),
                    index_factory=lambda: QuadTreeIndex())
        c.ingest_many(data)
        serial, critical, touched = [], [], []
        for q in qs:
            per_node = [0.0] * n
            pids = c.partitioner.partitions_for_bbox(_box(q, 50_000))
            touched.append(len(pids))
            for pid in pids:
                node = c._live_replica(pid)
                store = node.partitions.get(pid)
                if store is None:
                    continue
                dt, _ = timed(lambda s=store: s.radius(q, 50_000))
                per_node[node.node_id] += dt
            serial.append(sum(per_node))
            critical.append(max(per_node))
        msgs = 2 * statistics.fmean(touched)  # one request + one reply per partition
        ok = all({o.observation_id for o in c.radius(q, 50_000)} == brute(data, q, 50_000)
                 for q in qs[:3])
        total = sum(critical)
        t1 = t1 or sum(serial)
        loads = c.records_per_node()
        rows.append({"workers": n, "partitions": n * 4, "modeled_time_s": total,
                     "speedup": t1 / total if total else None,
                     "efficiency": (t1 / total) / n if total else None,
                     "partitions_touched": statistics.fmean(touched),
                     "messages_per_query": msgs,
                     "max_node_share": max(loads) / sum(loads), "correct": ok})
    return {"n": len(data), "query": "radius 50 km", "rows": rows}


def _box(c: Point, r: float):
    from .index import bbox_for_radius
    boxes = bbox_for_radius(c, r)
    return (min(b[0] for b in boxes), max(b[1] for b in boxes),
            min(b[2] for b in boxes), max(b[3] for b in boxes))


# --- E4: streaming throughput ---------------------------------------------------

def e4_streaming(sc: Scale, seed: int) -> dict:
    rows = []
    for rate in sc.e4_rates:
        n = max(1, int(rate * sc.e4_seconds))
        data = make_dataset(n, seed + rate)
        broker = Broker(4, max_backlog=50_000)
        gm = GeoMemory(QuadTreeIndex())
        proc = StreamProcessor(broker, gm, batch_size=500)
        done = threading.Event()
        q_lat: list[float] = []
        backlog_max = 0

        def consume():
            while not done.is_set() or broker.backlog("geomemory"):
                if not proc.poll_once():
                    time.sleep(0.0005)

        def query_load():
            qs = query_points(1000, seed)
            i = 0
            while not done.is_set():
                q_lat.append(timed(lambda: gm.radius(qs[i % len(qs)], 2_000))[0])
                i += 1
                time.sleep(0.01)

        workers = [threading.Thread(target=consume), threading.Thread(target=query_load)]
        for w in workers:
            w.start()
        start = time.perf_counter()
        for i, o in enumerate(data):
            target = start + i / rate
            delay = target - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            try:
                broker.produce(_raw(o))
            except BacklogFull:
                pass
            backlog_max = max(backlog_max, broker.backlog("geomemory"))
        produce_s = time.perf_counter() - start
        done.set()
        for w in workers:
            w.join()
        drain_s = time.perf_counter() - start
        m = proc.metrics
        rows.append({"offered_rate": rate, "achieved_ingest_per_s": m.stored / drain_s,
                     "produce_s": produce_s, "drain_s": drain_s, "stored": m.stored,
                     "dropped": broker.dropped, "max_backlog": backlog_max,
                     "e2e_p50_ms": 1e3 * m.p(0.5), "e2e_p99_ms": 1e3 * m.p(0.99),
                     "query_under_load": latency_stats(q_lat), "correct": m.stored == n})
    return {"rows": rows}


def _raw(o: Observation) -> dict:
    return {"observation_id": o.observation_id, "entity_id": o.entity_id,
            "event_type": o.event_type, "lat": o.location.lat, "lon": o.location.lon,
            "timestamp": o.timestamp.isoformat(), "source_id": o.source_id,
            "confidence": o.confidence, "uncertainty_m": o.spatial_uncertainty_m}


# --- E5: spatial skew ---------------------------------------------------------

def e5_skew(sc: Scale, seed: int, n_parts: int = 16) -> dict:
    rows = []
    for dist in ("uniform", "hotspot"):
        data = make_dataset(sc.e5_size, seed, dist)
        sample = [o.location for o in random.Random(seed).sample(data, min(2000, len(data)))]
        for pname, part in (("Grid 10°", GridPartitioner(n_parts, 10.0)),
                            ("KD (adaptive)", KDPartitioner(n_parts, sample))):
            c = Cluster(part, n_parts, 1, index_factory=lambda: GridIndex(0.1))
            c.ingest_many(data)
            sizes = sorted(c.partition_sizes().values())
            hot_q = [Point(la, lo) for la, lo in HOTSPOTS]
            touched = [len(part.partitions_for_bbox(_box(q, 5_000))) for q in hot_q]
            cost = []
            for q in hot_q:  # work on the busiest partition a hotspot query hits
                pids = part.partitions_for_bbox(_box(q, 5_000))
                cost.append(max(len(c._live_replica(p).partitions.get(p, ())) for p in pids))
            rows.append({"distribution": dist, "partitioner": pname,
                         "imbalance_max_over_mean": c.imbalance(),
                         "largest_partition": sizes[-1], "smallest_partition": sizes[0],
                         "empty_partitions": sum(1 for s in sizes if s == 0),
                         "hotspot_query_partitions": statistics.fmean(touched),
                         "hotspot_query_max_partition_rows": statistics.fmean(cost)})
    return {"partitions": n_parts, "rows": rows}


# --- E6: agent retrieval ----------------------------------------------------------

def e6_retrieval(sc: Scale, seed: int) -> dict:
    data = make_dataset(sc.e6_size, seed, "mixed")
    gm = GeoMemory(QuadTreeIndex())
    gm.ingest_many(data)
    agent = AgentInterface(gm)
    rng = random.Random(seed + 6)
    geo_p, geo_r, kw_p, kw_r, lat = [], [], [], [], []
    for _ in range(sc.queries):
        clat, clon = rng.choice(HOTSPOTS)
        c = Point(clat + rng.gauss(0, 0.02), clon + rng.gauss(0, 0.02))
        r = rng.choice([500, 1_000, 5_000])
        start = T0 + timedelta(days=rng.uniform(0, 330))
        end = start + timedelta(days=30)
        etype = rng.choice(EVENTS)
        relevant = {o.observation_id for o in data if o.event_type == etype
                    and start <= o.timestamp <= end and haversine_m(c, o.location) <= r}
        dt, res = timed(lambda: agent.dispatch("events_near", {
            "lat": c.lat, "lon": c.lon, "radius_m": r, "start": start.isoformat(),
            "end": end.isoformat(), "event_type": etype, "limit": 1000}))
        lat.append(dt)
        p, rc = precision_recall([o["observation_id"] for o in res["observations"]], relevant)
        geo_p.append(p), geo_r.append(rc)
        p, rc = precision_recall(keyword_baseline(data, [etype], 1000), relevant)
        kw_p.append(p), kw_r.append(rc)
    return {"n": len(data), "queries": sc.queries, "rows": [
        {"method": "GeoMemory spatial-temporal", "precision": statistics.fmean(geo_p),
         "recall": statistics.fmean(geo_r), "latency": latency_stats(lat)},
        {"method": "Keyword baseline", "precision": statistics.fmean(kw_p),
         "recall": statistics.fmean(kw_r), "latency": None}]}


EXPERIMENTS = {"E1": e1_scaling, "E2": e2_indexes, "E3": e3_processes, "E3M": e3_distributed,
               "E4": e4_streaming, "E5": e5_skew, "E6": e6_retrieval}


# --- report -----------------------------------------------------------------------

def _f(x, d=2):
    if x is None:
        return "–"
    if isinstance(x, bool):
        return "yes" if x else "**NO**"
    if isinstance(x, float):
        return f"{x:,.{d}f}"
    return f"{x:,}" if isinstance(x, int) else str(x)


def _table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(_f(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def render_markdown(res: dict) -> str:
    md = [f"# GeoMemory benchmark results\n",
          f"Scale `{res['scale']}`, seed {res['seed']}, Python {res['python']}, "
          f"{res['machine']}, run {res['started']}.\n",
          "Latencies are milliseconds. \"Correct\" means the answer equals a brute-force scan.\n"]
    e = res["experiments"]
    if "E1" in e:
        md += ["## E1: dataset scaling (QuadTree index)\n", _table(
            ["Observations", "Ingest/s", "Radius 2 km p50", "p95", "1-day window p50",
             "Place+time p50", "Memory MB", "Bytes/obs", "Correct"],
            [[r["n"], r["ingest_per_s"], r["radius_2km"]["p50_ms"], r["radius_2km"]["p95_ms"],
              r["time_1day"]["p50_ms"], r["spatiotemporal"]["p50_ms"], r["memory_mb"],
              r["bytes_per_obs"], r["correct"]] for r in e["E1"]["rows"]]), ""]
    if "E2" in e:
        md += [f"## E2: spatial index comparison ({e['E2']['n']:,} observations)\n", _table(
            ["Index", "Build s", "Index MB", "Radius 500 m p50", "Radius 50 km p50",
             "10-NN p50", "Polygon p50", "Correct"],
            [[r["index"], r["build_s"], r["index_memory_mb"], r["radius_500m"]["p50_ms"],
              r["radius_50km"]["p50_ms"], r["knn_10"]["p50_ms"], r["polygon"]["p50_ms"],
              r["correct"]] for r in e["E2"]["rows"]]), ""]
    if "E3" in e:
        x = e["E3"]
        md += [f"## E3: distributed scaling, real processes ({x['n']:,} observations, "
               f"batch of {x['queries']:,} radius queries, {x['cpus']} CPU cores)\n",
               "Each worker is a separate OS process. Wall-clock time, best of 3. Rows with more "
               "workers than cores are oversubscribed and cannot speed up further.\n",
               _table(["Workers", "Wall s", "Queries/s", "Speedup", "Efficiency", "Ingest/s",
                       "Partition imbalance", "Correct"],
                      [[r["workers"], r["wall_s"], r["queries_per_s"], r["speedup"],
                        r["efficiency"], r["ingest_per_s"], r["imbalance"], r["correct"]]
                       for r in x["rows"]]), ""]
    if "E3M" in e:
        md += [f"## E3M: distributed scaling, modeled ({e['E3M']['n']:,} observations, "
               f"{e['E3M']['query']})\n",
               "Simulated in one process: time is *modeled* as the slowest node per query.\n",
               _table(["Workers", "Partitions", "Modeled time s", "Speedup", "Efficiency",
                       "Partitions touched", "Messages/query", "Max node share", "Correct"],
                      [[r["workers"], r["partitions"], r["modeled_time_s"], r["speedup"],
                        r["efficiency"], r["partitions_touched"], r["messages_per_query"],
                        r["max_node_share"], r["correct"]] for r in e["E3M"]["rows"]]), ""]
    if "E4" in e:
        md += ["## E4: streaming throughput (with concurrent queries)\n", _table(
            ["Offered/s", "Achieved/s", "Stored", "Dropped", "Max backlog", "E2E p50",
             "E2E p99", "Query p50 under load", "Correct"],
            [[r["offered_rate"], r["achieved_ingest_per_s"], r["stored"], r["dropped"],
              r["max_backlog"], r["e2e_p50_ms"], r["e2e_p99_ms"],
              r["query_under_load"]["p50_ms"], r["correct"]] for r in e["E4"]["rows"]]), ""]
    if "E5" in e:
        md += [f"## E5: spatial skew ({e['E5']['partitions']} partitions)\n", _table(
            ["Data", "Partitioner", "Imbalance (max/mean)", "Largest", "Smallest", "Empty",
             "Hotspot query: partitions", "Hotspot query: rows on busiest"],
            [[r["distribution"], r["partitioner"], r["imbalance_max_over_mean"],
              r["largest_partition"], r["smallest_partition"], r["empty_partitions"],
              r["hotspot_query_partitions"], r["hotspot_query_max_partition_rows"]]
             for r in e["E5"]["rows"]]), ""]
    if "E6" in e:
        md += [f"## E6: agent retrieval ({e['E6']['queries']} queries over "
               f"{e['E6']['n']:,} observations)\n", _table(
                   ["Method", "Precision", "Recall", "Latency p50"],
                   [[r["method"], f'{r["precision"]:.4f}', f'{r["recall"]:.4f}',
                     r["latency"]["p50_ms"] if r["latency"] else None]
                    for r in e["E6"]["rows"]]), ""]
    return "\n".join(md)


def run(scale: str = "S", experiments=None, seed: int = 42, out: str | None = None,
        log: Callable[[str], None] = print) -> dict:
    sc = SCALES[scale]
    names = experiments or list(EXPERIMENTS)
    res = {"scale": scale, "seed": seed, "python": platform.python_version(),
           "machine": platform.machine(), "started": datetime.now(timezone.utc).isoformat(
               timespec="seconds"), "experiments": {}}
    for name in names:
        log(f"running {name} ...")
        dt, r = timed(lambda: EXPERIMENTS[name](sc, seed))
        r["seconds"] = dt
        res["experiments"][name] = r
        log(f"  {name} done in {dt:.1f}s")
    if out:
        d = Path(out)
        d.mkdir(parents=True, exist_ok=True)
        (d / "results.json").write_text(json.dumps(res, indent=2))
        (d / "RESULTS.md").write_text(render_markdown(res))
        log(f"wrote {d / 'results.json'} and {d / 'RESULTS.md'}")
    return res


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--scale", choices=SCALES, default="S")
    ap.add_argument("--experiments", default=",".join(EXPERIMENTS),
                    help="comma-separated subset, e.g. E1,E2")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="results")
    a = ap.parse_args(argv)
    names = [x.strip().upper() for x in a.experiments.split(",") if x.strip()]
    bad = [x for x in names if x not in EXPERIMENTS]
    if bad:
        ap.error(f"unknown experiments: {bad}")
    run(a.scale, names, a.seed, a.out)


if __name__ == "__main__":
    main()
