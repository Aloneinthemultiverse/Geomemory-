"""Multi-process cluster: each worker is a separate OS process that owns its
partitions, so queries really run in parallel across CPU cores (spec §18.7).

Same placement and routing as `Cluster` (partition p lives on workers
p, p+1, ... mod n), but data and work live in the workers. Worker death is
detected from the process itself (killed, crashed, or pipe closed), and
reads fail over to a live replica.
"""
from __future__ import annotations

import heapq
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from .cluster import PartitionUnavailable
from .index import QuadTreeIndex, bbox_for_radius
from .model import Observation, Point, haversine_m
from .partition import Partitioner
from .store import GeoMemory


def _worker(conn) -> None:
    stores: dict[int, GeoMemory] = {}

    def store(pid: int) -> GeoMemory:
        if pid not in stores:
            stores[pid] = GeoMemory(QuadTreeIndex())
        return stores[pid]

    def on(pids, fn):
        out = []
        for pid in pids:
            if pid in stores:
                out.extend(fn(stores[pid]))
        return out

    while True:
        try:
            msg = conn.recv()
        except (EOFError, OSError):
            return
        op, args = msg
        try:
            if op == "stop":
                conn.send(("ok", None))
                return
            elif op == "ingest":
                res = sum(store(pid).ingest(o) for pid, o in args)
            elif op == "radius":
                pids, c, r = args
                res = on(pids, lambda s: s.radius(c, r))
            elif op == "radius_between":
                pids, c, r, a, b = args
                res = on(pids, lambda s: s.radius_between(c, r, a, b))
            elif op == "between":
                pids, a, b = args
                res = on(pids, lambda s: s.between(a, b))
            elif op == "nearest":
                pids, c, k = args
                res = heapq.nsmallest(k, on(pids, lambda s: s.nearest(c, k)),
                                      key=lambda o: (haversine_m(c, o.location), o.observation_id))
            elif op == "within_polygon":
                pids, ring = args
                res = on(pids, lambda s: s.within_polygon(ring))
            elif op == "entity_history":
                pids, e = args
                res = on(pids, lambda s: s.entity_history(e))
            elif op == "radius_batch":
                # [(query_index, pids, center, r)] -> [(query_index, [ids])]
                res = [(qi, [o.observation_id for o in on(pids, lambda s: s.radius(c, r))])
                       for qi, pids, c, r in args]
            elif op == "sizes":
                res = {pid: len(s) for pid, s in stores.items()}
            elif op == "dump":
                res = [o for pid in args if pid in stores for o in stores[pid]._obs.values()]
            else:
                raise ValueError(f"unknown op {op}")
            conn.send(("ok", res))
        except Exception as e:  # report, keep serving
            conn.send(("err", f"{type(e).__name__}: {e}"))


class WorkerError(RuntimeError):
    pass


class ProcessCluster:
    def __init__(self, partitioner: Partitioner, n_workers: int, replication: int = 1,
                 start_method: str = "fork") -> None:
        if not 1 <= replication <= n_workers:
            raise ValueError("replication must be in [1, n_workers]")
        self.partitioner = partitioner
        self.n = n_workers
        self.replication = replication
        ctx = mp.get_context(start_method)
        self._procs, self._conns = [], []
        for _ in range(n_workers):
            parent, child = ctx.Pipe()
            p = ctx.Process(target=_worker, args=(child,), daemon=True)
            p.start()
            child.close()
            self._procs.append(p)
            self._conns.append(parent)
        self._dead: set[int] = set()

    # --- lifecycle --------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self) -> None:
        for i, conn in enumerate(self._conns):
            if i not in self._dead:
                try:
                    conn.send(("stop", None))
                    conn.recv()
                except (EOFError, OSError, BrokenPipeError):
                    pass
            conn.close()
        for p in self._procs:
            p.join(timeout=2)
            if p.is_alive():
                p.kill()

    def kill(self, worker: int) -> None:
        """Hard-kill a worker process (SIGKILL), as in a real node crash."""
        self._procs[worker].kill()
        self._procs[worker].join()

    def alive(self, worker: int) -> bool:
        return worker not in self._dead and self._procs[worker].is_alive()

    # --- placement ----------------------------------------------------------

    def replicas(self, pid: int) -> list[int]:
        return [(pid + i) % self.n for i in range(self.replication)]

    def _assign(self, pids: Iterable[int]) -> dict[int, list[int]]:
        """worker -> partitions it should serve, using the first live replica."""
        plan: dict[int, list[int]] = defaultdict(list)
        for pid in sorted(set(pids)):
            for w in self.replicas(pid):
                if self.alive(w):
                    plan[w].append(pid)
                    break
            else:
                raise PartitionUnavailable(f"partition {pid} has no live replica")
        return plan

    # --- messaging ------------------------------------------------------------

    def _exchange(self, requests: dict[int, tuple]) -> dict[int, object]:
        """Send all requests first, then collect: workers run concurrently.
        Returns results for workers that answered; marks dead ones."""
        sent = []
        for w, msg in requests.items():
            try:
                self._conns[w].send(msg)
                sent.append(w)
            except (BrokenPipeError, OSError):
                self._dead.add(w)
        out = {}
        for w in sent:
            try:
                status, res = self._conns[w].recv()
            except (EOFError, OSError, ConnectionResetError):
                self._dead.add(w)
                continue
            if status == "err":
                raise WorkerError(f"worker {w}: {res}")
            out[w] = res
        return out

    def _scatter(self, pids: Iterable[int], make_msg) -> list:
        """Run on the partitions' live replicas; on worker death, retry the
        lost partitions on the next replica until done or unavailable."""
        pending = set(pids)
        results: list = []
        while pending:
            plan = self._assign(pending)
            answers = self._exchange({w: make_msg(ps) for w, ps in plan.items()})
            for w, ps in plan.items():
                if w in answers:
                    results.extend(answers[w])
                    pending -= set(ps)
        return results

    # --- ingest ------------------------------------------------------------------

    def ingest_many(self, observations: Iterable[Observation], batch: int = 5000) -> int:
        """Write every observation to all live replicas of its partition."""
        new = 0
        buf: dict[int, list] = defaultdict(list)

        def flush():
            nonlocal new
            if buf:
                answers = self._exchange({w: ("ingest", items) for w, items in buf.items()})
                new += sum(answers.values()) // self.replication
                buf.clear()

        count = 0
        for o in observations:
            pid = self.partitioner.partition_of(o.location)
            live = [w for w in self.replicas(pid) if self.alive(w)]
            if not live:
                raise PartitionUnavailable(f"partition {pid} has no live replica")
            for w in live:
                buf[w].append((pid, o))
            count += 1
            if count % batch == 0:
                flush()
        flush()
        return new

    # --- queries ------------------------------------------------------------------

    def _pids_for_radius(self, c: Point, r: float) -> set[int]:
        pids: set[int] = set()
        for box in bbox_for_radius(c, r):
            pids |= self.partitioner.partitions_for_bbox(box)
        return pids

    def _all(self) -> range:
        return range(self.partitioner.n_partitions)

    def radius(self, c: Point, r: float) -> list[Observation]:
        return self._scatter(self._pids_for_radius(c, r), lambda ps: ("radius", (ps, c, r)))

    def radius_between(self, c: Point, r: float, a: datetime, b: datetime) -> list[Observation]:
        hits = self._scatter(self._pids_for_radius(c, r),
                             lambda ps: ("radius_between", (ps, c, r, a, b)))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    def between(self, a: datetime, b: datetime) -> list[Observation]:
        hits = self._scatter(self._all(), lambda ps: ("between", (ps, a, b)))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    def nearest(self, c: Point, k: int = 10) -> list[Observation]:
        hits = self._scatter(self._all(), lambda ps: ("nearest", (ps, c, k)))
        return heapq.nsmallest(k, hits, key=lambda o: (haversine_m(c, o.location), o.observation_id))

    def within_polygon(self, ring: list[Point]) -> list[Observation]:
        lats, lons = [p.lat for p in ring], [p.lon for p in ring]
        pids = self.partitioner.partitions_for_bbox((min(lats), max(lats), min(lons), max(lons)))
        return self._scatter(pids, lambda ps: ("within_polygon", (ps, ring)))

    def entity_history(self, entity_id: str) -> list[Observation]:
        hits = self._scatter(self._all(), lambda ps: ("entity_history", (ps, entity_id)))
        return sorted(hits, key=lambda o: (o.timestamp, o.observation_id))

    def radius_batch(self, queries: list[tuple[Point, float]]) -> list[set[str]]:
        """Many radius queries in one round trip per worker; returns id sets.
        This is the throughput workload for experiment E3."""
        out: list[set[str]] = [set() for _ in queries]
        pending = {qi: self._pids_for_radius(c, r) for qi, (c, r) in enumerate(queries)}
        while any(pending.values()):
            all_pids = set().union(*pending.values())
            owner = {pid: w for w, ps in self._assign(all_pids).items() for pid in ps}
            reqs: dict[int, list] = defaultdict(list)
            for qi, pids in pending.items():
                by_w: dict[int, list[int]] = defaultdict(list)
                for pid in pids:
                    by_w[owner[pid]].append(pid)
                for w, ps in by_w.items():
                    c, r = queries[qi]
                    reqs[w].append((qi, ps, c, r))
            answers = self._exchange({w: ("radius_batch", items) for w, items in reqs.items()})
            for w, items in reqs.items():
                if w not in answers:
                    continue
                for qi, ids in answers[w]:
                    out[qi].update(ids)
                for qi, ps, _, _ in items:
                    pending[qi] -= set(ps)
        return out

    def partition_sizes(self) -> dict[int, int]:
        sizes: dict[int, int] = {}
        plan = self._assign(self._all())
        answers = self._exchange({w: ("sizes", None) for w in plan})
        for w, ps in plan.items():
            for pid in ps:
                sizes[pid] = answers.get(w, {}).get(pid, 0)
        return sizes
