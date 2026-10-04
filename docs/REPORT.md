# GeoMemory: Evaluation Report

**A distributed spatial-temporal memory and provenance engine for AI agents**

This report covers what was built against the [specification](SPECIFICATION.md), how it was tested, and what the measurements show. Every number comes from `results/`. Every result row was checked against a brute-force answer or cross-checked between two engines; none failed.

---

## 1. Summary

| | |
|---|---|
| Code | ~4,000 lines of Python in `geomemory/`, plus a web UI |
| Tests | 115 tests. Randomized brute-force checks, edge cases, real failure injection, and tests against real Kafka, PostGIS, AGE and Spark. |
| Data | 4,534,327 real NYC Uber pickups (2014) and 23,412 global earthquakes (1965–2016), plus synthetic worlds |
| Machine | One container: 4 CPU cores, 15 GB RAM, Python 3.11, PostgreSQL 16 + PostGIS 3.4 + AGE 1.5, Kafka 3.8, Spark 3.5 + Sedona 1.6 |

**Headline results** on 1,000,000 real Uber pickups unless noted:

- **Adaptive indexing wins on real data.** The quadtree answers a 200 m search in 11.7 ms. Fixed grids and geohash take 63–88 ms, and no index takes 90 ms. On uniform synthetic data, the fine grid was nearly as fast as the quadtree. Real density changes the answer. (E2)
- **Near-linear parallel speedup.** Real multi-process workers reached **4.01× on 4 cores** (100% efficiency). 8 workers on 4 cores gave 4.58×. (E3)
- **Skew is the main distributed-systems risk, and adaptive partitioning fixes it.** On real pickups, a fixed grid puts **14.7×** the average load on one partition and leaves 9 of 16 partitions empty. KD partitioning gets that to **1.12×**, and the busiest node's work on a hotspot query falls **13×**. (E5)
- **Streaming holds up.** At 10,000 events/s offered, it stored 9,289/s with **zero drops** while answering queries (median 1 ms). (E4)
- **Agents get exactly the right context.** Place-and-time search returned exactly the relevant records (precision **1.00**, recall **1.00**). Keyword search scored precision **0.003**. (E6)
- **Distributed batch at full scale.** Spark + Sedona spatially joined **all 4.53M pickups against 71,424 zones in 27.7 s**, with none lost, and loaded 1.33M hourly rows into PostGIS.
- **PostGIS cross-check.** PostGIS returned identical answers on every cross-checked query. Its server-side query is fast. My Python client is the bottleneck on large results. (E7)

---

## 2. What was built

### 2.1 Spec phases

| Phase (spec §26) | Module | What it does |
|---|---|---|
| 1. Core data layer | `model.py`, `index.py`, `store.py` | The `Observation` record (entity, event, place, time, source, confidence, ± metres, parent). Grid, geohash and quadtree spatial indexes, plus a time index. Radius, k-nearest, polygon, time-window, place+time, history and lineage queries. |
| 2. Big data layer | `partition.py`, `cluster.py`, `procluster.py`, `stream.py` | Grid and adaptive KD partitioners. A replicated cluster with scatter-gather queries and failover: one simulated version, and one built from real OS processes. A Kafka-style stream processor. |
| 3. Relationship layer | `graph.py` | Typed edges (NEAR, INSIDE, OBSERVED_BY, BEFORE, CHANGED_FROM). Each edge records when it held and which observations prove it. |
| 4. Trust layer | `trust.py` | Uncertainty-weighted location fusion with outlier flagging. Grouping sightings of the same event, snapshot diffs, hotspots, assets getting worse, precursor mining, sequence similarity. |
| 5. Agent layer | `agent.py`, `api.py`, `ui/` | 8 JSON tools for LLM function calling. A stdlib HTTP server and a FastAPI service. A map UI with an evidence drawer. |

### 2.2 Spec technology stack (§24)

| Spec | Implementation | Verified by |
|---|---|---|
| PostgreSQL + PostGIS | `backends/postgis.py`: drop-in replacement for the in-memory store | Same answers as brute force, including the ±180° line and the poles |
| Neo4j or equivalent graph layer | `backends/graph.py`: Apache AGE (openCypher inside PostgreSQL) | Same neighbours, path lengths and evidence as the in-memory graph. Injection-safe. |
| Apache Kafka | `backends/kafka.py`: manual commits, dead-letter topic, idempotent sinks | Duplicates, garbage and injected crashes give exactly-once storage, in memory and in PostGIS |
| Apache Spark | `backends/spark.py`: Spark + Sedona spatial join pipeline | Every count equals a plain Python pass |
| FastAPI | `api.py`: typed bodies, OpenAPI at `/docs` | 400 fuzzed requests, none returned a 500 |
| MapLibre | `ui/index.html` | Driven in headless Chromium: no JS errors, light/dark, phone width |

Neo4j could not be downloaded in the build environment. The spec explicitly allows an equivalent graph layer integrated into storage, so AGE was used. It uses the same query language (Cypher) and lives in the same PostgreSQL database as the observations.

### 2.3 Canonical data model (spec §25)

| Question in spec §25 | Answer |
|---|---|
| What is an Observation? | An immutable record: `observation_id, entity_id, event_type, location, timestamp, duration, provenance, confidence ∈ [0,1], spatial_uncertainty_m, attributes, parent_observation` |
| How are locations represented? | WGS84 lat/lon points. Distances are great-circle on a sphere (R = 6,371,008.8 m), so they are identical in Python and PostGIS. |
| How is time represented? | Timezone-aware UTC instants. Naive timestamps are rejected. |
| How are relationships represented? | Typed graph edges with first/last-seen times and supporting observation ids |
| How is provenance stored? | Source, processing model, inputs and ingest time on every observation, plus a parent chain |
| How is uncertainty represented? | ± metres and confidence. Variance = σ² / confidence, used by fusion. |
| How are partitions created? | A KD tree over a data sample, recursively split at medians, so dense areas get more, smaller partitions |

---

## 3. Method

- **Correctness first.** Every query result is compared with a brute-force scan, or with the other engine in E7. A fast but wrong configuration would be reported as wrong. None were.
- **Datasets.**
  - *Uber:* NYC TLC FOIA release via FiveThirtyEight, April–September 2014. Real GPS points to 4 decimal places (~11 m). The 5 dispatch bases act as 5 independent sources. The 1M run uses the first million records (April to mid-May).
  - *Synthetic:* uniform, hotspot (90% in five city-sized clusters) and mixed.
- **Queries** are placed near real records, because that's where a user would ask. Radii are scaled to the dataset: 500 m is local in Manhattan.
- **E3 was run with nothing else on the machine.** An earlier E3 run was discarded because other jobs shared the CPU.
- **Latency** is reported as the median (p50), with p95/p99 where relevant.

---

## 4. Results (1M real Uber pickups)

### E1: Dataset scaling

| Observations | Ingest/s | Radius 500 m p50 | 1-day window p50 | Place+time p50 | Bytes/obs |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 119,568 | 0.38 ms | 0.06 ms | 0.32 ms | 251 |
| 100,000 | 44,397 | 6.07 ms | 0.23 ms | 4.91 ms | 285 |
| 1,000,000 | 12,414 | 70.9 ms | 4.71 ms | 61.8 ms | – |

Query time grows with the size of the answer, not the size of the store. Over Manhattan, 100× more pickups means about 100× more pickups inside every 500 m circle. The index keeps the overhead small; the cost is producing the results. Memory is about 250–285 bytes per observation. Ingest slows at 1M, which reflects Python object allocation and garbage collection.

### E2: Spatial index comparison

| Index | Build s | Index MB | Radius 200 m | Radius 5 km | 10-NN | Polygon |
|---|---:|---:|---:|---:|---:|---:|
| Scan (no index) | 66 | 69 | 89.9 ms | **1,370 ms** | 88.5 ms | **1,290 ms** |
| Grid 0.01° | 66 | 8.7 | 88.3 ms | 2,205 ms | 87.6 ms | 2,169 ms |
| Grid 1° | 65 | 8.1 | 1,352 ms | 1,820 ms | 1,356 ms | 1,731 ms |
| Geohash p6 | 71 | 8.7 | 62.9 ms | 2,182 ms | 67.8 ms | 2,265 ms |
| **QuadTree** | 81 | 105 | **11.7 ms** | 2,515 ms | **6.0 ms** | 2,575 ms |

- **Selective queries:** the quadtree is 5–8× faster than any fixed-cell index and 15× faster than k-nearest with no index. Fixed cells are sized for average density, and a 1 km cell in Midtown holds tens of thousands of pickups.
- **Unselective queries:** a 5 km circle in Manhattan contains about 76% of all records, and there **the plain scan wins**. Index overhead buys nothing when most of the data is returned. This is the classic selectivity crossover, and it argues for a planner that can fall back to scanning.
- **Contrast with synthetic data:** on 100K uniform-ish records the 0.01° grid was close to the quadtree (0.09 vs 0.05 ms). Benchmarks on uniform data would have hidden this effect.

### E3: Distributed scaling, real OS processes

200,000 pickups, 5,000 radius queries (0.5–3 km) per run, wall clock, best of 3, 4 cores, machine otherwise idle.

| Workers | Wall s | Queries/s | Speedup | Efficiency | Ingest/s |
|---:|---:|---:|---:|---:|---:|
| 1 | 494.1 | 10.1 | 1.00× | 100% | 17,291 |
| 2 | 250.8 | 19.9 | 1.97× | 98% | 25,517 |
| 4 | 123.3 | 40.6 | **4.01×** | **100%** | 37,316 |
| 8 | 107.8 | 46.4 | 4.58× | 57% | 34,184 |

Speedup is essentially linear up to the core count. KD partitioning keeps partitions balanced (imbalance 1.03–1.12), so no worker becomes the straggler. With 8 workers on 4 cores the gain is small (from 4.01× to 4.58×). The extra workers mainly smooth out load imbalance.

**E3M (modelled fan-out, 5 km queries):** with 4 → 32 partitions, a query touches 3.7 → 24.3 partitions and sends 7.3 → 48.6 messages. Modelled speedup reaches 5.9× at 8 workers. Finer partitioning raises parallelism and network fan-out together. That trade-off is the core of RQ1.

### E4: Streaming with concurrent queries

| Offered/s | Achieved/s | Dropped | Max backlog | End-to-end p50 / p99 | Query p50 under load |
|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 0 | 1 | 0.41 / 0.82 ms | 0.10 ms |
| 1,000 | 1,000 | 0 | 24 | 0.37 / 1.30 ms | 0.29 ms |
| 10,000 | 9,289 | 0 | 1,029 | 0.70 / 42.0 ms | 1.03 ms |

At 10,000 events/s, one Python consumer runs at about 93% of the offered rate. The backlog absorbs bursts, nothing is lost, and queries stay around a millisecond. The p99 tail (42 ms) is queueing behind batches. On the real Kafka path, offsets are committed only after a batch is stored, and idempotent sinks turn at-least-once delivery into exactly-once storage. Tests verify this with injected crashes.

### E5: Spatial skew (16 partitions)

| Data | Partitioner | Imbalance (max/mean) | Largest | Smallest | Empty | Rows on busiest node, hotspot query |
|---|---|---:|---:|---:|---:|---:|
| Real Uber | Grid 0.283° | **14.68×** | 183,534 | 0 | **9** | 183,534 |
| Real Uber | KD (adaptive) | **1.12×** | 14,038 | 10,938 | 0 | **13,729** |
| Synthetic hotspot (100K) | Grid 10° | 3.01× | 9,405 | 275 | 0 | 9,283 |
| Synthetic hotspot (100K) | KD (adaptive) | 1.12× | 3,500 | 2,786 | 0 | 3,303 |

Real data is far more skewed than the synthetic hotspot model: 14.7× vs 3.0× on the grid. Adaptive partitioning brings both to 1.12×. A hotspot query then spreads over about 6 partitions instead of 1, and the busiest node does 13× less work. That's the property that lets E3 scale.

### E6: Agent retrieval (100 questions over 100,000 pickups)

Questions take the form "pickups from base X within r of here during this period".

| Method | Precision | Recall | Latency p50 |
|---|---:|---:|---:|
| **GeoMemory place + time + source** | **1.0000** | **1.0000** | 4.56 ms |
| Keyword baseline (match base name, top 1,000) | 0.0029 | 0.2605 | – |

Keyword retrieval ignores where and when, so 99.7% of what it returns is irrelevant to a location-aware question, and it still misses 74% of the relevant records. Every GeoMemory result also carries its source, confidence and ± metres, so an agent can cite the evidence behind each claim (spec §18.12, provenance accuracy).

### E7: In-memory engine vs PostGIS (1M pickups)

| Backend | Ingest/s | Storage | Radius 200 m | Radius 5 km | 10-NN | Place+time | Answers agree |
|---|---:|---:|---:|---:|---:|---:|:---:|
| GeoMemory in-memory (QuadTree) | 11,921 | ~270 MB RAM | 12.8 ms | 2,631 ms | 4.9 ms | 78 ms | yes |
| PostGIS 3.4 (GiST geography), via Python client | **31,030** | 453 MB disk | 60.3 ms | 19,023 ms | 4.6 ms | 445 ms | yes |

These are end-to-end timings from Python, and the PostGIS columns include turning every row into a Python `Observation`. That conversion dominates large answers. For the 5 km query (759,117 rows), measured separately:

| Same 5 km query on PostGIS | Time |
|---|---:|
| Server-side `count(*)` | 545 ms |
| Fetching ids only | 678 ms |
| Fetching full Python objects | 19,909 ms |

So PostGIS's spatial engine is about 4× faster than the Python engine on large scans, and the bottleneck is the client. Takeaways:
- **Ingest:** PostGIS's COPY loads 2.6× faster.
- **k-nearest:** both are about 5 ms.
- **Selective queries:** the in-memory engine wins where round-trip and conversion overhead matter most.
- **Fix for production:** return ids or aggregates from SQL and build full objects only for what the agent actually reads.

### Spark + Sedona batch pipeline (all 4.5M pickups)

| | |
|---|---|
| Rows read / valid / rejected | 4,534,327 / 4,534,327 / 0 |
| Zones (0.01° grid, ~1 km) | 71,424 |
| Spatial join operator chosen by Sedona | BroadcastIndexJoin (distributed, R-tree indexed) |
| Pickups matched to exactly one zone | 4,534,327 (100%) |
| Hourly zone × base rows → PostGIS | 1,326,020 |
| Wall time (4 local cores) | 27.7 s |
| Busiest zone | Midtown around 40.75 N, 73.98 W: 222,857 pickups |

The zone grid is offset by half a coordinate step (0.00005°). The source rounds coordinates to 4 decimals, so an unshifted 0.01° grid would put thousands of pickups exactly on zone edges, where `ST_Contains` is false for every zone. Tests compare every count with a plain Python pass.

---

## 5. Research questions (spec §27)

**RQ1. How does spatial partitioning affect distributed query performance?**
It decides whether the system scales. On real data, a fixed grid left 9 of 16 partitions empty and one at 14.7× the average load (E5). KD partitioning gave 1.12× and near-linear speedup (4.01× on 4 cores, E3). The price is fan-out: finer partitions mean more partitions touched and more messages per query (3.7 → 24.3 partitions, E3M). Partition count should match worker count, not exceed it greatly.

**RQ2. How can spatial and temporal indexes be combined efficiently?**
With a planner that counts the time window exactly and cheaply (two binary searches), then walks spatial candidates lazily and abandons them once they outnumber the time window. Cost becomes about min(spatial, temporal) instead of spatial + temporal. On 1.8M pickups, a 9 km × 3 h query went from 7.2 s to 43 ms with identical answers. E2 adds a second rule: below about 20–30% selectivity, a plain scan beats any index.

**RQ3. How can continuously arriving observations be merged with historical memory?**
By making ingest idempotent (stable observation ids, `ON CONFLICT DO NOTHING`) and committing stream offsets only after storage. Duplicates, out-of-order arrival and crashes then can't corrupt history. Verified on a real Kafka broker into memory and into PostGIS. E4 shows 10,000 events/s with zero drops while queries stay around 1 ms.

**RQ4. How can conflicting observations be represented without losing source-level evidence?**
Never overwrite. Every observation stays as recorded. Fusion is a derived view: an inverse-variance weighted mean on the sphere, starting from the observation most consistent with the others. It reports an estimate, an error margin, supporting ids and outlier ids. Tested on random scenes with up to 40% gross outliers: every outlier flagged, estimate within 15 m. The stated error was calibrated (≥93% of truths inside 2.5σ). The demo flood shows a citizen report flagged, not deleted.

**RQ5. How can provenance and uncertainty be incorporated into spatial retrieval?**
Every tool response carries observation ids, sources, processing model, confidence and ± metres. `evidence` returns the full parent chain, via a recursive CTE in PostGIS. Uncertainty enters retrieval in three places: through fusion weights, through `min_confidence` filters, and when deciding whether an entity really moved between snapshots (movement must exceed both stated uncertainties).

**RQ6. How efficiently can agents retrieve relevant spatial-temporal context?**
Exactly and quickly: precision and recall 1.00 at a 4.6 ms median over 100K pickups, versus 0.003 precision for keyword retrieval (E6). Every place-and-time scenario in the 1.8M-pickup UI demo answers in under 0.3 s.

**RQ7. How does performance scale from millions toward hundreds of millions?**
Measured up to 4.5M real records. Query cost tracks answer size (E1). Throughput scales linearly with workers up to the core count (E3). The distributed batch tier processes all 4.5M in 27.7 s (Spark). Beyond this, the limit is single-node memory (~270 bytes per observation in Python, or ~450 bytes per row in PostGIS with indexes). The path forward is the one the project already implements: KD-partitioned workers on more machines, PostGIS/Sedona for storage and batch, and ids instead of full objects on wide queries (E7).

---

## 6. Bugs that testing found

The randomized and adversarial tests found real defects. All are fixed and covered by regression tests:

| Found by | Defect | Fix |
|---|---|---|
| Garbage-record test | A bad timestamp crashed the stream processor | Parse errors become dead-lettered records |
| Random outlier scenes | One strong outlier dragged the starting estimate so far that nothing was rejected | Robust start (weighted medoid) that accounts for the medoid's own uncertainty |
| Concurrent HTTP test | The stdlib server dropped bursts (listen backlog of 5) | Backlog raised to 256 |
| Oversized-request test | A 413 reply reset the connection | Drain the body before replying |
| Real Uber data | Place+time queries always ran the full spatial search first | Adaptive planner: 7.2 s → 43 ms |
| PostGIS round-trip test | Confidence stored as 32-bit `real` lost precision | `double precision` |
| Kafka test | Idle timer started before the consumer had partitions assigned | Idle clock starts at the first message |
| Spark count test | Pickups on zone edges would be dropped by the spatial join | Grid offset by half a coordinate step |
| 1M run | E3 copied tens of millions of ids between processes | Count-only batch mode |
| Benchmark bookkeeping | `--keep` was silently not applied, and results were overwritten | Fixed and tested. Lost experiments re-run, E1–E2 restored from git. |

---

## 7. Limitations and threats to validity

- **One machine.** Distribution is real OS processes on 4 cores, not a multi-machine cluster. Network latency is modelled (E3M), not measured. Kafka is a single broker and Spark runs in local mode.
- **Python engine.** Absolute latencies reflect CPython. The comparisons (index vs index, workers vs workers, planner vs planner) are the transferable results.
- **E7 measures PostGIS through a Python client.** Server-side timings are reported separately for the largest query.
- **Data coverage.** The 1M run uses April to mid-May 2014. Uber coordinates are rounded to about 11 m. The trust-layer stories (machines, flood, panels) use a synthetic world because no public dataset has labelled conflicting sources.
- **Neo4j replaced by Apache AGE** (same query language, allowed by spec §24).
- **Not measured:** 100M+ records, and failure recovery time under load. Failover correctness is tested.

---

## 8. Reproduce

```bash
python3 -m unittest discover -s tests          # 115 tests; backend tests skip if the service is absent
python3 -m geomemory.datasets download         # Uber + earthquakes into data/raw/
scripts/kafka-dev.sh                           # optional: Kafka for the streaming tests
scripts/run-all-benchmarks.sh                  # Spark pipeline + Uber 1M (E1–E7) + synthetic 100K
python3 -m geomemory.demo --dataset uber       # UI on 1.8M real pickups: http://127.0.0.1:8765
python3 -m geomemory.api --backend postgis     # FastAPI over PostGIS, docs at /docs
```

Raw numbers: `results/M-uber/results.json` (Uber 1M), `results/results.json` (synthetic 100K), and `results/spark/uber_pipeline.json` (Spark).
