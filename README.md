# GeoMemory

A distributed spatial-temporal memory and provenance engine for AI agents.
The full specification is in [docs/SPECIFICATION.md](docs/SPECIFICATION.md).

## Try the UI

```bash
python3 -m geomemory.demo          # open http://127.0.0.1:8765
```

This loads a demo world around Coimbatore. It has a factory where Machine_47 fails, a solar farm with panels getting worse, delivery trucks, and a flood reported by sources that disagree. Use the map dashboard to:
- **Explore:** pick a spot on the map and a time window, then ask what happened there or what changed. You can also ask what happened before an event, or combine conflicting sources into one location. Every result shows its source and confidence, and clicking a result opens its full provenance chain.
- **Benchmarks:** view the E1–E6 results as charts and tables. Switch between scales.

It uses MapLibre and OpenFreeMap tiles (no API key needed), and has light and dark mode. It works on phone-sized screens.

## Status: Phases 1–5 done

Everything is in plain Python with no outside libraries.

**Phase 1: core data layer**
- `geomemory/model.py`: the `Observation`, `Point` and `Provenance` types, with validation, confidence and spatial uncertainty
- `geomemory/index.py`: a pluggable `SpatialIndex` (grid baseline plus a geohash encoder) and a `TemporalIndex`. Radius searches split correctly across the ±180° longitude line and work near the poles.
- `geomemory/store.py`: the `GeoMemory` store, with radius, k-nearest, polygon, time-range, place-and-time, history, lineage and evidence queries

**Phase 2: big data layer (simulated cluster)**
- `geomemory/partition.py`: `GridPartitioner` (fixed grid, ignores data density) and `KDPartitioner` (adapts to the data, so hotspots are spread out)
- `geomemory/cluster.py`: `Cluster` with replication, queries that fan out to nodes and merge the results, node failure, and recovery that re-copies data from replicas
- `geomemory/stream.py`: a Kafka-like `Broker` (partitioned logs, committed offsets, a backlog limit) and a `StreamProcessor` (validates, sets bad records aside, removes duplicates, reports metrics)

**Phase 3: relationship layer**
- `geomemory/graph.py`: a `RelationshipGraph` whose edges record when they held and which observations support them. `derive_relationships` builds NEAR, INSIDE, OBSERVED_BY, BEFORE and CHANGED_FROM edges.

**Phase 4: trust layer**
- `geomemory/trust.py`: combines conflicting sightings into one best location, with an error estimate. Each source is weighted by its stated accuracy and confidence. The math works on the sphere, so it holds across the ±180° line and near the poles. Outlier sources are flagged but kept.
- Also in `trust.py`: grouping sightings that describe the same event, "what changed here between two times", hotspots of repeated events, assets that keep getting worse, common warning signs before a failure, and similarity between event sequences

**Phase 5: agent layer**
- `geomemory/agent.py`: six JSON tools: `events_near`, `what_changed`, `history_before`, `nearby_entities`, `evidence` and `fused_location`. They work on a single store or the cluster. Bad input returns an error message and never crashes.
- `serve()` runs an HTTP API with no outside libraries: `GET /tools`, `POST /tools/<name>`
- Retrieval scoring (precision/recall) plus a keyword-search baseline for experiment E6

**Benchmarks**
- `geomemory/bench.py` runs experiments E1–E6 from spec §20 on generated data: uniform, hotspot or mixed
- New index types to compare: `ScanIndex` (no index), `GeohashIndex` and `QuadTreeIndex`
- Every result is also checked against a full scan; a wrong answer is marked **NO**

```bash
python3 -m geomemory.bench --scale S --out results   # ~1 min; scales: tiny, S, M (up to 1M)
python3 -m geomemory.bench --experiments E2,E5       # run a subset
```

Latest results: [results/RESULTS.md](results/RESULTS.md).

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Every query is checked against a scan of every record, across many random seeds, cluster shapes and partitioners. The tests also cover:
- the ±180° longitude line, the poles and concave polygons
- injected node failures and losing every copy of a partition
- a messy stream: duplicates, shuffled order, invalid records and random crashes, which must still store each valid record exactly once
- hotspot balance, where the adaptive partitioner must beat the grid
- combining locations on 200+ random scenes: it must beat the typical single source, flag every outlier, and its stated error must match reality
- 3,000 random garbage tool calls, plus concurrent and malformed HTTP requests
- agent search: GeoMemory must find every right record and nothing else, while keyword search scores under 50% precision

## Next steps

- Swap the simulated parts for real backends: Kafka, Spark or Sedona, PostGIS, and a graph store
- Real multi-process workers, so the node-scaling test (E3) shows actual speedup
