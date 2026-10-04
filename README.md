# GeoMemory

A distributed spatial-temporal memory and provenance engine for AI agents.
The full specification is in [docs/SPECIFICATION.md](docs/SPECIFICATION.md).

## Status: Phases 1–3 done

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

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Every query is checked against a scan of every record, across many random seeds, cluster shapes and partitioners. The tests also cover:
- the ±180° longitude line, the poles and concave polygons
- injected node failures and losing every copy of a partition
- a messy stream: duplicates, shuffled order, invalid records and random crashes, which must still store each valid record exactly once
- hotspot balance, where the adaptive partitioner must beat the grid

## Next phases

Phase 4 adds uncertainty fusion, historical reasoning and pattern detection. Phase 5 adds the agent API (FastAPI).
