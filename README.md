# GeoMemory

A distributed spatial-temporal memory and provenance engine for AI agents.
The full specification is in [docs/SPECIFICATION.md](docs/SPECIFICATION.md).

## Status: Phase 1 (core data layer)

This repository currently has a single-node reference implementation of the core data layer (spec §26, Phase 1). It uses only the Python standard library.

- `geomemory/model.py`: canonical `Observation`, `Point`, and `Provenance` types, with validation, confidence, and spatial uncertainty
- `geomemory/index.py`: a pluggable `SpatialIndex` interface (grid baseline plus a geohash encoder), and a sorted `TemporalIndex`
- `geomemory/store.py`: the `GeoMemory` store, with radius, k-nearest, polygon, time-range, spatio-temporal, entity-history, lineage, and agent evidence queries

Run the tests:

```bash
python3 -m unittest discover -s tests -v
```

## Next phases

Phase 2 adds streaming ingestion (Kafka), distributed processing (Spark), and spatial partitioning. Phase 3 adds the graph and relationship layer. Phase 4 adds uncertainty reasoning and pattern detection. Phase 5 adds the agent API (FastAPI).
