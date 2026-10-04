# GeoMemory

A distributed spatial-temporal memory and provenance engine for AI agents.
The full specification is in [docs/SPECIFICATION.md](docs/SPECIFICATION.md). The **evaluation report**, with every experiment on 1M real NYC Uber pickups and answers to the research questions, is in [docs/REPORT.md](docs/REPORT.md).

## Run it (one command)

```bash
git clone -b ccr-bef85d66-vw1qxq https://github.com/aloneinthemultiverse/geomemory-.git geomemory && cd geomemory
./run.sh                 # opens http://127.0.0.1:8765. Windows: powershell -ExecutionPolicy Bypass -File run.ps1
```

You only need Python 3.10 or newer. The first run sets itself up (about a minute); after that it starts in under a second.

| Command | What you get |
|---|---|
| `./run.sh` | Map UI with the demo world (factory, solar farm, flood) |
| `./run.sh uber` | The same UI over 1.8M real NYC Uber pickups (downloads ~200 MB once) |
| `./run.sh full` | Everything: PostGIS, AGE and Kafka in Docker, all datasets, the full test suite, then the UI |

Live demo: <https://project-osn64.vercel.app>

For the full stack set up by hand (Docker, Spark), see `scripts/setup.sh`.

Step-by-step instructions for Windows, macOS and Linux, with troubleshooting, are in [docs/SETUP.md](docs/SETUP.md). If you only want to run everything in Docker: `docker compose up -d --build`.

## Try the UI

```bash
python3 -m geomemory.demo          # open http://127.0.0.1:8765
```

This loads a demo world around Coimbatore. It has a factory where Machine_47 fails, a solar farm with panels getting worse, delivery trucks, and a flood reported by sources that disagree. Use the map dashboard to:
- **Explore:** pick a spot on the map and a time window, then ask what happened there or what changed. You can also ask what happened before an event, or combine conflicting sources into one location. Every result shows its source and confidence, and clicking a result opens its full provenance chain.
- **Benchmarks:** view the E1–E6 results as charts and tables. Switch between scales.

**On real data:** `python3 -m geomemory.demo --dataset uber` loads 1.8M real NYC Uber pickups (July and September 2014). Its scenarios include:
- Times Square's weekly rhythm
- Brooklyn Heights on July 4th (near the fireworks, pickups were 2.7× a normal Friday)
- JFK pickups split by dispatch base
- Saturday night hotspots

Every query answers in under 0.3 s.

It uses MapLibre and OpenFreeMap tiles (no API key needed), and has light and dark mode. It works on phone-sized screens.

## Status: Phases 1–5 done

The core engine is plain Python. Its only dependency is `tzdata`, the time-zone database. The production backends are optional extras.

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

**Real datasets** (`geomemory/datasets.py`):

```bash
python3 -m geomemory.datasets download                        # ~200 MB into data/raw/ (git-ignored)
python3 -m geomemory.bench --dataset uber --scale M --out results/M-uber
```

| Dataset | Records | Why it is useful |
|---|---|---|
| `uber`: NYC Uber pickups, Apr–Sep 2014 (NYC TLC, via FiveThirtyEight) | 4,534,327 | Real GPS points with real hotspots (Manhattan, airports). The 5 dispatch bases act as 5 independent sources. |
| `quakes`: significant earthquakes, 1965–2016 (USGS) | 23,412 | Worldwide, including the ±180° line and polar regions, over 50 years |

Query sizes adapt to each dataset: 500 m is local in Manhattan, while 100 km is local for earthquakes.

## Production backends (spec §24)

The same interfaces run on real infrastructure. Each backend is optional, and its tests skip when it isn't available.

| Spec component | Implementation | Module | Verified by |
|---|---|---|---|
| Spatial DB: PostgreSQL + PostGIS | `PostGISStore`: COPY bulk load, GiST geography index, recursive-CTE lineage. Drop-in replacement for `GeoMemory`. | `backends/postgis.py` | Same answers as brute force, including the ±180° line and the poles |
| Graph layer (Neo4j or equivalent) | `AgeGraph`: Apache AGE, openCypher inside PostgreSQL | `backends/graph.py` | Same as the in-memory graph on neighbours, paths and evidence. Injection-safe. |
| Ingestion: Apache Kafka | `KafkaStreamProcessor`: manual commits, dead-letter topic, exactly-once storage through idempotent sinks | `backends/kafka.py` | Real broker; duplicates, garbage and injected crashes, into memory and into PostGIS |
| Distributed processing: Apache Spark | Spark + Apache Sedona batch pipeline: validate, spatial join to zones, hourly counts | `backends/spark.py` | Every count equals a plain Python pass |
| Backend: FastAPI | Typed tool endpoints and OpenAPI docs at `/docs`. Serves the web UI. | `api.py` | Type errors return 422 and bad values return 400; fuzzed without a single 500 |

```bash
apt install postgresql-16-postgis-3 postgresql-16-age      # or any PostGIS + AGE install
pip install "psycopg[binary]" fastapi uvicorn confluent-kafka pyspark==3.5.3 apache-sedona==1.6.1
export GEOMEMORY_PG_DSN=postgresql://geomemory:geomemory@localhost:5432/geomemory
scripts/kafka-dev.sh                                        # single-node Kafka (KRaft)
python3 -m geomemory.api --backend postgis                  # FastAPI over PostGIS
python3 -m geomemory.backends.spark --months all --to-postgis
scripts/run-all-benchmarks.sh                               # full evaluation, run on an idle machine
```

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

- A multi-machine deployment (the cluster here uses processes on one machine)
- Return ids or aggregates from PostGIS for wide queries (E7: the client, not PostGIS, is the bottleneck)
