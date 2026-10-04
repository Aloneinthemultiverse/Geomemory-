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
| `./run.sh` | The map UI: a 3D replay of NYC, plain-English questions, a crash test, and a guided tour (no download needed) |
| `./run.sh uber` | The same UI over 1.8M real NYC Uber pickups (downloads ~200 MB once) |
| `./run.sh full` | Everything: PostGIS, AGE and Kafka in Docker, all datasets, the full test suite, then the UI |

Live demo: <https://project-osn64.vercel.app>

For the full stack set up by hand (Docker, Spark), see `scripts/setup.sh`.

Step-by-step instructions for Windows, macOS and Linux, with troubleshooting, are in [docs/SETUP.md](docs/SETUP.md). If you only want to run everything in Docker: `docker compose up -d --build`.

## Try the UI

```bash
python3 -m geomemory.demo          # open http://127.0.0.1:8765
```

Press **▶ Take the 60-second tour** for a guided walk-through. Here's what you can do:

- **Watch NYC breathe.** A 3D time-lapse of a typical week in New York, built from all 4.5M real Uber pickups (Apr–Sep 2014). Each column is a ~650 m block, and its height is the number of rides starting there in that hour. Press play, or jump to Monday rush hour or Friday night.
- **Ask in plain English.** For example, "How busy was Times Square on July 4th?" or "Which companies picked people up at JFK over the weekend?". Every answer shows the original records behind it, and clicking one shows where it came from.
  - By default a small built-in reader turns the question into a search.
  - Set `ANTHROPIC_API_KEY` (optional) and Claude answers instead, calling GeoMemory's tools itself. The key stays in your environment; it is never stored or sent anywhere but the Anthropic API.
- **Speed counter.** Every answer shows how many records were searched and how long it took, usually a few milliseconds.
- **Crash test.** The NYC rides are split over 4 servers, and each piece is stored on 2 of them. Unplug one in the middle of a question and the answer doesn't change, record for record. The page also shows how much would have been lost without the copies.
- **Whom to trust.** Four sources report a flood in slightly different places. GeoMemory weights each by its reliability and flags the citizen report that doesn't fit.
- **Benchmarks.** The measured results of experiments E1–E7.

The default data works offline from the repo; nothing is downloaded. It combines:
- a 20% sample of real Uber pickups during July 4th week 2014 (24,804 rides, with their original record ids);
- the replay grid built from all 4.5M pickups;
- a small sensor world near Coimbatore (a factory where Machine_47 fails, a solar farm, a flood).

To rebuild the NYC pack from the raw data, run `python -m geomemory.showcase build`. With `--dataset uber`, the questions run over 1.8M real pickups instead.

It uses MapLibre with Esri gray basemap tiles and deck.gl for the 3D layer (no API keys needed). It has light and dark mode and works on phones.

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
