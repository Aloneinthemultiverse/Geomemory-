# GeoMemory benchmark results

Dataset `uber`, scale `M`, seed 42, Python 3.11.15, x86_64, run 2026-10-04T13:12:01+00:00.

Latencies are milliseconds. "Correct" means the answer equals a brute-force scan.

## E1: dataset scaling (QuadTree index)

| Observations | Ingest/s | Radius 500 m p50 | p95 | 1-day window p50 | Place+time p50 | Memory MB | Bytes/obs | Correct |
|---|---|---|---|---|---|---|---|---|
| 10,000 | 119,567.76 | 0.38 | 0.99 | 0.06 | 0.32 | 2.39 | 250.95 | yes |
| 100,000 | 44,396.67 | 6.07 | 14.19 | 0.23 | 4.91 | 27.17 | 284.90 | yes |
| 1,000,000 | 12,413.59 | 70.93 | 168.68 | 4.71 | 61.82 | – | – | yes |

## E2: spatial index comparison (1,000,000 observations)

| Index | Build s | Index MB | Radius 200 m p50 | Radius 5000 m p50 | 10-NN p50 | Polygon p50 | Correct |
|---|---|---|---|---|---|---|---|
| Scan (no index) | 66.15 | 69.09 | 89.90 | 1,370.24 | 88.52 | 1,290.49 | yes |
| Grid 0.01° | 66.15 | 8.74 | 88.33 | 2,204.91 | 87.64 | 2,169.42 | yes |
| Grid 1° | 65.33 | 8.12 | 1,351.72 | 1,820.16 | 1,355.89 | 1,730.96 | yes |
| Geohash p6 | 70.69 | 8.71 | 62.85 | 2,182.21 | 67.82 | 2,265.03 | yes |
| QuadTree | 81.18 | 105.37 | 11.72 | 2,514.55 | 6.01 | 2,574.80 | yes |

## E3: distributed scaling, real processes (200,000 observations, batch of 5,000 radius queries, 4 CPU cores)

Each worker is a separate OS process. Wall-clock time, best of 3. Rows with more workers than cores are oversubscribed and cannot speed up further.

| Workers | Wall s | Queries/s | Speedup | Efficiency | Ingest/s | Partition imbalance | Correct |
|---|---|---|---|---|---|---|---|
| 1 | 494.08 | 10.12 | 1.00 | 1.00 | 17,290.94 | 1.03 | yes |
| 2 | 250.84 | 19.93 | 1.97 | 0.98 | 25,517.46 | 1.08 | yes |
| 4 | 123.31 | 40.55 | 4.01 | 1.00 | 37,316.39 | 1.12 | yes |
| 8 | 107.80 | 46.38 | 4.58 | 0.57 | 34,183.85 | 1.30 | yes |

## E3M: distributed scaling, modeled (200,000 observations, radius 5 km)

Simulated in one process: time is *modeled* as the slowest node per query.

| Workers | Partitions | Modeled time s | Speedup | Efficiency | Partitions touched | Messages/query | Max node share | Correct |
|---|---|---|---|---|---|---|---|---|
| 1 | 4 | 35.25 | 1.00 | 1.00 | 3.65 | 7.30 | 1.00 | yes |
| 2 | 8 | 21.10 | 1.67 | 0.84 | 6.63 | 13.26 | 0.50 | yes |
| 4 | 16 | 10.79 | 3.27 | 0.82 | 13.11 | 26.22 | 0.26 | yes |
| 8 | 32 | 6.02 | 5.86 | 0.73 | 24.30 | 48.60 | 0.13 | yes |

## E4: streaming throughput (with concurrent queries)

| Offered/s | Achieved/s | Stored | Dropped | Max backlog | E2E p50 | E2E p99 | Query p50 under load | Correct |
|---|---|---|---|---|---|---|---|---|
| 100 | 100.09 | 500 | 0 | 1 | 0.41 | 0.82 | 0.10 | yes |
| 1,000 | 999.57 | 5,000 | 0 | 24 | 0.37 | 1.30 | 0.29 | yes |
| 10,000 | 9,288.87 | 50,000 | 0 | 1,029 | 0.70 | 42.00 | 1.03 | yes |

## E5: spatial skew (16 partitions)

| Data | Partitioner | Imbalance (max/mean) | Largest | Smallest | Empty | Hotspot query: partitions | Hotspot query: rows on busiest |
|---|---|---|---|---|---|---|---|
| real | Grid 0.283° | 14.68 | 183,534 | 0 | 9 | 1.40 | 183,534.00 |
| real | KD (adaptive) | 1.12 | 14,038 | 10,938 | 0 | 6.00 | 13,728.80 |

## E6: agent retrieval (100 queries over 100,000 observations)

| Method | Precision | Recall | Latency p50 |
|---|---|---|---|
| GeoMemory spatial-temporal | 1.0000 | 1.0000 | 4.56 |
| Keyword baseline | 0.0029 | 0.2605 | – |

## E7: in-memory engine vs PostGIS (1,000,000 observations)

Same data, same queries. Answers are cross-checked between the two.

| Backend | Ingest/s | Storage MB | Radius 200 m p50 | Radius 5000 m p50 | 10-NN p50 | Place+time p50 | Polygon p50 | Answers agree |
|---|---|---|---|---|---|---|---|---|
| GeoMemory in-memory (QuadTree) | 11,921.38 | – | 12.80 | 2,631.21 | 4.94 | 78.24 | 2,477.71 | yes |
| PostGIS 3.4 (GiST, geography) | 31,030.22 | 453.10 | 60.32 | 19,022.83 | 4.55 | 444.73 | 19,825.82 | yes |
