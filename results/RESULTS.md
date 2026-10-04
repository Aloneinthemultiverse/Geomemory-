# GeoMemory benchmark results

Scale `S`, seed 42, Python 3.11.15, x86_64, run 2026-10-04T10:29:12+00:00.

Latencies are milliseconds. "Correct" means the answer equals a brute-force scan.

## E1: dataset scaling (QuadTree index)

| Observations | Ingest/s | Radius 2 km p50 | p95 | 1-day window p50 | Place+time p50 | Memory MB | Bytes/obs | Correct |
|---|---|---|---|---|---|---|---|---|
| 10,000 | 185,703.59 | 0.05 | 0.15 | 0.01 | 0.05 | 1.47 | 154.56 | yes |
| 100,000 | 42,279.18 | 0.40 | 1.93 | 0.09 | 0.71 | 16.29 | 170.80 | yes |

## E2: spatial index comparison (100,000 observations)

| Index | Build s | Index MB | Radius 500 m p50 | Radius 50 km p50 | 10-NN p50 | Polygon p50 | Correct |
|---|---|---|---|---|---|---|---|
| Scan (no index) | 1.95 | 6.87 | 7.91 | 23.08 | 36.87 | 22.48 | yes |
| Grid 0.01° | 2.17 | 13.45 | 0.09 | 25.98 | 5.39 | 23.26 | yes |
| Grid 1° | 2.09 | 7.05 | 4.79 | 18.65 | 19.14 | 21.61 | yes |
| Geohash p6 | 2.77 | 19.96 | 0.62 | 27.24 | 3.20 | 25.49 | yes |
| QuadTree | 2.55 | 8.63 | 0.05 | 20.81 | 0.22 | 19.59 | yes |

## E3: distributed scaling (50,000 observations, radius 50 km)

Simulated in one process: time is *modeled* as the slowest node per query.

| Workers | Partitions | Modeled time s | Speedup | Efficiency | Partitions touched | Messages/query | Max node share | Correct |
|---|---|---|---|---|---|---|---|---|
| 1 | 4 | 0.24 | 1.00 | 1.00 | 1.10 | 2.20 | 1.00 | yes |
| 2 | 8 | 0.19 | 1.30 | 0.65 | 1.42 | 2.84 | 0.50 | yes |
| 4 | 16 | 0.12 | 1.94 | 0.49 | 2.00 | 4.00 | 0.25 | yes |
| 8 | 32 | 0.08 | 2.93 | 0.37 | 2.88 | 5.76 | 0.13 | yes |

## E4: streaming throughput (with concurrent queries)

| Offered/s | Achieved/s | Stored | Dropped | Max backlog | E2E p50 | E2E p99 | Query p50 under load | Correct |
|---|---|---|---|---|---|---|---|---|
| 100 | 100.14 | 200 | 0 | 1 | 0.37 | 0.73 | 0.04 | yes |
| 1,000 | 997.17 | 2,000 | 0 | 4 | 0.33 | 0.69 | 0.04 | yes |
| 10,000 | 9,996.57 | 20,000 | 0 | 495 | 0.59 | 10.85 | 0.09 | yes |

## E5: spatial skew (16 partitions)

| Data | Partitioner | Imbalance (max/mean) | Largest | Smallest | Empty | Hotspot query: partitions | Hotspot query: rows on busiest |
|---|---|---|---|---|---|---|---|
| uniform | Grid 10° | 1.06 | 3,318 | 2,795 | 0 | 1.00 | 3,205.60 |
| uniform | KD (adaptive) | 1.14 | 3,566 | 2,547 | 0 | 1.00 | 3,005.40 |
| hotspot | Grid 10° | 3.01 | 9,405 | 275 | 0 | 1.00 | 9,283.20 |
| hotspot | KD (adaptive) | 1.12 | 3,500 | 2,786 | 0 | 3.20 | 3,303.40 |

## E6: agent retrieval (50 queries over 20,000 observations)

| Method | Precision | Recall | Latency p50 |
|---|---|---|---|
| GeoMemory spatial-temporal | 1.0000 | 1.0000 | 0.23 |
| Keyword baseline | 0.0006 | 0.7286 | – |
