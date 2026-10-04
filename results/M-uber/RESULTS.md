# GeoMemory benchmark results

Dataset `uber`, scale `M`, seed 42, Python 3.11.15, x86_64, run 2026-10-04T11:19:16+00:00.

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
