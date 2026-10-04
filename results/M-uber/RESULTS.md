# GeoMemory benchmark results

Dataset `uber`, scale `M`, seed 42, Python 3.11.15, x86_64, run 2026-10-04T11:19:16+00:00.

Latencies are milliseconds. "Correct" means the answer equals a brute-force scan.

## E1: dataset scaling (QuadTree index)

| Observations | Ingest/s | Radius 500 m p50 | p95 | 1-day window p50 | Place+time p50 | Memory MB | Bytes/obs | Correct |
|---|---|---|---|---|---|---|---|---|
| 10,000 | 119,567.76 | 0.38 | 0.99 | 0.06 | 0.32 | 2.39 | 250.95 | yes |
| 100,000 | 44,396.67 | 6.07 | 14.19 | 0.23 | 4.91 | 27.17 | 284.90 | yes |
| 1,000,000 | 12,413.59 | 70.93 | 168.68 | 4.71 | 61.82 | – | – | yes |
