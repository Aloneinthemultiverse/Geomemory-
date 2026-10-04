#!/usr/bin/env bash
# Run the full evaluation sequentially on an otherwise idle machine
# (parallel-speedup numbers are meaningless if other jobs share the CPU).
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== Spark + Sedona pipeline, all 4.5M Uber pickups -> PostGIS"
python3 -m geomemory.backends.spark --months all --to-postgis --out data/spark_out \
  --stats results/spark/uber_pipeline.json > results/spark/run.log 2>&1
echo "== Uber 1M: E3M E4 E5 E6 E7 (keeping E1 E2), then E3 alone"
python3 -m geomemory.bench --dataset uber --scale M --out results/M-uber --keep \
  --experiments E3M,E4,E5,E6,E7
python3 -m geomemory.bench --dataset uber --scale M --out results/M-uber --keep --experiments E3
echo "== Synthetic 100K: everything"
python3 -m geomemory.bench --scale S --out results
echo "== done"
