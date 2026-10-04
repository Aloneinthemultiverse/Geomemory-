#!/usr/bin/env bash
# One-shot setup for macOS / Linux (and Windows via WSL or Git Bash).
#   scripts/setup.sh            # Python env + data + Docker backends + tests
#   scripts/setup.sh --core     # Python env only (demo, benchmarks; no Docker)
set -euo pipefail
cd "$(dirname "$0")/.."
CORE_ONLY=0; [ "${1:-}" = "--core" ] && CORE_ONLY=1

say() { printf "\n\033[1m== %s\033[0m\n" "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1. $2" >&2; exit 1; }; }

PY=python3; command -v python3 >/dev/null || PY=python
need "$PY" "Install Python 3.10+ from https://www.python.org/downloads/"
"$PY" -c 'import sys; assert sys.version_info >= (3, 10), "need Python 3.10+"'

say "Python virtual environment (.venv)"
[ -d .venv ] || "$PY" -m venv .venv
# shellcheck disable=SC1091
if [ -f .venv/bin/activate ]; then . .venv/bin/activate; else . .venv/Scripts/activate; fi
python -m pip install -q --upgrade pip
if [ "$CORE_ONLY" = 1 ]; then
  python -m pip install -q -e ".[api]"
else
  python -m pip install -q -e ".[all]"
fi

say "Real datasets (~200 MB, one time)"
python -m geomemory.datasets download

if [ "$CORE_ONLY" = 0 ]; then
  need docker "Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
  say "Database (PostGIS + AGE) and Kafka in Docker"
  docker compose up -d --build db kafka
  for _ in $(seq 60); do
    db=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q db)")
    kf=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q kafka)")
    [ "$db" = healthy ] && [ "$kf" = healthy ] && break
    sleep 3
  done
  docker compose ps
fi

say "Tests"
python -m unittest discover -s tests 2>&1 | tail -3

say "Done"
cat <<MSG
Activate the environment in new terminals:   . .venv/bin/activate
Demo UI (synthetic world):                   python -m geomemory.demo          -> http://127.0.0.1:8765
Demo UI (1.8M real Uber pickups):            python -m geomemory.demo --dataset uber
FastAPI over PostGIS (docs at /docs):        python -m geomemory.api --backend postgis
Benchmarks:                                  python -m geomemory.bench --scale S
MSG
