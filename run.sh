#!/usr/bin/env bash
# One command to run GeoMemory (macOS / Linux / WSL):
#   ./run.sh            demo world  -> http://127.0.0.1:8765
#   ./run.sh uber       1.8M real NYC Uber pickups (downloads ~200 MB once)
#   ./run.sh full       also starts PostGIS + Kafka in Docker and runs all tests
# First run creates .venv and installs what is needed; later runs start instantly.
set -euo pipefail
export PIP_DISABLE_PIP_VERSION_CHECK=1
cd "$(dirname "$0")"
MODE="${1:-demo}"
PY=python3; command -v python3 >/dev/null || PY=python
command -v "$PY" >/dev/null || { echo "Please install Python 3.10+: https://www.python.org/downloads/"; exit 1; }
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || { echo "Python 3.10+ is required."; exit 1; }

if [ ! -x .venv/bin/python ] && [ ! -x .venv/Scripts/python.exe ]; then
  echo "First run: setting up (about a minute)..."
  "$PY" -m venv .venv
fi
VPY=.venv/bin/python; [ -x "$VPY" ] || VPY=.venv/Scripts/python.exe
if [ "$MODE" = full ]; then
  "$VPY" -m pip install -q --upgrade pip && "$VPY" -m pip install -q -e ".[all]"
  docker compose up -d --build db kafka
  "$VPY" -m geomemory.datasets download
  "$VPY" -m unittest discover -s tests
  exec "$VPY" -m geomemory.api --backend memory --port 8765
fi
"$VPY" -c "import geomemory, tzdata" 2>/dev/null || "$VPY" -m pip install -q -e .
ARGS=(--port 8765)
if [ "$MODE" = uber ]; then
  "$VPY" -m geomemory.datasets download
  ARGS+=(--dataset uber)
fi
URL=http://127.0.0.1:8765
( sleep 3; command -v open >/dev/null && open "$URL" || { command -v xdg-open >/dev/null && xdg-open "$URL"; } ) >/dev/null 2>&1 &
echo "GeoMemory is starting at $URL  (Ctrl+C to stop)"
exec "$VPY" -m geomemory.demo "${ARGS[@]}"
