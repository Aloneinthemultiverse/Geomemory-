# Setting up GeoMemory on your laptop

Pick the level you need. Each level includes the ones above it.

| Level | You get | You need |
|---|---|---|
| **1. Core** | Engine, map UI, benchmarks, real Uber data, about 90 of the tests | Python 3.10+ |
| **2. Full stack** | + PostGIS, Apache AGE graph and Kafka (in Docker), FastAPI, all backend tests | + Docker Desktop |
| **3. Spark** | + the Spark/Sedona batch pipeline | + Java 17 |

Disk: about 1 GB for Python packages, 200 MB for data and 1.5 GB for Docker images. RAM: 8 GB is enough for everything except the full 1.8M-pickup demo, which uses about 3 GB.

---

## 0. Install the prerequisites (once)

| | Windows | macOS | Linux (Ubuntu) |
|---|---|---|---|
| Git | <https://git-scm.com/download/win> | `xcode-select --install` | `sudo apt install git` |
| Python 3.10+ | <https://www.python.org/downloads/>. **Tick "Add python.exe to PATH".** | `brew install python@3.11` | `sudo apt install python3 python3-venv python3-pip` |
| Docker | [Docker Desktop](https://www.docker.com/products/docker-desktop/) (uses WSL 2) | [Docker Desktop](https://www.docker.com/products/docker-desktop/) | [Docker Engine](https://docs.docker.com/engine/install/ubuntu/) |
| Java 17 (Spark only) | [Temurin 17](https://adoptium.net/temurin/releases/?version=17) | `brew install openjdk@17` | `sudo apt install openjdk-17-jre-headless` |

Check them:

```bash
git --version
python --version        # 3.10 or newer ("python3" on macOS/Linux)
docker compose version  # level 2+
java -version           # level 3 only
```

## 1. Get the code

The work is on the branch `ccr-bef85d66-vw1qxq`:

```bash
git clone -b ccr-bef85d66-vw1qxq https://github.com/aloneinthemultiverse/geomemory-.git geomemory
cd geomemory
```

## 2. Quickest: one command

```bash
./run.sh            # macOS / Linux / WSL
```
```powershell
powershell -ExecutionPolicy Bypass -File run.ps1      # Windows
```

This creates `.venv`, installs GeoMemory and opens the map UI. Add `uber` for the real-data demo, or `full` (macOS/Linux) for Docker backends and all tests. Use the setup scripts below to control each step yourself.

## 2b. Full setup script

**macOS / Linux:**
```bash
scripts/setup.sh            # full: Python env, data, Docker backends, tests
scripts/setup.sh --core     # level 1 only (no Docker)
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1          # full
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1 -Core    # level 1 only
```

The script:
1. creates a virtual environment in `.venv`
2. installs GeoMemory with its optional backends
3. downloads the real datasets into `data/raw/`
4. starts PostgreSQL (with PostGIS and AGE) and Kafka in Docker, then waits until they're healthy
5. runs the test suite

Tests for anything that isn't running are skipped, not failed.

## 3. Use it

Activate the environment in every new terminal:
```bash
. .venv/bin/activate              # macOS / Linux
.\.venv\Scripts\Activate.ps1      # Windows
```

| What | Command | Then open |
|---|---|---|
| Map UI, demo world | `python -m geomemory.demo` | <http://127.0.0.1:8765> |
| Map UI, 1.8M real Uber pickups | `python -m geomemory.demo --dataset uber` (about 2 min to load) | <http://127.0.0.1:8765> |
| FastAPI, in memory | `python -m geomemory.api` | <http://127.0.0.1:8000/docs> |
| FastAPI over PostGIS | `python -m geomemory.api --backend postgis` | <http://127.0.0.1:8000> |
| Quick benchmark (~1 min) | `python -m geomemory.bench --scale S` | `results/RESULTS.md` |
| Uber 1M benchmark (~1.5 h) | `python -m geomemory.bench --dataset uber --scale M --out results/M-uber` | `results/M-uber/RESULTS.md` |
| Spark pipeline, 4.5M pickups → PostGIS | `python -m geomemory.backends.spark --months all --to-postgis` | `psql`: `SELECT * FROM zone_hour_counts LIMIT 5;` |
| All tests | `python -m unittest discover -s tests` | |

**Everything in Docker instead.** You don't need Python on the laptop for this:
```bash
docker compose up -d --build      # db + kafka + app; UI at http://localhost:8765
docker compose exec app python -m unittest discover -s tests
docker compose down               # stop (add -v to also delete the database)
```

**Connect to the database yourself.** Use any client (psql, DBeaver, pgAdmin): host `localhost`, port `5432`, user, password and database all `geomemory`.

## 4. Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` (5432, 9092 or 8765) | Another Postgres or Kafka is running. Use other ports: `PG_PORT=5433 KAFKA_PORT=9093 UI_PORT=8766 docker compose up -d`. Then set `GEOMEMORY_PG_DSN=postgresql://geomemory:geomemory@localhost:5433/geomemory` and `GEOMEMORY_KAFKA=localhost:9093`. |
| `python` not found on Windows | Reinstall Python with "Add python.exe to PATH" ticked, or use `py -3.11` |
| PowerShell says scripts are disabled | Use the `powershell -ExecutionPolicy Bypass -File ...` form shown above |
| `ZoneInfoNotFoundError` | `pip install tzdata` (the setup script already does this) |
| Docker: "Cannot connect to the Docker daemon" | Start Docker Desktop and wait until it says "running" |
| Spark: `JAVA_HOME is not set` | Install Java 17 and reopen the terminal. On macOS: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`. |
| The first Spark run is slow | It downloads the Sedona jars (about 50 MB) once and caches them in `~/.ivy2` |
| The map is blank | Map tiles come from openfreemap.org and need internet. Queries and results still work offline. |
| The Uber demo runs out of memory | Load one month: `python -m geomemory.demo --dataset uber --months jul14` |
| Reset the database | `docker compose down -v && docker compose up -d db kafka` |
