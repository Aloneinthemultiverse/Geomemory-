"""Vercel entry point: the GeoMemory API + web UI over the synthetic demo
world, with the committed benchmark results. Serverless functions are
stateless and size-limited, so the 1.8M-pickup Uber demo and the
PostGIS/Kafka/Spark backends run locally (see docs/SETUP.md), not here."""
from pathlib import Path

from geomemory.agent import AgentInterface
from geomemory.api import create_app
from geomemory.demo import build_world
from geomemory.index import QuadTreeIndex
from geomemory.store import GeoMemory

_mem = GeoMemory(QuadTreeIndex())
_mem.ingest_many(build_world())
app = create_app(AgentInterface(_mem, "synthetic"),
                 results_dir=str(Path(__file__).parent / "results"))
