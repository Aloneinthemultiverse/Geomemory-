"""Vercel entry point: the GeoMemory API + web UI over the showcase data
(demo world + a 25k sample of real NYC pickups from July 4th week 2014),
with the committed benchmark results. Serverless functions are stateless and
size-limited, so the full 4.5M-pickup dataset and the PostGIS/Kafka/Spark
backends run locally (see docs/SETUP.md), not here."""
from pathlib import Path

from geomemory.agent import AgentInterface
from geomemory.api import create_app
from geomemory.showcase import build_memory

app = create_app(AgentInterface(build_memory("showcase"), "showcase"),
                 results_dir=str(Path(__file__).parent / "results"))
