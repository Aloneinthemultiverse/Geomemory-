"""Production backends behind the same interfaces as the in-memory engine.

postgis   PostGISStore: observations in PostgreSQL/PostGIS (spec §24)
graph     AgeGraph: relationship graph in Apache AGE (openCypher on PostgreSQL)
kafka     Kafka ingestion with committed offsets and idempotent sinks
spark     Spark + Apache Sedona batch pipeline (distributed spatial join)

Each module imports its driver lazily so the core package needs none of them.
"""
import os

DEFAULT_DSN = os.environ.get("GEOMEMORY_PG_DSN",
                             "postgresql://geomemory:geomemory@localhost:5432/geomemory")
DEFAULT_KAFKA = os.environ.get("GEOMEMORY_KAFKA", "localhost:9092")
