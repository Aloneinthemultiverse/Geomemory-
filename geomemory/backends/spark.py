"""Batch pipeline on Apache Spark + Apache Sedona (spec §15, §12.5, §24).

    python -m geomemory.backends.spark --months all --out data/spark_out --to-postgis

Raw Uber CSVs -> parse and validate (NYC local time to UTC) -> Sedona spatial
join of every pickup against a grid of zone polygons -> hourly counts per
zone and dispatch base (a historical hotspot table) -> Parquet, and
optionally PostGIS.

The zone grid is offset by half a coordinate quantum: the source publishes
coordinates to 4 decimals, so an unshifted 0.01° grid would put thousands of
points exactly on zone edges, where ST_Contains is false for every zone.
"""
from __future__ import annotations

import argparse
import math
import time
from pathlib import Path

SEDONA_PACKAGES = ("org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
                   "org.datasyslab:geotools-wrapper:1.6.1-28.2")
QUANTUM = 1e-4  # Uber coordinates are published to 4 decimal places


def spark_session(cores: int | str = "*", memory: str = "4g"):
    from sedona.spark import SedonaContext
    cfg = (SedonaContext.builder().master(f"local[{cores}]").appName("geomemory-batch")
           .config("spark.jars.packages", SEDONA_PACKAGES)
           .config("spark.driver.memory", memory)
           .config("spark.sql.session.timeZone", "UTC")
           .config("spark.sql.shuffle.partitions", "64")
           .config("spark.ui.enabled", "false")
           .getOrCreate())
    cfg.sparkContext.setLogLevel("ERROR")
    return SedonaContext.create(cfg)


def zone_grid(lat_min: float, lat_max: float, lon_min: float, lon_max: float,
              cell_deg: float) -> tuple[float, float, int, int]:
    """Grid origin (shifted off the coordinate lattice) and its size."""
    lat0 = math.floor(lat_min / cell_deg) * cell_deg - QUANTUM / 2
    lon0 = math.floor(lon_min / cell_deg) * cell_deg - QUANTUM / 2
    rows = int(math.ceil((lat_max - lat0) / cell_deg)) + 1
    cols = int(math.ceil((lon_max - lon0) / cell_deg)) + 1
    return lat0, lon0, rows, cols


def run_uber_pipeline(spark, data_dir: Path, months: list[str], out_dir: Path | None,
                      cell_deg: float = 0.01) -> dict:
    from pyspark.sql import functions as F
    t0 = time.time()
    paths = [str(data_dir / f"uber-raw-data-{m}.csv") for m in months]
    raw = spark.read.csv(paths, header=True)
    n_raw = raw.count()

    parsed = raw.select(
        F.to_utc_timestamp(F.to_timestamp(F.col("Date/Time"), "M/d/yyyy H:mm:ss"),
                           "America/New_York").alias("ts"),
        F.col("Lat").cast("double").alias("lat"), F.col("Lon").cast("double").alias("lon"),
        F.col("Base").alias("base"))
    ok = (F.col("ts").isNotNull() & F.col("lat").between(-90, 90) &
          F.col("lon").between(-180, 180))
    valid = parsed.where(ok).cache()
    n_valid = valid.count()
    b = valid.agg(F.min("lat"), F.max("lat"), F.min("lon"), F.max("lon")).first()
    lat0, lon0, rows, cols = zone_grid(b[0], b[1], b[2], b[3], cell_deg)

    zones = spark.range(rows * cols).select(
        F.col("id").alias("zone_id"),
        (F.lit(lat0) + F.floor(F.col("id") / cols) * cell_deg).alias("zlat"),
        (F.lit(lon0) + (F.col("id") % cols) * cell_deg).alias("zlon"))
    zones = zones.selectExpr(
        "zone_id", "zlat", "zlon",
        f"ST_PolygonFromEnvelope(zlon, zlat, zlon + {cell_deg}, zlat + {cell_deg}) AS zgeom")
    zones.createOrReplaceTempView("zones")
    valid.selectExpr("ts", "base", "lat", "lon", "ST_Point(lon, lat) AS geom") \
        .createOrReplaceTempView("pickups")

    # Sedona plans this as a distributed spatial (range) join.
    joined = spark.sql("""
        SELECT z.zone_id, p.ts, p.base
        FROM pickups p JOIN zones z ON ST_Contains(z.zgeom, p.geom)""")
    plan = joined._jdf.queryExecution().executedPlan().toString()
    hourly = (joined.groupBy("zone_id", F.date_trunc("hour", "ts").alias("hour"), "base")
              .count().cache())
    n_joined = hourly.agg(F.sum("count")).first()[0] or 0
    totals = (hourly.groupBy("zone_id").agg(F.sum("count").alias("pickups"))
              .join(zones.select("zone_id", "zlat", "zlon"), "zone_id")
              .orderBy(F.desc("pickups")))
    top = [r.asDict() for r in totals.limit(10).collect()]
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        hourly.write.mode("overwrite").parquet(str(out_dir / "zone_hour_counts"))
        totals.write.mode("overwrite").parquet(str(out_dir / "zone_totals"))
    stats = {"rows_in": n_raw, "valid": n_valid, "invalid": n_raw - n_valid,
             "joined": int(n_joined), "zones": rows * cols, "cell_deg": cell_deg,
             "grid": {"lat0": lat0, "lon0": lon0, "rows": rows, "cols": cols},
             "hourly_rows": hourly.count(), "top_zones": top,
             "spatial_join_operator": next((op for op in ("RangeJoin", "BroadcastIndexJoin",
                                                          "DistanceJoin") if op in plan),
                                           "other"),
             "seconds": time.time() - t0}
    valid.unpersist()
    return {"stats": stats, "hourly": hourly, "zones": zones}


def to_postgis(result: dict, dsn: str | None = None) -> int:
    """Load zone polygons and hourly counts into PostGIS (zones, zone_hour_counts)."""
    import psycopg
    from . import DEFAULT_DSN
    cell = result["stats"]["cell_deg"]
    with psycopg.connect(dsn or DEFAULT_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("""DROP TABLE IF EXISTS zone_hour_counts, zones;
            CREATE TABLE zones (zone_id bigint PRIMARY KEY, geom geometry(Polygon, 4326));
            CREATE TABLE zone_hour_counts (zone_id bigint REFERENCES zones,
                hour timestamptz, base text, pickups int,
                PRIMARY KEY (zone_id, hour, base));""")
        with cur.copy("COPY zones (zone_id, geom) FROM STDIN") as cp:
            for r in result["zones"].select("zone_id", "zlat", "zlon").toLocalIterator():
                cp.write_row((r.zone_id, f"SRID=4326;POLYGON(({r.zlon} {r.zlat},"
                              f"{r.zlon + cell} {r.zlat},{r.zlon + cell} {r.zlat + cell},"
                              f"{r.zlon} {r.zlat + cell},{r.zlon} {r.zlat}))"))
        n = 0
        with cur.copy("COPY zone_hour_counts FROM STDIN") as cp:
            for r in result["hourly"].toLocalIterator():
                cp.write_row((r.zone_id, r.hour, r.base, r["count"]))
                n += 1
        cur.execute("CREATE INDEX ON zones USING gist (geom); "
                    "CREATE INDEX ON zone_hour_counts (hour); ANALYZE zones, zone_hour_counts")
    return n


def main(argv=None) -> None:
    import json
    from ..datasets import DATA_DIR, UBER_MONTHS
    ap = argparse.ArgumentParser(description="GeoMemory Spark + Sedona batch pipeline")
    ap.add_argument("--months", default="all", help="e.g. apr14,may14 or all")
    ap.add_argument("--cell-deg", type=float, default=0.01)
    ap.add_argument("--cores", default="*")
    ap.add_argument("--out", default="data/spark_out")
    ap.add_argument("--to-postgis", action="store_true")
    a = ap.parse_args(argv)
    months = list(UBER_MONTHS) if a.months == "all" else a.months.split(",")
    spark = spark_session(a.cores)
    res = run_uber_pipeline(spark, DATA_DIR, months, Path(a.out), a.cell_deg)
    if a.to_postgis:
        res["stats"]["postgis_rows"] = to_postgis(res)
    print(json.dumps(res["stats"], indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
