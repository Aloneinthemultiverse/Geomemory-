"""Real public datasets (spec §17, §18.1).

    python -m geomemory.datasets download        # ~200 MB into data/raw/

uber   4.5M NYC Uber pickups, Apr–Sep 2014 (NYC TLC via FiveThirtyEight's
       FOIA release). Real GPS points, naturally skewed (Manhattan, airports),
       five dispatching bases act as five independent sources.
quakes 23k significant earthquakes worldwide, 1965–2016 (USGS catalogue,
       plotly mirror). Global coverage: antimeridian, poles, decades of time.
"""
from __future__ import annotations

import argparse
import csv
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

from .model import Observation, Point, Provenance

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
_UBER = "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data"
UBER_MONTHS = ("apr14", "may14", "jun14", "jul14", "aug14", "sep14")
SOURCES = {f"uber-raw-data-{m}.csv": f"{_UBER}/uber-raw-data-{m}.csv" for m in UBER_MONTHS}
SOURCES["earthquakes-23k.csv"] = \
    "https://raw.githubusercontent.com/plotly/datasets/master/earthquakes-23k.csv"

# TLC base licence numbers in the release; each is a separate dispatcher.
UBER_BASES = {"B02512": "Unter", "B02598": "Hinter", "B02617": "Weiter",
              "B02682": "Schmecken", "B02764": "Danach-NY"}
_NYC = ZoneInfo("America/New_York")


def download(data_dir: Path = DATA_DIR, log=print) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for name, url in SOURCES.items():
        dest = data_dir / name
        if dest.exists() and dest.stat().st_size > 0:
            log(f"have {name}")
            continue
        log(f"downloading {name} ...")
        tmp = dest.with_suffix(".part")
        urllib.request.urlretrieve(url, tmp)
        tmp.rename(dest)


def _need(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{path} missing; run: python -m geomemory.datasets download")
    return path


def load_uber(limit: int | None = None, data_dir: Path = DATA_DIR,
              months=UBER_MONTHS) -> Iterator[Observation]:
    """One observation per pickup. Coordinates are published to 4 decimals
    (~11 m), which we record as the spatial uncertainty."""
    n = 0
    for m in months:
        with open(_need(data_dir / f"uber-raw-data-{m}.csv"), newline="") as f:
            for i, row in enumerate(csv.DictReader(f)):
                if limit is not None and n >= limit:
                    return
                local = datetime.strptime(row["Date/Time"], "%m/%d/%Y %H:%M:%S")
                ts = local.replace(tzinfo=_NYC).astimezone(timezone.utc)
                base = row["Base"]
                yield Observation(
                    entity_id=f"{base}_trip_{m}_{i}", event_type="pickup",
                    location=Point(float(row["Lat"]), float(row["Lon"])), timestamp=ts,
                    provenance=Provenance(base, "TLC FOIL 2014"),
                    spatial_uncertainty_m=11.0, observation_id=f"uber_{m}_{i}",
                    attributes={"base_name": UBER_BASES.get(base, base)})
                n += 1


def _quake_time(s: str) -> datetime:
    s = s.strip()
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    return datetime.strptime(s, "%m/%d/%Y").replace(tzinfo=timezone.utc)


def load_quakes(limit: int | None = None, data_dir: Path = DATA_DIR) -> Iterator[Observation]:
    with open(_need(data_dir / "earthquakes-23k.csv"), newline="") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if limit is not None and i >= limit:
                return
            mag = float(row["Magnitude"])
            yield Observation(
                entity_id=f"quake_{i}", event_type="earthquake",
                location=Point(float(row["Latitude"]), float(row["Longitude"])),
                timestamp=_quake_time(row["Date"]),
                provenance=Provenance("USGS", "significant-earthquakes"),
                spatial_uncertainty_m=5000.0, observation_id=f"quake_{i}",
                attributes={"magnitude": mag})


LOADERS = {"uber": load_uber, "quakes": load_quakes}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Real datasets for GeoMemory")
    ap.add_argument("command", choices=["download", "info"])
    a = ap.parse_args(argv)
    if a.command == "download":
        download()
    else:
        for name, fn in LOADERS.items():
            try:
                n = sum(1 for _ in fn())
                print(f"{name}: {n:,} observations")
            except FileNotFoundError as e:
                print(f"{name}: {e}")


if __name__ == "__main__":
    main()
