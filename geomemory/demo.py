"""Demo world + web UI.

    python -m geomemory.demo            # then open http://127.0.0.1:8765

Builds a synthetic but realistic world around Coimbatore (spec §2's
11.x, 76.x example): a factory with the Machine_47 failure story (§22), a
solar farm whose panels deteriorate (§21.6), delivery trucks moving
between sites, and a flood seen by satellite, drone and ground sensors
that disagree about where it is (§18.9).
"""
from __future__ import annotations

import argparse
import math
import random
from datetime import datetime, timedelta, timezone

from .agent import AgentInterface, serve
from .model import Observation, Point, Provenance
from .store import GeoMemory

NOW = datetime(2026, 10, 4, 9, 30, tzinfo=timezone.utc)
FACTORY = (11.0168, 76.9558)
SOLAR = (10.9600, 77.0500)
FLOOD = (11.0500, 76.9000)
DEPOT = (11.0010, 76.9800)


def _o(rng, entity, event, lat, lon, t, source, conf, unc, model=None, parent=None, oid=None,
       **attrs):
    return Observation(entity, event, Point(lat, lon), t, Provenance(source, model),
                       confidence=round(conf, 3), spatial_uncertainty_m=unc,
                       attributes=attrs, parent_observation=parent,
                       observation_id=oid or f"obs_{rng.getrandbits(48):012x}")


def build_world(seed: int = 7) -> list[Observation]:
    rng = random.Random(seed)
    out: list[Observation] = []
    start = NOW - timedelta(days=60)

    # Factory: 6x8 grid of machines, hourly-ish telemetry, occasional anomalies.
    machines = {}
    for i in range(48):
        r, c = divmod(i, 8)
        machines[f"Machine_{i + 1:02d}"] = (FACTORY[0] + r * 0.00018, FACTORY[1] + c * 0.00022)
    for name, (la, lo) in machines.items():
        t = start
        while t < NOW - timedelta(hours=2):
            t += timedelta(hours=rng.uniform(4, 10))
            ev = "temperature_anomaly" if rng.random() < 0.03 else "telemetry"
            out.append(_o(rng, name, ev, la, lo, t, f"Sensor_{rng.randrange(180, 190)}",
                          rng.uniform(0.85, 0.99), 2.0, temp_c=round(rng.gauss(62, 6), 1)))
        if rng.random() < 0.15:
            out.append(_o(rng, name, "maintenance", la, lo,
                          start + timedelta(days=rng.uniform(5, 50)), "Technician_Log", 1.0, 1.0))

    # The Machine_47 story (§22).
    la, lo = machines["Machine_47"]
    t_fail = NOW - timedelta(minutes=20)
    a = _o(rng, "Machine_47", "temperature_anomaly", la, lo, t_fail - timedelta(minutes=10),
           "Sensor_184", 0.91, 2.0, oid="obs_m47_temp", temp_c=82.0)
    b = _o(rng, "Machine_47", "vibration_anomaly", la, lo, t_fail - timedelta(minutes=5),
           "Sensor_185", 0.88, 2.0, parent=a.observation_id, oid="obs_m47_vib", rms_g=3.4)
    f = _o(rng, "Machine_47", "failure", la, lo, t_fail, "Sensor_185", 0.99, 2.0,
           parent=b.observation_id, oid="obs_m47_fail")
    la48, lo48 = machines["Machine_48"]
    n = _o(rng, "Machine_48", "temperature_anomaly", la48, lo48, t_fail - timedelta(minutes=2),
           "Sensor_186", 0.8, 2.0, temp_c=74.0)
    out += [a, b, f, n]

    # Solar farm: monthly drone inspections; a few panels deteriorate.
    sev = ["normal", "hotspot", "crack"]
    for p in range(60):
        la = SOLAR[0] + (p // 10) * 0.0004
        lo = SOLAR[1] + (p % 10) * 0.0004
        level = 0
        worsening = p in (7, 13, 42, 55)
        for month in range(3):
            if worsening:
                level = min(2, month)
            elif rng.random() < 0.015:
                level = 1
            t = start + timedelta(days=month * 28 + rng.uniform(0, 2))
            out.append(_o(rng, f"Panel_{p + 1:03d}", sev[level], la, lo, t,
                          f"Drone_{rng.choice(['A', 'B'])}", rng.uniform(0.75, 0.97), 3.0,
                          model="ThermalDetector_v3"))

    # Delivery trucks: depot -> factory -> solar farm -> depot loops.
    route = [DEPOT, FACTORY, SOLAR, DEPOT]
    for tr in range(6):
        t = start + timedelta(hours=rng.uniform(0, 24))
        while t < NOW:
            for (a_la, a_lo), (b_la, b_lo) in zip(route, route[1:]):
                for k in range(8):
                    f_ = k / 8
                    out.append(_o(rng, f"Truck_{tr + 1}", "position",
                                  a_la + (b_la - a_la) * f_ + rng.gauss(0, 0.0002),
                                  a_lo + (b_lo - a_lo) * f_ + rng.gauss(0, 0.0002),
                                  t, f"GPS_T{tr + 1}", 0.95, 8.0))
                    t += timedelta(minutes=rng.uniform(4, 9))
            t += timedelta(hours=rng.uniform(20, 40))

    # Flood: three sources disagree about the location (§18.9).
    t_flood = NOW - timedelta(days=3)
    # Satellite ~30 m off, drone ~50 m off, ground sensor close: they disagree
    # within their stated uncertainty, so all are kept; the citizen report is not.
    for k in range(5):
        tk = t_flood + timedelta(hours=k * 6)
        out.append(_o(rng, "Flood_Event_1", "flood_extent", FLOOD[0] + 0.00027 + rng.gauss(0, 1e-4),
                      FLOOD[1] + rng.gauss(0, 1e-4), tk, "Satellite_17", 0.82, 30.0,
                      model="WaterSeg_v2"))
        out.append(_o(rng, "Flood_Event_1", "flood_extent", FLOOD[0] - 0.0003 + rng.gauss(0, 1e-4),
                      FLOOD[1] + 0.0003 + rng.gauss(0, 1e-4), tk + timedelta(minutes=40),
                      "Drone_C", 0.74, 25.0))
        out.append(_o(rng, "Flood_Event_1", "flood_extent", FLOOD[0] + rng.gauss(0, 2e-5),
                      FLOOD[1] + rng.gauss(0, 2e-5), tk + timedelta(minutes=10),
                      "Water_Level_Sensor_3", 0.91, 5.0))
    out.append(_o(rng, "Flood_Event_1", "flood_extent", FLOOD[0] + 0.03, FLOOD[1] + 0.02,
                  t_flood, "Citizen_Report", 0.4, 200.0))  # the outlier
    for r_ in range(4):
        ang = r_ * math.pi / 2
        out.append(_o(rng, f"Road_Segment_{r_ + 1}", "closed",
                      FLOOD[0] + 0.002 * math.cos(ang), FLOOD[1] + 0.002 * math.sin(ang),
                      t_flood + timedelta(hours=3 + r_), "Traffic_Control", 0.97, 10.0))
    return out


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="GeoMemory demo UI")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--results", default="results", help="benchmark results directory")
    ap.add_argument("--dataset", choices=["synthetic", "uber"], default="synthetic",
                    help="uber: real NYC pickups (run `python -m geomemory.datasets download`)")
    ap.add_argument("--months", default="jul14,sep14",
                    help="uber months to load, e.g. apr14,jul14 (all six: ~4.5M rows)")
    a = ap.parse_args(argv)
    from .index import QuadTreeIndex
    gm = GeoMemory(QuadTreeIndex())
    if a.dataset == "uber":
        from .datasets import load_uber
        months = tuple(m.strip() for m in a.months.split(",") if m.strip())
        print(f"loading Uber pickups for {', '.join(months)} ...", flush=True)
        gm.ingest_many(load_uber(months=months))
    else:
        gm.ingest_many(build_world())
    srv, t = serve(AgentInterface(gm, a.dataset), a.host, a.port, results_dir=a.results)
    print(f"GeoMemory UI: http://{a.host}:{srv.server_address[1]}  ({len(gm):,} observations)")
    try:
        t.join()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
