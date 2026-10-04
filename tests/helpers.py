"""Shared generators and brute-force oracles for tests."""
import random
from datetime import datetime, timedelta, timezone

from geomemory import Observation, Point, Provenance, haversine_m

T0 = datetime(2026, 10, 4, 9, 0, tzinfo=timezone.utc)


def obs(entity, event, lat, lon, minutes=0.0, source="sensor_1", conf=1.0, **kw):
    return Observation(entity, event, Point(lat, lon), T0 + timedelta(minutes=minutes),
                       Provenance(source), confidence=conf, **kw)


def uniform_world(rng: random.Random, n: int, n_entities: int = 50):
    return [obs(f"e{rng.randrange(n_entities)}", rng.choice(["ping", "fail", "move"]),
                rng.uniform(-90, 90), rng.uniform(-180, 180),
                minutes=rng.uniform(0, 10_000), source=f"s{rng.randrange(8)}",
                conf=rng.random()) for _ in range(n)]


def hotspot_world(rng: random.Random, n: int, center=(11.0, 76.0), spread=0.05):
    """Most points in a tiny area, plus a uniform background (spec §18.8)."""
    out = []
    for i in range(n):
        if rng.random() < 0.9:
            lat = min(90, max(-90, rng.gauss(center[0], spread)))
            lon = min(180, max(-180, rng.gauss(center[1], spread)))
        else:
            lat, lon = rng.uniform(-90, 90), rng.uniform(-180, 180)
        out.append(obs(f"e{i % 97}", "ping", lat, lon, minutes=rng.uniform(0, 1000),
                       source=f"s{i % 5}"))
    return out


def brute_radius(points, c, r):
    return {o.observation_id for o in points if haversine_m(c, o.location) <= r}


def brute_between(points, a, b):
    return {o.observation_id for o in points if a <= o.timestamp <= b}


def ids(xs):
    return {o.observation_id for o in xs}
