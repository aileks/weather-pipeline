"""The cities seed is the single authoritative city list.

Ingestion reads this file directly to build API requests and dim_location is a
thin select over it as a dbt seed; nothing else defines cities.
"""

import csv
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SEED_PATH = Path("dbt/seeds/cities.csv")


@dataclass(frozen=True)
class City:
    city_id: str
    city_name: str
    country: str
    latitude: float
    longitude: float
    timezone: str
    climate_zone: str


def load_cities(seed_path: Path = DEFAULT_SEED_PATH) -> list[City]:
    with seed_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    cities = [
        City(
            city_id=row["city_id"],
            city_name=row["city_name"],
            country=row["country"],
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            timezone=row["timezone"],
            climate_zone=row["climate_zone"],
        )
        for row in rows
    ]
    if not cities:
        raise ValueError(f"cities seed has no rows: {seed_path}")
    return cities
