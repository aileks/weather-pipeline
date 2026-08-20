"""Rebuild the raw zone from the landing zone: zero API calls."""

import sys

import duckdb

from weather_pipeline.raw_store import rebuild_all
from weather_pipeline.settings import Settings


def main() -> int:
    settings = Settings.from_env()
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.duckdb_path)) as connection:
        counts = rebuild_all(connection, settings.landing_dir)
    for day in sorted(counts):
        print(f"{day.isoformat()}: {counts[day]} rows")
    print(f"rebuilt {len(counts)} partitions from {settings.landing_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
