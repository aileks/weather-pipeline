"""Raw zone: the derived table mirroring each partition's latest snapshot.

Derivation is the only writer: one DuckDB transaction deletes the partition's
slice and re-inserts it from the latest snapshot. The table schema is the
raw.weather_observations contract in docs/data-contracts.md, identical to the
snapshot layout written by weather_pipeline.landing.
"""

import datetime as dt
from pathlib import Path

import duckdb

from weather_pipeline.landing import latest_snapshot

RAW_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.weather_observations (
    partition_date DATE,
    city_id VARCHAR,
    hour_ts_utc TIMESTAMP,
    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    apparent_temperature DOUBLE,
    precipitation DOUBLE,
    weather_code INT,
    pressure_msl DOUBLE,
    surface_pressure DOUBLE,
    cloud_cover DOUBLE,
    wind_speed_10m DOUBLE,
    wind_direction_10m DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    ingested_at_utc TIMESTAMPTZ,
    source_url VARCHAR
)
"""


class RawZoneError(Exception):
    """A partition could not be derived from the landing zone."""


def _partition_days(landing_dir: Path) -> list[dt.date]:
    days = []
    for day_dir in sorted(landing_dir.glob("year=*/month=*/day=*")):
        parts = dict(piece.split("=", 1) for piece in day_dir.parts[-3:])
        days.append(dt.date(int(parts["year"]), int(parts["month"]), int(parts["day"])))
    return days


def derive_partition(
    connection: duckdb.DuckDBPyConnection, landing_dir: Path, partition_day: dt.date
) -> int:
    """Refresh the raw table's partition slice from the latest snapshot.

    Returns the row count landed in the slice. Raises RawZoneError when no
    snapshot exists for the day.
    """
    snapshot = latest_snapshot(landing_dir, partition_day)
    if snapshot is None:
        raise RawZoneError(
            f"no snapshot for partition {partition_day.isoformat()} under {landing_dir}"
        )

    connection.execute(RAW_TABLE_DDL)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            "DELETE FROM raw.weather_observations WHERE partition_date = ?",
            [partition_day],
        )
        connection.execute(
            "INSERT INTO raw.weather_observations "
            # hive_partitioning stays off: the directory layout would otherwise
            # append year/month/day columns the table does not have
            f"SELECT * FROM read_parquet('{snapshot.as_posix()}' , hive_partitioning = false)"
        )
        count = connection.execute(
            "SELECT count(*) FROM raw.weather_observations WHERE partition_date = ?",
            [partition_day],
        ).fetchone()[0]
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return count


def rebuild_all(connection: duckdb.DuckDBPyConnection, landing_dir: Path) -> dict[dt.date, int]:
    """Re-derive every partition found in the landing zone, zero API calls."""
    counts = {}
    for partition_day in _partition_days(landing_dir):
        counts[partition_day] = derive_partition(connection, landing_dir, partition_day)
    return counts
