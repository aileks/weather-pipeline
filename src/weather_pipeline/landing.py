"""Landing zone: versioned immutable snapshots, one per run per partition day.

Snapshot identity is (partition_day, run identity): the filename pairs the
run's ingestion timestamp (captured once per run, constant across attempts)
with the run id, so a retry reuses its own snapshot and only a new run adds
one. Files are written atomically and never modified, renamed, or deleted.

Contract owner: docs/ingestion-storage.md#the-landing-zone-versioned-immutable-snapshots.
"""

import datetime as dt
import os
from dataclasses import astuple, dataclass
from pathlib import Path

import duckdb

SNAPSHOT_GLOB = "ingested_at=*.parquet"


class SnapshotError(Exception):
    """A snapshot could not be written or an existing one is inconsistent."""


@dataclass(frozen=True)
class SnapshotResult:
    path: Path
    reused: bool


def snapshot_path(
    landing_dir: Path, partition_day: dt.date, run_ingested_at: dt.datetime, run_id: str
) -> Path:
    """The deterministic snapshot path for one run fetching one partition day."""
    timestamp = run_ingested_at.astimezone(dt.UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return (
        landing_dir
        / f"year={partition_day.year:04d}"
        / f"month={partition_day.month:02d}"
        / f"day={partition_day.day:02d}"
        / f"ingested_at={timestamp}_run={run_id[:8]}.parquet"
    )


def _row_count(connection: duckdb.DuckDBPyConnection, path: Path) -> int:
    return connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]


def write_snapshot(
    connection: duckdb.DuckDBPyConnection,
    landing_dir: Path,
    rows: list,
    partition_day: dt.date,
    run_ingested_at: dt.datetime,
    run_id: str,
) -> SnapshotResult:
    """Land ``rows`` as this run's immutable snapshot.

    Write-once-with-reuse: when this run's snapshot already exists (an earlier
    attempt landed it), its row count is validated and the file is reused
    without rewriting. A new run id always writes a new file.
    """
    target = snapshot_path(landing_dir, partition_day, run_ingested_at, run_id)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        existing = _row_count(connection, target)
        if existing != len(rows):
            raise SnapshotError(
                f"snapshot {target} belongs to this run but holds {existing} rows, "
                f"expected {len(rows)}"
            )
        return SnapshotResult(path=target, reused=True)

    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE snapshot_rows (
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
    )
    connection.executemany(
        "INSERT INTO snapshot_rows VALUES (" + ",".join(["?"] * 17) + ")",
        [astuple(row) for row in rows],
    )

    temp_path = target.with_name(target.name + ".tmp")
    connection.execute(
        f"COPY (SELECT * FROM snapshot_rows) TO '{temp_path.as_posix()}' (FORMAT PARQUET)"
    )
    os.replace(temp_path, target)
    return SnapshotResult(path=target, reused=False)


def latest_snapshot(landing_dir: Path, partition_day: dt.date) -> Path | None:
    """The latest snapshot for a partition day, or None when none exists.

    The ingestion timestamp prefix is fixed-width, so a plain filename sort
    orders by ingestion time; the run id suffix breaks ties.
    """
    directory = (
        landing_dir
        / f"year={partition_day.year:04d}"
        / f"month={partition_day.month:02d}"
        / f"day={partition_day.day:02d}"
    )
    if not directory.is_dir():
        return None
    snapshots = sorted(path.name for path in directory.glob(SNAPSHOT_GLOB))
    return directory / snapshots[-1] if snapshots else None
