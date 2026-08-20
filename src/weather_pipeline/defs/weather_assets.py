"""The ingestion asset: fetch, land, derive, report, with blocking checks.

Orchestration constants (the 2026-07-01 partition start, retry policy) are
owned by docs/orchestration.md; this module implements them.

Snapshot identity is (partition_day, run): ingested_at comes from the Dagster
run's start time, constant across attempts, so a retry reuses the snapshot an
earlier attempt of the same run already landed.
"""

import datetime as dt

import dagster as dg
import duckdb

from weather_pipeline.cities import load_cities
from weather_pipeline.defs.resources import OpenMeteoHttpResource
from weather_pipeline.landing import write_snapshot
from weather_pipeline.open_meteo import (
    HOURLY_VARIABLES,
    RetryableFetchError,
    fetch_observations,
)
from weather_pipeline.raw_store import derive_partition
from weather_pipeline.settings import Settings

daily_partitions = dg.DailyPartitionsDefinition(start_date="2026-07-01")

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 30


def _run_started_at(context: dg.AssetExecutionContext) -> dt.datetime:
    """The run's start time: read once from the run's single RUN_START event.

    Event timestamps are immutable, so every attempt of this run, including
    retries, computes the same snapshot filename.
    """
    start_events = context.instance.all_logs(
        context.run.run_id, of_type=dg.DagsterEventType.RUN_START
    )
    if not start_events:
        raise dg.Failure(
            description="run start event is unavailable; snapshot identity requires it",
        )
    return dt.datetime.fromtimestamp(start_events[0].timestamp, tz=dt.UTC)


@dg.asset(
    key_prefix="raw",
    partitions_def=daily_partitions,
    group_name="ingestion",
    pool="warehouse",
    freshness_policy=dg.FreshnessPolicy.time_window(
        warn_window=dt.timedelta(hours=36),
        fail_window=dt.timedelta(hours=48),
    ),
    description="Hourly observations for one UTC day, landed as an immutable "
    "snapshot and derived into raw.weather_observations.",
)
def weather_observations(
    context: dg.AssetExecutionContext, open_meteo_http: OpenMeteoHttpResource
) -> dg.MaterializeResult:
    settings = Settings.from_env()
    day = context.partition_time_window.start.date()
    run_started_at = _run_started_at(context)
    context.log.info(f"ingestion_started partition={day.isoformat()}")

    cities = load_cities()
    try:
        with open_meteo_http.get_client() as client:
            rows = fetch_observations(
                client, settings.open_meteo_base_url, cities, day, run_started_at
            )
    except RetryableFetchError as error:
        raise dg.RetryRequested(
            max_retries=MAX_RETRIES, seconds_to_wait=RETRY_WAIT_SECONDS
        ) from error
    context.log.info(f"records_received count={len(rows)}")

    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.duckdb_path)) as connection:
        snapshot = write_snapshot(
            connection,
            settings.landing_dir,
            rows,
            day,
            run_started_at,
            context.run.run_id,
        )
        derived_count = derive_partition(connection, settings.landing_dir, day)

    null_counts = {
        variable: sum(1 for row in rows if getattr(row, variable) is None)
        for variable in HOURLY_VARIABLES
    }
    context.log.info(
        f"partition_written partition={day.isoformat()} snapshot={snapshot.path.name} "
        f"reused={snapshot.reused}"
    )

    return dg.MaterializeResult(
        metadata={
            "snapshot_path": dg.MetadataValue.path(snapshot.path),
            # bool is not an accepted metadata value; ints are
            "snapshot_reused": int(snapshot.reused),
            "rows_landed": len(rows),
            "rows_derived": derived_count,
            "null_counts": null_counts,
        },
    )


def _partition_slice_stats(
    settings: Settings, day: dt.date
) -> tuple[int, dt.datetime, dt.datetime]:
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as connection:
        count, first_hour, last_hour = connection.execute(
            "SELECT count(*), min(hour_ts_utc), max(hour_ts_utc) "
            "FROM raw.weather_observations WHERE partition_date = ?",
            [day],
        ).fetchone()
    return count, first_hour, last_hour


def _expected_row_count_result(settings: Settings, day: dt.date) -> dg.AssetCheckResult:
    count, _, _ = _partition_slice_stats(settings, day)
    expected = len(load_cities()) * 24
    return dg.AssetCheckResult(
        passed=count == expected,
        metadata={"row_count": count, "expected": expected},
    )


def _timestamps_within_partition_result(settings: Settings, day: dt.date) -> dg.AssetCheckResult:
    count, first_hour, last_hour = _partition_slice_stats(settings, day)
    start = dt.datetime.combine(day, dt.time.min)
    end = start + dt.timedelta(days=1)
    passed = count > 0 and first_hour >= start and last_hour < end
    return dg.AssetCheckResult(
        passed=passed,
        metadata={"first_hour": str(first_hour), "last_hour": str(last_hour)},
    )


@dg.asset_check(asset=weather_observations, blocking=True)
def expected_row_count(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    return _expected_row_count_result(
        Settings.from_env(), dt.date.fromisoformat(context.partition_key)
    )


@dg.asset_check(asset=weather_observations, blocking=True)
def timestamps_within_partition(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    return _timestamps_within_partition_result(
        Settings.from_env(), dt.date.fromisoformat(context.partition_key)
    )
