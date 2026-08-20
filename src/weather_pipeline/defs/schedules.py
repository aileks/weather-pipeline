"""Schedules and jobs: the daily reconciliation cadence.

Operational constants (06:00 UTC, trailing 8 partitions, 2026-07-01 start)
are owned by docs/orchestration.md; this module implements them.
"""

import datetime as dt

import dagster as dg

from weather_pipeline.defs.dbt_assets import fct_hourly_weather_dbt
from weather_pipeline.defs.weather_assets import (
    daily_partitions,
    expected_row_count,
    timestamps_within_partition,
    weather_observations,
)

RECONCILIATION_CRON = "0 6 * * *"
RECONCILIATION_WINDOW_DAYS = 8
PARTITION_START = dt.date(2026, 7, 1)

WAREHOUSE_RUN_TAGS = {"warehouse": "duckdb"}

partitioned_job = dg.define_asset_job(
    name="daily_weather_partitioned",
    selection=[
        weather_observations,
        fct_hourly_weather_dbt,
        expected_row_count,
        timestamps_within_partition,
    ],
    partitions_def=daily_partitions,
    run_tags=WAREHOUSE_RUN_TAGS,
    description="One UTC partition: fetch and land the snapshot, derive the "
    "raw slice, and merge the day into the fact.",
)


@dg.schedule(
    job=partitioned_job,
    cron_schedule=RECONCILIATION_CRON,
    name="daily_reconciliation",
    description="At 06:00 UTC, re-materialize the trailing 8 partitions: "
    "yesterday is a fresh fetch, the previous 7 days absorb upstream "
    "revisions of provisional values.",
)
def daily_reconciliation(context: dg.ScheduleEvaluationContext):
    tick_time = (
        context.scheduled_execution_time.astimezone(dt.UTC)
        if context.scheduled_execution_time
        else dt.datetime.now(dt.UTC)
    )
    today = tick_time.date()
    for offset in range(1, RECONCILIATION_WINDOW_DAYS + 1):
        day = today - dt.timedelta(days=offset)
        if day < PARTITION_START:
            continue
        yield dg.RunRequest(
            run_key=f"daily_reconciliation-{day.isoformat()}",
            partition_key=day.isoformat(),
            tags=WAREHOUSE_RUN_TAGS,
        )
