"""Schedules and jobs: the daily reconciliation cadence.

Operational constants (06:00 UTC, trailing 8 partitions, 2026-07-01 start)
are owned by docs/orchestration.md; this module implements them.

The partitioned job owns ingestion only (the ingestion asset and its
blocking checks): a spec-level edge cannot order a dbt multi-asset op after
the ingestion op inside one run, so the fact follows through its eager
automation condition in daemon-driven flows, and scripts finish with one
full dbt build.
"""

import datetime as dt

import dagster as dg

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
        expected_row_count,
        timestamps_within_partition,
    ],
    partitions_def=daily_partitions,
    run_tags=WAREHOUSE_RUN_TAGS,
    description="One UTC partition: fetch and land the snapshot, derive the "
    "raw slice, and pass the blocking checks.",
)


@dg.schedule(
    job=partitioned_job,
    cron_schedule=RECONCILIATION_CRON,
    name="daily_reconciliation",
    description="At 06:00 UTC, re-materialize the trailing 8 partitions: "
    "yesterday is a fresh fetch, the previous 7 days absorb upstream "
    "revisions of provisional values. The fact and marts follow through "
    "their automation conditions.",
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
