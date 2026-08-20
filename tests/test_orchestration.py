import datetime as dt

import dagster as dg

from weather_pipeline.defs.schedules import daily_reconciliation, partitioned_job


def test_reconciliation_schedule_covers_the_trailing_eight_partitions():
    tick = dt.datetime(2026, 8, 20, 6, 0, tzinfo=dt.UTC)
    context = dg.build_schedule_context(scheduled_execution_time=tick)

    requests = list(daily_reconciliation(context))

    assert [request.partition_key for request in requests] == [
        (tick.date() - dt.timedelta(days=offset)).isoformat() for offset in range(1, 9)
    ]
    assert requests[0].partition_key == "2026-08-19"
    assert requests[-1].partition_key == "2026-08-12"


def test_reconciliation_schedule_never_precedes_the_partition_start():
    tick = dt.datetime(2026, 7, 3, 6, 0, tzinfo=dt.UTC)
    context = dg.build_schedule_context(scheduled_execution_time=tick)

    requests = list(daily_reconciliation(context))

    assert [request.partition_key for request in requests] == [
        "2026-07-02",
        "2026-07-01",
    ]


def test_reconciliation_run_requests_carry_the_warehouse_tag():
    tick = dt.datetime(2026, 8, 20, 6, 0, tzinfo=dt.UTC)
    context = dg.build_schedule_context(scheduled_execution_time=tick)

    requests = list(daily_reconciliation(context))

    assert all(request.tags["warehouse"] == "duckdb" for request in requests)


def test_partitioned_job_runs_are_tagged_for_the_warehouse():
    assert partitioned_job.run_tags == {"warehouse": "duckdb"}
