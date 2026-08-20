"""Launch partition materializations for a date range or trailing window.

Each day runs the partitioned job's selection (ingestion, blocking checks,
and the partitioned fact group) through the local Dagster instance, so runs
appear in the UI's history; afterwards the unpartitioned dbt group is
materialized once to refresh dimensions and marts.
"""

import argparse
import datetime as dt
import sys

import dagster as dg
from dagster_dbt import DbtCliResource

from weather_pipeline.defs.dbt_assets import fct_hourly_weather_dbt, warehouse_dbt
from weather_pipeline.defs.resources import OpenMeteoHttpResource
from weather_pipeline.defs.schedules import PARTITION_START, WAREHOUSE_RUN_TAGS
from weather_pipeline.defs.weather_assets import (
    expected_row_count,
    timestamps_within_partition,
    weather_observations,
)

PARTITIONED_ASSETS = [
    weather_observations,
    fct_hourly_weather_dbt,
    expected_row_count,
    timestamps_within_partition,
]


def parse_day(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def partition_days(args: argparse.Namespace) -> list[dt.date]:
    today = dt.datetime.now(dt.UTC).date()
    if args.last:
        last_day = today - dt.timedelta(days=1)
        return [last_day - dt.timedelta(days=offset) for offset in range(args.last)][::-1]
    start = parse_day(args.from_date)
    end = parse_day(args.to_date)
    if end < start:
        raise SystemExit(f"--to ({end}) is before --from ({start})")
    return [start + dt.timedelta(days=offset) for offset in range((end - start).days + 1)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_date", help="first partition day (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="last partition day (YYYY-MM-DD)")
    parser.add_argument("--last", type=int, help="the trailing N partitions ending yesterday")
    args = parser.parse_args()

    if not ((args.from_date and args.to_date) or args.last):
        parser.error("provide either --from and --to, or --last N")

    days = [day for day in partition_days(args) if day >= PARTITION_START]
    with dg.DagsterInstance.get() as instance:
        for day in days:
            result = dg.materialize(
                PARTITIONED_ASSETS,
                instance=instance,
                resources={
                    "open_meteo_http": OpenMeteoHttpResource(),
                    "dbt": DbtCliResource(project_dir="dbt", profiles_dir="dbt"),
                },
                partition_key=day.isoformat(),
                tags=WAREHOUSE_RUN_TAGS,
                raise_on_error=False,
            )
            status = "ok" if result.success else "FAILED"
            print(f"{day.isoformat()}: {status}")
            if not result.success:
                return 1

        marts = dg.materialize(
            [warehouse_dbt],
            instance=instance,
            resources={"dbt": DbtCliResource(project_dir="dbt", profiles_dir="dbt")},
            tags=WAREHOUSE_RUN_TAGS,
            raise_on_error=False,
        )
    print(f"marts: {'ok' if marts.success else 'FAILED'}")
    return 0 if marts.success else 1


if __name__ == "__main__":
    sys.exit(main())
