"""Launch partition materializations for a date range or trailing window."""

import argparse
import datetime as dt
import sys

import dagster as dg

from weather_pipeline.defs.resources import OpenMeteoHttpResource
from weather_pipeline.defs.weather_assets import (
    expected_row_count,
    timestamps_within_partition,
    weather_observations,
)

ASSETS = [weather_observations, expected_row_count, timestamps_within_partition]


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

    for day in partition_days(args):
        result = dg.materialize(
            ASSETS,
            resources={"open_meteo_http": OpenMeteoHttpResource()},
            partition_key=day.isoformat(),
        )
        status = "ok" if result.success else "FAILED"
        print(f"{day.isoformat()}: {status}")
        if not result.success:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
