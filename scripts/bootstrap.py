"""Bootstrap the dbt foundation: seed, staging view, dimensions.

Creates the empty raw.weather_observations table first, because the staging
view cannot bind to a table that does not exist on a fresh warehouse
(docs/orchestration.md#bootstrap-and-first-run-ordering). Safe to re-run,
including before backfills that introduce a new city. dbt always runs with
its working directory inside the project, which keeps recorded paths
project-relative for dagster-dbt's later invocations.
"""

import sys

import duckdb

from weather_pipeline.dbt_cli import run_dbt
from weather_pipeline.raw_store import RAW_TABLE_DDL
from weather_pipeline.settings import Settings


def main() -> int:
    settings = Settings.from_env()
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(settings.duckdb_path)) as connection:
        connection.execute(RAW_TABLE_DDL)

    return run_dbt(
        [
            "build",
            "--profiles-dir",
            ".",
            "--select",
            "cities dim_location dim_date stg_hourly_observations",
            # bootstrap is structural: tests that reference the fact would
            # run before the fact exists (selection pulls them in through
            # their dimension parents); full builds test everything
            "--exclude",
            "resource_type:test",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
