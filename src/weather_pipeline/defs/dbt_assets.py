"""The dbt project exposed to Dagster as two asset groups.

Split on the incremental-materialization selector (the documented
dagster-dbt pattern): the partitioned group runs the fact one UTC day at a
time with start_date/end_date vars; the unpartitioned group builds the seed,
staging view, dimensions, and marts. Cross-group coupling (marts follow fact
updates) is sequenced by the unpartitioned group's automation condition,
because @dbt_assets supports no deps parameter (AGENTS.md).
"""

import datetime as dt
import json
import os

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from weather_pipeline.defs.weather_assets import daily_partitions
from weather_pipeline.settings import Settings

# dbt subprocesses launched by dagster-dbt run with cwd=dbt/, so the
# warehouse path must be absolute for them (profiles.yml reads this via
# env_var); resolving it here is the runbook's documented load-time
# configuration resolution and applies to every importer of the groups.
os.environ.setdefault(
    "WEATHER_PIPELINE_DUCKDB_PATH",
    str(Settings.from_env().duckdb_path.resolve()),
)

dbt_project = DbtProject(project_dir="dbt", profiles_dir="dbt")
dbt_project.prepare_if_dev()

INCREMENTAL_SELECTOR = "config.materialized:incremental"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partitions,
    retry_policy=dg.RetryPolicy(max_retries=1),
    pool="warehouse",
)
def fct_hourly_weather_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the incremental fact for exactly one partition day."""
    time_window = context.partition_time_window
    dbt_vars = {
        "start_date": time_window.start.strftime("%Y-%m-%d"),
        # the partition time window ends at the next midnight; the fact
        # filters date_utc <= end_date, so subtract the day to keep the
        # run touching exactly one UTC day
        "end_date": (time_window.end - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude=INCREMENTAL_SELECTOR,
    retry_policy=dg.RetryPolicy(max_retries=1),
    pool="warehouse",
)
def warehouse_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the seed, staging view, dimensions, and marts."""
    yield from dbt.cli(["build", "--exclude", INCREMENTAL_SELECTOR], context=context).stream()
