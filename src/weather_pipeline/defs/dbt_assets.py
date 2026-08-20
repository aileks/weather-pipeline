"""The dbt project exposed to Dagster as two asset groups.

Split on the incremental-materialization selector (the documented
dagster-dbt pattern): the partitioned group runs the fact one UTC day at a
time with start_date/end_date vars; the unpartitioned group builds the
staging view, dimensions, and marts. Cross-group coupling (marts follow
fact updates) is sequenced by automation conditions, because @dbt_assets
supports no deps parameter (AGENTS.md).
"""

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from weather_pipeline.dbt_cli import DBT_DIR
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

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
if not dbt_project.manifest_path.exists():
    # fresh checkout (CI, clean clone): install packages (dbt_packages/ is
    # gitignored) and parse once so the manifest exists. Parsing with
    # cwd=project dir records project-relative paths, matching how
    # dagster-dbt invokes dbt at runtime; output streams so failures show
    # their reason instead of a bare exit code.
    for args in (
        ["deps", "--profiles-dir", str(DBT_DIR)],
        ["parse", "--profiles-dir", str(DBT_DIR)],
    ):
        subprocess.run(
            [str(Path(sys.executable).parent / "dbt"), *args],
            cwd=DBT_DIR,
            check=True,
        )

INCREMENTAL_SELECTOR = "config.materialized:incremental"
RAW_KEY = dg.AssetKey(["raw", "weather_observations"])
FCT_KEY = dg.AssetKey(["core", "fct_hourly_weather"])


class EagerDbtTranslator(DagsterDbtTranslator):
    """Every asset in the group follows its upstreams via eager automation.

    The translator runs for every manifest node regardless of the group's
    selection, so per-node behavior must be guarded by key.
    """

    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        return spec.replace_attributes(automation_condition=dg.AutomationCondition.eager())


class FctDbtTranslator(EagerDbtTranslator):
    """The fact additionally documents its lineage dependency on the
    ingestion asset (same partitions)."""

    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        if spec.key == FCT_KEY:
            spec = spec.replace_attributes(deps=[*spec.deps, dg.AssetDep(RAW_KEY)])
        return spec


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partitions,
    pool="warehouse",
    dagster_dbt_translator=FctDbtTranslator(),
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
    pool="warehouse",
    dagster_dbt_translator=EagerDbtTranslator(),
)
def warehouse_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the staging view, dimensions, and marts.

    Seeds are excluded: loading cities.csv is a bootstrap concern
    (scripts/bootstrap.py), not a per-materialization one, and dbt-duckdb's
    seed file resolution under dagster-dbt's subprocess layout is fragile.
    Models referencing ref('cities') use the table bootstrap loaded.
    """
    yield from dbt.cli(
        ["build", "--exclude", f"{INCREMENTAL_SELECTOR},resource_type:seed"],
        context=context,
    ).stream()
