"""The Dagster code location: one Definitions object for everything."""

import dagster as dg
from dagster_dbt import DbtCliResource

from weather_pipeline.defs.dbt_assets import dbt_project, fct_hourly_weather_dbt, warehouse_dbt
from weather_pipeline.defs.resources import OpenMeteoHttpResource
from weather_pipeline.defs.schedules import daily_reconciliation, partitioned_job
from weather_pipeline.defs.weather_assets import (
    expected_row_count,
    timestamps_within_partition,
    weather_observations,
)

UNPARTITIONED_DBT_KEYS = set(warehouse_dbt.keys)
FCT_KEY = dg.AssetKey(["core", "fct_hourly_weather"])
RAW_KEY = dg.AssetKey(["raw", "weather_observations"])


def _wire_specs(spec: dg.AssetSpec) -> dg.AssetSpec:
    if spec.key in UNPARTITIONED_DBT_KEYS:
        # marts follow fact partitions: eager automation (the documented
        # fallback for cross-group coupling, AGENTS.md), launched by the
        # default automation condition sensor
        return spec.replace_attributes(automation_condition=dg.AutomationCondition.eager())
    if spec.key == FCT_KEY:
        # same-partition edge so the fact always builds after its raw slice
        # landed; without it the fact step can run before ingestion within
        # one run and silently insert nothing
        return spec.replace_attributes(deps=[*spec.deps, dg.AssetDep(RAW_KEY)])
    return spec


defs = dg.Definitions(
    assets=[weather_observations, fct_hourly_weather_dbt, warehouse_dbt],
    asset_checks=[expected_row_count, timestamps_within_partition],
    jobs=[partitioned_job],
    schedules=[daily_reconciliation],
    resources={
        "open_meteo_http": OpenMeteoHttpResource(),
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir, profiles_dir="dbt"),
    },
).map_asset_specs(func=_wire_specs)
