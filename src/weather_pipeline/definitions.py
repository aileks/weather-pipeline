"""The Dagster code location: one Definitions object for everything."""

import dagster as dg
from dagster_dbt import DbtCliResource

from weather_pipeline.defs.dbt_assets import (
    dbt_project,
    fct_hourly_weather_dbt,
    warehouse_dbt,
)
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
    if spec.key == FCT_KEY:
        # the fact builds after its raw slice: eager automation in
        # daemon-driven flows (a spec-level edge cannot order a dbt
        # multi-asset op inside one run), and the lineage edge documents
        # the same relationship for the graph
        return spec.replace_attributes(
            automation_condition=dg.AutomationCondition.eager(),
            deps=[*spec.deps, dg.AssetDep(RAW_KEY)],
        )
    if spec.key in UNPARTITIONED_DBT_KEYS:
        # marts follow the fact: eager automation (the documented fallback
        # for cross-group coupling, AGENTS.md), launched by the default
        # automation condition sensor
        return spec.replace_attributes(automation_condition=dg.AutomationCondition.eager())
    return spec


defs = dg.Definitions(
    assets=[weather_observations, fct_hourly_weather_dbt, warehouse_dbt],
    asset_checks=[expected_row_count, timestamps_within_partition],
    jobs=[partitioned_job],
    schedules=[daily_reconciliation],
    resources={
        "open_meteo_http": OpenMeteoHttpResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project.project_dir, profiles_dir=dbt_project.profiles_dir
        ),
    },
).map_asset_specs(func=_wire_specs)
