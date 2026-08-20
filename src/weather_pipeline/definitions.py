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
)
