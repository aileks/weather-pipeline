import datetime as dt

import dagster as dg

from weather_pipeline.definitions import defs


def asset_keys() -> set:
    return {key.to_user_string() for key in defs.resolve_all_asset_keys()}


def test_definitions_load_and_expose_the_full_asset_graph():
    keys = asset_keys()

    assert "raw/weather_observations" in keys
    assert "staging/stg_hourly_observations" in keys
    assert "core/fct_hourly_weather" in keys
    assert "core/dim_location" in keys
    assert "core/dim_date" in keys
    assert "marts/weather_anomalies" in keys
    assert "marts/daily_weather_summary" in keys


def test_ingestion_asset_feeds_the_dbt_graph_through_the_source_mapping():
    graph = defs.resolve_asset_graph()
    stg = graph.get(dg.AssetKey(["staging", "stg_hourly_observations"]))

    assert "raw/weather_observations" in {key.to_user_string() for key in stg.parent_keys}


def test_fact_waits_for_its_raw_slice_within_a_partitioned_run():
    # without this same-partition edge the fact step can run before
    # ingestion lands the day and silently insert nothing
    graph = defs.resolve_asset_graph()
    fct = graph.get(dg.AssetKey(["core", "fct_hourly_weather"]))

    assert "raw/weather_observations" in {key.to_user_string() for key in fct.parent_keys}


def test_partitioned_assets_share_the_daily_partition_start():
    graph = defs.resolve_asset_graph()
    raw = graph.get(dg.AssetKey(["raw", "weather_observations"]))
    fct = graph.get(dg.AssetKey(["core", "fct_hourly_weather"]))

    assert raw.partitions_def.start == dt.datetime(2026, 7, 1, tzinfo=dt.UTC)
    assert fct.partitions_def.start == dt.datetime(2026, 7, 1, tzinfo=dt.UTC)


def test_blocking_ingestion_checks_and_dbt_tests_are_registered():
    graph = defs.resolve_asset_graph()
    check_keys = {key.to_user_string() for key in graph.asset_check_keys}

    assert "raw/weather_observations:expected_row_count" in check_keys
    assert "raw/weather_observations:timestamps_within_partition" in check_keys
    # dbt data tests surface as checks automatically through dagster-dbt
    assert any(":not_null" in key for key in check_keys)
    assert any(":unique" in key for key in check_keys)
