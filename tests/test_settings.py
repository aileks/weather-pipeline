from pathlib import Path

from weather_pipeline.settings import Settings


def test_defaults_match_the_runbook_contract():
    settings = Settings.from_env(env={})

    assert settings.open_meteo_base_url == "https://archive-api.open-meteo.com/v1/archive"
    assert settings.duckdb_path == Path("warehouse/weather.duckdb")
    assert settings.landing_dir == Path("data/raw")
    assert settings.log_level == "INFO"


def test_environment_overrides_every_value():
    settings = Settings.from_env(
        env={
            "WEATHER_PIPELINE_OPEN_METEO_BASE_URL": "http://localhost:9/v1/archive",
            "WEATHER_PIPELINE_DUCKDB_PATH": "/tmp/warehouse.duckdb",
            "WEATHER_PIPELINE_LANDING_DIR": "/tmp/landing",
            "WEATHER_PIPELINE_LOG_LEVEL": "DEBUG",
        }
    )

    assert settings.open_meteo_base_url == "http://localhost:9/v1/archive"
    assert settings.duckdb_path == Path("/tmp/warehouse.duckdb")
    assert settings.landing_dir == Path("/tmp/landing")
    assert settings.log_level == "DEBUG"
