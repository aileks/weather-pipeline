import datetime as dt

from weather_pipeline.open_meteo import ObservationRow


def observation_row(**overrides) -> ObservationRow:
    """A valid row for one city-hour, overridable per test.

    hour_ts_utc is naive UTC by the raw-zone contract (docs/data-contracts.md).
    """
    defaults = {
        "partition_date": dt.date(2026, 8, 15),
        "city_id": "reykjavik",
        "hour_ts_utc": dt.datetime(2026, 8, 15, 14, 0),  # noqa: DTZ001
        "temperature_2m": 11.3,
        "relative_humidity_2m": 82.0,
        "apparent_temperature": 10.1,
        "precipitation": 0.0,
        "weather_code": 3,
        "pressure_msl": 1006.2,
        "surface_pressure": 1002.8,
        "cloud_cover": 75.0,
        "wind_speed_10m": 18.4,
        "wind_direction_10m": 225.0,
        "latitude": 64.13,
        "longitude": -21.94,
        "ingested_at_utc": dt.datetime(2026, 8, 16, 6, 0, 5, tzinfo=dt.UTC),
        "source_url": "https://archive-api.open-meteo.com/v1/archive?latitude=64.1466",
    }
    return ObservationRow(**(defaults | overrides))
