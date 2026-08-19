import copy
import datetime as dt
import json
from pathlib import Path

import httpx
import pytest

from weather_pipeline.cities import load_cities
from weather_pipeline.open_meteo import (
    InvalidResponseError,
    RetryableFetchError,
    fetch_observations,
)

FIXTURE = Path(__file__).parent / "fixtures" / "api" / "archive_2026-08-17.json"
SEED = Path(__file__).parents[1] / "dbt" / "seeds" / "cities.csv"
DAY = dt.date(2026, 8, 17)
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
INGESTED_AT = dt.datetime(2026, 8, 18, 6, 0, 5, tzinfo=dt.UTC)


def recorded_payload() -> dict:
    return json.loads(FIXTURE.read_text())


def client_returning(payload) -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    )


def fetch(client: httpx.Client):
    return fetch_observations(client, BASE_URL, load_cities(SEED), DAY, INGESTED_AT)


def test_happy_path_returns_one_typed_row_per_city_hour():
    rows = fetch(client_returning(recorded_payload()))

    assert len(rows) == 192
    first = rows[0]
    assert first.city_id == "new-york"
    assert first.partition_date == DAY
    assert (first.hour_ts_utc.year, first.hour_ts_utc.month, first.hour_ts_utc.day) == (
        2026,
        8,
        17,
    )
    assert first.hour_ts_utc.hour == 0
    assert first.hour_ts_utc.tzinfo is None  # naive UTC is the raw-zone contract
    assert first.ingested_at_utc == INGESTED_AT
    assert isinstance(first.temperature_2m, float)
    assert isinstance(first.weather_code, int)
    assert first.source_url.startswith(BASE_URL)


def test_city_mapping_follows_request_order_not_coordinates():
    rows = fetch(client_returning(recorded_payload()))

    assert rows[0].city_id == "new-york"
    assert rows[24].city_id == "london"
    assert rows[24 * 7].city_id == "reykjavik"


def test_hours_span_the_partition_day_exactly_per_city():
    rows = fetch(client_returning(recorded_payload()))

    for city_index in range(8):
        city_rows = rows[city_index * 24 : (city_index + 1) * 24]
        assert [row.hour_ts_utc.hour for row in city_rows] == list(range(24))
        assert all(row.hour_ts_utc.date() == DAY for row in city_rows)


def test_grid_snapped_coordinates_are_recorded_as_provenance():
    payload = recorded_payload()
    rows = fetch(client_returning(payload))

    assert rows[0].latitude == payload[0]["latitude"]
    assert rows[0].longitude == payload[0]["longitude"]


def test_null_measures_are_preserved():
    payload = recorded_payload()
    payload[0]["hourly"]["temperature_2m"][3] = None
    payload[0]["hourly"]["precipitation"][7] = None

    rows = fetch(client_returning(payload))

    assert rows[3].temperature_2m is None
    assert rows[7].precipitation is None
    assert rows[3].relative_humidity_2m is not None


def test_missing_city_in_response_is_invalid():
    payload = recorded_payload()[:-1]

    with pytest.raises(InvalidResponseError, match="expected 8 location objects"):
        fetch(client_returning(payload))


def test_unit_change_is_invalid():
    payload = recorded_payload()
    payload[2]["hourly_units"]["wind_speed_10m"] = "m/s"

    with pytest.raises(InvalidResponseError, match="unit change for wind_speed_10m"):
        fetch(client_returning(payload))


def test_truncated_hour_series_is_invalid():
    payload = recorded_payload()
    payload[0]["hourly"]["time"] = payload[0]["hourly"]["time"][:23]

    with pytest.raises(InvalidResponseError, match="expected 24 hourly timestamps"):
        fetch(client_returning(payload))


def test_variable_series_length_mismatch_is_invalid():
    payload = recorded_payload()
    payload[1]["hourly"]["precipitation"] = payload[1]["hourly"]["precipitation"][:20]

    with pytest.raises(InvalidResponseError, match="precipitation missing or not 24"):
        fetch(client_returning(payload))


def test_duplicated_hour_is_invalid():
    payload = copy.deepcopy(recorded_payload())
    payload[0]["hourly"]["time"][5] = payload[0]["hourly"]["time"][4]

    with pytest.raises(InvalidResponseError, match="do not span the partition day"):
        fetch(client_returning(payload))


def test_missing_variable_is_invalid():
    payload = recorded_payload()
    del payload[0]["hourly"]["cloud_cover"]

    with pytest.raises(InvalidResponseError, match="cloud_cover"):
        fetch(client_returning(payload))


def test_error_body_is_deterministic_failure():
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                400,
                json={"reason": "Parameter 'start_date' is out of allowed range", "error": True},
            )
        )
    )

    with pytest.raises(InvalidResponseError, match="out of allowed range"):
        fetch(client)


@pytest.mark.parametrize("status", [429, 500, 503])
def test_rate_limit_and_server_errors_are_retryable(status):
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(status)))

    with pytest.raises(RetryableFetchError):
        fetch(client)


def test_timeout_is_retryable():
    def raise_timeout(request):
        raise httpx.ConnectTimeout("connection timed out")

    client = httpx.Client(transport=httpx.MockTransport(raise_timeout))

    with pytest.raises(RetryableFetchError):
        fetch(client)
