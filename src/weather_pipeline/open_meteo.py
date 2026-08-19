"""Open-Meteo archive API client.

One deep function: ``fetch_observations`` hides the request shape, response
validation, unit assertions, and error classification. The httpx client is
injected so tests exercise the same seam as callers.

The API contract this module enforces lives in
docs/ingestion-storage.md#the-source-open-meteo-historical-weather-api.
"""

import datetime as dt
from dataclasses import dataclass

import httpx

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
)

EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "apparent_temperature": "°C",
    "precipitation": "mm",
    "weather_code": "wmo code",
    "pressure_msl": "hPa",
    "surface_pressure": "hPa",
    "cloud_cover": "%",
    "wind_speed_10m": "km/h",
    "wind_direction_10m": "°",
}

MEASURE_VARIABLES = tuple(v for v in HOURLY_VARIABLES if v != "weather_code")


class RetryableFetchError(Exception):
    """A plausibly transient failure: timeout, connection error, 429, or 5xx."""


class InvalidResponseError(Exception):
    """A deterministic failure: HTTP 400, unexpected shape, or changed units."""


@dataclass(frozen=True)
class ObservationRow:
    partition_date: dt.date
    city_id: str
    hour_ts_utc: dt.datetime
    temperature_2m: float | None
    relative_humidity_2m: float | None
    apparent_temperature: float | None
    precipitation: float | None
    weather_code: int | None
    pressure_msl: float | None
    surface_pressure: float | None
    cloud_cover: float | None
    wind_speed_10m: float | None
    wind_direction_10m: float | None
    latitude: float
    longitude: float
    ingested_at_utc: dt.datetime
    source_url: str


def _context(base_url: str, day: dt.date) -> str:
    return f"endpoint={base_url} partition={day.isoformat()}"


def _parse_hour_times(times: list, day: dt.date, context: str) -> list[dt.datetime]:
    if len(times) != 24:
        raise InvalidResponseError(f"{context}: expected 24 hourly timestamps, got {len(times)}")

    parsed = []
    for value in times:
        try:
            # Naive by contract: the API returns UTC wall times with no offset
            # and the raw zone stores naive UTC (docs/data-contracts.md).
            parsed.append(
                dt.datetime.strptime(value, "%Y-%m-%dT%H:%M")  # noqa: DTZ007
            )
        except (TypeError, ValueError) as error:
            raise InvalidResponseError(f"{context}: bad hourly time {value!r}") from error

    start = dt.datetime.combine(day, dt.time.min)
    expected = [start + dt.timedelta(hours=hour) for hour in range(24)]
    if parsed != expected:
        raise InvalidResponseError(f"{context}: hourly times do not span the partition day exactly")
    return parsed


def _parse_location(
    payload: dict, city, day: dt.date, ingested_at: dt.datetime, source_url: str, context: str
) -> list[ObservationRow]:
    hourly = payload.get("hourly")
    units = payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise InvalidResponseError(f"{context}: location {city.city_id} lacks hourly data")

    for variable, expected_unit in EXPECTED_UNITS.items():
        actual = units.get(variable)
        if actual is None or actual.casefold() != expected_unit.casefold():
            raise InvalidResponseError(
                f"{context}: unit change for {variable}: expected {expected_unit!r}, got {actual!r}"
            )

    times = _parse_hour_times(hourly.get("time", []), day, context)

    values: dict[str, list[float | None | int]] = {}
    for variable in HOURLY_VARIABLES:
        series = hourly.get(variable)
        if not isinstance(series, list) or len(series) != 24:
            raise InvalidResponseError(
                f"{context}: variable {variable} missing or not 24 values for {city.city_id}"
            )
        values[variable] = series

    try:
        latitude = float(payload["latitude"])
        longitude = float(payload["longitude"])
    except (KeyError, TypeError, ValueError) as error:
        raise InvalidResponseError(
            f"{context}: location {city.city_id} lacks usable coordinates"
        ) from error

    rows = []
    for hour in range(24):
        row = {
            variable: None if values[variable][hour] is None else float(values[variable][hour])
            for variable in MEASURE_VARIABLES
        }
        raw_code = values["weather_code"][hour]
        rows.append(
            ObservationRow(
                partition_date=day,
                city_id=city.city_id,
                hour_ts_utc=times[hour],
                weather_code=None if raw_code is None else int(raw_code),
                latitude=latitude,
                longitude=longitude,
                ingested_at_utc=ingested_at,
                source_url=source_url,
                **row,
            )
        )
    return rows


def fetch_observations(
    client: httpx.Client,
    base_url: str,
    cities: list,
    day: dt.date,
    ingested_at: dt.datetime,
) -> list[ObservationRow]:
    """Fetch one UTC day for all cities and return validated typed rows.

    Raises RetryableFetchError for transient transport failures and
    InvalidResponseError for every deterministic problem; both messages carry
    the endpoint and partition day.
    """
    context = _context(base_url, day)
    params = {
        "latitude": ",".join(str(city.latitude) for city in cities),
        "longitude": ",".join(str(city.longitude) for city in cities),
        "hourly": ",".join(HOURLY_VARIABLES),
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "timezone": "UTC",
    }

    try:
        response = client.get(base_url, params=params)
    except (httpx.TimeoutException, httpx.TransportError) as error:
        raise RetryableFetchError(f"{context}: transport failure") from error

    if response.status_code in (429,) or response.status_code >= 500:
        raise RetryableFetchError(f"{context}: HTTP {response.status_code}")
    if response.status_code != 200:
        reason = None
        try:
            body = response.json()
            if isinstance(body, dict):
                reason = body.get("reason")
        except ValueError:
            pass
        raise InvalidResponseError(f"{context}: HTTP {response.status_code} reason={reason!r}")

    try:
        payload = response.json()
    except ValueError as error:
        raise InvalidResponseError(f"{context}: response is not JSON") from error

    if not isinstance(payload, list):
        raise InvalidResponseError(
            f"{context}: expected a top-level location array, got {type(payload).__name__}"
        )
    if len(payload) != len(cities):
        raise InvalidResponseError(
            f"{context}: expected {len(cities)} location objects, got {len(payload)}"
        )

    source_url = str(response.request.url)
    rows: list[ObservationRow] = []
    for location_payload, city in zip(payload, cities):
        if not isinstance(location_payload, dict):
            raise InvalidResponseError(
                f"{context}: location entry for {city.city_id} is not an object"
            )
        rows.extend(_parse_location(location_payload, city, day, ingested_at, source_url, context))
    return rows
