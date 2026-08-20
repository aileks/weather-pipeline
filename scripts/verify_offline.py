"""Offline end-to-end verification: no network, throwaway warehouse.

Builds synthetic API responses for twenty UTC days, runs them through the
real partitioned assets (ingestion, blocking checks, fact group) and the
unpartitioned dbt group, then asserts the runbook's observable outcomes:
complete city-days in the summary, the planted outlier detected as an
anomaly, and a re-materialized partition leaving identical table state
(backfill equals scheduled).
"""

import datetime as dt
import math
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import dagster as dg
import duckdb
import httpx

from weather_pipeline.cities import load_cities
from weather_pipeline.defs.weather_assets import (
    expected_row_count,
    timestamps_within_partition,
    weather_observations,
)

WAREHOUSE_TAG = {"warehouse": "duckdb"}
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DAY = dt.date(2026, 7, 1)
DAYS = 20
OUTLIER_DAY_OFFSET = DAYS - 1
OUTLIER_CITY = "reykjavik"
OUTLIER_HOUR = 14
OUTLIER_TEMP = 55.0

INGESTION_ASSETS = [
    weather_observations,
    expected_row_count,
    timestamps_within_partition,
]

DBT_DIR = Path(__file__).parents[1].resolve() / "dbt"


class SyntheticApi:
    """Serves generated archive responses at the same seam as production."""

    def __init__(self, payloads: dict[dt.date, list]):
        self._payloads = payloads

    def get_client(self) -> httpx.Client:
        payloads = self._payloads

        def respond(request):
            day = request.url.params["start_date"]
            return httpx.Response(200, json=payloads[dt.date.fromisoformat(day)])

        return httpx.Client(transport=httpx.MockTransport(respond), base_url=BASE_URL)


def day_payload(day: dt.date, cities: list) -> list:
    locations = []
    for index, city in enumerate(cities):
        base = 8.0 + index * 3.0
        series = {
            variable: []
            for variable in (
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
        }
        times = []
        for hour in range(24):
            times.append(f"{day.isoformat()}T{hour:02d}:00")
            wobble = ((day.toordinal() + hour + index) % 7) * 0.3
            temperature = round(base + 6.0 * math.sin((hour - 6) / 24 * 2 * math.pi) + wobble, 1)
            if (
                day == START_DAY + dt.timedelta(days=OUTLIER_DAY_OFFSET)
                and city.city_id == OUTLIER_CITY
                and hour == OUTLIER_HOUR
            ):
                temperature = OUTLIER_TEMP
            series["temperature_2m"].append(temperature)
            series["apparent_temperature"].append(round(temperature - 1.2, 1))
            series["relative_humidity_2m"].append(40 + (hour % 5) * 10)
            series["precipitation"].append(0.4 if (day.toordinal() + hour) % 5 == 0 else 0.0)
            series["weather_code"].append(hour % 4)
            series["pressure_msl"].append(1010 + (hour % 3))
            series["surface_pressure"].append(1006 + (hour % 3))
            series["cloud_cover"].append((hour * 4 + index) % 101)
            series["wind_speed_10m"].append(10 + (hour % 6) * 2)
            series["wind_direction_10m"].append((hour * 15 + index * 30) % 360)
        locations.append(
            {
                "latitude": city.latitude,
                "longitude": city.longitude,
                "hourly_units": {
                    "time": "iso8601",
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
                },
                "hourly": {"time": times, **series},
            }
        )
    return locations


def run_day(resource, day: dt.date) -> bool:
    result = dg.materialize(
        INGESTION_ASSETS,
        resources={"open_meteo_http": resource},
        partition_key=day.isoformat(),
        tags=WAREHOUSE_TAG,
        raise_on_error=False,
    )
    return result.success


def main() -> int:
    repo = Path(__file__).parents[1]
    os.chdir(repo)
    workdir = Path(tempfile.mkdtemp(prefix="weather-verify-"))
    os.environ["WEATHER_PIPELINE_DUCKDB_PATH"] = str(workdir / "warehouse.duckdb")
    os.environ["WEATHER_PIPELINE_LANDING_DIR"] = str(workdir / "landing")

    cities = load_cities(repo / "dbt" / "seeds" / "cities.csv")
    days = [START_DAY + dt.timedelta(days=offset) for offset in range(DAYS)]
    api = SyntheticApi({day: day_payload(day, cities) for day in days})

    failures = []
    try:
        # the documented bootstrap: seed, staging view, and dimensions must
        # exist before the first fact partition runs
        bootstrap = subprocess.run(
            [sys.executable, str(repo / "scripts" / "bootstrap.py")],
            cwd=repo,
            capture_output=True,
            text=True,
            check=False,
        )
        if bootstrap.returncode != 0:
            failures.append(f"bootstrap failed:\n{bootstrap.stdout[-2000:]}")

        for day in days:
            if not run_day(api, day):
                failures.append(f"partition {day} failed")
        outlier_day = days[OUTLIER_DAY_OFFSET]

        # one full deterministic dbt build: fact for every ingested day,
        # dimensions, and marts
        build = subprocess.run(
            [str(Path(sys.executable).parent / "dbt"), "build", "--profiles-dir", "."],
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        if build.returncode != 0:
            failures.append(f"full dbt build failed:\n{build.stdout[-2000:]}")

        with duckdb.connect(os.environ["WEATHER_PIPELINE_DUCKDB_PATH"], read_only=True) as con:
            summary_rows, incomplete_days = con.execute(
                "SELECT count(*), count(*) FILTER (WHERE hours_observed <> 24) "
                "FROM marts.daily_weather_summary"
            ).fetchone()
            if summary_rows != DAYS * len(cities) or incomplete_days:
                failures.append(
                    f"summary expected {DAYS * len(cities)} complete city-days, "
                    f"got {summary_rows} ({incomplete_days} incomplete)"
                )

            planted = con.execute(
                "SELECT z_score FROM marts.weather_anomalies "
                "WHERE location_id = ? AND hour_ts_utc = ? AND variable = 'temperature_c'",
                [OUTLIER_CITY, dt.datetime.combine(outlier_day, dt.time(OUTLIER_HOUR))],
            ).fetchone()
            if planted is None or abs(planted[0]) < 3:
                failures.append(f"planted outlier not detected: {planted}")

            fact_before = con.execute(
                "SELECT count(*), sum(temperature_c) FROM core.fct_hourly_weather"
            ).fetchone()
            raw_before = con.execute("SELECT count(*) FROM raw.weather_observations").fetchone()[0]

        rematerialized = days[5]
        if not run_day(api, rematerialized):
            failures.append(f"re-materialization of {rematerialized} failed")
        with duckdb.connect(os.environ["WEATHER_PIPELINE_DUCKDB_PATH"], read_only=True) as con:
            fact_after = con.execute(
                "SELECT count(*), sum(temperature_c) FROM core.fct_hourly_weather"
            ).fetchone()
            raw_after = con.execute("SELECT count(*) FROM raw.weather_observations").fetchone()[0]
        if fact_after != fact_before or raw_after != raw_before:
            failures.append(
                f"backfill equivalence broken: fact {fact_before}->{fact_after}, "
                f"raw {raw_before}->{raw_after}"
            )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print(
        f"offline verification passed: {DAYS} days, {DAYS * len(cities)} summary "
        "city-days, planted outlier detected, re-materialization stable"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
