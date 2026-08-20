import datetime as dt
import subprocess
import sys
import textwrap

import duckdb
import pytest
from starlette.testclient import TestClient

from weather_pipeline.ui import queries
from weather_pipeline.ui.app import create_app

TOKYO = ("tokyo", "Tokyo", "Japan", "Asia/Tokyo")
NEW_YORK = ("new-york", "New York", "United States", "America/New_York")
SUMMARY_COLUMNS = """
    location_id varchar, date_utc date, temp_c_min double, temp_c_max double,
    temp_c_avg double, apparent_c_avg double, precipitation_mm_sum double,
    wind_kmh_avg double, wind_kmh_max double, dominant_wind_direction_deg double,
    pressure_msl_hpa_min double, pressure_msl_hpa_avg double,
    pressure_msl_hpa_max double, humidity_pct_min double, humidity_pct_avg double,
    humidity_pct_max double, cloud_cover_pct_avg double, dominant_weather_code integer,
    hours_observed bigint, anomaly_count bigint
"""
DAY = dt.date(2026, 8, 18)


def _summary_row(location_id, date_utc, anomaly_count):
    return [
        location_id,
        date_utc,
        21.0,
        29.5,
        25.2,
        26.8,
        4.2,
        12.3,
        33.0,
        135.0,
        1002.1,
        1006.4,
        1010.2,
        62.0,
        74.5,
        88.0,
        55.0,
        3,
        24,
        anomaly_count,
    ]


def _anomaly_row(location_id, hour, variable, z_score):
    return [location_id, hour, variable, 33.1, 25.9, 2.3, 29, z_score]


@pytest.fixture
def warehouse_path(tmp_path):
    """A minimal warehouse with the schemas and columns the UI reads.

    Two cities, two summary days, one full day of hourly rows for Tokyo, and
    two planted flags: a temperature high-side flag in Tokyo at 14:00 UTC and
    a precipitation low-side flag in New York at 03:00 UTC.
    """
    path = tmp_path / "fixture.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("create schema core; create schema marts")
    conn.execute(
        """
        create table core.dim_location as
        select * from (values
          (?, ?, ?, 35.6762, 139.6503, ?, 'humid subtropical'),
          (?, ?, ?, 40.7128, -74.0060, ?, 'humid subtropical')
        ) as t(location_id, city_name, country, latitude, longitude, timezone, climate_zone)
        """,
        [*TOKYO, *NEW_YORK],
    )
    conn.execute(f"create table marts.daily_weather_summary ({SUMMARY_COLUMNS})")
    rows = [
        _summary_row("tokyo", DAY - dt.timedelta(days=1), 0),
        _summary_row("tokyo", DAY, 1),
        _summary_row("new-york", DAY, 1),
    ]
    placeholder_rows = ",".join(["(" + ",".join(["?"] * 20) + ")"] * len(rows))
    conn.execute(
        f"insert into marts.daily_weather_summary values {placeholder_rows}",
        [value for row in rows for value in row],
    )
    conn.execute(
        """
        create table core.fct_hourly_weather (
          location_id varchar, hour_ts_utc timestamp, date_utc date, is_day boolean,
          temperature_c double, relative_humidity_pct double, apparent_temperature_c double,
          precipitation_mm double, pressure_msl_hpa double, surface_pressure_hpa double,
          cloud_cover_pct double, wind_speed_kmh double, wind_direction_deg double,
          weather_code integer, ingested_at_utc timestamptz)
        """
    )
    for hour in range(24):
        conn.execute(
            "insert into core.fct_hourly_weather values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                "tokyo",
                dt.datetime(2026, 8, 18, hour),  # noqa: DTZ001
                DAY,
                6 <= hour <= 10,
                22.0 + hour * 0.3,
                60.0 + hour,
                23.0 + hour * 0.3,
                0.1 if hour % 7 == 0 else 0.0,
                1005.0 + hour * 0.2,
                1001.0 + hour * 0.2,
                50.0 + hour,
                10.0 + hour * 0.5,
                135.0,
                3,
                dt.datetime(2026, 8, 19, 6, 0, 5, tzinfo=dt.UTC),
            ],
        )
    conn.execute(
        """
        create table marts.weather_anomalies (
          location_id varchar, hour_ts_utc timestamp, variable varchar,
          observed_value double, baseline_mean double, baseline_std double,
          comparable_obs_count bigint, z_score double)
        """
    )
    conn.execute(
        "insert into marts.weather_anomalies values (?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?)",
        _anomaly_row("tokyo", dt.datetime(2026, 8, 18, 14), "temperature_c", 3.13)  # noqa: DTZ001
        + _anomaly_row("new-york", dt.datetime(2026, 8, 18, 3), "precipitation_mm", -3.4),  # noqa: DTZ001
    )
    conn.close()
    return path


@pytest.fixture
def client(warehouse_path):
    return TestClient(create_app(warehouse_path))


def test_overview_lists_each_city_with_latest_day(client):
    body = client.get("/").text
    assert "Tokyo" in body and "New York" in body
    assert "data through 2026-08-18 utc" in body
    assert 'href="/locations/tokyo/2026-08-18"' in body


def test_location_redirects_to_latest_date(client):
    response = client.get("/locations/tokyo", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/locations/tokyo/2026-08-18"


def test_daily_report_shows_flag_with_utc_and_local_hours(client):
    body = client.get("/locations/tokyo/2026-08-18").text
    assert "2026-08-18 14:00" in body  # utc hour of the flag
    assert "2026-08-18 23:00" in body  # same hour in Asia/Tokyo
    assert "+3.1" in body  # signed z score
    assert "weak" not in body  # temperature is not the weak detector
    assert "hourly-chart" in body and "chart-data" in body


def test_daily_report_renders_empty_state_for_future_date(client):
    body = client.get("/locations/tokyo/2026-08-30").text
    assert "no report for this day" in body
    assert "hourly-chart" not in body


def test_daily_report_rejects_unknown_location_and_bad_date(client):
    assert client.get("/locations/nowhere/2026-08-18").status_code == 404
    assert client.get("/locations/tokyo/not-a-date").status_code == 404


def test_explorer_lists_flags_with_local_time_rolling_back_a_day(client):
    body = client.get("/anomalies").text
    assert "showing 2 of 2" in body
    # 03:00 UTC in America/New_York is the previous calendar day, 23:00 local
    assert "2026-08-17 23:00" in body


def test_explorer_filters_by_city_variable_and_z_sign(client):
    body = client.get("/anomalies?location=tokyo&variable=temperature_c&z=high").text
    assert "+3.1" in body and "-3.4" not in body
    assert 'value="tokyo" selected' in body

    body = client.get("/anomalies?z=low").text
    assert "-3.4" in body and "+3.1" not in body
    assert "weak" in body  # precipitation carries the weak-detector tag

    body = client.get("/anomalies?date_from=2026-08-19").text
    assert "showing 0 of 0" in body


def test_explorer_rejects_unknown_variable(client):
    assert client.get("/anomalies?variable=bogus").status_code == 422


def test_calendar_heatmap_links_flagged_days(client):
    body = client.get("/calendar?location=tokyo").text
    assert "August" in body and "2026" in body
    assert 'href="/locations/tokyo/2026-08-18"' in body
    assert "c1" in body  # one flag that day


def test_missing_warehouse_renders_unavailable_page(tmp_path):
    client = TestClient(create_app(tmp_path / "absent.duckdb"))
    response = client.get("/")
    assert response.status_code == 503
    assert "warehouse unavailable" in response.text


def test_lock_conflict_recovers_after_one_retry(warehouse_path, monkeypatch):
    client = TestClient(create_app(warehouse_path))
    real_connect = duckdb.connect
    failures = {"count": 0}

    def flaky_connect(*args, **kwargs):
        if failures["count"] == 0:
            failures["count"] += 1
            raise duckdb.IOException(
                'IO Error: Could not set lock on file "x": Conflicting lock is held'
            )
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(queries.duckdb, "connect", flaky_connect)
    monkeypatch.setattr(queries.time, "sleep", lambda _: None)
    assert client.get("/").status_code == 200


def test_lock_conflict_surfaces_busy_page_after_deadline(warehouse_path, monkeypatch):
    client = TestClient(create_app(warehouse_path))

    def locked_connect(*args, **kwargs):
        raise duckdb.IOException(
            'IO Error: Could not set lock on file "x": Conflicting lock is held'
        )

    monkeypatch.setattr(queries.duckdb, "connect", locked_connect)
    monkeypatch.setattr(queries, "LOCK_RETRY_DEADLINE_SECONDS", 0.0)
    monkeypatch.setattr(queries.time, "sleep", lambda _: None)
    response = client.get("/")
    assert response.status_code == 503
    assert "warehouse busy" in response.text


def test_real_writer_process_makes_reads_retry_then_busy(warehouse_path, monkeypatch):
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import duckdb, time
                conn = duckdb.connect({str(warehouse_path)!r})
                print("held", flush=True)
                time.sleep(30)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        monkeypatch.setattr(queries, "LOCK_RETRY_DEADLINE_SECONDS", 0.3)
        client = TestClient(create_app(warehouse_path))
        response = client.get("/")
        assert response.status_code == 503
        assert "warehouse busy" in response.text
    finally:
        holder.kill()
        holder.wait()

    # once the writer releases the file, the same app reads again
    monkeypatch.setattr(queries, "LOCK_RETRY_DEADLINE_SECONDS", 5.0)
    client = TestClient(create_app(warehouse_path))
    assert client.get("/").status_code == 200
