import datetime as dt
from pathlib import Path

import dagster as dg
import duckdb
import httpx
import pytest

from conftest import RECORDED_DAY, observation_row, recorded_payload
from weather_pipeline.defs import weather_assets
from weather_pipeline.landing import write_snapshot
from weather_pipeline.raw_store import derive_partition
from weather_pipeline.settings import Settings

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


class MockApi:
    """Test adapter at the same seam production uses: the httpx client."""

    def __init__(self, payload, status: int = 200):
        self._payload = payload
        self._status = status

    def get_client(self) -> httpx.Client:
        status, payload = self._status, self._payload

        def respond(request):
            return httpx.Response(status, json=payload)

        return httpx.Client(transport=httpx.MockTransport(respond), base_url=BASE_URL)


@pytest.fixture()
def env_paths(tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    warehouse = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("WEATHER_PIPELINE_LANDING_DIR", str(landing))
    monkeypatch.setenv("WEATHER_PIPELINE_DUCKDB_PATH", str(warehouse))
    return landing, warehouse


def raw_count(warehouse: Path, partition_date: dt.date | None = None) -> int:
    if not warehouse.exists():
        return 0
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        table_exists = connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'raw' AND table_name = 'weather_observations'"
        ).fetchone()[0]
        if not table_exists:
            return 0
        if partition_date is None:
            return connection.execute("SELECT count(*) FROM raw.weather_observations").fetchone()[0]
        return connection.execute(
            "SELECT count(*) FROM raw.weather_observations WHERE partition_date = ?",
            [partition_date],
        ).fetchone()[0]


def materialize_day(resource, raise_on_error: bool = True) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [
            weather_assets.weather_observations,
            weather_assets.expected_row_count,
            weather_assets.timestamps_within_partition,
        ],
        resources={"open_meteo_http": resource},
        partition_key=RECORDED_DAY.isoformat(),
        raise_on_error=raise_on_error,
    )


def test_materialization_lands_snapshot_and_derives_raw_zone(env_paths):
    landing, warehouse = env_paths

    result = materialize_day(MockApi(recorded_payload()))

    assert result.success
    assert raw_count(warehouse, RECORDED_DAY) == 192
    snapshots = list((landing / "year=2026/month=08/day=17").glob("ingested_at=*.parquet"))
    assert len(snapshots) == 1


def test_rematerialization_adds_a_snapshot_and_keeps_the_raw_slice_stable(env_paths):
    landing, warehouse = env_paths

    assert materialize_day(MockApi(recorded_payload())).success
    assert materialize_day(MockApi(recorded_payload())).success

    snapshots = list((landing / "year=2026/month=08/day=17").glob("ingested_at=*.parquet"))
    assert len(snapshots) == 2
    assert raw_count(warehouse, RECORDED_DAY) == 192


def test_deterministic_api_error_fails_the_run(env_paths):
    payload = {"reason": "Parameter 'start_date' is out of allowed range", "error": True}

    result = materialize_day(MockApi(payload, status=400), raise_on_error=False)

    assert not result.success
    assert raw_count(env_paths[1], RECORDED_DAY) == 0


def test_retryable_api_error_exhausts_retries_and_fails(env_paths, monkeypatch):
    monkeypatch.setattr(weather_assets, "RETRY_WAIT_SECONDS", 0)

    result = materialize_day(MockApi({}, status=503), raise_on_error=False)

    assert not result.success
    assert raw_count(env_paths[1], RECORDED_DAY) == 0


def land_single_row_slice(landing: Path, warehouse: Path) -> None:
    connection = duckdb.connect(str(warehouse))
    write_snapshot(
        connection,
        landing,
        [observation_row(partition_date=RECORDED_DAY)],
        RECORDED_DAY,
        dt.datetime(2026, 8, 18, 6, 0, 5, tzinfo=dt.UTC),
        "f41c2ab9",
    )
    derive_partition(connection, landing, RECORDED_DAY)
    connection.close()


def test_row_count_check_fails_on_an_incomplete_slice(env_paths):
    landing, warehouse = env_paths
    land_single_row_slice(landing, warehouse)

    result = weather_assets._expected_row_count_result(Settings.from_env(), RECORDED_DAY)

    assert result.passed is False
    assert result.metadata["row_count"].value == 1


def test_timestamp_check_rejects_hours_outside_the_partition(env_paths):
    landing, warehouse = env_paths
    land_single_row_slice(landing, warehouse)
    connection = duckdb.connect(str(warehouse))
    connection.execute(
        "UPDATE raw.weather_observations SET hour_ts_utc = TIMESTAMP '2026-08-18 01:00:00'"
    )
    connection.close()

    result = weather_assets._timestamps_within_partition_result(Settings.from_env(), RECORDED_DAY)

    assert result.passed is False
