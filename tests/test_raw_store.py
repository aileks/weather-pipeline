import datetime as dt

import duckdb
import pytest

from conftest import observation_row
from weather_pipeline.landing import write_snapshot
from weather_pipeline.raw_store import RawZoneError, derive_partition, rebuild_all

DAY = dt.date(2026, 8, 15)
OTHER_DAY = dt.date(2026, 8, 16)
FIRST_RUN_AT = dt.datetime(2026, 8, 16, 6, 0, 5, tzinfo=dt.UTC)
SECOND_RUN_AT = dt.datetime(2026, 8, 23, 6, 0, 4, tzinfo=dt.UTC)


@pytest.fixture()
def connection(tmp_path):
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    yield conn
    conn.close()


def day_rows(day: dt.date, temperature: float) -> list:
    base = observation_row(partition_date=day)
    second = observation_row(
        partition_date=day,
        city_id="london",
        hour_ts_utc=dt.datetime(day.year, day.month, day.day, 15, 0),  # noqa: DTZ001
        temperature_2m=temperature,
    )
    return [base, second]


def table_count(connection, partition_date: dt.date | None = None) -> int:
    if partition_date is None:
        return connection.execute("SELECT count(*) FROM raw.weather_observations").fetchone()[0]
    return connection.execute(
        "SELECT count(*) FROM raw.weather_observations WHERE partition_date = ?",
        [partition_date],
    ).fetchone()[0]


def test_derive_bootstraps_the_table_and_lands_the_latest_snapshot(tmp_path, connection):
    write_snapshot(connection, tmp_path, day_rows(DAY, 11.3), DAY, FIRST_RUN_AT, "f41c2ab9")

    count = derive_partition(connection, tmp_path, DAY)

    assert count == 2
    assert table_count(connection, DAY) == 2


def test_derive_is_idempotent(tmp_path, connection):
    write_snapshot(connection, tmp_path, day_rows(DAY, 11.3), DAY, FIRST_RUN_AT, "f41c2ab9")
    derive_partition(connection, tmp_path, DAY)

    derive_partition(connection, tmp_path, DAY)

    assert table_count(connection, DAY) == 2


def test_derive_follows_the_latest_snapshot_not_the_first(tmp_path, connection):
    write_snapshot(connection, tmp_path, day_rows(DAY, 11.3), DAY, FIRST_RUN_AT, "f41c2ab9")
    write_snapshot(connection, tmp_path, day_rows(DAY, 19.4), DAY, SECOND_RUN_AT, "8d07e315")

    derive_partition(connection, tmp_path, DAY)

    temperature = connection.execute(
        "SELECT temperature_2m FROM raw.weather_observations WHERE city_id = 'london'"
    ).fetchone()[0]
    assert temperature == 19.4
    assert table_count(connection, DAY) == 2


def test_derive_touches_only_its_own_partition_slice(tmp_path, connection):
    write_snapshot(connection, tmp_path, day_rows(DAY, 11.3), DAY, FIRST_RUN_AT, "f41c2ab9")
    write_snapshot(
        connection, tmp_path, day_rows(OTHER_DAY, 12.0), OTHER_DAY, FIRST_RUN_AT, "f41c2ab9"
    )
    derive_partition(connection, tmp_path, DAY)
    derive_partition(connection, tmp_path, OTHER_DAY)

    derive_partition(connection, tmp_path, DAY)

    assert table_count(connection, DAY) == 2
    assert table_count(connection, OTHER_DAY) == 2
    assert table_count(connection) == 4


def test_derive_without_a_snapshot_raises(tmp_path, connection):
    with pytest.raises(RawZoneError, match="no snapshot for partition 2026-08-15"):
        derive_partition(connection, tmp_path, DAY)


def test_rebuild_all_rederives_every_partition_from_the_landing_zone(tmp_path, connection):
    write_snapshot(connection, tmp_path, day_rows(DAY, 11.3), DAY, FIRST_RUN_AT, "f41c2ab9")
    write_snapshot(
        connection, tmp_path, day_rows(OTHER_DAY, 12.0), OTHER_DAY, FIRST_RUN_AT, "f41c2ab9"
    )
    derive_partition(connection, tmp_path, DAY)
    connection.execute("DELETE FROM raw.weather_observations")

    counts = rebuild_all(connection, tmp_path)

    assert counts == {DAY: 2, OTHER_DAY: 2}
    assert table_count(connection) == 4


def test_rebuild_all_on_an_empty_landing_zone_is_empty(tmp_path, connection):
    assert rebuild_all(connection, tmp_path) == {}
