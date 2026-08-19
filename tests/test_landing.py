import datetime as dt

import duckdb
import pytest

from conftest import observation_row
from weather_pipeline.landing import (
    SnapshotError,
    latest_snapshot,
    snapshot_path,
    write_snapshot,
)

DAY = dt.date(2026, 8, 15)
FIRST_RUN_AT = dt.datetime(2026, 8, 16, 6, 0, 5, tzinfo=dt.UTC)
LATER_RUN_AT = dt.datetime(2026, 8, 23, 6, 0, 4, tzinfo=dt.UTC)


@pytest.fixture()
def connection():
    conn = duckdb.connect()
    yield conn
    conn.close()


def two_rows() -> list:
    second_hour = dt.datetime(2026, 8, 15, 15, 0)  # noqa: DTZ001
    return [observation_row(), observation_row(hour_ts_utc=second_hour)]


def test_snapshot_path_is_deterministic_per_run(tmp_path):
    path = snapshot_path(tmp_path, DAY, FIRST_RUN_AT, "f41c2ab9-1234-5678")

    assert path == (
        tmp_path
        / "year=2026"
        / "month=08"
        / "day=15"
        / "ingested_at=2026-08-16T06-00-05Z_run=f41c2ab9.parquet"
    )


def test_write_lands_rows_atomically(tmp_path, connection):
    result = write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")

    assert result.reused is False
    count = connection.execute(
        f"SELECT count(*) FROM read_parquet('{result.path.as_posix()}')"
    ).fetchone()[0]
    assert count == 2
    assert not list(result.path.parent.glob("*.tmp"))


def test_same_run_write_reuses_its_snapshot_without_rewrite(tmp_path, connection):
    first = write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")
    content_before = first.path.read_bytes()

    again = write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")

    assert again.reused is True
    assert again.path == first.path
    assert again.path.read_bytes() == content_before
    assert len(list(first.path.parent.glob("ingested_at=*.parquet"))) == 1


def test_same_run_write_with_mismatched_row_count_raises(tmp_path, connection):
    write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")

    with pytest.raises(SnapshotError, match="holds 2 rows"):
        write_snapshot(
            connection, tmp_path, two_rows() + [observation_row()], DAY, FIRST_RUN_AT, "f41c2ab9"
        )


def test_new_run_appends_a_new_snapshot_and_becomes_latest(tmp_path, connection):
    first = write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")
    second = write_snapshot(connection, tmp_path, two_rows(), DAY, LATER_RUN_AT, "8d07e315")

    assert second.reused is False
    assert second.path != first.path
    assert first.path.exists()
    assert latest_snapshot(tmp_path, DAY) == second.path


def test_latest_selection_breaks_ties_by_run_id(tmp_path, connection):
    write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "aaaa1111")
    write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "bbbb2222")

    assert latest_snapshot(tmp_path, DAY).name.endswith("run=bbbb2222.parquet")


def test_stray_temp_files_are_ignored_by_selection(tmp_path, connection):
    result = write_snapshot(connection, tmp_path, two_rows(), DAY, FIRST_RUN_AT, "f41c2ab9")
    stray = result.path.with_name("ingested_at=9999-99-99T99-99-99Z_run=zzzzzzzz.parquet.tmp")
    stray.write_bytes(b"partial write")

    assert latest_snapshot(tmp_path, DAY) == result.path


def test_latest_snapshot_is_none_when_no_day_directory(tmp_path):
    assert latest_snapshot(tmp_path, DAY) is None
