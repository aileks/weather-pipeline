"""Read-only warehouse access for the report UI.

Connection policy (docs/reporting-ui.md): each request opens the DuckDB file
read-only just long enough to run its queries, then closes it, so a pipeline
run can take the write lock between requests. A conflicting lock is retried
briefly before surfacing as WarehouseBusyError; any other open failure, such
as a warehouse that has not been built yet, propagates unchanged.
"""

import datetime as dt
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

LOCK_RETRY_INTERVAL_SECONDS = 0.25
LOCK_RETRY_DEADLINE_SECONDS = 5.0


class WarehouseBusyError(RuntimeError):
    """A pipeline run held the warehouse lock for the whole retry window."""


@contextmanager
def connect(path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    deadline = time.monotonic() + LOCK_RETRY_DEADLINE_SECONDS
    while True:
        try:
            conn = duckdb.connect(str(path), read_only=True)
            break
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower():
                raise
            if time.monotonic() >= deadline:
                raise WarehouseBusyError(f"warehouse locked by a pipeline run: {path}") from exc
            time.sleep(LOCK_RETRY_INTERVAL_SECONDS)
    try:
        yield conn
    finally:
        conn.close()


def _rows(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict]:
    conn.execute(sql, params or [])
    columns = [column[0] for column in conn.description]
    return [dict(zip(columns, row)) for row in conn.fetchall()]


def locations(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    return _rows(
        conn,
        """
        select location_id, city_name, country, timezone
        from core.dim_location
        order by location_id
        """,
    )


def location(conn: duckdb.DuckDBPyConnection, location_id: str) -> dict | None:
    rows = _rows(
        conn,
        """
        select location_id, city_name, country, timezone
        from core.dim_location
        where location_id = ?
        """,
        [location_id],
    )
    return rows[0] if rows else None


def latest_summaries(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Each location's newest daily summary row, for the overview cards."""
    return _rows(
        conn,
        """
        select l.location_id, l.city_name, l.country, l.timezone,
               s.date_utc, s.temp_c_min, s.temp_c_max, s.temp_c_avg,
               s.dominant_weather_code, s.anomaly_count
        from core.dim_location l
        join marts.daily_weather_summary s on s.location_id = l.location_id
        qualify row_number() over (partition by l.location_id order by s.date_utc desc) = 1
        order by l.location_id
        """,
    )


def latest_date(conn: duckdb.DuckDBPyConnection, location_id: str) -> dt.date | None:
    rows = _rows(
        conn,
        """
        select max(date_utc) as latest
        from marts.daily_weather_summary
        where location_id = ?
        """,
        [location_id],
    )
    return rows[0]["latest"]


def summary(conn: duckdb.DuckDBPyConnection, location_id: str, date_utc: dt.date) -> dict | None:
    rows = _rows(
        conn,
        """
        select *
        from marts.daily_weather_summary
        where location_id = ? and date_utc = ?
        """,
        [location_id, date_utc],
    )
    return rows[0] if rows else None


def hours(conn: duckdb.DuckDBPyConnection, location_id: str, date_utc: dt.date) -> list[dict]:
    return _rows(
        conn,
        """
        select hour_ts_utc, temperature_c, apparent_temperature_c, precipitation_mm,
               relative_humidity_pct, wind_speed_kmh, wind_direction_deg,
               pressure_msl_hpa, cloud_cover_pct, weather_code, is_day
        from core.fct_hourly_weather
        where location_id = ? and date_utc = ?
        order by hour_ts_utc
        """,
        [location_id, date_utc],
    )


def day_anomalies(
    conn: duckdb.DuckDBPyConnection, location_id: str, date_utc: dt.date
) -> list[dict]:
    return _rows(
        conn,
        """
        select hour_ts_utc, variable, observed_value, baseline_mean,
               baseline_std, comparable_obs_count, z_score
        from marts.weather_anomalies
        where location_id = ? and cast(hour_ts_utc as date) = ?
        order by hour_ts_utc, variable
        """,
        [location_id, date_utc],
    )


def explorer_anomalies(
    conn: duckdb.DuckDBPyConnection,
    *,
    location_id: str | None = None,
    variable: str | None = None,
    date_from: dt.date | None = None,
    date_to: dt.date | None = None,
    z_sign: str | None = None,
    limit: int = 500,
) -> tuple[list[dict], int]:
    """Flags across all locations, newest first, plus the untruncated total."""
    where: list[str] = []
    params: list = []
    if location_id is not None:
        where.append("a.location_id = ?")
        params.append(location_id)
    if variable is not None:
        where.append("a.variable = ?")
        params.append(variable)
    if date_from is not None:
        where.append("cast(a.hour_ts_utc as date) >= ?")
        params.append(date_from)
    if date_to is not None:
        where.append("cast(a.hour_ts_utc as date) <= ?")
        params.append(date_to)
    if z_sign == "high":
        where.append("a.z_score > 0")
    elif z_sign == "low":
        where.append("a.z_score < 0")
    clause = f"where {' and '.join(where)}" if where else ""

    rows = _rows(
        conn,
        f"""
        select a.location_id, l.city_name, l.timezone, a.hour_ts_utc, a.variable,
               a.observed_value, a.baseline_mean, a.baseline_std,
               a.comparable_obs_count, a.z_score
        from marts.weather_anomalies a
        join core.dim_location l on l.location_id = a.location_id
        {clause}
        order by a.hour_ts_utc desc, a.location_id, a.variable
        limit ?
        """,
        [*params, limit],
    )
    total = _rows(
        conn,
        f"""
        select count(*) as total
        from marts.weather_anomalies a
        {clause}
        """,
        params,
    )[0]["total"]
    return rows, total


def month_counts(
    conn: duckdb.DuckDBPyConnection,
    location_id: str,
    date_from: dt.date,
    date_to: dt.date,
) -> list[dict]:
    return _rows(
        conn,
        """
        select date_utc, anomaly_count
        from marts.daily_weather_summary
        where location_id = ? and date_utc >= ? and date_utc <= ?
        order by date_utc
        """,
        [location_id, date_from, date_to],
    )


def anomaly_span(conn: duckdb.DuckDBPyConnection) -> tuple[dt.date, dt.date] | None:
    rows = _rows(
        conn,
        """
        select min(cast(hour_ts_utc as date)) as first, max(cast(hour_ts_utc as date)) as last
        from marts.weather_anomalies
        """,
    )
    span = rows[0]
    if span["first"] is None:
        return None
    return span["first"], span["last"]
