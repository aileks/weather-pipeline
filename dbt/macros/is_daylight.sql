{#
  Daylight flag from the requested coordinates and a naive UTC timestamp.

  NOAA-style approximation: day-of-year drives solar declination, UTC hour
  plus longitude drives the hour angle; the sun is up when the elevation
  sine is positive. Ignores the equation of time, so edges are accurate to
  a few minutes of true sunrise/sunset, which is enough for a night-vs-day
  context flag. The archive API does not serve is_day (AGENTS.md).
#}
{% macro is_daylight(latitude, longitude, ts_utc) %}
(
    with solar as (
        select
            radians(-23.44 * cos(radians(360.0 * (extract(doy from {{ ts_utc }}) + 10.0) / 365.0)))
            as declination,
            radians(
                15.0
                * (extract(hour from {{ ts_utc }}) + extract(minute from {{ ts_utc }}) / 60.0 - 12.0)
                + {{ longitude }}
            ) as hour_angle
    )
    select
        sin(radians({{ latitude }})) * sin(declination)
        + cos(radians({{ latitude }})) * cos(declination) * cos(hour_angle)
        > 0
    from solar
)
{% endmacro %}
