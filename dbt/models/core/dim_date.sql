{#
  The spine covers the partition window (2026-07-01) through the end of the
  current UTC week, independent of fact contents, so the dimension can be
  built before the first fact rows exist and the fact's relationship tests
  pass from the first partition (docs/orchestration.md#bootstrap-and-first-run-ordering).
#}
{% set spine_start = modules.datetime.date(2026, 7, 1) %}
{% set spine_end = modules.datetime.date.today() + modules.datetime.timedelta(days=7) %}

with spine as (

    select
        date_day::date as date_day
    from (
        {{ dbt_utils.date_spine(
            'day',
            "'" ~ spine_start ~ "'::date",
            "'" ~ spine_end ~ "'::date"
        ) }}
    )

)

select
    date_day,
    year(date_day)::integer as year,
    quarter(date_day)::integer as quarter,
    month(date_day)::integer as month,
    monthname(date_day) as month_name,
    day(date_day)::integer as day,
    isodow(date_day)::integer as day_of_week,
    dayname(date_day) as day_name,
    strftime(date_day, '%V')::integer as week_of_year,
    isodow(date_day) in (6, 7) as is_weekend
from spine
