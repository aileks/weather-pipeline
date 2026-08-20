{#
  The statistical specification is owned by docs/anomaly-detection.md:
  comparables are the same variable, same location, same UTC hour of day in
  the trailing 30 days before the observation; at least 14 non-null
  comparables (count(value), matching what the mean and stddev use); flags
  at abs(z) >= 3. Deliberately a heuristic, not a robust detector, and
  precipitation_mm is a known-weak variable.
#}
with hourly as (

    select
        location_id,
        hour_ts_utc,
        variable,
        value
    from {{ ref('fct_hourly_weather') }}
    unpivot (
        value for variable in
        (temperature_c, precipitation_mm, wind_speed_kmh, surface_pressure_hpa)
    )
    where value is not null

),

scored as (

    select
        h.location_id,
        h.hour_ts_utc,
        h.variable,
        h.value as observed_value,
        avg(c.value) as baseline_mean,
        stddev_samp(c.value) as baseline_std,
        count(c.value) as comparable_obs_count
    from hourly h
    join hourly c
        on c.location_id = h.location_id
        and c.variable = h.variable
        and extract(hour from c.hour_ts_utc) = extract(hour from h.hour_ts_utc)
        and c.hour_ts_utc >= h.hour_ts_utc - interval 30 day
        and c.hour_ts_utc < h.hour_ts_utc
    group by 1, 2, 3, 4

),

flagged as (

    select
        location_id,
        hour_ts_utc,
        variable,
        observed_value,
        baseline_mean,
        baseline_std,
        comparable_obs_count,
        {{ z_score('observed_value', 'baseline_mean', 'baseline_std') }} as z_score
    from scored
    where comparable_obs_count >= 14

)

select *
from flagged
where abs(z_score) >= 3
