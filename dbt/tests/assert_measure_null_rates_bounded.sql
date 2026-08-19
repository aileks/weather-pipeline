-- Each measure column's null fraction must stay at or below 5% per
-- location-day, so a quietly degrading variable fails a materialization
-- instead of thinning anomaly baselines (docs/quality-testing.md).
select
    location_id,
    date_utc
from {{ ref('stg_hourly_observations') }}
group by location_id, date_utc
having
    avg(case when temperature_c is null then 1.0 else 0.0 end) > 0.05
    or avg(case when relative_humidity_pct is null then 1.0 else 0.0 end) > 0.05
    or avg(case when apparent_temperature_c is null then 1.0 else 0.0 end) > 0.05
    or avg(case when precipitation_mm is null then 1.0 else 0.0 end) > 0.05
    or avg(case when pressure_msl_hpa is null then 1.0 else 0.0 end) > 0.05
    or avg(case when surface_pressure_hpa is null then 1.0 else 0.0 end) > 0.05
    or avg(case when cloud_cover_pct is null then 1.0 else 0.0 end) > 0.05
    or avg(case when wind_speed_kmh is null then 1.0 else 0.0 end) > 0.05
    or avg(case when wind_direction_deg is null then 1.0 else 0.0 end) > 0.05
