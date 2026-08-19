{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['location_id', 'hour_ts_utc'],
) }}

-- The persisted, keyed instance of the cleaned grain: re-running a window
-- deletes and re-inserts exactly that window's rows, the same converging
-- semantics as the raw zone one layer down (docs/transformation.md).
select
    location_id,
    hour_ts_utc,
    date_utc,
    is_day,
    temperature_c,
    relative_humidity_pct,
    apparent_temperature_c,
    precipitation_mm,
    weather_code,
    pressure_msl_hpa,
    surface_pressure_hpa,
    cloud_cover_pct,
    wind_speed_kmh,
    wind_direction_deg,
    ingested_at_utc
from {{ ref('stg_hourly_observations') }}
where date_utc >= '{{ var("start_date") }}'::date
  and date_utc <= '{{ var("end_date") }}'::date
