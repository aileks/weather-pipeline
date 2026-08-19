with ranked as (

    select
        *,
        row_number() over (
            partition by city_id, hour_ts_utc
            order by ingested_at_utc desc
        ) as recency
    from {{ source('raw', 'weather_observations') }}

), joined as (

    -- coordinates come from the seed (requested values), never the
    -- grid-snapped provenance columns
    select
        ranked.*,
        cities.latitude as requested_latitude,
        cities.longitude as requested_longitude
    from ranked
    join {{ ref('cities') }} cities on ranked.city_id = cities.city_id
    where ranked.recency = 1

)

select
    city_id as location_id,
    hour_ts_utc,
    cast(hour_ts_utc as date) as date_utc,
    {{ is_daylight('requested_latitude', 'requested_longitude', 'hour_ts_utc') }} as is_day,
    cast(temperature_2m as double) as temperature_c,
    cast(relative_humidity_2m as double) as relative_humidity_pct,
    cast(apparent_temperature as double) as apparent_temperature_c,
    cast(precipitation as double) as precipitation_mm,
    cast(weather_code as integer) as weather_code,
    cast(pressure_msl as double) as pressure_msl_hpa,
    cast(surface_pressure as double) as surface_pressure_hpa,
    cast(cloud_cover as double) as cloud_cover_pct,
    cast(wind_speed_10m as double) as wind_speed_kmh,
    cast(wind_direction_10m as double) as wind_direction_deg,
    ingested_at_utc
from joined
