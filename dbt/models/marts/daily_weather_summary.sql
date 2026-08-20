with observations as (

    select *
    from {{ ref('fct_hourly_weather') }}

),

daily as (

    select
        location_id,
        date_utc,
        min(temperature_c) as temp_c_min,
        max(temperature_c) as temp_c_max,
        avg(temperature_c) as temp_c_avg,
        avg(apparent_temperature_c) as apparent_c_avg,
        sum(precipitation_mm) as precipitation_mm_sum,
        avg(wind_speed_kmh) as wind_kmh_avg,
        max(wind_speed_kmh) as wind_kmh_max,
        min(pressure_msl_hpa) as pressure_msl_hpa_min,
        avg(pressure_msl_hpa) as pressure_msl_hpa_avg,
        max(pressure_msl_hpa) as pressure_msl_hpa_max,
        min(relative_humidity_pct) as humidity_pct_min,
        avg(relative_humidity_pct) as humidity_pct_avg,
        max(relative_humidity_pct) as humidity_pct_max,
        avg(cloud_cover_pct) as cloud_cover_pct_avg,
        count(*) as hours_observed
    from observations
    group by 1, 2

),

dominant_wind as (

    select location_id, date_utc, wind_direction_deg
    from (
        select
            location_id,
            date_utc,
            wind_direction_deg,
            row_number() over (
                partition by location_id, date_utc
                order by count(*) desc, wind_direction_deg asc
            ) as frequency_rank
        from observations
        where wind_direction_deg is not null
        group by 1, 2, 3
    )
    where frequency_rank = 1

),

dominant_code as (

    select location_id, date_utc, weather_code
    from (
        select
            location_id,
            date_utc,
            weather_code,
            row_number() over (
                partition by location_id, date_utc
                order by count(*) desc, weather_code asc
            ) as frequency_rank
        from observations
        where weather_code is not null
        group by 1, 2, 3
    )
    where frequency_rank = 1

),

anomaly_counts as (

    select
        location_id,
        cast(hour_ts_utc as date) as date_utc,
        count(*) as anomaly_count
    from {{ ref('weather_anomalies') }}
    group by 1, 2

)

select
    daily.location_id,
    daily.date_utc,
    daily.temp_c_min,
    daily.temp_c_max,
    daily.temp_c_avg,
    daily.apparent_c_avg,
    daily.precipitation_mm_sum,
    daily.wind_kmh_avg,
    daily.wind_kmh_max,
    dominant_wind.wind_direction_deg as dominant_wind_direction_deg,
    daily.pressure_msl_hpa_min,
    daily.pressure_msl_hpa_avg,
    daily.pressure_msl_hpa_max,
    daily.humidity_pct_min,
    daily.humidity_pct_avg,
    daily.humidity_pct_max,
    daily.cloud_cover_pct_avg,
    dominant_code.weather_code as dominant_weather_code,
    daily.hours_observed,
    coalesce(anomaly_counts.anomaly_count, 0) as anomaly_count
from daily
join {{ ref('dim_location') }} using (location_id)
join {{ ref('dim_date') }} on daily.date_utc = dim_date.date_day
-- left joins keep the grain: a city-day whose dominant column is fully
-- null keeps its summary row with a null dominant value
left join dominant_wind using (location_id, date_utc)
left join dominant_code using (location_id, date_utc)
left join anomaly_counts using (location_id, date_utc)
