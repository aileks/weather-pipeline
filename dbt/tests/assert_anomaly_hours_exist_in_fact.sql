-- Every flagged hour must exist in the fact (data-contracts.md:
-- relationships (location_id, hour_ts_utc) into fct_hourly_weather).
select
    a.location_id,
    a.hour_ts_utc,
    a.variable
from {{ ref('weather_anomalies') }} a
left join {{ ref('fct_hourly_weather') }} f
    on a.location_id = f.location_id
    and a.hour_ts_utc = f.hour_ts_utc
where f.location_id is null
