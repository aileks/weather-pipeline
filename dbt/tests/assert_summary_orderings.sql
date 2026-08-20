-- Aggregate orderings that must hold for every summary row
-- (docs/transformation.md: deterministic, internally consistent aggregates).
select *
from {{ ref('daily_weather_summary') }}
where temp_c_min > temp_c_max
   or wind_kmh_avg > wind_kmh_max
   or pressure_msl_hpa_min > pressure_msl_hpa_max
   or humidity_pct_min > humidity_pct_max
