select
    city_id as location_id,
    city_name,
    country,
    latitude,
    longitude,
    timezone,
    climate_zone
from {{ ref('cities') }}
