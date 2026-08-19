# Data Contracts

This document is the authoritative schema reference for the warehouse: the grain, keys, and columns of every table and model, in one place. It owns schemas only. Behavior and rationale live with their topic documents: the raw table's write path in [Ingestion & Storage](ingestion-storage.md), model logic and tests in [Transformation](transformation.md), run scheduling in [Orchestration](orchestration.md), the anomaly specification in [Anomaly Detection](anomaly-detection.md).

The implementation milestones must produce tables that match this contract; a schema change starts here. Types are intended DuckDB types. All timestamps are UTC; `*_utc` suffixes and `date_utc` make that visible in every schema.

## How the models relate

- `stg_hourly_observations` reads `raw.weather_observations`.
- `fct_hourly_weather` reads `stg_hourly_observations`.
- `dim_location` is loaded from the `cities` seed.
- `dim_date` spans the fact's date range, extended to the end of the current UTC week.
- `weather_anomalies` reads `fct_hourly_weather`.
- `daily_weather_summary` reads `fct_hourly_weather`, `dim_location`, `dim_date`, and `weather_anomalies`.

Relationships are tested by dbt `relationships` tests rather than enforced by the engine. Uniqueness and not-null constraints are likewise dbt tests; the table definitions themselves carry no enforced keys.

## raw.weather_observations

DuckDB table, written by the ingestion asset (transactional delete+insert per partition). Owned operationally by [Ingestion & Storage](ingestion-storage.md#the-raw-zone-a-derived-table). The table mirrors the latest snapshot per partition; column set and meaning are the snapshot's ([snapshot contents](ingestion-storage.md#snapshot-contents)).

Grain: one row per city per hour. Unique within a partition: (`city_id`, `hour_ts_utc`).

| Column | Type | Notes |
|---|---|---|
| `partition_date` | DATE | The UTC day being fetched |
| `city_id` | VARCHAR | Stable slug, e.g. `reykjavik` |
| `hour_ts_utc` | TIMESTAMP | Naive UTC, from `hourly.time` |
| `temperature_2m` | DOUBLE | API name, nulls preserved |
| `relative_humidity_2m` | DOUBLE | API name, nulls preserved |
| `apparent_temperature` | DOUBLE | API name, nulls preserved |
| `precipitation` | DOUBLE | Sum of the preceding hour, never averaged |
| `weather_code` | INT | WMO code set |
| `pressure_msl` | DOUBLE | API name, nulls preserved |
| `surface_pressure` | DOUBLE | API name, nulls preserved |
| `cloud_cover` | DOUBLE | API name, nulls preserved |
| `wind_speed_10m` | DOUBLE | API name, nulls preserved |
| `wind_direction_10m` | DOUBLE | API name, nulls preserved |
| `latitude` / `longitude` | DOUBLE | Grid-cell centers as returned, provenance only, never join keys |
| `ingested_at_utc` | TIMESTAMPTZ | Which fetch produced the row |
| `source_url` | VARCHAR | Full request URL, the snapshot's provenance |

## stg_hourly_observations

View, `staging` schema. The single cleaning boundary; everything downstream trusts it. Logic in [Transformation](transformation.md#stg_hourly_observations).

Grain: one row per (`location_id`, `hour_ts_utc`), unique.

| Column | Type | Notes |
|---|---|---|
| `location_id` | VARCHAR | From `city_id` |
| `hour_ts_utc` | TIMESTAMP | Naive UTC |
| `date_utc` | DATE | Derived from `hour_ts_utc` |
| `is_day` | BOOLEAN | Derived by the `is_daylight` macro, not fetched |
| `temperature_c` | DOUBLE | |
| `relative_humidity_pct` | DOUBLE | |
| `apparent_temperature_c` | DOUBLE | |
| `precipitation_mm` | DOUBLE | Sum of the preceding hour |
| `weather_code` | INT | WMO code set |
| `pressure_msl_hpa` | DOUBLE | |
| `surface_pressure_hpa` | DOUBLE | |
| `cloud_cover_pct` | DOUBLE | |
| `wind_speed_kmh` | DOUBLE | |
| `wind_direction_deg` | DOUBLE | |
| `ingested_at_utc` | TIMESTAMPTZ | Kept for lineage |

## fct_hourly_weather

Incremental table, `core` schema, `delete+insert` strategy. Logic in [Transformation](transformation.md#fct_hourly_weather).

Grain: one row per (`location_id`, `hour_ts_utc`); that pair is the `unique_key`. Columns are exactly the staging view's column set; the fact is the persisted, keyed instance of the cleaned grain.

Relationships: `location_id` must exist in `dim_location`; `date_utc` must exist in `dim_date`.

## dim_location

Table, `core` schema, a thin select over the `cities` seed. The seed is the canonical city list; see [Transformation](transformation.md#dim_location-the-cities-seed).

Grain: one row per city; `location_id` is the primary key.

| Column | Type | Notes |
|---|---|---|
| `location_id` | VARCHAR | Stable slug |
| `city_name` | VARCHAR | Display name |
| `country` | VARCHAR | |
| `latitude` | DOUBLE | The requested coordinate |
| `longitude` | DOUBLE | The requested coordinate |
| `timezone` | VARCHAR | IANA name, display context only |
| `climate_zone` | VARCHAR | |

## dim_date

Table, `core` schema, `dbt_utils.date_spine` at day grain. Logic in [Transformation](transformation.md#dim_date).

Grain: one row per day; `date_day` is the primary key.

| Column | Type |
|---|---|
| `date_day` | DATE |
| `year` | INT |
| `quarter` | INT |
| `month` | INT |
| `month_name` | VARCHAR |
| `day` | INT |
| `day_of_week` | INT |
| `day_name` | VARCHAR |
| `week_of_year` | INT |
| `is_weekend` | BOOLEAN |

## daily_weather_summary

Table, `marts` schema, full rebuild. Logic in [Transformation](transformation.md#daily_weather_summary).

Grain: one row per (`location_id`, `date_utc`), unique.

| Column | Type | Notes |
|---|---|---|
| `location_id` | VARCHAR | Joins `dim_location` |
| `date_utc` | DATE | Joins `dim_date.date_day` |
| `temp_c_min` | DOUBLE | |
| `temp_c_max` | DOUBLE | |
| `temp_c_avg` | DOUBLE | |
| `apparent_c_avg` | DOUBLE | |
| `precipitation_mm_sum` | DOUBLE | Sum over observed hours |
| `wind_kmh_avg` | DOUBLE | |
| `wind_kmh_max` | DOUBLE | |
| `dominant_wind_direction_deg` | DOUBLE | Mode, smallest-value tiebreak |
| `pressure_msl_hpa_min` | DOUBLE | |
| `pressure_msl_hpa_avg` | DOUBLE | |
| `pressure_msl_hpa_max` | DOUBLE | |
| `humidity_pct_min` | DOUBLE | |
| `humidity_pct_avg` | DOUBLE | |
| `humidity_pct_max` | DOUBLE | |
| `cloud_cover_pct_avg` | DOUBLE | |
| `dominant_weather_code` | INT | Mode, smallest-value tiebreak |
| `hours_observed` | INT | 1 to 24 accepted by tests |
| `anomaly_count` | INT | From `weather_anomalies`, 0 when none |

## weather_anomalies

Table, `marts` schema, full rebuild. The statistical specification (baseline window, guard, threshold) is owned by [Anomaly Detection](anomaly-detection.md).

Grain: one row per (`location_id`, `hour_ts_utc`, `variable`), only for observations flagged as anomalies.

| Column | Type | Notes |
|---|---|---|
| `location_id` | VARCHAR | |
| `hour_ts_utc` | TIMESTAMP | |
| `variable` | VARCHAR | One of the four covered variables |
| `observed_value` | DOUBLE | |
| `baseline_mean` | DOUBLE | Mean of comparables |
| `baseline_std` | DOUBLE | Sample standard deviation of comparables |
| `comparable_obs_count` | INT | At least 14 or no row is emitted |
| `z_score` | DOUBLE | Emitted only at abs(z) >= 3.0 |

Relationships: (`location_id`, `hour_ts_utc`) must exist in `fct_hourly_weather`.
