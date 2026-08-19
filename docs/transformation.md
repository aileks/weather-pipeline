# Transformation (dbt)

dbt owns everything between the raw table and the analytics-ready outputs. This document specifies every model, its purpose, inputs, outputs, configuration, when it runs, and its tests. The testing *strategy* (why these tests, what each layer catches) is [Quality & Testing](quality-testing.md); the *scheduling* of the two dbt asset groups is [Orchestration](orchestration.md). This document owns the model contracts; exact column-level schemas are consolidated in [Data Contracts](data-contracts.md).

Layering: staging cleans, core (the project's name for the intermediate + dimensional layer) holds reusable modeled entities, marts serve analytics. Nothing bypasses a layer; marts never read the raw zone directly.

## Project layout

```
dbt/
├── dbt_project.yml          # schema mapping, on_schema_change, vars defaults
├── profiles.yml             # duckdb target → ../warehouse/weather.duckdb
├── packages.yml             # dbt_utils (date spine, uniqueness tests)
├── seeds/
│   └── cities.csv           # the canonical city list
├── macros/
│   ├── is_daylight.sql      # solar elevation > 0, from lat + UTC timestamp
│   └── z_score.sql          # standardized value helper (see anomaly-detection.md)
├── models/
│   ├── staging/
│   │   ├── _staging__sources.yml
│   │   ├── _staging__models.yml
│   │   └── stg_hourly_observations.sql
│   ├── core/
│   │   ├── _core__models.yml
│   │   ├── fct_hourly_weather.sql
│   │   ├── dim_location.sql
│   │   └── dim_date.sql
│   └── marts/
│       ├── _marts__models.yml
│       ├── daily_weather_summary.sql
│       └── weather_anomalies.sql
└── tests/                   # singular tests (e.g. summary ordering invariants)
```

Conventions:

- Schemas are exactly `staging`, `core`, `marts` (a `generate_schema_name` macro drops dbt's default target prefix, the warehouse shows clean layer names).
- All datetime columns are UTC; `*_utc` suffixes make that non-optional in the reader's eye.
- `start_date` / `end_date` [vars](https://docs.getdbt.com/docs/build/project-variables) bound every partition-aware model. Defaults span everything, so a plain full build works without vars; Dagster passes the partition window per run.
- Tests use the dbt-core 1.11 syntax: `data_tests:` blocks, generic-test arguments under `arguments:`.

## Source mapping

`_staging__sources.yml` declares the raw table as a dbt **source**, with an asset-key mapping that welds the Dagster asset graph to the dbt DAG:

```yaml
sources:
  - name: raw
    schema: raw
    tables:
      - name: weather_observations
        meta:
          dagster:
            asset_key: ["raw", "weather_observations"]
```

The Python asset `raw_weather_observations` and this source resolve to the same asset key, so the Dagster UI shows one continuous lineage: ingestion → dbt models. dbt source tests (if any are added) would surface as asset observations rather than checks, a distinction that matters only if we add them, and one reason we don't rely on source tests.

## dim_location, the cities seed

`seeds/cities.csv` is the canonical list the whole system derives from (ingestion builds its request from the same list, see [Ingestion & Storage](ingestion-storage.md#configuration)):

| Column | Example | Notes |
|---|---|---|
| `city_id` | `reykjavik` | Stable slug; becomes `location_id` |
| `city_name` | `Reykjavík` | Display name |
| `country` | `Iceland` | n/a |
| `latitude` / `longitude` | `64.1466` / `-21.9426` | The **requested** coordinates (not the API's grid-snapped ones) |
| `timezone` | `Atlantic/Reykjavik` | IANA name, display context only, never transformation logic |
| `climate_zone` | `subpolar oceanic` | Analytics flavor |

Eight cities spanning hemispheres and climate regimes: New York, London, Tokyo, Sydney, São Paulo, Cairo, Mumbai, Reykjavík. Adding a city is a seed row plus a backfill of its partitions, no model changes.

`dim_location` (table, `core`) is a thin select over the seed; the seed is loaded by `dbt seed` before models in every run group (M3 milestone). Tests: `unique`/`not_null` on `location_id`, latitude in -90 to 90, longitude in -180 to 180.

## stg_hourly_observations

**View, `staging`.** The single cleaning boundary. Everything downstream trusts it completely, that is its job.

- **Input:** source `raw.weather_observations` (all partitions).
- **Output grain:** one row per `location_id` × `hour_ts_utc`.
- **Runs:** its DDL is refreshed by the unpartitioned dbt group (a view costs nothing; see [Orchestration](orchestration.md#two-dbt-asset-groups)).
- **Logic:**
  - Rename to analytical names: `temperature_c`, `relative_humidity_pct`, `apparent_temperature_c`, `precipitation_mm`, `weather_code`, `pressure_msl_hpa`, `surface_pressure_hpa`, `cloud_cover_pct`, `wind_speed_kmh`, `wind_direction_deg`.
  - Type: measures to `DOUBLE`, `weather_code` to `INT`, timestamps to `TIMESTAMP`; derive `date_utc` (DATE) from `hour_ts_utc`.
  - `location_id` from `city_id`; keep `ingested_at_utc` for lineage.
  - **Deduplicate** defensively: `row_number() over (partition by location_id, hour_ts_utc order by ingested_at_utc desc)` → keep first. The raw derivation should already guarantee uniqueness; this makes staging correct even if it ever doesn't.
  - Compute **`is_day`** with the `is_daylight` macro (below). The archive API does not serve this flag, so it is derived: NOAA solar-position approximation, day-of-year → solar declination, UTC time + longitude → hour angle, and `is_day = solar_elevation > 0`. Accurate to a few minutes of true sunrise/sunset, which is fine for a flag whose purpose is filtering night-vs-day context.
- **Model-level doc note:** `precipitation_mm` is the **preceding hour's sum**, never averaged.

Tests (in `_staging__models.yml`): `dbt_utils.unique_combination_of_columns` on (`location_id`, `hour_ts_utc`); `not_null` on keys and `date_utc`; `accepted_ranges`-style `accepted_values`/expression bounds: temperature -60 to 60, humidity 0 to 100, cloud cover 0 to 100, wind 0 to 300 km/h, precipitation at least 0, pressures 300 to 1100 hPa; `weather_code` in the WMO code set (0-3, 45/48, 51-67, 71-77, 80-82, 85/86, 95-99).

## fct_hourly_weather

**Incremental table, `core`.** The atomic fact, everything analytical is built from it.

- **Input:** `ref('stg_hourly_observations')`, filtered to the run window when run incrementally.
- **Output grain:** one row per `location_id` × `hour_ts_utc`, the grain `dim_location` and `dim_date` exist to decorate.
- **Runs:** in the **partitioned** dbt group, Dagster passes `start_date`/`end_date` vars for exactly one UTC day; the same SQL run without vars builds the full table (first run, or `--full-refresh`).
- **Configuration:**

```yaml
config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['location_id', 'hour_ts_utc'],
)
```

  `delete+insert` with a composite natural key: re-materializing a partition deletes that day's rows and re-inserts them, the same converging semantics as the raw zone, one layer down. `on_schema_change` is `sync_all_columns` project-wide (M3), so added columns fail loudly rather than silently truncating.
- **Columns:** the staging grain plus measures and lineage; the exact schema is in [Data Contracts](data-contracts.md).
- **Why incremental at ~2k rows/day:** not volume, *semantics*. The fact must absorb re-fetched (revised) partitions without touching the rest of history, and the pattern is the production-shaped one. Cost of full rebuilds is negligible; the marts deliberately choose the other tradeoff.

Tests: `unique_combination_of_columns` (`location_id`, `hour_ts_utc`); `not_null` keys; `relationships` → `dim_location.location_id` and → `dim_date.date_day`; the same physical-range bounds as staging (re-checked at the fact boundary, where dimensions join).

## dim_date

**Table, `core`.** `dbt_utils.date_spine` at day grain over the fact's date range, extended to the end of the current UTC week (so the spine doesn't lag mid-week). Standard calendar attributes (year through `is_weekend`); the exact column list is in [Data Contracts](data-contracts.md).

Tests: `unique`/`not_null` on `date_day`; `accepted_values` boolean for `is_weekend`.

Runs in the unpartitioned group (it has no partition window, it always covers all data).

## daily_weather_summary

**Table, `marts`.** Per `location_id` × `date_utc`, one row of daily aggregates: temperature (min/max/avg plus apparent avg), precipitation sum, wind (avg/max plus dominant direction), pressure and humidity min/avg/max, cloud cover average, dominant weather code, `hours_observed`, and `anomaly_count`. The exact column list is in [Data Contracts](data-contracts.md).

Every aggregate is deterministic under ties: `dominant_*` uses mode with smallest-value tiebreak; `anomaly_count` left-joins `weather_anomalies` (this mart depends on the anomalies mart, both are rebuilt in the same group, dbt orders them). `hours_observed` bounds how partial a day may be (tests accept 1 to 24).

Tests: `unique_combination_of_columns` (`location_id`, `date_utc`); `relationships` to both dims; `hours_observed` 1 to 24; `anomaly_count` at least 0; singular tests assert `temp_c_min` <= `temp_c_max`, `wind_kmh_avg` <= `wind_kmh_max`, `pressure_msl_hpa_min` <= `pressure_msl_hpa_max`.

## weather_anomalies

**Table, `marts`.** Grain: one row per (`location_id`, `hour_ts_utc`, `variable`), only for observations flagged as anomalies. The exact schema is in [Data Contracts](data-contracts.md).

The full specification, baseline window, hour-of-day matching, thresholds, the worked example, and the honest limitations (especially precipitation), is [Anomaly Detection](anomaly-detection.md), which owns that subject.

Tests: `unique_combination_of_columns` on the grain; `not_null` throughout; \|z\| ≥ 3; `comparable_obs_count` ≥ 14; `relationships` of (`location_id`, `hour_ts_utc`) back to the fact; `accepted_values` on `variable`.

## When models run, in one table

| Model | Group | Partitioned? | Refreshed |
|---|---|---|---|
| `stg_hourly_observations` | unpartitioned | n/a (view) | every group run |
| `dim_location` | unpartitioned | no | every group run (seed first) |
| `dim_date` | unpartitioned | no | every group run |
| `fct_hourly_weather` | partitioned | yes (UTC day) | per partition run |
| `daily_weather_summary` | unpartitioned | no (full rebuild) | after any partition run, via automation condition |
| `weather_anomalies` | unpartitioned | no (full rebuild) | after any partition run, via automation condition |

The why of this split, and why the marts rebuild fully while the fact is incremental, is in [Design Decisions](design-decisions.md); the how is [Orchestration](orchestration.md#two-dbt-asset-groups).
