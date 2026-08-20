# Quality & Testing

Quality control is layered so that each class of defect is caught by the cheapest test that can catch it, at the moment it can be caught. Three layers, three jobs:

| Layer | Runs | Catches |
|---|---|---|
| **pytest** (unit/integration, mocked API) | on every commit, before anything ships | Broken parsing, snapshot/table mechanics, graph wiring |
| **dbt data tests** (in every `dbt build`) | on every materialization | Broken data: duplicates, nulls, out-of-range values, broken relationships |
| **Dagster asset checks** (with materializations) | in the pipeline itself | Broken *runs*: a partition that landed wrong, before it poisons downstream tables |

The division of labor: pytest for Python behavior, dbt tests for model constraints, asset checks for pipeline-level validation. No test in any layer touches the live API.

## Layer 1: pytest

All tests run offline: HTTP is mocked, the landing zone and DuckDB warehouse are temporary directories, and partition materializations use Dagster's in-process test APIs with an ephemeral instance.

One real response body, recorded from the archive API at design time (`tests/fixtures/api/archive_2026-08-17.json`), anchors the parser tests; the other scenarios (scattered nulls, a missing city, unit changes, truncated or duplicated hours, the 400 error body) are mutations of that recording applied in-test, and the offline end-to-end scenario generates synthetic multi-day responses with a planted outlier. The revision scenario (the same day fetched twice with different values) is covered by re-materialization tests at the landing and asset levels.

Test modules and what each proves:

| Module | Tests | Intent |
|---|---|---|
| `test_open_meteo_client.py` | multi-location array parsing; response-order-to-city mapping; null preservation; units assertion raises on unexpected units; 400-with-error-body maps to a deterministic (non-retryable) failure; timeout/5xx map to retryable | The API contract in [Ingestion & Storage](ingestion-storage.md#the-source-open-meteo-historical-weather-api) is enforced, and failure classification is explicit |
| `test_is_daylight.py` | macro SQL executed on an ephemeral DuckDB: Svalbard polar-night noon is night; equator noon is day; London equinox sunrise hour flips `is_day` | The one piece of derived science in staging is right at its edges |
| `test_weather_asset.py` | materialize one partition (mocked transport, tmp landing, ephemeral DuckDB): snapshot appears at the exact expected path and name; raw table has cities x 24 rows; **re-materialize the same partition**: a second snapshot is added, the first is byte-identical (immutability), the table slice now mirrors the revised fixture values with unchanged row count (idempotent derive) | The snapshot/table mechanics that everything else trusts |
| `test_definitions.py` | Definitions object loads; asset graph contains the expected edges (raw -> staging -> fact -> marts); partition definition starts 2026-07-01; the reconciliation schedule exists and targets the trailing 8 partitions; freshness policy is attached; the `warehouse=duckdb` concurrency tag is applied | Wiring declared in [Orchestration](orchestration.md) cannot silently drift |

The `test_weather_asset.py` pair (immutability + idempotent re-derivation) is the regression net for the two properties this architecture is built on; if any future change breaks either, CI says so before a single real partition is touched.

## Layer 2: dbt data tests

Model-by-model test specifications live with their models in [Transformation](transformation.md); this section is about the *kinds* and what each is for. Tests use the dbt-core 1.11 syntax (`data_tests:` with `arguments:` for generic test args) and run inside every `dbt build`, which Dagster wraps so results appear as asset checks (layer 3).

| Kind | Example | What it catches when it fires |
|---|---|---|
| Uniqueness (`dbt_utils.unique_combination_of_columns`) | (`location_id`, `hour_ts_utc`) on staging and fact | A merge that duplicated rows, or a derive bug that double-inserted a slice |
| Not null | keys, `hour_ts_utc`, `date_utc` | Parsing gaps, broken joins producing null keys |
| Accepted ranges | temperature -60 to 60, humidity 0 to 100, pressure 300 to 1100, wind 0 to 300 | Unit changes upstream (a sudden Fahrenheit or m/s leak), sensor-model glitches |
| Null-rate bound | each measure column at most 5% nulls per location-day (singular test in staging) | A variable quietly degrading to mostly nulls, which would otherwise silently thin anomaly baselines |
| Accepted values | `weather_code` in the WMO set; `variable` in the anomaly variable set | Unexpected categorical values smuggled into a typed column |
| Relationships | fact -> `dim_location`, fact -> `dim_date`, anomalies -> fact | Referential breaks: a city or date the dimensions do not know about |
| Singular tests (project-level SQL) | `temp_c_min <= temp_c_max`, `wind_kmh_avg <= wind_kmh_max`, anomaly abs(z) >= 3, `comparable_obs_count >= 14` | Business-rule violations no generic test expresses |

Severity defaults to error (a failed test fails the materialization and blocks downstream models in that `dbt build`), which is the correct default for a pipeline whose marts feed conclusions.

## Layer 3: Dagster asset checks

Two blocking checks guard the ingestion asset, evaluated in the same run right after the asset materializes:

1. **`expected_row_count`**: the partition's raw slice holds exactly cities x 24 rows. A partial or padded response from the API fails here instead of quietly shrinking a daily summary.
2. **`timestamps_within_partition`**: every `hour_ts_utc` falls inside the partition's UTC day. A timezone or boundary bug in the request fails here rather than corrupting a neighboring day's slice.

`blocking=True` means a failed check skips downstream dbt materializations for that run: nothing unverified flows into the fact. The snapshots remain on disk (they always do), so investigation starts from exactly what was received.

dbt tests surface as asset checks automatically through the dagster-dbt integration's check-enabling translator setting; each model's tests render on its asset node in the UI, green or red, per materialization.

## What fails where

Two different promises are in play, and they fail in different places. Recorded fixtures prove the parser still handles the API contract as it was last observed; they cannot detect Open-Meteo changing its live contract tomorrow. Live drift is caught one place only: the ingestion asset's validation during a real materialization.

| Defect | First line of defense | Where it shows up |
|---|---|---|
| Parser regresses against the known API contract (shape, units, ordering) | pytest (client assertions on recorded fixtures) | CI, before merge |
| Live upstream contract drifts (shape or units change at the source) | ingestion validation during a real run | Failed run, partition isolated |
| API changes variable availability mid-window | units/shape assertions, then row-count asset check | Failed run, partition isolated |
| One city's data missing from a response | shape assertion (8 location objects) | CI / failed run |
| Snapshot write collides or partial file survives | atomic write + immutability test | CI |
| Duplicate hours reach the raw table | staging uniqueness test | dbt build failure, run log |
| Boundary hour lands in the wrong partition | `timestamps_within_partition` check | Blocked run, partition isolated |
| Dimensional join breaks (new city, new date) | relationships tests | dbt build failure |
| Anomaly SQL regresses (threshold, window) | anomaly tests + planted-outlier integration scenario | CI (offline pipeline run) |
| Schedule silently stops | freshness policy | UI freshness goes red within 36h |

## CI

GitHub Actions runs the whole pyramid offline on every push and pull request, in four stages: environment install, lint/format check (ruff), pytest, and an offline end-to-end scenario that generates synthetic days, runs them through real Dagster materializations into a throwaway warehouse, runs the full dbt build, and asserts outcomes (summary row counts per city-day, the planted outlier being detected as an anomaly, and re-materialization leaving identical table state). The stage list and rationale live here; the commands and the workflow file reference are in [Operations Runbook](operations-runbook.md#ci). CI never calls the live API; live verification is a manual step owned by the runbook's verification checklist.
