# Operations Runbook

This is the only document with commands. It covers setup, configuration, daily operation, verification, backfills, recovery, troubleshooting, Docker, and CI. Until the implementation milestones land, these commands are the *contract* the implementation must satisfy.

## Setup

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), optionally the DuckDB CLI for ad-hoc queries, optionally Docker.

```bash
git clone <repo> && cd pipeline-project
uv sync                      # create venv, install pinned dependencies
uv run pre-commit install    # format/lint hooks on commit
```

No API key and no secrets are needed: the Open-Meteo archive API is keyless for non-commercial use within its rate limits ([Ingestion & Storage](ingestion-storage.md#rate-limits-and-cost)).

Day-to-day entry point is the Dagster UI:

```bash
make dev                     # wraps: uv run dg dev  ->  http://localhost:3000
```

One setup step is not optional: enable the **default automation condition sensor** (UI: Automation section). Declarative automation, including the unpartitioned dbt group's eager condition, launches runs only through that sensor; without it, marts never rebuild on their own (see [troubleshooting](#troubleshooting)).

## Configuration

All environment-specific values flow through a settings module backed by environment variables with safe local defaults; nothing is hard-coded in pipeline logic.

| Variable | Default | Purpose |
|---|---|---|
| `WEATHER_PIPELINE_OPEN_METEO_BASE_URL` | `https://archive-api.open-meteo.com/v1/archive` | API endpoint (overridable for tests/proxies) |
| `WEATHER_PIPELINE_DUCKDB_PATH` | `warehouse/weather.duckdb` | Warehouse file |
| `WEATHER_PIPELINE_LANDING_DIR` | `data/raw` | Landing zone root |
| `WEATHER_PIPELINE_LOG_LEVEL` | `INFO` | Log level for pipeline loggers |

Dagster instance configuration (`dagster.yaml`): concurrency, a run-concurrency pool of one keyed on the `warehouse=duckdb` tag, which serializes all writers against DuckDB's single-writer file lock. Asset definitions: freshness policies, attached to the ingestion asset; the current freshness system needs no instance-level feature flag. Settings are resolved once at load; overrides go in the environment (shell, `.env`, or compose), never into committed code.

## Logging

The conventions are event-and-outcome messages, not implementation narration. The ingestion asset logs through the Dagster context (`ingestion_started`, `api_request_completed`, `records_received`, `partition_written`); client library code uses the standard `logging` module under the `weather_pipeline` namespace; dbt output streams into the run log via the dbt asset events. The Dagster UI (and its underlying event log) is the single place to read pipeline logs; nothing is written to log files. Secrets are never logged, and there are none to log.

## Daily operation and verifying a run

The `daily_reconciliation` schedule fires at 06:00 UTC and launches 8 partition runs, then the automation condition rebuilds the unpartitioned group ([Orchestration](orchestration.md#daily-reconciliation-schedule)). A healthy morning looks like this, in order:

1. **Runs:** 8 partition runs for yesterday and the 7 days before it, all green; then one unpartitioned group run, green. (UI: Runs, or `Overview -> Recent runs`.)
2. **Checks:** every asset check green on the touched partitions (row count, timestamp bounds, dbt tests).
3. **Freshness:** the ingestion asset's freshness badge is green.
4. **Landing zone:** one new snapshot file per partition directory:

```bash
ls data/raw/year=2026/month=08/day=18/   # -> ingested_at=<this morning>Z_run=<id>.parquet (+ earlier ones)
```

5. **Data:** the warehouse answers for yesterday:

```bash
uv run duckdb warehouse/weather.duckdb <<'SQL'
SELECT count(*) FROM raw.weather_observations WHERE partition_date = '2026-08-18';
-- expect 192 (8 cities x 24 hours)
SELECT count(*) FROM core.fct_hourly_weather WHERE date_utc = '2026-08-18';
-- expect 192
SELECT location_id, temp_c_min, temp_c_max, precipitation_mm_sum, anomaly_count
FROM marts.daily_weather_summary WHERE date_utc = '2026-08-18';
-- expect 8 rows, one per city
SELECT * FROM marts.weather_anomalies
WHERE hour_ts_utc >= TIMESTAMP '2026-08-18 00:00:00' ORDER BY hour_ts_utc;
-- expect anything from 0 rows up; empty is a legitimate outcome
SQL
```

Manual triggers when needed: a single partition from the UI (asset page, materialize partition), or a reconciliation-equivalent batch:

```bash
make reconcile               # backfills the trailing 8 partitions now
```

## Backfills

Any contiguous range, same semantics as scheduled runs (idempotent, converging):

```bash
make backfill FROM=2026-07-01 TO=2026-08-18   # the initial project backfill
make backfill FROM=2026-08-10 TO=2026-08-12   # a repair range
```

Both wrap `scripts/backfill.py`, which launches the partitioned job over the range through the local Dagster instance; the UI shows a resumable backfill with per-partition status. Re-running converged days is harmless: new snapshots land that simply repeat the source's final values. The unpartitioned group follows automatically via its automation condition once the backfill completes; if it does not (e.g. the daemon was down), `make dbt-build` (below) rebuilds it explicitly.

**Before the first backfill on an empty warehouse**, build the foundation (cities seed, staging view, `dim_location`, `dim_date`), or the first fact partition will find no view or dimensions to build against:

```bash
make bootstrap                 # dbt seed + staging view + dimensions
make backfill FROM=2026-07-01 TO=2026-08-18
```

**Before backfilling a new city's partitions**, run `make bootstrap` again so the seed and `dim_location` know the city before the fact's relationship tests execute. The ordering design is [Orchestration: bootstrap and first-run ordering](orchestration.md#bootstrap-and-first-run-ordering).

## Rebuilding the warehouse from the landing zone

When the DuckDB file is corrupted or deleted, everything derived is recoverable without a single API call, because snapshots are the record and tables are derived state ([Ingestion & Storage](ingestion-storage.md#the-raw-zone-a-derived-table)):

```bash
make rebuild-raw             # re-derives raw.weather_observations from latest snapshots
make dbt-build               # rebuilds all dbt models
```

Then re-run the verification queries above.

## Running dbt directly

Useful while iterating on models; orchestration normally does this for you:

```bash
uv run dbt build  --project-dir dbt --profiles-dir dbt          # models + tests, full build
uv run dbt test   --project-dir dbt --profiles-dir dbt          # tests only
uv run dbt docs generate --project-dir dbt --profiles-dir dbt    # lineage docs
uv run dbt docs serve   --project-dir dbt --profiles-dir dbt
```

`make dbt-build` is the wrapper. Partition-windowed builds (what Dagster executes) pass `--vars '{"start_date": "...", "end_date": "..."}'`; manual invocations default to the full-history window.

## Tests and lint

```bash
uv run pytest                # the offline test pyramid (quality-testing.md)
make verify                  # fixtures -> Dagster materialization -> dbt build -> assertions
uv run ruff check .
uv run ruff format --check .
make format                  # apply formatting
```

## Docker

The repo ships a uv-based Dockerfile and a compose file running the Dagster webserver, daemon, and the pipeline code location ([Overview](overview.md#intended-technologies-and-compatibility-assumptions)). Volumes mount `./warehouse`, `./data`, and the Dagster home directory, so container runs share state with local runs:

```bash
docker compose up -d         # UI on http://localhost:3000
docker compose logs -f dagster-daemon
docker compose down          # add -v only if you mean to drop Dagster's run history
```

One caveat inherited from DuckDB's single-writer model: do not run the UI locally (`make dev`) and the compose stack against the same warehouse file at once; one process holds the write lock ([troubleshooting](#troubleshooting)).

## CI

Workflow: `.github/workflows/ci.yml`, on every push and pull request, four offline stages (install, ruff, pytest, offline end-to-end; rationale in [Quality & Testing](quality-testing.md#ci)). CI never calls the live API. A red CI pytest or verify stage is always reproducible locally with `uv run pytest` / `make verify-offline`.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `Could not set lock on file` / `database is locked` | Another process holds the warehouse: a `duckdb` CLI session, a second `dg dev`, the Docker stack, or two runs racing (concurrency config not applied) | Close the other holder; verify `dagster.yaml` declares the `warehouse=duckdb` pool; re-run |
| Marts never rebuild after fact runs | Default automation condition sensor disabled, so the eager condition launches nothing | UI: Automation section, enable the default automation condition sensor, or rebuild explicitly with `make dbt-build` |
| Run fails with HTTP 400 and a `reason` | Deterministic API error: date out of allowed range (start before 1940-01-01, end after today), malformed request | Fix the partition range; do not retry, it will fail identically |
| HTTP 429 after retries | Rate limiting (unlikely: 8 calls/day vs 10k/day allowance) | Check for accidental parallel backfills or non-pipeline traffic from your IP |
| Row-count asset check red | Partial or padded response; API shape change | Inspect the latest snapshot (`uv run duckdb -c "SELECT ... FROM read_parquet('<snapshot path>')" `), compare to the [Ingestion & Storage](ingestion-storage.md#the-source-open-meteo-historical-weather-api) contract; re-materialize the partition after the cause is fixed |
| Freshness badge red | Schedule paused, daemon down, or repeated run failures | UI: Automation tab, resume schedule / restart daemon / read the failed run's log |
| dbt manifest or parse errors in Dagster | dbt project edited but not re-parsed | `uv run dbt parse --project-dir dbt --profiles-dir dbt`; reload the code location |
| `weather_anomalies` empty | Young baseline (guard needs 14 comparables), a genuinely quiet month, or a real bug | Query `comparable_obs_count` on an unpivoted window by hand; silence before ~2026-07-15 is expected by design |
| Docker UI unreachable | Port 3000 in use, or stack half-up | `docker compose ps`, `docker compose logs`, free the port or change the mapping |

## Make targets

| Target | Wraps |
|---|---|
| `make setup` | `uv sync` + `pre-commit install` |
| `make dev` | Dagster UI (`dg dev`) |
| `make backfill FROM=DATE TO=DATE` | range backfill via `scripts/backfill.py` |
| `make reconcile` | trailing-8 backfill (schedule-equivalent, immediate) |
| `make rebuild-raw` | re-derive raw table from latest snapshots |
| `make bootstrap` | `dbt seed` + staging view + dimensions (before first or new-city backfills) |
| `make dbt-build` | `dbt build` full project |
| `make test` | `uv run pytest` |
| `make verify` | offline end-to-end scenario |
| `make lint` / `make format` | ruff check / format |
| `make clean` | remove caches and build artifacts (never touches `data/` or `warehouse/`) |
