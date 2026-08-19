# Ingestion & Storage

This document covers everything between the Open-Meteo API and the raw table DuckDB serves to dbt: the API contract, the ingestion asset, the landing zone of immutable snapshots, and the derived raw zone. The orchestration *when* (schedules, retries wiring, backfills) is [Orchestration](orchestration.md); the *logic* of fetch → snapshot → table lives here. This document owns the API and storage contracts (request shape, response facts, snapshot layout, raw-zone semantics); other documents summarize and link.

## The source: Open-Meteo Historical Weather API

### Endpoint and request shape

The pipeline uses the archive endpoint exclusively (not the forecast endpoint):

```
GET https://archive-api.open-meteo.com/v1/archive
    ?latitude=51.5072,-23.5505,...          ← all eight cities, comma-separated
    &longitude=-0.1276,-46.6333,...
    &hourly=temperature_2m,relative_humidity_2m,apparent_temperature,
            precipitation,weather_code,pressure_msl,surface_pressure,
            cloud_cover,wind_speed_10m,wind_direction_10m
    &start_date=2026-08-15
    &end_date=2026-08-15
    &timezone=UTC
```

One request per UTC day, all cities at once. Exactly ten hourly variables, deliberate: Open-Meteo's weight-based call counting charges extra beyond ten weather variables, so ten keeps each day at one counted call.

Facts the ingestion code relies on (all live-verified 2026-08-19; live-verified against the production API):

- Multi-location requests return a **top-level JSON array**, one object per coordinate, in request order, with `location_id` added from the second element onward. The parser maps response index → city by request order; never by the returned coordinates.
- Returned `latitude`/`longitude` are **grid-cell centers**, a few kilometers from the requested point, and differ between archive and forecast endpoints. They are recorded but never used as join keys.
- `hourly.time` entries are naive ISO-8601 strings (`"2026-08-15T14:00"`) with no offset. With `timezone=UTC` they are UTC wall times; the parser attaches UTC semantics.
- `hourly_units` is an object keyed by variable; units are fixed by request defaults: °C, %, mm, km/h, hPa. The parser asserts the expected units and fails loudly if they change.
- Nulls appear index-aligned in variable arrays. Nulls are kept as nulls (rows are not dropped); staging tests bound each measure's null rate at 5% per location-day ([Quality & Testing](quality-testing.md#layer-2-dbt-data-tests)).
- `precipitation` is the **sum of the preceding hour**; other variables are instantaneous values at the hour. The column docs in [Transformation](transformation.md) repeat this so nobody averages precipitation naively.
- Errors are HTTP 400 with `{"reason": ..., "error": true}`, deterministic, not retryable.

### Provisional values (why reconciliation exists)

The archive endpoint answers queries for recent days immediately, but its default model selection ("Best Match") blends ECMWF IFS (available without delay) with ERA5 and ERA5-Land reanalysis (published with roughly a five-day delay). Open-Meteo does not document the exact blend mechanics or when a recent day's values switch to their reanalysis form, only the delays. The safe conclusion: **values fetched within roughly the last week are provisional and may be revised by the source.**

The pipeline therefore re-materializes recent partitions daily. [Orchestration](orchestration.md#daily-reconciliation-schedule) owns the schedule and the window (the trailing eight partitions, a safety margin over the reanalysis delay, not a claimed switch-over point); the decision record is [Design Decisions](design-decisions.md#3-converging-re-materialization-not-pinning-or-drifting).

### Rate limits and cost

Free, keyless, for non-commercial use: 600 calls/minute, 5,000/hour, 10,000/day (weight-based). The pipeline's steady state is **8 calls/day** (one per reconciled partition) and the initial backfill is ~50 calls, three orders of magnitude under the daily ceiling. No API key, no credentials, nothing to rotate. Misuse (sustained over-limit traffic) risks IP blocking, which is why ingestion never parallelizes API calls beyond retries.

## The ingestion asset

One Dagster asset, `raw_weather_observations`, owns fetch → snapshot → derive. It is partitioned by UTC day; the partition definition and its 2026-07-01 start date are owned by [Orchestration](orchestration.md#partitions).

Responsibilities, in order:

1. **Fetch.** Build the request for the partition day across all configured cities; call the API via the httpx-based client resource (timeout configured; one attempt, retries are the orchestrator's job).
2. **Validate.** Assert the response parses (array of 8 location objects), units match expectations, and 24 hourly timestamps span the partition day exactly. A malformed response raises with context (endpoint, partition, what was wrong), deterministic failures are never retried.
3. **Land.** Write the parsed rows **plus operational metadata** as this run's immutable snapshot, reusing an already-completed snapshot from an earlier attempt of the same run if one exists (next section).
4. **Derive.** Refresh the raw table's partition slice from the latest snapshot.
5. **Report.** Emit asset metadata: snapshot path, row count, per-variable null counts, the evidence a reviewer sees in the Dagster UI.

Transient transport failures (timeouts, 5xx, 429) surface to Dagster's `RetryPolicy` (configured in [Orchestration](orchestration.md#retries)); the asset itself contains no retry loops.

### Configuration

Nothing environment-specific is hard-coded. The asset reads, via a settings object backed by environment variables with safe local defaults (table with values: [Operations Runbook](operations-runbook.md#configuration)):

- API base URL (default: the archive endpoint above)
- Landing zone root (default `data/raw/`)
- DuckDB path (default `warehouse/weather.duckdb`)
- The city list (source of truth: `dbt/seeds/cities.csv` itself; ingestion reads the seed file directly, so there is no second list to drift, see [Transformation](transformation.md#dim_location-the-cities-seed))

## The landing zone: versioned immutable snapshots

### Layout

```
data/raw/
└── year=2026/
    └── month=08/
        └── day=15/
            ├── ingested_at=2026-08-16T06-00-05Z_run=f41c2ab9.parquet   ← first successful fetch
            └── ingested_at=2026-08-23T06-00-04Z_run=8d07e315.parquet   ← reconciliation re-fetch
```

Hive-style date directories (year/month/day) plus one Parquet file per fetch, named by its ingestion timestamp and a short run id (filesystem-safe: colons become hyphens). The timestamp keeps names human-sortable; the run id makes collisions impossible even if two runs ever shared a start second. Every successful run materializing a partition appends exactly one snapshot: retries within a run reuse it, and only a new run (the next reconciliation, a manual re-run, a backfill) adds another. **Snapshots are never modified, never renamed, never deleted** (retention is unlimited; pruning is a documented limitation, [Design Decisions](design-decisions.md#limitations)).

### Why versioned rather than "one file per day"

A single overwriteable file per day cannot be both immutable and reconcilable: re-fetching a revised day would have to destroy the record of what was originally received. Versioned snapshots resolve the conflict, reconciliation *adds* `ingested_at=2026-08-23T06-00-04Z_run=8d07e315.parquet` and re-points the derivation, leaving history intact. This was the one necessary architecture correction of the design review; full reasoning in [Design Decisions](design-decisions.md).

### Write path, atomicity, and snapshot identity

A snapshot's identity is the pair (**partition_date, Dagster run identity**). The filename pairs the `ingested_at` timestamp with the Dagster run id; the timestamp is captured once per run from the run's start time, never from the wall clock at attempt time, so every attempt of a run computes the same filename, and the run id guarantees uniqueness across runs. The write path is write-once-with-reuse:

- If no complete snapshot exists for this run, the rows are written to a temporary file in the partition directory, then atomically renamed into place (`os.replace`). A crash mid-write leaves at worst a stray temp file (ignored by snapshot selection), never a partial snapshot that looks valid.
- If a complete snapshot from this same run already exists (an earlier attempt landed it and then failed at a later step), the attempt validates its row count and reuses it without refetching. A retry therefore never adds a physically distinct snapshot for the same fetch operation.
- A genuinely new run always writes a new snapshot, which is how reconciliation and manual re-runs append history.

This is what makes retries idempotent rather than merely convergent: one run, at most one fetch's worth of snapshot, and a derive that replaces one slice inside a single transaction.

### Snapshot contents

One Parquet file = one fetch of one partition day = 8 cities × 24 hours = 192 rows (Parquet-native types):

| Column | Type | Notes |
|---|---|---|
| `partition_date` | DATE | The UTC day being fetched |
| `city_id` | VARCHAR | Stable slug, e.g. `reykjavik` |
| `hour_ts_utc` | TIMESTAMP | Naive UTC, from `hourly.time` |
| `temperature_2m` through `wind_direction_10m` | DOUBLE / INT | The ten API variables, API names, nulls preserved |
| `latitude` / `longitude` | DOUBLE | As returned (grid-cell centers), provenance only |
| `ingested_at_utc` | TIMESTAMPTZ | The run that produced this row; constant across that run's attempts |
| `source_url` | VARCHAR | The full request URL, the snapshot's provenance |

### Latest-snapshot selection

For a partition day, the **latest snapshot** is the file with the maximum `ingested_at` in its directory (fixed-width timestamp prefix, so a plain filename sort is correct; the run id suffix breaks any tie). Selection is a directory listing plus filename sort, no catalog, no database, no state outside the filesystem. Because writes are atomic and names are unique per run id, listing can never observe a torn state.

## The raw zone: a derived table

`raw.weather_observations` in the warehouse is **derived state, not a record**: for every partition, it mirrors that day's latest snapshot and nothing else. Derivation is the only writer:

```sql
-- refresh of one partition slice (conceptual; owned by the ingestion asset,
-- both statements inside a single DuckDB transaction)
BEGIN TRANSACTION;
DELETE FROM raw.weather_observations WHERE partition_date = ?;
INSERT INTO raw.weather_observations
SELECT * FROM read_parquet('<partition_dir>/ingested_at=<latest>.parquet');
COMMIT;
```

Consequences:

- **Idempotent by construction.** Re-materializing a partition rewrites the same slice inside one DuckDB transaction; row counts cannot drift, duplicates cannot accumulate, and a failed derive leaves the previous slice untouched. Combined with run-scoped snapshot identity, a retry of a run reuses its snapshot and re-derives the same slice: retries are idempotent, not merely convergent.
- **Full warehouse replay without refetch.** The whole table can be rebuilt from the landing zone alone; the recovery procedure in [Operations Runbook](operations-runbook.md#rebuilding-the-warehouse-from-the-landing-zone) is exactly this derivation applied to every partition.
- **Lineage.** Every raw row carries `ingested_at_utc` and `source_url` from its snapshot, so any value in any mart can be traced back to the fetch that produced it.

dbt consumes the table through a [source definition](transformation.md#source-mapping) whose asset key maps back to the Dagster asset, the asset graph in the UI is continuous across the Python/dbt boundary.

## Licensing and attribution

Open-Meteo API data is licensed **CC BY 4.0**. Obligations the project honors:

- Attribution link next to any displayed data: **"Weather data by [Open-Meteo.com](https://open-meteo.com/)"**, carried in the README and any future dashboard.
- Historical reanalysis incorporates Copernicus Climate Change Service information; the attribution line "Generated using Copernicus Climate Change Service information" is kept where datasets are published.
- Non-commercial, keyless use stays within the documented rate limits by design (8 calls/day steady state).

Details and sources: the licence and terms pages at open-meteo.com.
