# Concepts

Data engineering concepts, each explained the way this project uses it, with pointers to where the concept does real work.

## Idempotency

Running the same operation on the same input twice leaves the system in the same state as running it once.

Here, every write path is a replace, never a blind append. The ingestion asset's derive step is `DELETE` the partition slice, then `INSERT` from the snapshot, in one transaction ([Ingestion & Storage](ingestion-storage.md#the-raw-zone-a-derived-table)). The fact table uses dbt's `delete+insert` incremental strategy on the key (`location_id`, `hour_ts_utc`) ([Transformation](transformation.md#fct_hourly_weather)). Marts rebuild fully. So "materialize partition 2026-08-15" leaves the tables in the same state whether it runs once, five times, or once now and once next week: same grain, same row count, no duplicates anywhere. The one thing idempotency does not promise is identical values when a recent partition is re-fetched, because the source revises provisional days until they converge ([Ingestion & Storage](ingestion-storage.md#provisional-values-why-reconciliation-exists)). This is also the property under test in `test_raw_asset.py` ([Quality & Testing](quality-testing.md#layer-1-pytest)).

## Immutability

Some things are never edited after write; state is derived from them instead.

The landing zone is the project's immutable core: every successful fetch is a new Parquet snapshot, and no code path modifies, renames, or deletes one ([Ingestion & Storage](ingestion-storage.md#the-landing-zone-versioned-immutable-snapshots)). Mutation happens nowhere: the raw table is *derived* from the latest snapshot, so "the data changed" is expressed as "a new snapshot exists and the pointer moved". Immutability is what makes replay, lineage, and audit possible at zero extra machinery.

## Incremental loading

Process only what is new (or revised), leave the rest untouched.

`fct_hourly_weather` is an incremental model: each run inserts/replaces one UTC day's rows bounded by the run's `start_date`/`end_date` vars, instead of recomputing all history ([Transformation](transformation.md#fct_hourly_weather)). At this data scale it is not about volume; it is about *semantics*: a re-fetched (revised) partition must replace its old rows without disturbing neighbors, the same converging semantics as the raw zone one layer down. The tradeoff between the incremental fact and the full-rebuild marts is settled in [Design Decisions](design-decisions.md).

## Deterministic processing

Same inputs, same outputs, every time, independent of run time or execution order.

The pipeline earns this by construction: UTC-only timestamps, a fixed request shape per partition, deterministic tie-breaks in aggregates (mode with smallest-value tiebreak in the daily summary), baselines that only look backwards ([Anomaly Detection](anomaly-detection.md#implementation-shape)), and the deliberate absence of any `detected_at`-style column that would smuggle wall-clock time into data. The payoff: backfilled days and live-scored days are interchangeable, and reruns are byte-comparable. Determinism is a claim about replay: the same inputs always produce the same outputs. Whether a fresh fetch returns the same inputs for a recent day is the source's convergence property, not the pipeline's, and until convergence the two legitimately differ.

## Partitioning

Slice the dataset by a key so operations address one slice at a time.

The universal key here is the UTC day. It appears four times in four layers: Dagster daily partitions (one run per day, [Orchestration](orchestration.md#partitions)), landing-zone Hive directories (`year=/month=/day=`), the raw table's `partition_date` column, and the dbt vars bounding the fact's incremental window. A partition is the unit of fetch, storage, retry, backfill, and reconciliation; every "process this data" operation in the system is really "process these partitions".

## Backfills

Materialize a historical range of partitions after the fact.

Because partitions are independent and every write is idempotent, a backfill is just batched scheduled runs: `make backfill FROM=2026-07-01 TO=2026-08-18` executes the same code path the 06:00 schedule uses, per day ([Operations Runbook](operations-runbook.md#backfills)). The initial project backfill, repair ranges after incidents, and reconciliation re-fetches are the same operation with different ranges. What makes that safe is the stack of properties above: idempotency (no duplicates), determinism (same inputs, same outputs), and immutability (the landing zone records every fetch ever made, including backfill fetches).

## Schema validation

Assert the shape of data at boundaries, so shape drift fails loudly instead of corrupting quietly.

Three boundaries, three mechanisms: the ingestion client asserts response structure (8 location objects, expected `hourly_units`, 24 timestamps spanning the day) before anything is written ([Ingestion & Storage](ingestion-storage.md#the-ingestion-asset)); dbt casts and types everything at staging, so a changed column type surfaces at the first model, and `on_schema_change: sync_all_columns` makes fact-column drift fail or sync explicitly rather than silently truncate ([Transformation](transformation.md#fct_hourly_weather)); asset checks and accepted-range tests bound the *values*, not just types ([Quality & Testing](quality-testing.md)).

## Deduplication

Guarantee one row per grain, ever.

The grain is (`location_id`, `hour_ts_utc`). It is enforced in layers: the derive step's delete-then-insert cannot double-write a slice; staging deduplicates defensively with `row_number()` ordered by `ingested_at_utc` descending (newest snapshot wins); and uniqueness tests on staging, fact, and marts ([Transformation](transformation.md), [Quality & Testing](quality-testing.md#layer-2-dbt-data-tests)) make any duplicate a failed materialization rather than a silent doubling.

## Data quality

Fitness for use, enforced as executable expectations rather than opinions.

The project encodes its expectations as tests with intent: physical plausibility (temperature -60 to 60, pressure 300 to 1100), completeness (exactly cities x 24 rows per partition), referential integrity (every fact row joins to both dimensions), and business rules (min <= max aggregates, abs(z) >= 3) ([Quality & Testing](quality-testing.md)). Two properties make this more than decoration: checks are *blocking* (bad data cannot flow downstream), and failures carry context (which partition, which endpoint, which expectation).

## Dimensional modeling

Organize facts (events) and dimensions (context) so questions compose.

The fact is `fct_hourly_weather`: one row per city per hour, the atomic event, carrying measures and foreign keys only. Dimensions are `dim_location` (the cities seed: stable attributes like timezone and climate zone) and `dim_date` (calendar attributes like quarter and weekend flag) ([Transformation](transformation.md)). Analysts get combinable axes ("weekends in polar cities", "monsoon months") without touching the fact, and summary marts join fact to dimensions instead of re-deriving context.

## Staging, core, and mart layers

Each layer has one job; data flows one way.

Staging cleans and standardizes (rename, type, deduplicate, derive `is_day`) and exposes a stable interface. Core holds reusable modeled entities: the fact and its dimensions. Marts answer analytical questions: daily summaries and anomalies ([Transformation](transformation.md)). The layering rule ([Overview](overview.md#layer-responsibilities)) is no layer bypassing: marts never read raw, cleaning never mixes with aggregation, and peer reads inside a layer are fine where the DAG calls for them (the summary mart reads the anomalies mart). The payoff is that a change (new variable, new city) ripples through exactly the layers that care.

## Failure recovery

When something breaks, restore by re-running, not by repairing.

The failure model is enumerated in [Orchestration](orchestration.md#failure-recovery), and every row of that table ends in "re-run the thing": retry the run, backfill the range, or rebuild the warehouse from the landing zone ([Operations Runbook](operations-runbook.md#rebuilding-the-warehouse-from-the-landing-zone)). Manual data surgery appears nowhere, because append-only snapshots plus transactional slice replacement leave no partially-mutated state to repair.

## Retry safety

A retry must be *able* to run, and *safe* to run.

Able: retries target only plausibly transient failures (timeouts, 5xx, 429); deterministic errors (HTTP 400, validation) fail fast and are never retried ([Orchestration](orchestration.md#retries)). Safe: because the write path is idempotent (delete-then-insert in a transaction) and snapshots are immutable, a retry that lands after a partial failure either rewrites the same slice or adds another snapshot; no interleaving produces duplicates or torn state. The unit test that guards this exact scenario (kill point between snapshot and derive) is `test_raw_asset.py`'s re-materialization pair ([Quality & Testing](quality-testing.md#layer-1-pytest)).
