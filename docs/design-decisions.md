# Design Decisions

This document is the project's decision record: every hard-to-reverse choice in one table, the fuller tradeoff discussion for the three biggest calls, the honest limitations we accepted, and what changes when this local project grows into a production deployment.

## Decision index

| # | Decision | One-line why |
|---|---|---|---|
| 1 | DuckDB as the analytical engine | Zero-infra embedded OLAP with a first-class dbt adapter; SQL stays portable |
| 2 | UTC as the canonical time basis | One partition = one UTC day for every city; no DST ambiguity anywhere |
| 3 | Versioned immutable snapshots in the landing zone | Reconciliation requires new fetches without destroying received history |
| 4 | Converging re-materialization of recent partitions | Best Match serves provisional values; re-fetching the trailing 8 days absorbs upstream revisions |
| 5 | Incremental fact, full-rebuild marts | Incremental where revision semantics demand it; rebuilds where simplicity wins at trivial cost |
| 6 | Hour-of-day-matched z-score anomaly heuristic | Explainable, auditable, works from day 15; honestly labeled including its weak variable |

Smaller decisions, recorded here once:

- **Ten hourly variables exactly:** Open-Meteo's weight-based call counting charges extra beyond ten weather variables; ten keeps a day at one counted call ([Ingestion & Storage](ingestion-storage.md#rate-limits-and-cost)).
- **Two dbt asset groups** (partitioned incremental, unpartitioned rest): the documented dagster-dbt pattern; makes 50-day backfills avoid 50 mart rebuilds ([Orchestration](orchestration.md#two-dbt-asset-groups)).
- **`is_day` derived, not fetched:** the archive API does not serve the flag; a NOAA solar-position macro in staging derives it deterministically ([Transformation](transformation.md#stg_hourly_observations)).
- **No API key, no secrets manager (yet):** the source is keyless within free limits; nothing to rotate, so nothing to build (see [limitations](#limitations)).
- **Tag-based run concurrency of 1:** the direct consequence of DuckDB's single-writer file lock ([Orchestration](orchestration.md#duckdb-single-writer-and-run-concurrency)).

## The three big calls, in full

### 1. DuckDB, not Postgres or a cloud warehouse

Alternatives considered: PostgreSQL (a real server, closer to legacy production shapes, but needs a running service, lacks some analytical SQL, and still caps out as a row-store OLTP engine for analytical scans); Snowflake/BigQuery (true warehouse experience, but accounts, credentials, and trial expiry would eventually break a portfolio repo; the project stops working when the trial does).

DuckDB gives direct Parquet read/write, fast analytical queries on a single file, and a mature dbt adapter, with the one real cost being the single-writer lock, which this project's scale turns into a non-problem via serialized runs. Because dbt owns all SQL, the migration path to a bigger engine is a profiles.yml change plus dialect review, not a rewrite.

### 2. Versioned immutable snapshots, not one file per day

Alternatives considered: parsed rows directly into a DuckDB table, no landing zone at all (simplest, but loses warehouse replay and the audit record of what was actually received); one Parquet file per day, overwritten on re-fetch (supports reconciliation but destroys received history, so "immutable" becomes a lie and forensics are impossible).

Versioned snapshots cost one thing, storage growth (bounded: 8 cities x 24 hours x 10 variables x one file per day per re-fetch; kilobytes), and buy warehouse replay, lineage, and reconciliation without conflict: reconciliation appends `ingested_at=...` files and the derivation pointer moves. This was the one change the design review demanded, and it was correct: the original single-file design could not be both immutable and reconcilable.

One precision on what "replay" means here: snapshots hold **parsed** rows, not the verbatim API response. The warehouse and everything downstream can be reconstructed from the landing zone alone, but the original HTTP-response-to-parser transformation cannot be replayed; if a parser bug were discovered later, the affected days must be re-fetched from the API rather than re-parsed from disk. Landing verbatim JSON alongside the parsed rows would buy full parser replay at the cost of a second storage format and write path, and was deliberately not taken for a free, keyless source.

### 3. Converging re-materialization, not pinning or drifting

Alternatives considered: pin `models=era5` (values never revise, so no re-fetch machinery, but the pipeline runs five days behind reality and still trusts the API never to fix an error); fetch each day once and accept drift (simplest, but the warehouse quietly disagrees with its own source of truth for recent days).

Re-materializing the trailing 8 partitions daily costs 8 API calls and one honest mechanism, and converts an invisible data-quality problem (silent upstream revision) into an explicit, documented property: recent partitions converge to stable values, and every intermediate belief is preserved in the landing zone. The framing matters: we claim re-fetching absorbs reanalysis availability and revisions, not any specific IFS-to-ERA5 switchover, because Open-Meteo documents delays, not blend mechanics.

## Limitations

Accepted knowingly, each with its escape hatch:

- **DuckDB single-writer:** all warehouse writers serialize through one concurrency tag; throughput scales to "seconds per partition", not to parallel fleets. Escape: split raw/serve files, a client-server engine, or a real warehouse (see below).
- **Local files as infrastructure:** the landing zone and warehouse are directories on one machine; no replication, no ACLs, no versioning beyond git-ignored files. Escape: object storage for snapshots; managed storage for tables.
- **Snapshot retention is unbounded:** nothing prunes old snapshots, and source-value-equivalent re-fetches of converged days still land files (snapshots can never be byte-identical: each embeds its run's `ingested_at_utc`). Escape (when needed): a retention policy that keeps first and converged snapshots and prunes the middle; deliberately not built yet.
- **No secrets manager:** correct today (keyless API), but the moment a keyed endpoint or a cloud target appears, `.env` discipline is not enough. Escape: standard secret injection via environment at deployment time.
- **Precipitation anomaly detection is weak:** zero-heavy skew makes z-scores a poor detector there; kept deliberately and labeled loudly ([Anomaly Detection](anomaly-detection.md#variables-covered)). Escape: percentile or wet-day-conditioned methods, roadmap.
- **One timezone basis:** everything is UTC, so "local day" questions (what happened Tuesday in Tokyo) need conversion at query time; a `dim_location.timezone` column exists precisely to make that easy, but no local-day marts are planned.
- **CI proves the pipeline offline only:** live-API behavior is verified by the runbook's manual checklist, not by CI ([Quality & Testing](quality-testing.md#ci)).

## Moving to production

What changes, layer by layer, when this stops being a local project:

| Layer | Local (now) | Production |
|---|---|---|
| Storage | DuckDB file + Parquet on disk | Object storage (S3/GCS) for snapshots; DuckDB/MotherDuck or a cloud warehouse (Snowflake, BigQuery) for tables |
| Engine | DuckDB, single writer, serialized runs | Client-server or cloud warehouse: the concurrency tag limit disappears; dialect review of dbt SQL (dbt owns most of it) |
| Orchestration | `dg dev` / compose on one machine | Dagster deployed (Dagster+ or self-hosted with a real instance database); same asset code, real daemon HA |
| Ingestion secrets | None (keyless) | API key management via the platform's secrets store if a paid tier is adopted |
| Observability | UI, freshness policies, run logs | Same plus alerting routed from failed checks and freshness to on-call |
| CI/CD | Offline verification | Same offline stages, plus deploy job for the code location; the landing-zone rebuild procedure doubles as disaster recovery |
| Data volume | 8 cities x ~13 months | Trivially more cities (seed rows + backfill); years of history benefit from the bulk-download path instead of per-day calls |

The architecture was shaped so that each of these is a swap, not a redesign: snapshots are engine-agnostic Parquet with a naming contract; all table SQL lives in dbt behind sources; orchestration logic never touches data; and every operational procedure in [Operations Runbook](operations-runbook.md) is written against commands that have production equivalents.
