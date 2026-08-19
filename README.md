# Weather Pipeline

A data engineering project for ingesting, transforming, and modeling weather data.

The goal of this project is to build a production-style batch data pipeline while practicing core data engineering concepts including orchestration, incremental ingestion, data modeling, testing, partitioning, and backfills.

## The pipeline at a glance

```
Open-Meteo Historical Weather API  (one multi-location call per UTC day)
        ↓
Dagster ingestion asset            (daily partitions, retries)
        ↓
Landing Zone                       (immutable Parquet snapshots, one per fetch)
        ↓
Raw Zone                           (derived table: latest snapshot per partition)
        ↓
dbt                                (staging → core → marts)
        ↓
fct_hourly_weather · dim_location · dim_date
        ↓
daily_weather_summary · weather_anomalies
```

## Documentation index

| Document                                         | Read it when you want to understand                                                                     |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| [Overview](docs/overview.md)                     | Why this project exists, the end-to-end architecture, the journey of one hour of data                   |
| [Ingestion & Storage](docs/ingestion-storage.md) | The Open-Meteo API contract, the ingestion asset, the landing zone and raw zone                         |
| [Transformation](docs/transformation.md)         | Every dbt model: inputs, outputs, when it runs, and its tests                                           |
| [Data Contracts](docs/data-contracts.md)         | The exact grains, keys, and columns of every table and model, in one place                              |
| [Orchestration](docs/orchestration.md)           | The asset graph, partitions, schedules, reconciliation, backfills, failure recovery                     |
| [Quality & Testing](docs/quality-testing.md)     | The three testing layers and what each one catches                                                      |
| [Anomaly Detection](docs/anomaly-detection.md)   | The statistical heuristic, its worked example, and its honest limits                                    |
| [Operations Runbook](docs/operations-runbook.md) | Setup, configuration, running things, verifying runs, troubleshooting, the only document with commands  |
| [Concepts](docs/concepts.md)                     | Data engineering concepts (idempotency, backfills, dimensional modeling) explained through this project |
| [Design Decisions](docs/design-decisions.md)     | Why each choice was made, what was rejected, limitations, and the production migration path             |

## Attribution

Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0). Historical reanalysis incorporates Copernicus Climate Change Service information. See [Ingestion Storage](docs/ingestion-storage.md#licensing-and-attribution) for the full obligations.
