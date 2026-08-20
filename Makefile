# One shared local Dagster instance for dev, schedules, and script runs;
# the instance config is committed at .dagster/dagster.yaml.
export DAGSTER_HOME := $(abspath .dagster)

.PHONY: setup dev bootstrap backfill reconcile rebuild-raw dbt-build test verify lint format clean

setup:
	uv sync
	uv run pre-commit install

dev:
	uv run dagster dev -m weather_pipeline.definitions

bootstrap:
	uv run dbt build --project-dir dbt --profiles-dir dbt \
		--select cities dim_location dim_date stg_hourly_observations

backfill:
	uv run python scripts/backfill.py --from $(FROM) --to $(TO)

reconcile:
	uv run python scripts/backfill.py --last 8

rebuild-raw:
	uv run python scripts/rebuild_raw.py

dbt-build:
	uv run dbt build --project-dir dbt --profiles-dir dbt

test:
	uv run pytest

verify:
	uv run python scripts/verify_offline.py

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

clean:
	rm -rf .pytest_cache .ruff_cache dbt/target
