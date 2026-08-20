# One shared local Dagster instance for dev, schedules, and script runs;
# the instance config is committed at .dagster/dagster.yaml.
export DAGSTER_HOME := $(abspath .dagster)

.PHONY: setup dev ui bootstrap backfill reconcile rebuild-raw dbt-build test verify lint format clean

setup:
	uv sync
	uv run pre-commit install

dev:
	uv run dagster dev -m weather_pipeline.definitions

web-ui:
	uv run uvicorn weather_pipeline.ui.app:app

bootstrap:
	uv run python scripts/bootstrap.py

backfill:
	uv run python scripts/backfill.py --from $(FROM) --to $(TO)

reconcile:
	uv run python scripts/backfill.py --last 8

rebuild-raw:
	uv run python scripts/rebuild_raw.py

dbt-build:
	cd dbt && uv run dbt build --profiles-dir .

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
