.PHONY: setup dev bootstrap backfill reconcile rebuild-raw dbt-build test verify-offline lint fmt clean

setup:
	uv sync
	uv run pre-commit install

dev:
	uv run dg dev

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

verify-offline:
	uv run python scripts/verify_offline.py

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

clean:
	rm -rf .pytest_cache .ruff_cache dbt/target
