# uv-based image: build the venv once, run Dagster services from it
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/home \
    WEATHER_PIPELINE_DUCKDB_PATH=/app/warehouse/weather.duckdb \
    WEATHER_PIPELINE_LANDING_DIR=/app/data/raw

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY README.md ./
COPY workspace.yaml ./
COPY src ./src
COPY dbt ./dbt
COPY scripts ./scripts
COPY .dagster/dagster.yaml /opt/dagster/home/dagster.yaml
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev \
    && mkdir -p /app/warehouse /app/data/raw

EXPOSE 3000
