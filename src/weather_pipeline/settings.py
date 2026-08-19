"""Environment-backed configuration with safe local defaults.

The variable table and defaults are the runbook's configuration contract:
docs/operations-runbook.md#configuration. This module is the only reader of
WEATHER_PIPELINE_* environment variables.
"""

import os
from dataclasses import dataclass
from pathlib import Path

ENV_PREFIX = "WEATHER_PIPELINE_"
DEFAULT_OPEN_METEO_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_DUCKDB_PATH = Path("warehouse/weather.duckdb")
DEFAULT_LANDING_DIR = Path("data/raw")
DEFAULT_LOG_LEVEL = "INFO"


@dataclass(frozen=True)
class Settings:
    open_meteo_base_url: str
    duckdb_path: Path
    landing_dir: Path
    log_level: str

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        return cls(
            open_meteo_base_url=env.get(
                f"{ENV_PREFIX}OPEN_METEO_BASE_URL", DEFAULT_OPEN_METEO_BASE_URL
            ),
            duckdb_path=Path(env.get(f"{ENV_PREFIX}DUCKDB_PATH", DEFAULT_DUCKDB_PATH)),
            landing_dir=Path(env.get(f"{ENV_PREFIX}LANDING_DIR", DEFAULT_LANDING_DIR)),
            log_level=env.get(f"{ENV_PREFIX}LOG_LEVEL", DEFAULT_LOG_LEVEL),
        )
