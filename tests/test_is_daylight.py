import re
from pathlib import Path

import duckdb
import jinja2

MACRO_PATH = Path(__file__).parents[1] / "dbt" / "macros" / "is_daylight.sql"


def is_daylight(latitude: float, longitude: float, ts: str) -> bool:
    """Execute the real macro file's SQL for one (coordinate, timestamp) pair."""
    text = MACRO_PATH.read_text()
    body = re.search(
        r"{% macro is_daylight\(latitude, longitude, ts_utc\) %}(.*?){% endmacro %}",
        text,
        re.DOTALL,
    ).group(1)
    rendered = jinja2.Template(body).render(
        latitude=str(latitude),
        longitude=str(longitude),
        ts_utc=f"TIMESTAMP '{ts}'",
    )
    return duckdb.sql(f"select ({rendered})::boolean as is_day").fetchone()[0]


def test_reykjavik_summer_day():
    assert is_daylight(64.1466, -21.9426, "2026-08-17 12:00:00") is True


def test_reykjavik_midnight_sun():
    assert is_daylight(64.1466, -21.9426, "2026-06-21 23:30:00") is True


def test_svalbard_polar_night():
    # reykjavik sits below the arctic circle and has no true polar night;
    # svalbard at 78.2 degrees north does
    assert is_daylight(78.22, 15.65, "2026-12-21 12:00:00") is False


def test_equator_noon_is_day():
    assert is_daylight(0.0, -78.47, "2026-03-20 17:00:00") is True


def test_equator_midnight_is_night():
    assert is_daylight(0.0, -78.47, "2026-03-20 05:00:00") is False


def test_london_around_equinox_sunrise():
    assert is_daylight(51.5072, -0.1276, "2026-03-20 07:00:00") is True
    assert is_daylight(51.5072, -0.1276, "2026-03-20 04:30:00") is False


def test_southern_hemisphere_winter():
    assert is_daylight(-33.8688, 151.2093, "2026-08-17 03:00:00") is True
    assert is_daylight(-33.8688, 151.2093, "2026-08-17 18:00:00") is False
