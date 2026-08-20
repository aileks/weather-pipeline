"""One home for invoking the dbt CLI from Python.

dbt always runs with its working directory inside dbt/ so recorded paths
stay project-relative; scripts and assets share this module instead of
re-declaring the invocation shape.
"""

import subprocess
import sys
from pathlib import Path

DBT_DIR = Path(__file__).parents[2].resolve() / "dbt"


def run_dbt(args: list[str]) -> int:
    """Run the venv's dbt inside the project directory; return its exit code."""
    return subprocess.run(
        [str(Path(sys.executable).parent / "dbt"), *args],
        cwd=DBT_DIR,
        check=False,
    ).returncode
