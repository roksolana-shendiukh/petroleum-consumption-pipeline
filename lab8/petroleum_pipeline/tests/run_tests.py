import os
import sys

sys.dont_write_bytecode = True

import pytest

root = sys.argv[1]
threshold = sys.argv[2] if len(sys.argv) > 2 else "0"

os.environ["COVERAGE_FILE"] = "/tmp/.coverage"
sys.path.insert(0, f"{root}/src")

code = pytest.main([
    f"{root}/tests",
    "-v",
    "-p", "no:cacheprovider",
    "--import-mode=importlib",
    f"--cov={root}/src/petroleum_transformations",
    "--cov-report=term-missing",
    f"--cov-fail-under={threshold}",
])
if code != 0:
    raise RuntimeError(f"pytest failed with exit code {code}")