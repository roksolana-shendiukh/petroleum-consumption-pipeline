import argparse
import logging
import os
import sys

sys.dont_write_bytecode = True

import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_tests")
logger.info("arguments received: %s", sys.argv)

parser = argparse.ArgumentParser()
parser.add_argument("root")
parser.add_argument("--threshold", type=float, default=0.0)
args, ignored = parser.parse_known_args(sys.argv[1:])
if ignored:
    logger.info("ignored arguments: %s", ignored)

os.environ["COVERAGE_FILE"] = "/tmp/.coverage"
sys.path.insert(0, f"{args.root}/src")

code = pytest.main([
    f"{args.root}/tests",
    "-v",
    "-p", "no:cacheprovider",
    "--import-mode=importlib",
    f"--cov={args.root}/src/petroleum_transformations",
    "--cov-report=term-missing",
    f"--cov-fail-under={args.threshold}",
])
if code != 0:
    raise RuntimeError(f"pytest failed with exit code {code}")