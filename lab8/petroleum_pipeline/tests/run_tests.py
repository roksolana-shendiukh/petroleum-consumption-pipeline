import sys
sys.dont_write_bytecode = True 
import pytest

root = sys.argv[1]  
sys.path.insert(0, f"{root}/src")

code = pytest.main([
    f"{root}/tests",
    "-v",
    "-p", "no:cacheprovider",
    f"--rootdir={root}",
    "--import-mode=importlib",
])
if code != 0:
    raise RuntimeError(f"pytest failed with exit code {code}")