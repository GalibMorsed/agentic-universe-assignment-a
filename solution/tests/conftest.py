import sys
from pathlib import Path

SOLUTION_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SOLUTION_DIR.parent
sys.path.insert(0, str(SOLUTION_DIR))   # lets `pytest` run from the repo root or from solution/
