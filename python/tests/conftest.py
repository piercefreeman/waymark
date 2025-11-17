from __future__ import annotations

import sys
from pathlib import Path

PROJECT_PY_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_PY_DIR / "src"

for path in (PROJECT_PY_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
