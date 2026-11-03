"""
Unit tests for the downloaders. They run offline: CDSE is replaced by fakes.

Run from the repository root:
    python -m unittest discover -s tests -t .
"""

import sys
from pathlib import Path

# The scripts in src/ import each other as top-level modules
# (e.g. `from cdse import ...`), so src/ must be on the import path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
