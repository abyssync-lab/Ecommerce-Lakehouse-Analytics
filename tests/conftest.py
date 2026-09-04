"""Thiết lập đường dẫn import chung cho bộ kiểm thử."""

import sys
from pathlib import Path

SOURCE_DIR = Path(__file__).parents[1] / "SourceCode"
sys.path.insert(0, str(SOURCE_DIR))
