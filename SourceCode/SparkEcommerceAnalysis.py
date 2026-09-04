"""Pipeline ETL xử lý Data Lakehouse (Bronze -> Silver -> Gold) bằng PySpark & Delta Lake.

Wrapper tương thích ngược ủy quyền xử lý cho package `lakehouse.pipeline`.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Thêm SourceCode vào sys.path để import lakehouse package
SOURCE_DIR = Path(__file__).resolve().parent
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from lakehouse.pipeline import run_pipeline


def main() -> None:
    """Khởi chạy toàn bộ Pipeline Lakehouse."""
    spark = run_pipeline()
    spark.stop()


if __name__ == "__main__":
    main()
