"""Pipeline ETL xử lý Data Lakehouse (Bronze -> Silver -> Gold) bằng PySpark và Delta Lake.

Đây là điểm vào tương thích với lệnh cũ; logic nghiệp vụ nằm trong `lakehouse.pipeline`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Thêm SourceCode vào sys.path để nạp mã nguồn lakehouse.
SOURCE_DIR = Path(__file__).resolve().parent
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from config import SETTINGS
from lakehouse.pipeline import run_incremental_from_path, run_pipeline


def main() -> None:
    """Khởi chạy toàn bộ Pipeline Lakehouse."""
    parser = argparse.ArgumentParser(description="Spark Lakehouse Pipeline Runner")
    parser.add_argument("--input", type=str, default=None, help="Đường dẫn file CSV đầu vào")
    parser.add_argument("--scd2", action="store_true", default=None, help="Bật SCD Type 2")
    parser.add_argument(
        "--incremental", action="store_true", default=False, help="Chạy chế độ Incremental MERGE"
    )
    parser.add_argument("--batch-id", type=str, default=None, help="Mã nhận diện batch nạp")
    args = parser.parse_args()

    use_scd2 = args.scd2 if args.scd2 is not None else SETTINGS.use_scd2

    if args.incremental:
        run_incremental_from_path(
            input_path=args.input,
            batch_id=args.batch_id,
            use_scd2=use_scd2,
        )
        return

    result = run_pipeline(input_path=args.input, use_scd2=use_scd2)
    if result.spark is not None:
        result.spark.stop()


if __name__ == "__main__":
    main()
