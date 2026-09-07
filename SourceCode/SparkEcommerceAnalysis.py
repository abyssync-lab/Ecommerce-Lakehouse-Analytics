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

import argparse
import uuid

from config import SETTINGS
from lakehouse.ingestion import calculate_source_hash, read_raw_csv
from lakehouse.pipeline import (
    create_spark_session,
    run_delta_demo,
    run_incremental_pipeline,
    run_pipeline,
)
from lakehouse.registry import BatchRegistry


def main() -> None:
    """Khởi chạy toàn bộ Pipeline Lakehouse."""
    parser = argparse.ArgumentParser(description="Spark Lakehouse Pipeline Runner")
    parser.add_argument("--input", type=str, default=None, help="Đường dẫn file CSV đầu vào")
    parser.add_argument("--scd2", action="store_true", default=None, help="Bật SCD Type 2")
    parser.add_argument(
        "--incremental", action="store_true", default=False, help="Chạy chế độ Incremental MERGE"
    )
    parser.add_argument("--batch-id", type=str, default=None, help="Mã nhận diện batch nạp")
    parser.add_argument(
        "--demo", action="store_true", default=False, help="Chạy Delta Lakehouse Demos"
    )
    args = parser.parse_args()

    use_scd2 = args.scd2 if args.scd2 is not None else SETTINGS.use_scd2

    if args.incremental:
        spark = create_spark_session()
        source_uri = args.input or SETTINGS.get_input_path()
        source_hash = "hash_unavailable"
        # Hash phải được tính từ bytes của file trước khi DataFrame đi vào pipeline;
        # không dùng batch_id làm giả source identity.
        try:
            source_hash = calculate_source_hash(source_uri)
            new_batch_df = read_raw_csv(spark, args.input)
        except Exception as exc:
            # Khi file chưa đọc được, chưa thể có content hash. Dùng sentinel chỉ
            # cho run FAILED; sentinel này luôn bị loại khỏi idempotency lookup.
            registry = BatchRegistry(spark)
            run_id = f"run_{uuid.uuid4().hex[:8]}"
            batch_id = args.batch_id or f"batch_{source_hash[:8]}"
            registry.start_run(
                run_id=run_id,
                batch_id=batch_id,
                source_uri=source_uri,
                source_hash=source_hash,
                contract_version="2.0.0",
            )
            error_code = (
                "SOURCE_FILE_NOT_FOUND"
                if isinstance(exc, FileNotFoundError)
                else "FILE_SCHEMA_MISMATCH"
            )
            registry.mark_failed(run_id, exc, error_code=error_code)
            spark.stop()
            raise
        result = run_incremental_pipeline(
            new_batch_df,
            spark=spark,
            batch_id=args.batch_id,
            use_scd2=use_scd2,
            source_hash=source_hash,
            source_uri=source_uri,
        )
        active_spark = result.spark or spark
    else:
        result = run_pipeline(input_path=args.input, use_scd2=use_scd2)
        active_spark = result.spark

    if active_spark is not None:
        if args.demo or SETTINGS.run_delta_demo:
            run_delta_demo(active_spark)
        active_spark.stop()


if __name__ == "__main__":
    main()
