"""Chạy kiểm thử nhanh cho luồng incremental, DELETE, replay và serving."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "SourceCode"
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures"


def _read_event_file(spark, path: Path):
    """Đọc fixture v2 dưới dạng string; pipeline sẽ cast theo contract."""
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


def main() -> int:
    """Chạy hai batch độc lập rồi chạy lại batch thứ hai bằng tên file khác."""
    batch_one_path = FIXTURE_DIR / "order_events_batch_001.csv"
    batch_two_path = FIXTURE_DIR / "order_events_batch_002.csv"
    if not batch_one_path.is_file() or not batch_two_path.is_file():
        raise FileNotFoundError("Thiếu fixture incremental trong tests/fixtures")

    # SETTINGS được khởi tạo khi import package; đặt storage cô lập trước đó để
    # Kiểm thử nhanh không ghi đè Output/lakehouse của người dùng.
    with TemporaryDirectory(prefix="globalcart_demo_") as storage_dir:
        os.environ["ECOMMERCE_LOCAL_STORAGE_BASE"] = storage_dir
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
        if str(SOURCE_DIR) not in sys.path:
            sys.path.insert(0, str(SOURCE_DIR))

        from config import SETTINGS
        from lakehouse.ingestion import calculate_source_hash
        from lakehouse.pipeline import create_spark_session, run_incremental_pipeline
        from lakehouse.storage import resolve_path

        spark = create_spark_session()
        try:
            batch_one = _read_event_file(spark, batch_one_path)
            batch_two = _read_event_file(spark, batch_two_path)
            hash_one = calculate_source_hash(str(batch_one_path))
            hash_two = calculate_source_hash(str(batch_two_path))

            first = run_incremental_pipeline(
                batch_one,
                spark=spark,
                batch_id="demo_batch_001",
                source_hash=hash_one,
                source_uri=str(batch_one_path),
            )
            assert first.status == "SUCCESS"
            assert first.reconciliation_passed

            second = run_incremental_pipeline(
                batch_two,
                spark=spark,
                batch_id="demo_batch_002",
                source_hash=hash_two,
                source_uri=str(batch_two_path),
            )
            assert second.status == "SUCCESS"
            assert second.reconciliation_passed
            assert second.published_run_id == second.run_id

            # Kiểm chứng thứ tự đúng: chọn DELETE mới nhất trước khi lọc bản ghi xóa.
            lines_path = SETTINGS.get_storage_path(SETTINGS.silver_order_lines_delta)
            active_lines = spark.read.format("delta").load(lines_path).filter("Is_Deleted = false")
            remaining = active_lines.select(
                "Order_ID", "Order_Line_ID", "Quantity", "Net_Line_Amount"
            ).collect()
            assert [(row.Order_ID, row.Order_Line_ID, row.Quantity) for row in remaining] == [
                ("A001", "L1", 2)
            ]
            assert remaining[0].Net_Line_Amount == 200.0

            sales_path = resolve_path("/ecommerce/serving/fact_sales_line_delta")
            sales = (
                spark.read.format("delta")
                .load(sales_path)
                .filter(f"Publication_Run_ID = '{second.run_id}'")
            )
            assert sales.count() == 1
            assert sales.select("Net_Line_Amount").first()[0] == 200.0

            order_fact_path = resolve_path("/ecommerce/serving/fact_order_fulfillment_delta")
            order_fact = (
                spark.read.format("delta")
                .load(order_fact_path)
                .filter(f"Publication_Run_ID = '{second.run_id}'")
            )
            assert order_fact.count() == 1
            order_row = order_fact.first()
            assert order_row.Order_ID == "A001"
            assert order_row.Shipping_Cost == 20.0

            # Sao chép đúng bytes sang tên file khác để chứng minh hash dựa trên nội dung.
            replay_path = Path(storage_dir) / "renamed_replay.csv"
            replay_path.write_bytes(batch_two_path.read_bytes())
            replay = run_incremental_pipeline(
                _read_event_file(spark, replay_path),
                spark=spark,
                batch_id="demo_batch_002_replay",
                source_hash=calculate_source_hash(str(replay_path)),
                source_uri=str(replay_path),
            )
            assert replay.status == "SKIPPED"

            print("End-to-end lakehouse smoke test passed.")
            print(f"first={first.run_id} second={second.run_id} replay={replay.status}")
            return 0
        finally:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
