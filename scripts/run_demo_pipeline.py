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


def _require(condition: bool, message: str) -> None:
    """Dừng demo với thông báo đủ ngữ cảnh và tạo annotation rõ trên GitHub."""
    if not condition:
        print(f"::error title=GlobalCart incremental demo::{message}")
        raise RuntimeError(message)


def main() -> int:
    """Chạy hai batch độc lập rồi chạy lại batch thứ hai bằng tên file khác."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    batch_one_path = FIXTURE_DIR / "order_events_batch_001.csv"
    batch_two_path = FIXTURE_DIR / "order_events_batch_002.csv"
    if not batch_one_path.is_file() or not batch_two_path.is_file():
        raise FileNotFoundError("Thiếu fixture incremental trong tests/fixtures")

    # SETTINGS được khởi tạo khi import package; đặt storage cô lập trước đó để
    # kiểm thử nhanh không ghi đè Output/lakehouse của người dùng.
    with TemporaryDirectory(prefix="globalcart_demo_") as storage_dir:
        os.environ["ECOMMERCE_LOCAL_STORAGE_BASE"] = storage_dir
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
        if str(SOURCE_DIR) not in sys.path:
            sys.path.insert(0, str(SOURCE_DIR))

        from config import SETTINGS
        from lakehouse.ingestion import calculate_source_hash
        from lakehouse.storage import resolve_path

        try:
            from lakehouse.pipeline import create_spark_session, run_incremental_pipeline
        except ModuleNotFoundError as exc:
            print(
                f"LỖI: scripts/run_demo_pipeline.py yêu cầu môi trường có PySpark và Java. ({exc})",
                file=sys.stderr,
            )
            return 1

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
            _require(
                first.status == "SUCCESS",
                f"Batch 001 có trạng thái bất ngờ: {first.status!r}",
            )
            _require(
                first.reconciliation_passed,
                f"Batch 001 reconciliation không đạt: {first.reconciliation_report!r}",
            )

            second = run_incremental_pipeline(
                batch_two,
                spark=spark,
                batch_id="demo_batch_002",
                source_hash=hash_two,
                source_uri=str(batch_two_path),
            )
            _require(
                second.status == "SUCCESS",
                f"Batch 002 có trạng thái bất ngờ: {second.status!r}",
            )
            _require(
                second.reconciliation_passed,
                f"Batch 002 reconciliation không đạt: {second.reconciliation_report!r}",
            )
            _require(
                second.published_run_id == second.run_id,
                "Batch 002 không trỏ publication về chính run vừa hoàn tất.",
            )

            # Kiểm chứng thứ tự đúng: chọn DELETE mới nhất trước khi lọc bản ghi xóa.
            lines_path = SETTINGS.get_storage_path(SETTINGS.silver_order_lines_delta)
            active_lines = spark.read.format("delta").load(lines_path).filter("Is_Deleted = false")
            remaining = active_lines.select(
                "Order_ID", "Order_Line_ID", "Quantity", "Net_Line_Amount"
            ).collect()
            remaining_keys = [(row.Order_ID, row.Order_Line_ID, row.Quantity) for row in remaining]
            _require(
                remaining_keys == [("A001", "L1", 2)],
                f"Silver active lines sai sau DELETE: {remaining_keys!r}",
            )
            _require(
                remaining[0].Net_Line_Amount == 200.0,
                f"Net_Line_Amount của A001-L1 sai: {remaining[0].Net_Line_Amount!r}",
            )

            sales_path = resolve_path("/ecommerce/serving/fact_sales_line_delta")
            sales = (
                spark.read.format("delta")
                .load(sales_path)
                .filter(f"Publication_Run_ID = '{second.run_id}'")
            )
            sales_count = sales.count()
            _require(
                sales_count == 1,
                f"Fact sales của run {second.run_id} có {sales_count} dòng, kỳ vọng 1.",
            )
            sales_amount = sales.select("Net_Line_Amount").first()[0]
            _require(
                sales_amount == 200.0,
                f"Fact sales Net_Line_Amount sai: {sales_amount!r}",
            )

            order_fact_path = resolve_path("/ecommerce/serving/fact_order_fulfillment_delta")
            order_fact = (
                spark.read.format("delta")
                .load(order_fact_path)
                .filter(f"Publication_Run_ID = '{second.run_id}'")
            )
            order_fact_count = order_fact.count()
            _require(
                order_fact_count == 1,
                f"Fact fulfillment của run {second.run_id} có {order_fact_count} dòng, kỳ vọng 1.",
            )
            order_row = order_fact.first()
            _require(
                order_row.Order_ID == "A001",
                f"Fact fulfillment trả order bất ngờ: {order_row.Order_ID!r}",
            )
            _require(
                order_row.Shipping_Cost == 20.0,
                f"Shipping_Cost của A001 sai: {order_row.Shipping_Cost!r}",
            )

            # Sao chép đúng dãy byte sang tên file khác để chứng minh mã băm dựa trên nội dung.
            replay_path = Path(storage_dir) / "renamed_replay.csv"
            replay_path.write_bytes(batch_two_path.read_bytes())
            replay = run_incremental_pipeline(
                _read_event_file(spark, replay_path),
                spark=spark,
                batch_id="demo_batch_002_replay",
                source_hash=calculate_source_hash(str(replay_path)),
                source_uri=str(replay_path),
            )
            _require(
                replay.status == "SKIPPED",
                f"Replay cùng nội dung không bị bỏ qua, trạng thái: {replay.status!r}",
            )

            print("Kiểm thử đầu cuối lakehouse đã đạt.")
            print(f"first={first.run_id} second={second.run_id} replay={replay.status}")
            return 0
        finally:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
