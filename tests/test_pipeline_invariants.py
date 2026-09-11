"""Bộ kiểm thử các bất biến của order lakehouse.

Kiểm chứng các thuộc tính cốt lõi của Data Lakehouse:
1. Idempotency: Cùng một source hash không bao giờ bị append trùng vào Bronze.
2. Grain ổn định: Các micro-batch của cùng một Order không ghi đè chéo dòng sản phẩm.
3. Contract thực thi: contracts/ecommerce_order.yaml là nguồn quy tắc runtime duy nhất.
4. Đối soát Gold: Pipeline dừng ngay khi bất biến dữ liệu bị vi phạm.
5. Lineage chuẩn: FactSales và Gold Marts bảo toàn 100% doanh thu và phép tính.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from lakehouse.contracts.loader import load_contract, load_contract_for_columns
from lakehouse.dimensions import build_all_dimensions
from lakehouse.ingestion import calculate_source_hash, calculate_source_size, validate_raw_schema
from lakehouse.marts import (
    build_all_marts,
    build_fact_order_fulfillment,
    build_fact_sales,
    build_sales_enriched,
)
from lakehouse.pipeline import PipelineRunResult
from lakehouse.reconciliation import assert_unique_grain, run_full_reconciliation
from lakehouse.registry import BatchConflictError, BatchRegistry
from lakehouse.silver import (
    build_silver_order_lines_current,
    build_silver_orders_current,
    clean_and_enrich_silver,
)
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum


def test_contract_yaml_is_runtime_source_of_truth():
    """Kiểm tra contracts/ecommerce_order.yaml là nguồn chân lý điều khiển runtime."""
    contract = load_contract()
    assert contract.dataset == "ecommerce_order"
    assert "Order_ID" in contract.required_columns
    assert "Quantity" in contract.required_columns
    assert "Delivered" in contract.allowed_order_statuses

    import data_quality

    assert data_quality.REQUIRED_COLUMNS == contract.required_columns
    assert data_quality.ALLOWED_ORDER_STATUSES == contract.allowed_order_statuses


def test_change_contract_v2_is_selected_for_incremental_event_shape():
    """Event shape v2 phải được chọn tự động, không dùng nhầm contract bootstrap v1."""
    contract = load_contract_for_columns(
        {"Order_ID", "Order_Line_ID", "Source_Updated_At", "Operation"}
    )
    assert contract.version == "2.0.0"
    assert contract.sequence_column == "Source_Updated_At"
    assert contract.operation_column == "Operation"
    assert contract.order_line_key_cols == ["Order_ID", "Order_Line_ID"]


def test_partial_incremental_shape_fails_before_bronze():
    """File v2 thiếu một marker phải bị chặn ở file-level validation."""

    class RawFrame:
        columns = ["Order_ID", "Order_Line_ID", "Source_Updated_At"]

    with pytest.raises(ValueError, match="FILE_SCHEMA_MISMATCH"):
        validate_raw_schema(RawFrame())


def test_order_line_key_is_stable_across_micro_batches(spark_session):
    """BẢO VỆ P0: Hai micro-batch chứa các dòng khác nhau của cùng 1 đơn hàng KHÔNG trùng Order_Line_ID."""
    cols = [
        "Order_ID",
        "Order_Date",
        "Year",
        "Month",
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        "Product_Name",
        "Category",
        "Sub_Category",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Revenue",
        "Cost",
        "Profit",
        "Shipping_Cost",
        "Shipping_Days",
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "Region",
        "Country",
    ]
    # Batch 1: Order ORD-99 mua Laptop Pro
    b1_data = [
        (
            "ORD-99",
            "2026-08-01",
            2026,
            8,
            "C100",
            "Male",
            "Consumer",
            "Laptop Pro",
            "Tech",
            "PC",
            1,
            1000.0,
            0.0,
            1000.0,
            700.0,
            300.0,
            20.0,
            2,
            "Delivered",
            "Card",
            "Standard",
            "Asia",
            "Vietnam",
        )
    ]
    # Batch 2: Cùng Order ORD-99 nhưng mua thêm Mouse Pro (giao dịch phát sinh sau hoặc bổ sung)
    b2_data = [
        (
            "ORD-99",
            "2026-08-01",
            2026,
            8,
            "C100",
            "Male",
            "Consumer",
            "Mouse Pro",
            "Tech",
            "Accessory",
            1,
            50.0,
            0.0,
            50.0,
            30.0,
            20.0,
            5.0,
            2,
            "Delivered",
            "Card",
            "Standard",
            "Asia",
            "Vietnam",
        )
    ]

    df1 = spark_session.createDataFrame(b1_data, cols)
    df2 = spark_session.createDataFrame(b2_data, cols)

    clean1 = clean_and_enrich_silver(df1)
    clean2 = clean_and_enrich_silver(df2)

    line1 = clean1.select("Order_Line_ID").collect()[0][0]
    line2 = clean2.select("Order_Line_ID").collect()[0][0]

    # KHẲNG ĐỊNH: Hai dòng sản phẩm khác nhau trong cùng order PHẢI có line identity khác nhau,
    # không được bị gán cùng ORD-99-1 như khi dùng row_number() đơn thuần!
    assert line1 != line2, f"Xung đột Order_Line_ID: cả 2 dòng đều nhận {line1}"
    assert line1.startswith("ORD-99-")
    assert line2.startswith("ORD-99-")


def test_same_source_hash_idempotency_detection(spark_session, tmp_path):
    """Kiểm tra BatchRegistry phát hiện và đánh dấu batch đã xử lý thành công để tránh nạp trùng."""
    registry_path = (tmp_path / "delta_registry").as_uri()
    registry = BatchRegistry(spark_session, registry_path=registry_path)

    sample_hash = "abc123def4567890abcdef1234567890abcdef1234567890abcdef1234567890"

    # Khi chưa nạp
    assert not registry.is_batch_processed(sample_hash)

    # Đăng ký và hoàn thành thành công
    registry.start_run(
        run_id="run_001",
        batch_id="batch_001",
        source_uri="test.csv",
        source_hash=sample_hash,
        raw_rows=100,
    )
    registry.mark_published(
        run_id="run_001",
        gold_run_id="run_001",
        published_version="1",
    )

    # Khi đã hoàn thành thành công
    assert registry.is_batch_processed(sample_hash)


def test_registry_uses_one_schema_and_rejects_batch_id_conflict(spark_session, tmp_path):
    """Một run chỉ có một row lifecycle; reuse batch_id với hash khác phải fail."""
    registry = BatchRegistry(spark_session, registry_path=(tmp_path / "registry").as_uri())
    registry.start_run(
        run_id="run_lifecycle",
        batch_id="batch_lifecycle",
        source_uri="orders_a.csv",
        source_hash="hash_a",
    )
    registry.update_metrics("run_lifecycle", raw_rows=10, valid_event_rows=10)
    registry.mark_failed("run_lifecycle", "row validation failed", error_code="DQ_FAILED")

    failed = registry.find_by_run_id("run_lifecycle")
    assert failed["status"] == "FAILED"
    assert failed["error_code"] == "DQ_FAILED"
    assert failed["raw_rows"] == 10
    assert registry._read().count() == 1

    with pytest.raises(BatchConflictError):
        registry.start_run(
            run_id="run_retry",
            batch_id="batch_lifecycle",
            source_uri="orders_b.csv",
            source_hash="hash_b",
        )


def test_reconciliation_fails_when_revenue_discrepant(spark_session):
    """Kiểm tra Gold reconciliation phát hiện FAIL khi số liệu doanh thu bị lệch."""
    cols = [
        "Order_ID",
        "Order_Date",
        "Year",
        "Month",
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        "Product_Name",
        "Category",
        "Sub_Category",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Revenue",
        "Cost",
        "Profit",
        "Shipping_Cost",
        "Shipping_Days",
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "Region",
        "Country",
    ]
    data = [
        (
            "ORD01",
            "2026-08-01",
            2026,
            8,
            "C001",
            "Male",
            "Consumer",
            "Laptop Pro",
            "Electronics",
            "Tech",
            1,
            1000.0,
            0.0,
            1000.0,
            700.0,
            300.0,
            20.0,
            2,
            "Delivered",
            "Card",
            "Standard",
            "Asia",
            "Vietnam",
        )
    ]
    df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(df)
    dims = build_all_dimensions(spark_session, clean_df)
    fact = build_fact_sales(clean_df, dims)
    order_fact = build_fact_order_fulfillment(
        build_silver_orders_current(clean_df),
        build_silver_order_lines_current(clean_df),
        dims,
    )

    assert_unique_grain(fact, ["Order_ID", "Order_Line_ID"])
    assert_unique_grain(order_fact, ["Order_ID"])
    marts = build_all_marts(clean_df)

    # Cố tình giả lập sai lệch doanh thu trong overview mart.
    corrupted_mart = marts["mart_overview"].withColumn(
        "Total_Revenue", col("Total_Revenue") + 999.0
    )

    report = run_full_reconciliation(
        clean_df=clean_df,
        fact_sales=fact,
        mart_overview=corrupted_mart,
        dim_customer=dims["dim_customer"],
        raw_count=1,
        duplicate_count=0,
        invalid_count=0,
        run_id="test_corrupt_run",
    )

    assert report["overall_status"] == "FAIL"
    assert not report["checks"]["revenue_invariant"]["passed"]


def test_gold_sales_enriched_and_marts_consistency(spark_session):
    """Kiểm tra tính nhất quán 100% giữa Canonical Semantic Base (gold_sales_enriched) và FactSales."""
    cols = [
        "Order_ID",
        "Order_Date",
        "Year",
        "Month",
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        "Product_Name",
        "Category",
        "Sub_Category",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Revenue",
        "Cost",
        "Profit",
        "Shipping_Cost",
        "Shipping_Days",
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "Region",
        "Country",
    ]
    data = [
        (
            "ORD01",
            "2026-08-01",
            2026,
            8,
            "C001",
            "Male",
            "Consumer",
            "Laptop Pro",
            "Electronics",
            "Tech",
            1,
            1200.0,
            0.0,
            1200.0,
            800.0,
            400.0,
            25.0,
            2,
            "Delivered",
            "Card",
            "Standard",
            "Asia",
            "Vietnam",
        ),
        (
            "ORD02",
            "2026-08-02",
            2026,
            8,
            "C002",
            "Female",
            "Corporate",
            "Mouse Pro",
            "Electronics",
            "Tech",
            2,
            50.0,
            0.1,
            90.0,
            40.0,
            50.0,
            5.0,
            1,
            "Delivered",
            "Card",
            "Standard",
            "Asia",
            "Vietnam",
        ),
    ]
    df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(df)
    dims = build_all_dimensions(spark_session, clean_df)
    fact = build_fact_sales(clean_df, dims)

    assert set(dims) == {
        "dim_date",
        "dim_product",
        "dim_customer",
        "dim_geography",
        "dim_order_context",
    }
    assert {"GeographyKey", "ContextKey"}.issubset(set(fact.columns))
    assert dims["dim_geography"].filter(col("GeographyKey") == 0).count() == 1
    assert dims["dim_order_context"].filter(col("ContextKey") == 0).count() == 1

    enriched = build_sales_enriched(fact, dims)
    assert enriched.count() == fact.count()
    assert "Order_Date" in enriched.columns
    assert {str(row["Order_Date"]) for row in enriched.select("Order_Date").collect()} == {
        "2026-08-01",
        "2026-08-02",
    }

    # Kiểm tra mart chạy trên semantic base, không quay lại đọc dữ liệu Silver trực tiếp.
    marts = build_all_marts(enriched)
    assert "mart_order_summary" in marts
    assert "mart_rfm_customer_segmentation" in marts

    fact_rev = round(float(fact.select(spark_sum("Net_Line_Amount")).collect()[0][0]), 2)
    enriched_rev = round(float(enriched.select(spark_sum("Net_Line_Amount")).collect()[0][0]), 2)

    assert fact_rev == enriched_rev == 1290.0


def test_calculate_source_hash_idempotency(tmp_path):
    """Kiểm tra calculate_source_hash sinh hash tất định theo nội dung file, không phụ thuộc tên path."""
    file1 = tmp_path / "batch_alpha.csv"
    file2 = tmp_path / "batch_beta.csv"
    file3 = tmp_path / "batch_modified.csv"

    file1.write_bytes(b"Order_ID,Revenue\nORD-1,100.0\n")
    file2.write_bytes(b"Order_ID,Revenue\nORD-1,100.0\n")
    file3.write_bytes(b"Order_ID,Revenue\nORD-1,200.0\n")

    hash1 = calculate_source_hash(str(file1))
    hash2 = calculate_source_hash(str(file2))
    hash3 = calculate_source_hash(str(file3))

    assert hash1 == hash2, (
        "Hai file cùng nội dung nhưng khác tên phải sinh ra hash giống nhau để chống duplicate!"
    )
    assert hash1 != hash3, "Nội dung file khác nhau phải sinh ra hash khác nhau!"


def test_source_hash_and_size_accept_file_uri(tmp_path):
    """Đảm bảo file URI không bị mất dấu slash khi tính metadata nguồn."""
    source = tmp_path / "orders batch.csv"
    content = b"Order_ID,Revenue\nORD-1,100.0\n"
    source.write_bytes(content)

    assert calculate_source_hash(source.as_uri()) == hashlib.sha256(content).hexdigest()
    assert calculate_source_size(source.as_uri()) == len(content)


def test_pipeline_run_result_dataclass_contract():
    """Kiểm tra PipelineRunResult khởi tạo đúng các trường của một run."""
    res = PipelineRunResult(
        run_id="run_123",
        batch_id="batch_456",
        status="SUCCESS",
        bronze_rows=1000,
        silver_rows=950,
        quarantine_rows=40,
        duplicate_rows=10,
        reconciliation_passed=True,
    )
    assert res.run_id == "run_123"
    assert res.status == "SUCCESS"
    assert res.bronze_rows == res.silver_rows + res.quarantine_rows + res.duplicate_rows
