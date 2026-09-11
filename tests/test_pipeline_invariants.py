"""Bộ kiểm thử các bất biến của order lakehouse.

Kiểm chứng các thuộc tính cốt lõi của Data Lakehouse:
1. Idempotency: Cùng một source hash không bao giờ bị append trùng vào Bronze.
2. Grain ổn định: Các micro-batch của cùng một Order không ghi đè chéo dòng sản phẩm.
3. Contract thực thi: contracts/ecommerce_order.yaml là nguồn quy tắc runtime duy nhất.
4. Đối soát Gold: Pipeline dừng ngay khi bất biến dữ liệu bị vi phạm.
5. Lineage chuẩn: FactSales và Gold Marts bảo toàn 100% doanh thu và phép tính.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from lakehouse.dimensions import build_all_dimensions
from lakehouse.marts import (
    build_all_marts,
    build_fact_order_fulfillment,
    build_fact_sales,
    build_sales_enriched,
)
from lakehouse.reconciliation import assert_unique_grain, run_full_reconciliation
from lakehouse.registry import BatchConflictError, BatchRegistry
from lakehouse.silver import (
    build_silver_order_lines_current,
    build_silver_orders_current,
    clean_and_enrich_silver,
)
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum


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
