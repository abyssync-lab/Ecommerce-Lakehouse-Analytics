"""Unit test cho các module trong tầng PySpark Medallion Lakehouse (marts, dimensions, silver)."""

from __future__ import annotations

import sys
from pathlib import Path

# Add SourceCode to sys.path
SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from lakehouse.dimensions import build_all_dimensions, build_dim_customer, build_dim_customer_scd2
from lakehouse.marts import (
    build_abc_mart,
    build_fact_sales,
    build_mart_order_summary,
    build_rfm_mart,
)
from lakehouse.silver import clean_and_enrich_silver
from pyspark.sql.functions import col


def test_marts_when_import_and_rfm_abc(spark_session):
    """Kiểm tra build_rfm_mart và build_abc_mart hoạt động bình thường không gặp NameError 'when'."""
    data = [
        ("ORD01", "2026-08-01", "CUST01", "Laptop Pro", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD02", "2026-08-10", "CUST01", "Mouse", "Electronics", "Tech", 2, 25.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD03", "2026-08-20", "CUST02", "Keyboard", "Electronics", "Tech", 1, 50.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Customer_ID", "Product_Name", "Category", "Sub_Category",
        "Quantity", "Unit_Price", "Discount", "Revenue", "Cost", "Profit", "Shipping_Cost",
        "Shipping_Days", "Order_Status", "Payment_Method", "Shipping_Method", "Region", "Country"
    ]
    df = spark_session.createDataFrame(data, cols)
    df = clean_and_enrich_silver(df)

    rfm = build_rfm_mart(df, analysis_date="2026-08-25")
    assert rfm.count() == 2
    assert "RFM_Segment" in rfm.columns

    abc = build_abc_mart(df)
    assert abc.count() == 3
    assert "ABC_Class" in abc.columns


def test_dim_customer_deduplication_prevents_fact_duplication(spark_session):
    """Đảm bảo dim_customer deduplicate theo Customer_ID, tránh nhân đôi số lượng Fact Sales."""
    data = [
        ("ORD01", "2026-08-01", "C001", "Male", "Consumer", "Laptop Pro", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD02", "2026-08-05", "C001", "Male", "Corporate", "Mouse", "Electronics", "Tech", 1, 50.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Customer_ID", "Customer_Gender", "Customer_Segment",
        "Product_Name", "Category", "Sub_Category", "Quantity", "Unit_Price", "Discount",
        "Revenue", "Cost", "Profit", "Shipping_Cost", "Shipping_Days", "Order_Status",
        "Payment_Method", "Shipping_Method", "Region", "Country"
    ]
    df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(df)

    dim_cust = build_dim_customer(clean_df)
    # dim_customer must have exactly 1 row for Customer C001
    assert dim_cust.filter(col("Customer_ID") == "C001").count() == 1

    dims = build_all_dimensions(spark_session, clean_df)
    fact = build_fact_sales(clean_df, dims)

    # Fact sales count must equal original clean records (2 rows, not 4)
    assert fact.count() == 2


def test_dim_customer_scd2(spark_session):
    """Kiểm tra SCD Type 2 xử lý chính xác khách hàng chuyển đổi trạng thái lặp lại (A -> B -> A)."""
    data = [
        ("ORD01", "2026-01-01", "C001", "Male", "Consumer"),
        ("ORD02", "2026-06-01", "C001", "Male", "Corporate"),
        ("ORD03", "2026-10-01", "C001", "Male", "Consumer"),  # Quay lại Consumer
    ]
    cols = ["Order_ID", "Order_Date", "Customer_ID", "Customer_Gender", "Customer_Segment"]
    df = spark_session.createDataFrame(data, cols)

    scd2_df = build_dim_customer_scd2(df)
    assert "ValidFrom" in scd2_df.columns
    assert "ValidTo" in scd2_df.columns
    assert "Is_Current" in scd2_df.columns

    # Phải tạo 3 phiên bản riêng biệt (không bị groupBy làm gộp 2 lần Consumer)
    assert scd2_df.count() == 3

    records = scd2_df.orderBy("ValidFrom").collect()
    # Version 1: Consumer (2026-01-01 -> 2026-06-01, not current)
    assert records[0]["Customer_Segment"] == "Consumer"
    assert str(records[0]["ValidFrom"]) == "2026-01-01"
    assert str(records[0]["ValidTo"]) == "2026-06-01"
    assert records[0]["Is_Current"] == 0

    # Version 2: Corporate (2026-06-01 -> 2026-10-01, not current)
    assert records[1]["Customer_Segment"] == "Corporate"
    assert str(records[1]["ValidFrom"]) == "2026-06-01"
    assert str(records[1]["ValidTo"]) == "2026-10-01"
    assert records[1]["Is_Current"] == 0

    # Version 3: Consumer (2026-10-01 -> 9999-12-31, current)
    assert records[2]["Customer_Segment"] == "Consumer"
    assert str(records[2]["ValidFrom"]) == "2026-10-01"
    assert str(records[2]["ValidTo"]) == "9999-12-31"
    assert records[2]["Is_Current"] == 1


def test_certified_line_amount_and_quarantine(spark_session, tmp_path):
    """Kiểm tra line amount certified và không nhân Shipping_Cost theo số line."""
    data = [
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Laptop", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered"),
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Mouse", "Electronics", "Tech", 2, 25.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered"),
        ("ORD_BAD", "2026-08-01", 2026, 8, "C002", "Mouse", "Electronics", "Tech", -1, 25.0, 2.5, 50.0, 30.0, 20.0, 5.0, 1, "Delivered"), # invalid qty AND invalid discount
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Product_Name", "Category", "Sub_Category",
        "Quantity", "Unit_Price", "Discount", "Revenue", "Cost", "Profit", "Shipping_Cost",
        "Shipping_Days", "Order_Status"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    quarantine_dir = str(tmp_path / "quarantine_test")

    clean_df = clean_and_enrich_silver(raw_df, quarantine_path=quarantine_dir)

    # Valid rows must be 2
    assert clean_df.count() == 2

    amounts = clean_df.filter(col("Order_ID") == "ORD01").select("Net_Line_Amount").collect()
    assert sorted(row[0] for row in amounts) == [50.0, 1000.0]

    order_summary = build_mart_order_summary(clean_df)
    order = order_summary.filter(col("Order_ID") == "ORD01").collect()[0]
    assert order["Order_Total_Revenue"] == 1050.0
    assert order["Order_Shipping_Cost"] == 20.0

    # Check Order_Line_ID existence and uniqueness
    assert "Order_Line_ID" in clean_df.columns
    line_ids = [r["Order_Line_ID"] for r in clean_df.select("Order_Line_ID").collect()]
    assert len(line_ids) == len(set(line_ids))


def test_quarantine_multi_reason_array(spark_session, tmp_path):
    """Kiểm tra bảng Quarantine ghi nhận danh sách mảng đa lỗi (rejection_reasons)."""
    data = [
        # Vi phạm cả Quantity <= 0 VÀ Discount > 1
        ("ORD_MULTI_ERR", "2026-08-01", 2026, 8, "C001", "Laptop", "Electronics", "Tech", -5, 100.0, 2.5, 100.0, 70.0, 30.0, 10.0, 2, "Delivered"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Product_Name", "Category", "Sub_Category",
        "Quantity", "Unit_Price", "Discount", "Revenue", "Cost", "Profit", "Shipping_Cost",
        "Shipping_Days", "Order_Status"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    quarantine_dir = str(tmp_path / "quarantine_multi_err")

    clean_df = clean_and_enrich_silver(raw_df, quarantine_path=quarantine_dir)
    assert clean_df.count() == 0

    quarantine_df = spark_session.read.format("delta").load(quarantine_dir)
    record = quarantine_df.collect()[0]
    reasons = record["rejection_reasons"]

    assert "INVALID_QUANTITY" in reasons
    assert "INVALID_DISCOUNT" in reasons
    assert len(reasons) >= 2


def test_scd2_only_one_current_record_per_customer(spark_session):
    """Đảm bảo với bất kỳ lịch sử chuyển đổi SCD2 nào, mỗi khách hàng chỉ có duy nhất 1 bản ghi Is_Current = 1."""
    data = [
        ("ORD01", "2026-01-01", "C001", "Male", "Consumer"),
        ("ORD02", "2026-03-01", "C001", "Male", "Corporate"),
        ("ORD03", "2026-06-01", "C001", "Male", "Consumer"),
        ("ORD04", "2026-01-15", "C002", "Female", "Home Office"),
        ("ORD05", "2026-04-15", "C002", "Female", "Consumer"),
    ]
    cols = ["Order_ID", "Order_Date", "Customer_ID", "Customer_Gender", "Customer_Segment"]
    df = spark_session.createDataFrame(data, cols)

    scd2_df = build_dim_customer_scd2(df)
    c001_current = scd2_df.filter((col("Customer_ID") == "C001") & (col("Is_Current") == 1)).count()
    c002_current = scd2_df.filter((col("Customer_ID") == "C002") & (col("Is_Current") == 1)).count()

    assert c001_current == 1
    assert c002_current == 1
