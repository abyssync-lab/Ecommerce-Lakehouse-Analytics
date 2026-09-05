"""Unit test cho các module trong tầng PySpark Medallion Lakehouse (marts, dimensions, silver)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add SourceCode to sys.path
SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

try:
    from lakehouse.dimensions import (
        build_all_dimensions,
        build_dim_customer,
        build_dim_customer_scd2,
    )
    from lakehouse.marts import build_abc_mart, build_fact_sales, build_rfm_mart
    from lakehouse.silver import clean_and_enrich_silver
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@pytest.fixture(scope="module")
def spark_session():
    """Fixture khởi tạo SparkSession local cho unit testing."""
    if not HAS_PYSPARK:
        pytest.skip("PySpark chưa được cài đặt.")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("LakehouseUnitTest")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


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


def test_revenue_per_order_window_and_quarantine(spark_session, tmp_path):
    """Kiểm tra Order_Total_Revenue được cộng dồn theo Order_ID và phân lập Quarantine lưu đa lỗi."""
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

    # Order_Total_Revenue for ORD01 must be 1000.0 + 50.0 = 1050.0
    ord01_rev = clean_df.filter(col("Order_ID") == "ORD01").select("Order_Total_Revenue").collect()[0][0]
    assert ord01_rev == 1050.0

    # Backward compatibility alias
    ord01_rev_alias = clean_df.filter(col("Order_ID") == "ORD01").select("Revenue_Per_Order").collect()[0][0]
    assert ord01_rev_alias == 1050.0
