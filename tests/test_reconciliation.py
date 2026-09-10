"""Bộ kiểm thử đối soát tính toàn vẹn và bất biến dữ liệu (Data Reconciliation Tests).

Bảo đảm các định luật bảo toàn dữ liệu xuyên suốt các tầng Medallion Lakehouse:
1. Tổng doanh thu (Revenue Invariant): SUM(Silver.Revenue) == SUM(FactSales.Revenue) == Mart Overview Total_Revenue
2. Tính duy nhất của Grain: Mỗi khóa FactSales đại diện đúng 1 line item trong 1 order
3. Bảo toàn số dòng (Row Conservation): Raw = Clean + Quarantine + Deduplicated
"""

from __future__ import annotations

import sys
from pathlib import Path

SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from lakehouse.dimensions import build_all_dimensions
from lakehouse.marts import build_all_marts, build_fact_sales
from lakehouse.reconciliation import run_full_reconciliation
from lakehouse.silver import clean_and_enrich_silver
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import sum as spark_sum


def test_gold_revenue_reconciles_with_silver_and_marts(spark_session):
    """Kiểm tra bất biến doanh thu: Doanh thu tầng Silver == FactSales == Mart Overview."""
    data = [
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Male", "Consumer", "Laptop Pro", "Electronics", "Tech", 1, 1200.0, 0.0, 1200.0, 800.0, 400.0, 25.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Male", "Consumer", "Mouse Pro", "Electronics", "Tech", 2, 40.0, 0.1, 72.0, 40.0, 32.0, 5.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD02", "2026-08-05", 2026, 8, "C002", "Female", "Corporate", "Keyboard Pro", "Electronics", "Tech", 1, 150.0, 0.0, 150.0, 90.0, 60.0, 10.0, 3, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Customer_Gender", "Customer_Segment",
        "Product_Name", "Category", "Sub_Category", "Quantity", "Unit_Price", "Discount",
        "Revenue", "Cost", "Profit", "Shipping_Cost", "Shipping_Days", "Order_Status",
        "Payment_Method", "Shipping_Method", "Region", "Country"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(raw_df)

    dims = build_all_dimensions(spark_session, clean_df)
    fact = build_fact_sales(clean_df, dims)
    marts = build_all_marts(clean_df)

    # 1. Doanh thu tầng Silver
    silver_revenue = round(clean_df.select(spark_sum("Net_Line_Amount")).collect()[0][0], 2)

    # 2. Doanh thu tầng FactSales
    fact_revenue = round(fact.select(spark_sum("Net_Line_Amount")).collect()[0][0], 2)

    # 3. Doanh thu Mart Overview
    overview_revenue = round(marts["mart_overview"].select("Total_Revenue").collect()[0][0], 2)

    assert silver_revenue == 1422.0
    assert fact_revenue == silver_revenue, f"Fact Revenue ({fact_revenue}) lệch với Silver ({silver_revenue})"
    assert overview_revenue == fact_revenue, f"Overview Mart Revenue ({overview_revenue}) lệch với Fact ({fact_revenue})"


def test_fact_grain_uniqueness(spark_session):
    """Kiểm tra tính duy nhất của Grain trong FactSales (1 dòng = 1 sản phẩm trong 1 đơn hàng)."""
    data = [
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Male", "Consumer", "Laptop Pro", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Male", "Consumer", "Mouse", "Electronics", "Tech", 1, 50.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD02", "2026-08-02", 2026, 8, "C002", "Female", "Corporate", "Keyboard", "Electronics", "Tech", 1, 70.0, 0.0, 70.0, 40.0, 30.0, 5.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Customer_Gender", "Customer_Segment",
        "Product_Name", "Category", "Sub_Category", "Quantity", "Unit_Price", "Discount",
        "Revenue", "Cost", "Profit", "Shipping_Cost", "Shipping_Days", "Order_Status",
        "Payment_Method", "Shipping_Method", "Region", "Country"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(raw_df)
    dims = build_all_dimensions(spark_session, clean_df)
    fact = build_fact_sales(clean_df, dims)

    total_fact_rows = fact.count()
    distinct_sales_keys = fact.select(countDistinct("SalesKey")).collect()[0][0]

    assert total_fact_rows == 3
    assert distinct_sales_keys == total_fact_rows, "SalesKey trong FactSales không duy nhất!"


def test_row_count_conservation(spark_session, tmp_path):
    """Kiểm tra định luật bảo toàn số dòng: Raw = Clean + Quarantine + Deduplicated."""
    data = [
        # 2 dòng hợp lệ khác nhau
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Laptop", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered"),
        ("ORD02", "2026-08-02", 2026, 8, "C002", "Phone", "Electronics", "Tech", 1, 800.0, 0.0, 800.0, 500.0, 300.0, 15.0, 1, "Delivered"),
        # 1 dòng lỗi (Quantity âm)
        ("ORD03_ERR", "2026-08-03", 2026, 8, "C003", "Mouse", "Electronics", "Tech", -5, 20.0, 0.0, 100.0, 60.0, 40.0, 5.0, 1, "Delivered"),
        # 1 dòng trùng lặp y hệt ORD01
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Laptop", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Product_Name", "Category", "Sub_Category",
        "Quantity", "Unit_Price", "Discount", "Revenue", "Cost", "Profit", "Shipping_Cost",
        "Shipping_Days", "Order_Status"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    raw_count = raw_df.count()

    quarantine_dir = str(tmp_path / "recon_quarantine")
    clean_df = clean_and_enrich_silver(raw_df, quarantine_path=quarantine_dir)
    clean_count = clean_df.count()

    quarantine_df = spark_session.read.format("delta").load(quarantine_dir)
    quarantine_count = quarantine_df.count()

    dedup_raw_count = raw_df.dropDuplicates().count()
    duplicate_count = raw_count - dedup_raw_count

    # Định luật bảo toàn:
    assert raw_count == clean_count + quarantine_count + duplicate_count
    assert clean_count == 2
    assert quarantine_count == 1
    assert duplicate_count == 1


def test_gold_reconciliation_full_report(spark_session, tmp_path):
    """Kiểm tra hàm run_full_reconciliation xuất kết quả PASS và sinh báo cáo JSON hợp lệ."""
    data = [
        ("ORD01", "2026-08-01", 2026, 8, "C001", "Male", "Consumer", "Laptop Pro", "Electronics", "Tech", 1, 1000.0, 0.0, 1000.0, 700.0, 300.0, 20.0, 2, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
        ("ORD02", "2026-08-02", 2026, 8, "C002", "Female", "Corporate", "Mouse", "Electronics", "Tech", 1, 50.0, 0.0, 50.0, 30.0, 20.0, 5.0, 1, "Delivered", "Card", "Standard", "Asia", "Vietnam"),
    ]
    cols = [
        "Order_ID", "Order_Date", "Year", "Month", "Customer_ID", "Customer_Gender", "Customer_Segment",
        "Product_Name", "Category", "Sub_Category", "Quantity", "Unit_Price", "Discount",
        "Revenue", "Cost", "Profit", "Shipping_Cost", "Shipping_Days", "Order_Status",
        "Payment_Method", "Shipping_Method", "Region", "Country"
    ]
    raw_df = spark_session.createDataFrame(data, cols)
    clean_df = clean_and_enrich_silver(raw_df)

    dims = build_all_dimensions(spark_session, clean_df, use_scd2=True)
    fact = build_fact_sales(clean_df, dims)
    marts = build_all_marts(clean_df)

    json_report_path = tmp_path / "recon_report.json"
    report = run_full_reconciliation(
        clean_df=clean_df,
        fact_sales=fact,
        mart_overview=marts["mart_overview"],
        dim_customer=dims["dim_customer"],
        raw_count=2,
        duplicate_count=0,
        invalid_count=0,
        export_path=json_report_path,
    )

    assert report["overall_status"] == "PASS"
    assert json_report_path.exists()
    assert report["checks"]["revenue_invariant"]["passed"] is True
    assert report["checks"]["foreign_key_completeness"]["passed"] is True
