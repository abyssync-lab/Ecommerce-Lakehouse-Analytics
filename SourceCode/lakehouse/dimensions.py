"""Module xây dựng các bảng Dimension theo mô hình Kimball Star Schema."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import col, date_format, explode, expr, month, quarter, row_number, sequence, year
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window

LOGGER = logging.getLogger(__name__)


def add_surrogate_key(dataframe: Any, key_name: str, order_cols: list[str]) -> Any:
    """Sinh khóa đại diện (Surrogate Key) ổn định (1, 2, 3...) bằng `row_number()`.

    Dùng row_number() sắp xếp theo khóa nghiệp vụ tự nhiên thay vì monotonically_increasing_id()
    để đảm bảo kết quả ổn định và tái lập giữa các lần chạy lại pipeline.
    """
    window_spec = Window.orderBy(*order_cols)
    return dataframe.withColumn(key_name, row_number().over(window_spec))


def build_dim_product(clean_df: Any) -> Any:
    """Xây dựng Dimension Product."""
    dim = clean_df.select("Product_Name", "Category", "Sub_Category").dropDuplicates()
    dim = add_surrogate_key(dim, "ProductKey", ["Product_Name", "Category", "Sub_Category"])
    return dim.select("ProductKey", "Category", "Sub_Category", "Product_Name")


def build_dim_customer(clean_df: Any) -> Any:
    """Xây dựng Dimension Customer."""
    dim = clean_df.select("Customer_ID", "Customer_Gender", "Customer_Segment").dropDuplicates()
    dim = add_surrogate_key(dim, "CustomerKey", ["Customer_ID"])
    return dim.select("CustomerKey", "Customer_ID", "Customer_Gender", "Customer_Segment")


def build_dim_location(clean_df: Any) -> Any:
    """Xây dựng Dimension Location."""
    dim = clean_df.select("Region", "Country").dropDuplicates()
    dim = add_surrogate_key(dim, "LocationKey", ["Region", "Country"])
    return dim.select("LocationKey", "Region", "Country")


def build_dim_payment(clean_df: Any) -> Any:
    """Xây dựng Dimension Payment."""
    dim = clean_df.select("Payment_Method").dropDuplicates()
    dim = add_surrogate_key(dim, "PaymentKey", ["Payment_Method"])
    return dim.select("PaymentKey", "Payment_Method")


def build_dim_shipping(clean_df: Any) -> Any:
    """Xây dựng Dimension Shipping."""
    dim = clean_df.select("Shipping_Method", "Delivery_Level").dropDuplicates()
    dim = add_surrogate_key(dim, "ShippingKey", ["Shipping_Method", "Delivery_Level"])
    return dim.select("ShippingKey", "Shipping_Method", "Delivery_Level")


def build_dim_order_status(clean_df: Any) -> Any:
    """Xây dựng Dimension Order Status."""
    dim = clean_df.select("Order_Status", "Is_Returned", "Is_Cancelled").dropDuplicates()
    dim = add_surrogate_key(dim, "StatusKey", ["Order_Status"])
    return dim.select("StatusKey", "Order_Status", "Is_Returned", "Is_Cancelled")


def build_dim_date(spark: Any, clean_df: Any) -> Any:
    """Sinh DimDate đầy đủ từ ngày nhỏ nhất đến lớn nhất theo chuẩn Kimball.

    DateKey định dạng yyyyMMdd (ví dụ: 20260801).
    """
    date_range = clean_df.select(
        spark_min("Order_Date").alias("min_date"),
        spark_max("Order_Date").alias("max_date"),
    ).collect()[0]

    date_seq_df = spark.createDataFrame(
        [(date_range["min_date"], date_range["max_date"])], ["min_date", "max_date"]
    ).select(explode(sequence(col("min_date"), col("max_date"), expr("interval 1 day"))).alias("FullDate"))

    dim = (
        date_seq_df.withColumn("DateKey", date_format(col("FullDate"), "yyyyMMdd").cast("int"))
        .withColumn("Year", year(col("FullDate")))
        .withColumn("Month", month(col("FullDate")))
        .withColumn("Quarter", quarter(col("FullDate")))
    )

    return dim.select("DateKey", "FullDate", "Year", "Month", "Quarter")


def build_all_dimensions(spark: Any, clean_df: Any) -> dict[str, Any]:
    """Tạo toàn bộ 7 bảng Dimension cho Star Schema."""
    LOGGER.info("Bắt đầu xây dựng 7 bảng Dimension Kimball Star Schema...")
    dims = {
        "dim_product": build_dim_product(clean_df),
        "dim_customer": build_dim_customer(clean_df),
        "dim_location": build_dim_location(clean_df),
        "dim_payment": build_dim_payment(clean_df),
        "dim_shipping": build_dim_shipping(clean_df),
        "dim_order_status": build_dim_order_status(clean_df),
        "dim_date": build_dim_date(spark, clean_df),
    }
    LOGGER.info("Đã tạo hoàn tất 7 bảng Dimension.")
    return dims
