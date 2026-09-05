"""Module xây dựng các bảng Dimension theo mô hình Kimball Star Schema."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import (
    col,
    date_format,
    explode,
    expr,
    lag,
    lead,
    lit,
    month,
    quarter,
    row_number,
    sequence,
    to_date,
    when,
    year,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

LOGGER = logging.getLogger(__name__)


def add_surrogate_key(dataframe: Any, key_name: str, order_cols: list[str]) -> Any:
    """Sinh khóa đại diện (Surrogate Key) ổn định (1, 2, 3...) bằng `row_number()`.

    Chiến lược khóa (Surrogate Key Strategy):
    - **Portfolio Deterministic Rebuild Key (Áp dụng tại đây):** Dùng `row_number().over(orderBy(*order_cols))`
      sắp xếp theo khóa tự nhiên nghiệp vụ thay vì `monotonically_increasing_id()` để bảo đảm tính tái lập
      (reproducibility) 100% giữa các lần re-run pipeline từ cùng tập dữ liệu.
    - **Enterprise Production Persistent Key (Khuyến nghị Enterprise DW):** Tra cứu bảng Dimension Delta hiện hữu
      (Stateful Lookup), tái sử dụng surrogate key đã cấp cho natural key cũ, và chỉ cấp mới `max(key) + sequence`
      hoặc Hashed Surrogate Key cho các thành viên mới xuất hiện để không bao giờ làm đổi key lịch sử.
    """
    window_spec = Window.orderBy(*order_cols)
    return dataframe.withColumn(key_name, row_number().over(window_spec))



def build_dim_product(clean_df: Any) -> Any:
    """Xây dựng Dimension Product."""
    dim = clean_df.select("Product_Name", "Category", "Sub_Category").dropDuplicates()
    dim = add_surrogate_key(dim, "ProductKey", ["Product_Name", "Category", "Sub_Category"])
    return dim.select("ProductKey", "Category", "Sub_Category", "Product_Name")


def build_dim_customer(clean_df: Any, use_scd2: bool = False) -> Any:
    """Xây dựng Dimension Customer (mặc định 1 Customer_ID = đúng 1 record để tránh duplicate fact, hoặc SCD2)."""
    if use_scd2:
        return build_dim_customer_scd2(clean_df)

    if "Order_Date" in clean_df.columns:
        w = Window.partitionBy("Customer_ID").orderBy(col("Order_Date").desc())
        dim = (
            clean_df.select("Customer_ID", "Customer_Gender", "Customer_Segment", "Order_Date")
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn", "Order_Date")
        )
    else:
        dim = clean_df.select("Customer_ID", "Customer_Gender", "Customer_Segment").dropDuplicates(subset=["Customer_ID"])

    dim = add_surrogate_key(dim, "CustomerKey", ["Customer_ID"])
    return dim.select("CustomerKey", "Customer_ID", "Customer_Gender", "Customer_Segment")


def build_dim_customer_scd2(clean_df: Any) -> Any:
    """Xây dựng Dimension Customer theo chuẩn SCD Type 2 với Change-Detection Logic.

    Xử lý chính xác trường hợp khách hàng chuyển đổi trạng thái lặp lại (ví dụ A -> B -> A).
    Không dùng groupBy đơn thuần (sẽ làm mất các phiên bản lặp lại), mà phát hiện sự kiện thay đổi
    theo thứ tự thời gian bằng lag(), đánh dấu nhóm trạng thái (change_group), sau đó tính [ValidFrom, ValidTo).
    """
    # 1. Trích xuất sự kiện giao dịch của khách hàng
    order_cols = ["Order_Date"]
    if "Order_ID" in clean_df.columns:
        order_cols.append("Order_ID")

    cust_events = clean_df.select(
        "Customer_ID", "Customer_Gender", "Customer_Segment", *order_cols
    ).dropDuplicates()

    w_order = Window.partitionBy("Customer_ID").orderBy(*order_cols)

    # 2. Phát hiện thay đổi trạng thái thuộc tính (Change Detection)
    prev_gender = lag("Customer_Gender", 1).over(w_order)
    prev_segment = lag("Customer_Segment", 1).over(w_order)

    is_change = (
        when(prev_gender.isNull() | prev_segment.isNull(), 1)
        .when((col("Customer_Gender") != prev_gender) | (col("Customer_Segment") != prev_segment), 1)
        .otherwise(0)
    )

    events_with_change = cust_events.withColumn("is_change", is_change)

    # 3. Gom cụm các khoảng trạng thái liên tục (Island Grouping / Change Group)
    w_cum = (
        Window.partitionBy("Customer_ID")
        .orderBy(*order_cols)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    events_grouped = events_with_change.withColumn("change_group", spark_sum("is_change").over(w_cum))

    # 4. Xác định ValidFrom cho từng phiên bản
    state_versions = events_grouped.groupBy(
        "Customer_ID", "change_group", "Customer_Gender", "Customer_Segment"
    ).agg(spark_min("Order_Date").alias("ValidFrom"))

    # 5. Xác định ValidTo và Is_Current theo khoảng nửa mở [ValidFrom, ValidTo)
    w_scd = Window.partitionBy("Customer_ID").orderBy("ValidFrom")
    scd_df = (
        state_versions.withColumn("NextValidFrom", lead("ValidFrom", 1).over(w_scd))
        .withColumn(
            "ValidTo",
            when(col("NextValidFrom").isNotNull(), col("NextValidFrom")).otherwise(to_date(lit("9999-12-31"))),
        )
        .withColumn("Is_Current", when(col("NextValidFrom").isNull(), 1).otherwise(0))
        .drop("NextValidFrom", "change_group")
    )

    scd_df = add_surrogate_key(scd_df, "CustomerKey", ["Customer_ID", "ValidFrom"])
    return scd_df.select(
        "CustomerKey",
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        "ValidFrom",
        "ValidTo",
        "Is_Current",
    )


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


def build_all_dimensions(spark: Any, clean_df: Any, use_scd2: bool = False) -> dict[str, Any]:
    """Tạo toàn bộ 7 bảng Dimension cho Star Schema."""
    LOGGER.info("Bắt đầu xây dựng 7 bảng Dimension Kimball Star Schema...")
    dims = {
        "dim_product": build_dim_product(clean_df),
        "dim_customer": build_dim_customer(clean_df, use_scd2=use_scd2),
        "dim_location": build_dim_location(clean_df),
        "dim_payment": build_dim_payment(clean_df),
        "dim_shipping": build_dim_shipping(clean_df),
        "dim_order_status": build_dim_order_status(clean_df),
        "dim_date": build_dim_date(spark, clean_df),
    }
    LOGGER.info("Đã tạo hoàn tất 7 bảng Dimension.")
    return dims
