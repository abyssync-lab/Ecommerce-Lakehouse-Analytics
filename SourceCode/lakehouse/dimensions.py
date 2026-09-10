"""Mô-đun xây dựng các bảng Dimension theo mô hình Kimball Star Schema."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import (
    col,
    concat_ws,
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
    sha2,
    to_date,
    to_timestamp,
    trim,
    when,
    xxhash64,
    year,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

LOGGER = logging.getLogger(__name__)


def add_surrogate_key(dataframe: Any, key_name: str, order_cols: list[str]) -> Any:
    """Sinh surrogate key tất định từ natural key, không phụ thuộc row order của batch.

    Chiến lược khóa (Surrogate Key Strategy):
    `row_number()` thay đổi khi một dimension member mới xuất hiện và có thể làm fact
    cũ trỏ sang key khác. Hash key giữ nguyên khi rebuild/incremental; bảng mapping
    persistent riêng có thể thay thế sau này nếu cần chống collision tuyệt đối.
    """
    key_expression = xxhash64(*[col(column).cast("string") for column in order_cols])
    return dataframe.withColumn(key_name, key_expression.cast("long"))


def build_dim_product(clean_df: Any) -> Any:
    """Xây dựng Product dimension với Product_ID là natural key ổn định."""
    source = clean_df
    if "Product_ID" not in source.columns:
        source = source.withColumn("Product_ID", sha2(trim(col("Product_Name")), 256))
    dim = source.select("Product_ID", "Product_Name", "Category", "Sub_Category").dropDuplicates(
        ["Product_ID"]
    )
    dim = add_surrogate_key(dim, "ProductKey", ["Product_ID"])
    return dim.select("ProductKey", "Product_ID", "Product_Name", "Category", "Sub_Category")


def build_dim_customer(clean_df: Any, use_scd2: bool = False) -> Any:
    """Xây dựng Dimension Customer (mặc định 1 Customer_ID = đúng 1 record để tránh duplicate fact, hoặc SCD2)."""
    if use_scd2:
        return build_dim_customer_scd2(clean_df)

    if "Order_Date" in clean_df.columns:
        order_expressions = [col("Order_Date").desc()]
        if "Source_Updated_At" in clean_df.columns:
            order_expressions.insert(0, col("Source_Updated_At").desc())
        if "Order_ID" in clean_df.columns:
            order_expressions.append(col("Order_ID").desc())
        w = Window.partitionBy("Customer_ID").orderBy(*order_expressions)
        source_columns = [
            "Customer_ID",
            "Customer_Gender",
            "Customer_Segment",
            "Order_Date",
        ]
        for technical_column in ("Source_Updated_At", "Order_ID"):
            if technical_column in clean_df.columns:
                source_columns.append(technical_column)
        dim = (
            clean_df.select(*source_columns)
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn", "Order_Date", "Source_Updated_At", "Order_ID")
        )
    else:
        dim = clean_df.select("Customer_ID", "Customer_Gender", "Customer_Segment").dropDuplicates(
            subset=["Customer_ID"]
        )

    dim = add_surrogate_key(dim, "CustomerKey", ["Customer_ID"])
    return dim.select("CustomerKey", "Customer_ID", "Customer_Gender", "Customer_Segment")


def build_dim_customer_scd2(clean_df: Any) -> Any:
    """Xây dựng Dimension Customer theo chuẩn SCD Type 2 với Change-Detection Logic.

    Xử lý chính xác trường hợp khách hàng chuyển đổi trạng thái lặp lại (ví dụ A -> B -> A).
    Không dùng groupBy đơn thuần (sẽ làm mất các phiên bản lặp lại), mà phát hiện sự kiện thay đổi
    theo thứ tự thời gian bằng lag(), đánh dấu nhóm trạng thái (change_group), sau đó tính [ValidFrom, ValidTo).
    """
    # Với event v2, lịch sử customer phải dùng Source_Updated_At thay vì chỉ dùng
    # Order_Date. Nhờ vậy hai thay đổi trong cùng một ngày vẫn tạo đúng phiên bản.
    uses_event_timestamp = "Source_Updated_At" in clean_df.columns
    event_at = (
        to_timestamp(col("Source_Updated_At"))
        if uses_event_timestamp
        else to_date(col("Order_Date"))
    )
    end_of_time = (
        to_timestamp(lit("9999-12-31 23:59:59"))
        if uses_event_timestamp
        else to_date(lit("9999-12-31"))
    )

    event_columns = [
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        event_at.alias("_event_at"),
    ]
    if "Order_ID" in clean_df.columns:
        event_columns.append(col("Order_ID").alias("_order_id"))
    if "_record_hash" in clean_df.columns:
        event_columns.append(col("_record_hash").alias("_record_hash"))

    cust_events = clean_df.select(*event_columns).dropDuplicates()
    cust_events = cust_events.withColumn(
        "Attribute_Hash",
        sha2(
            concat_ws(
                "\u001f",
                col("Customer_Gender").cast("string"),
                col("Customer_Segment").cast("string"),
            ),
            256,
        ),
    )
    order_cols = ["_event_at"]
    if "_order_id" in cust_events.columns:
        order_cols.append("_order_id")
    if "_record_hash" in cust_events.columns:
        order_cols.append("_record_hash")
    w_order = Window.partitionBy("Customer_ID").orderBy(*order_cols)

    # So sánh an toàn với NULL rất quan trọng: hai phiên bản có cùng thuộc tính NULL không
    # được bị coi là hai thay đổi khác nhau.
    prev_gender = lag("Customer_Gender", 1).over(w_order)
    prev_segment = lag("Customer_Segment", 1).over(w_order)
    same_gender = col("Customer_Gender").eqNullSafe(prev_gender)
    same_segment = col("Customer_Segment").eqNullSafe(prev_segment)
    is_first_event = row_number().over(w_order) == 1
    is_change = when(is_first_event, 1).when(same_gender & same_segment, 0).otherwise(1)

    events_with_change = cust_events.withColumn("is_change", is_change)

    # Cộng dồn cờ thay đổi để tạo các phiên liên tục A -> B -> A riêng biệt.
    w_cum = (
        Window.partitionBy("Customer_ID")
        .orderBy(*order_cols)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    events_grouped = events_with_change.withColumn(
        "change_group", spark_sum("is_change").over(w_cum)
    )

    state_versions = events_grouped.groupBy(
        "Customer_ID",
        "change_group",
        "Customer_Gender",
        "Customer_Segment",
        "Attribute_Hash",
    ).agg(spark_min("_event_at").alias("ValidFrom"))

    # ValidTo là khoảng nửa mở [ValidFrom, ValidTo), dùng event kế tiếp làm mốc đóng.
    w_scd = Window.partitionBy("Customer_ID").orderBy("ValidFrom")
    scd_df = (
        state_versions.withColumn("NextValidFrom", lead("ValidFrom", 1).over(w_scd))
        .withColumn(
            "ValidTo",
            when(col("NextValidFrom").isNotNull(), col("NextValidFrom")).otherwise(end_of_time),
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
        "Attribute_Hash",
        "ValidFrom",
        "ValidTo",
        col("ValidFrom").alias("Valid_From"),
        col("ValidTo").alias("Valid_To"),
        "Is_Current",
    )


def build_dim_geography(clean_df: Any) -> Any:
    """Xây dựng geography dimension ở grain Region-Country."""
    dim = clean_df.select("Region", "Country").dropDuplicates()
    dim = add_surrogate_key(dim, "GeographyKey", ["Region", "Country"])
    dim = dim.select("GeographyKey", "Region", "Country")
    unknown = clean_df.sparkSession.createDataFrame([(0, "Unknown", "Unknown")], dim.schema)
    actual = dim.filter(~((col("Region") == "Unknown") & (col("Country") == "Unknown")))
    return unknown.unionByName(actual)


def build_dim_order_context(clean_df: Any) -> Any:
    """Gộp các thuộc tính cardinality thấp của order thành một context dimension."""
    context_columns = [
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "Delivery_Level",
    ]
    dim = clean_df.select(*context_columns).dropDuplicates()
    dim = add_surrogate_key(dim, "ContextKey", context_columns)
    dim = dim.select("ContextKey", *context_columns)
    unknown = clean_df.sparkSession.createDataFrame(
        [(0, "Unknown", "Unknown", "Unknown", "Unknown")], dim.schema
    )
    actual = dim.filter(
        ~(
            (col("Order_Status") == "Unknown")
            & (col("Payment_Method") == "Unknown")
            & (col("Shipping_Method") == "Unknown")
            & (col("Delivery_Level") == "Unknown")
        )
    )
    return unknown.unionByName(actual)


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
    ).select(
        explode(sequence(col("min_date"), col("max_date"), expr("interval 1 day"))).alias(
            "FullDate"
        )
    )

    dim = (
        date_seq_df.withColumn("DateKey", date_format(col("FullDate"), "yyyyMMdd").cast("int"))
        .withColumn("Year", year(col("FullDate")))
        .withColumn("Month", month(col("FullDate")))
        .withColumn("Quarter", quarter(col("FullDate")))
    )

    return dim.select("DateKey", "FullDate", "Year", "Month", "Quarter")


def build_all_dimensions(
    spark: Any,
    clean_df: Any,
    use_scd2: bool = False,
    customer_history_df: Any | None = None,
) -> dict[str, Any]:
    """Tạo các dimension; SCD2 có thể nhận toàn bộ customer event history từ Bronze."""
    LOGGER.info("Bắt đầu xây dựng 5 bảng Dimension Kimball...")
    customer_source = customer_history_df if customer_history_df is not None else clean_df
    dims = {
        "dim_product": build_dim_product(clean_df),
        "dim_customer": build_dim_customer(customer_source, use_scd2=use_scd2),
        "dim_geography": build_dim_geography(clean_df),
        "dim_order_context": build_dim_order_context(clean_df),
        "dim_date": build_dim_date(spark, clean_df),
    }
    LOGGER.info("Đã tạo hoàn tất 5 bảng Dimension.")
    return dims
