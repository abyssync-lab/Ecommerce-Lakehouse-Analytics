"""Module làm sạch và kiểm định dữ liệu (Silver Layer) trong Medallion Lakehouse."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import col, to_date, when

LOGGER = logging.getLogger(__name__)


def validate_silver_data(clean_df: Any, raw_count: int) -> None:
    """Kiểm tra các quy tắc nghiệp vụ cốt lõi Data Contract sau bước làm sạch."""
    rules = {
        "Order_ID không rỗng": col("Order_ID").isNotNull(),
        "Quantity > 0": col("Quantity") > 0,
        "Unit_Price >= 0": col("Unit_Price") >= 0,
        "Discount trong [0, 1]": col("Discount").between(0, 1),
        "Revenue >= 0": col("Revenue") >= 0,
        "Shipping_Days >= 0": col("Shipping_Days") >= 0,
    }
    failed = []
    for rule_name, condition in rules.items():
        invalid_count = clean_df.filter(~condition | condition.isNull()).count()
        LOGGER.info("Kiểm tra Silver Data Contract %-28s | lỗi: %d dòng", rule_name, invalid_count)
        if invalid_count > 0:
            failed.append(f"{rule_name}: {invalid_count} dòng")

    clean_count = clean_df.count()
    rejected_count = raw_count - clean_count
    reject_rate = (rejected_count / raw_count * 100) if raw_count > 0 else 0.0

    LOGGER.info(
        "Thống kê Silver Quality Gate: raw=%d, valid/clean=%d, rejected/quarantine=%d (tỷ lệ reject=%.2f%%)",
        raw_count,
        clean_count,
        rejected_count,
        reject_rate,
    )

    if clean_count == 0 or failed:
        raise ValueError("Tầng Silver không đạt chất lượng Data Contract: " + "; ".join(failed))


def clean_and_enrich_silver(raw_df: Any) -> Any:
    """Làm sạch, ép kiểu và tính toán các chỉ số bổ sung cho tầng Silver.

    Args:
        raw_df: Spark DataFrame thô từ tầng Ingestion.

    Returns:
        Spark DataFrame sạch đã vượt qua Data Quality Gate.
    """
    raw_count = raw_df.count()
    LOGGER.info("Bắt đầu quy trình làm sạch dữ liệu tầng Silver...")

    clean_df = raw_df.dropDuplicates()

    # Ép kiểu dữ liệu
    clean_df = clean_df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))

    clean_df = (
        clean_df.withColumn("Year", col("Year").cast("int"))
        .withColumn("Month", col("Month").cast("int"))
        .withColumn("Quantity", col("Quantity").cast("int"))
        .withColumn("Unit_Price", col("Unit_Price").cast("double"))
        .withColumn("Discount", col("Discount").cast("double"))
        .withColumn("Revenue", col("Revenue").cast("double"))
        .withColumn("Cost", col("Cost").cast("double"))
        .withColumn("Profit", col("Profit").cast("double"))
        .withColumn("Shipping_Cost", col("Shipping_Cost").cast("double"))
        .withColumn("Shipping_Days", col("Shipping_Days").cast("int"))
    )

    if "Profit_Margin_%" in clean_df.columns:
        clean_df = clean_df.withColumnRenamed("Profit_Margin_%", "Profit_Margin_Percent")

    if "Profit_Margin_Percent" in clean_df.columns:
        clean_df = clean_df.withColumn(
            "Profit_Margin_Percent", col("Profit_Margin_Percent").cast("double")
        )
    else:
        clean_df = clean_df.withColumn(
            "Profit_Margin_Percent",
            when(col("Revenue") != 0, (col("Profit") / col("Revenue")) * 100).otherwise(0.0),
        )

    # Loại bỏ các bản ghi chứa NULL tại các trường bắt buộc
    clean_df = clean_df.dropna(
        subset=[
            "Order_ID",
            "Order_Date",
            "Year",
            "Month",
            "Quantity",
            "Unit_Price",
            "Revenue",
            "Cost",
            "Profit",
            "Shipping_Cost",
            "Shipping_Days",
        ]
    )

    # Tính toán thuộc tính phái sinh
    clean_df = clean_df.withColumn("Revenue_Per_Order", col("Revenue"))
    clean_df = clean_df.withColumn("Net_Profit", col("Profit") - col("Shipping_Cost"))

    clean_df = clean_df.withColumn(
        "Is_Returned", when(col("Order_Status") == "Returned", 1).otherwise(0)
    )

    clean_df = clean_df.withColumn(
        "Is_Cancelled", when(col("Order_Status") == "Cancelled", 1).otherwise(0)
    )

    clean_df = clean_df.withColumn(
        "Delivery_Level",
        when(col("Shipping_Days") <= 3, "Fast")
        .when(col("Shipping_Days") <= 7, "Normal")
        .otherwise("Slow"),
    )

    clean_df = clean_df.cache()
    validate_silver_data(clean_df, raw_count)

    LOGGER.info("Hoàn tất xử lý Silver Layer với %d dòng bản ghi sạch.", clean_df.count())
    return clean_df
