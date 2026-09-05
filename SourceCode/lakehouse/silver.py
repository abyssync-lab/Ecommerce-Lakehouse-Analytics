"""Module làm sạch và kiểm định dữ liệu (Silver Layer) trong Medallion Lakehouse."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import (
    array,
    array_remove,
    col,
    concat_ws,
    current_timestamp,
    lit,
    round,
    size,
    to_date,
    when,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

LOGGER = logging.getLogger(__name__)


def record_pipeline_quality(
    spark: Any,
    raw_count: int,
    clean_count: int,
    rejected_count: int,
    run_id: str | None = None,
    batch_id: str | None = None,
) -> None:
    """Ghi nhận số liệu giám sát Data Observability vào bảng Delta `pipeline_quality`."""
    try:
        from config import SETTINGS
        target_path = SETTINGS.get_storage_path(f"{SETTINGS.gold_monitoring_base}/pipeline_quality_delta")
        reject_rate = (rejected_count / raw_count * 100) if raw_count > 0 else 0.0
        row_data = [
            (
                run_id or "run_default",
                batch_id or "batch_default",
                int(raw_count),
                int(clean_count),
                int(rejected_count),
                float(round(reject_rate, 2)),
            )
        ]
        schema = ["run_id", "batch_id", "raw_rows", "valid_rows", "rejected_rows", "reject_rate_percent"]
        quality_df = (
            spark.createDataFrame(row_data, schema)
            .withColumn("recorded_at", current_timestamp())
        )
        quality_df.write.format("delta").mode("append").save(target_path)
        LOGGER.info("Đã lưu trữ Data Quality Metrics vào %s", target_path)
    except Exception as e:
        LOGGER.debug("Không thể ghi nhận pipeline_quality Delta table (có thể đang chạy unit test local): %s", e)


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


def clean_and_enrich_silver(
    raw_df: Any,
    quarantine_path: str | None = None,
    run_id: str | None = None,
    batch_id: str | None = None,
) -> Any:
    """Làm sạch, ép kiểu và tính toán các chỉ số bổ sung cho tầng Silver.

    Args:
        raw_df: Spark DataFrame thô từ tầng Ingestion.
        quarantine_path: Đường dẫn tùy chọn để lưu trữ các bản ghi bị loại (Quarantine Table).
        run_id: Mã định danh lần chạy pipeline phục vụ monitoring.
        batch_id: Mã định danh batch nạp.

    Returns:
        Spark DataFrame sạch đã vượt qua Data Quality Gate.
    """
    raw_count = raw_df.count()
    LOGGER.info("Bắt đầu quy trình làm sạch dữ liệu tầng Silver...")

    # Deduplicate theo business grain
    typed_df = raw_df.dropDuplicates()

    # Ép kiểu dữ liệu chuẩn
    typed_df = typed_df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))

    typed_df = (
        typed_df.withColumn("Year", col("Year").cast("int"))
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

    if "Profit_Margin_%" in typed_df.columns:
        typed_df = typed_df.withColumnRenamed("Profit_Margin_%", "Profit_Margin_Percent")

    if "Profit_Margin_Percent" in typed_df.columns:
        typed_df = typed_df.withColumn(
            "Profit_Margin_Percent", col("Profit_Margin_Percent").cast("double")
        )
    else:
        typed_df = typed_df.withColumn(
            "Profit_Margin_Percent",
            when(col("Revenue") != 0, (col("Profit") / col("Revenue")) * 100).otherwise(0.0),
        )

    # Đánh giá điều kiện Hợp lệ và Phân lập Quarantine
    required_cols = [
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
    null_cond = col("Order_ID").isNotNull()
    for c in required_cols[1:]:
        null_cond = null_cond & col(c).isNotNull()

    business_cond = (
        (col("Quantity") > 0)
        & (col("Unit_Price") >= 0)
        & (col("Discount").between(0, 1))
        & (col("Revenue") >= 0)
        & (col("Shipping_Days") >= 0)
    )

    is_valid = null_cond & business_cond

    clean_df = typed_df.filter(is_valid)
    rejected_df = typed_df.filter(~is_valid)

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        # Thu thập toàn bộ danh sách các lỗi vi phạm (Multi-error tracking thay vì chỉ giữ 1 lỗi)
        all_reasons_arr = array_remove(
            array(
                when(~null_cond, lit("MISSING_REQUIRED_FIELDS")),
                when(col("Quantity") <= 0, lit("INVALID_QUANTITY")),
                when(col("Unit_Price") < 0, lit("INVALID_UNIT_PRICE")),
                when(~col("Discount").between(0, 1), lit("INVALID_DISCOUNT")),
                when(col("Revenue") < 0, lit("INVALID_REVENUE")),
                when(col("Shipping_Days") < 0, lit("INVALID_SHIPPING_DAYS")),
            ),
            None,
        )

        rejected_df = (
            rejected_df.withColumn("rejection_reasons", all_reasons_arr)
            .withColumn(
                "rejection_reason",
                when(size(col("rejection_reasons")) > 0, concat_ws("; ", col("rejection_reasons")))
                .otherwise(lit("DATA_CONTRACT_VIOLATION")),
            )
            .withColumn("rejected_at", current_timestamp())
        )

        LOGGER.warning("Phát hiện %d bản ghi vi phạm Data Quality Gate. Chuyển vào Quarantine.", rejected_count)
        if quarantine_path:
            try:
                rejected_df.write.format("delta").mode("append").save(quarantine_path)
                LOGGER.info("Đã lưu %d bản ghi lỗi vào Quarantine table tại: %s", rejected_count, quarantine_path)
            except Exception as e:
                LOGGER.warning("Không thể lưu Quarantine table Delta: %s", e)

    # Đặt tên rõ nghĩa cho chỉ số mức đơn: Order_Total_Revenue
    # CẢNH BÁO KIMBALL GRAIN: Giá trị lặp lại ở từng dòng sản phẩm (line-item grain);
    # Tuyệt đối không SUM(Order_Total_Revenue) khi chưa deduplicate Order_ID!
    order_window = Window.partitionBy("Order_ID")
    clean_df = clean_df.withColumn("Order_Total_Revenue", round(spark_sum("Revenue").over(order_window), 2))
    clean_df = clean_df.withColumn("Revenue_Per_Order", col("Order_Total_Revenue"))  # Alias tương thích ngược
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
    clean_count = clean_df.count()
    validate_silver_data(clean_df, raw_count)

    # Ghi nhận chỉ số Data Quality Metrics
    record_pipeline_quality(
        clean_df.sparkSession,
        raw_count=raw_count,
        clean_count=clean_count,
        rejected_count=rejected_count,
        run_id=run_id,
        batch_id=batch_id,
    )

    LOGGER.info("Hoàn tất xử lý Silver Layer với %d dòng bản ghi sạch.", clean_count)
    return clean_df
