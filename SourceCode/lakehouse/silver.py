"""Module làm sạch và kiểm định dữ liệu (Silver Layer) trong Medallion Lakehouse."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    concat_ws,
    countDistinct,
    current_timestamp,
    expr,
    lit,
    month,
    row_number,
    sha2,
    size,
    substring,
    to_date,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

from .contracts.loader import (
    DatasetContract,
    get_spark_silver_rules,
    load_contract,
    load_contract_for_columns,
)

LOGGER = logging.getLogger(__name__)


def validate_silver_data(
    clean_df: Any,
    raw_count: int,
    duplicate_count: int = 0,
    rejected_count: int = 0,
    contract: DatasetContract | None = None,
    source_columns: set[str] | None = None,
) -> None:
    """Kiểm tra các quy tắc nghiệp vụ cốt lõi Data Contract sau bước làm sạch."""
    active_contract = contract or load_contract()
    contract_rules = get_spark_silver_rules(active_contract)
    source_column_set = source_columns or set(clean_df.columns)
    rules = {
        rule_name: cond
        for rule_name, cond in contract_rules.items()
        if rule_name.split()[0] in source_column_set
    }
    # Đảm bảo các quy tắc cốt lõi luôn có mặt
    rules.setdefault("Order_ID không rỗng", col("Order_ID").isNotNull())
    rules.setdefault("Quantity > 0", col("Quantity") > 0)
    rules.setdefault("Unit_Price >= 0", col("Unit_Price") >= 0)
    rules.setdefault("Discount trong [0, 1]", col("Discount").between(0, 1))
    if "Source_Revenue" in clean_df.columns:
        rules.setdefault(
            "Source_Revenue >= 0",
            col("Source_Revenue").isNull() | (col("Source_Revenue") >= 0),
        )
    # Shipping_Days là thuộc tính cấp đơn và có thể chưa xuất hiện ở event
    # tạo mới; NULL không phải là 0 và không được biến thành dữ liệu giả.
    rules.setdefault(
        "Shipping_Days >= 0",
        col("Shipping_Days").isNull() | (col("Shipping_Days") >= 0),
    )

    failed = []
    # DELETE chỉ mang khóa + sequence metadata; không áp dụng các rule tài chính
    # của UPSERT lên những event này.
    validation_df = (
        clean_df.filter(col("Operation") != "DELETE")
        if "Operation" in clean_df.columns
        else clean_df
    )
    for rule_name, condition in rules.items():
        invalid_count = validation_df.filter(~condition | condition.isNull()).count()
        LOGGER.info("Kiểm tra Silver Data Contract %-28s | lỗi: %d dòng", rule_name, invalid_count)
        if invalid_count > 0:
            failed.append(f"{rule_name}: {invalid_count} dòng")

    clean_count = clean_df.count()
    reject_rate = (rejected_count / raw_count * 100) if raw_count > 0 else 0.0

    LOGGER.info(
        "Thống kê Silver validation: raw=%d, duplicate=%d, valid/clean=%d, rejected/quarantine=%d (tỷ lệ reject=%.2f%%)",
        raw_count,
        duplicate_count,
        clean_count,
        rejected_count,
        reject_rate,
    )

    # Một batch chỉ có dòng lỗi vẫn phải trả về DataFrame rỗng để caller kiểm tra
    # quarantine và reject-rate. Chính pipeline mới quyết định FAILED khi vượt ngưỡng.
    no_accepted_rows_without_reason = (
        clean_count == 0 and raw_count > 0 and rejected_count == 0 and duplicate_count == 0
    )
    if failed or no_accepted_rows_without_reason:
        raise ValueError("Tầng Silver không đạt chất lượng Data Contract: " + "; ".join(failed))


def clean_and_enrich_silver(
    raw_df: Any,
    quarantine_path: str | None = None,
    run_id: str | None = None,
    batch_id: str | None = None,
    allow_line_id_fallback: bool = True,
) -> Any:
    """Làm sạch, ép kiểu và tính toán các chỉ số bổ sung cho tầng Silver.

    Args:
        raw_df: Spark DataFrame thô từ tầng Ingestion.
        quarantine_path: Đường dẫn tùy chọn để lưu trữ các bản ghi bị loại (Quarantine Table).
        run_id: Mã định danh lần chạy pipeline phục vụ monitoring.
        batch_id: Mã định danh batch nạp.
        allow_line_id_fallback: Chỉ bật cho historical bootstrap; incremental phải nhận
            Order_Line_ID thật từ upstream.

    Returns:
        Spark DataFrame sạch sau khi vượt qua row validation.
    """
    raw_count = raw_df.count()
    LOGGER.info("Bắt đầu quy trình làm sạch dữ liệu tầng Silver...")

    # Deduplicate theo business grain (loại trừ metadata kỹ thuật) và tính chính xác số lượng trùng lặp
    biz_cols = [c for c in raw_df.columns if not c.startswith("_")]
    dedup_df = raw_df.dropDuplicates(subset=biz_cols) if biz_cols else raw_df.dropDuplicates()
    duplicate_count = raw_count - dedup_df.count()
    typed_df = dedup_df

    # Adapter giúp bootstrap historical dataset cũ tương thích với contract event v2.
    # Incremental mode không được tự sinh line id; caller truyền False để bắt lỗi nguồn.
    if "Product_Name" not in typed_df.columns and "Product_ID" in typed_df.columns:
        typed_df = typed_df.withColumn("Product_Name", col("Product_ID"))
    if "Product_ID" not in typed_df.columns and "Product_Name" in typed_df.columns:
        # Historical bootstrap chỉ được phép sinh natural key Product_ID từ
        # Product_Name; incremental v2 bắt buộc upstream gửi Product_ID thật.
        typed_df = typed_df.withColumn("Product_ID", sha2(trim(col("Product_Name")), 256))

    # Event v2 cho incremental bắt buộc có Product_ID ở mức dòng UPSERT.
    # Đưa cột còn thiếu về NULL để rule quality tạo MISSING_PRODUCT_ID thay vì
    # để lỗi schema phát nổ muộn hơn trong lúc build dimension.
    is_incremental_event = not allow_line_id_fallback and {
        "Order_Line_ID",
        "Source_Updated_At",
        "Operation",
    }.issubset(typed_df.columns)
    if is_incremental_event and "Product_ID" not in typed_df.columns:
        typed_df = typed_df.withColumn("Product_ID", lit(None).cast("string"))
    for optional_text in [
        "Category",
        "Sub_Category",
        "Customer_Gender",
        "Customer_Segment",
        "Payment_Method",
        "Shipping_Method",
        "Region",
        "Country",
    ]:
        if optional_text not in typed_df.columns:
            typed_df = typed_df.withColumn(optional_text, lit("Unknown"))
    for required_text in ["Customer_ID", "Order_Status"]:
        if required_text not in typed_df.columns:
            # Không gán Unknown cho khóa/semantic status bắt buộc của UPSERT;
            # để rule quarantine báo đúng lỗi nguồn.
            typed_df = typed_df.withColumn(required_text, lit(None).cast("string"))
    for numeric_column, spark_type in [
        ("Quantity", "int"),
        ("Unit_Price", "double"),
        ("Discount", "double"),
        ("Cost", "double"),
    ]:
        if numeric_column not in typed_df.columns:
            typed_df = typed_df.withColumn(numeric_column, lit(None).cast(spark_type))
    if "Revenue" not in typed_df.columns:
        # Contract v2 không bắt upstream gửi Revenue; đây là số liệu audit
        # tùy chọn, không được gán công thức rồi gọi nhầm là source value.
        typed_df = typed_df.withColumn("Revenue", lit(None).cast("double"))
    if "Profit" not in typed_df.columns:
        typed_df = typed_df.withColumn(
            "Profit",
            when(
                col("Revenue").isNotNull() & col("Cost").isNotNull(), col("Revenue") - col("Cost")
            ).otherwise(lit(None).cast("double")),
        )
    if "Shipping_Cost" not in typed_df.columns:
        # Chi phí vận chuyển chỉ có nghĩa ở order-level; giữ NULL khi nguồn
        # chưa cung cấp để tránh cộng thiếu dữ liệu thành giá trị bằng 0.
        typed_df = typed_df.withColumn("Shipping_Cost", lit(None).cast("double"))
    if "Shipping_Days" not in typed_df.columns:
        typed_df = typed_df.withColumn("Shipping_Days", lit(None).cast("int"))
    if "Order_Date" not in typed_df.columns:
        typed_df = typed_df.withColumn("Order_Date", lit(None).cast("date"))
    if "Source_Updated_At" not in typed_df.columns:
        typed_df = typed_df.withColumn(
            "Source_Updated_At", to_timestamp(col("Order_Date"), "yyyy-MM-dd")
        )
    else:
        typed_df = typed_df.withColumn("Source_Updated_At", to_timestamp(col("Source_Updated_At")))
    if "Operation" not in typed_df.columns:
        typed_df = typed_df.withColumn("Operation", lit("UPSERT"))
    else:
        typed_df = typed_df.withColumn("Operation", trim(col("Operation")).alias("Operation"))

    # Cùng line + cùng timestamp nhưng khác record hash là source conflict. Không
    # được chọn ngẫu nhiên một phiên bản vì sẽ làm mất auditability của event.
    if "_record_hash" not in typed_df.columns:
        hash_expr = concat_ws(
            "\u001f",
            *[coalesce(col(c).cast("string"), lit("")) for c in biz_cols if c in typed_df.columns],
        )
        typed_df = typed_df.withColumn("_record_hash", sha2(hash_expr, 256))
    if {"Order_ID", "Order_Line_ID", "Source_Updated_At"}.issubset(typed_df.columns):
        conflict_keys = ["Order_ID", "Order_Line_ID", "Source_Updated_At"]
        conflict_groups = typed_df.groupBy(*conflict_keys).agg(
            countDistinct("_record_hash").alias("_sequence_hash_count")
        )
        typed_df = (
            typed_df.join(conflict_groups, on=conflict_keys, how="left")
            .withColumn(
                "_sequence_conflict",
                coalesce(col("_sequence_hash_count") > 1, lit(False)),
            )
            .drop("_sequence_hash_count")
        )
    else:
        typed_df = typed_df.withColumn("_sequence_conflict", lit(False))

    # Ép kiểu dữ liệu chuẩn
    typed_df = typed_df.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))

    # Tự động trích xuất Year và Month từ Order_Date nếu dữ liệu nguồn chưa có
    if "Year" in typed_df.columns:
        typed_df = typed_df.withColumn("Year", col("Year").cast("int"))
    else:
        typed_df = typed_df.withColumn("Year", year(col("Order_Date")))

    if "Month" in typed_df.columns:
        typed_df = typed_df.withColumn("Month", col("Month").cast("int"))
    else:
        typed_df = typed_df.withColumn("Month", month(col("Order_Date")))

    typed_df = (
        typed_df.withColumn("Quantity", col("Quantity").cast("int"))
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
    # Các trường dưới đây là bắt buộc cho UPSERT. Shipping_Cost và
    # Shipping_Days thuộc order-level, không được dùng để loại một line event.
    required_cols = [
        "Order_ID",
        "Order_Date",
        "Customer_ID",
        "Order_Status",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Cost",
    ]
    null_cond = col("Order_ID").isNotNull()
    for c in required_cols[1:]:
        null_cond = null_cond & col(c).isNotNull()

    business_cond = (
        (col("Quantity") > 0)
        & (col("Unit_Price") >= 0)
        & (col("Discount").between(0, 1))
        & (col("Cost") >= 0)
        & (col("Shipping_Days").isNull() | (col("Shipping_Days") >= 0))
    )

    # coalesce(..., false) rất quan trọng: trong Spark, filter(NULL) loại dòng khỏi
    # cả valid lẫn invalid, làm sai công thức raw = valid + rejected + duplicate.
    if "Order_Line_ID" not in typed_df.columns:
        line_id_present = lit(False)
    else:
        line_id_present = col("Order_Line_ID").isNotNull() & (trim(col("Order_Line_ID")) != "")
    valid_operation = col("Operation").isin("UPSERT", "DELETE")
    valid_status = col("Order_Status").isin(
        "Processing", "Shipped", "Delivered", "Returned", "Cancelled"
    )
    valid_timestamp = col("Source_Updated_At").isNotNull()
    valid_event_time = ~(
        col("Order_Date").isNotNull()
        & col("Source_Updated_At").isNotNull()
        & (col("Order_Date").cast("timestamp") > col("Source_Updated_At"))
    )
    valid_upsert = (
        coalesce(null_cond & business_cond, lit(False))
        & valid_operation
        & valid_status
        & valid_timestamp
        & valid_event_time
    )
    if "Product_ID" in typed_df.columns:
        valid_upsert = valid_upsert & col("Product_ID").isNotNull()
    valid_delete = (
        col("Order_ID").isNotNull()
        & line_id_present
        & valid_timestamp
        & (col("Operation") == "DELETE")
    )
    is_valid = when(col("Operation") == "DELETE", valid_delete).otherwise(valid_upsert)
    if not allow_line_id_fallback:
        is_valid = is_valid & line_id_present
    is_valid = is_valid & ~col("_sequence_conflict")

    clean_df = typed_df.filter(coalesce(is_valid, lit(False)))
    rejected_df = typed_df.filter(~coalesce(is_valid, lit(False)))

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        # Thu thập toàn bộ danh sách các lỗi vi phạm (Multi-error tracking thay vì chỉ giữ 1 lỗi)
        is_upsert = col("Operation") == "UPSERT"
        order_id_present = col("Order_ID").isNotNull() & (
            trim(coalesce(col("Order_ID"), lit(""))) != ""
        )
        event_time_order_invalid = (
            col("Order_Date").isNotNull()
            & col("Source_Updated_At").isNotNull()
            & (col("Order_Date").cast("timestamp") > col("Source_Updated_At"))
        )
        all_reasons_arr = array(
            when(~coalesce(null_cond, lit(False)), lit("MISSING_REQUIRED_FIELDS")),
            when(~order_id_present, lit("MISSING_ORDER_ID")),
            when(is_upsert & col("Customer_ID").isNull(), lit("MISSING_CUSTOMER_ID")),
            when(is_upsert & col("Order_Status").isNull(), lit("MISSING_ORDER_STATUS")),
            when(~line_id_present, lit("MISSING_LINE_ID"))
            if not allow_line_id_fallback
            else lit(None),
            when(is_upsert & col("Product_ID").isNull(), lit("MISSING_PRODUCT_ID"))
            if "Product_ID" in typed_df.columns
            else lit(None),
            when(
                is_upsert & (col("Quantity").isNull() | (col("Quantity") <= 0)),
                lit("INVALID_QUANTITY"),
            ),
            when(
                is_upsert & (col("Unit_Price").isNull() | (col("Unit_Price") < 0)),
                lit("INVALID_UNIT_PRICE"),
            ),
            when(
                is_upsert & (col("Discount").isNull() | ~col("Discount").between(0, 1)),
                lit("INVALID_DISCOUNT"),
            ),
            when(
                is_upsert & col("Shipping_Days").isNotNull() & (col("Shipping_Days") < 0),
                lit("INVALID_SHIPPING_DAYS"),
            ),
            when(
                col("Operation").isNull() | ~col("Operation").isin("UPSERT", "DELETE"),
                lit("INVALID_OPERATION"),
            ),
            when(is_upsert & ~valid_status, lit("INVALID_ORDER_STATUS")),
            when(col("Source_Updated_At").isNull(), lit("INVALID_UPDATED_AT")),
            when(event_time_order_invalid, lit("INVALID_EVENT_TIME")),
            when(
                is_upsert & (col("Cost").isNull() | (col("Cost") < 0)),
                lit("INVALID_COST"),
            ),
            when(col("_sequence_conflict"), lit("SEQUENCE_CONFLICT")),
        )

        rejected_df = (
            rejected_df.withColumn("_error_codes_raw", all_reasons_arr)
            .withColumn("error_codes", expr("filter(_error_codes_raw, x -> x is not null)"))
            .withColumn("rejection_reasons", col("error_codes"))
            .withColumn(
                "rejection_reason",
                when(size(col("error_codes")) > 0, concat_ws("; ", col("error_codes"))).otherwise(
                    lit("DATA_CONTRACT_VIOLATION")
                ),
            )
            .drop("_error_codes_raw")
            .withColumn("rejected_at", current_timestamp())
        )

        LOGGER.warning(
            "Phát hiện %d bản ghi vi phạm row validation. Chuyển vào Quarantine.", rejected_count
        )
        if quarantine_path:
            try:
                rejected_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
                    quarantine_path
                )
                LOGGER.info(
                    "Đã lưu %d bản ghi lỗi vào Quarantine table tại: %s",
                    rejected_count,
                    quarantine_path,
                )
            except Exception:
                LOGGER.exception("Không thể lưu Quarantine table Delta")
                raise

    # Silver line không tạo chỉ số cấp đơn lặp lại trên từng dòng. Các cột
    # Source_* chỉ phục vụ audit; số liệu Gold phải tính lại từ nguyên liệu.
    clean_df = (
        clean_df.withColumnRenamed("Revenue", "Source_Revenue")
        .withColumnRenamed("Profit", "Source_Profit")
        .withColumn("Gross_Amount", spark_round(col("Quantity") * col("Unit_Price"), 2))
        .withColumn("Discount_Amount", spark_round(col("Gross_Amount") * col("Discount"), 2))
        .withColumn("Net_Line_Amount", spark_round(col("Gross_Amount") - col("Discount_Amount"), 2))
        .withColumn("Cost_Amount", spark_round(col("Quantity") * col("Cost"), 2))
        .withColumn("Gross_Profit", spark_round(col("Net_Line_Amount") - col("Cost_Amount"), 2))
    )
    clean_df = clean_df.withColumn(
        "Is_Deleted", when(col("Operation") == "DELETE", lit(True)).otherwise(lit(False))
    )
    clean_df = clean_df.drop("_sequence_conflict")

    # Định danh duy nhất cho từng dòng sản phẩm trong đơn (Order-Line Grain):
    # - Nếu upstream cung cấp Order_Line_ID (và không rỗng), bảo toàn nguyên bản.
    # - Nếu chưa có (dataset demo), sinh deterministic content fingerprint (Order_ID + Product_Name + Unit_Price + Quantity + Discount)
    #   thay vì row_number() động, ngăn ngừa hoàn toàn nguy cơ đè nhầm bản ghi giữa các micro-batch MERGE.
    source_contract = load_contract_for_columns(raw_df.columns)
    contract = source_contract
    fp_cols = [
        col(c).cast("string")
        for c in contract.fallback_line_fingerprint_cols
        if c in clean_df.columns
    ]
    if not fp_cols:
        fp_cols = [
            col("Order_ID").cast("string"),
            col("Product_Name").cast("string"),
            col("Quantity").cast("string"),
            col("Unit_Price").cast("string"),
            col("Discount").cast("string"),
        ]

    line_fingerprint = substring(sha2(concat_ws("||", *fp_cols), 256), 1, 8)

    if "Order_Line_ID" not in clean_df.columns and allow_line_id_fallback:
        clean_df = clean_df.withColumn(
            "Order_Line_ID", concat_ws("-", col("Order_ID"), line_fingerprint)
        )
    elif "Order_Line_ID" in clean_df.columns and allow_line_id_fallback:
        clean_df = clean_df.withColumn(
            "Order_Line_ID",
            when(
                col("Order_Line_ID").isNotNull() & (col("Order_Line_ID") != ""),
                col("Order_Line_ID"),
            ).otherwise(concat_ws("-", col("Order_ID"), line_fingerprint)),
        )

    clean_df = clean_df.withColumn(
        "Is_Returned", when(col("Order_Status") == "Returned", 1).otherwise(0)
    )

    clean_df = clean_df.withColumn(
        "Is_Cancelled", when(col("Order_Status") == "Cancelled", 1).otherwise(0)
    )

    clean_df = clean_df.withColumn(
        "Delivery_Level",
        when(col("Shipping_Days").isNull(), lit("Unknown"))
        .when(col("Shipping_Days") <= 3, lit("Fast"))
        .when(col("Shipping_Days") <= 7, lit("Normal"))
        .otherwise(lit("Slow")),
    )

    clean_df = clean_df.cache()
    clean_count = clean_df.count()
    validate_silver_data(
        clean_df,
        raw_count=raw_count,
        duplicate_count=duplicate_count,
        rejected_count=rejected_count,
        contract=source_contract,
        source_columns=set(raw_df.columns),
    )

    LOGGER.info("Hoàn tất xử lý Silver Layer với %d dòng bản ghi sạch.", clean_count)
    return clean_df


def _latest_event(df: Any, keys: list[str]) -> Any:
    """Chọn version mới nhất, chỉ dùng tie-breaker kỹ thuật khi timestamp bằng nhau."""
    order_columns = [col("Source_Updated_At").desc_nulls_last()]
    if "_source_row_number" in df.columns:
        order_columns.append(col("_source_row_number").desc_nulls_last())
    if "_record_hash" in df.columns:
        order_columns.append(col("_record_hash").desc_nulls_last())
    window = Window.partitionBy(*keys).orderBy(*order_columns)
    return (
        df.withColumn("_latest_row", row_number().over(window))
        .filter(col("_latest_row") == 1)
        .drop("_latest_row")
    )


def build_silver_current_events(events_df: Any) -> Any:
    """Lấy snapshot line hiện hành từ event history cho Gold và reconciliation.

    Bronze giữ mọi version để audit; bảng Silver backing có thể chứa event đã
    được MERGE. Hàm này vẫn bảo vệ Gold bằng cách chọn một event thắng cho mỗi
    order line và loại soft-delete trước khi dựng dimensions, facts hoặc marts.
    """
    if "Order_Line_ID" not in events_df.columns:
        if "Is_Deleted" not in events_df.columns:
            return events_df
        return events_df.filter(~coalesce(col("Is_Deleted"), lit(False)))

    latest = _latest_event(events_df, ["Order_ID", "Order_Line_ID"])
    if "Is_Deleted" not in latest.columns:
        return latest
    return latest.filter(~coalesce(col("Is_Deleted"), lit(False)))


def build_silver_order_lines_current(events_df: Any) -> Any:
    """Dựng Silver line current-state, grain `(Order_ID, Order_Line_ID)`."""
    latest = _latest_event(events_df, ["Order_ID", "Order_Line_ID"])
    columns = [
        "Order_ID",
        "Order_Line_ID",
        "Product_ID",
        "Product_Name",
        "Category",
        "Sub_Category",
        "Quantity",
        "Unit_Price",
        "Discount",
        "Source_Revenue",
        "Source_Profit",
        "Gross_Amount",
        "Discount_Amount",
        "Net_Line_Amount",
        "Cost_Amount",
        "Gross_Profit",
        "Order_Date",
        "Source_Updated_At",
        "Operation",
        "Is_Deleted",
        "_record_hash",
        "_run_id",
        "_batch_id",
        "_source_hash",
        "_source_uri",
        "_source_row_number",
        "_ingested_at",
        "_contract_version",
        "_pipeline_version",
    ]
    return latest.select(*[col(name) for name in columns if name in latest.columns])


def build_silver_orders_current(events_df: Any) -> Any:
    """Dựng Silver order current-state, grain `Order_ID`."""
    order_source = events_df
    if "Order_Operation" not in events_df.columns and "Operation" in events_df.columns:
        order_source = events_df.filter(col("Operation") != "DELETE")
    latest = _latest_event(order_source, ["Order_ID"])

    # Một order có thể chỉ nhận DELETE ở line-level. Khi đó header cũ vẫn còn
    # trong event history nhưng không còn line active để phục vụ Gold.
    active_order_ids = None
    if "Order_Line_ID" in events_df.columns:
        latest_lines = _latest_event(events_df, ["Order_ID", "Order_Line_ID"])
        active_order_ids = (
            latest_lines.filter(~col("Is_Deleted") & (col("Operation") != "DELETE"))
            .select("Order_ID")
            .dropDuplicates()
            .withColumn("_has_active_line", lit(True))
        )
        latest = latest.join(active_order_ids, on="Order_ID", how="left")

    # Event v2 hiện tại là line feed: DELETE một line không xóa order nếu vẫn còn
    # line active. Khi OMS có order feed riêng, Order_Operation có quyền quyết định
    # soft delete ở header; dữ liệu seed không có line id được coi là active.
    if "Order_Operation" in latest.columns:
        latest = latest.withColumn(
            "Is_Deleted",
            col("Order_Operation") == "DELETE",
        )
    elif active_order_ids is not None:
        latest = latest.withColumn("Is_Deleted", ~coalesce(col("_has_active_line"), lit(False)))
    else:
        latest = latest.withColumn("Is_Deleted", lit(False))
    columns = [
        "Order_ID",
        "Order_Date",
        "Customer_ID",
        "Customer_Gender",
        "Customer_Segment",
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "Shipping_Cost",
        "Shipping_Days",
        "Delivery_Level",
        "Region",
        "Country",
        "Source_Updated_At",
        "Operation",
        "Is_Deleted",
        "_record_hash",
        "_run_id",
        "_batch_id",
        "_source_hash",
        "_source_uri",
        "_source_row_number",
        "_ingested_at",
        "_contract_version",
        "_pipeline_version",
    ]
    return latest.select(*[col(name) for name in columns if name in latest.columns])
