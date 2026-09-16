"""Mô-đun đối soát tính toàn vẹn và bất biến dữ liệu Gold.

Thực hiện các kiểm tra bất biến toán học và tính toàn vẹn khóa ngoại xuyên suốt các tầng:
1. Bất biến doanh thu: Silver == FactSales == Mart Overview.
2. Bảo toàn số dòng: Raw == Valid + Invalid + Duplicate.
3. Grain Fact duy nhất: Distinct(SalesKey) == Count(FactSales).
4. Đầy đủ khóa ngoại: Không có foreign key mồ côi.
5. Toàn vẹn thời gian SCD2: Không chồng lấn, đúng 1 Is_Current=1 cho mỗi customer.
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql.functions import col, count, countDistinct, lead, when
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

from .marts import load_business_policy

LOGGER = logging.getLogger(__name__)


def reconcile_revenue_invariant(
    clean_df: Any, fact_sales: Any, mart_overview: Any
) -> dict[str, Any]:
    """Kiểm tra amount của Silver == Fact == Executive overview theo cùng policy."""
    revenue_policy = load_business_policy()["recognized_revenue"]
    silver_input = (
        clean_df.filter(col("Order_Status").isin(revenue_policy))
        if "Delivered_Revenue" in mart_overview.columns and "Order_Status" in clean_df.columns
        else clean_df
    )
    fact_input = (
        fact_sales.filter(col("Order_Status").isin(revenue_policy))
        if "Delivered_Revenue" in mart_overview.columns and "Order_Status" in fact_sales.columns
        else fact_sales
    )
    silver_column = "Net_Line_Amount" if "Net_Line_Amount" in silver_input.columns else "Revenue"
    fact_column = "Net_Line_Amount" if "Net_Line_Amount" in fact_input.columns else "Revenue"
    silver_rev = round(
        float(silver_input.select(spark_sum(silver_column)).collect()[0][0] or 0.0), 2
    )
    fact_rev = round(float(fact_input.select(spark_sum(fact_column)).collect()[0][0] or 0.0), 2)
    if "Total_Revenue" in mart_overview.columns:
        overview_value = mart_overview.select("Total_Revenue").collect()[0][0]
    else:
        overview_value = mart_overview.select(spark_sum("Delivered_Revenue")).collect()[0][0]
    overview_rev = round(float(overview_value or 0.0), 2)

    diff_silver_fact = abs(silver_rev - fact_rev)
    diff_fact_overview = abs(fact_rev - overview_rev)
    passed = diff_silver_fact < 0.01 and diff_fact_overview < 0.01

    return {
        "check": "revenue_invariant",
        "passed": passed,
        "silver_revenue": silver_rev,
        "fact_revenue": fact_rev,
        "overview_revenue": overview_rev,
        "difference": max(diff_silver_fact, diff_fact_overview),
    }


def reconcile_row_conservation(
    raw_count: int, valid_count: int, invalid_count: int, duplicate_count: int
) -> dict[str, Any]:
    """Kiểm tra định luật bảo toàn số dòng: Raw == Valid + Invalid (Quarantine) + Duplicate."""
    expected_sum = valid_count + invalid_count + duplicate_count
    passed = raw_count == expected_sum

    return {
        "check": "row_count_conservation",
        "passed": passed,
        "raw_count": raw_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "duplicate_count": duplicate_count,
        "sum_components": expected_sum,
    }


def reconcile_fact_grain_uniqueness(fact_sales: Any) -> dict[str, Any]:
    """Kiểm tra cả surrogate key và business grain của fact line."""
    total_rows = fact_sales.count()
    distinct_keys = fact_sales.select(countDistinct("SalesKey")).first()[0]
    grain_columns = ["Order_ID", "Order_Line_ID"]
    if not set(grain_columns).issubset(fact_sales.columns):
        grain_columns = ["Order_ID", "Product_ID"]
    distinct_business_keys = fact_sales.select(*grain_columns).distinct().count()
    duplicate_business_rows = total_rows - distinct_business_keys
    passed = total_rows == distinct_keys and duplicate_business_rows == 0

    return {
        "check": "fact_grain_uniqueness",
        "passed": passed,
        "total_fact_rows": total_rows,
        "distinct_sales_keys": distinct_keys,
        "grain_columns": grain_columns,
        "distinct_business_keys": distinct_business_keys,
        "duplicate_business_rows": duplicate_business_rows,
    }


def assert_unique_grain(dataframe: Any, columns: list[str]) -> None:
    """Ném AssertionError khi DataFrame không duy nhất theo grain được chỉ định."""
    total_rows = dataframe.count()
    distinct_rows = dataframe.select(*columns).distinct().count()
    if total_rows != distinct_rows:
        raise AssertionError(
            f"Grain không duy nhất theo {columns}: tổng={total_rows}, khác biệt={distinct_rows}"
        )


def reconcile_order_fact_grain(fact_order_fulfillment: Any) -> dict[str, Any]:
    """Kiểm tra fact_order_fulfillment chỉ có một dòng cho mỗi Order_ID."""
    total_rows = fact_order_fulfillment.count()
    distinct_orders = fact_order_fulfillment.select("Order_ID").distinct().count()
    passed = total_rows == distinct_orders
    return {
        "check": "order_fact_grain",
        "passed": passed,
        "total_order_fact_rows": total_rows,
        "distinct_order_ids": distinct_orders,
    }


def reconcile_fk_completeness(
    fact_sales: Any, fact_order_fulfillment: Any | None = None
) -> dict[str, Any]:
    """Kiểm tra foreign key của cả line fact và order fact."""
    fact_key_sets = {
        "fact_sales_line": ["CustomerKey", "ProductKey", "DateKey", "GeographyKey", "ContextKey"]
    }
    if fact_order_fulfillment is not None:
        fact_key_sets["fact_order_fulfillment"] = [
            "CustomerKey",
            "DateKey",
            "GeographyKey",
            "ContextKey",
        ]
    null_counts: dict[str, int] = {}
    fact_frames = {
        "fact_sales_line": fact_sales,
        "fact_order_fulfillment": fact_order_fulfillment,
    }
    for fact_name, fk_cols in fact_key_sets.items():
        dataframe = fact_frames[fact_name]
        for fk in fk_cols:
            if fk in dataframe.columns:
                cnt = dataframe.filter(col(fk).isNull()).count()
                null_counts[f"{fact_name}.{fk}"] = cnt

    total_nulls = sum(null_counts.values())
    return {
        "check": "foreign_key_completeness",
        "passed": total_nulls == 0,
        "null_key_counts": null_counts,
    }


def reconcile_current_state_grain(
    silver_orders_current: Any | None,
    silver_order_lines_current: Any | None,
) -> dict[str, Any]:
    """Kiểm tra grain riêng của order header và order line current-state."""
    if silver_orders_current is None or silver_order_lines_current is None:
        return {
            "check": "silver_current_state_grain",
            "passed": True,
            "note": "Không truyền hai bảng Silver tách grain; bỏ qua ở API tương thích.",
        }
    active_orders = silver_orders_current.filter(~col("Is_Deleted"))
    active_lines = silver_order_lines_current.filter(~col("Is_Deleted"))
    duplicate_orders = active_orders.count() - active_orders.select("Order_ID").distinct().count()
    duplicate_lines = (
        active_lines.count() - active_lines.select("Order_ID", "Order_Line_ID").distinct().count()
    )
    orphan_lines = active_lines.join(
        active_orders.select("Order_ID").distinct(), on="Order_ID", how="left_anti"
    ).count()
    passed = duplicate_orders == 0 and duplicate_lines == 0 and orphan_lines == 0
    return {
        "check": "silver_current_state_grain",
        "passed": passed,
        "duplicate_order_rows": duplicate_orders,
        "duplicate_order_line_rows": duplicate_lines,
        "orphan_active_lines": orphan_lines,
    }


def reconcile_scd2_temporal_integrity(dim_customer: Any) -> dict[str, Any]:
    """Kiểm tra tính toàn vẹn thời gian của SCD Type 2 Customer:

    - Không có khoảng chồng lấn (ValidFrom < ValidTo)
    - Đúng 1 bản ghi hiện hành (Is_Current = 1) cho mỗi khách hàng
    """
    if "ValidFrom" not in dim_customer.columns or "ValidTo" not in dim_customer.columns:
        return {
            "check": "scd2_temporal_integrity",
            "passed": True,
            "note": "dim_customer đang chạy ở chế độ SCD Type 1, bỏ qua kiểm tra SCD2.",
        }
    if "Is_Current" not in dim_customer.columns:
        return {
            "check": "scd2_temporal_integrity",
            "passed": False,
            "note": "dim_customer có cột thời gian nhưng thiếu Is_Current.",
        }

    # 1. Kiểm tra từng khoảng có đủ mốc và ValidFrom < ValidTo.
    invalid_intervals = dim_customer.filter(
        col("ValidFrom").isNull() | col("ValidTo").isNull() | (col("ValidFrom") >= col("ValidTo"))
    ).count()

    # 2. Kiểm tra hai khoảng liên tiếp của cùng customer không bị chồng lấn.
    ordered = dim_customer.withColumn(
        "_next_valid_from",
        lead("ValidFrom").over(
            Window.partitionBy("Customer_ID").orderBy("ValidFrom", "CustomerKey")
        ),
    )
    overlapping_intervals = ordered.filter(
        col("_next_valid_from").isNotNull() & (col("ValidTo") > col("_next_valid_from"))
    ).count()

    # 3. Bao gồm cả customer có 0 dòng hiện hành; chỉ group các dòng Is_Current=1
    # sẽ bỏ sót trường hợp này.
    current_counts = dim_customer.groupBy("Customer_ID").agg(
        count("CustomerKey").alias("version_cnt"),
        spark_sum(when(col("Is_Current") == 1, 1).otherwise(0)).alias("current_cnt"),
    )
    duplicate_current = current_counts.filter(col("current_cnt") != 1).count()

    passed = invalid_intervals == 0 and overlapping_intervals == 0 and duplicate_current == 0
    return {
        "check": "scd2_temporal_integrity",
        "passed": passed,
        "invalid_intervals_count": invalid_intervals,
        "overlapping_intervals_count": overlapping_intervals,
        "customers_with_abnormal_current_count": duplicate_current,
    }


def run_full_reconciliation(
    clean_df: Any,
    fact_sales: Any,
    mart_overview: Any,
    dim_customer: Any,
    raw_count: int,
    duplicate_count: int,
    invalid_count: int,
    run_id: str | None = None,
    export_path: str | Path | None = None,
    valid_count: int | None = None,
    silver_orders_current: Any | None = None,
    silver_order_lines_current: Any | None = None,
    fact_order_fulfillment: Any | None = None,
) -> dict[str, Any]:
    """Thực thi toàn bộ kiểm tra đối soát và xuất báo cáo JSON.

    Args:
        clean_df: DataFrame sạch tầng Silver.
        fact_sales: FactSales DataFrame.
        mart_overview: mart_overview DataFrame.
        dim_customer: dim_customer DataFrame.
        raw_count: Số dòng thô ban đầu.
        duplicate_count: Số dòng trùng lặp.
        invalid_count: Số dòng bị reject vào quarantine.
        run_id: Mã định danh lần chạy.
        export_path: Đường dẫn lưu trữ báo cáo JSON tùy chọn.
        valid_count: Số event hợp lệ của batch hiện tại. Incremental phải truyền giá trị
            này vì ``clean_df`` là current-state Silver, không phải toàn bộ Bronze event history.

    Returns:
        Dictionary chứa kết quả toàn bộ các kiểm tra đối soát.
    """
    rid = run_id or f"recon_{uuid.uuid4().hex[:8]}"
    LOGGER.info("=== BẮT ĐẦU GOLD RECONCILIATION (Run ID: %s) ===", rid)

    rev_check = reconcile_revenue_invariant(clean_df, fact_sales, mart_overview)
    row_check = reconcile_row_conservation(
        raw_count=raw_count,
        valid_count=clean_df.count() if valid_count is None else valid_count,
        invalid_count=invalid_count,
        duplicate_count=duplicate_count,
    )
    grain_check = reconcile_fact_grain_uniqueness(fact_sales)
    fk_check = reconcile_fk_completeness(fact_sales, fact_order_fulfillment)
    scd2_check = reconcile_scd2_temporal_integrity(dim_customer)
    silver_grain_check = reconcile_current_state_grain(
        silver_orders_current, silver_order_lines_current
    )
    order_fact_check = (
        reconcile_order_fact_grain(fact_order_fulfillment)
        if fact_order_fulfillment is not None
        else {
            "check": "order_fact_grain",
            "passed": True,
            "note": "Không truyền fact_order_fulfillment; bỏ qua ở API tương thích.",
        }
    )

    all_passed = all(
        [
            rev_check["passed"],
            row_check["passed"],
            grain_check["passed"],
            fk_check["passed"],
            scd2_check["passed"],
            silver_grain_check["passed"],
            order_fact_check["passed"],
        ]
    )

    report = {
        "run_id": rid,
        "overall_status": "PASS" if all_passed else "FAIL",
        "checks": {
            "revenue_invariant": rev_check,
            "row_count_conservation": row_check,
            "fact_grain_uniqueness": grain_check,
            "foreign_key_completeness": fk_check,
            "scd2_temporal_integrity": scd2_check,
            "silver_current_state_grain": silver_grain_check,
            "order_fact_grain": order_fact_check,
        },
    }

    LOGGER.info("Kết quả Gold reconciliation: %s", report["overall_status"])

    if export_path:
        out_path = Path(export_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        LOGGER.info("Đã xuất báo cáo Reconciliation sang: %s", out_path)

    return report
