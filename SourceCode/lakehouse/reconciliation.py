"""Module đối soát tính toàn vẹn và bất biến dữ liệu (Data Reconciliation Gate).

Thực hiện các kiểm tra bất biến toán học và tính toàn vẹn khóa ngoại xuyên suốt các tầng:
1. Bất biến Doanh thu (Revenue Invariant): Silver == FactSales == Mart Overview
2. Bất biến Số dòng (Row Conservation): Raw == Valid + Invalid + Duplicate
3. Tính Duy nhất của Fact Grain: Distinct(SalesKey) == Count(FactSales)
4. Toàn vẹn Khóa ngoại (FK Completeness): Zero orphan foreign keys
5. Toàn vẹn Chiều SCD2 (SCD2 Temporal Integrity): Không chồng lấn, đúng 1 Is_Current=1 / customer
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

LOGGER = logging.getLogger(__name__)


def reconcile_revenue_invariant(
    clean_df: Any, fact_sales: Any, mart_overview: Any
) -> dict[str, Any]:
    """Kiểm tra Silver certified amount == Fact == Executive overview."""
    certified_policy = "Delivered" if "Delivered_Revenue" in mart_overview.columns else None
    silver_input = (
        clean_df.filter(col("Order_Status") == certified_policy)
        if certified_policy and "Order_Status" in clean_df.columns
        else clean_df
    )
    fact_input = (
        fact_sales.filter(col("Order_Status") == certified_policy)
        if certified_policy and "Order_Status" in fact_sales.columns
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
    """Kiểm tra tính duy nhất của Grain trong FactSales (Mỗi SalesKey đại diện đúng 1 line item)."""
    total_rows = fact_sales.count()
    distinct_keys = fact_sales.select(countDistinct("SalesKey")).collect()[0][0]
    passed = total_rows == distinct_keys

    return {
        "check": "fact_grain_uniqueness",
        "passed": passed,
        "total_fact_rows": total_rows,
        "distinct_sales_keys": distinct_keys,
    }


def reconcile_fk_completeness(fact_sales: Any) -> dict[str, Any]:
    """Kiểm tra toàn vẹn khóa ngoại trong FactSales (Không được có orphan/null foreign keys)."""
    fk_cols = [
        "CustomerKey",
        "ProductKey",
        "DateKey",
        "LocationKey",
        "PaymentKey",
        "ShippingKey",
        "StatusKey",
    ]
    null_counts: dict[str, int] = {}
    for fk in fk_cols:
        if fk in fact_sales.columns:
            cnt = fact_sales.filter(col(fk).isNull()).count()
            null_counts[fk] = cnt

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

    # 3. Bao gồm cả customer có 0 current row; chỉ group các row Is_Current=1
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
    """Thực thi toàn bộ bộ kiểm thử Data Reconciliation Gate và xuất báo cáo JSON.

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
    LOGGER.info("=== BẮT ĐẦU DATA RECONCILIATION GATE (Run ID: %s) ===", rid)

    rev_check = reconcile_revenue_invariant(clean_df, fact_sales, mart_overview)
    row_check = reconcile_row_conservation(
        raw_count=raw_count,
        valid_count=clean_df.count() if valid_count is None else valid_count,
        invalid_count=invalid_count,
        duplicate_count=duplicate_count,
    )
    grain_check = reconcile_fact_grain_uniqueness(fact_sales)
    fk_check = reconcile_fk_completeness(fact_sales)
    scd2_check = reconcile_scd2_temporal_integrity(dim_customer)
    silver_grain_check = reconcile_current_state_grain(
        silver_orders_current, silver_order_lines_current
    )
    order_fact_check = {
        "check": "order_fact_grain",
        "passed": True,
        "note": "Không truyền fact_order_fulfillment; bỏ qua ở API tương thích.",
    }
    if fact_order_fulfillment is not None:
        total_orders = fact_order_fulfillment.count()
        distinct_orders = fact_order_fulfillment.select("OrderKey").distinct().count()
        order_fact_check = {
            "check": "order_fact_grain",
            "passed": total_orders == distinct_orders,
            "total_order_fact_rows": total_orders,
            "distinct_order_keys": distinct_orders,
        }

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

    LOGGER.info("Kết quả Reconciliation Gate: %s", report["overall_status"])

    if export_path:
        out_path = Path(export_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        LOGGER.info("Đã xuất báo cáo Reconciliation sang: %s", out_path)

    return report
