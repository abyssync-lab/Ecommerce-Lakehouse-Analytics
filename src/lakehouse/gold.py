"""Các bước dựng Gold dùng chung cho bootstrap và incremental."""

from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from config import SETTINGS

from .dimensions import build_all_dimensions
from .marts import (
    build_fact_order_fulfillment,
    build_fact_sales,
    build_gold_marts,
    build_sales_enriched,
)
from .publication import persist_gold_staging, publish_gold_run
from .reconciliation import ReconciliationError, run_full_reconciliation
from .registry import BatchRegistry
from .silver import clean_and_enrich_silver

LOGGER = logging.getLogger(__name__)


def customer_history_source(spark: SparkSession, fallback_df: Any, use_scd2: bool) -> Any | None:
    """Chuẩn hóa lại Bronze history để SCD2 không mất late customer events.

    Silver current-state chỉ giữ version thắng cuối cùng, nên không đủ để dựng
    khoảng thời gian SCD2. Bronze là nguồn audit duy nhất chứa toàn bộ event; các
    dòng lỗi được lọc bằng cùng Silver quality rules nhưng không ghi thêm metrics
    hoặc quarantine trong lúc rebuild dimension.
    """
    if not use_scd2:
        return None
    bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return fallback_df
    bronze_history = spark.read.format("delta").load(bronze_path)
    history = clean_and_enrich_silver(
        bronze_history,
        quarantine_path=None,
        run_id="scd2_history_rebuild",
        batch_id="scd2_history_rebuild",
        allow_line_id_fallback=True,
    )
    if "Operation" in history.columns:
        history = history.filter(col("Operation") != "DELETE")
    if "Customer_ID" not in history.columns:
        return fallback_df
    history = history.filter(col("Customer_ID").isNotNull())
    for column in ("Customer_Gender", "Customer_Segment"):
        if column not in history.columns:
            history = history.withColumn(column, lit("Unknown"))
    if "Order_Date" not in history.columns:
        history = history.withColumn("Order_Date", lit(None).cast("date"))
    if "_record_hash" in history.columns:
        history = history.dropDuplicates(["_record_hash"])
    return history


def build_and_publish_gold(
    spark: SparkSession,
    active_silver: Any,
    silver_orders_current: Any,
    silver_order_lines_current: Any,
    effective_scd2: bool,
    customer_history_source: Any,
    run_id: str,
    registry: BatchRegistry,
    raw_count: int,
    duplicate_count: int,
    invalid_count: int,
    valid_count: int,
    published_version: str = "1",
) -> dict[str, Any]:
    """Dựng Star Schema, Semantic Marts, chạy đối soát và publish serving snapshot."""
    LOGGER.info("--- GOLD LAYER - STAR SCHEMA (SCD2=%s) ---", effective_scd2)
    dimensions = build_all_dimensions(
        spark,
        active_silver,
        use_scd2=effective_scd2,
        customer_history_df=customer_history_source,
    )
    fact_sales = build_fact_sales(active_silver, dimensions)
    fact_order_fulfillment = build_fact_order_fulfillment(
        silver_orders_current,
        silver_order_lines_current,
        dimensions,
    )
    persist_gold_staging(
        spark,
        {
            **dimensions,
            "fact_sales_line": fact_sales,
            "fact_order_fulfillment": fact_order_fulfillment,
        },
        run_id,
        "gold_star",
    )

    LOGGER.info("--- GOLD LAYER - CANONICAL SEMANTIC BASE & MARTS ---")
    sales_enriched = build_sales_enriched(fact_sales, dimensions)
    persist_gold_staging(
        spark,
        {"gold_sales_enriched": sales_enriched},
        run_id,
        "gold_semantic",
    )
    gold_marts = build_gold_marts(sales_enriched, fact_order_fulfillment)
    persist_gold_staging(spark, gold_marts, run_id, "gold_mart")
    registry.update_status(run_id, "GOLD_BUILT", gold_run_id=run_id)

    LOGGER.info("--- GOLD RECONCILIATION ---")
    recon_report = run_full_reconciliation(
        clean_df=active_silver,
        fact_sales=fact_sales,
        mart_overview=gold_marts["mart_executive_daily"],
        dim_customer=dimensions["dim_customer"],
        raw_count=raw_count,
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        run_id=run_id,
        valid_count=valid_count,
        silver_orders_current=silver_orders_current,
        silver_order_lines_current=silver_order_lines_current,
        fact_order_fulfillment=fact_order_fulfillment,
    )
    if recon_report.get("overall_status") != "PASS":
        raise ReconciliationError(f"Gold reconciliation không đạt PASS: {recon_report}")

    registry.mark_reconciled(run_id)
    publish_gold_run(
        spark,
        {
            **dimensions,
            "fact_sales_line": fact_sales,
            "fact_order_fulfillment": fact_order_fulfillment,
            "gold_sales_enriched": sales_enriched,
            **gold_marts,
        },
        run_id,
    )
    try:
        registry.mark_published(run_id, gold_run_id=run_id, published_version=published_version)
    except Exception as finalization_error:
        # Snapshot đã đổi nhưng metadata chưa ghi được: giữ trạng thái riêng
        # để retry không đánh dấu FAILED sai một run đã được Power BI nhìn thấy.
        registry.mark_publish_metadata_pending(run_id, finalization_error)
        raise

    return recon_report
