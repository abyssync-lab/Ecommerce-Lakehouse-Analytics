"""Module orchestrate toàn bộ Data Lakehouse Pipeline (Bronze -> Silver -> Gold)."""

from __future__ import annotations

import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from config import SETTINGS, PipelineConfig, auto_set_spark_home_env

auto_set_spark_home_env()

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
if SETTINGS.use_local_storage and not os.environ.get("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

from .dimensions import build_all_dimensions
from .ingestion import enrich_with_ingestion_metadata, ingest_to_bronze
from .marts import build_all_marts, build_fact_sales, build_sales_enriched
from .reconciliation import run_full_reconciliation
from .registry import BatchRegistry
from .silver import clean_and_enrich_silver
from .storage import (
    check_schema_enforcement,
    check_versioning_and_time_travel,
    persist_tables,
    save_and_verify_delta,
    show_delta_history,
)

try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
except ImportError as error:
    raise ImportError(
        "\nChưa cài đặt Delta Lake.\nHãy chạy lệnh:\npython -m pip install delta-spark\n"
    ) from error

LOGGER = logging.getLogger(__name__)


@dataclass
class PipelineRunResult:
    """Kết quả hoàn chỉnh của một lượt thực thi Pipeline Lakehouse."""

    run_id: str
    batch_id: str
    status: str  # "SUCCESS", "FAILED", "SKIPPED"
    bronze_rows: int
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int
    reconciliation_passed: bool
    reconciliation_report: dict[str, Any] = field(default_factory=dict)
    certified_gold_version: int | None = None
    error_message: str | None = None
    spark: SparkSession | None = None


class PipelineCertificationError(Exception):
    """Ngoại lệ phát sinh khi Pipeline không vượt qua cổng kiểm toán Reconciliation Gate."""

    pass


def create_spark_session() -> SparkSession:
    """Khởi tạo và cấu hình SparkSession tích hợp Delta Lake & Hive Metastore."""
    driver_host = SETTINGS.spark_driver_host or ("127.0.0.1" if SETTINGS.use_local_storage else None)
    driver_bind = SETTINGS.spark_driver_bind_address or ("127.0.0.1" if SETTINGS.use_local_storage else None)

    builder = (
        SparkSession.builder.appName("Global Cart Intelligence Data Lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    if driver_bind:
        builder = builder.config("spark.driver.bindAddress", driver_bind)

    if not SETTINGS.use_local_storage:
        builder = builder.config("spark.hadoop.fs.defaultFS", SETTINGS.hdfs_base)

    try:
        builder = builder.enableHiveSupport()
    except Exception:
        LOGGER.warning(
            "Không thể bật Hive Support, sẽ chạy Spark local mode không có Hive metastore."
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_delta_demo(spark: SparkSession) -> None:
    """Chạy các bài thử nghiệm minh họa tính năng Delta Lake (Schema Enforcement, Time Travel, History)."""
    LOGGER.info("=== BẮT ĐẦU DELTA LAKEHOUSE DEMONSTRATION ===")
    show_delta_history(spark, SETTINGS.bronze_delta, "BRONZE")
    show_delta_history(spark, SETTINGS.silver_delta, "SILVER")
    check_schema_enforcement(spark)

    silver_path = SETTINGS.get_storage_path(SETTINGS.silver_delta)
    clean_df = spark.read.format("delta").load(silver_path)
    check_versioning_and_time_travel(spark, clean_df)
    LOGGER.info("=== HOÀN TẤT DELTA LAKEHOUSE DEMONSTRATION ===")


def run_pipeline(
    input_path: str | None = None,
    use_scd2: bool | None = None,
    config: PipelineConfig | None = None,
) -> PipelineRunResult:
    """Khởi chạy Medallion Lakehouse Pipeline hoàn chỉnh (Bootstrap Full Mode).

    Quy trình chuẩn hóa Enterprise Lifecycle:
    1. Ingestion: Xác thực mã băm, kiểm tra sổ cái Batch Registry
    2. Bronze: Lưu trữ dữ liệu thô bất biến
    3. Silver: Ép kiểu, làm sạch, tách Quarantine theo luật hợp đồng
    4. Gold: Xây dựng Kimball Star Schema (FactSales, Dimensions SCD2)
    5. Gold Semantic & Marts: Sinh Canonical Semantic Dataset và Data Marts
    6. Reconciliation Gate: Kiểm toán các bất biến bắt buộc (Doanh thu, Row conservation, Grain, FK, SCD2)
    7. Certification: Ghi nhận chứng nhận vào sổ cái nếu PASS, chặn đứng nếu FAIL.

    Args:
        input_path: Đường dẫn file CSV đầu vào tùy chọn.
        use_scd2: Bật mô hình hóa SCD Type 2 cho bảng dim_customer (ưu tiên hơn config/SETTINGS).
        config: Đối tượng PipelineConfig tập trung nếu có.

    Returns:
        PipelineRunResult đại diện cho trạng thái và số liệu của lượt chạy.
    """
    effective_scd2 = (
        use_scd2
        if use_scd2 is not None
        else (config.use_scd2 if config else SETTINGS.use_scd2)
    )
    effective_input = (
        input_path
        if input_path is not None
        else (config.input_path if config else None)
    )

    run_id = (config.run_id if config and config.run_id else f"run_{uuid.uuid4().hex[:8]}")
    batch_id = (config.batch_id if config and config.batch_id else f"batch_{uuid.uuid4().hex[:8]}")

    LOGGER.info("=====================================================")
    LOGGER.info("BẮT ĐẦU GLOBAL CART INTELLIGENCE LAKEHOUSE PIPELINE")
    LOGGER.info("Mode: BOOTSTRAP | Run ID: %s | Batch ID: %s | SCD2: %s", run_id, batch_id, effective_scd2)
    LOGGER.info("=====================================================")

    spark = create_spark_session()
    registry = BatchRegistry(spark)

    # 1. RAW INGESTION & 2. BRONZE LAYER (Append-only Raw Delta + Ingestion Metadata)
    LOGGER.info("--- 1. INGESTION & 2. BRONZE LAYER ---")
    bronze_df = ingest_to_bronze(spark, effective_input, mode="overwrite", batch_id=batch_id, run_id=run_id)
    bronze_rows = bronze_df.count()

    # 3. SILVER LAYER & DATA QUALITY GATE WITH QUARANTINE
    LOGGER.info("--- 3. SILVER LAYER & QUARANTINE ---")
    quarantine_path = (
        config.quarantine_path
        if config and config.quarantine_path
        else SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
    )
    clean_df = clean_and_enrich_silver(
        bronze_df, quarantine_path=quarantine_path, run_id=run_id, batch_id=batch_id
    )
    silver_rows = clean_df.count()

    # Kế toán dòng: Raw = Clean + Quarantine + Duplicate
    duplicate_rows = bronze_rows - bronze_df.dropDuplicates().count()
    quarantine_rows = max(0, bronze_rows - silver_rows - duplicate_rows)

    save_and_verify_delta(clean_df, SETTINGS.silver_delta, "silver.ecommerce_clean", mode="overwrite")

    # 4. GOLD LAYER - STAR SCHEMA (KIMBALL)
    LOGGER.info("--- 4. GOLD LAYER - STAR SCHEMA (SCD2=%s) ---", effective_scd2)
    dimensions = build_all_dimensions(spark, clean_df, use_scd2=effective_scd2)
    fact_sales = build_fact_sales(clean_df, dimensions)

    star_schema_tables = {**dimensions, "fact_sales": fact_sales}
    persist_tables(spark, star_schema_tables, SETTINGS.gold_star_schema_base, "gold_star")

    # 5. GOLD LAYER - CANONICAL SEMANTIC BASE & MARTS
    LOGGER.info("--- 5. GOLD LAYER - CANONICAL SEMANTIC BASE & MARTS ---")
    sales_enriched = build_sales_enriched(fact_sales, dimensions)
    persist_tables(spark, {"gold_sales_enriched": sales_enriched}, f"{SETTINGS.gold_star_schema_base}/semantic", "gold_semantic")

    gold_marts = build_all_marts(sales_enriched)
    persist_tables(spark, gold_marts, SETTINGS.gold_marts_base, "gold_mart")

    # 6. DATA RECONCILIATION GATE (Kiểm toán bắt buộc trước khi chứng nhận)
    LOGGER.info("--- 6. DATA RECONCILIATION & CERTIFICATION GATE ---")
    recon_report = run_full_reconciliation(
        clean_df=clean_df,
        fact_sales=fact_sales,
        mart_overview=gold_marts["mart_overview"],
        dim_customer=dimensions["dim_customer"],
        raw_count=bronze_rows,
        duplicate_count=duplicate_rows,
        invalid_count=quarantine_rows,
        run_id=run_id,
    )

    reconciliation_passed = (recon_report.get("overall_status") == "PASS")

    if not reconciliation_passed:
        registry.mark_batch_failed(
            run_id=run_id,
            batch_id=batch_id,
            error_message="Pipeline không đạt tiêu chuẩn Reconciliation Gate.",
        )
        raise PipelineCertificationError(
            f"Pipeline không đạt chứng nhận Reconciliation Gate: {recon_report}"
        )

    # 7. CERTIFY RUN
    registry.mark_batch_success(
        run_id=run_id,
        batch_id=batch_id,
        row_count=bronze_rows,
        certified_gold_version=1,
    )

    # 8. DEMONSTRATION FEATURES (chỉ chạy khi có cờ cấu hình bật)
    run_demo = config.run_delta_demo if config else SETTINGS.run_delta_demo
    if run_demo:
        run_delta_demo(spark)

    LOGGER.info("=====================================================")
    LOGGER.info("HOÀN THÀNH DATA LAKEHOUSE PIPELINE THÀNH CÔNG (CERTIFIED PASS)!")
    LOGGER.info("=====================================================")

    clean_df.unpersist()
    return PipelineRunResult(
        run_id=run_id,
        batch_id=batch_id,
        status="SUCCESS",
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        duplicate_rows=duplicate_rows,
        reconciliation_passed=True,
        reconciliation_report=recon_report,
        certified_gold_version=1,
        spark=spark,
    )


def run_incremental_pipeline(
    new_batch_df: Any,
    spark: SparkSession | None = None,
    batch_id: str | None = None,
    use_scd2: bool | None = None,
    config: PipelineConfig | None = None,
    source_hash: str | None = None,
) -> PipelineRunResult:
    """Thực thi Incremental Ingestion & Delta MERGE INTO từ Bronze -> Silver -> Gold.

    Quy trình:
    1. Kiểm tra Batch Registry: Bỏ qua nếu batch hash đã xử lý thành công (Idempotency)
    2. Bronze: Append dữ liệu mới kèm Ingestion Metadata
    3. Silver: Làm sạch, lọc Quarantine và MERGE INTO theo stable Order_Line_ID grain
    4. Gold: Cập nhật Star Schema và deterministic refresh Gold Marts
    5. Reconciliation Gate: Kiểm toán các bất biến bắt buộc trước khi chứng nhận.
    """
    effective_scd2 = (
        use_scd2
        if use_scd2 is not None
        else (config.use_scd2 if config else SETTINGS.use_scd2)
    )
    bid = (
        batch_id
        if batch_id is not None
        else (config.batch_id if config and config.batch_id else f"inc_batch_{uuid.uuid4().hex[:8]}")
    )
    run_id = (config.run_id if config and config.run_id else f"run_{uuid.uuid4().hex[:8]}")

    LOGGER.info("=====================================================")
    LOGGER.info("THỰC THI INCREMENTAL PIPELINE WITH DELTA MERGE")
    LOGGER.info("Run ID: %s | Batch ID: %s | SCD2: %s", run_id, bid, effective_scd2)
    LOGGER.info("=====================================================")

    if spark is None:
        spark = create_spark_session()

    registry = BatchRegistry(spark)

    # 1. Idempotency Guard: Nếu batch hash đã được nạp thành công, skip an toàn
    shash = source_hash or f"inc_hash_{bid}"
    if registry.is_batch_processed(shash):
        LOGGER.warning("Batch %s (Hash: %s) ĐÃ XỬ LÝ THÀNH CÔNG TRƯỚC ĐÓ. Bỏ qua.", bid, shash[:10])
        return PipelineRunResult(
            run_id=run_id,
            batch_id=bid,
            status="SKIPPED",
            bronze_rows=0,
            silver_rows=0,
            quarantine_rows=0,
            duplicate_rows=0,
            reconciliation_passed=True,
            spark=spark,
        )

    raw_count = new_batch_df.count()
    registry.register_batch_start(
        run_id=run_id,
        batch_id=bid,
        source_uri="incremental_dataframe",
        source_hash=shash,
        row_count=raw_count,
    )

    # 2. Incremental Bronze Append kèm metadata
    enriched_batch = enrich_with_ingestion_metadata(new_batch_df, batch_id=bid, source_hash=shash)
    bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
    enriched_batch.write.format("delta").mode("append").save(bronze_path)
    LOGGER.info("Đã append %d dòng bản ghi mới vào Bronze Delta table (Batch: %s).", raw_count, bid)

    # 3. Clean & Deduplicate Silver Batch
    quarantine_path = (
        config.quarantine_path
        if config and config.quarantine_path
        else SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
    )
    clean_batch = clean_and_enrich_silver(
        enriched_batch, quarantine_path=quarantine_path, run_id=run_id, batch_id=bid
    )

    # 4. Delta MERGE INTO Silver theo stable line grain (Order_ID + Order_Line_ID)
    silver_path = SETTINGS.get_storage_path(SETTINGS.silver_delta)
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_delta_table = DeltaTable.forPath(spark, silver_path)
        existing_cols = silver_delta_table.toDF().columns

        if "Order_Line_ID" in clean_batch.columns and "Order_Line_ID" in existing_cols:
            merge_cond = "target.Order_ID = source.Order_ID AND target.Order_Line_ID = source.Order_Line_ID"
        else:
            merge_cond = "target.Order_ID = source.Order_ID AND target.Product_Name = source.Product_Name"

        LOGGER.info("Thực thi Delta MERGE INTO Silver với điều kiện: %s", merge_cond)
        (
            silver_delta_table.alias("target")
            .merge(clean_batch.alias("source"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        LOGGER.info("Đã hoàn tất Delta MERGE INTO tầng Silver.")
    else:
        save_and_verify_delta(clean_batch, SETTINGS.silver_delta, "silver.ecommerce_clean", mode="append")

    # 5. Refresh Gold Core & Marts từ Silver cập nhật (Deterministic Gold Refresh, bảo toàn SCD2)
    LOGGER.info("--- 5. REFRESH GOLD CORE & MARTS TỪ SILVER (SCD2=%s) ---", effective_scd2)
    full_silver = spark.read.format("delta").load(silver_path)
    dimensions = build_all_dimensions(spark, full_silver, use_scd2=effective_scd2)
    fact_sales = build_fact_sales(full_silver, dimensions)
    persist_tables(spark, {**dimensions, "fact_sales": fact_sales}, SETTINGS.gold_star_schema_base, "gold_star")

    sales_enriched = build_sales_enriched(fact_sales, dimensions)
    persist_tables(spark, {"gold_sales_enriched": sales_enriched}, f"{SETTINGS.gold_star_schema_base}/semantic", "gold_semantic")

    gold_marts = build_all_marts(sales_enriched)
    persist_tables(spark, gold_marts, SETTINGS.gold_marts_base, "gold_mart")

    # 6. RECONCILIATION GATE
    full_bronze = spark.read.format("delta").load(bronze_path)
    total_bronze_rows = full_bronze.count()
    total_silver_rows = full_silver.count()
    total_duplicate = total_bronze_rows - full_bronze.dropDuplicates().count()
    total_quarantine = max(0, total_bronze_rows - total_silver_rows - total_duplicate)

    recon_report = run_full_reconciliation(
        clean_df=full_silver,
        fact_sales=fact_sales,
        mart_overview=gold_marts["mart_overview"],
        dim_customer=dimensions["dim_customer"],
        raw_count=total_bronze_rows,
        duplicate_count=total_duplicate,
        invalid_count=total_quarantine,
        run_id=run_id,
    )

    if recon_report.get("overall_status") != "PASS":
        registry.mark_batch_failed(
            run_id=run_id,
            batch_id=bid,
            error_message="Incremental Reconciliation gate failed",
        )
        raise PipelineCertificationError(
            f"Incremental Pipeline không đạt chứng nhận Reconciliation Gate: {recon_report}"
        )

    registry.mark_batch_success(
        run_id=run_id,
        batch_id=bid,
        row_count=raw_count,
        certified_gold_version=2,
    )

    LOGGER.info("--- THÀNH CÔNG: INCREMENTAL PIPELINE HOÀN TẤT VÀ ĐƯỢC CHỨNG NHẬN ---")
    return PipelineRunResult(
        run_id=run_id,
        batch_id=bid,
        status="SUCCESS",
        bronze_rows=raw_count,
        silver_rows=clean_batch.count(),
        quarantine_rows=total_quarantine,
        duplicate_rows=total_duplicate,
        reconciliation_passed=True,
        reconciliation_report=recon_report,
        certified_gold_version=2,
        spark=spark,
    )
