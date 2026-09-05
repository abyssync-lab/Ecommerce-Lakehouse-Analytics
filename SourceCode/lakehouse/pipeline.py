"""Module orchestrate toàn bộ Data Lakehouse Pipeline (Bronze -> Silver -> Gold)."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from config import SETTINGS, auto_set_spark_home_env

auto_set_spark_home_env()

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
if SETTINGS.use_local_storage and not os.environ.get("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

from .dimensions import build_all_dimensions
from .ingestion import read_raw_csv
from .marts import build_all_marts, build_fact_sales
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


def run_pipeline(input_path: str | None = None, use_scd2: bool = False) -> SparkSession:
    """Khởi chạy Medallion Lakehouse Pipeline hoàn chỉnh.

    Args:
        input_path: Đường dẫn file CSV đầu vào tùy chọn.
        use_scd2: Bật mô hình hóa SCD Type 2 cho bảng dim_customer.

    Returns:
        SparkSession đã hoàn thành xử lý.
    """
    LOGGER.info("=====================================================")
    LOGGER.info("BẮT ĐẦU GLOBAL CART INTELLIGENCE LAKEHOUSE PIPELINE")
    LOGGER.info("=====================================================")

    spark = create_spark_session()

    # 1. RAW INGESTION
    raw_df = read_raw_csv(spark, input_path)

    # 2. BRONZE LAYER
    LOGGER.info("--- 2. BRONZE LAYER ---")
    save_and_verify_delta(raw_df, SETTINGS.bronze_delta, "bronze.ecommerce_raw")

    # 3. SILVER LAYER & DATA QUALITY GATE WITH QUARANTINE
    LOGGER.info("--- 3. SILVER LAYER & QUARANTINE ---")
    quarantine_path = SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
    clean_df = clean_and_enrich_silver(raw_df, quarantine_path=quarantine_path)
    save_and_verify_delta(clean_df, SETTINGS.silver_delta, "silver.ecommerce_clean")

    # 4. GOLD LAYER - STAR SCHEMA (KIMBALL)
    LOGGER.info("--- 4. GOLD LAYER - STAR SCHEMA (SCD2=%s) ---", use_scd2)
    dimensions = build_all_dimensions(spark, clean_df, use_scd2=use_scd2)
    fact_sales = build_fact_sales(clean_df, dimensions)

    star_schema_tables = {**dimensions, "fact_sales": fact_sales}
    persist_tables(spark, star_schema_tables, SETTINGS.gold_star_schema_base, "gold_star")

    # 5. GOLD LAYER - MARTS
    LOGGER.info("--- 5. GOLD LAYER - MARTS ---")
    gold_marts = build_all_marts(clean_df)
    persist_tables(spark, gold_marts, SETTINGS.gold_marts_base, "gold_mart")

    # 6. DELTA LAKEHOUSE FEATURES DEMO
    show_delta_history(spark, SETTINGS.bronze_delta, "BRONZE")
    show_delta_history(spark, SETTINGS.silver_delta, "SILVER")

    if SETTINGS.run_delta_demo:
        check_schema_enforcement(spark, SETTINGS.silver_delta)
        check_versioning_and_time_travel(spark, clean_df)

    LOGGER.info("=====================================================")
    LOGGER.info("HOÀN THÀNH DATA LAKEHOUSE PIPELINE THÀNH CÔNG!")
    LOGGER.info("=====================================================")

    if SETTINGS.wait_before_exit:
        input("\nNhấn Enter để đóng Spark UI và kết thúc...")

    clean_df.unpersist()
    return spark


def run_incremental_pipeline(new_batch_df: Any, spark: SparkSession | None = None) -> SparkSession:
    """Thực thi Incremental Ingestion & Delta MERGE INTO từ Bronze -> Silver -> Gold."""
    LOGGER.info("--- THỰC THI INCREMENTAL PIPELINE WITH DELTA MERGE ---")
    if spark is None:
        spark = create_spark_session()

    # 1. Incremental Bronze Append
    bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
    new_batch_df.write.format("delta").mode("append").save(bronze_path)
    LOGGER.info("Đã append %d dòng bản ghi mới vào Bronze Delta table.", new_batch_df.count())

    # 2. Clean & Deduplicate Silver Batch
    quarantine_path = SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
    clean_batch = clean_and_enrich_silver(new_batch_df, quarantine_path=quarantine_path)

    # 3. Delta MERGE INTO Silver
    silver_path = SETTINGS.get_storage_path(SETTINGS.silver_delta)
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_delta_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_delta_table.alias("target")
            .merge(
                clean_batch.alias("source"),
                "target.Order_ID = source.Order_ID AND target.Product_Name = source.Product_Name",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        LOGGER.info("Đã hoàn tất Delta MERGE INTO tầng Silver.")
    else:
        save_and_verify_delta(clean_batch, SETTINGS.silver_delta, "silver.ecommerce_clean")

    # 4. Re-aggregate Full Silver for Gold Marts
    full_silver = spark.read.format("delta").load(silver_path)
    dimensions = build_all_dimensions(spark, full_silver)
    fact_sales = build_fact_sales(full_silver, dimensions)
    persist_tables(spark, {**dimensions, "fact_sales": fact_sales}, SETTINGS.gold_star_schema_base, "gold_star")

    gold_marts = build_all_marts(full_silver)
    persist_tables(spark, gold_marts, SETTINGS.gold_marts_base, "gold_mart")

    LOGGER.info("--- THÀNH CÔNG: INCREMENTAL PIPELINE HOÀN TẤT ---")
    return spark

