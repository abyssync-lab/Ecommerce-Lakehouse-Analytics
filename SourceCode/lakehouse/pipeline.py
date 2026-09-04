"""Module orchestrate toàn bộ Data Lakehouse Pipeline (Bronze -> Silver -> Gold)."""

from __future__ import annotations

import logging
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from config import SETTINGS, auto_set_spark_home_env

auto_set_spark_home_env()

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

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
except ImportError as error:
    raise ImportError(
        "\nChưa cài đặt Delta Lake.\nHãy chạy lệnh:\npython -m pip install delta-spark\n"
    ) from error

LOGGER = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Khởi tạo và cấu hình SparkSession tích hợp Delta Lake & Hive Metastore."""
    builder = (
        SparkSession.builder.appName("Global Cart Intelligence Data Lakehouse")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if not SETTINGS.use_local_storage:
        builder = builder.config("spark.hadoop.fs.defaultFS", SETTINGS.hdfs_base)

    try:
        builder = builder.enableHiveSupport()
    except Exception:
        LOGGER.warning(
            "Không thể bật Hive Support, sẽ chạy Spark local mode không có Hive metastore."
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_pipeline(input_path: str | None = None) -> SparkSession:
    """Khởi chạy Medallion Lakehouse Pipeline hoàn chỉnh.

    Args:
        input_path: Đường dẫn file CSV đầu vào tùy chọn.

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

    # 3. SILVER LAYER & DATA QUALITY GATE
    LOGGER.info("--- 3. SILVER LAYER ---")
    clean_df = clean_and_enrich_silver(raw_df)
    save_and_verify_delta(clean_df, SETTINGS.silver_delta, "silver.ecommerce_clean")

    # 4. GOLD LAYER - STAR SCHEMA (KIMBALL)
    LOGGER.info("--- 4. GOLD LAYER - STAR SCHEMA ---")
    dimensions = build_all_dimensions(spark, clean_df)
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
