"""Module quản lý lưu trữ Delta Lake, Hive Metastore Registration và kiểm tra tính năng Lakehouse."""

from __future__ import annotations

import logging
from typing import Any

from config import SETTINGS

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

LOGGER = logging.getLogger(__name__)


def resolve_path(relative_path: str) -> str:
    """Ghép đường dẫn lưu trữ đầy đủ (Local File URL hoặc HDFS URL)."""
    if relative_path.startswith(("file://", "hdfs://")):
        return relative_path
    return SETTINGS.get_storage_path(relative_path)


def save_delta(dataframe: Any, path: str, mode: str = "overwrite") -> None:
    """Lưu DataFrame thành Delta Table với đầy đủ Transaction Log (_delta_log)."""
    target_url = resolve_path(path)
    writer = dataframe.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(target_url)


def check_delta_log_exists(spark: Any, path: str) -> bool:
    """Kiểm tra sự tồn tại của thư mục `_delta_log`."""
    delta_log_path = resolve_path(path.rstrip("/") + "/_delta_log")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(delta_log_path)
    return fs.exists(jvm_path)


def save_and_verify_delta(dataframe: Any, path: str, table_label: str) -> None:
    """Ghi Delta Table và xác nhận transaction log `_delta_log` đã được tạo thành công."""
    save_delta(dataframe, path)
    if not check_delta_log_exists(dataframe.sparkSession, path):
        raise RuntimeError(f"{table_label} tại {path} không tạo được _delta_log!")
    LOGGER.info("Xác nhận ghi Delta Table thành công: %s tại %s", table_label, path)


def register_hive_table(spark: Any, path: str, table_name: str) -> None:
    """Đăng ký Delta Table vào Hive Metastore để phục vụ Thrift Server / Power BI."""
    hive_db = SETTINGS.hive_database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {hive_db}.{table_name}
        USING DELTA
        LOCATION '{resolve_path(path)}'
    """)
    LOGGER.info("Đã đăng ký Hive table: %s.%s", hive_db, table_name)


def persist_tables(spark: Any, tables: dict[str, Any], base_path: str, layer_name: str) -> None:
    """Lưu và đăng ký một nhóm bảng Delta theo chuẩn chung."""
    for table_name, dataframe in tables.items():
        delta_path = f"{base_path}/{table_name}_delta"
        save_and_verify_delta(dataframe, delta_path, f"{layer_name}.{table_name}")
        register_hive_table(spark, delta_path, table_name)


def show_delta_history(spark: Any, path: str, table_name: str) -> None:
    """Hiển thị lịch sử phiên bản (Version History) của Delta Table."""
    if DeltaTable is None:
        LOGGER.warning("Thư viện delta-spark chưa được nạp, bỏ qua hiển thị Delta History.")
        return
    LOGGER.info("=== DELTA HISTORY: %s ===", table_name)
    delta_table = DeltaTable.forPath(spark, resolve_path(path))
    delta_table.history().show(truncate=False)


def check_schema_enforcement(spark: Any, silver_path: str) -> None:
    """Thử nghiệm tính năng Schema Enforcement của Delta Lake."""
    LOGGER.info("=== KIỂM TRA SCHEMA ENFORCEMENT ===")
    wrong_schema_df = spark.createDataFrame(
        [("BAD_ORDER_001", "SAI_SCHEMA")], ["Order_ID", "Revenue"]
    )
    try:
        wrong_schema_df.write.format("delta").mode("append").save(resolve_path(silver_path))
        LOGGER.warning("CẢNH BÁO: Dữ liệu sai schema đã ghi được. Cần kiểm tra lại cấu hình Delta.")
    except Exception as e:
        LOGGER.info("THÀNH CÔNG: Delta Lake đã từ chối ghi dữ liệu sai schema (%s)", str(e)[:200])


def check_versioning_and_time_travel(spark: Any, clean_df: Any) -> None:
    """Thử nghiệm tính năng Versioning & Time Travel của Delta Lake."""
    LOGGER.info("=== KIỂM TRA VERSIONING VÀ TIME TRAVEL ===")
    version_path = SETTINGS.lakehouse_version_delta

    save_delta(clean_df.limit(50), version_path, mode="overwrite")
    clean_df.limit(10).write.format("delta").mode("append").save(resolve_path(version_path))

    version_0_df = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(resolve_path(version_path))
    )
    latest_df = spark.read.format("delta").load(resolve_path(version_path))

    LOGGER.info("Số dòng version 0: %d | Số dòng version mới nhất: %d", version_0_df.count(), latest_df.count())
