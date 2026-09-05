"""Module nạp dữ liệu thô (Ingestion Layer) cho Lakehouse Pipeline."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from config import SETTINGS
from data_quality import validate_columns
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    input_file_name,
    lit,
    sha2,
)

LOGGER = logging.getLogger(__name__)


def validate_raw_schema(raw_df: Any) -> None:
    """Kiểm tra và dừng sớm nếu dữ liệu đầu vào không đủ các cột theo Data Contract."""
    result = validate_columns(raw_df.columns)
    if not result.passed:
        raise ValueError(f"Dữ liệu thô không đạt Data Contract đầu vào: {result.detail}")


def enrich_with_ingestion_metadata(
    raw_df: Any, batch_id: str | None = None, source_system: str = "ecommerce_csv"
) -> Any:
    """Bổ sung các cột siêu dữ liệu (Ingestion Metadata) phục vụ truy vết, lineage và replay.

    Các cột bổ sung:
    - `_ingested_at`: Thời điểm nạp bản ghi vào Bronze Delta Table
    - `_source_file`: Đường dẫn file CSV nguồn gốc
    - `_batch_id`: Mã nhận diện batch / run ingestion
    - `_source_system`: Hệ thống nguồn phát sinh dữ liệu
    - `_record_hash`: Mã băm SHA-256 xác thực tính toàn vẹn của bản ghi
    """
    bid = batch_id or str(uuid.uuid4())[:8]
    data_cols = [col(c) for c in raw_df.columns if not c.startswith("_")]

    return (
        raw_df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(bid))
        .withColumn("_source_system", lit(source_system))
        .withColumn("_record_hash", sha2(concat_ws("||", *data_cols), 256))
    )


def read_raw_csv(spark: Any, input_path: str | None = None) -> Any:
    """Đọc dữ liệu CSV thô từ Local Storage hoặc HDFS.

    Args:
        spark: SparkSession active.
        input_path: Đường dẫn file CSV (mặc định lấy từ SETTINGS).

    Returns:
        DataFrame PySpark thô.
    """
    target_path = input_path or SETTINGS.get_input_path()
    LOGGER.info("Bắt đầu đọc dữ liệu CSV thô từ: %s", target_path)

    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(target_path)
    )

    validate_raw_schema(raw_df)
    raw_count = raw_df.count()
    LOGGER.info("Đã đọc xong dữ liệu thô, tổng số dòng: %,d", raw_count)

    return raw_df


def ingest_to_bronze(
    spark: Any,
    input_path: str | None = None,
    mode: str = "overwrite",
    batch_id: str | None = None,
) -> Any:
    """Nạp dữ liệu từ Landing / Source CSV vào tầng Bronze Delta với đầy đủ Ingestion Metadata.

    Args:
        spark: SparkSession active.
        input_path: Đường dẫn file CSV nguồn.
        mode: 'overwrite' (Bootstrap pipeline) hoặc 'append' (Incremental pipeline).
        batch_id: Mã nhận diện batch nạp.

    Returns:
        DataFrame tầng Bronze Delta đã được lưu trữ an toàn.
    """
    from .storage import save_and_verify_delta

    raw_df = read_raw_csv(spark, input_path)
    bronze_df = enrich_with_ingestion_metadata(raw_df, batch_id=batch_id)

    save_and_verify_delta(bronze_df, SETTINGS.bronze_delta, "bronze.ecommerce_raw", mode=mode)
    LOGGER.info("Đã hoàn tất Ingestion tầng Bronze Delta (chế độ: %s).", mode)
    return bronze_df
