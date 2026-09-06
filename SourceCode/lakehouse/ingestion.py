"""Module nạp dữ liệu thô (Ingestion Layer) cho Lakehouse Pipeline."""

from __future__ import annotations

import hashlib
import logging
import uuid
from pathlib import Path
from typing import Any

from config import SETTINGS
from data_quality import validate_columns

try:
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        input_file_name,
        lit,
        sha2,
        struct,
        to_json,
    )
except ImportError:
    col = current_timestamp = input_file_name = lit = sha2 = struct = to_json = None  # type: ignore

from .contracts.loader import get_spark_raw_schema, load_contract
from .registry import BatchRegistry

LOGGER = logging.getLogger(__name__)
PIPELINE_VERSION = "1.0.0"


def calculate_source_hash(file_path: str | None) -> str:
    """Tính mã băm SHA-256 của file nguồn đầu vào để phục vụ kiểm soát Idempotency.

    Args:
        file_path: Đường dẫn tới file nguồn (POSIX, file:// hoặc URI).

    Returns:
        Chuỗi băm SHA-256 hex digest.
    """
    if not file_path:
        return hashlib.sha256(b"default_stream_source").hexdigest()

    clean_path = file_path.replace("file:///", "").replace("file://", "")
    p = Path(clean_path)
    if p.exists() and p.is_file():
        hasher = hashlib.sha256()
        with open(p, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
    return hashlib.sha256(file_path.encode("utf-8")).hexdigest()


def validate_raw_schema(raw_df: Any) -> None:
    """Kiểm tra và dừng sớm nếu dữ liệu đầu vào không đủ các cột theo Data Contract."""
    result = validate_columns(raw_df.columns)
    if not result.passed:
        raise ValueError(f"Dữ liệu thô không đạt Data Contract đầu vào: {result.detail}")


def enrich_with_ingestion_metadata(
    raw_df: Any,
    batch_id: str | None = None,
    source_system: str = "ecommerce_csv",
    source_hash: str | None = None,
) -> Any:
    """Bổ sung các cột siêu dữ liệu (Ingestion Metadata) phục vụ truy vết, lineage và replay.

    Các cột bổ sung:
    - `_ingested_at`: Thời điểm nạp bản ghi vào Bronze Delta Table
    - `_source_file`: Đường dẫn file CSV nguồn gốc
    - `_batch_id`: Mã nhận diện batch / run ingestion
    - `_source_system`: Hệ thống nguồn phát sinh dữ liệu
    - `_source_hash`: Mã băm SHA-256 của file nguồn để kiểm tra idempotency
    - `_record_hash`: SHA-256 của JSON struct các trường nghiệp vụ (bảo đảm thứ tự & không xung đột delimiter)
    - `_pipeline_version`: Phiên bản pipeline nạp dữ liệu
    """
    bid = batch_id or str(uuid.uuid4())[:8]
    shash = source_hash or "hash_unspecified"
    contract = load_contract()

    # Sắp xếp cố định các cột nghiệp vụ để mã băm _record_hash hoàn toàn tất định
    biz_cols = [c for c in contract.business_column_names if c in raw_df.columns]
    if not biz_cols:
        biz_cols = sorted([c for c in raw_df.columns if not c.startswith("_")])

    record_struct = struct(*[col(c) for c in biz_cols])

    return (
        raw_df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(bid))
        .withColumn("_source_system", lit(source_system))
        .withColumn("_source_hash", lit(shash))
        .withColumn("_pipeline_version", lit(PIPELINE_VERSION))
        .withColumn("_record_hash", sha2(to_json(record_struct), 256))
    )


def record_ingestion_batch(
    spark: Any,
    batch_id: str,
    source_hash: str,
    row_count: int,
    status: str = "SUCCESS",
    run_id: str | None = None,
) -> None:
    """Lưu vết metadata batch nạp vào Batch Registry (Audit Trail & Idempotency Control)."""
    registry = BatchRegistry(spark)
    if status == "SUCCESS":
        registry.mark_batch_success(
            run_id=run_id or f"run_{batch_id}",
            batch_id=batch_id,
            row_count=row_count,
        )
    else:
        registry.mark_batch_failed(
            run_id=run_id or f"run_{batch_id}",
            batch_id=batch_id,
            error_message=f"Batch finished with status {status}",
        )


def read_raw_csv(spark: Any, input_path: str | None = None) -> Any:
    """Đọc dữ liệu CSV thô với schema rõ ràng (tránh inferSchema=True gây schema drift).

    Args:
        spark: SparkSession active.
        input_path: Đường dẫn file CSV (mặc định lấy từ SETTINGS).

    Returns:
        DataFrame PySpark thô.
    """
    target_path = input_path or SETTINGS.get_input_path()
    LOGGER.info("Bắt đầu đọc dữ liệu CSV thô từ: %s", target_path)

    raw_schema = get_spark_raw_schema()
    reader = spark.read.option("header", True)
    if raw_schema is not None:
        reader = reader.schema(raw_schema)
    else:
        reader = reader.option("inferSchema", False)

    raw_df = reader.csv(target_path)
    validate_raw_schema(raw_df)
    raw_count = raw_df.count()
    LOGGER.info("Đã đọc xong dữ liệu thô, tổng số dòng: %,d", raw_count)

    return raw_df


def ingest_to_bronze(
    spark: Any,
    input_path: str | None = None,
    mode: str = "overwrite",
    batch_id: str | None = None,
    run_id: str | None = None,
) -> Any:
    """Nạp dữ liệu từ Landing / Source CSV vào tầng Bronze Delta với kiểm soát Idempotency.

    Args:
        spark: SparkSession active.
        input_path: Đường dẫn file CSV nguồn.
        mode: 'overwrite' (Bootstrap reset) hoặc 'append' (Incremental pipeline).
        batch_id: Mã nhận diện batch nạp.
        run_id: Mã nhận diện lần chạy pipeline.

    Returns:
        DataFrame tầng Bronze Delta đã được lưu trữ an toàn.
    """
    from .storage import save_and_verify_delta

    target_path = input_path or SETTINGS.get_input_path()
    source_hash = calculate_source_hash(target_path)
    bid = batch_id or f"batch_{uuid.uuid4().hex[:8]}"
    rid = run_id or f"run_{uuid.uuid4().hex[:8]}"

    registry = BatchRegistry(spark)

    # Chặn nạp trùng lặp nếu batch hash này đã từng nạp thành công (Chỉ áp dụng cho chế độ append)
    if mode == "append" and registry.is_batch_processed(source_hash):
        LOGGER.warning(
            "Batch %s (Source Hash: %s) ĐÃ ĐƯỢC XỬ LÝ THÀNH CÔNG TRƯỚC ĐÓ. "
            "Bỏ qua append để bảo đảm tính lũy đẳng (Replay-Safe).",
            bid,
            source_hash[:10],
        )
        bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
        return spark.read.format("delta").load(bronze_path)

    raw_df = read_raw_csv(spark, target_path)
    raw_count = raw_df.count()

    registry.register_batch_start(
        run_id=rid,
        batch_id=bid,
        source_uri=target_path,
        source_hash=source_hash,
        row_count=raw_count,
    )

    bronze_df = enrich_with_ingestion_metadata(raw_df, batch_id=bid, source_hash=source_hash)
    save_and_verify_delta(bronze_df, SETTINGS.bronze_delta, "bronze.ecommerce_raw", mode=mode)
    record_ingestion_batch(spark, bid, source_hash, raw_count, "SUCCESS", run_id=rid)
    LOGGER.info("Đã hoàn tất Ingestion tầng Bronze Delta (chế độ: %s, Batch: %s).", mode, bid)
    return bronze_df
