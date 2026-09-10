"""Module nạp dữ liệu thô (Ingestion Layer) cho Lakehouse Pipeline."""

from __future__ import annotations

import hashlib
import logging
import uuid
from pathlib import Path
from typing import Any

from config import SETTINGS

try:
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        lit,
        monotonically_increasing_id,
        sha2,
        struct,
        to_json,
    )
except ImportError:
    col = current_timestamp = input_file_name = lit = sha2 = struct = to_json = None  # type: ignore

from .contracts.loader import load_contract_for_columns
from .file_manifest import FileManifest
from .registry import BatchRegistry

LOGGER = logging.getLogger(__name__)
PIPELINE_VERSION = "1.0.0"


def calculate_source_hash(file_path: str | None) -> str:
    """Tính SHA-256 trên bytes thật của file nguồn để kiểm soát idempotency.

    Args:
        file_path: Đường dẫn tới file nguồn (POSIX, file:// hoặc URI).

    Returns:
        Chuỗi băm SHA-256 dạng hex digest.

    Raises:
        ValueError: Khi không truyền đường dẫn file.
        FileNotFoundError: Khi đường dẫn không trỏ tới một file local có thể đọc.

    Ghi chú:
        Không được dùng filename hoặc URI làm nội dung hash.
    """
    if not file_path:
        raise ValueError("Không thể tính source_hash khi thiếu đường dẫn file nguồn")

    clean_path = file_path.replace("file:///", "").replace("file://", "")
    p = Path(clean_path)
    if not p.is_file():
        raise FileNotFoundError(
            f"Không thể tính source_hash: file nguồn không tồn tại hoặc không phải file: {file_path}"
        )

    hasher = hashlib.sha256()
    with p.open("rb") as source_file:
        while chunk := source_file.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def calculate_source_size(file_path: str | None) -> int:
    """Trả về kích thước bytes của source để audit manifest; URI không local trả về 0."""
    if not file_path:
        return 0
    clean_path = file_path.replace("file:///", "").replace("file://", "")
    path = Path(clean_path)
    return path.stat().st_size if path.exists() and path.is_file() else 0


def validate_raw_schema(raw_df: Any) -> None:
    """Kiểm tra và dừng sớm nếu dữ liệu đầu vào không đủ các cột theo Data Contract."""
    columns = list(raw_df.columns)
    if len(columns) != len(set(columns)):
        raise ValueError("FILE_SCHEMA_MISMATCH: header chứa cột trùng tên")

    v2_markers = {"Order_Line_ID", "Source_Updated_At", "Operation"}
    marker_count = len(v2_markers.intersection(columns))
    if 0 < marker_count < len(v2_markers):
        missing = sorted(v2_markers.difference(columns))
        raise ValueError(
            "FILE_SCHEMA_MISMATCH: file có dấu hiệu contract v2 nhưng thiếu cột: "
            + ", ".join(missing)
        )

    contract = load_contract_for_columns(columns)
    missing = sorted(contract.required_columns.difference(columns))
    if missing:
        raise ValueError(
            "FILE_SCHEMA_MISMATCH: dữ liệu thô thiếu cột bắt buộc: " + ", ".join(missing)
        )


def enrich_with_ingestion_metadata(
    raw_df: Any,
    batch_id: str | None = None,
    source_system: str = "ecommerce_csv",
    source_hash: str | None = None,
    source_uri: str | None = None,
    contract_version: str | None = None,
    run_id: str | None = None,
) -> Any:
    """Bổ sung các cột siêu dữ liệu (Ingestion Metadata) phục vụ truy vết, lineage và replay.

    Các cột bổ sung:
    - `_ingested_at`: Thời điểm nạp bản ghi vào Bronze Delta Table
    - `_source_uri`: Đường dẫn file CSV nguồn gốc
    - `_batch_id`: Mã nhận diện batch / run ingestion
    - `_source_system`: Hệ thống nguồn phát sinh dữ liệu
    - `_source_hash`: Mã băm SHA-256 của file nguồn để kiểm tra idempotency
    - `_source_row_number`: Số thứ tự kỹ thuật của row trong batch (dùng làm tie-breaker)
    - `_record_hash`: SHA-256 của JSON struct toàn bộ trường nghiệp vụ
    - `_contract_version`: Phiên bản contract đã validate
    - `_pipeline_version`: Phiên bản pipeline nạp dữ liệu
    """
    bid = batch_id or str(uuid.uuid4())[:8]
    rid = run_id or ""
    shash = source_hash or "hash_unspecified"
    contract = load_contract_for_columns(raw_df.columns)

    # Hash mọi cột nghiệp vụ thực tế của DataFrame để event v2 không bị bỏ qua
    # Source_Updated_At/Operation khi tạo record fingerprint.
    biz_cols = [c for c in raw_df.columns if not c.startswith("_")]
    if not biz_cols:
        raise ValueError("Không tìm thấy cột nghiệp vụ để tạo _record_hash")

    record_struct = struct(*[col(c) for c in biz_cols])

    return (
        raw_df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_run_id", lit(rid))
        .withColumn("_source_uri", lit(source_uri or ""))
        .withColumn("_source_file", lit(source_uri or ""))
        .withColumn("_batch_id", lit(bid))
        .withColumn("_source_system", lit(source_system))
        .withColumn("_source_hash", lit(shash))
        .withColumn("_source_row_number", monotonically_increasing_id().cast("long"))
        .withColumn("_contract_version", lit(contract_version or contract.version))
        .withColumn("_pipeline_version", lit(PIPELINE_VERSION))
        .withColumn("_record_hash", sha2(to_json(record_struct), 256))
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

    # Đọc header dưới dạng string; kiểu dữ liệu chỉ được cast sau khi qua
    # file-level contract và bootstrap adapter nếu đây là seed lịch sử.
    raw_df = spark.read.option("header", True).option("inferSchema", False).csv(target_path)
    raw_count = raw_df.count()
    LOGGER.info("Đã đọc xong dữ liệu thô, tổng số dòng: %,d", raw_count)

    if raw_count == 0:
        raise ValueError("FILE_EMPTY: file nguồn không có dòng dữ liệu")

    validate_raw_schema(raw_df)

    return raw_df


def ingest_to_bronze(
    spark: Any,
    input_path: str | None = None,
    mode: str = "overwrite",
    batch_id: str | None = None,
    run_id: str | None = None,
    source_hash: str | None = None,
    manage_registry: bool = True,
    contract_version: str | None = None,
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
    content_hash = source_hash or calculate_source_hash(target_path)
    bid = batch_id or f"batch_{uuid.uuid4().hex[:8]}"
    rid = run_id or f"run_{uuid.uuid4().hex[:8]}"

    registry = BatchRegistry(spark)
    manifest = FileManifest(spark)
    source_system = "ecommerce_csv"

    # File ledger quyết định Bronze đã commit hay chưa. Registry chỉ quản lý
    # lifecycle của run nên không được dùng để kiểm soát append của file.
    if mode == "append" and manifest.is_bronze_committed(source_system, content_hash):
        LOGGER.warning(
            "File %s (Source Hash: %s) đã commit Bronze. Đọc lại Bronze, không append lại.",
            bid,
            content_hash[:10],
        )
        bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
        return (
            spark.read.format("delta").load(bronze_path).filter(col("_source_hash") == content_hash)
        )

    manifest.register_discovered(
        source_system=source_system,
        source_hash=content_hash,
        source_uri=target_path,
        file_size_bytes=calculate_source_size(target_path),
        contract_version=contract_version or ("2.0.0" if mode == "append" else "1.0.0"),
        run_id=rid,
    )

    # Register trước khi đọc file để FILE_SCHEMA_MISMATCH/FILE_EMPTY cũng có
    # lifecycle và lịch sử retry rõ ràng.
    if manage_registry:
        registry.start_run(
            run_id=rid,
            batch_id=bid,
            source_uri=target_path,
            source_hash=content_hash,
            raw_rows=0,
            source_size_bytes=calculate_source_size(target_path),
            contract_version="2.0.0" if mode == "append" else "1.0.0",
        )

    try:
        raw_df = read_raw_csv(spark, target_path)
        raw_count = raw_df.count()

        bronze_df = enrich_with_ingestion_metadata(
            raw_df,
            batch_id=bid,
            run_id=rid,
            source_hash=content_hash,
            source_uri=target_path,
            contract_version=contract_version,
        )
        save_and_verify_delta(bronze_df, SETTINGS.bronze_delta, "bronze.ecommerce_raw", mode=mode)
        manifest.mark_bronze_committed(
            source_system=source_system,
            source_hash=content_hash,
            run_id=rid,
            raw_rows=raw_count,
        )

        if manage_registry:
            registry.update_metrics(rid, raw_rows=raw_count)
            # Ingestion riêng lẻ mới chỉ hoàn tất Bronze; không đánh dấu SUCCESS
            # vì Silver/Gold/Reconciliation chưa chạy.
            registry.mark_validated(rid)

        LOGGER.info("Đã hoàn tất Ingestion tầng Bronze Delta (chế độ: %s, Batch: %s).", mode, bid)
        return bronze_df
    except Exception as exc:
        manifest.mark_failed(
            source_system=source_system,
            source_hash=content_hash,
            run_id=rid,
            error_code="BRONZE_INGESTION_FAILED",
            error_message=str(exc),
        )
        if manage_registry and registry.find_by_run_id(rid) is not None:
            registry.mark_failed(rid, exc, error_code="BRONZE_INGESTION_FAILED")
        raise
