"""Mô-đun nạp dữ liệu thô cho tầng Bronze của Lakehouse Pipeline."""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from config import PIPELINE_VERSION, SETTINGS

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
    col = current_timestamp = lit = monotonically_increasing_id = sha2 = struct = to_json = None

from .contracts.loader import load_contract_for_columns

try:
    from .file_manifest import FileManifest
    from .registry import BatchRegistry
except ImportError:
    FileManifest = None
    BatchRegistry = None

LOGGER = logging.getLogger(__name__)


def _resolve_local_path(file_path: str | None) -> Path | None:
    """Đổi đường dẫn thường hoặc file URI thành đường dẫn local hợp lệ.

    Pipeline chỉ hỗ trợ file local ở phiên bản hiện tại. Không được âm thầm
    biến một URI remote thành tên file tương đối vì như vậy hash có thể được
    tính sai hoặc lỗi nguồn bị phát hiện quá muộn.
    """
    if not file_path:
        return None

    # urlparse hiểu "C:\\file.csv" là URI có scheme "c" nếu không xử lý trước.
    if len(file_path) >= 2 and file_path[1] == ":" and file_path[0].isalpha():
        return Path(file_path)

    parsed = urlparse(file_path)
    if parsed.scheme not in {"", "file"}:
        raise ValueError(
            f"Nguồn không được hỗ trợ: {file_path}. "
            "Pipeline hiện chỉ nhận file local hoặc file URI."
        )

    if parsed.scheme == "":
        return Path(unquote(parsed.path or file_path))

    if parsed.netloc not in {"", "localhost"}:
        # file://server/share là đường dẫn UNC hợp lệ trên Windows.
        raw_path = f"//{parsed.netloc}{parsed.path}"
    else:
        raw_path = parsed.path

    raw_path = unquote(raw_path)
    # file:///C:/... có thêm một dấu / trước drive letter trên Windows.
    if os.name == "nt" and len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
        raw_path = raw_path[1:]
    return Path(raw_path)


def calculate_source_hash(file_path: str | None) -> str:
    """Tính SHA-256 trên bytes thật của file nguồn để kiểm soát idempotency.

    Args:
        file_path: Đường dẫn tới file nguồn local hoặc file URI local.

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

    p = _resolve_local_path(file_path)
    if p is None:
        raise ValueError("Không thể tính source_hash khi thiếu đường dẫn file nguồn")
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
    """Trả về kích thước bytes của file local để ghi vào manifest nguồn."""
    if not file_path:
        return 0
    path = _resolve_local_path(file_path)
    if path is None:
        return 0
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

    # Mã băm dùng toàn bộ cột nghiệp vụ thực tế để event v2 không bị bỏ qua
    # Source_Updated_At hoặc Operation khi tạo dấu vân tay bản ghi.
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

    # Đọc header dưới dạng chuỗi; chỉ ép kiểu sau khi qua kiểm tra hợp đồng
    # cấp file và bộ chuyển đổi bootstrap cho dữ liệu lịch sử.
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

    # Sổ file quyết định Bronze đã commit hay chưa. Registry chỉ quản lý vòng đời
    # của run nên không được dùng để kiểm soát việc append file.
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

    # Đăng ký trước khi đọc file để FILE_SCHEMA_MISMATCH/FILE_EMPTY cũng có
    # vòng đời và lịch sử retry rõ ràng.
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
            # vì Silver/Gold/Reconciliation chưa chạy xong.
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
