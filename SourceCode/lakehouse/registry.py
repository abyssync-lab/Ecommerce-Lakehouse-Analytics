"""Module quản lý vòng đời Batch và kiểm soát tính lũy đẳng (Batch Registry & Idempotency Control).

Lưu trữ sổ cái kiểm toán (Control Plane) cho toàn bộ các lượt nạp dữ liệu vào Lakehouse:
- Trạng thái: RECEIVED -> PROCESSING -> SUCCESS / FAILED
- Chống nạp trùng lặp (Replay-safe / Idempotent Ingestion) dựa trên SHA-256 checksum của file nguồn.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from config import SETTINGS

try:
    from pyspark.sql.functions import col, current_timestamp
except ImportError:
    col = current_timestamp = None  # type: ignore

LOGGER = logging.getLogger(__name__)


class BatchRegistry:
    """Quản lý trạng thái và nhật ký thực thi của từng Batch nguồn."""

    def __init__(self, spark: Any, registry_path: str | None = None) -> None:
        self.spark = spark
        self.registry_path = registry_path or SETTINGS.get_storage_path(SETTINGS.ingestion_batches_delta)

    def _delta_exists(self) -> bool:
        try:
            from delta.tables import DeltaTable

            return DeltaTable.isDeltaTable(self.spark, self.registry_path)
        except Exception:
            return False

    def is_batch_processed(self, source_hash: str) -> bool:
        """Kiểm tra mã băm nguồn đã từng được nạp và xử lý thành công (SUCCESS) hay chưa.

        Args:
            source_hash: Chuỗi SHA-256 hex digest của file nguồn.

        Returns:
            True nếu batch đã xử lý thành công trước đó, False nếu là batch mới hoặc từng thất bại.
        """
        if not source_hash or source_hash in {"hash_unspecified", "default_stream_source"}:
            return False

        if not self._delta_exists():
            return False

        try:
            df = self.spark.read.format("delta").load(self.registry_path)
            if "source_hash" not in df.columns or "status" not in df.columns:
                return False
            success_count = (
                df.filter((col("source_hash") == source_hash) & (col("status") == "SUCCESS")).count()
            )
            return success_count > 0
        except Exception as e:
            LOGGER.debug("Lỗi khi đọc BatchRegistry Delta table: %s", e)
            return False

    def register_batch_start(
        self,
        run_id: str,
        batch_id: str,
        source_uri: str,
        source_hash: str,
        row_count: int = 0,
        source_system: str = "ecommerce_csv",
        pipeline_version: str = "1.0.0",
        contract_version: str = "1.0.0",
        source_size: int = 0,
    ) -> None:
        """Ghi nhận khởi đầu một lượt xử lý batch (Trạng thái: PROCESSING)."""
        try:
            row_data = [
                (
                    run_id,
                    batch_id,
                    source_system,
                    source_uri,
                    source_hash,
                    int(source_size),
                    int(row_count),
                    "PROCESSING",
                    datetime.now().isoformat(),
                    None,
                    pipeline_version,
                    contract_version,
                    None,
                )
            ]
            schema = [
                "run_id",
                "batch_id",
                "source_system",
                "source_uri",
                "source_hash",
                "source_size",
                "row_count",
                "status",
                "started_at",
                "completed_at",
                "pipeline_version",
                "contract_version",
                "error_message",
            ]
            df = self.spark.createDataFrame(row_data, schema).withColumn("registered_at", current_timestamp())
            df.write.format("delta").mode("append").save(self.registry_path)
            LOGGER.info("BatchRegistry: Đã đăng ký BẮT ĐẦU xử lý Batch %s (Hash: %s).", batch_id, source_hash[:10])
        except Exception as e:
            LOGGER.debug("Không thể ghi nhận register_batch_start vào Delta (local test mode): %s", e)

    def mark_batch_success(
        self,
        run_id: str,
        batch_id: str,
        row_count: int | None = None,
        certified_gold_version: int | None = None,
    ) -> None:
        """Cập nhật trạng thái Batch thành SUCCESS khi toàn bộ pipeline và đối soát đã đạt."""
        try:
            row_data = [
                (
                    run_id,
                    batch_id,
                    "SUCCESS",
                    int(row_count or 0),
                    int(certified_gold_version or 1),
                    datetime.now().isoformat(),
                    None,
                )
            ]
            schema = [
                "run_id",
                "batch_id",
                "status",
                "row_count",
                "certified_gold_version",
                "completed_at",
                "error_message",
            ]
            df = self.spark.createDataFrame(row_data, schema).withColumn("registered_at", current_timestamp())
            df.write.format("delta").mode("append").save(self.registry_path)
            LOGGER.info("BatchRegistry: Đã xác nhận THÀNH CÔNG Batch %s (Run: %s).", batch_id, run_id)
        except Exception as e:
            LOGGER.debug("Không thể ghi nhận mark_batch_success vào Delta: %s", e)

    def mark_batch_failed(
        self,
        run_id: str,
        batch_id: str,
        error_message: str,
    ) -> None:
        """Ghi nhận Batch thất bại (Trạng thái: FAILED) kèm nguyên nhân cụ thể."""
        try:
            row_data = [
                (
                    run_id,
                    batch_id,
                    "FAILED",
                    0,
                    None,
                    datetime.now().isoformat(),
                    str(error_message)[:1000],
                )
            ]
            schema = [
                "run_id",
                "batch_id",
                "status",
                "row_count",
                "certified_gold_version",
                "completed_at",
                "error_message",
            ]
            df = self.spark.createDataFrame(row_data, schema).withColumn("registered_at", current_timestamp())
            df.write.format("delta").mode("append").save(self.registry_path)
            LOGGER.warning("BatchRegistry: Đã ghi nhận THẤT BẠI Batch %s: %s", batch_id, error_message)
        except Exception as e:
            LOGGER.debug("Không thể ghi nhận mark_batch_failed vào Delta: %s", e)

    def is_latest_run_certified(self) -> bool:
        """Kiểm tra lần chạy gần nhất có được chứng nhận (SUCCESS) hay không."""
        if not self._delta_exists():
            return False
        try:
            df = self.spark.read.format("delta").load(self.registry_path)
            if "status" not in df.columns:
                return False
            latest_row = df.orderBy(col("registered_at").desc()).first()
            return latest_row is not None and latest_row["status"] == "SUCCESS"
        except Exception:
            return False
