"""Sổ theo dõi lifecycle của từng pipeline run.

Registry là bảng trạng thái hiện tại của một run, không phải event log append-only.
Mỗi ``run_id`` chỉ có một dòng và mọi chuyển trạng thái đều cập nhật đúng dòng đó.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from config import SETTINGS
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)


class BatchConflictError(RuntimeError):
    """Báo lỗi khi cùng ``batch_id`` được dùng cho hai nội dung khác nhau."""


class BatchRegistry:
    """Quản lý một dòng trạng thái duy nhất cho mỗi lần chạy pipeline."""

    REGISTRY_SCHEMA = StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("batch_id", StringType(), nullable=False),
            StructField("source_system", StringType(), nullable=True),
            StructField("source_uri", StringType(), nullable=True),
            StructField("source_hash", StringType(), nullable=True),
            StructField("source_size_bytes", LongType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("started_at", TimestampType(), nullable=True),
            StructField("completed_at", TimestampType(), nullable=True),
            StructField("raw_rows", LongType(), nullable=True),
            StructField("exact_duplicate_rows", LongType(), nullable=True),
            StructField("rejected_rows", LongType(), nullable=True),
            StructField("sequence_conflict_rows", LongType(), nullable=True),
            StructField("valid_event_rows", LongType(), nullable=True),
            StructField("superseded_rows", LongType(), nullable=True),
            StructField("inserted_rows", LongType(), nullable=True),
            StructField("updated_rows", LongType(), nullable=True),
            StructField("unchanged_rows", LongType(), nullable=True),
            StructField("stale_rows", LongType(), nullable=True),
            StructField("deleted_rows", LongType(), nullable=True),
            StructField("orphan_delete_rows", LongType(), nullable=True),
            StructField("gold_run_id", StringType(), nullable=True),
            StructField("published_version", StringType(), nullable=True),
            StructField("error_code", StringType(), nullable=True),
            StructField("error_message", StringType(), nullable=True),
            StructField("pipeline_version", StringType(), nullable=True),
            StructField("contract_version", StringType(), nullable=True),
            StructField("last_updated_at", TimestampType(), nullable=True),
        ]
    )

    METRIC_COLUMNS = {
        "raw_rows",
        "exact_duplicate_rows",
        "rejected_rows",
        "sequence_conflict_rows",
        "valid_event_rows",
        "superseded_rows",
        "inserted_rows",
        "updated_rows",
        "unchanged_rows",
        "stale_rows",
        "deleted_rows",
        "orphan_delete_rows",
    }

    def __init__(self, spark: Any, registry_path: str | None = None) -> None:
        self.spark = spark
        self.registry_path = registry_path or SETTINGS.get_storage_path(
            SETTINGS.pipeline_runs_delta
        )

    def _delta_exists(self) -> bool:
        """Kiểm tra bảng tồn tại; lỗi Delta phải được phát ra, không được nuốt."""
        exists = DeltaTable.isDeltaTable(self.spark, self.registry_path)
        if exists:
            actual = set(self.spark.read.format("delta").load(self.registry_path).columns)
            expected = {field.name for field in self.REGISTRY_SCHEMA.fields}
            missing = expected.difference(actual)
            if missing:
                raise RuntimeError(
                    "ctl_pipeline_runs đang dùng schema cũ; cần migration trước khi chạy: "
                    + ", ".join(sorted(missing))
                )
        return exists

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC).replace(tzinfo=None)

    def _empty_payload(self) -> dict[str, Any]:
        """Tạo payload đầy đủ schema để insert không làm drift schema."""
        return {field.name: None for field in self.REGISTRY_SCHEMA.fields}

    def _merge(self, payload: dict[str, Any]) -> None:
        """Upsert một phần trạng thái vào đúng ``run_id``."""
        run_id = payload.get("run_id")
        if not run_id:
            raise ValueError("Registry payload bắt buộc phải có run_id")

        existing = self.find_by_run_id(run_id) if self._delta_exists() else None
        row = {
            field.name: (existing[field.name] if existing is not None else None)
            for field in self.REGISTRY_SCHEMA.fields
        }
        row.update(payload)
        # Mỗi lần update phải có timestamp mới để truy vấn run gần nhất đúng.
        row["last_updated_at"] = self._now()
        source_df = self.spark.createDataFrame([row], self.REGISTRY_SCHEMA)

        if not self._delta_exists():
            source_df.write.format("delta").mode("overwrite").save(self.registry_path)
            return

        table = DeltaTable.forPath(self.spark, self.registry_path)
        update_values = {column: f"source.{column}" for column in payload if column != "run_id"}
        update_values["last_updated_at"] = "source.last_updated_at"

        insert_values = {
            field.name: f"source.{field.name}" for field in self.REGISTRY_SCHEMA.fields
        }

        (
            table.alias("target")
            .merge(source_df.alias("source"), "target.run_id = source.run_id")
            .whenMatchedUpdate(set=update_values)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

    def _read(self) -> Any:
        if not self._delta_exists():
            return None
        return self.spark.read.format("delta").load(self.registry_path)

    def find_by_run_id(self, run_id: str) -> Any:
        """Đọc trạng thái hiện tại của một run."""
        dataframe = self._read()
        if dataframe is None:
            return None
        rows = dataframe.filter(col("run_id") == run_id).limit(1).collect()
        return rows[0] if rows else None

    def find_by_source_hash(self, source_hash: str) -> Any:
        """Đọc run gần nhất của một source hash."""
        if not source_hash:
            return None
        dataframe = self._read()
        if dataframe is None:
            return None
        rows = (
            dataframe.filter(col("source_hash") == source_hash)
            .orderBy(col("last_updated_at").desc())
            .limit(1)
            .collect()
        )
        return rows[0] if rows else None

    def assert_batch_identity(self, batch_id: str, source_hash: str) -> None:
        """Không cho phép reuse batch_id với nội dung file khác."""
        dataframe = self._read()
        if dataframe is None:
            return
        conflicts = (
            dataframe.filter(
                (col("batch_id") == batch_id)
                & col("source_hash").isNotNull()
                & (col("source_hash") != source_hash)
            )
            .limit(1)
            .collect()
        )
        if conflicts:
            raise BatchConflictError(
                f"batch_id={batch_id} đã tồn tại với source_hash khác; không được ghi đè nội dung batch."
            )

    def is_batch_processed(self, source_hash: str) -> bool:
        """True khi hash đã đạt trạng thái SUCCESS hoặc PUBLISHED."""
        if not source_hash or source_hash in {
            "hash_unspecified",
            "hash_unavailable",
            "default_stream_source",
        }:
            return False
        row = self.find_by_source_hash(source_hash)
        return row is not None and row["status"] in {"SUCCESS", "PUBLISHED"}

    def start_run(
        self,
        *,
        run_id: str,
        batch_id: str,
        source_uri: str,
        source_hash: str,
        raw_rows: int = 0,
        source_system: str = "ecommerce_csv",
        pipeline_version: str = "1.0.0",
        contract_version: str = "1.0.0",
        source_size_bytes: int = 0,
    ) -> None:
        """Tạo hoặc reset đúng một run ở trạng thái PROCESSING."""
        self.assert_batch_identity(batch_id, source_hash)
        existing = self.find_by_run_id(run_id)
        if existing is not None and existing["status"] not in {"FAILED", "PROCESSING"}:
            raise BatchConflictError(
                f"run_id={run_id} đã ở trạng thái {existing['status']}, không thể start lại."
            )

        self._merge(
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "source_system": source_system,
                "source_uri": source_uri,
                "source_hash": source_hash,
                "source_size_bytes": int(source_size_bytes),
                "status": "PROCESSING",
                "started_at": self._now(),
                "completed_at": None,
                "raw_rows": int(raw_rows),
                "exact_duplicate_rows": None,
                "rejected_rows": None,
                "sequence_conflict_rows": None,
                "valid_event_rows": None,
                "superseded_rows": None,
                "inserted_rows": None,
                "updated_rows": None,
                "unchanged_rows": None,
                "stale_rows": None,
                "deleted_rows": None,
                "orphan_delete_rows": None,
                "gold_run_id": None,
                "published_version": None,
                "error_code": None,
                "error_message": None,
                "pipeline_version": pipeline_version,
                "contract_version": contract_version,
            }
        )
        LOGGER.info("Registry: run=%s status=PROCESSING batch=%s", run_id, batch_id)

    def update_metrics(self, run_id: str, **metrics: int) -> None:
        """Cập nhật các metric đã biết, không thay đổi các metric khác."""
        unknown = set(metrics) - self.METRIC_COLUMNS
        if unknown:
            raise ValueError(f"Metric registry không được hỗ trợ: {sorted(unknown)}")
        self._merge({"run_id": run_id, **{key: int(value) for key, value in metrics.items()}})

    def update_status(self, run_id: str, status: str, **fields: Any) -> None:
        """Cập nhật trạng thái và các field liên quan của run."""
        allowed = {field.name for field in self.REGISTRY_SCHEMA.fields}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"Field registry không được hỗ trợ: {sorted(unknown)}")
        self._merge({"run_id": run_id, "status": status, **fields})

    def mark_validated(self, run_id: str) -> None:
        self.update_status(run_id, "VALIDATED")

    def mark_silver_merged(self, run_id: str) -> None:
        self.update_status(run_id, "SILVER_MERGED")

    def mark_reconciled(self, run_id: str) -> None:
        self.update_status(run_id, "RECONCILED")

    def mark_publish_metadata_pending(self, run_id: str, error: Exception | str) -> None:
        """Snapshot đã đổi nhưng metadata chưa chốt được; không báo FAILED giả."""
        self.update_status(
            run_id,
            "PUBLISH_METADATA_PENDING",
            error_code="PUBLISH_METADATA_PENDING",
            error_message=str(error)[:4000],
        )

    def mark_published(
        self,
        run_id: str,
        *,
        gold_run_id: str,
        published_version: str,
    ) -> None:
        self.update_status(
            run_id,
            "PUBLISHED",
            gold_run_id=gold_run_id,
            published_version=published_version,
            completed_at=self._now(),
        )

    def mark_failed(
        self,
        run_id: str,
        error: Exception | str,
        *,
        error_code: str = "PIPELINE_FAILED",
    ) -> None:
        """Đánh dấu FAILED và để lỗi registry tự nổi lên nếu ghi thất bại."""
        message = str(error)[:4000]
        self.update_status(
            run_id,
            "FAILED",
            error_code=error_code,
            error_message=message,
            completed_at=self._now(),
        )
        LOGGER.error("Registry: run=%s status=FAILED code=%s", run_id, error_code)
