"""Sổ cái file nguồn của control plane.

`ctl_ingestion_files` trả lời một câu hỏi khác với `ctl_pipeline_runs`:
file này đã được ghi vào Bronze hay chưa? Một file có thể có nhiều run do
retry, nhưng chỉ được commit vào Bronze đúng một lần theo
`source_system + source_hash`.
"""

from __future__ import annotations

from datetime import datetime, timezone
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


class FileManifest:
    """Quản lý trạng thái commit Bronze của từng file nguồn."""

    SCHEMA = StructType(
        [
            StructField("source_system", StringType(), nullable=False),
            StructField("source_hash", StringType(), nullable=False),
            StructField("source_uri", StringType(), nullable=True),
            StructField("file_size_bytes", LongType(), nullable=True),
            StructField("contract_version", StringType(), nullable=True),
            StructField("first_seen_at", TimestampType(), nullable=True),
            StructField("bronze_status", StringType(), nullable=False),
            StructField("bronze_committed_at", TimestampType(), nullable=True),
            StructField("first_run_id", StringType(), nullable=True),
            StructField("last_run_id", StringType(), nullable=True),
            StructField("raw_rows", LongType(), nullable=True),
            StructField("last_error_code", StringType(), nullable=True),
            StructField("last_error_message", StringType(), nullable=True),
            StructField("last_updated_at", TimestampType(), nullable=True),
        ]
    )

    def __init__(self, spark: Any, manifest_path: str | None = None) -> None:
        self.spark = spark
        self.manifest_path = manifest_path or SETTINGS.get_storage_path(
            SETTINGS.ingestion_files_delta
        )

    @staticmethod
    def _now() -> datetime:
        """Dùng timestamp UTC không timezone để tương thích Spark TimestampType."""
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def _exists(self) -> bool:
        """Kiểm tra manifest và chặn schema cũ thay vì tự merge mù quáng."""
        exists = DeltaTable.isDeltaTable(self.spark, self.manifest_path)
        if exists:
            actual = set(self.spark.read.format("delta").load(self.manifest_path).columns)
            expected = {field.name for field in self.SCHEMA.fields}
            missing = expected.difference(actual)
            if missing:
                raise RuntimeError(
                    "ctl_ingestion_files thiếu cột bắt buộc; cần migration trước khi chạy: "
                    + ", ".join(sorted(missing))
                )
        return exists

    def _read(self) -> Any:
        if not self._exists():
            return None
        return self.spark.read.format("delta").load(self.manifest_path)

    def find(self, source_system: str, source_hash: str) -> Any:
        """Tìm file theo khóa nội dung, không theo filename hoặc batch_id."""
        dataframe = self._read()
        if dataframe is None:
            return None
        rows = (
            dataframe.filter(
                (col("source_system") == source_system) & (col("source_hash") == source_hash)
            )
            .limit(1)
            .collect()
        )
        return rows[0] if rows else None

    def _merge(self, payload: dict[str, Any]) -> None:
        required = {"source_system", "source_hash"}
        missing = required.difference(payload)
        if missing:
            raise ValueError(f"Manifest thiếu khóa: {sorted(missing)}")

        existing = self.find(payload["source_system"], payload["source_hash"])
        row = {
            field.name: (existing[field.name] if existing is not None else None)
            for field in self.SCHEMA.fields
        }
        row.update(payload)
        row["last_updated_at"] = self._now()
        source = self.spark.createDataFrame([row], self.SCHEMA)

        if not self._exists():
            source.write.format("delta").mode("overwrite").save(self.manifest_path)
            return

        table = DeltaTable.forPath(self.spark, self.manifest_path)
        update_values = {
            key: f"source.{key}" for key in payload if key not in {"source_system", "source_hash"}
        }
        update_values["last_updated_at"] = "source.last_updated_at"
        insert_values = {field.name: f"source.{field.name}" for field in self.SCHEMA.fields}
        (
            table.alias("target")
            .merge(
                source.alias("source"),
                "target.source_system = source.source_system "
                "AND target.source_hash = source.source_hash",
            )
            .whenMatchedUpdate(set=update_values)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

    def register_discovered(
        self,
        *,
        source_system: str,
        source_hash: str,
        source_uri: str,
        file_size_bytes: int,
        contract_version: str,
        run_id: str,
    ) -> Any:
        """Đăng ký file khi bắt đầu xử lý nhưng chưa tuyên bố đã commit Bronze."""
        existing = self.find(source_system, source_hash)
        if existing is not None and existing["bronze_status"] == "BRONZE_COMMITTED":
            return existing

        self._merge(
            {
                "source_system": source_system,
                "source_hash": source_hash,
                "source_uri": source_uri,
                "file_size_bytes": int(file_size_bytes),
                "contract_version": contract_version,
                "first_seen_at": existing["first_seen_at"] if existing else self._now(),
                "bronze_status": "DISCOVERED",
                "first_run_id": existing["first_run_id"] if existing else run_id,
                "last_run_id": run_id,
            }
        )
        return self.find(source_system, source_hash)

    def mark_bronze_committed(
        self,
        *,
        source_system: str,
        source_hash: str,
        run_id: str,
        raw_rows: int,
    ) -> None:
        """Chỉ đánh dấu sau khi Delta write đã hoàn tất và verify thành công."""
        if self.find(source_system, source_hash) is None:
            raise ValueError("Không thể commit manifest cho file chưa được register")
        self._merge(
            {
                "source_system": source_system,
                "source_hash": source_hash,
                "bronze_status": "BRONZE_COMMITTED",
                "bronze_committed_at": self._now(),
                "last_run_id": run_id,
                "raw_rows": int(raw_rows),
                "last_error_code": None,
                "last_error_message": None,
            }
        )

    def mark_failed(
        self,
        *,
        source_system: str,
        source_hash: str,
        run_id: str,
        error_code: str,
        error_message: str,
    ) -> None:
        """Ghi lỗi đọc/ghi file để retry biết Bronze chưa hoàn tất."""
        if self.find(source_system, source_hash) is None:
            raise ValueError("Không thể ghi lỗi manifest cho file chưa được register")
        self._merge(
            {
                "source_system": source_system,
                "source_hash": source_hash,
                "bronze_status": "FAILED",
                "last_run_id": run_id,
                "last_error_code": error_code,
                "last_error_message": error_message[:4000],
            }
        )

    def is_bronze_committed(self, source_system: str, source_hash: str) -> bool:
        """True khi retry phải đọc lại Bronze thay vì append thêm lần nữa."""
        row = self.find(source_system, source_hash)
        return row is not None and row["bronze_status"] == "BRONZE_COMMITTED"
