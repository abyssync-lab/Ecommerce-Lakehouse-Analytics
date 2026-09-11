"""Bộ kiểm thử đơn vị thuần Python cho các bất biến của pipeline và Data Contract.

Các kiểm chứng này không yêu cầu SparkSession hay Java runtime, chạy độc lập và cực nhanh:
1. Contract thực thi: contracts/ecommerce_order.yaml là nguồn quy tắc runtime duy nhất.
2. Ingestion shape: Tự động nhận diện Contract v1 và Contract v2 theo danh sách cột.
3. Schema guard: Chặn file vi phạm schema trước khi đi vào Bronze.
4. Idempotency: SHA-256 nội dung file tất định, không phụ thuộc vào đường dẫn hay tên file.
5. URI resolution: File URI (file://) được phân giải chính xác trên cả Windows và POSIX.
6. Data Contract: PipelineRunResult tuân thủ hợp đồng dataclass và bảo toàn số dòng.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from config import PipelineRunResult
from lakehouse.contracts.loader import load_contract, load_contract_for_columns
from lakehouse.ingestion import calculate_source_hash, calculate_source_size, validate_raw_schema


def test_contract_yaml_is_runtime_source_of_truth():
    """Kiểm tra contracts/ecommerce_order.yaml là nguồn chân lý điều khiển runtime."""
    contract = load_contract()
    assert contract.dataset == "ecommerce_order"
    assert "Order_ID" in contract.required_columns
    assert "Quantity" in contract.required_columns
    assert "Delivered" in contract.allowed_order_statuses

    import data_quality

    assert data_quality.REQUIRED_COLUMNS == contract.required_columns
    assert data_quality.ALLOWED_ORDER_STATUSES == contract.allowed_order_statuses


def test_change_contract_v2_is_selected_for_incremental_event_shape():
    """Event shape v2 phải được chọn tự động, không dùng nhầm contract bootstrap v1."""
    contract = load_contract_for_columns(
        {"Order_ID", "Order_Line_ID", "Source_Updated_At", "Operation"}
    )
    assert contract.version == "2.0.0"
    assert contract.sequence_column == "Source_Updated_At"
    assert contract.operation_column == "Operation"
    assert contract.order_line_key_cols == ["Order_ID", "Order_Line_ID"]


def test_partial_incremental_shape_fails_before_bronze():
    """File v2 thiếu một marker phải bị chặn ở file-level validation."""

    class RawFrame:
        columns = ["Order_ID", "Order_Line_ID", "Source_Updated_At"]

    with pytest.raises(ValueError, match="FILE_SCHEMA_MISMATCH"):
        validate_raw_schema(RawFrame())


def test_calculate_source_hash_idempotency(tmp_path: Path):
    """Kiểm tra calculate_source_hash sinh hash tất định theo nội dung file, không phụ thuộc tên path."""
    file1 = tmp_path / "batch_alpha.csv"
    file2 = tmp_path / "batch_beta.csv"
    file3 = tmp_path / "batch_modified.csv"

    file1.write_bytes(b"Order_ID,Revenue\nORD-1,100.0\n")
    file2.write_bytes(b"Order_ID,Revenue\nORD-1,100.0\n")
    file3.write_bytes(b"Order_ID,Revenue\nORD-1,200.0\n")

    hash1 = calculate_source_hash(str(file1))
    hash2 = calculate_source_hash(str(file2))
    hash3 = calculate_source_hash(str(file3))

    assert hash1 == hash2, (
        "Hai file cùng nội dung nhưng khác tên phải sinh ra hash giống nhau để chống duplicate!"
    )
    assert hash1 != hash3, "Nội dung file khác nhau phải sinh ra hash khác nhau!"


def test_source_hash_and_size_accept_file_uri(tmp_path: Path):
    """Đảm bảo file URI không bị mất dấu slash khi tính metadata nguồn."""
    source = tmp_path / "orders batch.csv"
    content = b"Order_ID,Revenue\nORD-1,100.0\n"
    source.write_bytes(content)

    assert calculate_source_hash(source.as_uri()) == hashlib.sha256(content).hexdigest()
    assert calculate_source_size(source.as_uri()) == len(content)


def test_pipeline_run_result_dataclass_contract():
    """Kiểm tra PipelineRunResult khởi tạo đúng các trường của một run."""
    res = PipelineRunResult(
        run_id="run_123",
        batch_id="batch_456",
        status="SUCCESS",
        bronze_rows=1000,
        silver_rows=950,
        quarantine_rows=40,
        duplicate_rows=10,
        reconciliation_passed=True,
    )
    assert res.run_id == "run_123"
    assert res.status == "SUCCESS"
    assert res.bronze_rows == res.silver_rows + res.quarantine_rows + res.duplicate_rows
