"""Module đọc, phân tích và thực thi hợp đồng dữ liệu (Executable Data Contract).

Đóng vai trò Single Source of Truth cho toàn bộ Schema, Ràng buộc kiểm tra chất lượng (DQ),
và Business Keys từ file `contracts/ecommerce_order.yaml`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[3] / "contracts" / "ecommerce_order.yaml"
CHANGE_CONTRACT_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "ecommerce_order_change_v2.yaml"
)


@dataclass(frozen=True)
class ColumnContract:
    """Đặc tả hợp đồng cho một cột dữ liệu."""

    name: str
    type: str
    nullable: bool = True
    min: float | None = None
    max: float | None = None
    allowed_values: list[str] | None = None
    format: str | None = None
    description: str = ""


@dataclass(frozen=True)
class DatasetContract:
    """Đặc tả hợp đồng dữ liệu toàn diện của dataset."""

    version: str
    dataset: str
    grain: str
    description: str
    columns: dict[str, ColumnContract]
    metadata_columns: dict[str, ColumnContract] = field(default_factory=dict)
    business_keys: dict[str, list[str]] = field(default_factory=dict)
    sequence_column: str | None = None
    operation_column: str | None = None

    @property
    def required_columns(self) -> set[str]:
        """Tập hợp các cột bắt buộc (không được null) cho dữ liệu thô đầu vào."""
        # Order_Line_ID là cột định danh mức dòng có thể được sinh hoặc nạp
        return {
            name
            for name, col in self.columns.items()
            if not col.nullable and name != "Order_Line_ID"
        }

    @property
    def allowed_order_statuses(self) -> set[str]:
        """Danh sách trạng thái đơn hàng hợp lệ."""
        status_col = self.columns.get("Order_Status")
        if status_col and status_col.allowed_values:
            return set(status_col.allowed_values)
        return {"Delivered", "Returned", "Cancelled", "Processing", "Shipped"}

    @property
    def business_column_names(self) -> list[str]:
        """Danh sách tên các cột nghiệp vụ (loại trừ metadata)."""
        return list(self.columns.keys())

    @property
    def order_key_cols(self) -> list[str]:
        """Khóa định danh đơn hàng."""
        return self.business_keys.get("order_key", ["Order_ID"])

    @property
    def order_line_key_cols(self) -> list[str]:
        """Khóa định danh từng dòng sản phẩm trong đơn."""
        return self.business_keys.get("order_line_key", ["Order_ID", "Order_Line_ID"])

    @property
    def fallback_line_fingerprint_cols(self) -> list[str]:
        """Các trường dùng để sinh fingerprint định danh dòng khi source không có line id."""
        return self.business_keys.get(
            "fallback_line_fingerprint",
            ["Order_ID", "Product_Name", "Quantity", "Unit_Price", "Discount"],
        )


_CACHED_CONTRACT: DatasetContract | None = None


def load_contract(contract_path: str | Path | None = None) -> DatasetContract:
    """Nạp file hợp đồng YAML và chuyển thành đối tượng `DatasetContract`.

    Args:
        contract_path: Đường dẫn tới file yaml (nếu None sẽ dùng đường dẫn mặc định).

    Returns:
        DatasetContract đã phân tích cú pháp.
    """
    global _CACHED_CONTRACT
    target_path = Path(contract_path) if contract_path else DEFAULT_CONTRACT_PATH

    if _CACHED_CONTRACT is not None and contract_path is None:
        return _CACHED_CONTRACT

    if not target_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file contract tại {target_path}; "
            "pipeline không được chạy khi thiếu executable contract."
        )

    with open(target_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    cols = {}
    for cname, cinfo in data.get("columns", {}).items():
        cols[cname] = ColumnContract(
            name=cname,
            type=cinfo.get("type", "string"),
            nullable=cinfo.get("nullable", True),
            min=cinfo.get("min"),
            max=cinfo.get("max"),
            allowed_values=cinfo.get("allowed_values"),
            format=cinfo.get("format"),
            description=cinfo.get("description", ""),
        )

    meta_cols = {}
    for mname, minfo in data.get("metadata_columns", {}).items():
        meta_cols[mname] = ColumnContract(
            name=mname,
            type=minfo.get("type", "string"),
            nullable=minfo.get("nullable", True),
            description=minfo.get("description", ""),
        )

    contract = DatasetContract(
        version=str(data.get("version", "1.0.0")),
        dataset=str(data.get("dataset", "ecommerce_order")),
        grain=str(data.get("grain", "")),
        description=str(data.get("description", "")),
        columns=cols,
        metadata_columns=meta_cols,
        business_keys=data.get("business_keys", {}),
        sequence_column=data.get("sequence_column"),
        operation_column=data.get("operation_column"),
    )

    if contract_path is None:
        _CACHED_CONTRACT = contract
    return contract


def detect_contract_path(columns: set[str] | list[str]) -> Path:
    """Chọn contract theo shape của file, không đoán theo tên file.

    Event contract v2 có ba cột nhận diện bắt buộc. Các file historical hiện tại
    không có chúng và tiếp tục đi qua contract bootstrap cho seed lịch sử.
    """
    column_set = set(columns)
    if {"Order_Line_ID", "Source_Updated_At", "Operation"}.issubset(column_set):
        return CHANGE_CONTRACT_PATH
    return DEFAULT_CONTRACT_PATH


def load_contract_for_columns(columns: set[str] | list[str]) -> DatasetContract:
    """Nạp đúng contract cho schema thực tế của một file nguồn."""
    return load_contract(detect_contract_path(columns))


def get_spark_raw_schema(contract: DatasetContract | None = None) -> Any:
    """Sinh schema PySpark StringType cho toàn bộ các cột dữ liệu thô (Bronze Ingestion).

    Tránh inferSchema=True gây schema drift giữa các batch.
    """
    try:
        from pyspark.sql.types import StringType, StructField, StructType
    except ImportError:
        return None

    c = contract or load_contract()
    fields = [StructField(col_name, StringType(), True) for col_name in c.columns.keys()]
    return StructType(fields)


def get_spark_silver_rules(contract: DatasetContract | None = None) -> dict[str, Any]:
    """Sinh biểu thức kiểm định chất lượng (PySpark Column expressions) từ contract."""
    from pyspark.sql.functions import col, lit

    c = contract or load_contract()
    rules: dict[str, Any] = {}

    for col_name, col_contract in c.columns.items():
        if not col_contract.nullable and col_name != "Order_Line_ID":
            rules[f"{col_name} không rỗng"] = col(col_name).isNotNull()

        # Cột nullable chỉ bị kiểm tra khi có giá trị. Nếu không bọc biểu thức
        # bằng isNull(), Spark sẽ trả về NULL và validate_silver_data sẽ hiểu
        # nhầm đó là một lỗi của event v2.
        nullable_ok = col(col_name).isNull() if col_contract.nullable else lit(False)

        if col_contract.min is not None:
            if col_contract.min == 0.0:
                condition = col(col_name) >= 0
            elif col_contract.min == 1:
                condition = col(col_name) > 0
            else:
                condition = col(col_name) >= col_contract.min
            rule_name = (
                f"{col_name} >= 0"
                if col_contract.min == 0.0
                else f"{col_name} > 0"
                if col_contract.min == 1
                else f"{col_name} >= {col_contract.min}"
            )
            rules[rule_name] = nullable_ok | condition

        if col_contract.max is not None and col_contract.min is not None:
            rules[f"{col_name} trong [{col_contract.min}, {col_contract.max}]"] = nullable_ok | col(
                col_name
            ).between(col_contract.min, col_contract.max)

        if col_contract.allowed_values:
            rules[f"{col_name} thuộc danh sách hợp lệ"] = nullable_ok | col(col_name).isin(
                col_contract.allowed_values
            )

    return rules
