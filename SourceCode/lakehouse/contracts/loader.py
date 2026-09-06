"""Module đọc, phân tích và thực thi hợp đồng dữ liệu (Executable Data Contract).

Đóng vai trò Single Source of Truth cho toàn bộ Schema, Ràng buộc kiểm tra chất lượng (DQ),
và Business Keys từ file `contracts/ecommerce_order.yaml`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger(__name__)

DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[3] / "contracts" / "ecommerce_order.yaml"


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
        LOGGER.warning("Không tìm thấy file contract tại %s, sử dụng fallback mặc định.", target_path)
        return _create_fallback_contract()

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
    )

    if contract_path is None:
        _CACHED_CONTRACT = contract
    return contract


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
    from pyspark.sql.functions import col

    c = contract or load_contract()
    rules: dict[str, Any] = {}

    for col_name, col_contract in c.columns.items():
        if not col_contract.nullable and col_name != "Order_Line_ID":
            rules[f"{col_name} không rỗng"] = col(col_name).isNotNull()

        if col_contract.min is not None:
            if col_contract.min == 0.0:
                rules[f"{col_name} >= 0"] = col(col_name) >= 0
            elif col_contract.min == 1:
                rules[f"{col_name} > 0"] = col(col_name) > 0
            else:
                rules[f"{col_name} >= {col_contract.min}"] = col(col_name) >= col_contract.min

        if col_contract.max is not None and col_contract.min is not None:
            rules[f"{col_name} trong [{col_contract.min}, {col_contract.max}]"] = col(col_name).between(
                col_contract.min, col_contract.max
            )

        if col_contract.allowed_values:
            rules[f"{col_name} thuộc danh sách hợp lệ"] = col(col_name).isin(col_contract.allowed_values)

    return rules


def _create_fallback_contract() -> DatasetContract:
    """Tạo fallback contract khi file yaml không thể nạp (đảm bảo an toàn runtime)."""
    return DatasetContract(
        version="1.0.0",
        dataset="ecommerce_order",
        grain="one product line item within one customer order",
        description="Fallback contract",
        columns={
            "Order_ID": ColumnContract("Order_ID", "string", nullable=False),
            "Order_Date": ColumnContract("Order_Date", "date", nullable=False),
            "Customer_ID": ColumnContract("Customer_ID", "string", nullable=False),
            "Product_Name": ColumnContract("Product_Name", "string", nullable=False),
            "Quantity": ColumnContract("Quantity", "integer", nullable=False, min=1),
            "Unit_Price": ColumnContract("Unit_Price", "double", nullable=False, min=0.0),
            "Discount": ColumnContract("Discount", "double", nullable=False, min=0.0, max=1.0),
            "Revenue": ColumnContract("Revenue", "double", nullable=False, min=0.0),
            "Cost": ColumnContract("Cost", "double", nullable=False, min=0.0),
            "Profit": ColumnContract("Profit", "double", nullable=False),
            "Shipping_Cost": ColumnContract("Shipping_Cost", "double", nullable=False, min=0.0),
            "Shipping_Days": ColumnContract("Shipping_Days", "integer", nullable=False, min=0),
            "Order_Status": ColumnContract(
                "Order_Status",
                "string",
                nullable=False,
                allowed_values=["Delivered", "Returned", "Cancelled", "Processing", "Shipped"],
            ),
        },
        business_keys={
            "order_key": ["Order_ID"],
            "order_line_key": ["Order_ID", "Order_Line_ID"],
            "fallback_line_fingerprint": ["Order_ID", "Product_Name", "Quantity", "Unit_Price", "Discount"],
        },
    )
