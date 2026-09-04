"""Module kiểm tra chẩn đoán môi trường hệ thống (System Environment Doctor).

Xác minh tính khả dụng của Java, Hadoop HDFS CLI, Python, Pandas, PySpark, Delta Lake và PyMongo
trước khi khởi chạy các quy trình xử lý dữ liệu.
"""

from __future__ import annotations

import importlib.util
import shutil
from dataclasses import dataclass


@dataclass(frozen=True)
class DependencyStatus:
    """Đại diện cho trạng thái của một phần mềm hoặc thư viện phụ thuộc trong hệ thống.

    Attributes:
        name: Tên gói phần mềm / lệnh terminal.
        available: True nếu tìm thấy và sẵn sàng sử dụng.
        required_for: Mục đích sử dụng của gói này.
        detail: Chi tiết đường dẫn hoặc trạng thái cài đặt.
    """

    name: str
    available: bool
    required_for: str
    detail: str


def command_status(command: str, required_for: str) -> DependencyStatus:
    """Kiểm tra sự tồn tại của một lệnh thực thi (Command-line Utility) trong PATH.

    Args:
        command: Tên lệnh cần tìm (ví dụ: 'java', 'hdfs').
        required_for: Mô tả mục đích sử dụng.

    Returns:
        DependencyStatus phản ánh trạng thái của lệnh.
    """
    executable = shutil.which(command)
    return DependencyStatus(
        name=command,
        available=executable is not None,
        required_for=required_for,
        detail=executable or "Không tìm thấy trong hệ thống PATH",
    )


def module_status(module: str, required_for: str) -> DependencyStatus:
    """Kiểm tra sự tồn tại của một thư viện Python mà không kích hoạt import nặng.

    Args:
        module: Tên module Python (ví dụ: 'pyspark', 'delta').
        required_for: Mô tả mục đích sử dụng.

    Returns:
        DependencyStatus phản ánh trạng thái thư viện.
    """
    available = importlib.util.find_spec(module) is not None
    return DependencyStatus(
        name=module,
        available=available,
        required_for=required_for,
        detail="Đã cài đặt thành công" if available else "Chưa được cài đặt",
    )


def inspect_environment() -> list[DependencyStatus]:
    """Kiểm tra toàn bộ danh sách phụ thuộc cho hệ thống GlobalCart Intelligence.

    Returns:
        Danh sách chứa trạng thái của từng phần mềm/thư viện.
    """
    return [
        command_status("java", "Spark & Hadoop Core"),
        command_status("hdfs", "Hadoop HDFS Storage CLI"),
        module_status("pandas", "Xử lý dữ liệu & Báo cáo Portfolio"),
        module_status("pyspark", "PySpark Engine & Medallion Pipeline"),
        module_status("delta", "Delta Lake Storage Format"),
        module_status("pymongo", "Đồng bộ MongoDB Serving Database"),
    ]


def format_environment_report(statuses: list[DependencyStatus]) -> str:
    """Định dạng kết quả kiểm tra thành bảng báo cáo trực quan trên Terminal.

    Args:
        statuses: Danh sách các DependencyStatus.

    Returns:
        Chuỗi văn bản báo cáo đã định dạng.
    """
    lines = ["🏥 BÁO CÁO CHẨN ĐOÁN MÔI TRƯỜNG HỆ THỐNG", "=" * 72]
    for status in statuses:
        marker = "ĐẠT" if status.available else "THIẾU"
        lines.append(
            f"[{marker:<5}] {status.name:<12} | {status.required_for:<28} | {status.detail}"
        )
    return "\n".join(lines)
