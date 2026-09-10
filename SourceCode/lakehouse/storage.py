"""Các helper ghi Delta dùng chung cho các tầng của pipeline."""

from __future__ import annotations

from typing import Any

from config import SETTINGS


def resolve_path(relative_path: str) -> str:
    """Ghép đường dẫn file URI cho một bảng Delta local."""
    if "://" in relative_path and not relative_path.startswith("file://"):
        raise ValueError("V1 chỉ hỗ trợ Delta trên local filesystem")
    if relative_path.startswith("file://"):
        return relative_path
    return SETTINGS.get_storage_path(relative_path)


def save_delta(dataframe: Any, path: str, mode: str = "overwrite") -> None:
    """Lưu DataFrame thành Delta Table với đầy đủ Transaction Log (_delta_log)."""
    target_url = resolve_path(path)
    writer = dataframe.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(target_url)


def check_delta_log_exists(spark: Any, path: str) -> bool:
    """Kiểm tra sự tồn tại của thư mục `_delta_log`."""
    delta_log_path = resolve_path(path.rstrip("/") + "/_delta_log")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(delta_log_path)
    return fs.exists(jvm_path)


def save_and_verify_delta(
    dataframe: Any, path: str, table_label: str, mode: str = "overwrite"
) -> None:
    """Ghi Delta Table và xác nhận transaction log `_delta_log` đã được tạo thành công."""
    save_delta(dataframe, path, mode=mode)
    if not check_delta_log_exists(dataframe.sparkSession, path):
        raise RuntimeError(f"{table_label} tại {path} không tạo được _delta_log!")
