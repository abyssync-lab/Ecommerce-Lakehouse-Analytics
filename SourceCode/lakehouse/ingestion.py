"""Module nạp dữ liệu thô (Ingestion Layer) cho Lakehouse Pipeline."""

from __future__ import annotations

import logging
from typing import Any

from config import SETTINGS
from data_quality import validate_columns

from .storage import resolve_path

LOGGER = logging.getLogger(__name__)


def validate_raw_schema(raw_df: Any) -> None:
    """Kiểm tra và dừng sớm nếu dữ liệu đầu vào không đủ các cột theo Data Contract."""
    result = validate_columns(raw_df.columns)
    if not result.passed:
        raise ValueError(f"Dữ liệu thô không đạt Data Contract đầu vào: {result.detail}")


def read_raw_csv(spark: Any, input_path: str | None = None) -> Any:
    """Đọc dữ liệu CSV thô từ Local Storage hoặc HDFS.

    Args:
        spark: SparkSession active.
        input_path: Đường dẫn file CSV (mặc định lấy từ SETTINGS).

    Returns:
        DataFrame PySpark thô.
    """
    target_path = input_path or SETTINGS.get_input_path()
    LOGGER.info("Bắt đầu đọc dữ liệu CSV thô từ: %s", target_path)

    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(target_path)
    )

    validate_raw_schema(raw_df)
    raw_count = raw_df.count()
    LOGGER.info("Đã đọc xong dữ liệu thô, tổng số dòng: %,d", raw_count)

    return raw_df
