"""Các mô-đun xử lý kiến trúc Medallion bằng PySpark và Delta Lake."""

from __future__ import annotations

__all__ = [
    "run_pipeline",
    "run_incremental_pipeline",
    "run_incremental_from_path",
    "run_full_reconciliation",
]


def __getattr__(name: str):
    """Nạp API công khai theo nhu cầu mà không che giấu lỗi phụ thuộc."""
    if name in {"run_pipeline", "run_incremental_pipeline", "run_incremental_from_path"}:
        from .pipeline import run_incremental_from_path, run_incremental_pipeline, run_pipeline

        return {
            "run_pipeline": run_pipeline,
            "run_incremental_pipeline": run_incremental_pipeline,
            "run_incremental_from_path": run_incremental_from_path,
        }[name]
    if name == "run_full_reconciliation":
        from .reconciliation import run_full_reconciliation

        return run_full_reconciliation
    raise AttributeError(f"module {__name__!r} không có thuộc tính {name!r}")
