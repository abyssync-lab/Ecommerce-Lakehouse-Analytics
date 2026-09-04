"""Package Lakehouse chứa các module xử lý Medallion Data Lakehouse bằng PySpark & Delta Lake."""

from __future__ import annotations

from .pipeline import run_pipeline

__all__ = ["run_pipeline"]
