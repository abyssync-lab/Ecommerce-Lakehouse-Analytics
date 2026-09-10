"""Kiểm thử các trường hợp biên của validation, RFM và ABC."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from analytics_rules import calculate_abc_pandas, calculate_rfm_pandas
from data_quality import validate_business_values, validate_columns


def test_empty_dataframe_quality():
    """Kiểm tra xử lý DataFrame rỗng."""
    report = validate_business_values(pd.DataFrame())
    assert not report.passed
    assert report.invalid_rows > 0


def test_missing_required_columns():
    """Kiểm tra phát hiện thiếu cột bắt buộc."""
    df = pd.DataFrame({"Order_ID": ["O1"], "Revenue": [100.0]})
    res = validate_columns(df.columns)
    assert not res.passed
    assert "required_columns" in res.rule.lower()
    assert "Thiếu các cột" in res.detail


def test_negative_metrics_validation():
    """Kiểm tra phát hiện chỉ số âm."""
    df = pd.DataFrame(
        {
            "Order_ID": ["O1"],
            "Order_Date": ["2026-08-01"],
            "Customer_ID": ["C1"],
            "Product_Name": ["P1"],
            "Quantity": [-5],
            "Unit_Price": [10.0],
            "Discount": [0.1],
            "Revenue": [45.0],
            "Cost": [20.0],
            "Profit": [25.0],
            "Shipping_Cost": [5.0],
            "Shipping_Days": [2],
            "Order_Status": ["Delivered"],
        }
    )
    report = validate_business_values(df)
    assert not report.passed
    qty_res = next(r for r in report.results if r.rule.lower() == "quantity_positive")
    assert not qty_res.passed


def test_order_id_with_multiple_line_items():
    """Kiểm tra 1 đơn hàng có nhiều sản phẩm (3 dòng nhưng chỉ 2 đơn hàng)."""
    df = pd.DataFrame(
        {
            "Order_ID": ["O1", "O1", "O2"],
            "Order_Date": ["2026-08-01", "2026-08-01", "2026-08-02"],
            "Customer_ID": ["C1", "C1", "C2"],
            "Product_Name": ["Item A", "Item B", "Item A"],
            "Revenue": [100.0, 50.0, 200.0],
        }
    )
    rfm = calculate_rfm_pandas(df)
    c1 = rfm[rfm["Customer_ID"] == "C1"].iloc[0]
    assert c1["Frequency"] == 1
    assert c1["Monetary"] == 150.0


def test_abc_zero_total_revenue():
    """Kiểm tra ABC từ chối tổng doanh thu <= 0."""
    df = pd.DataFrame(
        {
            "Order_ID": ["O1"],
            "Order_Date": ["2026-08-01"],
            "Customer_ID": ["C1"],
            "Product_Name": ["Item Zero"],
            "Quantity": [1],
            "Revenue": [0.0],
            "Profit": [0.0],
        }
    )
    with pytest.raises(ValueError, match="tổng doanh thu <= 0"):
        calculate_abc_pandas(df)
