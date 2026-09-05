"""Contract test kiểm chứng tính đồng nhất 100% giữa Pandas và PySpark cho RFM và Pareto ABC."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Thêm SourceCode vào sys.path
SOURCE_DIR = Path(__file__).resolve().parents[1] / "SourceCode"
if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))

from analytics_rules import calculate_abc_pandas, calculate_rfm_pandas


@pytest.fixture
def shared_analytics_fixture() -> pd.DataFrame:
    """Tạo tập dữ liệu giao dịch chuẩn để so sánh hai engine Pandas và Spark."""
    return pd.DataFrame(
        {
            "Order_ID": ["ORD01", "ORD02", "ORD03", "ORD04", "ORD05", "ORD06", "ORD07"],
            "Order_Date": [
                "2026-08-01",
                "2026-08-05",
                "2026-08-10",
                "2026-08-15",
                "2026-08-20",
                "2026-08-22",
                "2026-08-25",
            ],
            "Customer_ID": ["C1", "C1", "C1", "C2", "C2", "C3", "C4"],
            "Product_Name": [
                "Laptop Pro",
                "Laptop Pro",
                "Mouse",
                "Laptop Pro",
                "Keyboard",
                "Monitor",
                "USB Cable",
            ],
            "Category": [
                "Tech",
                "Tech",
                "Tech",
                "Tech",
                "Tech",
                "Tech",
                "Tech",
            ],
            "Sub_Category": [
                "PC",
                "PC",
                "Acc",
                "PC",
                "Acc",
                "Screen",
                "Acc",
            ],
            "Quantity": [1, 1, 2, 1, 1, 1, 10],
            "Unit_Price": [1000.0, 1000.0, 25.0, 1000.0, 50.0, 200.0, 5.0],
            "Discount": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "Revenue": [1000.0, 1000.0, 50.0, 1000.0, 50.0, 200.0, 50.0],
            "Cost": [700.0, 700.0, 15.0, 700.0, 30.0, 140.0, 30.0],
            "Profit": [300.0, 300.0, 35.0, 300.0, 20.0, 60.0, 20.0],
            "Shipping_Cost": [20.0, 20.0, 5.0, 20.0, 5.0, 10.0, 5.0],
            "Shipping_Days": [2, 3, 1, 4, 2, 2, 1],
            "Order_Status": [
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
            ],
        }
    )


def test_pandas_rfm_logic(shared_analytics_fixture: pd.DataFrame) -> None:
    """Kiểm tra logic phân hạng RFM trên Pandas."""
    rfm_df = calculate_rfm_pandas(shared_analytics_fixture, analysis_date="2026-08-25")
    c1 = rfm_df[rfm_df["Customer_ID"] == "C1"].iloc[0]
    assert c1["Frequency"] == 3
    assert c1["Monetary"] == 2050.0
    assert c1["RFM_Segment"] == "Champions"


def test_pandas_abc_logic(shared_analytics_fixture: pd.DataFrame) -> None:
    """Kiểm tra logic phân loại Pareto ABC trên Pandas."""
    abc_df = calculate_abc_pandas(shared_analytics_fixture)
    laptop = abc_df[abc_df["Product_Name"] == "Laptop Pro"].iloc[0]
    assert laptop["Total_Revenue"] == 3000.0
    # Laptop Pro chiếm 3000 / 3350 = 89.55% doanh thu -> Sản phẩm đầu tiên làm tổng >80% vẫn thuộc Class A
    assert laptop["ABC_Class"] == "Class A (Top 80% Revenue)"
