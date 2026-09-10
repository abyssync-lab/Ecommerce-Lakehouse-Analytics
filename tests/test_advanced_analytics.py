"""Kiểm thử đơn vị cho phân khúc khách hàng RFM và phân loại sản phẩm Pareto ABC."""

from __future__ import annotations

import pandas as pd
import pytest
from business_metrics import calculate_abc_product_analysis, calculate_rfm_segmentation


@pytest.fixture
def sample_sales_data() -> pd.DataFrame:
    """Tạo dữ liệu giao dịch giả lập để kiểm thử thuật toán."""
    return pd.DataFrame(
        {
            "Order_ID": ["ORD001", "ORD002", "ORD003", "ORD004", "ORD005", "ORD006"],
            "Order_Date": [
                "2026-08-01",
                "2026-08-05",
                "2026-08-10",
                "2026-08-15",
                "2026-08-20",
                "2026-08-25",
            ],
            "Customer_ID": ["CUST01", "CUST01", "CUST01", "CUST02", "CUST02", "CUST03"],
            "Product_Name": [
                "Laptop Pro",
                "Laptop Pro",
                "Mouse",
                "Laptop Pro",
                "Keyboard",
                "Mouse",
            ],
            "Category": [
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
                "Electronics",
            ],
            "Quantity": [1, 1, 2, 1, 1, 5],
            "Unit_Price": [1000.0, 1000.0, 25.0, 1000.0, 50.0, 25.0],
            "Discount": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "Revenue": [1000.0, 1000.0, 50.0, 1000.0, 50.0, 125.0],
            "Cost": [700.0, 700.0, 15.0, 700.0, 30.0, 15.0],
            "Profit": [300.0, 300.0, 35.0, 300.0, 20.0, 110.0],
            "Shipping_Cost": [20.0, 20.0, 5.0, 20.0, 5.0, 5.0],
            "Shipping_Days": [2, 3, 1, 4, 2, 1],
            "Order_Status": [
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
                "Delivered",
            ],
        }
    )


def test_calculate_rfm_segmentation(sample_sales_data: pd.DataFrame) -> None:
    """Kiểm tra kết quả phân hạng khách hàng RFM."""
    rfm_df = calculate_rfm_segmentation(sample_sales_data)

    assert "RFM_Segment" in rfm_df.columns
    assert "Recency" in rfm_df.columns
    assert "Frequency" in rfm_df.columns
    assert "Monetary" in rfm_df.columns

    # CUST01 có 3 đơn hàng, tổng chi tiêu cao nhất ($2050)
    cust01 = rfm_df[rfm_df["Customer_ID"] == "CUST01"].iloc[0]
    assert cust01["Frequency"] == 3
    assert cust01["Monetary"] == 2050.0
    assert cust01["RFM_Segment"] in ["Champions", "Loyal Customers"]


def test_calculate_abc_product_analysis(sample_sales_data: pd.DataFrame) -> None:
    """Kiểm tra kết quả phân loại sản phẩm Pareto ABC."""
    abc_df = calculate_abc_product_analysis(sample_sales_data)

    assert "ABC_Class" in abc_df.columns
    assert "Cumulative_Revenue_Percent" in abc_df.columns

    # Laptop Pro đóng góp doanh thu lớn nhất ($3000 / $3225 ≈ 93%)
    laptop = abc_df[abc_df["Product_Name"] == "Laptop Pro"].iloc[0]
    assert laptop["Total_Revenue"] == 3000.0
    assert laptop["ABC_Class"] == "Class A (Top 80% Revenue)"


def test_rfm_rejects_empty_data():
    with pytest.raises(ValueError, match="dữ liệu rỗng"):
        calculate_rfm_segmentation(pd.DataFrame())


def test_rfm_rejects_orders_after_analysis_date(sample_sales_data: pd.DataFrame):
    with pytest.raises(ValueError, match="analysis_date"):
        calculate_rfm_segmentation(sample_sales_data, analysis_date="2026-08-10")


def test_rfm_handles_identical_customer_values():
    frame = pd.DataFrame(
        {
            "Order_ID": ["O1", "O2"],
            "Order_Date": ["2026-08-01", "2026-08-01"],
            "Customer_ID": ["C1", "C2"],
            "Revenue": [100.0, 100.0],
        }
    )
    result = calculate_rfm_segmentation(frame)
    assert len(result) == 2
    assert result["RFM_Segment"].notna().all()


def test_abc_rejects_empty_and_zero_revenue():
    with pytest.raises(ValueError, match="dữ liệu rỗng"):
        calculate_abc_product_analysis(pd.DataFrame())

    frame = pd.DataFrame(
        {
            "Product_Name": ["A"],
            "Order_ID": ["O1"],
            "Quantity": [1],
            "Revenue": [0.0],
            "Profit": [0.0],
        }
    )
    with pytest.raises(ValueError, match="tổng doanh thu"):
        calculate_abc_product_analysis(frame)
