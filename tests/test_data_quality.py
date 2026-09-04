from pathlib import Path

import pandas as pd
import pytest
from data_quality import assert_quality, validate_business_values, validate_columns


def test_required_columns_reports_missing_fields():
    result = validate_columns(["Order_ID"])
    assert not result.passed
    assert "Revenue" in result.detail


def test_valid_business_values_pass():
    row = {
        "Order_ID": "ORD-1",
        "Order_Date": "2024-01-01",
        "Customer_ID": "CUS-1",
        "Product_Name": "Sản phẩm",
        "Quantity": 1,
        "Unit_Price": 10.0,
        "Discount": 0.1,
        "Revenue": 9.0,
        "Cost": 5.0,
        "Profit": 4.0,
        "Shipping_Cost": 1.0,
        "Shipping_Days": 2,
        "Order_Status": "Delivered",
    }
    results = validate_business_values(pd.DataFrame([row]))
    assert all(result.passed for result in results)


def test_invalid_discount_fails_fast():
    frame = pd.read_csv(Path(__file__).parents[1] / "Data" / "EcommerceSalesDataset.csv", nrows=2)
    frame.loc[0, "Discount"] = 2
    with pytest.raises(ValueError, match="discount_range"):
        assert_quality(validate_business_values(frame))


def test_negative_revenue_fails_quality_gate():
    frame = pd.read_csv(Path(__file__).parents[1] / "Data" / "EcommerceSalesDataset.csv", nrows=1)
    frame.loc[0, "Revenue"] = -1
    with pytest.raises(ValueError, match="revenue_non_negative"):
        assert_quality(validate_business_values(frame))
