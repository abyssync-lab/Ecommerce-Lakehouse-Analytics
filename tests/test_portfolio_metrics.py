import pandas as pd
from portfolio_metrics import aggregate_performance, calculate_overview, safe_ratio


def sample_frame():
    return pd.DataFrame(
        [
            {
                "Order_ID": "A",
                "Customer_ID": "C1",
                "Order_Status": "Delivered",
                "Region": "Asia",
                "Revenue": 100.0,
                "Profit": 30.0,
                "Shipping_Cost": 5.0,
            },
            {
                "Order_ID": "B",
                "Customer_ID": "C2",
                "Order_Status": "Returned",
                "Region": "Europe",
                "Revenue": 50.0,
                "Profit": 10.0,
                "Shipping_Cost": 2.0,
            },
        ]
    )


def test_safe_ratio_handles_zero_denominator():
    assert safe_ratio(10, 0) == 0


def test_calculate_overview_uses_weighted_margin():
    metrics = calculate_overview(sample_frame())
    assert metrics.orders == 2
    assert metrics.net_profit == 33.0
    assert metrics.profit_margin_percent == 26.67
    assert metrics.return_rate_percent == 50.0


def test_aggregate_performance_sorts_by_revenue():
    result = aggregate_performance(sample_frame(), ["Region"])
    assert result.iloc[0]["Region"] == "Asia"
