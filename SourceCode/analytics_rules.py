"""Module quản lý tập trung các quy tắc phân tích kinh doanh (Business Analytics Rules).

Single Source of Truth cho các thuật toán phân hạng khách hàng RFM (Recency, Frequency, Monetary)
và Phân loại sản phẩm Pareto ABC. Được sử dụng chung bởi cả Pandas Engine và PySpark Lakehouse.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class RFMThresholds:
    """Các ngưỡng số ngày và số đơn để phân hạng khách hàng RFM."""

    champions_recency_max_days: int = 30
    champions_frequency_min_orders: int = 3
    loyal_frequency_min_orders: int = 3
    at_risk_recency_min_days: int = 90


@dataclass(frozen=True)
class ABCThresholds:
    """Các ngưỡng phần trăm doanh thu tích lũy Pareto ABC."""

    class_a_cumulative_max_percent: float = 80.0
    class_b_cumulative_max_percent: float = 95.0


RFM_RULE_VERSION = "1.0.0"
ABC_RULE_VERSION = "1.0.0"

DEFAULT_RFM_THRESHOLDS = RFMThresholds()
DEFAULT_ABC_THRESHOLDS = ABCThresholds()

# Tên phân nhóm RFM chuẩn
RFM_CHAMPIONS = "Champions"
RFM_LOYAL = "Loyal Customers"
RFM_AT_RISK = "At-Risk Customers"
RFM_CASUAL = "Recent & Casual Customers"

# Tên lớp Pareto ABC chuẩn
ABC_CLASS_A = "Class A (Top 80% Revenue)"
ABC_CLASS_B = "Class B (Next 15% Revenue)"
ABC_CLASS_C = "Class C (Tail 5% Revenue)"

BUSINESS_METRICS_PATH = Path(__file__).resolve().parents[1] / "contracts" / "business_metrics.yaml"


@lru_cache(maxsize=1)
def _load_business_status_policy() -> dict[str, list[str]]:
    """Đọc status policy chung để Pandas và Spark dùng cùng một quy tắc KPI."""
    if not BUSINESS_METRICS_PATH.exists():
        raise FileNotFoundError(f"Thiếu business metric contract: {BUSINESS_METRICS_PATH}")
    data = yaml.safe_load(BUSINESS_METRICS_PATH.read_text(encoding="utf-8")) or {}
    return {
        metric: [str(status) for status in data.get(metric, {}).get("included_statuses", [])]
        for metric in ("rfm", "abc")
    }


def _filter_metric_rows(dataframe: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Lọc dữ liệu theo policy nếu DataFrame có cột trạng thái."""
    if "Order_Status" not in dataframe.columns:
        return dataframe
    statuses = _load_business_status_policy()[metric]
    if not statuses:
        raise ValueError(f"Business policy {metric} không có included_statuses")
    return dataframe[dataframe["Order_Status"].isin(statuses)].copy()


def _measure_column(dataframe: pd.DataFrame, gold_name: str, fallback_name: str) -> str:
    """Chọn measure đã chứng nhận trước measure nguồn tương thích ngược."""
    if gold_name in dataframe.columns:
        return gold_name
    if fallback_name in dataframe.columns:
        return fallback_name
    raise ValueError(
        f"Thiếu measure {gold_name}; dữ liệu cũng không có cột dự phòng {fallback_name}"
    )


def classify_rfm_segment(
    recency_days: int,
    frequency_orders: int,
    thresholds: RFMThresholds = DEFAULT_RFM_THRESHOLDS,
) -> str:
    """Gán phân nhóm RFM cho một khách hàng dựa trên Recency và Frequency.

    Args:
        recency_days: Số ngày từ lần mua cuối tới mốc phân tích.
        frequency_orders: Tổng số đơn hàng độc lập.
        thresholds: Đối tượng cấu hình các ngưỡng phân hạng.

    Returns:
        Chuỗi tên phân nhóm RFM tương ứng.
    """
    if (
        recency_days <= thresholds.champions_recency_max_days
        and frequency_orders >= thresholds.champions_frequency_min_orders
    ):
        return RFM_CHAMPIONS
    if frequency_orders >= thresholds.loyal_frequency_min_orders:
        return RFM_LOYAL
    if recency_days > thresholds.at_risk_recency_min_days:
        return RFM_AT_RISK
    return RFM_CASUAL


def classify_abc_class(
    cumulative_revenue_before_percent: float,
    thresholds: ABCThresholds = DEFAULT_ABC_THRESHOLDS,
) -> str:
    """Gán phân lớp Pareto ABC dựa trên tỷ trọng doanh thu tích lũy TRƯỚC sản phẩm hiện tại.

    Sử dụng tỷ trọng doanh thu tích lũy của các sản phẩm đứng trước giúp đảm bảo sản phẩm
    làm tổng doanh thu tích lũy vượt mốc 80% (hoặc 95%) vẫn nằm đúng ở nhóm A (hoặc nhóm B).

    Args:
        cumulative_revenue_before_percent: Tỷ trọng doanh thu tích lũy (%) của sản phẩm đứng trước.
        thresholds: Đối tượng cấu hình các ngưỡng Pareto.

    Returns:
        Chuỗi tên nhóm Pareto ABC tương ứng.
    """
    if cumulative_revenue_before_percent < thresholds.class_a_cumulative_max_percent:
        return ABC_CLASS_A
    if cumulative_revenue_before_percent < thresholds.class_b_cumulative_max_percent:
        return ABC_CLASS_B
    return ABC_CLASS_C


def calculate_rfm_pandas(
    dataframe: pd.DataFrame,
    analysis_date: str | pd.Timestamp | None = None,
    thresholds: RFMThresholds = DEFAULT_RFM_THRESHOLDS,
) -> pd.DataFrame:
    """Phân hạng khách hàng RFM bằng Pandas Engine sử dụng quy tắc chuẩn hóa.

    Args:
        dataframe: DataFrame chứa các dòng giao dịch.
        analysis_date: Mốc ngày tham chiếu (mặc định lấy max Order_Date).
        thresholds: Các ngưỡng phân hạng RFM.

    Returns:
        DataFrame chứa thông tin Customer_ID, Last_Purchase, Frequency, Monetary, Recency, RFM_Segment.
    """
    import pandas as pd

    if dataframe.empty:
        raise ValueError("Không thể phân tích RFM với dữ liệu rỗng")

    df = _filter_metric_rows(dataframe, "rfm")
    if df.empty:
        raise ValueError("Không có order Delivered để phân tích RFM")
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="raise")
    monetary_column = _measure_column(df, "Net_Line_Amount", "Revenue")

    reference_date = (
        pd.Timestamp(analysis_date) if analysis_date is not None else df["Order_Date"].max()
    )

    if (df["Order_Date"] > reference_date).any():
        raise ValueError("Order_Date trong dữ liệu không được lớn hơn analysis_date")

    rfm = (
        df.groupby("Customer_ID")
        .agg(
            Last_Purchase=("Order_Date", "max"),
            Frequency=("Order_ID", "nunique"),
            Monetary=(monetary_column, "sum"),
        )
        .reset_index()
    )

    rfm["Recency"] = (reference_date - rfm["Last_Purchase"]).dt.days

    rfm["RFM_Segment"] = rfm.apply(
        lambda row: classify_rfm_segment(
            recency_days=int(row["Recency"]),
            frequency_orders=int(row["Frequency"]),
            thresholds=thresholds,
        ),
        axis=1,
    )

    return rfm.sort_values("Monetary", ascending=False).reset_index(drop=True)


def calculate_abc_pandas(
    dataframe: pd.DataFrame,
    thresholds: ABCThresholds = DEFAULT_ABC_THRESHOLDS,
) -> pd.DataFrame:
    """Phân loại sản phẩm Pareto ABC bằng Pandas Engine sử dụng quy tắc chuẩn hóa.

    Args:
        dataframe: DataFrame chứa các dòng giao dịch.
        thresholds: Các ngưỡng Pareto ABC.

    Returns:
        DataFrame sản phẩm kèm Total_Revenue, Revenue_Share_Percent, Cumulative_Revenue_Percent, ABC_Class.
    """
    if dataframe.empty:
        raise ValueError("Không thể phân tích ABC với dữ liệu rỗng")

    df = _filter_metric_rows(dataframe, "abc")
    if df.empty:
        raise ValueError("Không có order Delivered để phân tích ABC")
    revenue_column = _measure_column(df, "Net_Line_Amount", "Revenue")
    profit_column = _measure_column(df, "Gross_Profit", "Profit")

    product_sales = (
        df.groupby("Product_Name")
        .agg(
            Total_Orders=("Order_ID", "nunique"),
            Total_Quantity=("Quantity", "sum"),
            Total_Revenue=(revenue_column, "sum"),
            Total_Profit=(profit_column, "sum"),
        )
        .sort_values("Total_Revenue", ascending=False)
        .reset_index()
    )

    grand_total_revenue = float(product_sales["Total_Revenue"].sum())
    if grand_total_revenue <= 0:
        raise ValueError("Không thể phân tích ABC khi tổng doanh thu <= 0")

    product_sales["Revenue_Share_Percent"] = (
        product_sales["Total_Revenue"] / grand_total_revenue
    ) * 100
    product_sales["Cumulative_Revenue_Percent"] = product_sales["Revenue_Share_Percent"].cumsum()

    # Tỷ trọng lũy kế TRƯỚC sản phẩm hiện tại
    product_sales["Cumulative_Before_Percent"] = (
        product_sales["Cumulative_Revenue_Percent"] - product_sales["Revenue_Share_Percent"]
    )

    product_sales["ABC_Class"] = product_sales["Cumulative_Before_Percent"].apply(
        lambda cum_before: classify_abc_class(cum_before, thresholds=thresholds)
    )

    return product_sales.drop(columns=["Cumulative_Before_Percent"])
