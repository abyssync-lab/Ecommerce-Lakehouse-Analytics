"""Tính KPI, RFM và ABC độc lập với Spark.

Module này dùng Pandas cho kiểm thử đơn vị và kiểm tra chéo kết quả Gold.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from analytics_rules import calculate_abc_pandas, calculate_rfm_pandas

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class OverviewMetrics:
    """Nhóm KPI tổng quan của dữ liệu bán hàng.

    Attributes:
        rows: Tổng số dòng bản ghi giao dịch trong tập dữ liệu.
        orders: Tổng số đơn hàng phân biệt (Unique Order IDs).
        customers: Tổng số khách hàng phân biệt (Unique Customer IDs).
        revenue: Tổng doanh thu ($).
        profit: Tổng lợi nhuận gộp ($).
        net_profit: Lợi nhuận ròng sau khi trừ chi phí vận chuyển ($).
        profit_margin_percent: Biên lợi nhuận gộp (%).
        average_order_value: Giá trị trung bình trên mỗi đơn hàng AOV ($).
        return_rate_percent: Tỷ lệ đơn hàng bị hoàn trả (%).
        cancellation_rate_percent: Tỷ lệ đơn hàng bị hủy (%).
    """

    rows: int
    orders: int
    customers: int
    revenue: float
    profit: float
    net_profit: float
    profit_margin_percent: float
    average_order_value: float
    return_rate_percent: float
    cancellation_rate_percent: float

    def to_dict(self) -> dict[str, int | float]:
        """Chuyển đổi kết quả chỉ số KPI thành kiểu Dictionary."""
        return asdict(self)


def safe_ratio(numerator: float, denominator: float) -> float:
    """Tính tỷ lệ chia an toàn, tự động trả về 0.0 nếu mẫu số bằng 0.

    Args:
        numerator: Tử số.
        denominator: Mẫu số.

    Returns:
        Kết quả phép chia hoặc 0.0 nếu mẫu số là 0.
    """
    return numerator / denominator if denominator else 0.0


def _normalise_reporting_measures(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Đưa measure Gold về tên báo cáo; ưu tiên số liệu đã tính lại ở Silver."""
    result = dataframe.copy()
    if "Revenue" not in result.columns and "Net_Line_Amount" in result.columns:
        result["Revenue"] = result["Net_Line_Amount"]
    if "Profit" not in result.columns and "Gross_Profit" in result.columns:
        result["Profit"] = result["Gross_Profit"]
    if "Shipping_Cost" not in result.columns and "Order_Shipping_Cost" in result.columns:
        result["Shipping_Cost"] = result["Order_Shipping_Cost"]
    required = {"Revenue", "Profit"}.difference(result.columns)
    if required:
        raise ValueError(f"Thiếu Gold measures cho báo cáo: {sorted(required)}")
    return result


def _order_level_shipping_cost(dataframe: pd.DataFrame) -> float:
    """Tính chi phí vận chuyển một lần cho mỗi order, không nhân theo số line."""
    if "Shipping_Cost" not in dataframe.columns:
        return 0.0
    return float(
        dataframe[["Order_ID", "Shipping_Cost"]]
        .drop_duplicates("Order_ID")
        .loc[:, "Shipping_Cost"]
        .fillna(0.0)
        .sum()
    )


def calculate_overview(dataframe: pd.DataFrame) -> OverviewMetrics:
    """Tính toán toàn bộ KPI tổng quan theo đúng Grain đơn hàng (Order Grain).

    Args:
        dataframe: Pandas DataFrame chứa tập dữ liệu giao dịch bán hàng.

    Returns:
        Đối tượng OverviewMetrics chứa các con số KPI đã được làm tròn.
    """
    dataframe = _normalise_reporting_measures(dataframe)
    order_count = int(dataframe["Order_ID"].nunique())
    revenue = float(dataframe["Revenue"].sum())
    profit = float(dataframe["Profit"].sum())
    shipping_cost = _order_level_shipping_cost(dataframe)

    # Lọc đơn hàng phân biệt để tính tỷ lệ hủy/trả hàng chính xác
    status_by_order = dataframe[["Order_ID", "Order_Status"]].drop_duplicates("Order_ID")
    returned_orders = int(status_by_order["Order_Status"].eq("Returned").sum())
    cancelled_orders = int(status_by_order["Order_Status"].eq("Cancelled").sum())

    return OverviewMetrics(
        rows=len(dataframe),
        orders=order_count,
        customers=int(dataframe["Customer_ID"].nunique()),
        revenue=round(revenue, 2),
        profit=round(profit, 2),
        net_profit=round(profit - shipping_cost, 2),
        profit_margin_percent=round(safe_ratio(profit, revenue) * 100, 2),
        average_order_value=round(safe_ratio(revenue, order_count), 2),
        return_rate_percent=round(safe_ratio(returned_orders, order_count) * 100, 2),
        cancellation_rate_percent=round(safe_ratio(cancelled_orders, order_count) * 100, 2),
    )


def aggregate_performance(
    dataframe: pd.DataFrame, dimensions: list[str], limit: int = 10
) -> pd.DataFrame:
    """Tổng hợp chỉ số kinh doanh theo các chiều (Dimensions) và sắp xếp giảm dần theo Doanh thu.

    Args:
        dataframe: Tập dữ liệu giao dịch bán hàng.
        dimensions: Danh sách tên cột phân nhóm (ví dụ: ['Region'], ['Category']).
        limit: Số lượng dòng tối đa cần lấy.

    Returns:
        DataFrame đã tổng hợp các chỉ số Orders, Revenue, Profit, Profit_Margin_Percent.
    """
    dataframe = _normalise_reporting_measures(dataframe)
    return (
        dataframe.groupby(dimensions, dropna=False)
        .agg(
            Orders=("Order_ID", "nunique"),
            Revenue=("Revenue", "sum"),
            Profit=("Profit", "sum"),
        )
        .assign(
            Profit_Margin_Percent=lambda frame: (
                frame["Profit"] / frame["Revenue"].replace(0, float("nan")) * 100
            ).fillna(0.0)
        )
        .sort_values("Revenue", ascending=False)
        .head(limit)
        .reset_index()
    )


def calculate_rfm_segmentation(
    dataframe: pd.DataFrame, analysis_date: str | pd.Timestamp | None = None
) -> pd.DataFrame:
    """Phân hạng khách hàng theo mô hình RFM (Recency, Frequency, Monetary).

    Ủy quyền cho module quy tắc chung `analytics_rules.calculate_rfm_pandas`.
    """
    return calculate_rfm_pandas(dataframe, analysis_date=analysis_date)


def calculate_abc_product_analysis(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Phân loại sản phẩm theo nguyên lý Pareto (Phân tích ABC Analysis).

    Ủy quyền cho module quy tắc chung `analytics_rules.calculate_abc_pandas`.
    """
    return calculate_abc_pandas(dataframe)
