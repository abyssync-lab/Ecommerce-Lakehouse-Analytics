"""Module định nghĩa các quy tắc kiểm tra chất lượng dữ liệu (Data Quality Gate & Data Contract).

Có thể sử dụng độc lập trên DataFrame Pandas hoặc tích hợp vào Pipeline PySpark
để ngăn ngừa dữ liệu lỗi đi vào tầng Silver / Gold Lakehouse.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pandas as pd

REQUIRED_COLUMNS: set[str] = {
    "Order_ID",
    "Order_Date",
    "Customer_ID",
    "Product_Name",
    "Quantity",
    "Unit_Price",
    "Discount",
    "Revenue",
    "Cost",
    "Profit",
    "Shipping_Cost",
    "Shipping_Days",
    "Order_Status",
}

ALLOWED_ORDER_STATUSES: set[str] = {"Delivered", "Returned", "Cancelled", "Processing", "Shipped"}

SeverityLevel = Literal["ERROR", "WARNING"]


@dataclass(frozen=True)
class QualityResult:
    """Đại diện cho kết quả kiểm tra của một quy tắc chất lượng dữ liệu (Data Contract Rule)."""

    rule: str
    passed: bool
    detail: str
    severity: SeverityLevel = "ERROR"
    invalid_count: int = 0


@dataclass(frozen=True)
class QualityReportSummary:
    """Tóm tắt báo cáo Data Quality Gate cho một đợt chạy pipeline."""

    raw_rows: int
    valid_rows: int
    invalid_rows: int
    reject_rate_percent: float
    results: list[QualityResult]

    def __iter__(self) -> Iterator[QualityResult]:
        """Hỗ trợ duyệ t từng QualityResult để tương thích ngược với code kiểm thử cũ."""
        return iter(self.results)

    @property
    def passed(self) -> bool:
        """True nếu không có bất kỳ quy tắc ERROR nào bị vi phạm."""
        return all(r.passed for r in self.results if r.severity == "ERROR")


def validate_columns(columns: Iterable[str]) -> QualityResult:
    """Xác minh danh sách các cột trong dữ liệu thô có đủ trường bắt buộc hay không."""
    missing = sorted(REQUIRED_COLUMNS.difference(columns))
    return QualityResult(
        rule="required_columns",
        passed=not missing,
        detail="Đủ các cột bắt buộc" if not missing else f"Thiếu các cột: {', '.join(missing)}",
        severity="ERROR",
        invalid_count=len(missing),
    )


def validate_business_values(df: pd.DataFrame) -> QualityReportSummary:
    """Kiểm tra chuyên sâu các quy tắc Data Contract trên DataFrame (Pandas)."""
    col_res = validate_columns(df.columns)
    if not col_res.passed:
        return QualityReportSummary(
            raw_rows=len(df),
            valid_rows=0,
            invalid_rows=max(1, len(df)),
            reject_rate_percent=100.0,
            results=[col_res],
        )

    results: list[QualityResult] = [col_res]

    # Rule 1: Quantity > 0
    inv_qty = int((df["Quantity"] <= 0).sum())
    results.append(
        QualityResult(
            rule="quantity_positive",
            passed=inv_qty == 0,
            detail=f"Số lượng sản phẩm (Quantity) phải > 0 (Vi phạm: {inv_qty} dòng)",
            severity="ERROR",
            invalid_count=inv_qty,
        )
    )

    # Rule 2: Unit_Price >= 0
    inv_price = int((df["Unit_Price"] < 0).sum())
    results.append(
        QualityResult(
            rule="unit_price_non_negative",
            passed=inv_price == 0,
            detail=f"Đơn giá (Unit_Price) phải >= 0 (Vi phạm: {inv_price} dòng)",
            severity="ERROR",
            invalid_count=inv_price,
        )
    )

    # Rule 3: Discount in [0, 1]
    inv_disc = int((~df["Discount"].between(0, 1)).sum())
    results.append(
        QualityResult(
            rule="discount_range",
            passed=inv_disc == 0,
            detail=f"Tỷ lệ chiết khấu (Discount) phải trong khoảng [0, 1] (Vi phạm: {inv_disc} dòng)",
            severity="ERROR",
            invalid_count=inv_disc,
        )
    )

    # Rule 4: Revenue >= 0
    inv_rev = int((df["Revenue"] < 0).sum())
    results.append(
        QualityResult(
            rule="revenue_non_negative",
            passed=inv_rev == 0,
            detail=f"Doanh thu (Revenue) phải >= 0 (Vi phạm: {inv_rev} dòng)",
            severity="ERROR",
            invalid_count=inv_rev,
        )
    )

    # Rule 5: Shipping_Days >= 0
    inv_ship = int((df["Shipping_Days"] < 0).sum())
    results.append(
        QualityResult(
            rule="shipping_days_non_negative",
            passed=inv_ship == 0,
            detail=f"Số ngày vận chuyển (Shipping_Days) phải >= 0 (Vi phạm: {inv_ship} dòng)",
            severity="ERROR",
            invalid_count=inv_ship,
        )
    )

    # Rule 6: Expected Revenue Formula Check
    expected_rev = df["Quantity"] * df["Unit_Price"] * (1 - df["Discount"])
    inv_formula = int(((df["Revenue"] - expected_rev).abs() > 0.05).sum())
    results.append(
        QualityResult(
            rule="revenue_formula_consistency",
            passed=inv_formula == 0,
            detail=f"Công thức Revenue khớp với Quantity * Unit_Price * (1 - Discount) (Vi phạm: {inv_formula} dòng)",
            severity="WARNING",
            invalid_count=inv_formula,
        )
    )

    # Rule 7: Allowed Order Statuses
    inv_status = int((~df["Order_Status"].isin(ALLOWED_ORDER_STATUSES)).sum())
    results.append(
        QualityResult(
            rule="allowed_order_status",
            passed=inv_status == 0,
            detail=f"Trạng thái đơn hàng thuộc danh sách hợp lệ (Vi phạm: {inv_status} dòng)",
            severity="WARNING",
            invalid_count=inv_status,
        )
    )

    raw_count = len(df)
    error_invalid_count = max(r.invalid_count for r in results if r.severity == "ERROR")
    valid_count = max(0, raw_count - error_invalid_count)
    reject_rate = (error_invalid_count / raw_count * 100) if raw_count > 0 else 0.0

    return QualityReportSummary(
        raw_rows=raw_count,
        valid_rows=valid_count,
        invalid_rows=error_invalid_count,
        reject_rate_percent=round(reject_rate, 2),
        results=results,
    )


def assert_quality(report: QualityReportSummary | Iterable[QualityResult]) -> None:
    """Đảm bảo chất lượng dữ liệu; ném ra ngoại lệ ValueError nếu có quy tắc ERROR bị thất bại."""
    results = report.results if isinstance(report, QualityReportSummary) else list(report)
    failures = [r for r in results if not r.passed and r.severity == "ERROR"]
    if failures:
        messages = "; ".join(f"[{r.rule}] {r.detail}" for r in failures)
        raise ValueError(f"Dữ liệu không đạt chất lượng kiểm duyệt: {messages}")
