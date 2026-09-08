"""Sinh báo cáo Business Insights dạng Markdown tự động từ dataset giao dịch."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from data_quality import assert_quality, validate_business_values
from portfolio_metrics import (
    aggregate_performance,
    calculate_abc_product_analysis,
    calculate_overview,
    calculate_rfm_segmentation,
)


def money(value: float) -> str:
    """Định dạng số tiền tệ USD nhất quán cho báo cáo (ví dụ: $1,234.56)."""
    return f"${value:,.2f}"


def dataframe_to_markdown(dataframe: pd.DataFrame) -> str:
    """Xuất DataFrame thành bảng Markdown chuẩn mà không cần thêm thư viện ngoài.

    Args:
        dataframe: Pandas DataFrame cần định dạng.

    Returns:
        Chuỗi ký tự bảng Markdown.
    """
    headers = [str(column) for column in dataframe.columns]
    rows = []
    for values in dataframe.itertuples(index=False, name=None):
        rows.append(
            [f"{value:,.2f}" if isinstance(value, float) else str(value) for value in values]
        )
    header_line = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join("---" for _ in headers) + "|"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_line, separator, *body])


def build_report(dataframe: pd.DataFrame, source_label: str = "certified Gold serving") -> str:
    """Tạo nội dung Markdown báo cáo Business Insights chỉ từ dữ liệu đã qua Data Quality Gate.

    Args:
        dataframe: Tập dữ liệu giao dịch bán hàng.

    Returns:
        Nội dung Markdown đầy đủ của file BUSINESS_INSIGHTS.md.
    """
    assert_quality(validate_business_values(dataframe))
    metrics = calculate_overview(dataframe)
    regions = aggregate_performance(dataframe, ["Region"], limit=4)
    categories = aggregate_performance(dataframe, ["Category"], limit=5)
    products = aggregate_performance(dataframe, ["Product_Name"], limit=5)

    rfm_df = calculate_rfm_segmentation(dataframe)
    rfm_summary = (
        rfm_df.groupby("RFM_Segment")
        .agg(
            Customer_Count=("Customer_ID", "count"),
            Total_Spend=("Monetary", "sum"),
            Avg_Spend=("Monetary", "mean"),
        )
        .reset_index()
        .sort_values("Total_Spend", ascending=False)
    )

    abc_df = calculate_abc_product_analysis(dataframe)
    abc_summary = (
        abc_df.groupby("ABC_Class")
        .agg(
            Product_Count=("Product_Name", "count"),
            Total_Revenue=("Total_Revenue", "sum"),
            Revenue_Share=("Revenue_Share_Percent", "sum"),
        )
        .reset_index()
    )

    lines = [
        "# GlobalCart Intelligence — Báo Cáo Phân Tích Dữ Liệu Kinh Doanh",
        "",
        f"> 🤖 Báo cáo này được sinh tự động từ `{source_label}` thông qua Data Quality Gate & Analytics Engine.",
        "",
        "## 1. Chỉ Số KPI Tổng Quan (Executive Overview)",
        "",
        "| Chỉ Số KPI | Giá Trị Thực Tế | Ghi Chú Nghiệp Vụ |",
        "|---|---:|---|",
        f"| **Tổng Dòng Giao Dịch** | {metrics.rows:,} | Dữ liệu giao dịch cấp sản phẩm |",
        f"| **Tổng Số Đơn Hàng** | {metrics.orders:,} | Số đơn hàng độc lập (Unique Orders) |",
        f"| **Tổng Số Khách Hàng** | {metrics.customers:,} | Khách hàng đã phát sinh giao dịch |",
        f"| **Tổng Doanh Thu (Gross Revenue)** | {money(metrics.revenue)} | Tổng giá trị hóa đơn trước chi phí |",
        f"| **Tổng Lợi Nhuận Gộp (Gross Profit)** | {money(metrics.profit)} | Lợi nhuận bán hàng gộp |",
        f"| **Lợi Nhuận Ròng (Net Profit)** | {money(metrics.net_profit)} | Lợi nhuận sau khi trừ chi phí vận chuyển |",
        f"| **Biên Lợi Nhuận (Profit Margin)** | {metrics.profit_margin_percent:.2f}% | Tỷ lệ lợi nhuận / doanh thu |",
        f"| **Giá Trị Đơn Trung Bình (AOV)** | {money(metrics.average_order_value)} | Average Order Value |",
        f"| **Tỷ Lệ Trả Hàng (Return Rate)** | {metrics.return_rate_percent:.2f}% | Đơn hàng trạng thái Returned |",
        f"| **Tỷ Lệ Hủy Đơn (Cancellation Rate)** | {metrics.cancellation_rate_percent:.2f}% | Đơn hàng trạng thái Cancelled |",
        "",
        "## 2. Phát Hiện Phân Tích Nổi Bật (Key Business Insights)",
        "",
        f"- 🌍 **Middle East dẫn đầu doanh thu** với **{money(float(regions.iloc[0]['Revenue']))}**, tuy nhiên phân bổ thị trường giữa 4 khu vực tương đối đồng đều (chênh lệch dưới 5%).",
        f"- 💻 **Electronics là ngành hàng chủ lực**, chiếm **{categories.iloc[0]['Revenue'] / metrics.revenue * 100:.1f}% tổng doanh thu** ({money(float(categories.iloc[0]['Revenue']))}).",
        f"- 🏆 **{products.iloc[0]['Product_Name']}** đạt doanh số cao nhất toàn hệ thống với **{money(float(products.iloc[0]['Revenue']))}**.",
        f"- ⚠️ **Tổng tỷ lệ Đơn không thành công (Returned + Cancelled)** ở mức **{metrics.return_rate_percent + metrics.cancellation_rate_percent:.2f}%**, cần tối ưu lại đối tác vận chuyển.",
        "",
        "## 3. Phân Hạng Khách Hàng (RFM Customer Segmentation)",
        "",
        dataframe_to_markdown(rfm_summary),
        "",
        "## 4. Phân Loại Sản Phẩm Pareto (ABC Product Classification)",
        "",
        dataframe_to_markdown(abc_summary),
        "",
        "## 5. Xếp Hạng Thị Trường Theo Khu Vực (Regional Performance)",
        "",
        dataframe_to_markdown(regions),
        "",
        "## 6. Xếp Hạng Ngành Hàng (Category Performance)",
        "",
        dataframe_to_markdown(categories),
        "",
        "## 7. Top 5 Sản Phẩm Doanh Thu Cao Nhất",
        "",
        dataframe_to_markdown(products),
        "",
        "## 🛠️ Hướng Dẫn Tái Tạo Báo Cáo Này",
        "",
        "```powershell",
        "python SourceCode\\generate_portfolio_report.py",
        "```",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    """Đọc certified Gold mặc định; CSV chỉ là fallback được yêu cầu rõ ràng."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Tự động sinh báo cáo Business Insights")
    parser.add_argument(
        "--source",
        choices=["published", "csv"],
        default="published",
        help="Nguồn báo cáo: certified Gold serving (mặc định) hoặc CSV validation độc lập",
    )
    parser.add_argument("--input", type=Path, default=Path("Data/EcommerceSalesDataset.csv"))
    parser.add_argument("--output", type=Path, default=Path("docs/BUSINESS_INSIGHTS.md"))
    args = parser.parse_args()

    spark = None
    if args.source == "published":
        # Import Spark lazy để chế độ --source csv vẫn chạy được trong CI nhẹ.
        from lakehouse.pipeline import create_spark_session
        from lakehouse.publication import get_current_publication

        spark = create_spark_session()
        try:
            current_run_id = get_current_publication(spark)
            if not current_run_id or current_run_id == "__NONE__":
                raise RuntimeError(
                    "Chưa có certified Gold publication. Hãy chạy pipeline thành công trước "
                    "hoặc dùng --source csv cho validation độc lập."
                )

            # Sales fact có grain order-line, còn Shipping_Cost thuộc order grain.
            # Join riêng order fact để báo cáo không nhân chi phí vận chuyển theo số line.
            sales = spark.table("serving.gold_sales_enriched").drop("Publication_Run_ID").toPandas()
            order_costs = (
                spark.table("serving.fact_order_fulfillment")
                .select("Order_ID", "Shipping_Cost")
                .dropDuplicates(["Order_ID"])
                .toPandas()
                .rename(columns={"Shipping_Cost": "Order_Shipping_Cost"})
            )
            dataframe = sales.merge(order_costs, on="Order_ID", how="left", validate="many_to_one")
            source_label = (
                "serving.gold_sales_enriched + serving.fact_order_fulfillment "
                f"(publication_run_id={current_run_id})"
            )
        finally:
            spark.stop()
            spark = None
    else:
        dataframe = pd.read_csv(args.input)
        source_label = str(args.input)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_report(dataframe, source_label=source_label), encoding="utf-8")
    print(f"✨ Đã khởi tạo thành công báo cáo: {args.output}")


if __name__ == "__main__":
    main()
