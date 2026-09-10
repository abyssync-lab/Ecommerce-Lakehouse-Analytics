"""Mô-đun xây dựng FactSales và các Data Mart ở tầng Gold của Data Lakehouse."""

from __future__ import annotations

import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from analytics_rules import (
    ABC_CLASS_A,
    ABC_CLASS_B,
    ABC_CLASS_C,
    ABC_RULE_VERSION,
    RFM_AT_RISK,
    RFM_CASUAL,
    RFM_CHAMPIONS,
    RFM_LOYAL,
    RFM_RULE_VERSION,
)
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    date_format,
    datediff,
    first,
    lit,
    round,
    when,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

from .dimensions import add_surrogate_key

LOGGER = logging.getLogger(__name__)
BUSINESS_METRICS_PATH = Path(__file__).resolve().parents[2] / "contracts" / "business_metrics.yaml"
SHIPPING_SLA_PATH = Path(__file__).resolve().parents[2] / "contracts" / "shipping_sla.yaml"


def _ensure_gold_measures(dataframe: Any) -> Any:
    """Chuẩn hóa measure Gold, không dùng revenue/profit nguồn thay số liệu tính lại."""
    result = dataframe
    if "Net_Line_Amount" not in result.columns:
        if "Revenue" in result.columns:
            result = result.withColumn("Net_Line_Amount", col("Revenue"))
        else:
            result = result.withColumn(
                "Net_Line_Amount",
                col("Quantity") * col("Unit_Price") * (lit(1.0) - col("Discount")),
            )
    if "Gross_Profit" not in result.columns:
        if "Profit" in result.columns:
            result = result.withColumn("Gross_Profit", col("Profit"))
        else:
            result = result.withColumn("Gross_Profit", col("Net_Line_Amount") - col("Cost_Amount"))
    if "Cost_Amount" not in result.columns:
        result = result.withColumn("Cost_Amount", col("Quantity") * col("Cost"))
    if "Gross_Amount" not in result.columns:
        result = result.withColumn("Gross_Amount", col("Quantity") * col("Unit_Price"))
    if "Discount_Amount" not in result.columns:
        result = result.withColumn("Discount_Amount", col("Gross_Amount") * col("Discount"))
    return result


@lru_cache(maxsize=1)
def _load_shipping_sla() -> tuple[int, dict[str, int]]:
    """Đọc SLA từ contract, không hard-code rule trong transformation."""
    data = yaml.safe_load(SHIPPING_SLA_PATH.read_text(encoding="utf-8")) or {}
    default_days = int(data.get("default_sla_days", 7))
    method_days = {
        str(method): int(days) for method, days in (data.get("shipping_methods", {}) or {}).items()
    }
    return default_days, method_days


def _sla_days_expression(method_column: Any) -> Any:
    """Sinh Spark expression SLA_Days từ shipping_sla.yaml."""
    default_days, method_days = _load_shipping_sla()
    result = lit(default_days)
    for method, days in method_days.items():
        result = when(method_column == method, lit(days)).otherwise(result)
    return result


@lru_cache(maxsize=1)
def load_business_policy() -> dict[str, list[str]]:
    """Đọc status policy từ contract thay vì hard-code trong từng mart."""
    if not BUSINESS_METRICS_PATH.exists():
        raise FileNotFoundError(f"Thiếu business metric contract: {BUSINESS_METRICS_PATH}")

    data = yaml.safe_load(BUSINESS_METRICS_PATH.read_text(encoding="utf-8")) or {}
    policy: dict[str, list[str]] = {}
    for metric_name in (
        "rfm",
        "abc",
        "recognized_revenue",
        "cancelled_value",
    ):
        statuses = data.get(metric_name, {}).get("included_statuses", ["Delivered"])
        if not statuses:
            raise ValueError(f"Business policy {metric_name} không có included_statuses")
        policy[metric_name] = [str(status) for status in statuses]
    policy["return_numerator"] = [
        str(status) for status in data.get("return_rate", {}).get("numerator_status", [])
    ]
    policy["return_denominator"] = [
        str(status) for status in data.get("return_rate", {}).get("denominator_status", [])
    ]
    return policy


def _filter_policy_rows(clean_df: Any, metric_name: str) -> Any:
    """Lọc đúng các trạng thái được phép của một metric Gold."""
    statuses = load_business_policy()[metric_name]
    return clean_df.filter(col("Order_Status").isin(statuses))


def build_fact_sales(
    clean_df: Any,
    dimensions: dict[str, Any],
) -> Any:
    """Xây dựng FactSales theo đúng Grain: 1 dòng = 1 sản phẩm trong 1 đơn hàng."""
    LOGGER.info("Bắt đầu xây dựng FactSales table...")

    fact = _ensure_gold_measures(clean_df).withColumn(
        "DateKey", date_format(col("Order_Date"), "yyyyMMdd").cast("int")
    )

    product_dim = dimensions["dim_product"]
    if "Product_ID" in fact.columns and "Product_ID" in product_dim.columns:
        fact = fact.join(
            product_dim.select("Product_ID", "ProductKey"),
            on=["Product_ID"],
            how="left",
        )
    else:
        fact = fact.join(
            product_dim,
            on=["Product_Name", "Category", "Sub_Category"],
            how="left",
        )

    dim_cust = dimensions["dim_customer"]
    if "ValidFrom" in dim_cust.columns and "ValidTo" in dim_cust.columns:
        # Join theo thời gian với SCD Type 2.
        fact_event_time = (
            col("Source_Updated_At") if "Source_Updated_At" in fact.columns else col("Order_Date")
        )
        fact = fact.join(
            dim_cust,
            (fact["Customer_ID"] == dim_cust["Customer_ID"])
            & (fact_event_time >= dim_cust["ValidFrom"])
            & (fact_event_time < dim_cust["ValidTo"]),
            how="left",
        ).drop(dim_cust["Customer_ID"])
    else:
        # Join SCD Type 1 chuẩn; dim_customer bảo đảm mỗi Customer_ID chỉ có 1 dòng.
        fact = fact.join(dim_cust, on=["Customer_ID"], how="left")
    fact = fact.join(dimensions["dim_geography"], on=["Region", "Country"], how="left")
    fact = fact.join(
        dimensions["dim_order_context"],
        on=[
            "Order_Status",
            "Payment_Method",
            "Shipping_Method",
            "Delivery_Level",
        ],
        how="left",
    )

    order_cols = ["Order_ID"]
    if "Order_Line_ID" in fact.columns:
        order_cols.append("Order_Line_ID")
    elif "Product_ID" in fact.columns:
        order_cols.append("Product_ID")

    fact = add_surrogate_key(
        fact,
        "SalesKey",
        order_cols,
    )

    fact_cols = ["SalesKey", "Order_ID"]
    if "Order_Line_ID" in fact.columns:
        fact_cols.append("Order_Line_ID")
    if "Product_ID" in fact.columns:
        fact_cols.append("Product_ID")
    fact_cols.extend(
        [
            "DateKey",
            "Order_Status",
            "CustomerKey",
            "GeographyKey",
            "ProductKey",
            "ContextKey",
            "Unit_Price",
            "Quantity",
            "Discount",
            "Gross_Amount",
            "Discount_Amount",
            "Net_Line_Amount",
            "Cost_Amount",
            "Gross_Profit",
            "Source_Revenue",
            "Source_Profit",
        ]
    )

    fact_sales = fact.select(*fact_cols)

    LOGGER.info("FactSales đã được khởi tạo thành công (%d dòng).", fact_sales.count())
    return fact_sales


def build_fact_order_fulfillment(
    silver_orders_current: Any,
    silver_order_lines_current: Any,
    dimensions: dict[str, Any],
) -> Any:
    """Xây fact order grain, không nhân bản Shipping_Cost theo số line."""
    lines = _ensure_gold_measures(silver_order_lines_current.filter(~col("Is_Deleted")))
    line_metrics = lines.groupBy("Order_ID").agg(
        countDistinct("Order_Line_ID").alias("Line_Count"),
        spark_sum("Quantity").alias("Total_Quantity"),
        round(spark_sum("Net_Line_Amount"), 2).alias("Order_Value"),
        round(spark_sum("Gross_Profit"), 2).alias("Order_Profit"),
    )
    # Fact order chỉ đại diện cho order còn ít nhất một dòng hiện hành. Nếu dùng
    # left join, order đã xóa toàn bộ line sẽ vẫn xuất hiện với measure NULL.
    orders = silver_orders_current.filter(~col("Is_Deleted")).join(
        line_metrics, on="Order_ID", how="inner"
    )
    orders = orders.withColumn("DateKey", date_format(col("Order_Date"), "yyyyMMdd").cast("int"))
    customer_dim = dimensions["dim_customer"]
    if {"ValidFrom", "ValidTo"}.issubset(
        set(customer_dim.columns)
    ) and "Source_Updated_At" in orders.columns:
        orders = orders.join(
            customer_dim.select("Customer_ID", "CustomerKey", "ValidFrom", "ValidTo"),
            (orders["Customer_ID"] == customer_dim["Customer_ID"])
            & (orders["Source_Updated_At"] >= customer_dim["ValidFrom"])
            & (orders["Source_Updated_At"] < customer_dim["ValidTo"]),
            how="left",
        ).drop(customer_dim["Customer_ID"], "ValidFrom", "ValidTo")
    else:
        orders = orders.join(
            customer_dim.select("Customer_ID", "CustomerKey"),
            on="Customer_ID",
            how="left",
        )
    orders = orders.join(dimensions["dim_geography"], on=["Region", "Country"], how="left")
    orders = orders.join(
        dimensions["dim_order_context"],
        on=[
            "Order_Status",
            "Payment_Method",
            "Shipping_Method",
            "Delivery_Level",
        ],
        how="left",
    )
    orders = add_surrogate_key(orders, "OrderKey", ["Order_ID"])
    return orders.select(
        "OrderKey",
        "Order_ID",
        "Order_Date",
        "Customer_ID",
        "Region",
        "Country",
        "Order_Status",
        "Payment_Method",
        "Shipping_Method",
        "DateKey",
        "CustomerKey",
        "GeographyKey",
        "ContextKey",
        "Line_Count",
        "Total_Quantity",
        "Order_Value",
        "Order_Profit",
        "Shipping_Cost",
        "Shipping_Days",
        _sla_days_expression(col("Shipping_Method")).alias("SLA_Days"),
        when(col("Shipping_Days").isNull(), lit(None).cast("boolean"))
        .when(col("Shipping_Days") > _sla_days_expression(col("Shipping_Method")), lit(True))
        .otherwise(lit(False))
        .alias("Is_SLA_Breached"),
        when(col("Order_Status") == "Delivered", True).otherwise(False).alias("Is_Delivered"),
        when(col("Order_Status") == "Returned", True).otherwise(False).alias("Is_Returned"),
        when(col("Order_Status") == "Cancelled", True).otherwise(False).alias("Is_Cancelled"),
    )


def build_rfm_mart(clean_df: Any, analysis_date: str | None = None) -> Any:
    """Xây dựng RFM từ các đơn Delivered theo business policy v1."""
    delivered_df = _filter_policy_rows(_ensure_gold_measures(clean_df), "rfm")
    if delivered_df.limit(1).count() == 0:
        return clean_df.sparkSession.createDataFrame(
            [],
            "Customer_ID string, Last_Purchase date, Frequency long, "
            "Monetary double, Recency int, RFM_Segment string, Rule_Version string",
        )

    if analysis_date is not None:
        max_date_val = analysis_date
    else:
        max_date_val = delivered_df.select(spark_max("Order_Date")).collect()[0][0]

    rfm_base = (
        delivered_df.groupBy("Customer_ID")
        .agg(
            spark_max("Order_Date").alias("Last_Purchase"),
            countDistinct("Order_ID").alias("Frequency"),
            round(spark_sum("Net_Line_Amount"), 2).alias("Monetary"),
        )
        .withColumn("Recency", datediff(lit(max_date_val), col("Last_Purchase")))
    )

    rfm_mart = (
        rfm_base.withColumn(
            "RFM_Segment",
            when((col("Recency") <= 30) & (col("Frequency") >= 3), RFM_CHAMPIONS)
            .when(col("Frequency") >= 3, RFM_LOYAL)
            .when(col("Recency") > 90, RFM_AT_RISK)
            .otherwise(RFM_CASUAL),
        )
        .withColumn("Rule_Version", lit(RFM_RULE_VERSION))
        .orderBy(col("Monetary").desc())
    )

    return rfm_mart


def build_abc_mart(clean_df: Any) -> Any:
    """Xây dựng Pareto ABC từ Delivered Revenue theo business policy v1."""
    delivered_df = _filter_policy_rows(_ensure_gold_measures(clean_df), "abc")
    total_rev = delivered_df.select(spark_sum("Net_Line_Amount")).collect()[0][0]
    if not total_rev or total_rev <= 0:
        return clean_df.sparkSession.createDataFrame(
            [],
            "Product_Name string, Category string, Total_Quantity long, "
            "Total_Revenue double, Total_Profit double, Cumulative_Revenue double, "
            "Cumulative_Before_Percent double, Cum_Percent double, ABC_Class string, "
            "Rule_Version string",
        )

    prod_base = delivered_df.groupBy("Product_Name", "Category").agg(
        spark_sum("Quantity").alias("Total_Quantity"),
        round(spark_sum("Net_Line_Amount"), 2).alias("Total_Revenue"),
        round(spark_sum("Gross_Profit"), 2).alias("Total_Profit"),
    )

    # Thêm Product_Name để thứ tự phân loại ổn định khi hai sản phẩm cùng doanh thu.
    window_spec = Window.orderBy(col("Total_Revenue").desc(), col("Product_Name").asc())

    abc_mart = (
        prod_base.withColumn("Cumulative_Revenue", spark_sum("Total_Revenue").over(window_spec))
        .withColumn(
            "Cumulative_Before_Percent",
            round(
                ((col("Cumulative_Revenue") - col("Total_Revenue")) / total_rev) * 100,
                2,
            ),
        )
        .withColumn("Cum_Percent", round((col("Cumulative_Revenue") / total_rev) * 100, 2))
        .withColumn(
            "ABC_Class",
            when(col("Cumulative_Before_Percent") < 80.0, ABC_CLASS_A)
            .when(col("Cumulative_Before_Percent") < 95.0, ABC_CLASS_B)
            .otherwise(ABC_CLASS_C),
        )
        .withColumn("Rule_Version", lit(ABC_RULE_VERSION))
        .orderBy(col("Total_Revenue").desc())
    )

    return abc_mart


def build_mart_order_summary(clean_df: Any) -> Any:
    """Xây dựng Data Mart tổng hợp ở mức Đơn Hàng (Order Grain), phân tách rành mạch với FactSales (Line-Item Grain)."""
    clean_df = _ensure_gold_measures(clean_df)
    return (
        clean_df.groupBy("Order_ID", "Order_Date", "Customer_ID", "Order_Status")
        .agg(
            spark_sum("Quantity").alias("Total_Items"),
            round(spark_sum("Net_Line_Amount"), 2).alias("Order_Total_Revenue"),
            round(spark_sum("Cost_Amount"), 2).alias("Order_Total_Cost"),
            round(spark_sum("Gross_Profit"), 2).alias("Order_Total_Profit"),
            first("Shipping_Cost", ignorenulls=True).alias("Order_Shipping_Cost"),
        )
        .orderBy(col("Order_Total_Revenue").desc())
    )


def build_gold_marts(
    sales_enriched: Any,
    fact_order_fulfillment: Any,
    publication_as_of_date: str | None = None,
) -> dict[str, Any]:
    """Tạo sáu mart nghiệp vụ từ cùng line/order facts đã chuẩn hóa."""
    sales = _ensure_gold_measures(sales_enriched)
    orders = fact_order_fulfillment
    policy = load_business_policy()
    delivered = col("Order_Status").isin(policy["recognized_revenue"])
    returned = col("Order_Status").isin(policy["return_numerator"])
    cancelled = col("Order_Status").isin(policy["cancelled_value"])

    executive = orders.groupBy("Order_Date").agg(
        countDistinct("Order_ID").alias("Total_Orders"),
        countDistinct(when(delivered, col("Order_ID"))).alias("Delivered_Orders"),
        countDistinct(when(returned, col("Order_ID"))).alias("Returned_Orders"),
        countDistinct(
            when(col("Order_Status").isin(policy["return_denominator"]), col("Order_ID"))
        ).alias("Return_Denominator_Orders"),
        countDistinct(when(cancelled, col("Order_ID"))).alias("Cancelled_Orders"),
        round(spark_sum(when(delivered, col("Order_Value")).otherwise(0.0)), 2).alias(
            "Delivered_Revenue"
        ),
        round(spark_sum(when(returned, col("Order_Value")).otherwise(0.0)), 2).alias(
            "Returned_Value"
        ),
        round(spark_sum(when(delivered, col("Order_Profit")).otherwise(0.0)), 2).alias(
            "Delivered_Profit"
        ),
    )
    executive = (
        executive.withColumn(
            "AOV",
            when(col("Delivered_Orders") > 0, col("Delivered_Revenue") / col("Delivered_Orders")),
        )
        .withColumn(
            "Return_Rate",
            when(
                col("Return_Denominator_Orders") > 0,
                col("Returned_Orders") / col("Return_Denominator_Orders"),
            ),
        )
        .withColumn(
            "Cancellation_Rate",
            when(col("Total_Orders") > 0, col("Cancelled_Orders") / col("Total_Orders")),
        )
    )

    product = (
        sales.groupBy("Product_ID", "Product_Name", "Category")
        .agg(
            countDistinct("Order_ID").alias("Orders"),
            spark_sum("Quantity").alias("Quantity"),
            round(spark_sum(when(delivered, col("Net_Line_Amount")).otherwise(0.0)), 2).alias(
                "Delivered_Revenue"
            ),
            round(spark_sum(when(delivered, col("Gross_Profit")).otherwise(0.0)), 2).alias(
                "Delivered_Profit"
            ),
            countDistinct(when(delivered, col("Order_ID"))).alias("Delivered_Orders"),
            countDistinct(when(returned, col("Order_ID"))).alias("Returned_Orders"),
        )
        .withColumn(
            "Return_Rate",
            when(
                (col("Delivered_Orders") + col("Returned_Orders")) > 0,
                col("Returned_Orders") / (col("Delivered_Orders") + col("Returned_Orders")),
            ),
        )
        .withColumn(
            "Profit_Margin",
            when(col("Delivered_Revenue") != 0, col("Delivered_Profit") / col("Delivered_Revenue")),
        )
    )

    geography = sales.groupBy("Region", "Country").agg(
        countDistinct("Order_ID").alias("Orders"),
        round(spark_sum(when(delivered, col("Net_Line_Amount")).otherwise(0.0)), 2).alias(
            "Delivered_Revenue"
        ),
        round(spark_sum(when(delivered, col("Gross_Profit")).otherwise(0.0)), 2).alias(
            "Delivered_Profit"
        ),
    )

    fulfillment = (
        orders.groupBy("Shipping_Method")
        .agg(
            countDistinct("Order_ID").alias("Total_Shipments"),
            round(avg("Shipping_Days"), 2).alias("Average_Shipping_Days"),
            round(avg("Shipping_Cost"), 2).alias("Average_Shipping_Cost"),
            countDistinct(when(col("Is_SLA_Breached"), col("Order_ID"))).alias("SLA_Breach_Count"),
        )
        .withColumn(
            "SLA_Breach_Rate",
            when(
                col("Total_Shipments") > 0,
                col("SLA_Breach_Count") / col("Total_Shipments"),
            ),
        )
    )

    analysis_date = publication_as_of_date or datetime.today().date().isoformat()
    return {
        "mart_executive_daily": executive,
        "mart_product_performance": product,
        "mart_geography_performance": geography,
        "mart_fulfillment_sla": fulfillment,
        "mart_customer_rfm": build_rfm_mart(sales, analysis_date=analysis_date),
        "mart_product_abc": build_abc_mart(sales),
    }


def build_sales_enriched(fact_sales: Any, dimensions: dict[str, Any]) -> Any:
    """Xây dựng Gold Semantic Base (`gold_sales_enriched`) kết nối FactSales và Dimensions.

    Tạo một nguồn dữ liệu kinh doanh duy nhất cho toàn bộ Gold Marts,
    đảm bảo tính nhất quán tuyệt đối giữa mô hình Kimball và các Mart phục vụ BI / Reporting.
    """
    enriched = fact_sales

    dim_product = dimensions.get("dim_product")
    if dim_product is not None and "ProductKey" in enriched.columns:
        product_columns = [
            column
            for column in ["Product_ID", "Product_Name", "Category", "Sub_Category"]
            if column in dim_product.columns and column not in enriched.columns
        ]
        enriched = enriched.join(
            dim_product.select("ProductKey", *product_columns),
            on="ProductKey",
            how="left",
        )

    dim_customer = dimensions.get("dim_customer")
    if dim_customer is not None and "CustomerKey" in enriched.columns:
        cust_cols = [
            c
            for c in dim_customer.columns
            if c in ["CustomerKey", "Customer_ID", "Customer_Gender", "Customer_Segment"]
        ]
        enriched = enriched.join(dim_customer.select(*cust_cols), on="CustomerKey", how="left")

    dim_geography = dimensions.get("dim_geography")
    if dim_geography is not None and "GeographyKey" in enriched.columns:
        enriched = enriched.join(dim_geography, on="GeographyKey", how="left")

    dim_context = dimensions.get("dim_order_context")
    if dim_context is not None and "ContextKey" in enriched.columns:
        context_columns = [
            column
            for column in dim_context.columns
            if column == "ContextKey" or column not in enriched.columns
        ]
        enriched = enriched.join(dim_context.select(*context_columns), on="ContextKey", how="left")

    dim_date = dimensions.get("dim_date")
    if dim_date is not None and "DateKey" in enriched.columns:
        date_cols = ["DateKey"]
        if "Year" in dim_date.columns:
            date_cols.append("Year")
        if "Month" in dim_date.columns:
            date_cols.append("Month")
        if "Quarter" in dim_date.columns:
            date_cols.append("Quarter")

        # FactSales chỉ giữ DateKey để tránh lặp thuộc tính ngày. Semantic base phải
        # khôi phục lại ngày nghiệp vụ với tên Order_Date mà toàn bộ mart đang dùng.
        if "Order_Date" not in enriched.columns:
            if "FullDate" in dim_date.columns:
                date_cols.append(dim_date["FullDate"].alias("Order_Date"))
            elif "Date" in dim_date.columns:
                date_cols.append(dim_date["Date"].alias("Order_Date"))
            elif "Order_Date" in dim_date.columns:
                date_cols.append("Order_Date")

        enriched = enriched.join(dim_date.select(*date_cols), on="DateKey", how="left")

    return enriched


def build_all_marts(clean_df: Any) -> dict[str, Any]:
    """Tạo bộ mart nhỏ cho các kiểm thử semantic tương thích.

    Production luôn gọi ``build_gold_marts``. Hàm này chỉ giữ overview, order
    summary và RFM để các kiểm thử API cũ không kéo theo một danh sách mart
    trùng lặp với Gold production.
    """
    LOGGER.info("Bắt đầu xây dựng semantic smoke marts...")

    clean_df = _ensure_gold_measures(clean_df)
    overview = clean_df.agg(
        countDistinct("Order_ID").alias("Total_Orders"),
        spark_sum("Quantity").alias("Total_Quantity"),
        round(spark_sum("Net_Line_Amount"), 2).alias("Total_Revenue"),
        round(spark_sum("Cost_Amount"), 2).alias("Total_Cost"),
        round(spark_sum("Gross_Profit"), 2).alias("Total_Profit"),
        round((spark_sum("Gross_Profit") / spark_sum("Net_Line_Amount")) * 100, 2).alias(
            "Average_Profit_Margin"
        ),
        round(avg("Shipping_Days"), 2).alias("Average_Shipping_Days"),
        round(avg("Shipping_Cost"), 2).alias("Average_Shipping_Cost"),
    )

    return {
        "mart_overview": overview,
        "mart_order_summary": build_mart_order_summary(clean_df),
        "mart_rfm_customer_segmentation": build_rfm_mart(clean_df),
    }
