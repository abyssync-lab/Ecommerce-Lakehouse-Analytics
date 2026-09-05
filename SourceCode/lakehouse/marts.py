"""Module xây dựng FactSales và 12 Data Marts tầng Gold trong Data Lakehouse."""

from __future__ import annotations

import logging
from typing import Any

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
from pyspark.sql.functions import avg, col, countDistinct, date_format, datediff, lit, round, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

from .dimensions import add_surrogate_key

LOGGER = logging.getLogger(__name__)



def build_fact_sales(
    clean_df: Any,
    dimensions: dict[str, Any],
) -> Any:
    """Xây dựng FactSales theo đúng Grain: 1 dòng = 1 sản phẩm trong 1 đơn hàng."""
    LOGGER.info("Bắt đầu xây dựng FactSales table...")

    fact = clean_df.withColumn("DateKey", date_format(col("Order_Date"), "yyyyMMdd").cast("int"))

    fact = fact.join(dimensions["dim_product"], on=["Product_Name", "Category", "Sub_Category"], how="left")
    
    dim_cust = dimensions["dim_customer"]
    if "ValidFrom" in dim_cust.columns and "ValidTo" in dim_cust.columns:
        # SCD Type 2 Temporal Join
        fact = fact.join(
            dim_cust,
            (fact["Customer_ID"] == dim_cust["Customer_ID"])
            & (fact["Order_Date"] >= dim_cust["ValidFrom"])
            & (fact["Order_Date"] < dim_cust["ValidTo"]),
            how="left",
        ).drop(dim_cust["Customer_ID"])
    else:
        # Standard SCD Type 1 Join (guaranteed 1 Customer_ID = 1 row in dim_customer)
        fact = fact.join(dim_cust, on=["Customer_ID"], how="left")
    fact = fact.join(dimensions["dim_location"], on=["Region", "Country"], how="left")
    fact = fact.join(dimensions["dim_payment"], on=["Payment_Method"], how="left")
    fact = fact.join(dimensions["dim_shipping"], on=["Shipping_Method", "Delivery_Level"], how="left")
    fact = fact.join(dimensions["dim_order_status"], on=["Order_Status", "Is_Returned", "Is_Cancelled"], how="left")

    order_cols = ["Order_ID"]
    if "Order_Line_ID" in fact.columns:
        order_cols.append("Order_Line_ID")
    order_cols.extend(["ProductKey", "CustomerKey", "DateKey"])

    fact = add_surrogate_key(
        fact,
        "SalesKey",
        order_cols,
    )

    fact_cols = ["SalesKey", "Order_ID"]
    if "Order_Line_ID" in fact.columns:
        fact_cols.append("Order_Line_ID")
    fact_cols.extend([
        "DateKey",
        "CustomerKey",
        "LocationKey",
        "ProductKey",
        "ShippingKey",
        "PaymentKey",
        "StatusKey",
        "Unit_Price",
        "Quantity",
        "Discount",
        "Revenue",
        "Cost",
        "Profit",
        "Profit_Margin_Percent",
        "Shipping_Cost",
    ])

    fact_sales = fact.select(*fact_cols)

    LOGGER.info("FactSales đã được khởi tạo thành công (%d dòng).", fact_sales.count())
    return fact_sales


def aggregate_sales(
    dataframe: Any,
    dimensions: list[str],
    *,
    include_quantity: bool = False,
    include_average_order_value: bool = False,
) -> Any:
    """Tạo các measure bán hàng dùng chung cho các Gold Data Mart."""
    metrics = [countDistinct("Order_ID").alias("Total_Orders")]
    if include_quantity:
        metrics.append(spark_sum("Quantity").alias("Total_Quantity"))
    metrics.extend(
        [
            round(spark_sum("Revenue"), 2).alias("Total_Revenue"),
            round(spark_sum("Profit"), 2).alias("Total_Profit"),
        ]
    )
    if include_average_order_value:
        metrics.append(
            round(spark_sum("Revenue") / countDistinct("Order_ID"), 2).alias("Average_Order_Value")
        )
    return dataframe.groupBy(*dimensions).agg(*metrics)


def build_rfm_mart(clean_df: Any, analysis_date: str | None = None) -> Any:
    """Xây dựng Data Mart phân hạng RFM bằng PySpark với quy tắc chuẩn hóa `analytics_rules.py`."""
    if analysis_date is not None:
        max_date_val = analysis_date
    else:
        max_date_val = clean_df.select(spark_max("Order_Date")).collect()[0][0]

    rfm_base = (
        clean_df.groupBy("Customer_ID")
        .agg(
            spark_max("Order_Date").alias("Last_Purchase"),
            countDistinct("Order_ID").alias("Frequency"),
            round(spark_sum("Revenue"), 2).alias("Monetary"),
        )
        .withColumn("Recency", datediff(lit(max_date_val), col("Last_Purchase")))
    )

    rfm_mart = rfm_base.withColumn(
        "RFM_Segment",
        when((col("Recency") <= 30) & (col("Frequency") >= 3), RFM_CHAMPIONS)
        .when(col("Frequency") >= 3, RFM_LOYAL)
        .when(col("Recency") > 90, RFM_AT_RISK)
        .otherwise(RFM_CASUAL),
    ).withColumn("Rule_Version", lit(RFM_RULE_VERSION)).orderBy(col("Monetary").desc())

    return rfm_mart


def build_abc_mart(clean_df: Any) -> Any:
    """Xây dựng Data Mart Pareto ABC bằng PySpark với quy tắc tích lũy trước sản phẩm chuẩn hóa."""
    total_rev = clean_df.select(spark_sum("Revenue")).collect()[0][0]
    if not total_rev or total_rev <= 0:
        raise ValueError("Không thể phân tích ABC khi tổng doanh thu <= 0")

    prod_base = clean_df.groupBy("Product_Name", "Category").agg(
        spark_sum("Quantity").alias("Total_Quantity"),
        round(spark_sum("Revenue"), 2).alias("Total_Revenue"),
        round(spark_sum("Profit"), 2).alias("Total_Profit"),
    )

    window_spec = Window.orderBy(col("Total_Revenue").desc())

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
    return (
        clean_df.groupBy("Order_ID", "Order_Date", "Customer_ID", "Order_Status")
        .agg(
            spark_sum("Quantity").alias("Total_Items"),
            round(spark_sum("Revenue"), 2).alias("Order_Total_Revenue"),
            round(spark_sum("Cost"), 2).alias("Order_Total_Cost"),
            round(spark_sum("Profit"), 2).alias("Order_Total_Profit"),
            round(spark_sum("Shipping_Cost"), 2).alias("Order_Shipping_Cost"),
        )
        .orderBy(col("Order_Total_Revenue").desc())
    )


def build_all_marts(clean_df: Any) -> dict[str, Any]:
    """Tạo toàn bộ Gold Data Marts tổng hợp cho tầng Gold."""
    LOGGER.info("Bắt đầu xây dựng Gold Data Marts...")

    overview = clean_df.agg(
        countDistinct("Order_ID").alias("Total_Orders"),
        spark_sum("Quantity").alias("Total_Quantity"),
        round(spark_sum("Revenue"), 2).alias("Total_Revenue"),
        round(spark_sum("Cost"), 2).alias("Total_Cost"),
        round(spark_sum("Profit"), 2).alias("Total_Profit"),
        round((spark_sum("Profit") / spark_sum("Revenue")) * 100, 2).alias("Average_Profit_Margin"),
        round(avg("Shipping_Days"), 2).alias("Average_Shipping_Days"),
        round(avg("Shipping_Cost"), 2).alias("Average_Shipping_Cost"),
    )

    marts = {
        "mart_overview": overview,
        "mart_order_summary": build_mart_order_summary(clean_df),
        "mart_revenue_by_region": aggregate_sales(clean_df, ["Region"]).orderBy(col("Total_Revenue").desc()),
        "mart_revenue_by_country": aggregate_sales(clean_df, ["Region", "Country"]).orderBy(col("Total_Revenue").desc()),
        "mart_revenue_by_category": aggregate_sales(clean_df, ["Category", "Sub_Category"], include_quantity=True).orderBy(col("Total_Revenue").desc()),
        "mart_top_products_by_revenue": (
            clean_df.groupBy("Product_Name", "Category", "Sub_Category")
            .agg(
                spark_sum("Quantity").alias("Total_Quantity"),
                round(spark_sum("Revenue"), 2).alias("Total_Revenue"),
                round(spark_sum("Profit"), 2).alias("Total_Profit"),
            )
            .orderBy(col("Total_Revenue").desc())
            .limit(10)
        ),
        "mart_payment_analysis": aggregate_sales(clean_df, ["Payment_Method"]).orderBy(col("Total_Revenue").desc()),
        "mart_shipping_analysis": (
            clean_df.groupBy("Shipping_Method", "Delivery_Level")
            .agg(
                countDistinct("Order_ID").alias("Total_Orders"),
                round(avg("Shipping_Days"), 2).alias("Avg_Shipping_Days"),
                round(avg("Shipping_Cost"), 2).alias("Avg_Shipping_Cost"),
                round(spark_sum("Revenue"), 2).alias("Total_Revenue"),
            )
            .orderBy(col("Total_Orders").desc())
        ),
        "mart_order_status_analysis": aggregate_sales(clean_df, ["Order_Status"]).orderBy(col("Total_Orders").desc()),
        "mart_monthly_revenue": aggregate_sales(clean_df, ["Year", "Month"]).orderBy("Year", "Month"),
        "mart_customer_segment_analysis": aggregate_sales(clean_df, ["Customer_Segment"], include_average_order_value=True).orderBy(col("Total_Revenue").desc()),
        "mart_rfm_customer_segmentation": build_rfm_mart(clean_df),
        "mart_abc_product_analysis": build_abc_mart(clean_df),
    }

    LOGGER.info("Đã tạo hoàn tất Gold Data Marts.")
    return marts

