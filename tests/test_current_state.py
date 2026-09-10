"""Kiểm tra snapshot current-state trước khi dựng Gold."""

from __future__ import annotations

from lakehouse.silver import build_silver_current_events, clean_and_enrich_silver
from pyspark.sql.functions import col


def test_current_event_snapshot_keeps_latest_non_deleted_line(spark_session):
    """Gold không được cộng version cũ hoặc line đã soft-delete."""
    data = [
        ("ORD01", "L1", "2026-08-01 08:00:00", False, 100.0),
        ("ORD01", "L1", "2026-08-01 09:00:00", False, 120.0),
        ("ORD01", "L2", "2026-08-01 08:00:00", False, 50.0),
        ("ORD01", "L2", "2026-08-01 10:00:00", True, 50.0),
    ]
    events = spark_session.createDataFrame(
        data,
        ["Order_ID", "Order_Line_ID", "Source_Updated_At", "Is_Deleted", "Net_Line_Amount"],
    ).withColumn("Source_Updated_At", col("Source_Updated_At").cast("timestamp"))

    current = build_silver_current_events(events)
    rows = current.select("Order_Line_ID", "Net_Line_Amount").collect()

    assert [(row["Order_Line_ID"], row["Net_Line_Amount"]) for row in rows] == [("L1", 120.0)]


def test_latest_delete_is_selected_before_tombstone_filter(spark_session):
    """Bản DELETE mới nhất phải loại line, không để UPSERT cũ hồi sinh dữ liệu."""
    events = spark_session.createDataFrame(
        [
            ("A001", "L1", "2026-08-01 10:00:00", "UPSERT", False, 100.0),
            ("A001", "L1", "2026-08-01 11:00:00", "UPSERT", False, 200.0),
            ("A001", "L1", "2026-08-01 12:00:00", "DELETE", True, 200.0),
        ],
        [
            "Order_ID",
            "Order_Line_ID",
            "Source_Updated_At",
            "Operation",
            "Is_Deleted",
            "Net_Line_Amount",
        ],
    ).withColumn("Source_Updated_At", col("Source_Updated_At").cast("timestamp"))

    current = build_silver_current_events(events)

    assert current.count() == 0


def test_invalid_event_time_is_quarantined(spark_session, tmp_path):
    """Sự kiện có Order_Date sau Source_Updated_At không được vào Silver."""
    events = spark_session.createDataFrame(
        [
            (
                "ORD02",
                "2026-08-02",
                "C02",
                "Product",
                1,
                10.0,
                0.0,
                5.0,
                "2026-08-01 00:00:00",
                "UPSERT",
                "Delivered",
            ),
            (
                "ORD03",
                "2026-08-02",
                "C03",
                "Product",
                1,
                10.0,
                0.0,
                5.0,
                "2026-08-02 00:00:00",
                "UPSERT",
                "Delivered",
            ),
        ],
        [
            "Order_ID",
            "Order_Date",
            "Customer_ID",
            "Product_Name",
            "Quantity",
            "Unit_Price",
            "Discount",
            "Cost",
            "Source_Updated_At",
            "Operation",
            "Order_Status",
        ],
    )

    clean = clean_and_enrich_silver(
        events,
        quarantine_path=str(tmp_path / "quarantine"),
        run_id="run_invalid_event_time",
        batch_id="batch_invalid_event_time",
    )

    assert clean.count() == 1
    quarantine = spark_session.read.format("delta").load(str(tmp_path / "quarantine"))
    assert quarantine.select("rejection_reason").first()[0] == "INVALID_EVENT_TIME"
