"""Script đồng bộ dữ liệu từ tầng Gold Delta Lake sang MongoDB Collections.

SERVING CONTRACT (GOLD-ONLY):
MongoDB là kho lưu trữ thứ cấp (Secondary Operational Document Store) phục vụ ứng dụng web / microservices
với độ trễ thấp (low latency document lookup), KHÔNG PHẢI nguồn chân lý phân tích thay thế Power BI.
Đọc dữ liệu 100% từ Gold Data Marts để đảm bảo Single Source of Truth tuyệt đối xuyên suốt nền tảng.
"""


from __future__ import annotations

import logging
import os
import sys
from collections.abc import Generator, Iterable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from config import SETTINGS

logging.basicConfig(
    level=os.getenv("ECOMMERCE_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)
BATCH_SIZE = int(os.getenv("ECOMMERCE_MONGO_BATCH_SIZE", "1000"))


def create_spark_session() -> Any:
    """Khởi tạo SparkSession tích hợp Delta Lake để đọc dữ liệu lưu trữ.

    Returns:
        SparkSession đã sẵn sàng kết nối Delta Lake.
    """
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "Chưa cài đặt gói phụ thuộc; hãy chạy pip install -r requirements.txt"
        ) from error

    builder = (
        SparkSession.builder.appName("Ecommerce Delta to MongoDB Sync")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    if not SETTINGS.use_local_storage:
        builder = builder.config("spark.hadoop.fs.defaultFS", SETTINGS.hdfs_base)

    return configure_spark_with_delta_pip(builder).getOrCreate()


def to_mongo_value(value: Any) -> Any:
    """Chuyển đổi kiểu dữ liệu Spark/Python thành định dạng BSON tương thích với MongoDB.

    Args:
        value: Giá trị nguyên bản từ Spark Row.

    Returns:
        Giá trị đã được ép kiểu BSON hợp lệ (ví dụ: Decimal -> float, date -> datetime).
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime.min.time())
    return value


def iter_documents(dataframe: Any) -> Generator[dict[str, Any], None, None]:
    """Duyệt từng dòng dữ liệu bằng iterator để tiết kiệm RAM bộ nhớ máy.

    Args:
        dataframe: Spark DataFrame cần đọc.

    Yields:
        Dictionary tương ứng với từng MongoDB Document.
    """
    for row in dataframe.toLocalIterator():
        yield {key: to_mongo_value(val) for key, val in row.asDict().items()}


def publish_collection_atomic(
    database: Any, name: str, dataframe: Any, index_fields: Iterable[str]
) -> None:
    """Nạp dữ liệu vào collection staging, kiểm tra số lượng và tráo đổi nguyên tử (Atomic Switch).

    Tránh tuyệt đối downtime hoặc tình trạng ứng dụng đọc phải dữ liệu load dở dang (partial reads)
    nếu tiến trình đồng bộ gặp sự cố giữa chừng.

    Args:
        database: Đối tượng Database kết nối từ PyMongo.
        name: Tên Collection đích trong MongoDB.
        dataframe: Spark DataFrame chứa dữ liệu cần nạp.
        index_fields: Danh sách các trường cần đánh chỉ mục (Index).
    """
    staging_name = f"{name}_staging"
    database.drop_collection(staging_name)
    staging_coll = database[staging_name]
    batch: list[dict[str, Any]] = []
    inserted = 0

    for document in iter_documents(dataframe):
        batch.append(document)
        if len(batch) >= BATCH_SIZE:
            staging_coll.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []
    if batch:
        staging_coll.insert_many(batch, ordered=False)
        inserted += len(batch)

    for field in index_fields:
        if field in dataframe.columns:
            staging_coll.create_index(field)

    # Xác thực số lượng bản ghi nạp vào staging
    staging_count = staging_coll.count_documents({})
    expected_count = dataframe.count()
    if staging_count != expected_count:
        database.drop_collection(staging_name)
        raise ValueError(
            f"Lỗi toàn vẹn dữ liệu khi đồng bộ MongoDB '{name}': "
            f"Staging ({staging_count}) != Expected ({expected_count})"
        )

    # Tráo đổi nguyên tử sang collection chính thức (Atomic Collection Swap)
    try:
        staging_coll.rename(name, dropTarget=True)
        LOGGER.info("Collection '%s': Đã nạp và tráo đổi nguyên tử (Atomic Published) %,d documents", name, inserted)
    except Exception as err:
        LOGGER.error("Lỗi khi tráo đổi collection nguyên tử cho '%s': %s", name, err)
        raise


def replace_collection(
    database: Any, name: str, dataframe: Any, index_fields: Iterable[str]
) -> None:
    """Hàm ủy quyền tương thích ngược; thực hiện publish_collection_atomic."""
    publish_collection_atomic(database, name, dataframe, index_fields)


def main() -> None:
    """Hàm chính điều phối quá trình đọc Delta Lake và ghi vào MongoDB."""
    try:
        from pymongo import MongoClient
    except ImportError as error:
        raise RuntimeError(
            "Chưa cài đặt thư viện pymongo; hãy chạy pip install -r requirements.txt"
        ) from error

    mongo_kwargs: dict[str, Any] = {"serverSelectionTimeoutMS": 5000}
    if SETTINGS.mongo_username and SETTINGS.mongo_password:
        mongo_kwargs["username"] = SETTINGS.mongo_username
        mongo_kwargs["password"] = SETTINGS.mongo_password

    spark = create_spark_session()
    client = MongoClient(SETTINGS.mongo_uri, **mongo_kwargs)

    try:
        from lakehouse.registry import BatchRegistry

        registry = BatchRegistry(spark)
        if not registry.is_latest_run_certified():
            LOGGER.warning(
                "CẢNH BÁO KIỂM TOÁN: Lần chạy gần nhất chưa được chứng nhận (SUCCESS) trong Batch Registry! "
                "Chỉ các phiên chạy vượt qua Reconciliation Gate mới được khuyến nghị xuất bản sang Operational MongoDB."
            )
    except Exception as e:
        LOGGER.debug("Không thể kiểm tra certification trong registry: %s", e)

    try:
        client.admin.command("ping")
        database = client[SETTINGS.mongo_database]

        sources = {
            "Gold_Overview": (f"{SETTINGS.gold_marts_base}/mart_overview_delta", []),
            "Gold_RevenueByRegion": (
                f"{SETTINGS.gold_marts_base}/mart_revenue_by_region_delta",
                ["Region"],
            ),
            "Gold_RevenueByCountry": (
                f"{SETTINGS.gold_marts_base}/mart_revenue_by_country_delta",
                ["Region", "Country"],
            ),
            "Gold_RevenueByCategory": (
                f"{SETTINGS.gold_marts_base}/mart_revenue_by_category_delta",
                ["Category", "Sub_Category"],
            ),
            "Gold_TopProducts": (
                f"{SETTINGS.gold_marts_base}/mart_top_products_by_revenue_delta",
                ["Product_Name", "Category"],
            ),
            "Gold_MonthlyRevenue": (
                f"{SETTINGS.gold_marts_base}/mart_monthly_revenue_delta",
                ["Year", "Month"],
            ),
            "Gold_CustomerSegments": (
                f"{SETTINGS.gold_marts_base}/mart_customer_segment_analysis_delta",
                ["Customer_Segment"],
            ),
            "Gold_RFM_Segmentation": (
                f"{SETTINGS.gold_marts_base}/mart_rfm_customer_segmentation_delta",
                ["Customer_ID", "RFM_Segment"],
            ),
            "Gold_ABC_ProductAnalysis": (
                f"{SETTINGS.gold_marts_base}/mart_abc_product_analysis_delta",
                ["Product_Name", "ABC_Class"],
            ),
            "Gold_OrderSummary": (
                f"{SETTINGS.gold_marts_base}/mart_order_summary_delta",
                ["Order_ID", "Customer_ID"],
            ),
        }


        for name, (path, indexes) in sources.items():
            full_path = SETTINGS.get_storage_path(path)
            frame = spark.read.format("delta").load(full_path)
            replace_collection(database, name, frame, indexes)

        LOGGER.info("Hoàn thành đồng bộ toàn bộ Data Lakehouse sang MongoDB!")

    except Exception as err:
        LOGGER.error("Lỗi trong quá trình đồng bộ MongoDB: %s", err)
        raise
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    main()
