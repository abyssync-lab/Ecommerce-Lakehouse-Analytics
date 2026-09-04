"""Script đồng bộ dữ liệu từ tầng Silver và Gold Delta Lake sang MongoDB.

Đọc dữ liệu trực tiếp từ Delta Lake để đảm bảo Power BI, Spark và MongoDB cùng dùng chung
một nguồn dữ liệu nhất quán (Single Source of Truth), không gây sai lệch số liệu.
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


def replace_collection(
    database: Any, name: str, dataframe: Any, index_fields: Iterable[str]
) -> None:
    """Xóa collection cũ, ghi dữ liệu mới theo từng Batch và tự động tạo Index.

    Args:
        database: Đối tượng Database kết nối từ PyMongo.
        name: Tên Collection trong MongoDB.
        dataframe: Spark DataFrame chứa dữ liệu cần nạp.
        index_fields: Danh sách các trường cần đánh chỉ mục (Index).
    """
    database.drop_collection(name)
    collection = database[name]
    batch: list[dict[str, Any]] = []
    inserted = 0

    for document in iter_documents(dataframe):
        batch.append(document)
        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []
    if batch:
        collection.insert_many(batch, ordered=False)
        inserted += len(batch)

    for field in index_fields:
        if field in dataframe.columns:
            collection.create_index(field)

    LOGGER.info("Collection '%s': Đã nạp thành công %,d documents", name, inserted)


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
        client.admin.command("ping")
        database = client[SETTINGS.mongo_database]

        sources = {
            "Orders": (
                SETTINGS.silver_delta,
                [
                    "Order_ID",
                    "Order_Date",
                    "Customer_ID",
                    "Region",
                    "Country",
                    "Category",
                    "Order_Status",
                ],
            ),
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
