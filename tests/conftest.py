"""Thiết lập đường dẫn import và Spark fixture chung cho bộ kiểm thử."""

import os
import sys
from pathlib import Path

import pytest

SOURCE_DIR = Path(__file__).parents[1] / "SourceCode"
sys.path.insert(0, str(SOURCE_DIR))

# Ép Spark chạy loopback trong CI để driver không cố bind vào hostname không phân giải được.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("HADOOP_USER_NAME", "hadoop")


@pytest.fixture(scope="session")
def spark_session():
    """Khởi tạo một SparkSession có nạp đầy đủ Delta Lake cho integration tests."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("GlobalCartTestSuite")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )

    # Hàm này vừa đăng ký Delta extensions vừa thêm đúng JAR tương thích với PySpark.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
