"""Khởi tạo Spark dùng chung cho các job của GlobalCart."""

from __future__ import annotations

import os
import sys
import tempfile

# Giữ cùng interpreter cho driver và worker khi chạy local hoặc trong CI.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

from pyspark.sql import SparkSession

from config import SETTINGS, auto_set_spark_home_env

try:
    from delta import configure_spark_with_delta_pip
except ImportError as error:
    raise ImportError(
        "\nChưa cài đặt Delta Lake.\nHãy chạy lệnh:\npython -m pip install delta-spark\n"
    ) from error

auto_set_spark_home_env()


def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession local tích hợp Delta Lake."""
    driver_host = SETTINGS.spark_driver_host or "127.0.0.1"
    driver_bind = SETTINGS.spark_driver_bind_address or "127.0.0.1"

    builder = (
        SparkSession.builder.appName("GlobalCart Order Lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", SETTINGS.get_storage_path("/spark-warehouse"))
        .config(
            "spark.jars.ivy",
            os.getenv(
                "SPARK_IVY_DIR",
                str(os.path.join(tempfile.gettempdir(), "globalcart-ivy2")),
            ),
        )
    )
    # Điểm vào dùng python nên cần master mặc định khi không chạy qua spark-submit.
    builder = builder.master(os.getenv("SPARK_MASTER") or "local[2]")

    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    if driver_bind:
        builder = builder.config("spark.driver.bindAddress", driver_bind)

    # Dùng catalog dùng chung để stable view serving tồn tại giữa các process.
    return configure_spark_with_delta_pip(builder.enableHiveSupport()).getOrCreate()
