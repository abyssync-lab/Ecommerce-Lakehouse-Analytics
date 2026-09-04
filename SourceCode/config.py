"""Cấu hình tập trung cho hệ thống GlobalCart Intelligence Lakehouse & Analytics.

Mọi tham số đều hỗ trợ đọc từ biến môi trường (Environment Variables),
giúp hệ thống dễ dàng triển khai trên nhiều môi trường (Local, HDFS Cluster, Docker, CI/CD).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def detect_spark_home() -> Path:
    """Tự động tìm kiếm và xác định đường dẫn SPARK_HOME.

    Returns:
        Đường dẫn Path tới thư mục gốc Spark.

    Raises:
        RuntimeError: Nếu chưa cài đặt thư viện PySpark.
    """
    configured_home = os.getenv("SPARK_HOME")
    if configured_home:
        return Path(configured_home)
    try:
        import pyspark

        return Path(pyspark.__file__).resolve().parent
    except ImportError as error:
        raise RuntimeError("Chưa cài PySpark; hãy chạy pip install -r requirements.txt") from error


def auto_set_spark_home_env() -> str | None:
    """Tự động thiết lập biến môi trường SPARK_HOME nếu chưa được đặt."""
    if os.environ.get("SPARK_HOME"):
        return os.environ.get("SPARK_HOME")
    try:
        home = detect_spark_home()
        os.environ["SPARK_HOME"] = str(home)
        return str(home)
    except RuntimeError:
        return None


def _env_bool(name: str, default: bool = False) -> bool:
    """Đọc biến môi trường và chuyển đổi sang kiểu boolean một cách an toàn."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """Quản lý tập trung toàn bộ cấu hình vận hành của Pipeline và Data Lakehouse."""

    # Chế độ lưu trữ: True = Local File System, False = Hadoop HDFS
    use_local_storage: bool = _env_bool("ECOMMERCE_USE_LOCAL_STORAGE", True)

    # Cấu hình HDFS & Local Root Path
    hdfs_base: str = os.getenv("ECOMMERCE_HDFS_BASE", "hdfs://localhost:9000")
    local_storage_base: str = os.getenv(
        "ECOMMERCE_LOCAL_STORAGE_BASE",
        str(Path(__file__).resolve().parents[1] / "Output" / "lakehouse"),
    )

    # Đường dẫn file dữ liệu đầu vào
    input_csv: str = os.getenv("ECOMMERCE_INPUT_CSV", "/ecommerce/raw/EcommerceSalesDataset.csv")
    local_input_csv: str = os.getenv(
        "ECOMMERCE_LOCAL_INPUT_CSV",
        str(Path(__file__).resolve().parents[1] / "Data" / "EcommerceSalesDataset.csv"),
    )

    # Database & Service Endpoint Configs
    hive_database: str = os.getenv("ECOMMERCE_HIVE_DATABASE", "gold")
    mongo_uri: str = os.getenv("ECOMMERCE_MONGO_URI", "mongodb://localhost:27017/")
    mongo_database: str = os.getenv("ECOMMERCE_MONGO_DATABASE", "GlobalEcommerceDB")
    mongo_username: str | None = os.getenv("ECOMMERCE_MONGO_USERNAME")
    mongo_password: str | None = os.getenv("ECOMMERCE_MONGO_PASSWORD")
    thrift_port: int = int(os.getenv("ECOMMERCE_THRIFT_PORT", "10001"))

    # Feature Flags
    run_delta_demo: bool = _env_bool("ECOMMERCE_RUN_DELTA_DEMO", False)
    wait_before_exit: bool = _env_bool("ECOMMERCE_WAIT_BEFORE_EXIT", False)

    # Đường dẫn các tầng Medallion Lakehouse (Bronze, Silver, Gold)
    bronze_delta: str = "/ecommerce/bronze/ecommerce_raw_delta"
    silver_delta: str = "/ecommerce/silver/ecommerce_clean_delta"
    gold_star_schema_base: str = "/ecommerce/gold/star_schema"
    gold_marts_base: str = "/ecommerce/gold/marts"
    lakehouse_version_delta: str = "/ecommerce/lakehouse/versioning_delta"

    def get_storage_path(self, relative_path: str) -> str:
        """Trả về đường dẫn lưu trữ phù hợp dựa trên chế độ (Local hoặc HDFS).

        Args:
            relative_path: Đường dẫn tương đối của tầng dữ liệu (ví dụ: '/ecommerce/bronze')

        Returns:
            URL đầy đủ dạng hdfs://... hoặc đường dẫn thư mục cục bộ dạng file://...
        """
        clean_rel = relative_path.lstrip("/")
        if self.use_local_storage:
            local_path = (Path(self.local_storage_base) / clean_rel).resolve()
            return local_path.as_uri()
        return f"{self.hdfs_base.rstrip('/')}/{clean_rel}"

    def get_input_path(self) -> str:
        """Trả về nguồn CSV đúng với chế độ lưu trữ đang được chọn."""
        if self.use_local_storage:
            return Path(self.local_input_csv).resolve().as_uri()
        return self.get_storage_path(self.input_csv)


SETTINGS = Settings()
