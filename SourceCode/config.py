"""Cấu hình tập trung cho pipeline GlobalCart.

V1 chỉ hỗ trợ một đường chạy PySpark local + Delta Lake. Các biến môi trường
chỉ dùng để đổi thư mục dữ liệu, file nguồn và một vài tuỳ chọn thật sự cần thiết.
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

    # V1 chỉ dùng local filesystem để có một đường chạy tái lập được.
    local_storage_base: str = os.getenv(
        "ECOMMERCE_LOCAL_STORAGE_BASE",
        str(Path(__file__).resolve().parents[1] / "Output" / "lakehouse"),
    )

    # Đường dẫn file dữ liệu đầu vào
    input_csv: str = os.getenv(
        "ECOMMERCE_INPUT_CSV",
        str(Path(__file__).resolve().parents[1] / "Data" / "EcommerceSalesDataset.csv"),
    )

    # Feature flags chỉ giữ cấu hình có tác động trực tiếp đến pipeline.
    use_scd2: bool = _env_bool("ECOMMERCE_USE_SCD2", False)

    # Spark Driver Network Configs dùng cho local và CI.
    spark_driver_host: str | None = os.getenv("SPARK_DRIVER_HOST") or os.getenv("SPARK_LOCAL_IP")
    spark_driver_bind_address: str | None = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

    # Đường dẫn các tầng Medallion Lakehouse trên local filesystem.
    bronze_delta: str = "/ecommerce/bronze/ecommerce_raw_delta"
    ingestion_batches_delta: str = "/ecommerce/bronze/ingestion_batches_delta"
    ingestion_files_delta: str = "/ecommerce/control/ctl_ingestion_files_delta"
    silver_delta: str = "/ecommerce/silver/ecommerce_clean_delta"
    silver_orders_delta: str = "/ecommerce/silver/silver_orders_current_delta"
    silver_order_lines_delta: str = "/ecommerce/silver/silver_order_lines_current_delta"
    quarantine_delta: str = "/ecommerce/quarantine/rejected_rows"
    gold_star_schema_base: str = "/ecommerce/gold/star_schema"
    gold_marts_base: str = "/ecommerce/gold/marts"

    def get_storage_path(self, relative_path: str) -> str:
        """Trả về file URI cho một bảng Delta trong local lakehouse.

        Args:
            relative_path: Đường dẫn tương đối của tầng dữ liệu (ví dụ: '/ecommerce/bronze')

        Returns:
            File URI tuyệt đối để Spark đọc/ghi ổn định giữa CLI và test.
        """
        clean_rel = relative_path.lstrip("/")
        local_path = (Path(self.local_storage_base) / clean_rel).resolve()
        return local_path.as_uri()

    def get_input_path(self) -> str:
        """Trả về URI file của nguồn CSV mô phỏng OMS."""
        return Path(self.input_csv).resolve().as_uri()


@dataclass
class PipelineConfig:
    """Cấu hình thực thi một chu trình Lakehouse Pipeline (Bootstrap hoặc Incremental).

    Attributes:
        execution_mode: 'bootstrap' (full refresh) hoặc 'incremental' (micro-batch MERGE).
        use_scd2: Kích hoạt SCD Type 2 cho bảng dim_customer.
        batch_id: Mã nhận diện batch xử lý dữ liệu.
        run_id: Mã nhận diện phiên chạy pipeline.
        input_path: Đường dẫn file CSV nguồn.
        quarantine_path: Đường dẫn lưu trữ bảng Quarantine Delta.
    """

    execution_mode: str = "bootstrap"
    use_scd2: bool = False
    batch_id: str | None = None
    run_id: str | None = None
    input_path: str | None = None
    quarantine_path: str | None = None


SETTINGS = Settings()
