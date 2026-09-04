"""Script khởi động Spark Thrift Server (HiveThriftServer2).

Cho phép các công cụ BI như Power BI, Tableau hoặc SQL Clients truy vấn trực tiếp
các bảng Delta Lake trong Hive Metastore qua kết nối ODBC/JDBC (HTTP Transport Mode).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from config import SETTINGS, detect_spark_home


def build_command(spark_home: Path, port: int) -> list[str]:
    """Tạo câu lệnh khởi chạy spark-submit với cấu hình Delta Lake & Hive HTTP.

    Args:
        spark_home: Đường dẫn gốc Spark.
        port: Cổng mạng Listening (mặc định 10001).

    Returns:
        Danh sách tham số lệnh thực thi.
    """
    executable = "spark-submit.cmd" if os.name == "nt" else "spark-submit"
    spark_submit = spark_home / "bin" / executable
    if not spark_submit.exists():
        raise FileNotFoundError(f"Không tìm thấy file spark-submit tại: {spark_submit}")
    return [
        str(spark_submit),
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--class",
        "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
        "--master",
        "local[*]",
        "--hiveconf",
        "hive.server2.transport.mode=http",
        "--hiveconf",
        f"hive.server2.thrift.http.port={port}",
        "--hiveconf",
        "hive.server2.http.endpoint=cliservice",
    ]


def main() -> None:
    """Khởi động Spark Thrift Server ở foreground và lắng nghe kết nối từ Power BI."""
    command = build_command(detect_spark_home(), SETTINGS.thrift_port)
    print(
        f"🚀 Spark Thrift Server đang lắng nghe tại: http://localhost:{SETTINGS.thrift_port}/cliservice"
    )
    print(
        "📌 Giữ cửa sổ terminal này MỞ trong suốt quá trình Power BI kết nối. Nhấn Ctrl+C để dừng server."
    )
    try:
        subprocess.run(command, check=True)
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng tiến trình Spark Thrift Server an toàn.")


if __name__ == "__main__":
    main()
