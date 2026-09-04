"""Entry Point CLI điều phối thống nhất toàn bộ hệ thống GlobalCart Intelligence."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from environment_check import format_environment_report, inspect_environment

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "SourceCode"
DEFAULT_DATASET = PROJECT_ROOT / "Data" / "EcommerceSalesDataset.csv"

logging.basicConfig(
    level=os.getenv("ECOMMERCE_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("GlobalCartCLI")


def run_python(
    script_name: str, arguments: list[str] | None = None, env_vars: dict[str, str] | None = None
) -> int:
    """Khởi chạy một script Python con với môi trường đã thiết lập.

    Args:
        script_name: Tên file script trong thư mục SourceCode.
        arguments: Danh sách đối số truyền cho script.
        env_vars: Từ điển các biến môi trường bổ sung.

    Returns:
        Mã thoát (Exit Code) của tiến trình con.
    """
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)
    command = [sys.executable, str(SOURCE_DIR / script_name), *(arguments or [])]
    return subprocess.run(command, cwd=PROJECT_ROOT, env=env, check=False).returncode


def run_quality_checks() -> int:
    """Chạy chuỗi kiểm tra nhanh tính hợp lệ dữ liệu, sinh báo cáo và Unit Tests.

    Returns:
        Mã thoát 0 nếu toàn bộ quy trình kiểm tra thành công.
    """
    LOGGER.info("Kiểm tra hợp lệ file CSV thô đầu vào...")
    validate_code = run_python("validate_input.py", [str(DEFAULT_DATASET)])
    if validate_code != 0:
        LOGGER.error("Kiểm tra validate_input thất bại với exit code %d", validate_code)
        return validate_code

    LOGGER.info("Sinh lại báo cáo Business Insights tự động...")
    report_code = run_python("generate_portfolio_report.py")
    if report_code != 0:
        LOGGER.error("Tạo báo cáo report thất bại với exit code %d", report_code)
        return report_code

    LOGGER.info("Khởi chạy bộ kiểm thử tự động Pytest...")
    temp_dir = PROJECT_ROOT / "scratch" / "pytest_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    return subprocess.run(
        [sys.executable, "-m", "pytest", "-v", f"--basetemp={temp_dir}"],
        cwd=PROJECT_ROOT,
        check=False,
    ).returncode


def build_parser() -> argparse.ArgumentParser:
    """Khởi tạo Bộ điều hướng CLI với các subcommands hỗ trợ Tiếng Việt chi tiết.

    Returns:
        ArgumentParser đã được đăng ký toàn bộ subcommands.
    """
    parser = argparse.ArgumentParser(
        prog="globalcart",
        description="🌐 GlobalCart Intelligence CLI — Hệ thống quản lý Lakehouse & BI Platform",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser(
        "doctor", help="🏥 Chẩn đoán môi trường Python, Java, Hadoop HDFS và thư viện"
    )
    commands.add_parser(
        "check", help="🧪 Kiểm tra chất lượng dữ liệu đầu vào, sinh báo cáo & chạy Pytest"
    )
    commands.add_parser(
        "report", help="📊 Sinh lại báo cáo Business Insights tự động (docs/BUSINESS_INSIGHTS.md)"
    )

    pipeline_parser = commands.add_parser(
        "pipeline", help="⚙️ Chạy Pipeline PySpark Medallion Lakehouse (Bronze-Silver-Gold)"
    )
    pipeline_parser.add_argument(
        "--local",
        action="store_true",
        help="Chạy chế độ Spark Local Storage (không cần HDFS cluster)",
    )

    commands.add_parser(
        "mongodb", help="🍃 Đồng bộ các bảng Silver & Gold Delta Lake sang MongoDB Collections"
    )
    commands.add_parser(
        "thrift", help="🔌 Khởi động Spark Thrift Server kết nối Power BI qua ODBC/JDBC"
    )

    return parser


def main(arguments: list[str] | None = None) -> int:
    """Điều phối thực thi các lệnh từ dòng lệnh.

    Args:
        arguments: Danh sách tham số đầu vào.

    Returns:
        Mã thoát của chương trình.
    """
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = build_parser().parse_args(arguments)

    if args.command == "doctor":
        print(format_environment_report(inspect_environment()))
        return 0

    if args.command == "check":
        return run_quality_checks()

    if args.command == "pipeline":
        env_vars = {}
        if getattr(args, "local", False):
            env_vars["ECOMMERCE_USE_LOCAL_STORAGE"] = "true"
            LOGGER.info("Khởi chạy Spark Lakehouse Pipeline ở chế độ Local Storage Mode...")
        return run_python("SparkEcommerceAnalysis.py", env_vars=env_vars)

    script_by_command = {
        "report": "generate_portfolio_report.py",
        "mongodb": "InsertMongoDB.py",
        "thrift": "start_thrift_server.py",
    }
    return run_python(script_by_command[args.command])


if __name__ == "__main__":
    raise SystemExit(main())
