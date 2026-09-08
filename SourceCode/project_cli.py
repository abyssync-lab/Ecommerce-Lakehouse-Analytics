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

    # Certified report chỉ có thể sinh sau khi bootstrap đã tạo và publish Gold.
    # Không đọc report cũ hoặc CSV raw để che khuất lỗi của pipeline chính.
    LOGGER.info("Chạy bootstrap pipeline để tạo certified Gold...")
    pipeline_code = run_python(
        "SparkEcommerceAnalysis.py",
        ["--input", str(DEFAULT_DATASET)],
    )
    if pipeline_code != 0:
        LOGGER.error("Bootstrap pipeline thất bại với exit code %d", pipeline_code)
        return pipeline_code

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


def run_reconciliation() -> int:
    """Khởi chạy bộ kiểm toán đối soát bất biến doanh thu, số dòng và SCD2."""
    LOGGER.info("Khởi chạy kiểm toán Data Reconciliation Gate...")
    temp_dir = PROJECT_ROOT / "scratch" / "pytest_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_reconciliation.py", "-v", f"--basetemp={temp_dir}"],
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

    reconcile_parser = commands.add_parser(
        "reconcile", help="⚖️ Chạy kiểm toán đối soát bất biến doanh thu, grain và SCD2"
    )
    reconcile_parser.add_argument("--run-id", type=str, default=None, help="Mã nhận diện phiên kiểm toán")

    pipeline_parser = commands.add_parser(
        "pipeline", help="⚙️ Chạy Pipeline PySpark Medallion Lakehouse (Bootstrap / Incremental)"
    )
    pipeline_parser.add_argument(
        "mode",
        nargs="?",
        default="bootstrap",
        choices=["bootstrap", "incremental"],
        help="Chế độ thực thi: 'bootstrap' (khởi tạo Bronze rỗng) hoặc 'incremental' (Micro-batch MERGE)",
    )
    pipeline_parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Đường dẫn file CSV đầu vào",
    )
    pipeline_parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Mã nhận diện batch nạp",
    )
    pipeline_parser.add_argument(
        "--local",
        action="store_true",
        help="Chạy chế độ Spark Local Storage (không cần HDFS cluster)",
    )
    pipeline_parser.add_argument(
        "--scd2",
        action="store_true",
        help="Kích hoạt mô hình hóa SCD Type 2 cho bảng Dimension Customer",
    )
    pipeline_parser.add_argument(
        "--demo",
        action="store_true",
        help="Chạy kèm các bài demo Delta Lake (Time Travel, Schema Enforcement an toàn)",
    )
    pipeline_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Cờ tương đương chế độ 'incremental'",
    )

    serve_parser = commands.add_parser(
        "serve", help="🚀 Khởi động dịch vụ phục vụ dữ liệu (thrift hoặc mongodb)"
    )
    serve_parser.add_argument(
        "target",
        choices=["thrift", "mongodb"],
        help="Dịch vụ cần khởi chạy: thrift (Power BI) hoặc mongodb (Serving Collections)",
    )

    commands.add_parser(
        "delta-demo", help="🧪 Khởi chạy các kịch bản thử nghiệm Delta Lake (Time Travel, Schema Enforcement cô lập)"
    )
    commands.add_parser(
        "benchmark", help="📈 Khởi chạy Synthetic Scalability Benchmark (10K, 100K, 1M rows)"
    )
    commands.add_parser(
        "mongodb", help="🍃 Đồng bộ các bảng Gold Analytics / Serving sang MongoDB Collections"
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

    if args.command == "reconcile":
        return run_reconciliation()

    if args.command == "benchmark":
        LOGGER.info("Khởi chạy Synthetic Scalability Benchmark...")
        benchmark_script = PROJECT_ROOT / "scripts" / "benchmark_scalability.py"
        return subprocess.run([sys.executable, str(benchmark_script)], cwd=PROJECT_ROOT, check=False).returncode

    if args.command == "delta-demo":
        LOGGER.info("Khởi chạy kịch bản thử nghiệm Delta Lake...")
        return run_python("SparkEcommerceAnalysis.py", arguments=["--demo"])

    if args.command == "serve":
        script_by_serve = {
            "thrift": "start_thrift_server.py",
            "mongodb": "InsertMongoDB.py",
        }
        return run_python(script_by_serve[args.target])

    if args.command == "pipeline":
        env_vars = {}
        script_args = []
        is_incremental = getattr(args, "incremental", False) or getattr(args, "mode", "bootstrap") == "incremental"

        if getattr(args, "local", False):
            env_vars["ECOMMERCE_USE_LOCAL_STORAGE"] = "true"
        if getattr(args, "scd2", False):
            env_vars["ECOMMERCE_USE_SCD2"] = "true"
            script_args.append("--scd2")
        if getattr(args, "demo", False):
            script_args.append("--demo")
        if getattr(args, "input", None):
            script_args.extend(["--input", str(args.input)])
        if getattr(args, "batch_id", None):
            script_args.extend(["--batch-id", str(args.batch_id)])
        if is_incremental:
            script_args.append("--incremental")

        LOGGER.info(
            "Khởi chạy Spark Lakehouse Pipeline (Mode=%s, Local=%s, SCD2=%s)...",
            "INCREMENTAL" if is_incremental else "BOOTSTRAP",
            env_vars.get("ECOMMERCE_USE_LOCAL_STORAGE", "false"),
            env_vars.get("ECOMMERCE_USE_SCD2", "false"),
        )
        return run_python("SparkEcommerceAnalysis.py", arguments=script_args, env_vars=env_vars)

    script_by_command = {
        "report": "generate_portfolio_report.py",
        "mongodb": "InsertMongoDB.py",
        "thrift": "start_thrift_server.py",
    }
    return run_python(script_by_command[args.command])


if __name__ == "__main__":
    raise SystemExit(main())
