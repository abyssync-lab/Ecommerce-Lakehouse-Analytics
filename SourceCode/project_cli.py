"""CLI đơn giản cho pipeline order lakehouse của GlobalCart."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from environment_check import format_environment_report, inspect_environment

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = PROJECT_ROOT / "Data" / "EcommerceSalesDataset.csv"

logging.basicConfig(
    level=os.getenv("ECOMMERCE_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("GlobalCartCLI")


def run_quality_checks() -> int:
    """Chạy chuỗi kiểm tra nhanh dữ liệu, sinh báo cáo và kiểm thử đơn vị.

    Returns:
        Mã thoát 0 nếu toàn bộ quy trình kiểm tra thành công.
    """
    from build_business_report import build_report, load_published_dataframe
    from lakehouse.pipeline import run_pipeline
    from validate_input import validate_input_file

    try:
        LOGGER.info("Kiểm tra hợp lệ file CSV thô đầu vào...")
        validate_input_file(DEFAULT_DATASET)

        # Báo cáo chỉ có thể sinh sau khi bootstrap đã tạo và publish Gold.
        # Không đọc report cũ hoặc CSV raw để che khuất lỗi của pipeline chính.
        LOGGER.info("Chạy bootstrap pipeline để tạo published Gold...")
        result = run_pipeline(input_path=str(DEFAULT_DATASET))
        if result.spark is not None:
            result.spark.stop()

        LOGGER.info("Sinh lại báo cáo Business Insights từ Gold...")
        dataframe, source_label = load_published_dataframe()
        output_path = PROJECT_ROOT / "docs" / "BUSINESS_INSIGHTS.md"
        output_path.write_text(build_report(dataframe, source_label), encoding="utf-8")
    except Exception:
        LOGGER.exception("Lệnh check thất bại")
        return 1

    LOGGER.info("Khởi chạy bộ kiểm thử tự động Pytest...")
    import pytest

    return pytest.main([str(PROJECT_ROOT / "tests"), "-v"])


def run_reconciliation() -> int:
    """Khởi chạy bộ kiểm toán đối soát bất biến doanh thu, số dòng và SCD2."""
    LOGGER.info("Khởi chạy kiểm tra Gold reconciliation...")
    import pytest

    return pytest.main([str(PROJECT_ROOT / "tests" / "test_reconciliation.py"), "-v"])


def run_pipeline_command(args: argparse.Namespace) -> int:
    """Chạy bootstrap hoặc incremental bằng API package, không tạo process con."""
    from lakehouse.pipeline import run_incremental_from_path, run_pipeline

    is_incremental = args.incremental or args.mode == "incremental"
    LOGGER.info(
        "Khởi chạy Spark Lakehouse Pipeline (Mode=%s, SCD2=%s)...",
        "INCREMENTAL" if is_incremental else "BOOTSTRAP",
        str(bool(args.scd2)).lower(),
    )

    try:
        if is_incremental:
            result = run_incremental_from_path(
                input_path=args.input,
                batch_id=args.batch_id,
                use_scd2=args.scd2,
            )
        else:
            result = run_pipeline(input_path=args.input, use_scd2=args.scd2)
            if result.spark is not None:
                result.spark.stop()
        return 0 if result.status in {"SUCCESS", "SKIPPED"} else 1
    except Exception:
        LOGGER.exception("Lệnh pipeline thất bại")
        return 1


def run_report_command() -> int:
    """Sinh báo cáo từ Gold snapshot đang được công bố."""
    from build_business_report import build_report, load_published_dataframe

    dataframe, source_label = load_published_dataframe()
    output_path = PROJECT_ROOT / "docs" / "BUSINESS_INSIGHTS.md"
    output_path.write_text(build_report(dataframe, source_label), encoding="utf-8")
    LOGGER.info("Đã tạo báo cáo: %s", output_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Khởi tạo Bộ điều hướng CLI với các subcommands hỗ trợ Tiếng Việt chi tiết.

    Returns:
        ArgumentParser đã được đăng ký toàn bộ subcommands.
    """
    parser = argparse.ArgumentParser(
        prog="globalcart",
        description="GlobalCart Order Lakehouse CLI — chạy, kiểm tra và đọc Gold snapshot",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("doctor", help="Chẩn đoán môi trường Python, Java và Spark")
    commands.add_parser(
        "check", help="Kiểm tra chất lượng dữ liệu đầu vào, sinh báo cáo và chạy Pytest"
    )
    commands.add_parser("report", help="Sinh báo cáo kinh doanh từ Gold snapshot đã publish")

    reconcile_parser = commands.add_parser(
        "reconcile", help="Chạy kiểm tra đối soát doanh thu, grain và SCD2"
    )
    reconcile_parser.add_argument(
        "--run-id", type=str, default=None, help="Mã nhận diện phiên kiểm toán"
    )

    pipeline_parser = commands.add_parser(
        "pipeline", help="Chạy pipeline PySpark Medallion Lakehouse (Bootstrap / Incremental)"
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
        "--scd2",
        action="store_true",
        default=None,
        help="Kích hoạt mô hình hóa SCD Type 2 cho bảng Dimension Customer",
    )
    pipeline_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Cờ tương đương chế độ 'incremental'",
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

    if args.command == "pipeline":
        return run_pipeline_command(args)

    if args.command == "report":
        return run_report_command()

    raise ValueError(f"Lệnh không được hỗ trợ: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
