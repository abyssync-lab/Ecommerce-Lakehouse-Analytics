"""Kiểm tra nhanh file CSV trước khi đưa vào Bronze."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from data_quality import assert_quality, validate_business_values


def validate_input_file(csv_path: Path) -> int:
    """Kiểm tra một file CSV và trả về số dòng hợp lệ."""
    dataframe = pd.read_csv(csv_path)
    results = validate_business_values(dataframe)
    for result in results:
        icon = "ĐẠT" if result.passed else "LỖI"
        print(f"[{icon}] {result.rule}: {result.detail}")
    assert_quality(results)
    print(f"\nDữ liệu hợp lệ: {len(dataframe):,} dòng, {len(dataframe.columns)} cột.")
    return len(dataframe)


def main() -> None:
    # Windows có thể dùng code page cũ; ép UTF-8 để thông báo tiếng Việt không lỗi.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Kiểm tra chất lượng CSV Ecommerce")
    parser.add_argument("csv_path", type=Path, help="Đường dẫn file CSV cần kiểm tra")
    args = parser.parse_args()
    validate_input_file(args.csv_path)


if __name__ == "__main__":
    main()
