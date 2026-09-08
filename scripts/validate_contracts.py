"""Kiểm tra tất cả YAML contract trước khi pipeline được chạy."""

from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = PROJECT_ROOT / "contracts"


def main() -> int:
    """Parse contract và dừng với mã lỗi nếu file YAML không hợp lệ."""
    contract_paths = sorted(CONTRACTS_DIR.glob("*.yaml"))
    if not contract_paths:
        raise SystemExit("Không tìm thấy YAML contract trong thư mục contracts")

    for contract_path in contract_paths:
        with contract_path.open("r", encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if document is None:
            raise SystemExit(f"Contract rỗng: {contract_path}")
        print(f"VALID: {contract_path.relative_to(PROJECT_ROOT)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
