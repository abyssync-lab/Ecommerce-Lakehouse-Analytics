"""Ghi Gold snapshot và tạo view ổn định cho Power BI.

Delta chỉ atomic trong phạm vi từng table. Module này dùng hai lớp bảo vệ:

* Mỗi run được ghi riêng dưới ``gold/_runs/<run_id>`` để reconciliation đọc một snapshot.
* Serving giữ snapshot theo ``Publication_Run_ID``; view chỉ trả run đang nằm trong
  snapshot hiện hành. Run lỗi có thể để lại dữ liệu staging nhưng không đổi
  snapshot đang được sử dụng.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from config import SETTINGS
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from .storage import resolve_path, save_and_verify_delta

LOGGER = logging.getLogger(__name__)

PUBLICATION_NAME = "gold"
PUBLICATION_PATH = "/ecommerce/control/ctl_publications_delta"
SERVING_BASE = "/ecommerce/serving"


def _run_base(run_id: str) -> str:
    return f"{SETTINGS.gold_star_schema_base}/_runs/{run_id}"


def persist_gold_staging(
    spark: Any,
    tables: dict[str, Any],
    run_id: str,
    layer_name: str,
) -> dict[str, str]:
    """Ghi toàn bộ output của run vào path riêng, chưa đưa vào serving."""
    paths: dict[str, str] = {}
    for table_name, dataframe in tables.items():
        path = f"{_run_base(run_id)}/{table_name}_delta"
        save_and_verify_delta(dataframe, path, f"{layer_name}.{table_name}", mode="overwrite")
        paths[table_name] = path
    return paths


def _upsert_publication_pointer(spark: Any, run_id: str) -> None:
    """Cập nhật một dòng pointer sau khi mọi serving table đã ghi xong."""
    target_path = resolve_path(PUBLICATION_PATH)
    row = [
        (
            PUBLICATION_NAME,
            run_id,
            datetime.now(UTC).replace(tzinfo=None),
            "PUBLISHED",
        )
    ]
    schema = StructType(
        [
            StructField("publication_name", StringType(), nullable=False),
            StructField("current_run_id", StringType(), nullable=False),
            StructField("published_at", TimestampType(), nullable=False),
            StructField("status", StringType(), nullable=False),
        ]
    )
    source = spark.createDataFrame(row, schema)
    if not DeltaTable.isDeltaTable(spark, target_path):
        source.write.format("delta").mode("overwrite").save(target_path)
        return

    table = DeltaTable.forPath(spark, target_path)
    (
        table.alias("target")
        .merge(
            source.alias("source"),
            "target.publication_name = source.publication_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def publish_gold_run(spark: Any, tables: dict[str, Any], run_id: str) -> None:
    """Đưa snapshot đã reconciliation vào serving rồi mới đổi current pointer.

    Nếu retry cùng run, phần serving của run đó được thay thế trước khi append lại,
    tránh tạo duplicate rows trong trường hợp process chết giữa chừng.
    """
    for table_name, dataframe in tables.items():
        serving_path = resolve_path(f"{SERVING_BASE}/{table_name}_delta")
        published = dataframe.withColumn("Publication_Run_ID", lit(run_id))

        if DeltaTable.isDeltaTable(spark, serving_path):
            serving_table = DeltaTable.forPath(spark, serving_path)
            serving_table.delete(col("Publication_Run_ID") == run_id)
            published.write.format("delta").mode("append").save(serving_path)
        else:
            published.write.format("delta").mode("overwrite").save(serving_path)

    # Lần publish đầu tiên cần có giá trị mồi để Spark phân giải được view.
    publication_target = resolve_path(PUBLICATION_PATH)
    if not DeltaTable.isDeltaTable(spark, publication_target):
        _upsert_publication_pointer(spark, "__NONE__")

    # Tạo view trước, con trỏ đổi sau cùng. Nếu ghi dữ liệu hoặc tạo view lỗi,
    # current_run_id vẫn là run cũ và Power BI tiếp tục đọc snapshot cũ.
    _create_serving_views(spark, list(tables))
    _upsert_publication_pointer(spark, run_id)
    LOGGER.info("Gold snapshot đã publish: current_run_id=%s", run_id)


def _create_serving_views(spark: Any, table_names: list[str]) -> None:
    """Tạo stable views cho Power BI, luôn lọc theo pointer hiện hành."""
    spark.sql("CREATE DATABASE IF NOT EXISTS serving")
    publication_path = resolve_path(PUBLICATION_PATH)
    for table_name in table_names:
        serving_path = resolve_path(f"{SERVING_BASE}/{table_name}_delta")
        spark.sql(
            f"""
            CREATE OR REPLACE VIEW serving.{table_name} AS
            SELECT data.*
            FROM delta.`{serving_path}` data
            INNER JOIN delta.`{publication_path}` publication
              ON data.Publication_Run_ID = publication.current_run_id
            WHERE publication.publication_name = '{PUBLICATION_NAME}'
            """
        )


def get_current_publication(spark: Any) -> str | None:
    """Đọc pointer hiện tại để kiểm thử hoặc monitoring."""
    target_path = resolve_path(PUBLICATION_PATH)
    if not DeltaTable.isDeltaTable(spark, target_path):
        return None
    rows = (
        spark.read.format("delta")
        .load(target_path)
        .filter(col("publication_name") == PUBLICATION_NAME)
        .select("current_run_id")
        .limit(1)
        .collect()
    )
    return rows[0]["current_run_id"] if rows else None


def read_published_table(spark: Any, table_name: str, run_id: str | None = None) -> Any:
    """Đọc trực tiếp serving Delta theo publication pointer hiện hành.

    Stable view được tạo cho Power BI, nhưng catalog mặc định của Spark có thể
    chỉ sống trong một process. Báo cáo và CLI vì vậy không phụ thuộc vào view
    đã đăng ký từ process bootstrap trước đó; chúng đọc đúng path serving và
    lọc theo cùng ``Publication_Run_ID``.
    """
    current_run_id = run_id or get_current_publication(spark)
    if not current_run_id or current_run_id == "__NONE__":
        raise RuntimeError("Chưa có Gold publication khả dụng.")

    serving_path = resolve_path(f"{SERVING_BASE}/{table_name}_delta")
    if not DeltaTable.isDeltaTable(spark, serving_path):
        raise RuntimeError(f"Serving table chưa tồn tại: {table_name}")

    return (
        spark.read.format("delta")
        .load(serving_path)
        .filter(col("Publication_Run_ID") == current_run_id)
    )
