"""Luồng bootstrap đầy đủ từ nguồn CSV đến Gold đã đối soát."""

from __future__ import annotations

import logging
import uuid

from delta.tables import DeltaTable

from config import PIPELINE_VERSION, SETTINGS, PipelineConfig, PipelineRunResult

from .gold import build_and_publish_gold, customer_history_source
from .ingestion import calculate_source_hash, calculate_source_size, ingest_to_bronze
from .reconciliation import ReconciliationError
from .registry import BatchRegistry
from .silver import (
    build_silver_current_events,
    build_silver_order_lines_current,
    build_silver_orders_current,
    clean_and_enrich_silver,
)
from .spark import create_spark_session
from .storage import save_and_verify_delta

LOGGER = logging.getLogger(__name__)


def run_pipeline(
    input_path: str | None = None,
    use_scd2: bool | None = None,
    config: PipelineConfig | None = None,
) -> PipelineRunResult:
    """Khởi chạy Medallion Lakehouse Pipeline hoàn chỉnh (Bootstrap Full Mode).

    Quy trình xử lý một batch:
    1. Ingestion: Xác thực mã băm, kiểm tra sổ cái Batch Registry
    2. Bronze: Lưu trữ dữ liệu thô bất biến
    3. Silver: Ép kiểu, làm sạch, tách Quarantine theo luật hợp đồng
    4. Gold: Xây dựng Kimball Star Schema (FactSales, Dimensions SCD2)
    5. Gold Semantic & Marts: Sinh Canonical Semantic Dataset và Data Marts
    6. Gold reconciliation: Kiểm toán row conservation, grain, foreign key và tài chính.
    7. Published snapshot: Chỉ đổi snapshot Power BI sau khi reconciliation PASS.

    Args:
        input_path: Đường dẫn file CSV đầu vào tùy chọn.
        use_scd2: Bật mô hình hóa SCD Type 2 cho bảng dim_customer (ưu tiên hơn config/SETTINGS).
        config: Đối tượng PipelineConfig tập trung nếu có.

    Returns:
        PipelineRunResult đại diện cho trạng thái và số liệu của lượt chạy.
    """
    effective_scd2 = (
        use_scd2 if use_scd2 is not None else (config.use_scd2 if config else SETTINGS.use_scd2)
    )
    effective_input = (
        input_path if input_path is not None else (config.input_path if config else None)
    )
    source_uri = effective_input or SETTINGS.get_input_path()
    source_hash_error: Exception | None = None
    try:
        source_hash = calculate_source_hash(source_uri)
    except Exception as exc:
        # Không thể mã băm file thì không được giả làm mã băm nội dung. Giá trị này
        # chỉ để ghi nhận FAILED; giá trị này luôn bị loại khỏi tra cứu
        # idempotency; khi file xuất hiện lại sẽ tính mã băm thật.
        source_hash = "hash_unavailable"
        source_hash_error = exc
    run_id = config.run_id if config and config.run_id else f"run_{uuid.uuid4().hex[:8]}"
    batch_id = config.batch_id if config and config.batch_id else f"batch_{uuid.uuid4().hex[:8]}"

    LOGGER.info("=====================================================")
    LOGGER.info("BẮT ĐẦU GLOBALCART ORDER LAKEHOUSE PIPELINE")
    LOGGER.info(
        "Mode: BOOTSTRAP | Run ID: %s | Batch ID: %s | SCD2: %s", run_id, batch_id, effective_scd2
    )
    LOGGER.info("=====================================================")

    spark = create_spark_session()
    registry = BatchRegistry(spark)
    clean_df = None
    pipeline_succeeded = False
    run_started = False

    try:
        # Bootstrap vẫn phải an toàn khi chạy lại. Chỉ thao tác reset demo có chủ
        # đích mới được dọn storage; mã băm đã công bố không được nạp lại.
        if registry.is_batch_processed(source_hash):
            previous = registry.find_by_source_hash(source_hash)
            LOGGER.warning(
                "Source hash %s đã được publish ở run=%s; bỏ qua bootstrap replay.",
                source_hash[:10],
                previous["run_id"] if previous else "unknown",
            )
            return PipelineRunResult(
                run_id=run_id,
                batch_id=batch_id,
                status="SKIPPED",
                bronze_rows=0,
                silver_rows=0,
                quarantine_rows=0,
                duplicate_rows=0,
                reconciliation_passed=True,
                spark=spark,
            )

        registry.start_run(
            run_id=run_id,
            batch_id=batch_id,
            source_uri=source_uri,
            source_hash=source_hash,
            source_size_bytes=calculate_source_size(source_uri),
            pipeline_version=PIPELINE_VERSION,
            contract_version="1.0.0",
        )
        run_started = True

        if source_hash_error is not None:
            raise source_hash_error

        # 1. Bronze: đọc file, kiểm tra schema và append event thô kèm metadata truy vết.
        LOGGER.info("--- 1. INGESTION & 2. BRONZE LAYER ---")
        bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
        if DeltaTable.isDeltaTable(spark, bronze_path):
            raise ValueError(
                "BOOTSTRAP_REQUIRES_EMPTY_BRONZE: Bronze đã có dữ liệu; "
                "hãy dùng incremental hoặc dọn local storage có chủ đích trước khi bootstrap."
            )
        bronze_df = ingest_to_bronze(
            spark,
            effective_input,
            # Bootstrap bình thường append vào Bronze bất biến; việc xóa dữ liệu
            # phải do người vận hành thực hiện ngoài pipeline với phạm vi được xác nhận.
            mode="append",
            batch_id=batch_id,
            run_id=run_id,
            source_hash=source_hash,
            manage_registry=False,
            contract_version="1.0.0",
        )
        bronze_rows = bronze_df.count()
        registry.update_metrics(run_id, raw_rows=bronze_rows)
        registry.mark_validated(run_id)

        # 2. Silver: làm sạch, quarantine và tính đối soát số dòng của batch.
        LOGGER.info("--- 3. SILVER LAYER & QUARANTINE ---")
        quarantine_path = (
            config.quarantine_path
            if config and config.quarantine_path
            else SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
        )
        clean_df = clean_and_enrich_silver(
            bronze_df,
            quarantine_path=quarantine_path,
            run_id=run_id,
            batch_id=batch_id,
        )
        silver_rows = clean_df.count()
        if bronze_rows > 0 and silver_rows == 0:
            raise ValueError(
                "NO_VALID_EVENTS: bootstrap không có event hợp lệ để dựng Silver và Gold"
            )
        bronze_business_columns = [
            column for column in bronze_df.columns if not column.startswith("_")
        ]
        duplicate_rows = (
            bronze_rows - bronze_df.dropDuplicates(subset=bronze_business_columns).count()
        )
        quarantine_rows = max(0, bronze_rows - silver_rows - duplicate_rows)
        registry.update_metrics(
            run_id,
            exact_duplicate_rows=duplicate_rows,
            rejected_rows=quarantine_rows,
            valid_event_rows=silver_rows,
        )

        # Bảng Silver backing phải là current-state theo line. Bronze mới giữ mọi
        # version; nếu ghi toàn bộ event vào đây, MERGE lần sau có thể gặp nhiều
        # target cùng một khóa và làm mất tính xác định của incremental processing.
        current_silver = build_silver_current_events(clean_df)
        save_and_verify_delta(
            current_silver, SETTINGS.silver_delta, "silver.ecommerce_clean", mode="overwrite"
        )
        silver_orders_current = build_silver_orders_current(current_silver)
        silver_order_lines_current = build_silver_order_lines_current(current_silver)
        save_and_verify_delta(
            silver_orders_current,
            SETTINGS.silver_orders_delta,
            "silver.silver_orders_current",
            mode="overwrite",
        )
        save_and_verify_delta(
            silver_order_lines_current,
            SETTINGS.silver_order_lines_delta,
            "silver.silver_order_lines_current",
            mode="overwrite",
        )
        registry.mark_silver_merged(run_id)

        # 3. Gold: dựng star schema, semantic marts, đối soát và publish.
        recon_report = build_and_publish_gold(
            spark=spark,
            active_silver=current_silver,
            silver_orders_current=silver_orders_current,
            silver_order_lines_current=silver_order_lines_current,
            effective_scd2=effective_scd2,
            customer_history_source=customer_history_source(spark, clean_df, effective_scd2),
            run_id=run_id,
            registry=registry,
            raw_count=bronze_rows,
            duplicate_count=duplicate_rows,
            invalid_count=quarantine_rows,
            valid_count=silver_rows,
            published_version="1",
        )

        LOGGER.info("HOÀN THÀNH PIPELINE: Gold snapshot đã được publish.")
        pipeline_succeeded = True
        return PipelineRunResult(
            run_id=run_id,
            batch_id=batch_id,
            status="SUCCESS",
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            quarantine_rows=quarantine_rows,
            duplicate_rows=duplicate_rows,
            reconciliation_passed=True,
            reconciliation_report=recon_report,
            published_run_id=run_id,
            spark=spark,
        )
    except Exception as exc:
        # Lỗi ghi Registry phải được giữ nguyên để người vận hành biết metadata bị lỗi.
        if run_started:
            current = registry.find_by_run_id(run_id)
            if current is None or current["status"] != "PUBLISH_METADATA_PENDING":
                registry.mark_failed(
                    run_id,
                    exc,
                    error_code=(
                        "RECONCILIATION_FAILED"
                        if isinstance(exc, ReconciliationError)
                        else "SOURCE_FILE_NOT_FOUND"
                        if isinstance(exc, FileNotFoundError)
                        else "PIPELINE_FAILED"
                    ),
                )
        raise

    finally:
        if clean_df is not None:
            clean_df.unpersist()
        if not pipeline_succeeded:
            # CLI không nhận được Spark để đóng khi pipeline lỗi.
            spark.stop()
