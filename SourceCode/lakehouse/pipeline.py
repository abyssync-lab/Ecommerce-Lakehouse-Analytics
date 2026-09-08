"""Module orchestrate toàn bộ Data Lakehouse Pipeline (Bronze -> Silver -> Gold)."""

from __future__ import annotations

import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any

# Tôn trọng runtime do CI hoặc cluster cung cấp; local mới dùng Python hiện tại.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from config import SETTINGS, PipelineConfig, auto_set_spark_home_env

auto_set_spark_home_env()

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")
if SETTINGS.use_local_storage and not os.environ.get("SPARK_LOCAL_IP"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
if SETTINGS.use_local_storage and not os.environ.get("SPARK_LOCAL_HOSTNAME"):
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

from .dimensions import build_all_dimensions
from .file_manifest import FileManifest
from .ingestion import (
    calculate_source_hash,
    calculate_source_size,
    enrich_with_ingestion_metadata,
    ingest_to_bronze,
)
from .marts import (
    build_certified_marts,
    build_fact_order_fulfillment,
    build_fact_sales,
    build_sales_enriched,
)
from .publication import persist_gold_staging, publish_gold_run
from .reconciliation import run_full_reconciliation
from .registry import BatchRegistry
from .silver import (
    build_silver_current_events,
    build_silver_order_lines_current,
    build_silver_orders_current,
    clean_and_enrich_silver,
)
from .storage import (
    check_schema_enforcement,
    check_versioning_and_time_travel,
    save_and_verify_delta,
    show_delta_history,
)

try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
except ImportError as error:
    raise ImportError(
        "\nChưa cài đặt Delta Lake.\nHãy chạy lệnh:\npython -m pip install delta-spark\n"
    ) from error

LOGGER = logging.getLogger(__name__)


def _enforce_reject_rate(
    raw_rows: int, rejected_rows: int, max_reject_rate: float | None = None
) -> None:
    """Chặn batch có tỷ lệ quarantine vượt ngưỡng vận hành đã cấu hình."""
    if raw_rows <= 0:
        return
    threshold = SETTINGS.max_reject_rate if max_reject_rate is None else max_reject_rate
    reject_rate = rejected_rows / raw_rows
    if reject_rate > threshold:
        raise ValueError(
            f"REJECT_RATE_EXCEEDED: tỷ lệ quarantine {reject_rate:.2%} vượt ngưỡng {threshold:.2%}"
        )


def _customer_history_source(spark: SparkSession, fallback_df: Any, use_scd2: bool) -> Any | None:
    """Chuẩn hóa lại Bronze history để SCD2 không mất late customer events.

    Silver current-state chỉ giữ version thắng cuối cùng, nên không đủ để dựng
    khoảng thời gian SCD2. Bronze là nguồn audit duy nhất chứa toàn bộ event; các
    dòng lỗi được lọc bằng cùng Silver quality rules nhưng không ghi thêm metrics
    hoặc quarantine trong lúc rebuild dimension.
    """
    if not use_scd2:
        return None
    bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return fallback_df
    bronze_history = spark.read.format("delta").load(bronze_path)
    history = clean_and_enrich_silver(
        bronze_history,
        quarantine_path=None,
        run_id="scd2_history_rebuild",
        batch_id="scd2_history_rebuild",
        allow_line_id_fallback=True,
        record_quality_metrics=False,
    )
    if "Operation" in history.columns:
        history = history.filter(col("Operation") != "DELETE")
    if "Customer_ID" not in history.columns:
        return fallback_df
    history = history.filter(col("Customer_ID").isNotNull())
    for column in ("Customer_Gender", "Customer_Segment"):
        if column not in history.columns:
            history = history.withColumn(column, lit("Unknown"))
    if "Order_Date" not in history.columns:
        history = history.withColumn("Order_Date", lit(None).cast("date"))
    if "_record_hash" in history.columns:
        history = history.dropDuplicates(["_record_hash"])
    return history


@dataclass
class PipelineRunResult:
    """Kết quả hoàn chỉnh của một lượt thực thi Pipeline Lakehouse."""

    run_id: str
    batch_id: str
    status: str  # "SUCCESS", "FAILED", "SKIPPED"
    bronze_rows: int
    silver_rows: int
    quarantine_rows: int
    duplicate_rows: int
    reconciliation_passed: bool
    reconciliation_report: dict[str, Any] = field(default_factory=dict)
    certified_gold_version: int | None = None
    error_message: str | None = None
    spark: SparkSession | None = None


class PipelineCertificationError(Exception):
    """Ngoại lệ phát sinh khi Pipeline không vượt qua cổng kiểm toán Reconciliation Gate."""

    pass


def create_spark_session() -> SparkSession:
    """Khởi tạo và cấu hình SparkSession tích hợp Delta Lake & Hive Metastore."""
    driver_host = SETTINGS.spark_driver_host or (
        "127.0.0.1" if SETTINGS.use_local_storage else None
    )
    driver_bind = SETTINGS.spark_driver_bind_address or (
        "127.0.0.1" if SETTINGS.use_local_storage else None
    )

    builder = (
        SparkSession.builder.appName("Global Cart Intelligence Data Lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    if driver_bind:
        builder = builder.config("spark.driver.bindAddress", driver_bind)

    if not SETTINGS.use_local_storage:
        builder = builder.config("spark.hadoop.fs.defaultFS", SETTINGS.hdfs_base)

    try:
        builder = builder.enableHiveSupport()
    except Exception:
        LOGGER.warning(
            "Không thể bật Hive Support, sẽ chạy Spark local mode không có Hive metastore."
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_delta_demo(spark: SparkSession) -> None:
    """Chạy các bài thử nghiệm minh họa tính năng Delta Lake (Schema Enforcement, Time Travel, History)."""
    LOGGER.info("=== BẮT ĐẦU DELTA LAKEHOUSE DEMONSTRATION ===")
    show_delta_history(spark, SETTINGS.bronze_delta, "BRONZE")
    show_delta_history(spark, SETTINGS.silver_delta, "SILVER")
    check_schema_enforcement(spark)

    silver_path = SETTINGS.get_storage_path(SETTINGS.silver_delta)
    clean_df = spark.read.format("delta").load(silver_path)
    check_versioning_and_time_travel(spark, clean_df)
    LOGGER.info("=== HOÀN TẤT DELTA LAKEHOUSE DEMONSTRATION ===")


def run_pipeline(
    input_path: str | None = None,
    use_scd2: bool | None = None,
    config: PipelineConfig | None = None,
) -> PipelineRunResult:
    """Khởi chạy Medallion Lakehouse Pipeline hoàn chỉnh (Bootstrap Full Mode).

    Quy trình chuẩn hóa Enterprise Lifecycle:
    1. Ingestion: Xác thực mã băm, kiểm tra sổ cái Batch Registry
    2. Bronze: Lưu trữ dữ liệu thô bất biến
    3. Silver: Ép kiểu, làm sạch, tách Quarantine theo luật hợp đồng
    4. Gold: Xây dựng Kimball Star Schema (FactSales, Dimensions SCD2)
    5. Gold Semantic & Marts: Sinh Canonical Semantic Dataset và Data Marts
    6. Reconciliation Gate: Kiểm toán các bất biến bắt buộc (Doanh thu, Row conservation, Grain, FK, SCD2)
    7. Certification: Ghi nhận chứng nhận vào sổ cái nếu PASS, chặn đứng nếu FAIL.

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
        # Không thể hash file thì không được giả làm content hash. Sentinel này
        # chỉ giúp ghi nhận FAILED trong control plane và luôn bị loại khỏi
        # idempotency lookup; khi file xuất hiện lại sẽ tính hash thật.
        source_hash = "hash_unavailable"
        source_hash_error = exc
    run_id = config.run_id if config and config.run_id else f"run_{uuid.uuid4().hex[:8]}"
    batch_id = config.batch_id if config and config.batch_id else f"batch_{uuid.uuid4().hex[:8]}"

    LOGGER.info("=====================================================")
    LOGGER.info("BẮT ĐẦU GLOBAL CART INTELLIGENCE LAKEHOUSE PIPELINE")
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
        # Bootstrap cũng phải replay-safe. Chỉ môi trường demo/reset mới được phép
        # overwrite một source hash đã certified; run bình thường không được nạp lại.
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
            pipeline_version="1.1.0",
            contract_version="1.0.0",
        )
        run_started = True

        if source_hash_error is not None:
            raise source_hash_error

        # 1. Bronze: đọc file, validate schema và append raw event kèm lineage.
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
            # phải do operator thực hiện ngoài pipeline với phạm vi được xác nhận.
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

        # 2. Silver: clean, quarantine và tính accounting của batch.
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
        bronze_business_columns = [
            column for column in bronze_df.columns if not column.startswith("_")
        ]
        duplicate_rows = (
            bronze_rows - bronze_df.dropDuplicates(subset=bronze_business_columns).count()
        )
        quarantine_rows = max(0, bronze_rows - silver_rows - duplicate_rows)
        _enforce_reject_rate(
            bronze_rows,
            quarantine_rows,
            config.max_reject_rate if config else None,
        )
        registry.update_metrics(
            run_id,
            exact_duplicate_rows=duplicate_rows,
            rejected_rows=quarantine_rows,
            valid_event_rows=silver_rows,
        )

        save_and_verify_delta(
            clean_df, SETTINGS.silver_delta, "silver.ecommerce_clean", mode="overwrite"
        )
        # Hai bảng current-state có grain rõ ràng; bảng combined ở trên chỉ
        # giữ tương thích ngược trong giai đoạn chuyển đổi source code.
        silver_orders_current = build_silver_orders_current(clean_df)
        silver_order_lines_current = build_silver_order_lines_current(clean_df)
        current_silver = build_silver_current_events(clean_df)
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

        # 3. Gold: build star schema và semantic marts từ cùng một Silver snapshot.
        LOGGER.info("--- 4. GOLD LAYER - STAR SCHEMA (SCD2=%s) ---", effective_scd2)
        dimensions = build_all_dimensions(
            spark,
            current_silver,
            use_scd2=effective_scd2,
            customer_history_df=_customer_history_source(spark, clean_df, effective_scd2),
        )
        fact_sales = build_fact_sales(current_silver, dimensions)
        fact_order_fulfillment = build_fact_order_fulfillment(
            silver_orders_current,
            silver_order_lines_current,
            dimensions,
        )
        persist_gold_staging(
            spark,
            {
                **dimensions,
                "fact_sales_line": fact_sales,
                "fact_order_fulfillment": fact_order_fulfillment,
            },
            run_id,
            "gold_star",
        )

        LOGGER.info("--- 5. GOLD LAYER - CANONICAL SEMANTIC BASE & MARTS ---")
        sales_enriched = build_sales_enriched(fact_sales, dimensions)
        persist_gold_staging(
            spark,
            {"gold_sales_enriched": sales_enriched},
            run_id,
            "gold_semantic",
        )
        gold_marts = build_certified_marts(sales_enriched, fact_order_fulfillment)
        persist_gold_staging(spark, gold_marts, run_id, "gold_mart")
        registry.update_status(run_id, "GOLD_BUILT", gold_run_id=run_id)

        # 4. Chỉ certification sau reconciliation; mọi exception đều đi qua FAILED.
        LOGGER.info("--- 6. DATA RECONCILIATION & CERTIFICATION GATE ---")
        recon_report = run_full_reconciliation(
            clean_df=current_silver,
            fact_sales=fact_sales,
            mart_overview=gold_marts["mart_executive_daily"],
            dim_customer=dimensions["dim_customer"],
            raw_count=bronze_rows,
            duplicate_count=duplicate_rows,
            invalid_count=quarantine_rows,
            run_id=run_id,
            valid_count=silver_rows,
            silver_orders_current=silver_orders_current,
            silver_order_lines_current=silver_order_lines_current,
            fact_order_fulfillment=fact_order_fulfillment,
        )
        if recon_report.get("overall_status") != "PASS":
            raise PipelineCertificationError(
                f"Pipeline không đạt chứng nhận Reconciliation Gate: {recon_report}"
            )

        run_demo = config.run_delta_demo if config else SETTINGS.run_delta_demo
        if run_demo:
            run_delta_demo(spark)

        registry.mark_reconciled(run_id)
        registry.mark_ready_to_publish(run_id)
        publish_gold_run(
            spark,
            {
                **dimensions,
                "fact_sales_line": fact_sales,
                "fact_order_fulfillment": fact_order_fulfillment,
                "gold_sales_enriched": sales_enriched,
                **gold_marts,
            },
            run_id,
        )
        try:
            registry.mark_published(run_id, gold_run_id=run_id, published_version="1")
        except Exception as finalization_error:
            # Pointer đã commit thì run không được gắn FAILED, vì Power BI đã
            # nhìn thấy run này; operator cần xử lý trạng thái control pending.
            registry.mark_control_finalization_pending(run_id, finalization_error)
            raise

        LOGGER.info("HOÀN THÀNH DATA LAKEHOUSE PIPELINE THÀNH CÔNG (CERTIFIED PASS)!")
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
            certified_gold_version=1,
            spark=spark,
        )
    except Exception as exc:
        # Registry write failure phải được giữ nguyên để operator biết control plane hỏng.
        if run_started:
            current = registry.find_by_run_id(run_id)
            if current is None or current["status"] != "CONTROL_FINALIZATION_PENDING":
                registry.mark_failed(
                    run_id,
                    exc,
                    error_code=(
                        "RECONCILIATION_FAILED"
                        if isinstance(exc, PipelineCertificationError)
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


def run_incremental_pipeline(
    new_batch_df: Any,
    spark: SparkSession | None = None,
    batch_id: str | None = None,
    use_scd2: bool | None = None,
    config: PipelineConfig | None = None,
    source_hash: str | None = None,
    source_uri: str | None = None,
) -> PipelineRunResult:
    """Thực thi Incremental Ingestion & Delta MERGE INTO từ Bronze -> Silver -> Gold.

    Quy trình:
    1. Kiểm tra Batch Registry: Bỏ qua nếu batch hash đã xử lý thành công (Idempotency)
    2. Bronze: Append dữ liệu mới kèm Ingestion Metadata
    3. Silver: Làm sạch, lọc Quarantine và MERGE INTO theo stable Order_Line_ID grain
    4. Gold: Cập nhật Star Schema và deterministic refresh Gold Marts
    5. Reconciliation Gate: Kiểm toán các bất biến bắt buộc trước khi chứng nhận.
    """
    effective_scd2 = (
        use_scd2 if use_scd2 is not None else (config.use_scd2 if config else SETTINGS.use_scd2)
    )
    bid = (
        batch_id
        if batch_id is not None
        else (
            config.batch_id if config and config.batch_id else f"inc_batch_{uuid.uuid4().hex[:8]}"
        )
    )
    run_id = config.run_id if config and config.run_id else f"run_{uuid.uuid4().hex[:8]}"

    # Incremental không được tự sinh hash từ batch_id. Nếu làm vậy thì replay cùng file
    # nhưng đổi batch_id sẽ nạp lại Bronze và phá vỡ idempotency theo nội dung.
    if not source_hash:
        raise ValueError(
            "Incremental pipeline bắt buộc nhận source_hash SHA-256 của nội dung file."
        )
    LOGGER.info("=====================================================")
    LOGGER.info("THỰC THI INCREMENTAL PIPELINE WITH DELTA MERGE")
    LOGGER.info("Run ID: %s | Batch ID: %s | SCD2: %s", run_id, bid, effective_scd2)
    LOGGER.info("=====================================================")

    owns_spark = spark is None
    if spark is None:
        spark = create_spark_session()

    registry = BatchRegistry(spark)
    manifest = FileManifest(spark)
    shash = source_hash
    source_location = source_uri or "incremental_dataframe"

    # Idempotency kiểm tra trước mọi side effect. Cùng content hash đã PUBLISHED/SUCCESS
    # phải trả về SKIPPED, còn FAILED vẫn được phép retry.
    try:
        already_processed = registry.is_batch_processed(shash)
    except Exception:
        # Nếu control plane không đọc được, không được để Spark do function sở hữu
        # chạy ngầm sau khi caller đã nhận lỗi.
        if owns_spark:
            spark.stop()
        raise
    if already_processed:
        LOGGER.warning("Batch %s (Hash: %s) ĐÃ XỬ LÝ THÀNH CÔNG TRƯỚC ĐÓ. Bỏ qua.", bid, shash[:10])
        if owns_spark:
            spark.stop()
        return PipelineRunResult(
            run_id=run_id,
            batch_id=bid,
            status="SKIPPED",
            bronze_rows=0,
            silver_rows=0,
            quarantine_rows=0,
            duplicate_rows=0,
            reconciliation_passed=True,
            spark=spark,
        )

    raw_count = 0
    clean_batch = None
    manifest_registered = False
    bronze_committed = False
    run_started = False
    try:
        raw_count = new_batch_df.count()
        registry.start_run(
            run_id=run_id,
            batch_id=bid,
            source_uri=source_location,
            source_hash=shash,
            raw_rows=raw_count,
            source_size_bytes=calculate_source_size(source_location),
            pipeline_version="1.0.0",
            contract_version="2.0.0",
        )
        run_started = True

        # Đăng ký file trước file-level validation để schema lỗi vẫn xuất hiện
        # trong control plane và có thể phân biệt với file chưa từng được phát hiện.
        manifest.register_discovered(
            source_system="ecommerce_csv",
            source_hash=shash,
            source_uri=source_location,
            file_size_bytes=calculate_source_size(source_location),
            contract_version="2.0.0",
            run_id=run_id,
        )
        manifest_registered = True

        required_event_columns = {"Order_ID", "Order_Line_ID", "Source_Updated_At", "Operation"}
        missing_event_columns = required_event_columns.difference(new_batch_df.columns)
        if missing_event_columns:
            raise ValueError(
                "FILE_SCHEMA_MISMATCH: incremental batch thiếu cột contract v2: "
                + ", ".join(sorted(missing_event_columns))
            )

        bronze_path = SETTINGS.get_storage_path(SETTINGS.bronze_delta)
        bronze_has_source = DeltaTable.isDeltaTable(spark, bronze_path) and (
            spark.read.format("delta")
            .load(bronze_path)
            .filter(col("_source_hash") == shash)
            .limit(1)
            .count()
            > 0
        )
        if manifest.is_bronze_committed("ecommerce_csv", shash) or bronze_has_source:
            # Retry sau Gold failure: Bronze đã an toàn, chỉ đọc lại đúng file
            # theo content hash và tiếp tục Silver/Gold.
            enriched_batch = (
                spark.read.format("delta").load(bronze_path).filter(col("_source_hash") == shash)
            )
            bronze_committed = True
            if not manifest.is_bronze_committed("ecommerce_csv", shash):
                # Khôi phục manifest nếu process chết sau Delta commit nhưng
                # trước bước cập nhật control plane.
                manifest.register_discovered(
                    source_system="ecommerce_csv",
                    source_hash=shash,
                    source_uri=source_location,
                    file_size_bytes=calculate_source_size(source_location),
                    contract_version="2.0.0",
                    run_id=run_id,
                )
                manifest.mark_bronze_committed(
                    source_system="ecommerce_csv",
                    source_hash=shash,
                    run_id=run_id,
                    raw_rows=enriched_batch.count(),
                )
            LOGGER.info(
                "Retry run=%s dùng lại Bronze đã commit cho source_hash=%s; không append.",
                run_id,
                shash[:10],
            )
        else:
            manifest.register_discovered(
                source_system="ecommerce_csv",
                source_hash=shash,
                source_uri=source_location,
                file_size_bytes=calculate_source_size(source_location),
                contract_version="2.0.0",
                run_id=run_id,
            )
            # Bronze chỉ append raw event và metadata; không cập nhật current state ở đây.
            enriched_batch = enrich_with_ingestion_metadata(
                new_batch_df,
                batch_id=bid,
                run_id=run_id,
                source_hash=shash,
                source_uri=source_location,
                contract_version="2.0.0",
            )
            enriched_batch.write.format("delta").mode("append").save(bronze_path)
            # Delta append đã thành công; registry file chỉ là bước cập nhật
            # control plane tiếp theo và không được đánh đồng hai sự kiện này.
            bronze_committed = True
            manifest.mark_bronze_committed(
                source_system="ecommerce_csv",
                source_hash=shash,
                run_id=run_id,
                raw_rows=raw_count,
            )
        registry.mark_validated(run_id)
        LOGGER.info(
            "Đã append %d dòng bản ghi mới vào Bronze Delta table (Batch: %s).", raw_count, bid
        )

        # 2. Silver quality + quarantine theo đúng run hiện tại.
        quarantine_path = (
            config.quarantine_path
            if config and config.quarantine_path
            else SETTINGS.get_storage_path(SETTINGS.quarantine_delta)
        )
        clean_batch = clean_and_enrich_silver(
            enriched_batch,
            quarantine_path=quarantine_path,
            run_id=run_id,
            batch_id=bid,
            allow_line_id_fallback=False,
        )
        valid_count = clean_batch.count()
        winning_window = Window.partitionBy("Order_ID", "Order_Line_ID").orderBy(
            col("Source_Updated_At").desc(),
            col("_source_row_number").desc(),
        )
        merge_batch = (
            clean_batch.withColumn("_winning_row", row_number().over(winning_window))
            .filter(col("_winning_row") == 1)
            .drop("_winning_row")
        )
        superseded_count = valid_count - merge_batch.count()
        batch_business_columns = [
            column for column in new_batch_df.columns if not column.startswith("_")
        ]
        deduplicated_batch_count = new_batch_df.dropDuplicates(
            subset=batch_business_columns
        ).count()
        batch_duplicate_count = raw_count - deduplicated_batch_count
        batch_rejected_count = max(0, raw_count - batch_duplicate_count - valid_count)
        sequence_conflict_count = 0
        _enforce_reject_rate(
            raw_count,
            batch_rejected_count,
            config.max_reject_rate if config else None,
        )

        # 3. Silver MERGE: chỉ dùng stable key của contract v2, tuyệt đối không fallback
        # sang Product_Name hay các thuộc tính mutable.
        silver_path = SETTINGS.get_storage_path(SETTINGS.silver_delta)
        inserted_rows = updated_rows = unchanged_rows = stale_rows = deleted_rows = 0
        orphan_delete_rows = 0
        if DeltaTable.isDeltaTable(spark, silver_path):
            silver_delta_table = DeltaTable.forPath(spark, silver_path)
            existing_cols = silver_delta_table.toDF().columns
            required_target_columns = {
                "Order_ID",
                "Order_Line_ID",
                "Source_Updated_At",
                "Is_Deleted",
            }
            missing_target_columns = required_target_columns.difference(existing_cols)
            if missing_target_columns:
                raise ValueError(
                    "Silver hiện tại chưa theo contract v2; thiếu cột: "
                    + ", ".join(sorted(missing_target_columns))
                )
            missing_merge_columns = set(merge_batch.columns).difference(existing_cols)
            if missing_merge_columns:
                raise ValueError(
                    "Silver schema không tương thích với event batch; thiếu cột đích: "
                    + ", ".join(sorted(missing_merge_columns))
                )

            target_snapshot = silver_delta_table.toDF().select(
                col("Order_ID").alias("_target_order_id"),
                col("Order_Line_ID").alias("_target_line_id"),
                col("Source_Updated_At").alias("_target_updated_at"),
                col("_record_hash").alias("_target_record_hash"),
            )
            comparison = merge_batch.alias("source").join(
                target_snapshot.alias("target"),
                (col("source.Order_ID") == col("target._target_order_id"))
                & (col("source.Order_Line_ID") == col("target._target_line_id")),
                how="left",
            )
            target_exists = col("target._target_order_id").isNotNull()
            source_newer = col("source.Source_Updated_At") > col("target._target_updated_at")
            same_hash = col("source._record_hash").isNotNull() & (
                col("source._record_hash") == col("target._target_record_hash")
            )

            # Cùng key và cùng timestamp với current state nhưng khác nội dung là
            # sequence conflict ở cấp lịch sử, không được âm thầm bỏ qua.
            historical_conflict_condition = (
                target_exists
                & (col("source.Source_Updated_At") == col("target._target_updated_at"))
                & ~same_hash
            )
            historical_conflicts = comparison.filter(historical_conflict_condition).select(
                *[col(f"source.{column}").alias(column) for column in merge_batch.columns]
            )
            sequence_conflict_count = historical_conflicts.count()
            if sequence_conflict_count:
                rejected_historical = (
                    historical_conflicts.withColumn(
                        "rejection_reasons", array(lit("SEQUENCE_CONFLICT"))
                    )
                    .withColumn("rejection_reason", lit("SEQUENCE_CONFLICT"))
                    .withColumn("rejected_at", current_timestamp())
                )
                rejected_historical.write.format("delta").mode("append").option(
                    "mergeSchema", "true"
                ).save(quarantine_path)
                conflict_keys = historical_conflicts.select(
                    "Order_ID", "Order_Line_ID"
                ).dropDuplicates()
                merge_batch = merge_batch.join(
                    conflict_keys, on=["Order_ID", "Order_Line_ID"], how="left_anti"
                )
                valid_count -= sequence_conflict_count
                batch_rejected_count += sequence_conflict_count
                _enforce_reject_rate(
                    raw_count,
                    batch_rejected_count,
                    config.max_reject_rate if config else None,
                )

                # Tính lại comparison trên đúng tập event được phép merge.
                comparison = merge_batch.alias("source").join(
                    target_snapshot.alias("target"),
                    (col("source.Order_ID") == col("target._target_order_id"))
                    & (col("source.Order_Line_ID") == col("target._target_line_id")),
                    how="left",
                )
                target_exists = col("target._target_order_id").isNotNull()
                source_newer = col("source.Source_Updated_At") > col("target._target_updated_at")
                same_hash = col("source._record_hash").isNotNull() & (
                    col("source._record_hash") == col("target._target_record_hash")
                )

            inserted_rows = comparison.filter(
                ~target_exists & (col("source.Operation") != "DELETE")
            ).count()
            updated_rows = comparison.filter(
                target_exists & source_newer & (col("source.Operation") != "DELETE") & ~same_hash
            ).count()
            unchanged_rows = comparison.filter(target_exists & same_hash).count()
            stale_rows = comparison.filter(
                target_exists
                & (col("source.Source_Updated_At") < col("target._target_updated_at"))
                & ~same_hash
            ).count()
            deleted_rows = comparison.filter(
                target_exists & source_newer & (col("source.Operation") == "DELETE")
            ).count()
            orphan_delete_rows = comparison.filter(
                ~target_exists & (col("source.Operation") == "DELETE")
            ).count()
            if orphan_delete_rows:
                # DELETE không có current-state target phải được audit như dữ liệu
                # lỗi; không được coi là valid event đã merge thành công.
                orphan_deletes = comparison.filter(
                    ~target_exists & (col("source.Operation") == "DELETE")
                ).select(*[col(f"source.{column}").alias(column) for column in merge_batch.columns])
                (
                    orphan_deletes.withColumn("error_codes", array(lit("ORPHAN_DELETE")))
                    .withColumn("rejection_reasons", col("error_codes"))
                    .withColumn("rejection_reason", lit("ORPHAN_DELETE"))
                    .withColumn("rejected_at", current_timestamp())
                    .write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(quarantine_path)
                )
                valid_count -= orphan_delete_rows
                batch_rejected_count += orphan_delete_rows
                _enforce_reject_rate(
                    raw_count,
                    batch_rejected_count,
                    config.max_reject_rate if config else None,
                )
                merge_batch = merge_batch.join(
                    orphan_deletes.select("Order_ID", "Order_Line_ID").dropDuplicates(),
                    on=["Order_ID", "Order_Line_ID"],
                    how="left_anti",
                )

            merge_cond = (
                "target.Order_ID = source.Order_ID AND target.Order_Line_ID = source.Order_Line_ID"
            )
            LOGGER.info("Thực thi Delta MERGE INTO Silver với điều kiện: %s", merge_cond)
            delete_assignments = {
                column: f"source.{column}"
                for column in [
                    "Source_Updated_At",
                    "Operation",
                    "_record_hash",
                    "_run_id",
                    "_batch_id",
                    "_source_hash",
                    "_source_uri",
                    "_source_file",
                    "_source_row_number",
                    "_ingested_at",
                    "_contract_version",
                    "_pipeline_version",
                ]
                if column in existing_cols and column in merge_batch.columns
            }
            delete_assignments["Is_Deleted"] = "true"
            insert_assignments = {
                column: f"source.{column}"
                for column in merge_batch.columns
                if column in existing_cols
            }
            (
                silver_delta_table.alias("target")
                .merge(merge_batch.alias("source"), merge_cond)
                .whenMatchedUpdate(
                    condition=(
                        "source.Source_Updated_At > target.Source_Updated_At "
                        "AND source.Operation = 'DELETE'"
                    ),
                    set=delete_assignments,
                )
                # UPSERT event cũ/stale không được ghi đè event mới.
                .whenMatchedUpdateAll(
                    condition=(
                        "source.Source_Updated_At > target.Source_Updated_At "
                        "AND source.Operation <> 'DELETE'"
                    )
                )
                # Orphan DELETE chỉ là metric/quarantine, không tự tạo current-state row.
                .whenNotMatchedInsert(
                    condition="source.Operation <> 'DELETE'",
                    values=insert_assignments,
                )
                .execute()
            )
            LOGGER.info("Đã hoàn tất Delta MERGE INTO tầng Silver.")
        else:
            silver_inserts = merge_batch.filter(col("Operation") != "DELETE")
            inserted_rows = silver_inserts.count()
            orphan_delete_rows = merge_batch.filter(col("Operation") == "DELETE").count()
            if orphan_delete_rows:
                # Giữ lại DELETE mồ côi trong Quarantine để không làm mất event nguồn.
                (
                    merge_batch.filter(col("Operation") == "DELETE")
                    .withColumn("error_codes", array(lit("ORPHAN_DELETE")))
                    .withColumn("rejection_reasons", col("error_codes"))
                    .withColumn("rejection_reason", lit("ORPHAN_DELETE"))
                    .withColumn("rejected_at", current_timestamp())
                    .write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(quarantine_path)
                )
                valid_count -= orphan_delete_rows
                batch_rejected_count += orphan_delete_rows
                _enforce_reject_rate(
                    raw_count,
                    batch_rejected_count,
                    config.max_reject_rate if config else None,
                )
                merge_batch = silver_inserts
            if inserted_rows == 0:
                raise ValueError(
                    "ORPHAN_DELETE: không thể tạo Silver current state từ DELETE mồ côi"
                )
            save_and_verify_delta(
                silver_inserts, SETTINGS.silver_delta, "silver.ecommerce_clean", mode="append"
            )
        superseded_count = valid_count - merge_batch.count()
        # Đồng bộ hai current-state table sau khi MERGE line events hoàn tất.
        full_merged_events = spark.read.format("delta").load(silver_path)
        silver_orders_current = build_silver_orders_current(full_merged_events)
        silver_order_lines_current = build_silver_order_lines_current(full_merged_events)
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
        registry.update_metrics(
            run_id,
            raw_rows=raw_count,
            exact_duplicate_rows=batch_duplicate_count,
            rejected_rows=batch_rejected_count,
            sequence_conflict_rows=sequence_conflict_count,
            valid_event_rows=valid_count,
            superseded_rows=superseded_count,
            inserted_rows=inserted_rows,
            updated_rows=updated_rows,
            unchanged_rows=unchanged_rows,
            stale_rows=stale_rows,
            deleted_rows=deleted_rows,
            orphan_delete_rows=orphan_delete_rows,
        )
        registry.mark_silver_merged(run_id)

        # 4. Refresh Gold core và marts từ current state Silver.
        LOGGER.info("--- 4. REFRESH GOLD CORE & MARTS TỪ SILVER (SCD2=%s) ---", effective_scd2)
        full_silver = spark.read.format("delta").load(silver_path)
        active_silver = build_silver_current_events(full_silver)
        dimensions = build_all_dimensions(
            spark,
            active_silver,
            use_scd2=effective_scd2,
            customer_history_df=_customer_history_source(spark, active_silver, effective_scd2),
        )
        fact_sales = build_fact_sales(active_silver, dimensions)
        fact_order_fulfillment = build_fact_order_fulfillment(
            silver_orders_current,
            silver_order_lines_current,
            dimensions,
        )
        persist_gold_staging(
            spark,
            {
                **dimensions,
                "fact_sales_line": fact_sales,
                "fact_order_fulfillment": fact_order_fulfillment,
            },
            run_id,
            "gold_star",
        )

        sales_enriched = build_sales_enriched(fact_sales, dimensions)
        persist_gold_staging(
            spark,
            {"gold_sales_enriched": sales_enriched},
            run_id,
            "gold_semantic",
        )

        gold_marts = build_certified_marts(sales_enriched, fact_order_fulfillment)
        persist_gold_staging(spark, gold_marts, run_id, "gold_mart")
        registry.update_status(run_id, "GOLD_BUILT", gold_run_id=run_id)

        # 5. Reconciliation Gate chạy sau khi mọi Gold output đã được ghi.
        recon_report = run_full_reconciliation(
            clean_df=active_silver,
            fact_sales=fact_sales,
            mart_overview=gold_marts["mart_executive_daily"],
            dim_customer=dimensions["dim_customer"],
            raw_count=raw_count,
            duplicate_count=batch_duplicate_count,
            invalid_count=batch_rejected_count,
            run_id=run_id,
            valid_count=valid_count,
            silver_orders_current=silver_orders_current,
            silver_order_lines_current=silver_order_lines_current,
            fact_order_fulfillment=fact_order_fulfillment,
        )

        if recon_report.get("overall_status") != "PASS":
            raise PipelineCertificationError(
                f"Incremental Pipeline không đạt chứng nhận Reconciliation Gate: {recon_report}"
            )

        registry.mark_reconciled(run_id)
        registry.mark_ready_to_publish(run_id)
        publish_gold_run(
            spark,
            {
                **dimensions,
                "fact_sales_line": fact_sales,
                "fact_order_fulfillment": fact_order_fulfillment,
                "gold_sales_enriched": sales_enriched,
                **gold_marts,
            },
            run_id,
        )
        try:
            registry.mark_published(run_id, gold_run_id=run_id, published_version="2")
        except Exception as finalization_error:
            registry.mark_control_finalization_pending(run_id, finalization_error)
            raise

        LOGGER.info("--- THÀNH CÔNG: INCREMENTAL PIPELINE HOÀN TẤT VÀ ĐƯỢC CHỨNG NHẬN ---")
        return PipelineRunResult(
            run_id=run_id,
            batch_id=bid,
            status="SUCCESS",
            bronze_rows=raw_count,
            silver_rows=active_silver.count(),
            quarantine_rows=batch_rejected_count,
            duplicate_rows=batch_duplicate_count,
            reconciliation_passed=True,
            reconciliation_report=recon_report,
            certified_gold_version=2,
            spark=spark,
        )
    except Exception as exc:
        # Bất kỳ lỗi nào ở Bronze/Silver/Gold/Reconciliation đều phải làm control plane
        # chuyển FAILED để lần retry sau được phân biệt với một run đang chạy dở.
        try:
            if run_started:
                current = registry.find_by_run_id(run_id)
                if current is None or current["status"] != "CONTROL_FINALIZATION_PENDING":
                    registry.mark_failed(
                        run_id,
                        exc,
                        error_code=(
                            "RECONCILIATION_FAILED"
                            if isinstance(exc, PipelineCertificationError)
                            else "PIPELINE_FAILED"
                        ),
                    )
        finally:
            if manifest_registered and not bronze_committed:
                manifest.mark_failed(
                    source_system="ecommerce_csv",
                    source_hash=shash,
                    run_id=run_id,
                    error_code=(
                        "FILE_SCHEMA_MISMATCH"
                        if "FILE_SCHEMA_MISMATCH" in str(exc)
                        else "PIPELINE_FAILED"
                    ),
                    error_message=str(exc),
                )
            # Chỉ đóng Spark do function tự tạo; fixture hoặc caller vẫn sở hữu Spark.
            if owns_spark:
                spark.stop()
        raise
