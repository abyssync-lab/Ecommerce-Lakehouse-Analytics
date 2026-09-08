# Operations, Chạy local và Failure Handling

Tài liệu này mô tả cách vận hành v1 ở local/CI. Airflow chưa phải dependency
của pipeline; khi thêm sẽ chỉ gọi cùng package entrypoint, không copy business
logic vào DAG.

## Dependencies

Yêu cầu:

- Python 3.10+;
- Java 17;
- PySpark 3.5.x;
- delta-spark 3.x;
- PyYAML, Pytest và Ruff.

~~~powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
~~~

Local storage mặc định:

~~~powershell
$env:ECOMMERCE_USE_LOCAL_STORAGE = "true"
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:PYSPARK_PYTHON = "python"
~~~

Storage path được resolve bởi Settings. Không hard-code local path trong các
module transformation.

## Entrypoints

### Bootstrap

Bootstrap là full initialization và chỉ chạy khi Bronze chưa tồn tại. Nếu Bronze
đã có dữ liệu, không chạy lại bootstrap với source khác; hãy chạy incremental hoặc
dọn local storage có chủ đích.

~~~powershell
python SourceCode/validate_input.py Data/EcommerceSalesDataset.csv
python SourceCode/SparkEcommerceAnalysis.py --input Data/EcommerceSalesDataset.csv
~~~

### Incremental

~~~powershell
python SourceCode/SparkEcommerceAnalysis.py --incremental --input Data/sample_batches/batch_001.csv --batch-id batch_20260907_0200
~~~

### Report

~~~powershell
python SourceCode/generate_portfolio_report.py --source published
~~~

published là source chuẩn. csv chỉ dành cho independent validation.

## Run lifecycle

Pipeline phải bao trọn execution trong try/except/finally:

~~~text
start_run
try:
    ingest -> validate -> Silver -> Gold -> reconcile -> publish
    mark_published
except:
    mark_failed
    raise
finally:
    release_resources
~~~

Lỗi ở ingestion, quarantine, registry, Gold hoặc reconciliation đều phải đi qua
mark_failed. Không dùng LOGGER.debug để nuốt lỗi ở control-plane write.

## Certified publication

Mỗi table Gold có Delta transaction riêng nên không thể giả định một transaction
toàn schema. Publication module:

1. ghi snapshot run vào staging;
2. ghi serving rows kèm Publication_Run_ID;
3. tạo stable views;
4. cuối cùng mới update ctl_publications current_run_id.

Nếu bước 1 hoặc 2 thất bại, pointer vẫn giữ run cũ. Nếu bước 4 thất bại sau khi
data đã commit, registry ghi CONTROL_FINALIZATION_PENDING để cần xử lý vận hành.

## Failure và retry

| Failure | Hành động |
|---|---|
| File schema mismatch | Fail file, không ghi Bronze |
| Row quality fail | Ghi quarantine, tính reject rate |
| Reject rate vượt ngưỡng | FAILED, không publish |
| Bronze write fail | FAILED, retry sau khi sửa storage |
| Silver merge fail | FAILED, không đổi publication |
| Gold reconciliation fail | FAILED, Power BI vẫn dùng run cũ |
| Registry finalization fail | CONTROL_FINALIZATION_PENDING, kiểm tra pointer |

Retry phải dùng content hash hiện tại. Không xóa Bronze để “làm lại” một cách
thủ công, vì điều đó phá audit và có thể làm mất bằng chứng ingestion.

## Monitoring tối thiểu

Theo dõi theo run_id:

- thời gian bắt đầu/kết thúc;
- raw, valid, rejected, duplicate;
- inserted, updated, unchanged, stale, deleted;
- reject rate;
- reconciliation status;
- current publication;
- error code/message.

Alert khi:

- reject rate vượt ngưỡng;
- không có publication mới sau lịch chạy;
- run ở FAILED hoặc CONTROL_FINALIZATION_PENDING;
- financial reconciliation khác 0;
- row count Gold giảm bất thường.

## CI

Workflow .github/workflows/quality.yml phải:

1. cài Python và Java 17;
2. cài requirements và Ruff;
3. chạy Ruff;
4. chạy unit và Spark integration tests;
5. validate input contract;
6. bootstrap certified Gold;
7. build report từ published Gold.

Không dùng pytest.skip để che việc thiếu PySpark/Delta trong integration job.

## Reset demo

Reset storage chỉ dành cho môi trường demo/test cô lập. Không xóa Bronze hoặc
control tables trong môi trường cần audit. Khi cần reset, ghi rõ target path và
đảm bảo không trỏ vào HDFS hoặc storage production.
