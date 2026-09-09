# Ecommerce Lakehouse Analytics — GlobalCart Order & Fulfillment Platform

End-to-end batch lakehouse xử lý incremental order events từ OMS bằng PySpark + Delta Lake.
Pipeline đảm bảo idempotency, event ordering, quarantine, reconciliation và atomic publication trước khi dữ liệu được Power BI sử dụng.

[![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2--3.3-00ADD8.svg)](https://delta.io/)
[![Pandas](https://img.shields.io/badge/Pandas-2.x-150458.svg)](https://pandas.pydata.org/)
[![PyYAML](https://img.shields.io/badge/PyYAML-6.x-cc0000.svg)](https://pyyaml.org/)
[![Ruff](https://img.shields.io/badge/lint-Ruff-261230.svg)](https://docs.astral.sh/ruff/)
[![Pytest](https://img.shields.io/badge/test-Pytest-0A9EDC.svg)](https://docs.pytest.org/)
[![CI](https://github.com/haminhthong/Ecommerce-Lakehouse-Analytics/actions/workflows/quality.yml/badge.svg)](https://github.com/haminhthong/Ecommerce-Lakehouse-Analytics/actions/workflows/quality.yml)

## Project Snapshot

| Thành phần | Nội dung |
|---|---|
| Business problem | Theo dõi order và fulfillment từ OMS, có lịch sử thay đổi và trạng thái hiện tại |
| Source | Incremental CSV batch từ một OMS |
| Processing | PySpark |
| Storage | Delta Lake trên local filesystem hoặc HDFS |
| Architecture | Medallion: Bronze → Silver → Gold |
| Modeling | Kimball dimensions, facts và business marts |
| Quality | Contract validation, quarantine, duplicate accounting, reconciliation |
| Serving | Certified Gold snapshot qua stable views cho Power BI |
| Status | End-to-end batch pipeline cho portfolio và kiểm thử vận hành |

## Bài toán và phạm vi ứng dụng (Problem & Scope)

GlobalCart nhận các file thay đổi đơn hàng theo batch, không phải toàn bộ lịch sử:

| Thời điểm | Order | Line | Thay đổi |
|---|---|---|---|
| 08:00 | O100 | L1 | Tạo mới, Processing |
| 09:00 | O100 | L1 | Chuyển thành Shipped |
| 15:00 | O100 | L1 | Chuyển thành Delivered |
| Hai ngày sau | O100 | L1 | Returned |

Bronze giữ đủ bốn event để audit và replay. Silver giữ trạng thái hiện tại của
order và order line. Gold tạo fact ở grain line và grain order để tránh cộng lặp
chi phí cấp order. Power BI chỉ đọc publication đã qua reconciliation.

V1 cố ý chỉ dùng một nguồn OMS CSV và batch processing. Kafka, streaming, AI/ML,
MongoDB và nhiều nguồn dữ liệu chưa nằm trong phạm vi. Phạm vi này giúp dự án tập
trung vào correctness, incremental processing, idempotency, data quality và retry.

## Quy trình kỹ thuật duy nhất

Sơ đồ này là source of truth cho mã nguồn, cấu hình và báo cáo. Control plane quản
lý trạng thái; data plane xử lý dữ liệu. Hai phần không dùng chung một bảng để
đoán trạng thái của nhau.

~~~mermaid
flowchart TD
    CONTROL["CONTROL PLANE<br/>File Registry | Run Registry | Publication"]
    LANDING["OMS CSV<br/>Landing"]
    BRONZE["BRONZE<br/>Raw order-change events"]
    QUALITY["DATA QUALITY<br/>Contract + row validation"]
    QUARANTINE["QUARANTINE<br/>Rejected rows + error codes"]
    SILVER["SILVER<br/>Current order + order-line state"]
    GOLD["GOLD STAGING<br/>Dimensions + facts + marts"]
    RECON["RECONCILIATION<br/>PASS / FAIL"]
    POINTER["ctl_publications.current_run_id<br/>Publication pointer"]
    CERTIFIED["CERTIFIED GOLD<br/>Stable serving views"]
    PBI["POWER BI"]

    CONTROL --> LANDING
    LANDING --> BRONZE
    BRONZE --> QUALITY
    QUALITY --> QUARANTINE
    QUALITY --> SILVER
    SILVER --> GOLD
    GOLD --> RECON
    RECON -->|PASS| POINTER
    RECON -->|FAIL - giữ run trước| POINTER
    POINTER --> CERTIFIED
    CERTIFIED --> PBI
~~~

## Luồng xử lý một batch

Một batch đi qua cùng một lifecycle từ CLI, test và CI:

1. **Discover và hash file**: tính SHA-256 từ bytes của file, không dùng filename hay URI.
2. **Register run**: tạo run_id, kiểm tra source_system + source_hash để xử lý replay/retry.
3. **Validate cấp file**: kiểm tra tồn tại, encoding, header, contract version, cột bắt buộc và file rỗng.
4. **Append Bronze**: lưu event nguồn cùng metadata; incremental flow không overwrite Bronze.
5. **Validate cấp dòng**: cast dữ liệu, tạo error_codes, tách valid và quarantine mà không làm mất dòng null.
6. **Merge Silver**: chọn event mới nhất theo order line, loại exact duplicate, quarantine sequence conflict và bỏ stale event.
7. **Build Gold staging**: tạo dimensions, facts và marts trong path riêng của run.
8. **Reconcile và publish**: chỉ đổi publication pointer khi mọi invariant đạt.

Chi tiết contract, idempotency, merge và publication nằm trong [docs](docs/).
README giữ logic cần hiểu khi review; implementation detail được tách theo concern.

## Mô hình dữ liệu

### Bronze

`bronze.ecommerce_raw` có grain một change event cho một order line. Giá trị
nguồn được giữ nguyên dạng raw; metadata gồm run, batch, source hash, row number,
record hash, contract version và pipeline version.

### Silver

| Bảng | Grain | Vai trò |
|---|---|---|
| silver.ecommerce_clean | Một current event cho mỗi order line | Bảng Delta backing cho MERGE; không phải nguồn audit lịch sử |
| silver.silver_orders_current_delta | Một Order_ID | Current order header |
| silver.silver_order_lines_current_delta | Một (Order_ID, Order_Line_ID) | Current order line, có soft delete |
| quarantine.rejected_rows | Một dòng bị loại | Dữ liệu lỗi và error_codes để điều tra |

Measure được chứng nhận ở Silver là Gross_Amount, Discount_Amount,
Net_Line_Amount, Cost_Amount và Gross_Profit. Revenue hoặc Profit từ nguồn, nếu có,
được đổi tên thành Source_Revenue và Source_Profit để không nhầm với measure tính lại.

### Gold

**Dimensions**

- dim_date
- dim_product
- dim_customer, hỗ trợ SCD Type 2 khi bật ECOMMERCE_USE_SCD2=true
- dim_location, chứa Region và Country
- dim_payment, dim_shipping và dim_order_status cho các khóa nghiệp vụ tương thích

**Facts**

- fact_sales_line: một row cho order line hiện hành; SalesKey ổn định theo business key.
- fact_order_fulfillment: một row cho order; shipping cost chỉ xuất hiện một lần ở order grain.

**Certified marts**

- mart_executive_daily
- mart_product_performance
- mart_geography_performance
- mart_fulfillment_sla
- mart_customer_rfm
- mart_product_abc

Policy cho revenue, return, RFM và ABC được khai báo trong
[contracts/business_metrics.yaml](contracts/business_metrics.yaml), không hard-code
rải rác trong report.

`build_all_marts` vẫn tồn tại như compatibility helper cho test và consumer cũ;
publication production dùng `build_certified_marts` để bảo đảm sáu mart trên cùng
policy và cùng `Publication_Run_ID`.

## Data Quality Results

Các invariant dưới đây là tiêu chí publish. Test tương ứng nằm trong tests/ và
được chạy trong CI khi Spark/Delta đã được cài.

| Check | Expected result |
|---|---|
| Duplicate ingestion | 0 duplicated accepted events sau replay cùng content hash |
| Orphan row loss | 0; dòng lỗi phải vào quarantine hoặc được ghi nhận là duplicate |
| Raw accounting | raw = valid + rejected + exact_duplicate |
| Failed Gold run published | 0; run lỗi không đổi current_run_id |
| Old event ghi đè event mới | 0; stale event chỉ tăng metric stale_rows |
| Silver/Fact financial reconciliation | SUM(Net_Line_Amount) bằng nhau theo cùng policy |
| Current-state grain | Không trùng order và không trùng (Order_ID, Order_Line_ID) |
| SCD2 integrity | Tối đa một customer version hiện hành, không có khoảng chồng lấn |

Ngưỡng reject mặc định là 5%. Có thể cấu hình bằng ECOMMERCE_MAX_REJECT_RATE;
vượt ngưỡng sẽ làm batch thất bại trước publication.

## CI/CD và cách phát hành dữ liệu

CI là GitHub Actions trong
[.github/workflows/quality.yml](.github/workflows/quality.yml), gồm hai job nối tiếp.
`fast-checks` chạy Ruff, format check và parse contract. `spark-integration` cài Java
17, PySpark 3.5.x và Delta Lake 3.2–3.3, rồi chạy toàn bộ unit/integration/e2e tests,
kiểm tra input, bootstrap Gold và dựng report từ publication. Integration test không
được skip khi thiếu Spark; thiếu dependency phải làm job đỏ.

CD của v1 là certified data publication, không phải deploy ứng dụng riêng. Mỗi
pipeline run ghi Gold vào `gold/_runs/<run_id>`, reconciliation kiểm tra snapshot,
rồi mới cập nhật `ctl_publications.current_run_id`. Vì vậy đây là chuỗi phát hành:

~~~text
code change -> CI xanh -> pipeline run -> Gold staging -> reconciliation PASS
            -> publication pointer -> serving views -> Power BI
~~~

Nếu reconciliation hoặc bước ghi Gold thất bại, run được đánh dấu `FAILED` và
publication pointer vẫn trỏ tới run tốt trước đó. CLI là entrypoint chuẩn dùng cho
local, test và CI; Airflow chỉ được thêm sau khi entrypoint này ổn định và DAG sẽ
chỉ gọi job, không chứa business logic.

## Certified publication cho Power BI

Delta Lake atomic theo từng table, không atomic cho toàn bộ Gold schema. Pipeline:

1. Ghi dimensions, facts và marts vào `gold/_runs/<run_id>`.
2. Chạy reconciliation trên toàn bộ snapshot staging.
3. Chỉ khi PASS mới cập nhật `ctl_publications.current_run_id`.
4. Stable views ở serving đọc theo pointer; không copy một phần Gold mới vào
   serving trước khi kiểm tra hoàn tất.

Các view trong database serving chỉ trả row có Publication_Run_ID bằng
current_run_id. Nếu run mới lỗi, run cũ vẫn là bản Power BI nhìn thấy.

Report artifact hiện có tại [GlobalCart_Analytics.pbix](<powerbi/GlobalCart_Analytics.pbix>). Code sinh báo cáo mặc
định đọc `serving.gold_sales_enriched` và `serving.fact_order_fulfillment` từ
publication hiện hành. Tùy chọn `--source csv` chỉ dành cho validation độc lập,
không phải nguồn dashboard certified. Repo hiện chưa có screenshot dashboard được
commit nên không nhúng ảnh giả.

## Cấu trúc thư mục dự án (Project Structure)

~~~text
Ecommerce-Lakehouse-Analytics/
├── SourceCode/
│   ├── lakehouse/
│   │   ├── ingestion.py          # đọc CSV, hash và append Bronze
│   │   ├── file_manifest.py      # registry theo source hash
│   │   ├── registry.py           # lifecycle của pipeline run
│   │   ├── silver.py             # quality, current-state và merge helpers
│   │   ├── dimensions.py         # dimensions và SCD2
│   │   ├── marts.py              # facts, semantic base và marts
│   │   ├── reconciliation.py     # batch/current-state/financial checks
│   │   ├── publication.py        # staging, pointer và stable views
│   │   └── storage.py            # local/HDFS path và Delta write helpers
│   ├── SparkEcommerceAnalysis.py # entrypoint bootstrap/incremental
│   ├── generate_portfolio_report.py
│   ├── project_cli.py             # nhóm lệnh tiện ích local
│   └── validate_input.py
├── contracts/                     # data contract và business policy
├── Data/                          # seed và sample batches
├── docs/                          # thiết kế chi tiết theo từng concern
├── scripts/                       # validate contract và benchmark
├── tests/                         # unit, integration và invariant tests
├── .github/workflows/quality.yml  # CI có Spark + Delta thật
├── powerbi/                        # Power BI artifact và screenshot
│   └── GlobalCart_Analytics.pbix
├── requirements.txt
└── pyproject.toml
~~~

Một số script legacy như MongoDB connector và Thrift server vẫn còn để giữ tương
thích với repository cũ, nhưng không được gọi bởi v1 batch pipeline.

## Hướng dẫn cài đặt (Installation)

Yêu cầu:

- Python 3.10 trở lên
- Java 17 cho Spark local
- PySpark 3.5.x và Delta Lake 3.2–3.3

~~~powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
~~~

Local mode mặc định ghi vào Output/lakehouse:

~~~powershell
$env:ECOMMERCE_USE_LOCAL_STORAGE = "true"
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:PYSPARK_PYTHON = "python"
~~~

Để chạy HDFS, đặt ECOMMERCE_USE_LOCAL_STORAGE=false và cấu hình ECOMMERCE_HDFS_BASE.

## Hướng dẫn chạy thử nghiệm (Running the pipeline)

### Bootstrap

~~~powershell
python SourceCode/validate_input.py Data/EcommerceSalesDataset.csv
python SourceCode/SparkEcommerceAnalysis.py --input Data/EcommerceSalesDataset.csv
~~~

Bootstrap tạo Bronze history, Silver current state, Gold staging và publication đầu tiên.
Bootstrap chỉ hợp lệ khi Bronze đang rỗng; nếu lakehouse đã có dữ liệu, dùng
incremental hoặc dọn local storage có chủ đích trước khi bootstrap lại để tránh
ghi đè current state.

### Incremental batch

~~~powershell
python SourceCode/SparkEcommerceAnalysis.py `
  --incremental `
  --input Data/sample_batches/batch_001.csv `
  --batch-id batch_20260907_0200
~~~

Incremental bắt buộc dùng Order_Line_ID ổn định từ upstream. Dataset historical
bootstrap có thể dùng adapter sinh ID, nhưng không được suy diễn ID từ price hoặc
quantity cho batch thực tế.

### Replay và retry

Chạy lại cùng file để kiểm tra idempotency:

~~~powershell
python SourceCode/SparkEcommerceAnalysis.py `
  --incremental `
  --input Data/sample_batches/batch_001.csv `
  --batch-id batch_20260907_0200
~~~

Nếu content hash đã được publish, run được đánh dấu SKIPPED, Bronze không append
thêm và publication pointer không đổi. Nếu run trước đó FAILED, pipeline có thể
retry với cùng source hash.

### Sinh báo cáo

~~~powershell
python SourceCode/generate_portfolio_report.py --source published
~~~

Mặc định report đọc `serving.gold_sales_enriched` và ghép `serving.fact_order_fulfillment`
đã publish, nhờ đó vừa dùng certified line measures vừa giữ đúng order-level
shipping cost. Dùng `--source csv` chỉ khi cần validation độc lập với lakehouse.

### Nhóm lệnh local

~~~powershell
python SourceCode/project_cli.py --help
python SourceCode/project_cli.py check
python SourceCode/project_cli.py reconcile
~~~

Các lệnh demo Delta hoặc connector legacy không thuộc data path chính của v1.

## Kiểm tra chất lượng code

~~~powershell
python -m ruff check SourceCode tests scripts
python scripts/validate_contracts.py
python -m compileall -q SourceCode
python -m pytest -q
~~~

CI trong .github/workflows/quality.yml cài Java 17, PySpark, Delta Lake và Ruff,
sau đó chạy lint, format check, parse contract, toàn bộ test, validate input,
bootstrap certified Gold và report validation. Integration test không được skip khi
thiếu PySpark trong CI.

## Tài liệu thiết kế

| Tài liệu | Nội dung |
|---|---|
| [DATA_CONTRACT.md](docs/DATA_CONTRACT.md) | Contract v2, bootstrap adapter, file-level và row-level rules |
| [DATA_QUALITY.md](docs/DATA_QUALITY.md) | Error codes, quarantine, duplicate accounting và reconciliation |
| [INCREMENTAL_PROCESSING.md](docs/INCREMENTAL_PROCESSING.md) | Hash idempotency, registry lifecycle, retry và stale events |
| [SILVER_MERGE.md](docs/SILVER_MERGE.md) | Event winner, Delta MERGE, current-state và soft delete |
| [GOLD_MODEL.md](docs/GOLD_MODEL.md) | Dimensions, facts, SCD2, KPI policy và run-scoped staging |
| [OPERATIONS.md](docs/OPERATIONS.md) | Cài đặt, chạy batch, failure handling, monitoring và CI |

Chỉ giữ các tài liệu thiết kế cần cho pipeline và benchmark có thể tái sinh.
`BUSINESS_INSIGHTS.md` là output runtime của report, không commit bản snapshot cũ
để tránh nhầm báo cáo CSV với certified Gold.

## Trạng thái kiểm chứng hiện tại

Đã kiểm tra trong môi trường phát triển:

- Ruff check cho SourceCode, tests, scripts: đạt.
- Ruff format check cho các module lakehouse đã chỉnh: đạt.
- Compile toàn bộ SourceCode: đạt.
- YAML contracts: parse thành công.
- Nhóm unit/contract/CLI/portfolio tests không cần Spark: đạt.

Full Spark/Delta integration cần runtime có Java, PySpark và delta-spark. CI đã cấu
hình đúng dependency; nếu môi trường chưa cài Spark thì đó là lỗi môi trường, không
được biến thành pytest.skip trong integration job.

## Định hướng sau v1

Chỉ mở rộng sau khi các invariant của v1 ổn định:

1. Chuẩn hóa package src/globalcart nhưng giữ entrypoint tương thích.
2. Thêm Airflow DAG mỏng chỉ gọi package entrypoint, không chứa business logic.
3. Bổ sung monitoring/alerting dựa trên control plane.
4. Khi có yêu cầu nghiệp vụ thật mới đánh giá thêm nguồn dữ liệu hoặc streaming.

## Thông điệp chính của dự án

Đây không phải dự án chỉ đọc CSV rồi groupBy để tạo biểu đồ. Giá trị của
GlobalCart nằm ở việc chứng minh một data pipeline thực tế có thể:

- biết file nào đã xử lý;
- retry mà không nhân bản dữ liệu;
- giữ lịch sử và current state cùng lúc;
- phân biệt duplicate, conflict và late event;
- đối soát trước khi publish;
- giữ dashboard ở phiên bản tốt cuối cùng khi run mới thất bại.
