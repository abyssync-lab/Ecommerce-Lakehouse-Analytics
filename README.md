# GlobalCart Daily Order & Fulfillment Lakehouse

GlobalCart là một lakehouse batch hằng ngày cho dữ liệu đơn hàng thương mại điện tử.
Pipeline tiếp nhận các file thay đổi từ OMS, lưu toàn bộ lịch sử ở Bronze, duy trì
trạng thái hiện tại ở Silver, xây dựng mô hình phân tích ở Gold và chỉ công bố
phiên bản đã đối soát cho Power BI.

Trọng tâm của dự án là tính đúng đắn và khả năng vận hành:

- xử lý incremental theo nội dung file;
- idempotency và retry an toàn;
- không để event cũ ghi đè event mới;
- quarantine dữ liệu lỗi thay vì làm mất dòng;
- reconciliation giữa các tầng dữ liệu;
- certified publication để dữ liệu lỗi không lọt vào dashboard.

## 1. Phạm vi phiên bản hiện tại

### Có trong v1

- Một nguồn dữ liệu: OMS xuất CSV theo batch.
- PySpark và Delta Lake cho data plane.
- PyYAML cho data contract và business policy.
- Control plane riêng cho file, pipeline run và publication pointer.
- Bronze append lịch sử event.
- Silver current state cho order và order line.
- Soft delete cho event DELETE.
- Data quality và quarantine theo error code.
- Gold run-scoped staging và certified serving.
- Report đọc từ Gold đã publish.
- Unit test, Spark integration test và CI quality checks.

### Chưa thuộc v1

Kafka, AI/ML, MongoDB, nhiều nguồn dữ liệu, streaming và Airflow production chưa
phải là dependency của pipeline hiện tại. Airflow chỉ được thêm sau khi CLI và
end-to-end flow đã ổn định.

## 2. Bài toán nghiệp vụ

OMS không xuất lại toàn bộ lịch sử. Mỗi file chỉ chứa những order line được tạo
hoặc cập nhật kể từ lần xuất trước:

~~~
orders_2026-09-07_0100.csv
orders_2026-09-07_0200.csv
orders_2026-09-07_0300.csv
~~~

Ví dụ cùng một order line thay đổi theo thời gian:

| Thời điểm | Order | Line | Trạng thái |
|---|---|---|---|
| 08:00 | O100 | L1 | Processing |
| 09:00 | O100 | L1 | Shipped |
| 15:00 | O100 | L1 | Delivered |
| Hai ngày sau | O100 | L1 | Returned |

Kết quả mong muốn:

- Bronze giữ đủ bốn event để audit.
- Silver chỉ giữ trạng thái hiện tại là Returned.
- Gold tính KPI theo trạng thái hiện tại và vẫn truy ngược được về Bronze.

## 3. Kiến trúc logic

Control plane và data plane được tách riêng.

~~~
CONTROL PLANE
  ctl_ingestion_files  -> file hash, file status, retry
  ctl_pipeline_runs    -> lifecycle, metrics, errors
  ctl_publications     -> current certified run

DATA PLANE
  landing CSV
      |
      v
  file-level validation
      |
      v
  Bronze: raw change events, append history
      |                         \
      |                          -> quarantine + quality metrics
      v
  Silver: typed events -> current order/order-line state
      |
      v
  Gold staging: dimensions, facts, marts
      |
      v
  reconciliation
      |
      +-- FAIL: current publication không đổi
      |
      +-- PASS: cập nhật current_run_id
                         |
                         v
                  serving views -> Power BI
~~~

Delta chỉ atomic ở từng table. Vì vậy Gold không được ghi trực tiếp vào bảng
đang đọc bởi BI rồi mới kiểm tra. Mỗi run phải xây dựng staging riêng, chạy
reconciliation, sau đó mới chuyển publication pointer.

## 4. Luồng xử lý một batch

### 4.1. Tạo run và nhận diện file

Mỗi batch có:

~~~
run_id   = run_20260907_0200_a7f93c
batch_id = batch_20260907_0200
~~~

source_hash được tính bằng SHA-256 trên bytes thật của file, không phải trên
filename hoặc URI.

Khóa idempotency là:

~~~
source_system + source_hash
~~~

| Tình huống | Hành động |
|---|---|
| Hash chưa tồn tại | Xử lý batch |
| Hash đã SUCCESS hoặc PUBLISHED | SKIPPED |
| Hash từng FAILED | Cho phép retry |
| Batch ID cũ nhưng hash khác | Fail conflict |
| Filename khác nhưng hash giống | Skip |

### 4.2. File-level validation

Trước khi ghi Bronze, pipeline kiểm tra:

- file tồn tại và đọc được;
- file không rỗng;
- có header;
- header không có cột trùng;
- schema version được hỗ trợ;
- các cột bắt buộc tồn tại.

Lỗi cấu trúc file là lỗi cấp file, ví dụ FILE_SCHEMA_MISMATCH. Không chuyển
từng dòng vào quarantine nếu toàn bộ file không thể diễn giải đúng.

### 4.3. Ghi Bronze

Grain của Bronze là một dòng thay đổi cho một order line. Giá trị nguồn được giữ
dưới dạng raw string; việc cast và business validation thực hiện ở Silver.

Metadata chuẩn:

~~~
_run_id
_batch_id
_source_system
_source_uri
_source_file
_source_hash
_source_row_number
_ingested_at
_contract_version
_pipeline_version
_record_hash
~~~

_record_hash dùng để nhận diện exact duplicate. Bronze của incremental run phải
append. Chế độ overwrite chỉ dành cho bootstrap/reset của môi trường demo.

### 4.4. Validate và merge Silver

Silver thực hiện theo thứ tự:

1. Chuẩn hóa field và cast timestamp, date, integer, decimal.
2. Sinh danh sách error code cho từng dòng.
3. Tách valid_df và invalid_df bằng điều kiện null-safe.
4. Ghi toàn bộ dòng lỗi vào quarantine.
5. Nhận diện exact duplicate và sequence conflict.
6. Chọn event thắng trong batch theo timestamp.
7. Phân loại incoming event so với current state.
8. MERGE vào Silver current-state.

Với cùng (Order_ID, Order_Line_ID) trong một batch, event có
Source_Updated_At mới nhất thắng. Nếu timestamp giống nhau nhưng nội dung khác
nhau, cả nhóm bị quarantine với SEQUENCE_CONFLICT; pipeline không tự ý chọn một
dòng ngẫu nhiên.

### 4.5. Build Gold, đối soát và publish

Gold được ghi vào vùng run-scoped:

~~~
gold/_runs/<run_id>/...
~~~

Sau khi build xong, pipeline chạy reconciliation. Chỉ khi mọi check đạt PASS,
ctl_publications.current_run_id mới được đổi sang run mới. Các view ổn định ở
serving chỉ đọc dữ liệu thuộc current run.

Nếu run mới thất bại, Power BI vẫn nhìn thấy run certified trước đó.

## 5. Lifecycle của pipeline run

Lifecycle logic:

~~~
PROCESSING
    -> VALIDATED
    -> SILVER_MERGED
    -> GOLD_BUILT
    -> RECONCILED
    -> PUBLISHED
~~~

Lỗi tại bất kỳ bước nào đều chuyển run sang FAILED, lưu error_code và
error_message, rồi re-raise lỗi để scheduler biết task thất bại. Không được
nuốt lỗi ở registry write, quarantine write, quality metrics write hoặc publish.

Trong code, orchestration phải có dạng tương đương:

~~~~python
run = registry.start_run(...)

try:
    result = execute_pipeline(...)
    registry.mark_published(run.run_id, ...)
    return result
except Exception as exc:
    registry.mark_failed(run.run_id, exc)
    raise
finally:
    release_resources()
~~~~

## 6. Data contract

### Contract bootstrap

contracts/ecommerce_order.yaml phục vụ dataset lịch sử hiện có. Dataset cũ có
thể chưa có Order_Line_ID, Source_Updated_At hoặc Operation đầy đủ.

### Contract incremental v2

contracts/ecommerce_order_change_v2.yaml là contract chính cho event mới:

~~~~yaml
version: "2.0.0"
dataset: ecommerce_order_change
grain: one change event for one order line
merge_keys:
  - Order_ID
  - Order_Line_ID
sequence_column: Source_Updated_At
operation_column: Operation
~~~~

Các field quan trọng:

| Field | Ý nghĩa |
|---|---|
| Order_ID | ID đơn hàng |
| Order_Line_ID | ID ổn định của order line |
| Product_ID | ID sản phẩm |
| Source_Updated_At | Thời điểm cập nhật từ OMS |
| Operation | UPSERT hoặc DELETE |
| Order_Date | Ngày đặt hàng |
| Quantity | Số lượng |
| Unit_Price | Đơn giá |
| Discount | Tỷ lệ chiết khấu trong [0, 1] |
| Cost | Giá vốn |
| Order_Status | Trạng thái nghiệp vụ |

Không dùng các field mutable như price, quantity hoặc product name làm merge key.

Đối với historical bootstrap, pipeline có thể dùng:

~~~
Order_Line_ID = Order_ID + "-1"
Product_ID    = sha2(normalized(Product_Name))
Operation     = UPSERT
~~~

Đây chỉ là adapter cho dữ liệu cũ. Incremental mode phải nhận Order_Line_ID từ
upstream; không được tự sinh khóa từ price/quantity.

## 7. Silver data quality và duplicate accounting

### 7.1. Rule cấp dòng

| Rule | Error code | Hành động |
|---|---|---|
| Order_ID null | MISSING_ORDER_ID | Quarantine |
| Order_Line_ID null | MISSING_LINE_ID | Quarantine |
| Product_ID null với UPSERT | MISSING_PRODUCT_ID | Quarantine |
| Timestamp cast lỗi | INVALID_UPDATED_AT | Quarantine |
| Operation không hợp lệ | INVALID_OPERATION | Quarantine |
| Quantity <= 0 với UPSERT | INVALID_QUANTITY | Quarantine |
| Price < 0 | INVALID_UNIT_PRICE | Quarantine |
| Discount ngoài [0, 1] | INVALID_DISCOUNT | Quarantine |
| Cost < 0 | INVALID_COST | Quarantine |
| Order date sau updated time | INVALID_EVENT_TIME | Quarantine |
| Cùng key/timestamp, khác nội dung | SEQUENCE_CONFLICT | Quarantine |

DELETE chỉ bắt buộc Order_ID, Order_Line_ID, Source_Updated_At và Operation.
Các field về giá, quantity hoặc product không bắt buộc cho delete.

### 7.2. Null-safe accounting

Không được tách valid/invalid bằng filter(condition) và filter(~condition)
nếu condition có thể là NULL. Dòng null có thể biến mất khỏi cả hai nhánh.

Thay vào đó, pipeline tạo error_codes rồi lọc các phần tử null:

~~~~python
error_codes = expr("filter(error_codes, x -> x is not null)")
valid_df = evaluated.filter(size("error_codes") == 0)
invalid_df = evaluated.filter(size("error_codes") > 0)
~~~~

Accounting của batch phải minh bạch:

~~~
raw_rows
  = exact_duplicate_rows
  + rejected_rows
  + valid_event_rows
~~~

Nếu trong cùng batch có nhiều event hợp lệ cho một line:

~~~
valid_event_rows
  = winning_events
  + superseded_rows
~~~

Không dùng công thức này để so sánh toàn bộ Bronze history với current-state
Silver.

### 7.3. Ba loại duplicate/event

- **Exact duplicate:** cùng _record_hash; giữ một event và đếm phần còn lại.
- **Sequence conflict:** cùng key và timestamp nhưng nội dung khác; quarantine cả
  nhóm.
- **Multiple versions:** cùng line nhưng timestamp khác; không phải duplicate,
  giữ toàn bộ ở Bronze và chọn event mới nhất để merge current state.

## 8. Silver current-state model

### silver_orders_current

Grain: một dòng cho mỗi Order_ID.

Các field chính:

~~~
Order_ID, Order_Date, Customer_ID, Customer_Segment,
Order_Status, Payment_Method, Shipping_Method,
Shipping_Cost, Shipping_Days, Region, Country,
Source_Updated_At, Is_Deleted, Record_Hash,
Last_Run_ID, Last_Source_Hash, Created_At, Updated_At
~~~

Field cấp order không được cộng lặp theo số line.

### silver_order_lines_current

Grain: một dòng cho mỗi (Order_ID, Order_Line_ID).

Các field tính toán:

~~~
Gross_Amount      = Quantity * Unit_Price
Discount_Amount   = Gross_Amount * Discount_Rate
Net_Line_Amount   = Gross_Amount - Discount_Amount
Gross_Profit      = Net_Line_Amount - Cost_Amount
~~~

Nếu nguồn có Revenue hoặc Profit, lưu thành Source_Revenue và Source_Profit để
đối chiếu với các field certified được tính lại.

DELETE được áp dụng dưới dạng soft delete bằng Is_Deleted = true. Gold chỉ lấy
active rows. Delete không tìm thấy current row được đếm là ORPHAN_DELETE.

### Phân loại trước khi MERGE

| Điều kiện | Metric |
|---|---|
| Target không tồn tại, UPSERT | inserted_rows |
| Target tồn tại, incoming mới hơn | updated_rows |
| Hash giống target | unchanged_rows |
| Incoming cũ hơn target | stale_rows |
| Incoming mới hơn, DELETE | deleted_rows |
| DELETE không có target | orphan_delete_rows |

MERGE luôn join bằng stable key (Order_ID, Order_Line_ID) và chỉ update khi
Source_Updated_At mới hơn.

## 9. Gold semantic model

Mô hình Gold hướng đến constellation gồm hai fact:

### Dimensions

- dim_date
- dim_product
- dim_customer — SCD Type 2 cho các thuộc tính cần lịch sử
- dim_geography
- dim_order_context — gộp status, payment method và shipping method

### fact_sales_line

Grain: một active order line hiện hành.

Các measure chính:

~~~
Quantity
Gross_Amount
Discount_Amount
Net_Line_Amount
Cost_Amount
Gross_Profit
Publication_Run_ID
~~~

Sales key phải ổn định theo (Order_ID, Order_Line_ID), không dùng row_number()
trên toàn bảng để cấp lại key sau mỗi lần chạy.

### fact_order_fulfillment

Grain: một active order.

Các measure chính:

~~~
Line_Count
Total_Quantity
Order_Value
Shipping_Cost
Shipping_Days
SLA_Days
Is_SLA_Breached
Is_Delivered
Is_Returned
Is_Cancelled
Publication_Run_ID
~~~

Order_Value phải tổng hợp từ Silver order lines, không lấy một field order-level
bị lặp trên từng line.

### SCD Type 2

Customer dimension chỉ theo dõi các thuộc tính cần lịch sử, ví dụ
Customer_Segment và Customer_Gender. Attribute_Hash xác định version:

~~~
Order timestamp >= Valid_From
AND Order timestamp < Valid_To
~~~

Khi attribute hash đổi, đóng version hiện tại và mở version mới. Late-arriving
event nằm giữa hai version phải rebuild lịch sử của customer đó, không rebuild
toàn bộ dimension.

## 10. Business metrics

Chính sách KPI đặt trong contracts/business_metrics.yaml, không rải cứng trong
nhiều mart.

Các policy cần thống nhất:

- recognized revenue: status Delivered;
- return rate: Returned / (Delivered + Returned);
- RFM: chỉ dùng order Delivered;
- profit margin: SUM(Gross_Profit) / SUM(Net_Line_Amount);
- AOV: delivered revenue / delivered orders.

Không tính average của từng phần trăm line rồi mới average tiếp ở cấp report.

Code hiện tại vẫn giữ các mart cũ để tương thích trong giai đoạn refactor. Định
hướng semantic model cuối cùng là sáu mart có ý nghĩa nghiệp vụ:

1. mart_executive_daily
2. mart_product_performance
3. mart_geography_performance
4. mart_fulfillment_sla
5. mart_customer_rfm
6. mart_product_abc

Không coi việc tạo nhiều bảng groupBy đơn giản là hoàn thành data mart.

## 11. Reconciliation và certified publication

Reconciliation được chia thành các nhóm độc lập:

### Batch accounting

Kiểm tra raw, duplicate, rejected và valid của đúng batch đang chạy. Với
incremental run, không lấy tổng toàn bộ Bronze history để so sánh với current
state.

### Silver current state

- Không trùng Order_ID.
- Không trùng (Order_ID, Order_Line_ID).
- Mọi active line có order header.
- Không có active line thuộc deleted order.
- Incoming stale không làm giảm Source_Updated_At hiện tại.

### Fact và tài chính

- Số active Silver lines khớp fact_sales_line.
- Số active Silver orders khớp fact_order_fulfillment.
- Tổng Silver.Net_Line_Amount bằng tổng FactSales.Net_Line_Amount.
- Delivered revenue giữa Silver, fact và executive mart khớp theo cùng policy.

### Dimension integrity

- Foreign key không null hoặc trỏ về unknown key 0.
- Mỗi natural key Type 1 chỉ có một row.
- Mỗi customer chỉ có một Is_Current = 1.
- Các khoảng SCD2 không chồng lấn.
- Temporal join của fact match đúng một customer version.

### Publication

- Tất cả Gold tables có cùng Publication_Run_ID.
- Đủ các table bắt buộc.
- Không có row count bất thường bằng 0.
- current_run_id chỉ đổi sau khi mọi check PASS.

## 12. Cấu trúc repository

Repository hiện vẫn dùng package dưới SourceCode/lakehouse trong lúc refactor;
chưa chuyển toàn bộ sang src/globalcart.

~~~
GlobalEcommerceBigData/
├── SourceCode/
│   ├── lakehouse/
│   │   ├── pipeline.py           # orchestration
│   │   ├── ingestion.py          # file read, hash, Bronze metadata
│   │   ├── registry.py            # run lifecycle và metrics
│   │   ├── publication.py         # staging, serving, pointer
│   │   ├── silver.py              # quality, dedup, current state
│   │   ├── dimensions.py          # dimensions và SCD2
│   │   ├── marts.py               # semantic marts hiện tại
│   │   ├── reconciliation.py      # data quality checks
│   │   └── contracts/             # contract loader/evaluator
│   ├── SparkEcommerceAnalysis.py  # CLI entry point hiện tại
│   ├── project_cli.py             # project utilities
│   └── generate_portfolio_report.py
├── contracts/
│   ├── ecommerce_order.yaml
│   ├── ecommerce_order_change_v2.yaml
│   ├── business_metrics.yaml
│   └── shipping_sla.yaml
├── Data/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── .github/workflows/quality.yml
├── requirements.txt
└── README.md
~~~

## 13. Cài đặt môi trường

Yêu cầu đề xuất:

- Python 3.10 hoặc 3.11;
- Java 17 cho Spark;
- PySpark và Delta Lake tương thích;
- PyYAML;
- Pytest và Ruff.

Cài dependencies:

~~~~powershell
python -m pip install -r requirements.txt
~~~~

Biến môi trường quan trọng:

~~~powershell
$env:ECOMMERCE_USE_LOCAL_STORAGE = "true"
$env:ECOMMERCE_MAX_REJECT_RATE = "0.05"
~~~

Mặc định, batch có hơn 5% dòng bị reject sẽ FAILED và không được publish.

Kiểm tra nhanh:

~~~~powershell
python -m compileall SourceCode
ruff check SourceCode tests
~~~~

## 14. Chạy pipeline local

### Bootstrap dữ liệu lịch sử

~~~~powershell
python SourceCode/SparkEcommerceAnalysis.py --input Data/EcommerceSalesDataset.csv
~~~~

Bootstrap dùng adapter cho dataset lịch sử, tạo Silver/Gold ban đầu và publish
run đầu tiên.

### Chạy incremental batch

~~~~powershell
python SourceCode/SparkEcommerceAnalysis.py --incremental --input Data/sample_batches/batch_001.csv --batch-id batch_20260907_0200
~~~~

Incremental input phải theo contract v2 và phải có Order_Line_ID từ upstream.

### Sinh report từ certified Gold

~~~~powershell
python SourceCode/generate_portfolio_report.py --source published
~~~~

--source published là đường chạy chuẩn. Đọc CSV trực tiếp chỉ được dùng cho
independent validation hoặc so sánh legacy:

~~~~powershell
python SourceCode/generate_portfolio_report.py --source csv
~~~~

Report không được dùng CSV raw để giả lập kết quả lakehouse trong demo chính.

## 15. Chạy test và CI

### Local

~~~~powershell
python -m pytest tests/unit -v
python -m pytest tests/integration -v --basetemp=./scratch/pytest_temp
python -m pytest tests/e2e -v --basetemp=./scratch/pytest_temp
~~~~

### CI

CI có hai nhóm kiểm tra:

1. Fast checks: Ruff, YAML validation và unit tests.
2. Spark integration: Java, PySpark, Delta, bootstrap nhỏ, incremental update,
   replay và report từ published Gold.

Integration test không được pytest.skip chỉ vì thiếu PySpark. Nếu dependency
không cài được, CI phải fail để phản ánh môi trường không hợp lệ.

## 16. Demo bắt buộc

Nên chuẩn bị các batch nhỏ để chứng minh từng invariant:

~~~
000_bootstrap.csv
001_new_orders.csv
002_status_updates.csv
003_returns_and_cancellations.csv
004_invalid_records.csv
005_late_and_duplicate_events.csv
~~~

Kịch bản cần thể hiện:

1. Bootstrap tạo current state và publication đầu tiên.
2. Replay cùng file trả SKIPPED, không tăng Bronze/Silver và không đổi
   publication.
3. Update cùng line với timestamp mới làm updated_rows = 1.
4. Event cũ làm tăng stale_rows, không thay đổi Silver.
5. Record lỗi đi vào quarantine, không biến mất.
6. Cố tình làm reconciliation fail; current publication vẫn trỏ về run trước.

## 17. Mục tiêu vận hành

Đây là các mục tiêu cần benchmark và chứng minh bằng test/monitoring, không phải
con số được mặc định coi là đã đạt:

- lịch chạy lúc 02:00 mỗi ngày;
- xử lý batch 1 triệu dòng dưới 15 phút;
- không nạp lại cùng một file;
- không để event cũ ghi đè event mới;
- quarantine đầy đủ dòng lỗi;
- tỷ lệ quarantine trên 5% làm batch thất bại;
- sai lệch doanh thu Silver/Gold bằng 0;
- retry sau sự cố không tạo duplicate;
- Power BI không nhìn thấy Gold chưa reconciliation.

## 18. Thứ tự refactor tiếp theo

Các thay đổi nên tách thành PR nhỏ, mỗi PR có test và invariant riêng:

1. Làm CI chạy được Spark/Delta thật.
2. Hoàn thiện data contract v2 và contract-driven quarantine.
3. Tách rõ order header và order line trong Silver.
4. Bổ sung reject-threshold enforcement và quality metrics đầy đủ.
5. Hoàn thiện persistent dimension key và incremental SCD2.
6. Tách fact building khỏi mart building.
7. Chuyển semantic model sang sáu mart nghiệp vụ.
8. Bổ sung test publication failure và serving pointer.
9. Chỉ sau đó mới thêm Airflow DAG gọi cùng CLI entry point.
10. Cập nhật Power BI và portfolio report chỉ đọc certified Gold.

## 19. Thông điệp chính của dự án

Đây không phải dự án chỉ đọc CSV rồi groupBy để tạo biểu đồ. Giá trị của
GlobalCart nằm ở việc chứng minh một data pipeline thực tế có thể:

- biết file nào đã xử lý;
- retry mà không nhân bản dữ liệu;
- giữ lịch sử và current state cùng lúc;
- phân biệt duplicate, conflict và late event;
- đối soát trước khi publish;
- giữ dashboard ở phiên bản tốt cuối cùng khi run mới thất bại.
