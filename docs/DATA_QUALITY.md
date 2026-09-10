# Data Quality, Quarantine và Reconciliation

Data quality của GlobalCart có hai vai trò: bảo vệ Silver khỏi dữ liệu không thể
tin cậy và chứng minh Gold đủ điều kiện để publish. Dữ liệu lỗi không được biến
mất chỉ vì một điều kiện Spark trả về NULL.

## Null-safe row accounting

Pipeline tạo một array error_codes, lọc các phần tử null rồi phân loại:

~~~text
valid rows   = size(error_codes) = 0
invalid rows = size(error_codes) > 0
~~~

Không dùng cặp filter phủ định đơn giản vì điều kiện NULL có thể làm một dòng
không xuất hiện ở cả valid lẫn invalid.

Mỗi run phải có accounting:

~~~text
raw_rows = valid_event_rows
          + rejected_rows
          + exact_duplicate_rows
          + file_level_excluded_rows
~~~

Trong file hợp lệ, file_level_excluded_rows bằng 0. Superseded event không được
trừ khỏi raw accounting; event đó vẫn hợp lệ và tồn tại trong Bronze.

## Quarantine

Quarantine lưu:

- raw business columns;
- error_codes;
- rejection_reasons;
- run_id, batch_id, source_hash;
- source_row_number;
- ingested_at;
- contract_version và pipeline_version.

Quarantine write là bước bắt buộc. Nếu ghi quarantine thất bại, run phải FAILED
thay vì nuốt exception và tiếp tục publish.

## Phân loại duplicate

### Exact duplicate

Hai dòng có cùng toàn bộ payload và metadata nghiệp vụ có cùng record_hash. Giữ
một dòng để xử lý; các bản sao còn lại tăng exact_duplicate_rows.

### Sequence conflict

Cùng Order_ID, Order_Line_ID và Source_Updated_At nhưng payload khác nhau. Đây
là lỗi nguồn, không chọn tùy ý một dòng. Cả nhóm vào quarantine với
SEQUENCE_CONFLICT.

### Superseded event

Cùng order line nhưng timestamp khác nhau. Ví dụ Processing lúc 09:00 và Shipped
lúc 10:00 đều là event hợp lệ. Event 10:00 thắng cho current-state merge, nhưng
event 09:00 vẫn được giữ ở Bronze.

## Reject-rate reporting

Pipeline chỉ báo cáo tỷ lệ quarantine để vận hành và không tự đặt một ngưỡng
5% không xuất phát từ yêu cầu nghiệp vụ:

~~~text
reject_rate = rejected_rows / raw_rows
~~~

File-level failure (schema sai, file rỗng hoặc không đọc được) vẫn làm run
FAILED. Row-level failure được ghi vào Quarantine; các row hợp lệ tiếp tục đi
qua Silver và Gold. Operator có thể dùng `reject_rate` trong monitoring để
đặt cảnh báo phù hợp với từng nguồn dữ liệu.

## Reconciliation groups

### Batch accounting

- raw = valid + rejected + exact duplicate;
- mọi rejected row tồn tại trong quarantine;
- không có accepted row bị mất source hash hoặc run id.

### Silver current state

- không trùng Order_ID trong order header;
- không trùng (Order_ID, Order_Line_ID) trong line state;
- mọi active line có order header;
- line active không thuộc order deleted;
- event cũ hơn current state không được ghi đè.

### Fact reconciliation

- active silver lines = fact_sales_line;
- active silver orders = fact_order_fulfillment;
- tổng Net_Line_Amount của Silver bằng Fact;
- shipping cost không bị nhân theo số line.

### Dimension integrity

- foreign key không null ở fact;
- unknown member dùng key 0 khi cần;
- dim_customer có tối đa một Is_Current = 1 cho mỗi natural key;
- SCD2 intervals không chồng lấn;
- temporal join match nhiều nhất một customer version.

### Publication

- toàn bộ Gold output cùng Publication_Run_ID;
- đủ các table bắt buộc;
- reconciliation PASS trước khi pointer đổi;
- run FAILED không bao giờ là current publication.

## Expected results

| Check | Expected |
|---|---|
| Duplicate ingestion | 0 accepted duplicate sau replay cùng hash |
| Orphan row loss | 0 |
| Raw accounting | Khớp tuyệt đối |
| Failed Gold run published | 0 |
| Old event overwriting newer | 0 |
| Silver và Fact financial total | Chênh lệch 0 |

Test coverage chính nằm ở test_data_quality.py, test_pipeline_invariants.py và
test_reconciliation.py. Spark integration test phải chạy thật trong CI, không
được skip khi dependency bị thiếu.

DELETE không tìm thấy current-state target được phân loại là `ORPHAN_DELETE`, ghi
vào quarantine và cộng vào rejected rows; không được âm thầm coi là merge thành
công.
