# Data Contract và Schema Evolution

Tài liệu này mô tả contract điều khiển cách một file OMS được đọc, validate và
đưa vào Bronze/Silver. Contract là một phần của data product, không phải chỉ là
documentation.

## Contract runtime

Contract canonical cho batch order-change là:

~~~text
contracts/ecommerce_order_change_v2.yaml
~~~

Contract v2 có:

- version: 2.0.0
- dataset: ecommerce_order_change
- grain: one change event for one order line
- merge keys: Order_ID và Order_Line_ID
- sequence column: Source_Updated_At
- operation column: Operation

Dataset seed hiện tại có schema lịch sử cũ hơn nên `ecommerce_order.yaml` chỉ
được dùng tại bootstrap adapter. Mọi batch incremental phải theo canonical v2;
adapter không được chạy trong incremental flow.

## Grain và business key

Một dòng v2 là một event thay đổi của một order line. Khóa ổn định là:

~~~text
(Order_ID, Order_Line_ID)
~~~

Không dùng Product_Name, Quantity, Unit_Price hoặc Discount làm khóa. Đây là
mutable attributes; nếu giá hoặc số lượng thay đổi thì đó là version mới của
cùng order line.

Order_Line_ID phải do OMS cung cấp cho incremental batch. Adapter bootstrap có
thể sinh ID cho dataset lịch sử hiện có:

~~~text
Order_Line_ID = Order_ID + "-1"
Product_ID = sha2(normalized(Product_Name))
Source_Updated_At = to_timestamp(Order_Date)
Operation = UPSERT
~~~

ID bootstrap không được dùng để che lỗi thiếu ID trong dữ liệu incremental. Nếu
batch incremental thiếu Order_Line_ID, pipeline phải fail ở contract validation.

## Các cột nghiệp vụ

| Cột | Kiểu Silver | Bắt buộc | Ghi chú |
|---|---|---:|---|
| Order_ID | string | Có | Khóa order |
| Order_Line_ID | string | Có | Khóa line ổn định từ OMS |
| Product_ID | string | UPSERT | Natural key sản phẩm |
| Source_Updated_At | timestamp | Có | Event time, dùng để chống stale event |
| Operation | string | Có | UPSERT hoặc DELETE |
| Order_Date | date | UPSERT | Ngày đặt hàng |
| Customer_ID | string | UPSERT | Natural key customer |
| Quantity | integer | UPSERT | Lớn hơn 0 |
| Unit_Price | decimal | UPSERT | Không âm |
| Discount | decimal | UPSERT | Trong khoảng 0 đến 1 |
| Cost | decimal | UPSERT | Không âm |
| Order_Status | string | UPSERT | Processing, Shipped, Delivered, Returned, Cancelled |
| Payment_Method | string | UPSERT | Phương thức thanh toán |
| Shipping_Method | string | UPSERT | Phương thức vận chuyển |
| Region | string | Có thể null | Geography |
| Country | string | Có thể null | Geography |

DELETE chỉ bắt buộc Order_ID, Order_Line_ID, Source_Updated_At và Operation.
Các measure và thuộc tính mô tả line có thể null vì DELETE không cần payload
đầy đủ.

## Metadata bắt buộc

Ingestion thêm các cột metadata sau, không lấy từ dữ liệu nghiệp vụ:

~~~text
_run_id
_batch_id
_source_system
_source_uri
_source_hash
_source_row_number
_ingested_at
_record_hash
_contract_version
_pipeline_version
~~~

source_hash là SHA-256 của bytes file. record_hash dùng để nhận diện exact
duplicate. source_row_number hỗ trợ truy vết dòng lỗi về file nguồn.

## Hai cấp validation

### File-level

File bị từ chối trước Bronze nếu:

- không tồn tại hoặc không đọc được;
- rỗng;
- không có header;
- header có cột trùng;
- encoding không đọc được;
- contract version không hỗ trợ;
- thiếu cột bắt buộc.

Các lỗi này là FILE_SCHEMA_MISMATCH hoặc FILE_EMPTY ở cấp file. Không biến một
file không thể parse thành nhiều row-level rejection giả.

### Row-level

Sau khi file hợp lệ, mỗi dòng được cast và đánh giá theo rules:

| Điều kiện | Error code |
|---|---|
| Order_ID null | MISSING_ORDER_ID |
| Order_Line_ID null | MISSING_LINE_ID |
| Product_ID null với UPSERT | MISSING_PRODUCT_ID |
| Source_Updated_At cast lỗi | INVALID_UPDATED_AT |
| Operation ngoài UPSERT/DELETE | INVALID_OPERATION |
| Quantity nhỏ hơn hoặc bằng 0 | INVALID_QUANTITY |
| Unit_Price âm | INVALID_UNIT_PRICE |
| Discount ngoài [0, 1] | INVALID_DISCOUNT |
| Cost âm | INVALID_COST |
| Order_Date sau Source_Updated_At | INVALID_EVENT_TIME |

Một dòng có thể có nhiều error code. Tất cả code phải được giữ trong array
error_codes và ghi vào quarantine cùng raw values/metadata.

## Schema evolution

Thay đổi contract phải:

1. tăng version;
2. cập nhật YAML;
3. cập nhật evaluator và test;
4. quyết định cột mới nullable hay backfill;
5. không âm thầm đổi nghĩa của merge key hoặc sequence column.

Đổi Order_Line_ID hoặc Source_Updated_At là breaking change. Phải có migration
strategy, không chỉ sửa tên cột.
