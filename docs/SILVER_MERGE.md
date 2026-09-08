# Silver Current-State và Delta MERGE

Silver tách hai mục đích:

1. event history đã chuẩn hóa để audit;
2. current state để Gold và dashboard truy vấn nhanh.

Không dùng current-state table thay cho Bronze history.

## Hai bảng current-state

### silver_orders_current

Grain: một row cho Order_ID.

Các field chính:

~~~text
Order_ID, Order_Date, Customer_ID, Customer_Segment
Order_Status, Payment_Method, Shipping_Method
Shipping_Cost, Shipping_Days, Region, Country
Source_Updated_At, Is_Deleted, Record_Hash
Last_Run_ID, Last_Source_Hash, Created_At, Updated_At
~~~

Order header không được cộng theo số line. Khi input chỉ là line-level change,
DELETE của một line không được xóa toàn bộ order header. Header chỉ deleted khi
có order-level operation hoặc không còn active line theo business rule đã thống
nhất.

### silver_order_lines_current

Grain: một row cho (Order_ID, Order_Line_ID).

Measure chuẩn:

~~~text
Gross_Amount = Quantity * Unit_Price
Discount_Amount = Gross_Amount * Discount_Rate
Net_Line_Amount = Gross_Amount - Discount_Amount
Cost_Amount = Quantity * Cost
Gross_Profit = Net_Line_Amount - Cost_Amount
~~~

Is_Deleted là soft delete. Gold chỉ đọc line có Is_Deleted = false.

## Chọn event thắng trong batch

Window partition theo Order_ID và Order_Line_ID, order:

~~~text
Source_Updated_At DESC
_source_row_number DESC
~~~

Row đầu tiên là candidate thắng trong batch. source row number chỉ là tie-breaker
deterministic; nó không thay thế event time.

Các trường hợp cùng timestamp nhưng khác payload là sequence conflict và phải
quarantine trước bước winner selection.

## So sánh với current target

Incoming candidate được join với target bằng hai merge keys:

~~~text
target.Order_ID = source.Order_ID
AND target.Order_Line_ID = source.Order_Line_ID
~~~

Phân loại:

| Điều kiện | Metric |
|---|---|
| Target không tồn tại, UPSERT | inserted_rows |
| Target tồn tại, event mới hơn, hash khác | updated_rows |
| Target tồn tại, event mới hơn, DELETE | deleted_rows |
| Target tồn tại, hash giống | unchanged_rows |
| Incoming cũ hơn target | stale_rows |
| DELETE không có target | orphan_delete_rows hoặc rejected event |

Điều kiện update phải yêu cầu:

~~~text
source.Source_Updated_At > target.Source_Updated_At
~~~

Không dùng arrival time để cho event đến muộn ghi đè event mới.

## MERGE semantics

UPSERT insert khi target chưa có. UPSERT update khi source timestamp mới hơn.
DELETE không xóa vật lý mà cập nhật Is_Deleted = true và giữ timestamp/hash
để audit.

Bronze vẫn append tất cả event hợp lệ. Silver event history chỉ append những
event hợp lệ đã qua duplicate/conflict handling; current tables được rebuild
hoặc merge từ tập event đầy đủ.

## Bảo vệ tài chính

Không dùng Revenue hoặc Profit lặp ở mỗi line để tính order-level KPI. Fact
sales tính từ certified Silver measures. Fact fulfillment tổng hợp line amount
theo order, nhưng Shipping_Cost chỉ lấy một lần từ order header.

Reconciliation so sánh cùng status policy:

~~~text
Silver Net_Line_Amount
= Fact Sales Net_Line_Amount
= Executive Mart metric
~~~

