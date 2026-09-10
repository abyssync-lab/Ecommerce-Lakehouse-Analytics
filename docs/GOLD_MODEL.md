# Gold Model và Business Semantics

Gold được xây dựng theo run-scoped snapshot. Mỗi run tạo dimensions, facts và
marts riêng trong gold staging; chỉ snapshot đã reconcile mới được phục vụ BI.
Facts và Type 1 dimensions chỉ đọc current event snapshot của Silver. SCD2
customer đọc lại Bronze history qua cùng quality rules để giữ late-arriving events;
Bronze vẫn là nguồn audit, không được dùng trực tiếp làm nguồn số liệu Gold.

## Dimensions

### dim_date

Một row cho ngày: `DateKey`, `FullDate`, `Year`, `Month` và `Quarter`. Đây là
đúng tập cột mà `build_dim_date()` sinh ra cho Gold hiện tại.

### dim_product

Natural key là Product_ID. ProductKey phải ổn định giữa các run. Nếu dataset
bootstrap chưa có Product_ID, adapter sinh từ normalized Product_Name nhưng
incremental input vẫn phải cung cấp Product_ID.

### dim_customer

Type 1 hoặc SCD Type 2 tùy cấu hình. Khi bật SCD2, chỉ theo dõi các thuộc tính
được khai báo:

~~~text
Customer_Segment
Customer_Gender
~~~

Attribute_Hash được tính từ các thuộc tính này. SCD2 có ValidFrom, ValidTo,
Is_Current và khóa CustomerKey persistent.

### dim_geography và dim_order_context

`dim_geography` giữ Region/Country ở một natural key ổn định. Các thuộc tính
cardinality thấp `Order_Status`, `Payment_Method`, `Shipping_Method` và
`Delivery_Level` được gộp trong `dim_order_context`. Cả
fact line và fact order cùng dùng `GeographyKey` và `ContextKey`, nên mô hình
không tạo các dimension nhỏ trùng lặp chỉ vì mỗi thuộc tính có một bảng riêng.
Giá trị thiếu được chuẩn hóa thành `Unknown`; unknown member có surrogate key `0`
để fact không phát sinh foreign key null.

## Fact sales line

Grain: một order line hiện hành chưa deleted.

Các measure:

~~~text
Quantity
Gross_Amount
Discount_Amount
Net_Line_Amount
Cost_Amount
Gross_Profit
~~~

SalesKey phải ổn định từ Order_ID + Order_Line_ID hoặc persistent mapping. Không
dùng row_number trên toàn bảng vì key sẽ thay đổi khi dữ liệu mới đến.

## Fact order fulfillment

Grain: một order hiện hành chưa deleted.

Các measure:

- Line_Count;
- Total_Quantity;
- Order_Value;
- Shipping_Cost;
- Shipping_Days;
- SLA_Days;
- Is_SLA_Breached;
- Is_Delivered, Is_Returned, Is_Cancelled.

Order_Value và Order_Profit lấy từ active order lines. Shipping_Cost lấy ở
order grain để không nhân theo số line.

## SCD Type 2 và temporal join

Khi customer đổi tracked attribute:

1. đóng version hiện tại tại Source_Updated_At;
2. insert version mới từ cùng timestamp;
3. giữ key và lịch sử version trước đó.

Fact temporal join dùng:

~~~text
Order_Timestamp >= ValidFrom
AND Order_Timestamp < ValidTo
~~~

Late event nằm giữa hai version phải rebuild history của customer đó, không rebuild
toàn bộ dimension. Ví dụ A lúc 08:00, B lúc 10:00, A lúc 12:00 và event C đến
muộn lúc 09:00 phải tạo các khoảng A 08-09, C 09-10, B 10-12, A 12 trở đi.

## Business policy

Policy được đọc từ contracts/business_metrics.yaml:

- recognized revenue: Delivered;
- return rate: Returned / (Delivered + Returned);
- RFM: chỉ dùng Delivered;
- ABC: xếp hạng theo Delivered Revenue;
- margin: SUM(Gross_Profit) / SUM(Net_Line_Amount).

Không dùng average của phần trăm margin theo row.

## Gold marts hiện tại

| Mart | Grain | Mục đích |
|---|---|---|
| mart_executive_daily | Ngày | KPI điều hành |
| mart_product_performance | Product | Revenue, profit, return |
| mart_geography_performance | Country/month | So sánh thị trường |
| mart_fulfillment_sla | Shipping method/month | SLA và chi phí vận chuyển |
| mart_customer_rfm | Customer | RFM score, segment, analysis date |
| mart_product_abc | Product | ABC classification và rule version |

Các mart không tự định nghĩa lại metric. Chúng dùng fact đã chuẩn hóa và policy
chung để tránh dashboard mỗi nơi tính một kiểu.

## Run-scoped staging

Pattern:

~~~text
gold/_runs/<run_id>/fact_sales_line
gold/_runs/<run_id>/fact_order_fulfillment
gold/_runs/<run_id>/dim_customer
gold/_runs/<run_id>/marts/...
~~~

Reconciliation đọc snapshot này. Serving chỉ được cập nhật sau PASS.
