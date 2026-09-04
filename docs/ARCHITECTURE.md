# Kiến trúc GlobalCart Intelligence

## Mục tiêu thiết kế

Hệ thống được thiết kế cho bốn yêu cầu cốt lõi: dữ liệu có thể truy vết, chạy lại không nhân đôi (idempotency), mô hình Kimball dễ dùng trong BI và chỉ có một nguồn sự thật (Single Source of Truth). Đây là kiến trúc portfolio mô phỏng nền tảng Data Lakehouse; các quyết định production hóa được ghi nhận rõ ràng để đảm bảo tính trung thực.

## Luồng dữ liệu Medallion Lakehouse

```
CSV Raw -> Ingestion Layer -> Bronze Delta Lake (_delta_log)
       -> Silver Delta Lake (Quality Gate & Data Contract)
       -> Gold Star Schema (Kimball Fact & 7 Dims) + 12 Gold Marts
       -> Hive Metastore / Spark Thrift Server -> Power BI
       -> MongoDB Batch Sync -> NoSQL Document Collections
```

1. **Ingestion & Validation:** File CSV thô được kiểm tra schema và Data Contract đầu vào qua `lakehouse.ingestion`.
2. **Bronze Layer:** Ghi dữ liệu thô nguyên vẹn dưới định dạng Delta Lake để tạo Transaction Log (`_delta_log`).
3. **Silver Layer:** Ép kiểu dữ liệu, làm sạch, loại bỏ trùng lặp và tính toán thuộc tính phái sinh (`Revenue_Per_Order`, `Net_Profit`, `Delivery_Level`, `Profit_Margin_Percent`).
4. **Data Quality Gate / Contract:** Kiểm tra các quy tắc nghiệp vụ nghiêm ngặt (severity `ERROR` và `WARNING`). Dữ liệu không đạt sẽ bị từ chối hoặc chuyển vào log/quarantine.
5. **Gold Layer (Kimball Star Schema & Data Marts):**
   - Xây dựng **1 Fact Table** (`fact_sales` - grain: 1 dòng = 1 sản phẩm trong 1 đơn) và **7 Dimension Tables**.
   - Xây dựng **12 Gold Data Marts** tổng hợp kinh doanh, bao gồm **RFM Customer Segmentation** và **Pareto ABC Analysis** dựa trên Engine tập trung `analytics_rules.py`.
6. **Data Serving (Hive & MongoDB):**
   - Đăng ký toàn bộ bảng Gold vào Hive Metastore để Spark Thrift Server phục vụ Power BI qua ODBC.
   - Đồng bộ dữ liệu Gold Delta Lake trực tiếp sang MongoDB Collections qua PyMongo batch iterator.

## Quyết định kỹ thuật & Đánh đổi

| Quyết định | Lý do | Đánh đổi |
|---|---|---|
| Package Lakehouse Modular (`SourceCode/lakehouse/`) | Tách biệt trách nhiệm (Single Responsibility), dễ bảo trì và unit test | Cần cấu hình module package Python |
| Delta Lake thay Parquet thuần | ACID transactions, schema enforcement, time travel | Yêu cầu dependency `delta-spark` |
| Rule Analytics Tập Trung (`analytics_rules.py`) | Đảm bảo Pandas và PySpark cho kết quả RFM/ABC giống hệt nhau 100% | Cần duy trì Contract Test giữa 2 engine |
| Star schema Kimball | Quan hệ 1-nhiều rõ ràng, DAX Power BI đơn giản | Tốn công xây dựng khóa surrogate key |
| Surrogate key bằng `row_number()` | Kết quả tái lập ổn định khi re-run pipeline | Window ordering cần cân nhắc khi dữ liệu lớn |
| Idempotent Overwrite Mode | An toàn khi chạy lại pipeline, không lo trùng lặp dữ liệu | Đơn giản cho portfolio; production cần MERGE incremental |
| Derby Metastore local | Dễ dàng chạy demo trên một máy đơn lẻ | Không hỗ trợ nhiều kết quả truy vấn song song đồng thời |

## Mô hình Gold (Star Schema)

`fact_sales` có grain: **1 sản phẩm trong 1 đơn hàng**. Các khóa ngoại liên kết tới:

- `dim_date`: ngày, tháng, quý và năm (DateKey dạng `yyyyMMdd`).
- `dim_customer`: khách hàng, giới tính và phân khúc.
- `dim_product`: category, sub-category và tên sản phẩm.
- `dim_location`: khu vực và quốc gia.
- `dim_shipping`: phương thức và mức giao hàng.
- `dim_payment`: phương thức thanh toán.
- `dim_order_status`: trạng thái, cờ trả hàng và hủy đơn.

Các chỉ số measure cộng được lưu tại Fact: `Quantity`, `Revenue`, `Cost`, `Profit`, `Shipping_Cost`. Tỷ lệ biên lợi nhuận được tính bằng `SUM(Profit) / SUM(Revenue) * 100` tại tầng BI/Marts thay vì cộng trung bình các tỷ lệ dòng.

## Security, Privacy & Compliance

- **PII Protection:** Hỗ trợ quy tắc mã hóa/hashing `Customer_ID` trước khi công bố dữ liệu public.
- **Credential Safety:** Mọi thông số kết nối (MongoDB URI, Thrift Port, HDFS Base) được quản lý qua biến môi trường (`config.py`).
- **Data Governance:** Định nghĩa rõ Data Contract với Severity (`ERROR`, `WARNING`) và tỷ lệ Reject Rate %.

## Ranh giới & Hạn chế Portfolio

- Pipeline vận hành theo chế độ Batch Full Refresh 10.000 dòng.
- RFM và Pareto ABC là phân tích theo quy tắc (Rule-based), không phải Machine Learning.
- Chưa thực hiện cluster sizing hay load test concurrency 100 người dùng thực tế.
