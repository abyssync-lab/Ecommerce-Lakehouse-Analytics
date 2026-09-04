# Data Dictionary, Data Contract & KPI Definitions

## 1. Grain & Identifiers

- **Grain Silver / FactSales:** Một dòng sản phẩm trong một đơn hàng.
- `Order_ID`: Mã đơn hàng nghiệp vụ (có thể lặp lại nếu một đơn hàng mua nhiều sản phẩm).
- `SalesKey`: Surrogate Key duy nhất của từng dòng Fact table (sinh bằng `row_number()`).
- `DateKey`: Khóa ngày dạng số nguyên `yyyyMMdd` (ví dụ: `20260801`).

## 2. Danh Mục Các Trường Trong Lakehouse

| Nhóm | Trường Tiêu Biểu | Ý Nghĩa & Quy Tắc Ép Kiểu |
|---|---|---|
| Thời gian | `Order_Date`, `Year`, `Month`, `Quarter`, `Season` | Thời điểm giao dịch, định dạng Date `yyyy-MM-dd` |
| Khách hàng | `Customer_ID`, `Customer_Gender`, `Customer_Segment` | Thuộc tính định danh và phân khúc khách hàng |
| Địa lý | `Region`, `Country` | Thị trường giao dịch bán hàng |
| Sản phẩm | `Category`, `Sub_Category`, `Product_Name` | Phân cấp danh mục sản phẩm |
| Tài chính | `Unit_Price`, `Quantity`, `Discount`, `Revenue`, `Cost`, `Profit` | Chỉ số tài chính nguyên bản của từng sản phẩm |
| Vận chuyển | `Shipping_Cost`, `Shipping_Method`, `Shipping_Days` | Chi phí và số ngày vận chuyển |
| Trạng thái | `Order_Status`, `Payment_Method` | Trạng thái xử lý đơn (`Delivered`, `Returned`, `Cancelled`) |

## 3. Các Trường Phái Sinh (Enriched Fields ở Silver)

| Trường | Công Thức / Logic Chuẩn Hóa |
|---|---|
| `Profit_Margin_Percent` | `(Profit / Revenue) * 100` (nếu Revenue != 0, ngược lại 0.0) |
| `Net_Profit` | `Profit - Shipping_Cost` |
| `Is_Returned` | 1 nếu `Order_Status == 'Returned'`, ngược lại 0 |
| `Is_Cancelled` | 1 nếu `Order_Status == 'Cancelled'`, ngược lại 0 |
| `Delivery_Level` | `Fast` (<= 3 ngày), `Normal` (<= 7 ngày), `Slow` (> 7 ngày) |

## 4. Data Contract Rules & Validation Strategy

| Rule Code | Mô Tả & Điều Kiện Kiểm Tra | Severity | Xử Lý Khi Vi Phạm |
|---|---|---|---|
| `REQUIRED_COLUMNS` | Đủ 13 trường cột bắt buộc trong schema đầu vào | ERROR | Dừng pipeline lập tức |
| `QUANTITY_POSITIVE` | `Quantity > 0` | ERROR | Đưa bản ghi vào quarantine |
| `UNIT_PRICE_NON_NEGATIVE` | `Unit_Price >= 0` | ERROR | Đưa bản ghi vào quarantine |
| `DISCOUNT_RANGE` | `Discount` trong khoảng `[0.0, 1.0]` | ERROR | Đưa bản ghi vào quarantine |
| `REVENUE_NON_NEGATIVE` | `Revenue >= 0` | ERROR | Đưa bản ghi vào quarantine |
| `SHIPPING_DAYS_NON_NEGATIVE` | `Shipping_Days >= 0` | ERROR | Đưa bản ghi vào quarantine |
| `REVENUE_FORMULA_CONSISTENCY` | `|Revenue - (Quantity * Unit_Price * (1 - Discount))| <= 0.05` | WARNING | Log cảnh báo |
| `ALLOWED_ORDER_STATUS` | `Order_Status` thuộc domain hợp lệ | WARNING | Map thành 'Unknown' hoặc log cảnh báo |

## 5. Quy Tắc Phân Tích RFM & Pareto ABC (`analytics_rules.py`)

### RFM Customer Segmentation
- **Champions:** Recency <= 30 ngày VÀ Frequency >= 3 đơn hàng.
- **Loyal Customers:** Frequency >= 3 đơn hàng.
- **At-Risk Customers:** Recency > 90 ngày.
- **Recent & Casual Customers:** Các trường hợp còn lại.

### Pareto ABC Product Analysis
- **Class A (Top 80% Revenue):** Tỷ trọng doanh thu tích lũy *trước* sản phẩm hiện tại `< 80.0%`.
- **Class B (Next 15% Revenue):** Tỷ trọng doanh thu tích lũy *trước* sản phẩm hiện tại `< 95.0%`.
- **Class C (Tail 5% Revenue):** Các sản phẩm còn lại.

## 6. Công Thức KPI Chuẩn Cho Power BI / Data Marts

| Chỉ Số KPI | Công Thức Khái Niệm | Ghi Chú Tính Toán |
|---|---|---|
| Total Revenue | `SUM(Revenue)` | Tổng doanh thu toàn bộ bản ghi |
| Total Orders | `DISTINCTCOUNT(Order_ID)` | Không dùng count(Order_ID) để tránh nhân đôi |
| Total Profit | `SUM(Profit)` | Tổng lợi nhuận gộp |
| Net Profit | `SUM(Profit) - SUM(Shipping_Cost)` | Lợi nhuận ròng sau chi phí vận chuyển |
| Profit Margin % | `SUM(Profit) / SUM(Revenue) * 100` | **Không** tính trung bình cộng tỷ lệ dòng |
| Average Order Value (AOV) | `SUM(Revenue) / DISTINCTCOUNT(Order_ID)` | Giá trị trung bình trên một đơn hàng phân biệt |
| Return Rate % | `DISTINCTCOUNT(Returned_Orders) / DISTINCTCOUNT(Order_ID) * 100` | Tỷ lệ đơn hàng bị trả lại |
| Cancellation Rate % | `DISTINCTCOUNT(Cancelled_Orders) / DISTINCTCOUNT(Order_ID) * 100` | Tỷ lệ đơn hàng bị hủy |
