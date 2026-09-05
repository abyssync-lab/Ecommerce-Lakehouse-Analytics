# Data Dictionary, Data Contracts & KPI Metrics

## 1. Hệ Thống 3 Tầng Data Contracts

Nền tảng GlobalCart Lakehouse phân định rõ ràng 3 hợp đồng dữ liệu tương ứng với ranh giới xử lý:

```
Source Contract (Landing CSV) ──> Silver Contract (Clean Table) ──> Gold Contract (Warehouse & Marts)
```

### 1.1. Source Contract (Landing Layer)
Định nghĩa tại tệp [contracts/ecommerce_order.yaml](../contracts/ecommerce_order.yaml):
- **Grain:** 1 dòng sản phẩm trong 1 đơn hàng (1 order line item).
- **Trường bắt buộc (NOT NULL):** `Order_ID`, `Order_Date`, `Customer_ID`, `Product_Name`, `Quantity` ($> 0$), `Unit_Price` ($\ge 0$), `Discount` ($0..1$), `Revenue` ($\ge 0$), `Shipping_Days` ($\ge 0$), `Order_Status`.
- **Ràng buộc nghiệp vụ:** $Revenue \approx Quantity \times Unit\_Price \times (1 - Discount)$.

### 1.2. Silver Contract (Validated Processing Layer)
- **Kiểu dữ liệu:** Đã ép kiểu chặt chẽ (`Date`, `Double`, `Integer`).
- **Duy nhất & Nhận diện dòng:** Bổ sung `Order_Line_ID` (kết hợp `Order_ID` và line sequence/hash) phục vụ Delta MERGE INTO idempotent.
- **Kế toán số lượng:** Bảo toàn $Raw = Valid + Invalid + Duplicate$.
- **Bảng Quarantine:** Lưu vết các bản ghi lỗi kèm danh sách mảng đa nguyên nhân `rejection_reasons: array<string>`.

### 1.3. Gold Contract (Certified Analytical Layer)
- **FactSales Grain:** Chuẩn xác 1 dòng sản phẩm trong 1 đơn hàng, kết nối với 7 Dimensions qua Surrogate Keys.
- **SCD Type 2 Semantics:** `dim_customer` quản lý lịch sử theo khoảng nửa mở $[ValidFrom, ValidTo)$, đảm bảo đúng 1 bản ghi `Is_Current = 1` cho mỗi khách hàng.
- **Serving Contract:** Power BI và MongoDB chỉ được phép đọc từ tầng Gold Certified.

---

## 2. Phân Loại Chỉ Số Fact: Additive vs Non-Additive Measures

| Nhóm Measure | Danh Sách Trường | Tính Chất Đại Số | Quy Tắc Tập Hợp & Khuyến Nghị BI |
|---|---|---|---|
| **Additive Measures** (Cộng dồn được) | `Quantity`, `Revenue`, `Cost`, `Profit`, `Shipping_Cost` | Có thể cộng gộp theo mọi chiều (Thời gian, Khách hàng, Địa lý, Sản phẩm) | Sử dụng `SUM(...)` trực tiếp trong DAX Power BI hoặc SQL queries. |
| **Non-Additive Measures** (Không cộng dồn) | `Profit_Margin_Percent` | Tỷ lệ phần trăm biên lợi nhuận của dòng | **Tuyệt đối không dùng `AVG()` các dòng**. Phải tính bằng công thức: $\frac{\sum Profit}{\sum Revenue} \times 100$. |
| **Repeated Non-Additive Attribute** | `Order_Total_Revenue` | Tổng doanh thu cả đơn hàng, lặp lại trên từng line item | **Tuyệt đối không dùng `SUM(Order_Total_Revenue)`** trên bảng FactSales vì sẽ gây nhân đôi doanh thu. Để phân tích theo đơn, sử dụng `mart_order_summary`. |

---

## 3. Danh Mục Các Bảng Kimball Star Schema (Gold Core)

### 3.1. FactSales Table (Grain: 1 Order Line)
- `SalesKey` (Surrogate Key, PK): Khóa đại diện duy nhất sinh bằng `row_number().over(orderBy(...))`.
- `Order_ID` (Degenerate Dimension): Mã hóa đơn kinh doanh.
- `Order_Line_ID` (Line Identity): Mã định danh dòng đơn hàng duy nhất.
- `CustomerKey` (FK): Khóa ngoại trỏ sang `dim_customer` (nối theo khoảng thời gian SCD2).
- `ProductKey` (FK): Khóa ngoại trỏ sang `dim_product`.
- `DateKey` (FK): Khóa ngày định dạng số nguyên `yyyyMMdd` (ví dụ: `20260801`).
- `LocationKey` (FK): Khóa ngoại trỏ sang `dim_location`.
- `PaymentKey` (FK): Khóa ngoại trỏ sang `dim_payment`.
- `ShippingKey` (FK): Khóa ngoại trỏ sang `dim_shipping`.
- `StatusKey` (FK): Khóa ngoại trỏ sang `dim_order_status`.
- Additive Measures: `Unit_Price`, `Quantity`, `Discount`, `Revenue`, `Cost`, `Profit`, `Shipping_Cost`.
- Non-Additive Measure: `Profit_Margin_Percent`.

### 3.2. 7 Dimension Tables
1. **`dim_customer` (SCD Type 2):** `CustomerKey`, `Customer_ID`, `Customer_Gender`, `Customer_Segment`, `ValidFrom`, `ValidTo`, `Is_Current`.
2. **`dim_product`:** `ProductKey`, `Product_Name`, `Category`, `Sub_Category`.
3. **`dim_date`:** `DateKey`, `FullDate`, `Year`, `Month`, `Quarter`.
4. **`dim_location`:** `LocationKey`, `Region`, `Country`.
5. **`dim_payment`:** `PaymentKey`, `Payment_Method`.
6. **`dim_shipping`:** `ShippingKey`, `Shipping_Method`, `Delivery_Level`.
7. **`dim_order_status`:** `StatusKey`, `Order_Status`, `Is_Returned`, `Is_Cancelled`.

---

## 4. Danh Mục 12 Gold Data Marts & Phiên Bản Chính Sách

- `mart_overview`: Tổng quan điều hành doanh nghiệp (Doanh thu, Đơn hàng, Lợi nhuận gộp).
- `mart_order_summary`: Tổng hợp ở mức đơn hàng (Order grain: `Order_ID`, `Total_Items`, `Order_Total_Revenue`).
- `mart_revenue_by_region`: Doanh thu và lợi nhuận theo khu vực địa lý.
- `mart_revenue_by_country`: Doanh thu chi tiết theo quốc gia.
- `mart_revenue_by_category`: Phân tích theo ngành hàng và phân nhóm.
- `mart_top_products_by_revenue`: Top 10 sản phẩm đóng góp doanh thu cao nhất.
- `mart_payment_analysis`: Hiệu quả doanh thu theo phương thức thanh toán.
- `mart_shipping_analysis`: Thời gian giao hàng và chi phí logistics theo phương thức.
- `mart_order_status_analysis`: Tỷ lệ hoàn thành, hủy đơn và hoàn trả.
- `mart_monthly_revenue`: Xu hướng doanh thu chuỗi thời gian theo năm và tháng.
- `mart_customer_segment_analysis`: Giá trị đơn hàng trung bình (AOV) theo phân khúc khách hàng.
- `mart_rfm_customer_segmentation`: Phân khúc RFM (*Champions, Loyal, At-Risk, Casual*) kèm `Rule_Version = rfm-v1`.
- `mart_abc_product_analysis`: Phân loại Pareto ABC (*Class A: 80%, Class B: 15%, Class C: 5%*) kèm `Rule_Version = abc-v1`.

---

## 5. Công Thức Đo Lường KPI Chuẩn (DAX / Power BI / SQL)

| Chỉ Số KPI | Công Thức Khái Niệm | Ghi Chú & Lưu Ý Kỹ Thuật |
|---|---|---|
| **Total Revenue** | `SUM(Revenue)` | Tính trên bảng `FactSales` |
| **Total Orders** | `DISTINCTCOUNT(Order_ID)` | Không dùng `COUNT(Order_ID)` trên FactSales để tránh nhân đôi |
| **Total Profit** | `SUM(Profit)` | Lợi nhuận gộp toàn hệ thống |
| **Net Profit** | `SUM(Profit) - SUM(Shipping_Cost)` | Lợi nhuận thực sau khi trừ chi phí vận chuyển |
| **Profit Margin %** | `(SUM(Profit) / SUM(Revenue)) * 100` | **KPI Hợp đồng**: Không tính trung bình cộng tỷ lệ dòng |
| **Average Order Value (AOV)** | `SUM(Revenue) / DISTINCTCOUNT(Order_ID)` | Doanh thu trung bình trên mỗi đơn hàng độc lập |
| **Return Rate %** | `DISTINCTCOUNT(Returned_Orders) / DISTINCTCOUNT(Order_ID) * 100` | Tỷ lệ đơn phát sinh hoàn trả |
| **Cancellation Rate %** | `DISTINCTCOUNT(Cancelled_Orders) / DISTINCTCOUNT(Order_ID) * 100` | Tỷ lệ đơn bị hủy trước khi giao |
