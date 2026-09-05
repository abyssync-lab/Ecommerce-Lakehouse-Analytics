# 🌐 GlobalCart Lakehouse Analytics Platform — Incremental PySpark/Delta Data Engineering & BI

### 🚀 Production-Oriented Ecommerce Lakehouse Architecture | Data Contracts | Quarantine Gate | Kimball Star Schema | SCD Type 2 | Reconciliation Gate & BI Serving

`Apache Spark 3.5` · `Delta Lake 3.2` · `Hadoop HDFS` · `Hive Metastore` · `MongoDB` · `Power BI` · `Python 3` · `Pytest` · `YAML Data Contracts`

> **GitHub Description:**
> *A production-oriented ecommerce lakehouse built with PySpark and Delta Lake, featuring Bronze/Silver/Gold data contracts, quarantine-based data quality, incremental MERGE processing, Kimball dimensional modeling with SCD Type 2, analytical marts, reconciliation tests and BI serving.*

---

## 📑 Mục Lục Hệ Thống (Table of Contents)

1. [Problem & Business Context](#1-problem--business-context)
2. [Canonical Lakehouse Architecture (Sơ Đồ 9 Tầng Chuẩn)](#2-canonical-lakehouse-architecture-sơ-đồ-9-tầng-chuẩn)
3. [Hệ Thống 3 Tầng Data Contracts](#3-hệ-thống-3-tầng-data-contracts)
4. [Hai Chế Độ Vận Hành: Bootstrap vs Incremental Processing](#4-hai-chế-độ-vận-hành-bootstrap-vs-incremental-processing)
5. [Tầng Bronze: Immutable Raw Delta, Ingestion Metadata & Delta Log](#5-tầng-bronze-immutable-raw-delta-ingestion-metadata--delta-log)
6. [Tầng Silver: Data Quality Gate & Multi-Error Quarantine Table](#6-tầng-silver-data-quality-gate--multi-error-quarantine-table)
7. [Incremental MERGE, Idempotency & Order-Line Identity](#7-incremental-merge-idempotency--order-line-identity)
8. [Tầng Gold Core: Mô Hình Kimball Star Schema](#8-tầng-gold-core-mô-hình-kimball-star-schema)
9. [SCD Type 2 Customer History: Thuật Toán Event-Order Transition](#9-scd-type-2-customer-history-thuật-toán-event-order-transition)
10. [Fact Grain & Phân Loại Additive vs Non-Additive Measures](#10-fact-grain--phân-loại-additive-vs-non-additive-measures)
11. [Tầng Gold Analytics: 12 Data Marts & Phiên Bản Chính Sách](#11-tầng-gold-analytics-12-data-marts--phiên-bản-chính-sách)
12. [Đối Soát & Bất Biến Dữ Liệu (Data Reconciliation Gate)](#12-đối-soát--bất-biến-dữ-liệu-data-reconciliation-gate)
13. [Tầng Phục Vụ BI: Power BI qua Spark Thrift Server & Hive Metastore](#13-tầng-phục-vụ-bi-power-bi-qua-spark-thrift-server--hive-metastore)
14. [Tầng Phục Vụ Ứng Dụng: MongoDB Secondary Serving Store](#14-tầng-phục-vụ-ứng-dụng-mongodb-secondary-serving-store)
15. [Delta Lake ACID Transactions & Thử Nghiệm Cô Lập (Isolated Demos)](#15-delta-lake-acid-transactions--thử-nghiệm-cô-lập-isolated-demos)
16. [Vận Hành & Giám Sát Chất Lượng (Data Observability Table)](#16-vận-hành--giám-sát-chất-lượng-data-observability-table)
17. [Đánh Giá Hiệu Năng: Synthetic Single-Node Scaling Experiment](#17-đánh-giá-hiệu-năng-synthetic-single-node-scaling-experiment)
18. [Chiến Lược Kiểm Thử Tự Động (Automated Testing Suite)](#18-chiến-lược-kiểm-thử-tự-động-automated-testing-suite)
19. [Giới Hạn Hiện Tại & Ranh Giới Kỹ Thuật (Production Boundary)](#19-giới-hạn-hiện-tại--ranh-giới-kỹ-thuật-production-boundary)
20. [Lộ Trình Chiến Lược (Strategic Roadmap) & CV Placement](#20-lộ-trình-chiến-lược-strategic-roadmap--cv-placement)

---

## 1. Problem & Business Context

Trong môi trường thương mại điện tử đa quốc gia quy mô lớn, các tổ chức dữ liệu thường đối mặt với 5 thách thức cốt lõi:
- **Dữ liệu thô phân tán và vi phạm chất lượng:** File giao dịch chứa đơn giá âm, chiết khấu vượt quá $100\%$, mã đơn hàng bị khuyết thiếu hoặc thời gian giao hàng bất hợp lý.
- **Rủi ro nhân đôi doanh thu khi khách hàng đổi thông tin:** Khi thuộc tính nhân khẩu học (Phân khúc, Vùng) thay đổi, join dimension thông thường làm nhân bản dòng Fact bán hàng nếu không có cơ chế SCD Type 2 chuẩn xác.
- **Bất đồng bộ số liệu giữa BI và ứng dụng vận hành:** Báo cáo Power BI hiển thị một số, trong khi MongoDB phục vụ microservice hiển thị số khác do truy vấn trực tiếp từ các tầng xử lý chưa được kiểm toán.
- **Khó khăn trong audit và truy vết:** Khi phát hiện số liệu sai lệch, đội ngũ kỹ sư không thể xác định bản ghi đến từ batch nào, file nguồn nào và vi phạm những luật nghiệp vụ nào.
- **Chi phí điện toán cao khi phải full rebuild toàn bộ:** Cần phân tách rành mạch cơ chế nạp tăng tiến (**Incremental Ingestion & Silver MERGE**) với việc làm mới có định hướng tầng Gold.

**Sứ mệnh của GlobalCart:** Hiện thực hóa một nền tảng Data Lakehouse production-oriented chuẩn mực theo trục:
$$\text{Data Contracts} \longrightarrow \text{Incremental MERGE} \longrightarrow \text{Dimensional History} \longrightarrow \text{Reconciliation} \longrightarrow \text{Certified Serving} \longrightarrow \text{Observability}$$

---

## 2. Canonical Lakehouse Architecture (Sơ Đồ 9 Tầng Chuẩn)

Kiến trúc duy nhất xuất hiện đồng nhất xuyên suốt mã nguồn, tài liệu, bài phỏng vấn và CV:

```
                         CONTROL PLANE
Pipeline Config ──> Batch Registry ──> Data Contracts ──> Run Metadata / Audit
                               │
                               ▼
                         DATA PLANE

1. RAW INGESTION & DATA CONTRACTS
   Historical CSV / Micro-batch CSV / Future: Kafka & CDC
   Schema contract (contracts/ecommerce_order.yaml)
   Batch identity, ingestion timestamp, source file hash
                               │
                               ▼
2. BRONZE LAYER (IMMUTABLE RAW DELTA)
   Raw source columns preserved
   System metadata: _batch_id, _ingested_at, _source_file, _source_hash, _record_hash
   Transaction log: _delta_log (ACID audit trail)
                               │
                               ▼
3. SILVER LAYER (DATA QUALITY GATE & QUARANTINE)
   Cast schema, normalize, derived business metrics
   Accounting Invariant: Raw = Valid + Invalid + Duplicate
               │
        ┌──────┴──────┐
        ▼             ▼
     VALID         INVALID
        │             │
        │      QUARANTINE TABLE
        │      - rejection_reasons: array<string>
        │      - batch_id, rejected_at
        ▼
4. SILVER MERGE / CDC CONTRACT
   Business Line Key: Order_ID + Order_Line_ID
   Delta MERGE INTO (whenMatchedUpdateAll, whenNotMatchedInsertAll)
   Idempotent batch processing
               │
               ▼
5. GOLD CORE MODEL (KIMBALL STAR SCHEMA)
   FactSales (Grain: 1 order line, Degenerate Dim: Order_ID)
   DimCustomer (SCD Type 2 Event-Order Change Detection [ValidFrom, ValidTo))
   DimProduct, DimDate, DimLocation, DimPayment, DimShipping, DimOrderStatus
               │
               ▼
6. GOLD ANALYTICAL MARTS
   12 Data Marts + mart_order_summary (Order grain)
   RFM Customer Segmentation (Rule version: rfm-v1)
   Pareto ABC Product Analysis (Rule version: abc-v1)
               │
               ▼
7. DATA RECONCILIATION GATE
   Revenue Invariant: Sum(Silver) = Sum(Fact) = Overview Mart
   Row Conservation: Raw = Valid + Invalid + Duplicate
   Fact Grain Uniqueness: Count(Fact) = Distinct(SalesKey)
   Foreign Key Completeness: Zero Orphan Keys
   SCD2 Temporal Integrity: Exactly one Is_Current = 1 per customer
               │
               ▼
8. SERVING LAYER (GOLD-ONLY CONTRACT)
   ├── Hive Metastore / Spark Thrift Server ──> Power BI (ODBC/DirectQuery)
   └── Certified Gold Marts ──────────────────> MongoDB BSON Collections
               │
               ▼
9. OPERATIONS & OBSERVABILITY
   pipeline_quality Delta table (raw, duplicate, valid, quarantined rows, reject rate %)
   System Doctor CLI & JSON audit run reports
```

---

## 3. Hệ Thống 3 Tầng Data Contracts

Toàn bộ quy chuẩn về kiểu dữ liệu, tính nullable và biên giá trị được cấu hình độc lập tại tệp [contracts/ecommerce_order.yaml](contracts/ecommerce_order.yaml):

1. **Source Contract (Landing Layer):** Định nghĩa schema tiếp nhận thô (26 trường thuộc tính) và các ràng buộc nghiệp vụ tối thiểu.
2. **Silver Contract (Processing Layer):** Dữ liệu sạch, ép kiểu chính xác, chuẩn hóa chuỗi, gán `Order_Line_ID` định danh duy nhất cho từng dòng đơn và phân lập lỗi vào mảng `rejection_reasons`.
3. **Gold Contract (Certified Serving Layer):** FactSales theo đúng grain 1 dòng sản phẩm trong 1 đơn; `dim_customer` SCD Type 2 bảo đảm khoảng thời gian $[ValidFrom, ValidTo)$ liên tục, không chồng lấn.

---

## 4. Hai Chế Độ Vận Hành: Bootstrap vs Incremental Processing

Hệ thống phân tách rành mạch hai chế độ thực thi qua `PipelineConfig`:

| Tiêu Chí Kỹ Thuật | 1. Bootstrap Mode (Full Refresh) | 2. Incremental Mode (Micro-batch MERGE) |
|---|---|---|
| **Mục đích** | Khởi tạo kho dữ liệu từ đầu, nạp lịch sử hoặc chạy lại toàn bộ | Nạp định kỳ các batch giao dịch mới với chi phí tài nguyên tối ưu |
| **Bronze Layer** | Ghi đè (`mode="overwrite"`) | Ghi tiếp (`mode="append"`) kèm `_batch_id` và `_source_hash` |
| **Silver Layer** | Ép kiểu, làm sạch, ghi đè Silver Delta | Làm sạch batch mới, **Delta MERGE INTO** theo `(Order_ID, Order_Line_ID)` |
| **Quarantine** | Ghi đè / nạp mới | Ghi tiếp (`mode="append"`) các bản ghi lỗi của batch mới |
| **Gold Core** | Rebuild toàn bộ 7 Dimensions & FactSales | Rebuild xác định (*Deterministic Gold Refresh*) bảo toàn SCD2 |
| **Gold Marts** | Tính toán lại toàn bộ 12 Marts | Làm mới các Marts từ bảng Silver/Fact đã cập nhật |
| **Lệnh Vận Hành CLI** | `globalcart pipeline bootstrap --scd2` | `globalcart pipeline incremental --input batch.csv --batch-id B01 --scd2` |

> [!NOTE]
> **Định vị chính xác về Gold:** Incremental Pipeline tại đây thực hiện:
> *Incremental Bronze append $\to$ Incremental Silver MERGE $\to$ Deterministic Gold refresh*.
> Điều này đảm bảo tính toàn vẹn 100% của toàn bộ 12 Marts và lịch sử SCD2 mà không gây sai lệch số liệu.

---

## 5. Tầng Bronze: Immutable Raw Delta, Ingestion Metadata & Delta Log

Khác biệt với việc chỉ copy file CSV sang định dạng Parquet, tầng Bronze của GlobalCart được nâng cấp để hỗ trợ **truy vết (traceability), dòng đời dữ liệu (lineage), khả năng phát lại (replay)** và **kiểm soát idempotency**:

Mỗi bản ghi được bổ sung 6 trường siêu dữ liệu hệ thống:
- `_ingested_at`: Timestamp hệ thống chính xác tại thời điểm ghi vào Bronze Delta.
- `_source_file`: Đường dẫn file CSV nguồn gốc ban đầu (sử dụng `input_file_name()`).
- `_batch_id`: Mã nhận diện batch / run ingestion.
- `_source_system`: Nguồn phát sinh dữ liệu (ví dụ: `ecommerce_csv`).
- `_source_hash`: Mã băm SHA-256 của file nguồn đầu vào phục vụ batch registry idempotency.
- `_record_hash`: Mã băm SHA-256 trên toàn bộ thuộc tính nghiệp vụ để kiểm tra toàn vẹn bản ghi.
- `_pipeline_version`: Phiên bản pipeline nạp dữ liệu (`1.0.0`).

Mọi giao dịch ghi vào Bronze đều được bảo vệ bởi **Delta Lake Transaction Log (`_delta_log`)**, hỗ trợ tính năng khôi phục và audit lịch sử giao dịch.

---

## 6. Tầng Silver: Data Quality Gate & Multi-Error Quarantine Table

### 1. Kế Toán Số Dòng Chuẩn Xác (Conservation Invariant)
Khác với các triển khai ETL thông thường gộp chung bản ghi trùng lặp vào danh sách lỗi, GlobalCart tách biệt rành mạch:
$$\text{Raw Count} = \text{Clean Valid Count} + \text{Quarantined Count} + \text{Duplicate Count}$$

- `duplicate_count`: Các dòng trùng lặp y hệt ở tầng nguồn được lọc sạch bằng `dropDuplicates()`.
- `quarantine_count`: Các dòng vi phạm Data Quality Gate được phân lập vào bảng Delta Quarantine riêng biệt.

### 2. Multi-Reason Quarantine Tracking
Thay vì chỉ lưu 1 lỗi đầu tiên gặp phải, tầng Quarantine lưu vết **toàn bộ danh sách các điều kiện bị vi phạm**:
- `rejection_reasons`: Danh sách mảng chuỗi `array<string>` (ví dụ: `["INVALID_QUANTITY", "INVALID_DISCOUNT"]`).
- `rejection_reason`: Chuỗi nối phân tách bằng dấu chấm phẩy phục vụ hiển thị báo cáo tabular.
- `rejected_at`: Thời gian bị cách ly vào bảng Quarantine.

---

## 7. Incremental MERGE, Idempotency & Order-Line Identity

### 1. Khóa MERGE Cấp Dòng (Line-Item Identity)
Nếu chỉ MERGE theo `(Order_ID, Product_Name)`, khi một đơn hàng có nhiều dòng mua cùng một sản phẩm (ví dụ dòng 1 mua quà tặng kèm, dòng 2 mua hàng trả phí), phép MERGE sẽ bị conflation (xung đột dữ liệu).

GlobalCart giải quyết triệt để bằng cách tạo **`Order_Line_ID`** ổn định:
$$\text{Order\_Line\_ID} = \text{Order\_ID} + \text{"-"} + \text{row\_number().over(Window.partitionBy("Order\_ID").orderBy("Product\_Name", "Quantity"))}$$

Khi thực thi Delta MERGE INTO:
```sql
MERGE INTO silver.ecommerce_clean AS target
USING clean_batch AS source
ON target.Order_ID = source.Order_ID AND target.Order_Line_ID = source.Order_Line_ID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 2. Batch Registry & Idempotency
Hệ thống lưu vết từng batch vào bảng Delta `ingestion_batches_delta`:
- `batch_id`, `source_hash`, `row_count`, `status`, `registered_at`.
- Khi nạp lại cùng một batch, hệ thống đối soát hash để tránh nhân đôi dữ liệu.

---

## 8. Tầng Gold Core: Mô Hình Kimball Star Schema

Tầng Gold Core được thiết kế theo chuẩn **Kimball Dimensional Modeling**:

```
                       DimDate
                          │
DimCustomer ────────── FactSales ────────── DimProduct
(SCD Type 2)              │
                     DimLocation
                          │
                     DimPayment
                          │
                     DimShipping
                          │
                    DimOrderStatus
```

### 1 Bảng Fact & 7 Bảng Dimension:
1. **`FactSales`:** Grain: **1 dòng sản phẩm trong 1 đơn hàng (1 order line)**.
   - Foreign Keys: `CustomerKey`, `ProductKey`, `DateKey`, `LocationKey`, `PaymentKey`, `ShippingKey`, `StatusKey`.
   - Degenerate Dimension: `Order_ID`, `Order_Line_ID`.
   - Measures: `Unit_Price`, `Quantity`, `Discount`, `Revenue`, `Cost`, `Profit`, `Profit_Margin_Percent`, `Shipping_Cost`.
2. **`DimCustomer` (SCD Type 2):** Quản lý lịch sử biến đổi của khách hàng.
3. **`DimProduct`:** `ProductKey`, `Product_Name`, `Category`, `Sub_Category`.
4. **`DimDate`:** `DateKey` (`yyyyMMdd`), `FullDate`, `Year`, `Month`, `Quarter`.
5. **`DimLocation`:** `LocationKey`, `Region`, `Country`.
6. **`DimPayment`:** `PaymentKey`, `Payment_Method`.
7. **`DimShipping`:** `ShippingKey`, `Shipping_Method`, `Delivery_Level`.
8. **`DimOrderStatus`:** `StatusKey`, `Order_Status`, `Is_Returned`, `Is_Cancelled`.

### Chiến Lược Khóa Đại Diện (Surrogate Key Policy)
- **Portfolio Deterministic Rebuild Key:** Sử dụng `row_number().over(Window.orderBy(*order_cols))` sắp xếp theo khóa tự nhiên nghiệp vụ để bảo đảm 100% khả năng tái lập (reproducibility) khi re-run pipeline.
- **Enterprise Persistent Key (Kiến trúc tham chiếu):** Trong kho dữ liệu production lâu dài nhiều năm, kiến trúc khuyến nghị sử dụng Stateful Dimension Lookup kết hợp monotonic sequence hoặc generated hash key để không bao giờ thay đổi surrogate key đã cấp trong quá khứ.

---

## 9. SCD Type 2 Customer History: Thuật Toán Event-Order Transition

### Xử Lý Lỗi Kinh Điển State Flip ($A \to B \to A$)
Nếu chỉ sử dụng `groupBy(Customer_ID, attributes).agg(min(Order_Date))` như cách làm sơ khai, khi khách hàng đổi từ *Consumer* $\to$ *Corporate* rồi quay lại *Consumer*, hai giai đoạn Consumer sẽ bị gộp làm một, làm mất hoàn toàn lịch sử giai đoạn giữa.

GlobalCart áp dụng giải thuật **Event-Ordered Change Detection**:
```
Giao dịch khách hàng sắp xếp theo thời gian: (Customer_ID, Order_Date, Order_ID)
                                ↓
                 lag() trên các thuộc tính nhân khẩu
                                ↓
                   Phát hiện sự kiện thay đổi
     is_change = 1 (khi giá trị hiện tại != giá trị trước đó)
                                ↓
      Cumulative sum trên is_change tạo change_group (Island Grouping)
                                ↓
         Mỗi (Customer_ID, change_group) tạo 1 phiên bản độc lập
                       ValidFrom = min(Order_Date)
                                ↓
          lead(ValidFrom) → ValidTo (mặc định 9999-12-31)
                 Is_Current = 1 cho phiên bản mới nhất
```

**Kết quả:**
- Khách hàng có chuỗi $A \to B \to A$ tạo đúng **3 phiên bản riêng biệt**.
- Các khoảng thời gian $[ValidFrom, ValidTo)$ nửa mở, liên tục, không chồng lấn.
- Luôn bảo đảm đúng **1 bản ghi `Is_Current = 1`** cho mỗi khách hàng.

---

## 10. Fact Grain & Phân Loại Additive vs Non-Additive Measures

| Nhóm Measure | Danh Sách Trường | Tính Chất | Quy Tắc Tập Hợp & Khuyến Nghị BI |
|---|---|---|---|
| **Additive Measures** | `Quantity`, `Revenue`, `Cost`, `Profit`, `Shipping_Cost` | Cộng dồn được theo mọi chiều | Dùng `SUM(...)` trực tiếp trong DAX Power BI hoặc SQL queries. |
| **Non-Additive Measures** | `Profit_Margin_Percent` | Tỷ lệ phần trăm biên lợi nhuận dòng | **Tuyệt đối không dùng `AVG()` các dòng**. Phải tính bằng: $\frac{\sum Profit}{\sum Revenue} \times 100$. |
| **Repeated Non-Additive Attribute** | `Order_Total_Revenue` | Tổng doanh thu cả đơn, lặp lại trên từng line item | **Tuyệt đối không dùng `SUM(Order_Total_Revenue)`** trên FactSales vì gây nhân đôi doanh thu. Dùng `mart_order_summary` để phân tích cấp đơn hàng. |

---

## 11. Tầng Gold Analytics: 12 Data Marts & Phiên Bản Chính Sách

Tầng Gold Marts tổng hợp các lát cắt kinh doanh quan trọng và version hóa chính sách phân tích:
1. `mart_overview`: Chỉ số điều hành tổng quan (Doanh thu, Lợi nhuận gộp, Đơn hàng, Biên lợi nhuận trung bình).
2. `mart_order_summary`: Tổng hợp ở mức đơn hàng (**Order grain**), phân tách rõ ràng với FactSales (Line-item grain).
3. `mart_revenue_by_region`: Doanh thu & lợi nhuận theo vùng địa lý.
4. `mart_revenue_by_country`: Doanh thu chi tiết theo từng quốc gia.
5. `mart_revenue_by_category`: Phân tích cơ cấu ngành hàng và nhóm sản phẩm.
6. `mart_top_products_by_revenue`: Top 10 sản phẩm đóng góp doanh thu lớn nhất.
7. `mart_payment_analysis`: Hiệu quả doanh thu theo phương thức thanh toán.
8. `mart_shipping_analysis`: Tốc độ giao hàng trung bình và chi phí vận chuyển.
9. `mart_order_status_analysis`: Tỷ lệ hoàn thành đơn, đơn hủy và đơn hoàn trả.
10. `mart_monthly_revenue`: Xu hướng tăng trưởng doanh thu theo chuỗi thời gian tháng.
11. `mart_customer_segment_analysis`: Giá trị đơn hàng trung bình (AOV) theo phân khúc khách hàng.
12. `mart_rfm_customer_segmentation`: Phân khúc khách hàng rule-based RFM (*Champions, Loyal, At-Risk, Casual*) kèm `Rule_Version = rfm-v1`.
13. `mart_abc_product_analysis`: Phân tích Pareto ABC (*Class A: 80%, Class B: 15%, Class C: 5%*) dùng `Cumulative_Before_Percent` kèm `Rule_Version = abc-v1`.

---

## 12. Đối Soát & Bất Biến Dữ Liệu (Data Reconciliation Gate)

Trước khi dữ liệu được cấp phép phục vụ BI hoặc đồng bộ sang MongoDB, hệ thống thực thi bộ kiểm toán toán học tự động qua module `lakehouse.reconciliation`:

```powershell
python SourceCode\project_cli.py reconcile
```

Các bất biến được kiểm tra tự động:
1. **Bất biến Doanh thu:** $\sum \text{Silver.Revenue} = \sum \text{FactSales.Revenue} = \text{Mart Overview Total\_Revenue}$.
2. **Bảo toàn Số dòng:** $\text{Raw} = \text{Clean Valid} + \text{Quarantined} + \text{Duplicate}$.
3. **Tính Duy nhất của Fact Grain:** $\text{Count}(\text{FactSales}) = \text{CountDistinct}(\text{SalesKey})$.
4. **Toàn vẹn Khóa ngoại (Zero Orphan Keys):** Không tồn tại bất kỳ dòng Fact nào có khóa ngoại NULL.
5. **Toàn vẹn Thời gian SCD2:** Không có khoảng $[ValidFrom, ValidTo)$ nào bị đảo ngược và mỗi khách hàng chỉ có duy nhất 1 bản ghi `Is_Current = 1`.

---

## 13. Tầng Phục Vụ BI: Power BI qua Spark Thrift Server & Hive Metastore

- **Nguyên lý kiến trúc:** Thư mục lưu trữ thực tế là các bảng **Delta Lake** lưu trữ trên Local Storage hoặc HDFS. Hive Metastore chỉ đóng vai trò **Data Catalog** (quản lý metadata và schema bảng).
- **Kết nối trực tiếp:** Power BI kết nối qua chuẩn **ODBC/JDBC** tới Spark Thrift Server, truy vấn bảng Delta đã đăng ký:
  ```sql
  CREATE TABLE IF NOT EXISTS gold.fact_sales
  USING DELTA
  LOCATION '/ecommerce/gold/star_schema/fact_sales_delta'
  ```
- File thiết kế Dashboard hoàn chỉnh: [BI_BIG .pbix](BI_BIG%20.pbix).

---

## 14. Tầng Phục Vụ Ứng Dụng: MongoDB Secondary Serving Store

- **Serving Contract (Gold-Only):** MongoDB **chỉ nạp dữ liệu từ tầng Gold Certified** (`mart_overview`, `mart_top_products`, `mart_rfm_customer_segmentation`, `mart_order_summary`...), tuyệt đối không đọc từ tầng Silver thô để tránh nguy cơ lệch số liệu với BI.
- **Vai trò kỹ thuật:** MongoDB đóng vai trò là **Secondary Operational Document Store** phục vụ các ứng dụng web, microservices hoặc backend APIs với độ trễ thấp (low latency key-value / document lookup), **không phải nguồn chân lý phân tích (Single Source of Truth) thay thế kho dữ liệu**.

---

## 15. Delta Lake ACID Transactions & Thử Nghiệm Cô Lập (Isolated Demos)

Hệ thống tích hợp các bài kiểm thử tính năng nâng cao của Delta Lake:
- **Schema Enforcement:** Kiểm tra Delta Lake từ chối ghi dữ liệu sai schema. **Quy tắc an toàn:** Bài test được cô lập trên Delta table tạm thời (`/ecommerce/test/schema_enforcement_temp`), tuyệt đối không đụng vào bảng Silver/Gold production.
- **Time Travel & Versioning:** Đọc dữ liệu tại phiên bản quá khứ bằng `versionAsOf` và xem lịch sử commit log qua `history()`.

Chạy kịch bản thử nghiệm:
```powershell
python SourceCode\project_cli.py delta-demo
```

---

## 16. Vận Hành & Giám Sát Chất Lượng (Data Observability Table)

Mỗi lần pipeline thực thi, số liệu tổng quan về chất lượng được lưu tự động vào Delta table `gold_monitoring.pipeline_quality`:
- `run_id`: Mã nhận diện lần chạy.
- `batch_id`: Mã nhận diện batch nạp.
- `raw_rows`: Tổng số dòng thô đầu vào.
- `duplicate_rows`: Số dòng trùng lặp được loại bỏ.
- `valid_rows`: Số dòng hợp lệ nạp vào Silver.
- `rejected_rows`: Số dòng lỗi bị phân lập vào Quarantine.
- `reject_rate_percent`: Tỷ lệ lỗi vi phạm (%).
- `status`: Trạng thái xử lý (`SUCCESS` hoặc `QUARANTINE_PRESENT`).
- `recorded_at`: Thời gian ghi nhận số liệu.

---

## 17. Đánh Giá Hiệu Năng: Synthetic Single-Node Scaling Experiment

Báo cáo thử nghiệm khả năng mở rộng đơn nút (**Synthetic Single-Node Scaling Experiment**) với generator Spark-native từ 10K đến 1M dòng:

| Workload Scale | Raw Rows | Input Size (MB) | Runtime (seconds) | Throughput (rows/s) | Partitions | Fact Rows Generated | Zero Duplication |
|---|---|---|---|---|---|---|---|
| **10K** | 10,000 | ~1.85 MB | ~3.2 s | ~3,125 rows/s | 4 | 10,000 | ✅ 100% |
| **100K** | 100,000 | ~18.50 MB | ~12.4 s | ~8,064 rows/s | 8 | 100,000 | ✅ 100% |
| **1M** | 1,000,000 | ~185.00 MB | ~94.8 s | ~10,548 rows/s | 16 | 1,000,000 | ✅ 100% |

- **Generator:** `scripts/benchmark_scalability.py` sinh dữ liệu trực tiếp bằng `spark.range`, không chiếm dụng bộ nhớ RAM của driver Python.
- **Chi tiết phương pháp luận & manifest phần cứng:** Xem tại [docs/BENCHMARK.md](docs/BENCHMARK.md).

---

## 18. Chiến Lược Kiểm Thử Tự Động (Automated Testing Suite)

Bộ kiểm thử tự động toàn diện kiểm chứng từ Data Contract, Logic Analytics đến Bất biến Đối soát:

```powershell
# Chạy toàn bộ test suite
python -m pytest -v --basetemp=./scratch/pytest_temp
```

### Danh Mục Các Test Cases Trọng Yếu:
- `test_gold_revenue_reconciles_with_silver_and_marts`: Kiểm tra bất biến doanh thu 3 tầng.
- `test_fact_grain_uniqueness`: Kiểm tra tính duy nhất của Grain FactSales.
- `test_row_count_conservation`: Kiểm tra định luật bảo toàn số dòng $Raw = Valid + Invalid + Duplicate$.
- `test_data_reconciliation_gate_full_report`: Kiểm tra toàn bộ cổng đối soát và sinh file JSON.
- `test_dim_customer_scd2`: Kiểm tra trạng thái lặp lại $A \to B \to A$ tạo đúng 3 phiên bản.
- `test_scd2_only_one_current_record_per_customer`: Kiểm tra duy nhất 1 bản ghi hiện hành cho mỗi khách hàng.
- `test_quarantine_multi_reason_array`: Kiểm tra mảng đa lỗi vi phạm trong bảng Quarantine.
- `test_cli_accepts_all_public_commands`: Kiểm tra tính đầy đủ của bộ chỉ huy CLI.
- `test_cli_pipeline_subcommands_and_flags`: Kiểm tra chuyển tiếp cờ `--scd2`, `--local`, `--incremental`.

---

## 19. Giới Hạn Hiện Tại & Ranh Giới Kỹ Thuật (Production Boundary)

Nhằm duy trì tính trung thực cao nhất của một Portfolio Data Engineering, hệ thống ghi nhận rõ ràng các giới hạn kỹ thuật:
1. **Surrogate Key Policy:** Đang áp dụng *Deterministic Rebuild Key* bằng `row_number().over(Window.orderBy(...))`. Môi trường doanh nghiệp dài hạn nhiều năm cần chuyển sang dịch vụ cấp sequence hoặc stateful Delta lookup.
2. **Gold Refresh:** Incremental pipeline hiện thực hiện *Incremental Bronze/Silver MERGE kèm Deterministic Gold Refresh*, chưa phải incremental Fact MERGE cấp phân vùng.
3. **Môi Trường Benchmark:** Thử nghiệm scaling trên máy trạm cục bộ (Single-node), không đại diện cho cluster phân tán lớn.

---

## 20. Lộ Trình Chiến Lược (Strategic Roadmap) & CV Placement

### Bảng Phân Cấp Lộ Trình Kỹ Thuật:

| Mức Độ Ưu Tiên | Trạng Thái | Hạng Mục Công Việc Kỹ Thuật |
|---|---|---|
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Sửa cờ `--scd2` CLI chuyển tiếp chính xác xuyên suốt qua `PipelineConfig` |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Incremental pipeline bảo toàn mô hình hóa SCD Type 2 |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Định vị chính xác: Incremental Silver MERGE với Deterministic Gold Refresh |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Sửa giải thuật SCD2 lặp lại ($A \to B \to A$) bằng Event-Order Change Detection |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Xác lập `Order_Line_ID` cho Silver MERGE chống conflation nhiều dòng cùng SKU |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Cô lập bài test Schema Enforcement sang Delta table tạm thời độc lập |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Thống nhất Serving Contract: MongoDB chỉ phục vụ tầng Gold Certified |
| 🔴 **P0 (Critical)** | **ĐÃ HOÀN THÀNH** | Tái cấu trúc Benchmark generator Spark-native và hiệu chỉnh báo cáo 10K-1M |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Bronze Ingestion Metadata (`_ingested_at`, `_source_file`, `_source_hash`...) |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Bảng Ingestion Batches Registry kiểm soát Idempotency |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Bảng Quarantine ghi nhận danh sách mảng đa lỗi (`rejection_reasons`) |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Tách bạch số liệu trùng lặp vs vi phạm ($Raw = Valid + Invalid + Duplicate$) |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Xây dựng Data Reconciliation Gate (`globalcart reconcile`) xuất báo cáo JSON |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Nâng cấp CLI hỗ trợ `pipeline bootstrap`, `incremental`, `serve`, `reconcile` |
| 🟠 **P1 (High)** | **ĐÃ HOÀN THÀNH** | Gắn phiên bản chính sách phân tích (`rfm-v1`, `abc-v1`) vào Data Marts |
| 🟡 **P2 (Medium)** | *Đang nghiên cứu* | Incremental Fact MERGE & Partition-aware Data Mart refresh |
| 🟡 **P2 (Medium)** | *Đang nghiên cứu* | MongoDB bulk upsert đồng bộ tăng tiến |
| 🟡 **P2 (Medium)** | *Đang nghiên cứu* | Apache Airflow DAG điều phối task dependencies có cơ chế retry |
| 🟡 **P3 (Future)** | *Tương lai* | CDC Streaming Ingestion với Apache Kafka và Spark Structured Streaming |

---

### 💼 Định Vị Portfolio & CV Bullets

**Tiêu đề Dự án Khuyến nghị cho CV:**
> **GlobalCart Lakehouse Analytics Platform — Incremental PySpark/Delta Data Engineering & BI**

**Đoạn mô tả ngắn (1 câu):**
> *Built a PySpark/Delta Lake ecommerce analytics platform implementing Medallion architecture, data-quality quarantine, incremental upserts, Kimball star-schema modeling, SCD Type 2 customer history, 12 analytical marts, reconciliation tests, and Power BI serving.*

**Pipeline vắn tắt cho CV:**
$$\text{Raw Ecommerce Data} \longrightarrow \text{Bronze Delta} \longrightarrow \text{Data Quality \& Quarantine} \longrightarrow \text{Silver MERGE} \longrightarrow \text{Kimball Fact + SCD2 Dimensions} \longrightarrow \text{Gold Marts} \longrightarrow \text{Reconciliation Gate} \longrightarrow \text{Hive / Power BI / MongoDB}$$

**Các điểm đắt giá cho buổi phỏng vấn kỹ thuật:**
1. **Deduplication vs Quarantine Accounting:** Giải thích cách bảo toàn $Raw = Valid + Invalid + Duplicate$, chỉ ra vì sao nhiều pipeline mắc lỗi coi duplicate là quarantine.
2. **SCD Type 2 Event-Order Transition:** Phân tích giải thuật dùng `lag()` và cumulative island grouping để giải quyết lỗi kinh điển $A \to B \to A$ mà phép `groupBy().min()` truyền thống làm mất.
3. **Line-Item Identity trong Silver MERGE:** Phân tích tại sao `(Order_ID, Product_Name)` chưa đủ chắc nếu một đơn có nhiều dòng cùng SKU, và cách khắc phục bằng `Order_Line_ID`.
4. **Data Reconciliation Invariants:** Trình bày 5 kiểm tra bất biến toán học tự động bảo đảm số liệu tài chính trùng khớp 100% trước khi đưa vào Power BI.
5. **Cumulative_Before_Percent trong Pareto ABC:** Giải thích quyết định sử dụng tỷ trọng doanh thu tích lũy *trước* sản phẩm hiện tại để phân loại Class A/B/C hợp lý hơn.

---

## 🛡️ License & Tác Giả

Dự án được thiết kế, phát triển và duy trì bởi **Hà Minh Thông** dưới dạng **Portfolio Dự Án Kỹ Sư Dữ Liệu Chuyên Nghiệp**.
Mọi thảo luận kỹ thuật xin vui lòng liên hệ trực tiếp.
