# Kiến trúc Nền tảng GlobalCart Lakehouse Analytics

## 1. Tầm Nhìn & Mục Tiêu Thiết Kế

Hệ thống được định vị là **Production-Oriented Ecommerce Lakehouse Platform**, mô phỏng các tiêu chuẩn kỹ thuật dữ liệu cấp doanh nghiệp:
- **Data Contracts:** Ràng buộc schema và luật nghiệp vụ độc lập ngay tại cửa ngõ tiếp nhận.
- **Traceability & Lineage:** Tầng Bronze bất biến lưu vết siêu dữ liệu nạp và mã băm nguồn gốc.
- **Data Quality Gate & Quarantine:** Phân lập bản ghi vi phạm vào bảng lỗi đa nguyên nhân, bảo đảm không làm gián đoạn pipeline.
- **Idempotency & Execution Modes:** Hỗ trợ cả hai chế độ **Bootstrap (Full Refresh)** và **Incremental Processing (Silver MERGE + Deterministic Gold Refresh)**.
- **Dimensional History (Kimball & SCD Type 2):** Xử lý chính xác chuyển đổi trạng thái khách hàng ($A \to B \to A$) không gộp version.
- **Data Reconciliation Gate:** Bộ kiểm toán bất biến tài chính và tính duy nhất của Grain trước khi chứng nhận dữ liệu phục vụ BI.
- **Single Source of Truth:** Tầng Serving (Power BI & MongoDB) được kết nối duy nhất từ tầng Gold đã chứng nhận.

---

## 2. Sơ Đồ Kiến Trúc Toàn Diện (Control Plane & Data Plane)

```
                         CONTROL PLANE
Pipeline Config ──> Batch Registry ──> Data Contracts ──> Run Metadata / Audit
                               │
                               ▼
                         DATA PLANE

1. RAW INGESTION & DATA CONTRACTS
   - contracts/ecommerce_order.yaml
   - Ingestion Control: Batch ID, Ingested Timestamp, Source File Hash, Line Key
                               │
                               ▼
2. BRONZE LAYER (IMMUTABLE RAW DELTA)
   - Lưu trữ nguyên bản trường dữ liệu nguồn
   - Metadata: _batch_id, _ingested_at, _source_file, _source_hash, _record_hash, _pipeline_version
   - Transaction Log: _delta_log (ACID & Time Travel)
                               │
                               ▼
3. SILVER LAYER (DATA QUALITY GATE & QUARANTINE)
   - Ép kiểu chuẩn, chuẩn hóa chuỗi, tính toán measures phái sinh
   - Phân tách kế toán: Raw = Valid + Invalid + Duplicate
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
   - Business Line Key: Order_ID + Order_Line_ID
   - Delta MERGE INTO (whenMatchedUpdateAll, whenNotMatchedInsertAll)
   - Idempotent execution
               │
               ▼
5. GOLD CORE MODEL (KIMBALL STAR SCHEMA)
   - FactSales (Grain: 1 order line, Degenerate Dim: Order_ID)
   - DimCustomer (SCD Type 2 Event Change Detection [ValidFrom, ValidTo))
   - DimProduct, DimDate, DimLocation, DimPayment, DimShipping, DimOrderStatus
               │
               ▼
6. GOLD ANALYTICAL MARTS
   - 12 Data Marts tổng hợp + mart_order_summary (Order grain)
   - RFM Customer Segmentation (Rule version: rfm-v1)
   - Pareto ABC Product Analysis (Rule version: abc-v1)
               │
               ▼
7. DATA RECONCILIATION GATE
   - Bất biến Doanh thu: Sum(Silver) = Sum(Fact) = Overview Mart
   - Bảo toàn Số dòng: Raw = Valid + Invalid + Duplicate
   - Fact Grain Uniqueness: Count(Fact) = Distinct(SalesKey)
   - Toàn vẹn Khóa ngoại: Không tồn tại Orphan Keys
   - SCD2 Integrity: Đúng 1 Is_Current = 1 cho mỗi khách hàng
               │
               ▼
8. SERVING LAYER
   ├── Hive Metastore / Spark Thrift Server ──> Power BI (ODBC/DirectQuery)
   └── Certified Gold Marts ──────────────────> MongoDB BSON Collections
               │
               ▼
9. OPERATIONS & OBSERVABILITY
   - pipeline_quality Delta table (raw, dup, valid, quarantine rows, reject rate %)
   - Audit run reports & System Doctor
```

---

## 3. Hai Chế Độ Vận Hành (Execution Modes)

Hệ thống phân tách rành mạch hai cơ chế thực thi:

| Tiêu Chí So Sánh | 1. Bootstrap Mode (Full Refresh) | 2. Incremental Mode (Micro-batch MERGE) |
|---|---|---|
| **Mục đích** | Khởi tạo kho từ đầu, nạp lịch sử hoặc chạy lại toàn bộ | Xử lý các lô dữ liệu định kỳ với tài nguyên và thời gian tối ưu |
| **Bronze Layer** | Ghi đè (`mode="overwrite"`) | Ghi tiếp (`mode="append"`) kèm `_batch_id` và `_source_hash` |
| **Silver Layer** | Ép kiểu, làm sạch, ghi đè Silver Delta | Làm sạch batch mới, **Delta MERGE INTO** theo `(Order_ID, Order_Line_ID)` |
| **Quarantine** | Ghi đè / append bảng reject | Ghi tiếp (`mode="append"`) các bản ghi lỗi của batch mới |
| **Gold Core** | Rebuild toàn bộ 7 Dimensions & FactSales | Rebuild xác định (*Deterministic Gold Refresh*) bảo toàn SCD2 |
| **Gold Marts** | Tính toán lại toàn bộ 12 Marts | Làm mới các Marts từ bảng Silver/Fact đã cập nhật |
| **Lệnh CLI** | `globalcart pipeline bootstrap --scd2` | `globalcart pipeline incremental --input batch.csv --batch-id B01` |

---

## 4. Quyết Định Kỹ Thuật & Đánh Đổi Kiến Trúc

| Quyết Định | Cơ Sở Kỹ Thuật (Rationale) | Đánh Đổi & Hướng Enterprise (Trade-off) |
|---|---|---|
| **Delta Lake thay vì Parquet thuần** | Hỗ trợ ACID transactions, Time Travel, Schema Enforcement và Delta MERGE INTO | Yêu cầu runtime dependencies `delta-spark` |
| **Phân tách Duplicate vs Quarantine** | Đảm bảo định luật bảo toàn $Raw = Valid + Invalid + Duplicate$, không quy kết duplicate là vi phạm nghiệp vụ | Cần thêm 1 bước tính toán count `raw_df.dropDuplicates()` |
| **Multi-Error Quarantine Array** | Lưu vết toàn bộ danh sách vi phạm (`rejection_reasons: array<string>`) giúp đội vận hành sửa lỗi toàn diện | Cấu trúc mảng cần explode khi phân tích chi tiết trong BI |
| **Grain FactSales: 1 Order Line** | Grain chuẩn xác nhất trong bán lẻ, cho phép slice-and-dice theo mọi chiều sản phẩm | Cần cột `Order_Line_ID` chống trùng lặp dòng cùng SKU trong một đơn |
| **Khóa MERGE: Order_ID + Order_Line_ID** | Tránh conflation khi một đơn hàng có nhiều dòng sản phẩm cùng tên | Yêu cầu sinh `LineKey` xác định từ nguồn nếu file CSV thô thiếu |
| **SCD Type 2 Event-Change Detection** | Dùng `lag()` và cumulative island grouping để xử lý chính xác khách hàng lặp trạng thái ($A \to B \to A$) | Tốn thêm window evaluation pass so với `groupBy().min(Order_Date)` |
| **Deterministic Rebuild Key** | Dùng `row_number().over(orderBy(...))` để bảo đảm 100% tái lập khóa khi re-run pipeline | Với enterprise data warehouse dài hạn nhiều năm, cần chuyển sang *Stateful Delta Lookup + Monotonic Sequence* |
| **Gold-only Serving Contract cho MongoDB** | Bảo đảm MongoDB và Power BI cùng chung một Single Source of Truth | MongoDB là secondary operational document store, không thay thế analytical DW |
| **Độc Lập Quy Tắc Analytics (`analytics_rules.py`)** | Single source of truth cho luật RFM và Pareto ABC, dùng chung cho cả Spark và Pandas | Cần duy trì Contract Tests bảo đảm tính tương đồng giữa 2 engine |

---

## 5. Data Reconciliation Gate (Cổng Chứng Nhận Dữ Liệu)

Trước khi dữ liệu tầng Gold được công bố cho Power BI hoặc đồng bộ sang MongoDB, hệ thống thực thi 5 kiểm tra đối soát toán học tự động:

1. **Bất biến Doanh thu:**
   $$\left| \sum \text{Silver.Revenue} - \sum \text{FactSales.Revenue} \right| < 0.01$$
   $$\left| \sum \text{FactSales.Revenue} - \text{Mart Overview Total\_Revenue} \right| < 0.01$$
2. **Bảo toàn Số lượng Dòng:**
   $$\text{Raw Count} = \text{Clean Valid Count} + \text{Quarantined Count} + \text{Duplicate Count}$$
3. **Tính Duy nhất của Fact Grain:**
   $$\text{Count}(\text{FactSales}) = \text{CountDistinct}(\text{SalesKey})$$
4. **Toàn vẹn Khóa ngoại (Zero Orphan Keys):**
   Mọi bản ghi FactSales đều phải liên kết thành công với Dimension tương ứng (`CustomerKey`, `ProductKey`, `DateKey` không NULL).
5. **Toàn vẹn Thời gian SCD2:**
   Mọi khoảng hiệu lực phải thỏa mãn $ValidFrom < ValidTo$ và mỗi khách hàng chỉ có đúng $1$ bản ghi hiện hành ($Is\_Current = 1$).

---

## 6. Ranh Giới & Hạn Chế Hiện Tại (Production Boundary)

- **Môi trường Benchmark:** Kiểm thử Single-node Scaling trên máy trạm cục bộ (10K đến 1M dòng), không đại diện cho cụm phân tán nhiều nodes.
- **Surrogate Keys:** Đang vận hành theo mô hình *Deterministic Rebuild Key* thích hợp cho batch refresh portfolio. Môi trường continuous append cần triển khai sequence lookup service.
- **Serving:** Power BI kết nối qua Spark Thrift Server local. Môi trường cloud khuyến nghị Azure Synapse Serverless SQL hoặc Databricks SQL Warehouse.
