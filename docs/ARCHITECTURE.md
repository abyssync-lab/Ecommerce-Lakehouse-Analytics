# Kiến trúc Nền tảng GlobalCart Lakehouse Analytics

## 1. Tầm Nhìn & Mục Tiêu Thiết Kế

Hệ thống được định vị là **Production-Oriented Ecommerce Lakehouse Platform**, mô phỏng các tiêu chuẩn kỹ thuật dữ liệu cấp doanh nghiệp:
- **Executable Data Contracts:** Ràng buộc schema và luật nghiệp vụ thực thi động qua `ContractLoader` (`contracts/ecommerce_order.yaml`) là nguồn chân lý duy nhất.
- **Batch Registry & Real Idempotency:** Sổ cái kiểm toán theo dõi trạng thái `RECEIVED -> PROCESSING -> SUCCESS / FAILED` và ngăn chặn nạp trùng file dựa trên mã băm SHA-256.
- **Traceability & Lineage:** Tầng Bronze bất biến lưu vết siêu dữ liệu nạp và mã băm JSON struct `_record_hash`.
- **Data Quality Gate & Quarantine:** Phân lập bản ghi vi phạm vào bảng lỗi đa nguyên nhân, bảo đảm không làm gián đoạn pipeline.
- **Stable Order-Line Business Identity:** Đảm bảo tính duy nhất và ổn định của Grain mức dòng sản phẩm giữa các micro-batch, loại bỏ hoàn toàn nguy cơ ghi đè dòng chéo khi MERGE.
- **Incremental Batch Upsert:** Delta MERGE INTO theo cặp khóa `(Order_ID, Order_Line_ID)` thay vì overclaim full CDC khi chưa có stream offset hay change stream log.
- **Dimensional History (Kimball & SCD Type 2):** Xử lý chính xác chuyển đổi trạng thái khách hàng ($A \to B \to A$) không gộp version.
- **Canonical Semantic Layer:** Bảng `gold_sales_enriched` kết nối Fact và Dimensions làm nền tảng tính toán đồng nhất cho toàn bộ Marts.
- **Mandatory Reconciliation Gate:** Bộ kiểm toán bất biến tài chính và tính duy nhất của Grain nằm trực tiếp trong luồng `run_pipeline()`, ngăn chặn xuất bản nếu chưa đạt chứng nhận (`PASS`).
- **Atomic MongoDB Serving:** Cơ chế tráo đổi collection nguyên tử (Staging Swap) loại bỏ hoàn toàn rủi ro partial reads khi phục vụ ứng dụng.

---

## 2. Sơ Đồ Kiến Trúc Toàn Diện (Control Plane & Data Plane)

```
                    SOURCE SYSTEMS (Historical & Micro-batch CSV)
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 1. LANDING & INGESTION CONTROL (CONTROL PLANE)                         │
│ - BatchRegistry: compute SHA-256 source_hash, lookup status            │
│   ├─ already SUCCESS ──> SKIP (Replay-safe / Idempotent)               │
│   └─ NEW / RETRY     ──> Register PROCESSING                           │
│ - Executable Contract: contracts/ecommerce_order.yaml via ContractLoader│
│ - Explicit String Schema (tránh inferSchema=True gây drift)            │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 2. BRONZE LAYER (IMMUTABLE RAW DELTA)                                  │
│ - Lưu trữ nguyên bản trường dữ liệu thô dạng text                      │
│ - Metadata: _batch_id, _ingested_at, _source_file, _source_hash        │
│ - Deterministic Fingerprint: _record_hash = sha256(to_json(struct))    │
│ - Append-only trong vận hành; Reset khi chạy Bootstrap                 │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 3. SILVER QUALITY GATE & CANONICAL CASTING                             │
│ - Ép kiểu chuẩn mực theo Data Contract                                 │
│ - Phân tách kế toán bảo toàn: Raw = Valid + Invalid + Duplicate        │
│                │                                                       │
│         ┌──────┴──────┐                                                │
│         ▼             ▼                                                │
│      VALID         QUARANTINE TABLE                                    │
│         │          - rejection_reasons: array<string>                  │
│         │          - _batch_id, rejected_at                            │
└─────────┼──────────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 4. SILVER INCREMENTAL UPSERT (STABLE BUSINESS GRAIN)                   │
│ - Stable Line Key: Order_ID + Order_Line_ID                            │
│ - Fallback: Content-derived deterministic fingerprint                  │
│ - Delta MERGE INTO (whenMatchedUpdateAll, whenNotMatchedInsertAll)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 5. GOLD CORE MODEL (KIMBALL STAR SCHEMA)                               │
│ - FactSales (Grain: 1 order line, Degenerate Dim: Order_ID)            │
│ - DimCustomer (SCD Type 2 Event Change Detection [ValidFrom, ValidTo)) │
│ - DimProduct, DimDate, DimLocation, DimPayment, DimShipping, DimStatus  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 6. CANONICAL GOLD SEMANTIC BASE & MARTS                                │
│ - gold_sales_enriched: Denormalized canonical dataset                  │
│ - 12 Certified Data Marts (Overview, Monthly, Regional, RFM, ABC...)   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 7. MANDATORY DATA RECONCILIATION GATE                                  │
│ - Bất biến Doanh thu: Sum(Silver) = Sum(Fact) = Overview Mart          │
│ - Bảo toàn Số dòng: Raw = Valid + Invalid + Duplicate                  │
│ - Fact Grain Uniqueness: Count(Fact) = Distinct(SalesKey)              │
│ - Toàn vẹn Khóa ngoại: Zero Orphan Keys                                │
│ - SCD2 Integrity: Không chồng lấn, đúng 1 Is_Current = 1 / Customer    │
│                │                                                       │
│         ┌──────┴──────┐                                                │
│         ▼ FAIL        ▼ PASS                                           │
│   Abort Pipeline   Mark SUCCESS in Registry                            │
│   Block Publish    Issue PipelineRunResult                             │
└───────────────────────┬────────────────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 8. CERTIFIED PUBLICATION GATE                                          │
│ ├── Spark Thrift Server ─────────> Power BI (ODBC / DirectQuery)       │
│ └── Operational Serving Store ───> MongoDB (Atomic Staging Swap)       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Hai Chế Độ Vận Hành (Execution Modes)

Hệ thống phân tách rành mạch hai cơ chế thực thi:

| Tiêu Chí So Sánh | 1. Bootstrap Mode (Destructive Reset) | 2. Incremental Mode (Micro-batch MERGE) |
|---|---|---|
| **Mục đích** | Khởi tạo kho từ đầu, nạp lại lịch sử hoặc reset môi trường demo | Xử lý các lô dữ liệu định kỳ với tài nguyên và thời gian tối ưu |
| **Batch Registry** | Khởi tạo sổ cái, ghi đè trạng thái | Kiểm tra `source_hash`: nếu đã `SUCCESS` thì **SKIP** ngay lập tức |
| **Bronze Layer** | Ghi đè (`mode="overwrite"`) | Ghi tiếp append-only (`mode="append"`) kèm `_source_hash` |
| **Silver Layer** | Ép kiểu, làm sạch, ghi đè Silver Delta | Làm sạch batch mới, **Delta MERGE INTO** theo `(Order_ID, Order_Line_ID)` |
| **Grain Line Key** | Deterministic fingerprint | Bảo toàn stable line identity giữa các micro-batches |
| **Quarantine** | Ghi đè bảng reject | Ghi tiếp (`mode="append"`) các bản ghi lỗi của batch mới |
| **Gold Core** | Rebuild toàn bộ 7 Dimensions & FactSales | Rebuild xác định (*Deterministic Gold Refresh*) bảo toàn SCD2 |
| **Reconciliation** | Kiểm toán toàn bộ 5 bất biến bắt buộc | Kiểm toán tích lũy trên toàn bộ dữ liệu sau MERGE |
| **Lệnh CLI** | `globalcart pipeline reset-bootstrap --scd2` | `globalcart pipeline incremental --input batch.csv --batch-id B01` |

---

## 4. Quyết Định Kỹ Thuật & Đánh Đổi Kiến Trúc

| Quyết Định | Cơ Sở Kỹ Thuật (Rationale) | Đánh Đổi & Hướng Enterprise (Trade-off) |
|---|---|---|
| **Executable YAML Contract (`ContractLoader`)** | Xóa bỏ code smell trùng lặp quy tắc giữa yaml, validation và transformation | Cần duy trì schema mapping giữa YAML và PySpark Types |
| **BatchRegistry Idempotency Guard** | Ngăn chặn nạp trùng file gây nhân đôi số liệu khi scheduler hoặc người dùng replay batch | Cần quản lý bảng audit log Delta (`ingestion_batches_delta`) |
| **Stable Order_Line_ID Grain** | Sử dụng content-derived deterministic line key thay vì `row_number()` động để tránh xung đột MERGE | Với hệ thống CDC thực tế, cần yêu cầu upstream cung cấp số thứ tự dòng đơn hàng |
| **Delta Lake thay vì Parquet thuần** | Hỗ trợ ACID transactions, Time Travel, Schema Enforcement và Delta MERGE INTO | Yêu cầu runtime dependencies `delta-spark` |
| **Phân tách Duplicate vs Quarantine** | Đảm bảo định luật bảo toàn $Raw = Valid + Invalid + Duplicate$, không quy kết duplicate là vi phạm nghiệp vụ | Cần thêm 1 bước tính toán count `raw_df.dropDuplicates()` |
| **Multi-Error Quarantine Array** | Lưu vết toàn bộ danh sách vi phạm (`rejection_reasons: array<string>`) giúp đội vận hành sửa lỗi toàn diện | Cấu trúc mảng cần explode khi phân tích chi tiết trong BI |
| **Canonical Gold Semantic Base (`gold_sales_enriched`)** | Đảm bảo FactSales và Gold Marts chia sẻ cùng một dòng dữ liệu kế thừa từ Kimball Core | Tốn thêm một bước denormalize bảng trước khi tổng hợp Marts |
| **Mandatory Reconciliation Pipeline Gate** | Pipeline không bao giờ kết thúc âm thầm khi có sai lệch số liệu; tự động dừng và chặn xuất bản | Tăng thời gian chạy pipeline thêm vài giây để hoàn thành bộ kiểm tra |
| **Atomic MongoDB Publication** | Staging collection + verification + atomic rename `dropTarget=True` loại bỏ partial read windows | Cần dung lượng bộ nhớ đệm cho collection staging trong lúc nạp |
| **Legacy SQL Server Isolation** | Di dời `EcommerceDW.sql` về `legacy/sqlserver/` để khẳng định Delta Lakehouse là Single Source of Truth | Giữ lại script T-SQL phục vụ đối sánh kiến trúc warehouse truyền thống |

---

## 5. Data Reconciliation Gate (Cổng Chứng Nhận Dữ Liệu)

Trước khi dữ liệu tầng Gold được công bố cho Power BI hoặc đồng bộ sang MongoDB, hệ thống thực thi 5 kiểm tra đối soát toán học tự động trực tiếp trong `run_pipeline()`:

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
