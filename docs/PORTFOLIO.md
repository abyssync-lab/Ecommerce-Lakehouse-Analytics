# 💼 Portfolio Highlights & Technical Interview Guide

Tài liệu hướng dẫn phỏng vấn kỹ thuật và tóm tắt điểm nhấn dành cho Nhà tuyển dụng (Recruiters & Technical Hiring Managers) cho dự án **GlobalCart Lakehouse Analytics Platform**.

---

## 🎯 Định Vị Dự Án & Năng Lực Cốt Lõi

**Tiêu đề chuẩn cho Hồ sơ:**
> **GlobalCart Lakehouse Analytics Platform — Incremental PySpark/Delta Data Engineering & BI**

**Mô tả một câu (Elevator Pitch):**
> *Built a production-oriented PySpark/Delta Lake ecommerce analytics platform implementing Medallion architecture, quarantine-based data quality, incremental upserts, Kimball star-schema modeling, SCD Type 2 customer history, 12 analytical marts, reconciliation tests, and Power BI serving.*

**Pipeline Canonical tóm tắt:**
$$\text{Raw Ecommerce Data} \longrightarrow \text{Bronze Delta} \longrightarrow \text{Data Quality \& Quarantine} \longrightarrow \text{Silver MERGE} \longrightarrow \text{Kimball Fact + SCD2 Dimensions} \longrightarrow \text{Gold Marts} \longrightarrow \text{Reconciliation Gate} \longrightarrow \text{Hive / Power BI / MongoDB}$$

---

## 📝 CV Bullet Points (Đưa Vào CV)

### Tiếng Việt
- **Xây dựng Nền tảng Lakehouse Thương Mại Điện Tử** theo kiến trúc Medallion (Bronze - Silver - Gold) bằng **PySpark, Delta Lake và HDFS**; tích hợp **Data Quality Gate** tự động phân lập bản ghi lỗi vào **Quarantine Table** lưu danh sách đa lỗi (`rejection_reasons`).
- **Hiện thực hóa Quy Trình Xử Lý Tăng Tiến (Incremental Pipeline)**: Tách bạch chế độ Bootstrap (Full Refresh) và Incremental (Silver MERGE theo khóa duy nhất `Order_Line_ID` kèm Deterministic Gold Refresh); đảm bảo tính Idempotency qua Ingestion Batch Registry.
- **Thiết kế Kho Dữ Liệu Kimball Star Schema** (1 Fact, 7 Dimensions) với **SCD Type 2** áp dụng giải thuật Event-Ordered Change Detection xử lý chính xác chuỗi trạng thái $A \to B \to A$ và bảo đảm đúng 1 bản ghi hiện hành.
- **Xây dựng Data Reconciliation Gate**: Tự động kiểm toán 5 bất biến toán học (Bảo toàn doanh thu 3 tầng, bảo toàn số dòng $Raw = Valid + Invalid + Duplicate$, tính duy nhất của Grain, và toàn vẹn khóa ngoại) trước khi chứng nhận phục vụ BI.
- **Đồng bộ Tầng Phục Vụ (Serving Layer)**: Đăng ký bảng Delta vào Hive Metastore để phục vụ **Power BI** qua Spark Thrift Server (ODBC/DirectQuery) và đồng bộ sang **MongoDB** theo Gold-only Serving Contract cho ứng dụng vận hành.
- **Thực thi Scalability Benchmark**: Xây dựng bộ sinh dữ liệu Spark-native đo lường thông lượng mở rộng từ 10K đến 1M dòng (đạt throughput ~10.5K dòng/s trên máy trạm đơn nút, lưu manifest JSON chi tiết).

### English
- **Architected a Production-Oriented Ecommerce Lakehouse** (Bronze, Silver, Gold) using **PySpark, Delta Lake, and HDFS**, featuring automated Data Quality Gates and a multi-error **Delta Quarantine Table** preserving detailed rejection reasons.
- **Implemented Incremental Ingestion & Silver MERGE**: Engineered a dual-mode pipeline (Bootstrap Full Refresh vs Incremental Micro-batch) with line-item grain MERGE (`Order_Line_ID`) and an Ingestion Batch Registry ensuring strict idempotency.
- **Engineered Kimball Star Schema with SCD Type 2**: Developed event-ordered change detection with window lag functions to accurately model customer state transitions ($A \to B \to A$) with temporal non-overlapping $[ValidFrom, ValidTo)$ intervals.
- **Built an Automated Data Reconciliation Gate**: Formulated automated invariant audits verifying 3-tier financial consistency ($\sum \text{Silver} = \sum \text{Fact} = \text{Overview Mart}$), row conservation ($Raw = Valid + Invalid + Duplicate$), and foreign key completeness.
- **Unified Dual Serving Layer**: Configured Hive Metastore cataloging for **Power BI** via Spark Thrift Server (ODBC/DirectQuery) and enforced a Gold-only serving contract for **MongoDB** operational document stores.
- **Conducted Scalability Benchmark**: Developed a Spark-native synthetic dataset generator evaluating throughput from 10K to 1M rows (~10,500 rows/s single-node throughput, automated JSON environment manifests).

---

## 🗣️ 5 Điểm Nhấn Đắt Giá Trong Phỏng Vấn Kỹ Thuật (Technical Deep-Dive)

### 1. Kế Toán Trùng Lặp vs Vi Phạm Chất Lượng (Duplicate vs Quarantine Accounting)
> **Câu hỏi:** *Làm thế nào bạn phân biệt giữa dữ liệu duplicate và dữ liệu không hợp lệ?*
> **Trả lời:** Nhiều pipeline mắc lỗi tính `raw_count - clean_count` và gọi toàn bộ là quarantine/reject. Trong dự án này, tôi tách bạch hai bước: lọc duplicate ở tầng nguồn bằng `dropDuplicates()`, ghi nhận `duplicate_count`, sau đó mới chạy bộ quy tắc Data Contract trên dữ liệu đã deduplicate để xác định `invalid_count`. Hệ thống luôn bảo đảm định luật bảo toàn tuyệt đối:
> $$\text{Raw Count} = \text{Clean Valid Count} + \text{Quarantined Count} + \text{Duplicate Count}$$

### 2. Thuật Toán SCD Type 2 Xử Lý State Flip ($A \to B \to A$)
> **Câu hỏi:** *Tại sao không dùng `groupBy(Customer_ID, attributes).agg(min(Order_Date))` cho SCD Type 2?*
> **Trả lời:** Cách làm `groupBy` đơn thuần sẽ gộp tất cả các lần xuất hiện cùng một trạng thái. Nếu khách hàng là *Consumer* vào tháng 1, chuyển sang *Corporate* vào tháng 4, rồi quay lại *Consumer* vào tháng 7, `groupBy` sẽ gộp tháng 1 và tháng 7 thành 1 phiên bản, làm mất giai đoạn giữa. Tôi đã sử dụng giải thuật **Event-Ordered Change Detection**: sắp xếp giao dịch theo ngày, dùng `lag()` để phát hiện thay đổi thuộc tính, cộng dồn tạo `change_group` (Island Grouping), từ đó sinh chính xác 3 phiên bản riêng biệt với các khoảng $[ValidFrom, ValidTo)$ liên tục và duy nhất 1 bản ghi `Is_Current = 1`.

### 3. Line-Item Identity Trong Silver MERGE
> **Câu hỏi:** *Khóa MERGE của bạn tại tầng Silver là gì và tại sao không chỉ dùng `(Order_ID, Product_Name)`?*
> **Trả lời:** Trong bán lẻ, một đơn hàng hoàn toàn có thể chứa nhiều dòng có cùng một SKU (ví dụ: dòng 1 là sản phẩm chính, dòng 2 là quà tặng kèm hoặc cùng mã nhưng mua giá ưu đãi riêng). Nếu chỉ MERGE theo `(Order_ID, Product_Name)`, Delta Lake sẽ gặp rủi ro conflation. Tôi đã chuẩn hóa Data Contract với trường `Order_Line_ID` (kết hợp `Order_ID` và line sequence ổn định), bảo đảm phép MERGE luôn chuẩn xác 100% ở cấp dòng.

### 4. Bất Biến Kiểm Toán Dữ Liệu (Data Reconciliation Invariants)
> **Câu hỏi:** *Làm sao bạn chứng minh được dữ liệu trên Power BI và MongoDB không bị lệch so với dữ liệu nguồn?*
> **Trả lời:** Tôi xây dựng mô-đun `lakehouse.reconciliation` đóng vai trò là Certification Gate. Trước khi dữ liệu được cấp phép phục vụ, hệ thống chạy 5 kiểm toán tự động: kiểm tra tổng doanh thu Silver = FactSales = Mart Overview (dung sai $< 0.01$), kiểm tra không có khóa ngoại NULL, kiểm tra tính duy nhất của Grain, và kiểm tra tính toàn vẹn SCD2. Nếu kiểm toán không đạt `PASS`, hệ thống sẽ từ chối cập nhật serving pointer.

### 5. Lựa Chọn Phân Hạng Pareto ABC Bằng `Cumulative_Before_Percent`
> **Câu hỏi:** *Tại sao bạn dùng doanh thu tích lũy trước sản phẩm thay vì sau sản phẩm trong phân loại ABC?*
> **Trả lời:** Khi một sản phẩm có doanh thu lớn làm cho tỷ trọng tích lũy vượt qua ngưỡng 80% (ví dụ từ 75% lên 85%), nếu kiểm tra cumulative-after, sản phẩm đó sẽ bị đẩy xuống Class B. Sử dụng `Cumulative_Before_Percent` giúp giữ sản phẩm đó ở Class A vì khi bắt đầu xét đến nó, danh mục vẫn nằm trong nhóm 80% doanh số đầu bảng, phản ánh đúng hơn bản chất đóng góp giá trị của sản phẩm.

---

## 🔗 Liên Kết Tài Liệu Kỹ Thuật
- 📘 [Kiến Trúc & Quyết Định Kỹ Thuật (docs/ARCHITECTURE.md)](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/ARCHITECTURE.md)
- 📖 [Data Contracts & Từ Điển Chỉ Số (docs/DATA_DICTIONARY.md)](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/DATA_DICTIONARY.md)
- 📈 [Báo Cáo Benchmark Khả Năng Mở Rộng (docs/BENCHMARK.md)](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/BENCHMARK.md)
- 📊 [Báo Cáo Phân Tích Kinh Doanh Tự Động (docs/BUSINESS_INSIGHTS.md)](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/BUSINESS_INSIGHTS.md)
