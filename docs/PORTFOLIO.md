# 💼 Portfolio Highlights & Recruiter Guide - GlobalCart Intelligence

Tài liệu dành cho Nhà tuyển dụng (Recruiters & Technical Hiring Managers), tóm tắt các điểm sáng kỹ thuật, câu chuyện STAR phỏng vấn và CV Bullets song ngữ (Việt - Anh).

---

## 🎯 Tổng Quan Năng Lực Dự Án (Core Competencies)

`Data Ingestion` · `Incremental ETL Pipeline` · `Medallion Lakehouse Design` · `Kimball Dimensional Modeling (SCD Type 2)` · `Data Quality & Quarantine Control` · `Rule-based Analytics (RFM & Pareto ABC)` · `NoSQL Serving (MongoDB)` · `BI Integration (Power BI ODBC)` · `Unit & Contract Testing (Pytest)` · `Scalability Benchmark` · `CLI Tooling`

---

## 📝 CV Bullet Points (Đưa Vào CV)

### Tiếng Việt
- **Xây dựng hệ thống Medallion Data Lakehouse** (Bronze - Silver - Gold) xử lý dữ liệu giao dịch thương mại điện tử bằng **PySpark, Delta Lake và HDFS**; tích hợp **Data Quality Gate** tự động phân lập bản ghi lỗi vào **Quarantine Table Delta**.
- **Thiết kế mô hình sao Kimball** (1 Fact, 7 Dimensions) hỗ trợ **SCD Type 2 (Slowly Changing Dimensions)** và **12 Data Marts chuyên sâu** (**RFM Customer Segmentation** & **Pareto ABC Analysis**) cho Power BI và MongoDB.
- **Phát triển Incremental Pipeline & Scalability Benchmark**: Xử lý nạp dữ liệu vi mô với **Delta MERGE INTO**; đo lường hiệu năng mở rộng tuyến tính từ 10K đến 1M+ bản ghi (**Benchmark Report: docs/BENCHMARK.md**).
- **Đảm bảo chất lượng & bảo trì**: Cấu hình môi trường tập trung, hỗ trợ Local/HDFS Hybrid Mode, kiểm thử hợp đồng (Contract Testing) đảm bảo 100% nhất quán giữa Pandas và Spark với bộ Pytest tự động.

### English
- **Engineered an End-to-End Medallion Data Lakehouse** (Bronze, Silver, Gold) using **PySpark, Delta Lake, and HDFS** featuring automated Data Quality Gates and a **Delta Quarantine Table** for invalid records.
- **Implemented a Kimball Star Schema** (1 Fact, 7 Dimensions) with **SCD Type 2** support and **12 Data Marts** including **RFM Customer Segmentation** and **Pareto ABC Product Analysis** serving Power BI and MongoDB.
- **Built Incremental Ingestion & Scalability Benchmarks**: Enabled micro-batch processing with **Delta MERGE INTO** and validated linear performance scaling from 10K up to 1M+ records (**Benchmark Report: docs/BENCHMARK.md**).
- **Ensured High Code Quality & Maintainability**: Developed modular package architecture, Local/HDFS hybrid execution modes, centralized settings management, and automated Pytest contract test suites.

---

## 🗣️ Câu Chuyện Phỏng Vấn Theo Mô Hình STAR (STAR Story)

### 📌 Situational Context (Bối cảnh):
Một dự án thương mại điện tử lớn có nhiều nguồn dữ liệu thô, báo cáo BI thường bị chậm do tính toán lại toàn bộ dữ liệu chi tiết, đồng thời có sự lệch số liệu giữa bộ phận Marketing (RFM) và Kho vận (Pareto ABC).

### 🎯 Task (Nhiệm vụ):
Xây dựng pipeline ETL phân tán chuẩn hóa dữ liệu từ thô đến báo cáo, bảo đảm chất lượng dữ liệu khắt khe, đồng nhất 100% quy tắc phân tích và phục vụ dữ liệu ổn định cho cả hệ sinh thái BI (Power BI) lẫn ứng dụng NoSQL (MongoDB).

### ⚙️ Action (Hành động):
1. Thiết kế **Medallion Lakehouse** 3 tầng với **Delta Lake**: Bronze (raw copy), Silver (cleaned & enriched, quarantine rows), Gold (Kimball Star Schema & 12 Data Marts).
2. Xây dựng **Data Quality Gate / Data Contract** để phát hiện dữ liệu lỗi và lưu vào **Quarantine Table** Delta kèm lý do vi phạm (`rejection_reason`) và thời gian (`rejected_at`).
3. Khắc phục nguy cơ duplicate Fact bằng cách áp dụng **Deduplication 1 Customer_ID = 1 Record** và hỗ trợ **SCD Type 2**.
4. Chuẩn hóa quy tắc phân tích RFM & ABC vào engine tập trung (`analytics_rules.py`), kiểm chứng bằng **Contract Tests** tự động giữa Pandas và PySpark.
5. Triển khai **Incremental Pipeline** sử dụng Delta `MERGE INTO` và thực thi **Synthetic Scalability Benchmark** (10K - 1M rows).

### 🏆 Result (Kết quả):
- Loại bỏ hoàn toàn rủi ro trùng lặp số liệu Fact sales.
- Bảo đảm 100% đồng nhất phân hạng RFM và ABC giữa các phòng ban.
- Benchmark chứng minh tốc độ xử lý mở rộng tuyến tính và sẵn sàng phục vụ báo cáo Power BI realtime qua Hive Thrift Server.

---

## 🔗 Liên Kết Tài Liệu Liên Quan
- 📘 [Giải thích Kiến trúc & Quyết định Kỹ thuật](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/ARCHITECTURE.md)
- 📖 [Data Dictionary & Định nghĩa KPI](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/DATA_DICTIONARY.md)
- 📈 [Báo cáo Scalability Benchmark](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/BENCHMARK.md)
- 📊 [Báo cáo Business Insights tự động](file:///d:/hoc/can%20lam/Project%20c%C3%A1%20nh%C3%A2n/GlobalEcommerceBigData/GlobalEcommerceBigData/docs/BUSINESS_INSIGHTS.md)
