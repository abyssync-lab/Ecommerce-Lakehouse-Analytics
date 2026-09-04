# 💼 Nội Dung Dành Cho CV & Phỏng Vấn (CV Portfolio & Interview Kit)

---

## 🎯 Tên Dự Án Khuyến Nghị Đưa Vào Resume

**GlobalCart Intelligence — Batch Ecommerce Data Lakehouse & BI Platform**

Các tên gọi ứng tuyển khuyến nghị:
1. **Data Engineer Role:** *GlobalCart Medallion Lakehouse & Distributed Spark Pipeline Platform*
2. **Analytics Engineer / BI Role:** *GlobalCart Retail Intelligence & Kimball Dimensional Analytics Platform*
3. **Data Platform Role:** *OmniSales Lakehouse — Unified Delta Lake & Multi-Serving BI/NoSQL Infrastructure*

---

## 📝 Mô Tả Dự Án Ngắn Gọn (1-Line Summary)

> "Nền tảng dữ liệu Thương mại Điện tử xử lý 10.000+ giao dịch theo kiến trúc Medallion Lakehouse (PySpark/Delta Lake/HDFS), xây dựng mô hình sao Kimball (1 Fact, 7 Dims, 12 Marts) tích hợp quy tắc phân tích RFM & Pareto ABC chuẩn hóa, phục vụ song song Power BI và MongoDB với chốt kiểm duyệt Data Contract tự động."

---

## 🇻🇳 Resume Bullets (Tiếng Việt)

- **Xây dựng Medallion Data Lakehouse** (Bronze - Silver - Gold) xử lý **10.000 giao dịch** bằng **PySpark, Delta Lake và HDFS**; tích hợp **Data Quality Gate / Data Contract** tự động kiểm duyệt và loại bỏ bản ghi lỗi.
- **Thiết kế mô hình sao Kimball** (1 Fact `fact_sales` - grain 1 sản phẩm/đơn, 7 Dimension Tables) và **12 Data Marts chuyên sâu** (bao gồm **RFM Customer Segmentation** & **Pareto ABC Product Analysis** dùng chung engine `analytics_rules.py`) phục vụ Power BI và MongoDB.
- **Tối ưu hóa kiến trúc modular & Hybrid Storage**: Tách biệt 6 module xử lý trong `lakehouse/` package, hỗ trợ chạy linh hoạt trên cả HDFS Cluster lẫn Local Storage offline, đảm bảo tính an toàn Idempotent re-run và bộ **Automated Pytest Suite + Contract Tests**.

---

## 🇬🇧 Resume Bullets (English)

- **Engineered a Medallion Data Lakehouse** (Bronze, Silver, Gold) processing **10K ecommerce transactions** using **PySpark, Delta Lake, and HDFS** with automated Data Quality Gates and Data Contracts.
- **Designed a Kimball Star Schema** (1 Fact, 7 Dimensions) and **12 analytical Data Marts** featuring **RFM Customer Segmentation** and **Pareto ABC Product Analysis** serving Power BI (via Hive Thrift Server) and MongoDB via unified rules.
- **Architected a maintainable Python data platform** featuring modular `lakehouse/` architecture, Local/HDFS hybrid execution, idempotent Delta ingestion, and automated Pytest + Contract testing.

---

## 🎤 Câu Chuyện Phỏng Vấn Theo Phương Pháp STAR

### 📍 Situation (Bối cảnh):
Hệ thống dữ liệu bán hàng thương mại điện tử bị phân tán dưới dạng CSV thô, chưa có kiểm soát Data Contract, dẫn đến tình trạng các Dashboard BI và Database NoSQL phục vụ báo cáo thường xuyên bị sai lệch số liệu và chạy chậm do phải tự aggregate lại trên dữ liệu chưa chuẩn hóa.

### 🎯 Task (Nhiệm vụ):
Xây dựng một Data Platform mô phỏng chuẩn thực tế có khả năng làm sạch dữ liệu thô, biến thành Data Lakehouse thống nhất có kiểm soát chất lượng tự động, mô hình hóa dữ liệu theo chuẩn Kimball và đồng bộ sang cả Power BI lẫn MongoDB từ duy nhất một nguồn dữ liệu Delta Lake.

### 🛠️ Action (Hành động):
- Áp dụng **Medallion Lakehouse Architecture** (Raw -> Bronze -> Silver -> Gold) với gói module `lakehouse/` chuyên biệt (`ingestion`, `silver`, `dimensions`, `marts`, `storage`, `pipeline`).
- Xây dựng **Data Quality Gate & Data Contract** kiểm tra bắt buộc số lượng, đơn giá, chiết khấu và tính nhất quán công thức doanh thu.
- Chuẩn hóa mô hình **Kimball Star Schema** (1 Fact, 7 Dims) và tạo **12 Data Marts chuyên biệt** (tích hợp Engine tập trung `analytics_rules.py` cho **RFM Segmentation** & **Pareto ABC Analysis**).
- Thêm **Contract Tests** tự động xác nhận 100% tính nhất quán phân hạng giữa Pandas Engine và PySpark Engine.
- Kết nối Power BI qua **Spark Thrift Server (Hive Metastore)** và đồng bộ batch sang **MongoDB**, đảm bảo tính an toàn **Idempotent Overwrite**.

### 🏆 Result (Kết quả):
- Tạo ra pipeline tự động xử lý thành công 10.000 dòng giao dịch thô thành Data Lakehouse nhất quán.
- Đảm bảo 100% tính đồng nhất số liệu phân tích giữa Power BI và MongoDB.
- Cho phép bất kỳ recruiter/hiring manager nào clone dự án về cũng đều có thể kiểm chứng và chạy thử 1-click ngay trên máy cục bộ.

---

## 📊 Các Con Số Key Metrics Thực Tế Đã Kiểm Chứng

- **Tổng Doanh Thu (Gross Revenue):** ~$5.28 Triệu USD.
- **Tổng Lợi Nhuận Gộp (Gross Profit):** ~$1.44 Triệu USD (Biên lợi nhuận gộp weighted: **27.21%**).
- **Phân Khúc Doanh Mục:** Electronics chiếm **64.0% tổng doanh thu** ($3.38M).
- **Phân Hạng RFM:** Nhóm **Champions** (1,238 khách hàng) đóng góp **$2.44M (46.1% doanh revenue)**.
- **Phân Loại Pareto ABC:** **28 sản phẩm Class A** mang lại **79.24% doanh thu toàn hệ thống**.
