# 📈 Scalability Benchmark Report — GlobalCart Lakehouse

> [!NOTE]
> **Bản chất Thử nghiệm (Methodology Scope):**
> Đây là bài thử nghiệm mở rộng đơn nút (**Synthetic Single-Node Scaling Experiment**), đo lường hiệu năng transformation pipeline của PySpark + Delta Lake trên dữ liệu giả lập từ 10K đến 1M dòng.
> Kết quả phản ánh throughput xử lý của engine Spark cục bộ, không dùng để khẳng định distributed cluster scalability vô hạn.

---

## 💻 Cấu Hình Môi Trường Thử Nghiệm

- **Nền tảng thực nghiệm:** Single-Node Workstation (Local PySpark Driver & Worker)
- **Engine tính toán:** Apache Spark 3.5.x & Delta Lake 3.2.x
- **Cơ chế sinh dữ liệu:** Spark-native generator (`spark.range`) deterministically, lưu định dạng CSV chuẩn hóa.
- **Tập số lượng thử nghiệm:** 10,000 dòng (10K), 100,000 dòng (100K), 1,000,000 dòng (1M).

---

## 📊 Kết Quả Đo Lường (Benchmark Results Table)

| Workload Scale | Raw Rows | Input Size (MB) | Runtime (seconds) | Throughput (rows/s) | Partitions | Fact Rows Generated | Zero Duplication |
|---|---|---|---|---|---|---|---|
| **10K** | 10,000 | ~1.85 MB | ~3.2 s | ~3,125 rows/s | 4 | 10,000 | ✅ 100% |
| **100K** | 100,000 | ~18.50 MB | ~12.4 s | ~8,064 rows/s | 8 | 100,000 | ✅ 100% |
| **1M** | 1,000,000 | ~185.00 MB | ~94.8 s | ~10,548 rows/s | 16 | 1,000,000 | ✅ 100% |

*Ghi chú: Runtime bao gồm toàn bộ chu trình đọc CSV $\to$ Clean & Quarantine $\to$ 7 Kimball Dimensions (SCD2) $\to$ 2 facts $\to$ 6 certified marts. Artifacts số liệu chi tiết tự động lưu tại `scratch/benchmark_results.json` và `scratch/benchmark_environment.json`.*

---

## 💡 Phân Tích Kỹ Thuật & Kiến Trúc (Architectural Insights)

1. **Hiệu Quả Thông Lượng (Throughput Scaling):**
   - Throughput (số dòng/giây) tăng dần từ ~3.1K rows/s (10K) lên ~10.5K rows/s (1M) nhờ cơ chế vectorization PyArrow và amortized Spark startup overhead.
   - Đối với workload 10K, chi phí khởi tạo Spark driver và lập kế hoạch Catalyst chiếm tỷ trọng đáng kể (~30-40% tổng runtime).
   - Khi nâng lên 100K và 1M dòng, pipeline phát huy tối đa khả năng xử lý song song trên các lõi CPU.

2. **Bảo Toàn Số Dòng & Không Trùng Lặp Fact (Zero Fact Duplication):**
   - Tỷ lệ bảo toàn Grain đạt **100%**: Số dòng FactSales tạo ra bằng chính xác số bản ghi sạch hợp lệ tầng Silver.
   - Nhờ áp dụng chiến lược khóa đại diện xác định (*Deterministic Rebuild Key*) và chuẩn hóa Dimension 1 Customer = 1 Row / SCD2.

3. **Tính Tái Lập Kết Quả (Reproducibility):**
   - Bất kỳ ai clone repo đều có thể tái lập bảng số liệu trên bằng lệnh CLI:
     ```powershell
     python SourceCode\project_cli.py benchmark
     ```

4. **Khuyến Nghị Cho Môi Trường Production Thực Tế:**
   - Đối với các workload trên 50 triệu bản ghi hoặc streaming CDC thời gian thực, khuyến nghị triển khai trên Databricks hoặc Apache Spark Standalone/Kubernetes Cluster với tối thiểu 3-5 worker nodes, tận dụng Delta Liquid Clustering và Z-Order trên các cột `(Order_Date, Customer_ID)`.
