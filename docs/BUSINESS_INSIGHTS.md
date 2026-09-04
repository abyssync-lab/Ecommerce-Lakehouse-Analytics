# GlobalCart Intelligence — Báo Cáo Phân Tích Dữ Liệu Kinh Doanh

> 🤖 Báo cáo này được sinh tự động từ `Data/EcommerceSalesDataset.csv` thông qua Data Quality Gate & Analytics Engine.

## 1. Chỉ Số KPI Tổng Quan (Executive Overview)

| Chỉ Số KPI | Giá Trị Thực Tế | Ghi Chú Nghiệp Vụ |
|---|---:|---|
| **Tổng Dòng Giao Dịch** | 10,000 | Dữ liệu giao dịch cấp sản phẩm |
| **Tổng Số Đơn Hàng** | 10,000 | Số đơn hàng độc lập (Unique Orders) |
| **Tổng Số Khách Hàng** | 5,348 | Khách hàng đã phát sinh giao dịch |
| **Tổng Doanh Thu (Gross Revenue)** | $5,284,387.70 | Tổng giá trị hóa đơn trước chi phí |
| **Tổng Lợi Nhuận Gộp (Gross Profit)** | $1,437,638.31 | Lợi nhuận bán hàng gộp |
| **Lợi Nhuận Ròng (Net Profit)** | $1,114,639.42 | Lợi nhuận sau khi trừ chi phí vận chuyển |
| **Biên Lợi Nhuận (Profit Margin)** | 27.21% | Tỷ lệ lợi nhuận / doanh thu |
| **Giá Trị Đơn Trung Bình (AOV)** | $528.44 | Average Order Value |
| **Tỷ Lệ Trả Hàng (Return Rate)** | 18.57% | Đơn hàng trạng thái Returned |
| **Tỷ Lệ Hủy Đơn (Cancellation Rate)** | 9.08% | Đơn hàng trạng thái Cancelled |

## 2. Phát Hiện Phân Tích Nổi Bật (Key Business Insights)

- 🌍 **Middle East dẫn đầu doanh thu** với **$1,348,593.22**, tuy nhiên phân bổ thị trường giữa 4 khu vực tương đối đồng đều (chênh lệch dưới 5%).
- 💻 **Electronics là ngành hàng chủ lực**, chiếm **64.0% tổng doanh thu** ($3,382,028.46).
- 🏆 **Lenovo ThinkPad X1** đạt doanh số cao nhất toàn hệ thống với **$379,258.55**.
- ⚠️ **Tổng tỷ lệ Đơn không thành công (Returned + Cancelled)** ở mức **27.65%**, cần tối ưu lại đối tác vận chuyển.

## 3. Phân Hạng Khách Hàng (RFM Customer Segmentation)

| RFM_Segment | Customer_Count | Total_Spend | Avg_Spend |
|---|---|---|---|
| At-Risk Customers | 4067 | 3,113,203.54 | 765.48 |
| Loyal Customers | 1204 | 2,116,102.01 | 1,757.56 |
| Recent & Casual Customers | 72 | 45,886.31 | 637.31 |
| Champions | 5 | 9,195.84 | 1,839.17 |

## 4. Phân Loại Sản Phẩm Pareto (ABC Product Classification)

| ABC_Class | Product_Count | Total_Revenue | Revenue_Share |
|---|---|---|---|
| Class A (Top 80% Revenue) | 29 | 4,238,729.64 | 80.21 |
| Class B (Next 15% Revenue) | 26 | 801,614.87 | 15.17 |
| Class C (Tail 5% Revenue) | 15 | 244,043.19 | 4.62 |

## 5. Xếp Hạng Thị Trường Theo Khu Vực (Regional Performance)

| Region | Orders | Revenue | Profit | Profit_Margin_Percent |
|---|---|---|---|---|
| Middle East | 2458 | 1,348,593.22 | 384,522.85 | 28.51 |
| North America | 2602 | 1,331,129.75 | 366,717.42 | 27.55 |
| Asia | 2494 | 1,330,007.18 | 350,413.48 | 26.35 |
| Europe | 2446 | 1,274,657.55 | 335,984.56 | 26.36 |

## 6. Xếp Hạng Ngành Hàng (Category Performance)

| Category | Orders | Revenue | Profit | Profit_Margin_Percent |
|---|---|---|---|---|
| Electronics | 1977 | 3,382,028.46 | 925,448.15 | 27.36 |
| Home & Kitchen | 2008 | 892,842.02 | 232,421.45 | 26.03 |
| Clothing | 1977 | 407,471.79 | 115,705.29 | 28.40 |
| Books & Media | 2043 | 386,644.46 | 105,037.69 | 27.17 |
| Beauty & Health | 1995 | 215,400.97 | 59,025.73 | 27.40 |

## 7. Top 5 Sản Phẩm Doanh Thu Cao Nhất

| Product_Name | Orders | Revenue | Profit | Profit_Margin_Percent |
|---|---|---|---|---|
| Lenovo ThinkPad X1 | 106 | 379,258.55 | 100,584.09 | 26.52 |
| MacBook Air M2 | 88 | 365,336.21 | 97,941.48 | 26.81 |
| Dell XPS 15 | 108 | 341,123.75 | 89,052.11 | 26.11 |
| HP Spectre x360 | 92 | 283,432.55 | 89,029.69 | 31.41 |
| ASUS ZenBook | 92 | 272,549.12 | 70,166.24 | 25.74 |

## 🛠️ Hướng Dẫn Tái Tạo Báo Cáo Này

```powershell
python SourceCode\generate_portfolio_report.py
```
