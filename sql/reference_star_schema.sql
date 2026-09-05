/* ============================================================
   GLOBACART INTELLIGENCE - REFERENCE T-SQL STAR SCHEMA DDL
   ============================================================
   Mô tả kiến trúc:
   File SQL này đóng vai trò là bản mẫu thiết kế kho dữ liệu (Reference Data Warehouse
   Star Schema DDL & Views) dành cho các hệ cơ sở dữ liệu quan hệ (T-SQL, Microsoft
   SQL Server, Azure Synapse, Fabric DW).

   Mô hình này đồng nhất 100% với kiến trúc PySpark Medallion Lakehouse tầng Gold Core:
   - 1 Fact Table: dbo.FactSales (grain: 1 product line item within 1 order)
   - 7 Dimension Tables: DimCustomer, DimProduct, DimDate, DimLocation,
     DimPayment, DimShipping, DimOrderStatus
   - 5 Analytical Views: vw_SalesAnalysis, vw_RevenueByMonth, vw_RevenueByRegion,
     vw_ProductPerformance, vw_OrderStatusAnalysis
   ============================================================ */

IF DB_ID('EcommerceDW') IS NULL
BEGIN
    CREATE DATABASE EcommerceDW;
END;
GO

USE EcommerceDW;
GO


-- Cấu hình đường dẫn file CSV xuất ra từ tầng Gold Lakehouse.
-- Có thể ghi đè biến bằng SQLCMD: sqlcmd -v EcommerceCsvPath="D:\path\to\ecommerce_final.csv" -i EcommerceDW.sql
IF "$(EcommerceCsvPath)" = ""
BEGIN
    :setvar EcommerceCsvPath "Data/EcommerceSalesDataset.csv"
END
GO

/* ============================================================
   BƯỚC 1: XÓA DỮ LIỆU CŨ ĐỂ CHẠY LẠI TỪ ĐẦU
   ============================================================ */

IF OBJECT_ID('dbo.vw_SalesAnalysis', 'V') IS NOT NULL DROP VIEW dbo.vw_SalesAnalysis;
IF OBJECT_ID('dbo.vw_RevenueByMonth', 'V') IS NOT NULL DROP VIEW dbo.vw_RevenueByMonth;
IF OBJECT_ID('dbo.vw_RevenueByRegion', 'V') IS NOT NULL DROP VIEW dbo.vw_RevenueByRegion;
IF OBJECT_ID('dbo.vw_ProductPerformance', 'V') IS NOT NULL DROP VIEW dbo.vw_ProductPerformance;
IF OBJECT_ID('dbo.vw_OrderStatusAnalysis', 'V') IS NOT NULL DROP VIEW dbo.vw_OrderStatusAnalysis;
GO

IF OBJECT_ID('dbo.FactSales', 'U') IS NOT NULL DROP TABLE dbo.FactSales;
IF OBJECT_ID('dbo.DimOrderStatus', 'U') IS NOT NULL DROP TABLE dbo.DimOrderStatus;
IF OBJECT_ID('dbo.DimPayment', 'U') IS NOT NULL DROP TABLE dbo.DimPayment;
IF OBJECT_ID('dbo.DimShipping', 'U') IS NOT NULL DROP TABLE dbo.DimShipping;
IF OBJECT_ID('dbo.DimProduct', 'U') IS NOT NULL DROP TABLE dbo.DimProduct;
IF OBJECT_ID('dbo.DimLocation', 'U') IS NOT NULL DROP TABLE dbo.DimLocation;
IF OBJECT_ID('dbo.DimCustomer', 'U') IS NOT NULL DROP TABLE dbo.DimCustomer;
IF OBJECT_ID('dbo.DimDate', 'U') IS NOT NULL DROP TABLE dbo.DimDate;
IF OBJECT_ID('dbo.Landing_EcommerceFinal', 'U') IS NOT NULL DROP TABLE dbo.Landing_EcommerceFinal;
GO


/* ============================================================
   BƯỚC 2: TẠO BẢNG LANDING

   Landing_EcommerceFinal là bảng dùng để nhận file final từ Spark.
   File này đã được Spark xử lý trước đó.
   SQL Server chỉ nhận file vào bảng này để tiếp tục tách ra Dim và Fact.
   ============================================================ */

CREATE TABLE dbo.Landing_EcommerceFinal (
    Order_ID NVARCHAR(50) NOT NULL,
    Order_Date DATE NOT NULL,
    [Year] INT NOT NULL,
    [Month] INT NOT NULL,
    [Quarter] NVARCHAR(10) NOT NULL,
    Season NVARCHAR(20) NOT NULL,

    Customer_ID NVARCHAR(50) NOT NULL,
    Customer_Gender NVARCHAR(20) NOT NULL,
    Customer_Segment NVARCHAR(50) NOT NULL,

    Region NVARCHAR(100) NOT NULL,
    Country NVARCHAR(100) NOT NULL,

    Category NVARCHAR(100) NOT NULL,
    Sub_Category NVARCHAR(100) NOT NULL,
    Product_Name NVARCHAR(255) NOT NULL,

    Unit_Price DECIMAL(18,2) NOT NULL,
    Quantity INT NOT NULL,
    Discount DECIMAL(10,4) NOT NULL,
    Revenue DECIMAL(18,2) NOT NULL,
    Cost DECIMAL(18,2) NOT NULL,
    Profit DECIMAL(18,2) NOT NULL,
    Profit_Margin_Percent DECIMAL(10,4) NOT NULL,

    Shipping_Cost DECIMAL(18,2) NOT NULL,
    Shipping_Method NVARCHAR(50) NOT NULL,
    Shipping_Days INT NOT NULL,

    Payment_Method NVARCHAR(50) NOT NULL,
    Order_Status NVARCHAR(50) NOT NULL,

    Revenue_Per_Order DECIMAL(18,2) NOT NULL,
    Net_Profit DECIMAL(18,2) NOT NULL,

    Is_Returned BIT NOT NULL,
    Is_Cancelled BIT NOT NULL,
    Delivery_Level NVARCHAR(50) NOT NULL
);
GO


/* ============================================================
   BƯỚC 3: IMPORT FILE CSV VÀO BẢNG LANDING
   ============================================================ */

BULK INSERT dbo.Landing_EcommerceFinal
FROM '$(EcommerceCsvPath)'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    TABLOCK
);
GO


/* ============================================================
   BƯỚC 4: KIỂM TRA DỮ LIỆU SAU KHI IMPORT

   Mục đích:
   - Kiểm tra file đã vào SQL chưa
   - Kiểm tra có đủ 10000 dòng không
   - Kiểm tra có bị thiếu dữ liệu quan trọng không
   - Kiểm tra ngày bắt đầu và ngày kết thúc của dữ liệu
   ============================================================ */

-- Kiểm tra tổng số dòng.
SELECT COUNT(*) AS Landing_Total_Rows
FROM dbo.Landing_EcommerceFinal;

-- Kiểm tra các cột quan trọng có bị thiếu dữ liệu không.
SELECT
    SUM(CASE WHEN Order_ID IS NULL THEN 1 ELSE 0 END) AS Missing_Order_ID,
    SUM(CASE WHEN Order_Date IS NULL THEN 1 ELSE 0 END) AS Missing_Order_Date,
    SUM(CASE WHEN Customer_ID IS NULL THEN 1 ELSE 0 END) AS Missing_Customer_ID,
    SUM(CASE WHEN Product_Name IS NULL THEN 1 ELSE 0 END) AS Missing_Product_Name,
    SUM(CASE WHEN Revenue IS NULL THEN 1 ELSE 0 END) AS Missing_Revenue
FROM dbo.Landing_EcommerceFinal;

-- Xem dữ liệu đang nằm trong khoảng thời gian nào.
SELECT
    MIN(Order_Date) AS Min_Order_Date,
    MAX(Order_Date) AS Max_Order_Date
FROM dbo.Landing_EcommerceFinal;
GO


/* ============================================================
   BƯỚC 5: TẠO CÁC BẢNG DIMENSION
   ============================================================ */

CREATE TABLE dbo.DimDate (
    DateKey INT NOT NULL PRIMARY KEY,   -- Mã ngày theo dạng yyyyMMdd
    FullDate DATE NOT NULL,
    [Year] INT NOT NULL,
    [Month] INT NOT NULL,
    [Quarter] NVARCHAR(10) NOT NULL,
    Season NVARCHAR(20) NOT NULL,

    CONSTRAINT UQ_DimDate_FullDate UNIQUE (FullDate)
);

CREATE TABLE dbo.DimCustomer (
    CustomerKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Customer_ID NVARCHAR(50) NOT NULL,
    Customer_Gender NVARCHAR(20) NOT NULL,
    Customer_Segment NVARCHAR(50) NOT NULL,

    CONSTRAINT UQ_DimCustomer UNIQUE (
        Customer_ID,
        Customer_Gender,
        Customer_Segment
    )
);

CREATE TABLE dbo.DimLocation (
    LocationKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Region NVARCHAR(100) NOT NULL,
    Country NVARCHAR(100) NOT NULL,

    CONSTRAINT UQ_DimLocation UNIQUE (Region, Country)
);

CREATE TABLE dbo.DimProduct (
    ProductKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Category NVARCHAR(100) NOT NULL,
    Sub_Category NVARCHAR(100) NOT NULL,
    Product_Name NVARCHAR(255) NOT NULL,

    CONSTRAINT UQ_DimProduct UNIQUE (
        Category,
        Sub_Category,
        Product_Name
    )
);

CREATE TABLE dbo.DimShipping (
    ShippingKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Shipping_Method NVARCHAR(50) NOT NULL,
    Delivery_Level NVARCHAR(50) NOT NULL,

    CONSTRAINT UQ_DimShipping UNIQUE (
        Shipping_Method,
        Delivery_Level
    )
);

CREATE TABLE dbo.DimPayment (
    PaymentKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Payment_Method NVARCHAR(50) NOT NULL,

    CONSTRAINT UQ_DimPayment UNIQUE (Payment_Method)
);

CREATE TABLE dbo.DimOrderStatus (
    StatusKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Order_Status NVARCHAR(50) NOT NULL,
    Is_Returned BIT NOT NULL,
    Is_Cancelled BIT NOT NULL,

    CONSTRAINT UQ_DimOrderStatus UNIQUE (
        Order_Status,
        Is_Returned,
        Is_Cancelled
    )
);
GO


/* ============================================================
   BƯỚC 6: TẠO BẢNG FACTSALES
   ============================================================ */

CREATE TABLE dbo.FactSales (
    SalesKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    Order_ID NVARCHAR(50) NOT NULL,

    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    LocationKey INT NOT NULL,
    ProductKey INT NOT NULL,
    ShippingKey INT NOT NULL,
    PaymentKey INT NOT NULL,
    StatusKey INT NOT NULL,

    Unit_Price DECIMAL(18,2) NOT NULL,
    Quantity INT NOT NULL,
    Discount DECIMAL(10,4) NOT NULL,
    Revenue DECIMAL(18,2) NOT NULL,
    Cost DECIMAL(18,2) NOT NULL,
    Profit DECIMAL(18,2) NOT NULL,
    Profit_Margin_Percent DECIMAL(10,4) NOT NULL,
    Shipping_Cost DECIMAL(18,2) NOT NULL,
    Shipping_Days INT NOT NULL,
    Revenue_Per_Order DECIMAL(18,2) NOT NULL,
    Net_Profit DECIMAL(18,2) NOT NULL,

    CONSTRAINT FK_FactSales_DimDate
        FOREIGN KEY (DateKey) REFERENCES dbo.DimDate(DateKey),

    CONSTRAINT FK_FactSales_DimCustomer
        FOREIGN KEY (CustomerKey) REFERENCES dbo.DimCustomer(CustomerKey),

    CONSTRAINT FK_FactSales_DimLocation
        FOREIGN KEY (LocationKey) REFERENCES dbo.DimLocation(LocationKey),

    CONSTRAINT FK_FactSales_DimProduct
        FOREIGN KEY (ProductKey) REFERENCES dbo.DimProduct(ProductKey),

    CONSTRAINT FK_FactSales_DimShipping
        FOREIGN KEY (ShippingKey) REFERENCES dbo.DimShipping(ShippingKey),

    CONSTRAINT FK_FactSales_DimPayment
        FOREIGN KEY (PaymentKey) REFERENCES dbo.DimPayment(PaymentKey),

    CONSTRAINT FK_FactSales_DimOrderStatus
        FOREIGN KEY (StatusKey) REFERENCES dbo.DimOrderStatus(StatusKey)
);
GO


/* ============================================================
   BƯỚC 7: ĐƯA DỮ LIỆU VÀO CÁC BẢNG DIMENSION

   SELECT DISTINCT dùng để lấy dữ liệu không bị trùng.
   Ví dụ cùng một sản phẩm xuất hiện nhiều lần trong Landing,
   nhưng trong DimProduct chỉ cần lưu một lần.
   ============================================================ */

INSERT INTO dbo.DimDate (
    DateKey,
    FullDate,
    [Year],
    [Month],
    [Quarter],
    Season
)
SELECT DISTINCT
    CAST(CONVERT(CHAR(8), Order_Date, 112) AS INT) AS DateKey,
    Order_Date AS FullDate,
    [Year],
    [Month],
    [Quarter],
    Season
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimCustomer (
    Customer_ID,
    Customer_Gender,
    Customer_Segment
)
SELECT DISTINCT
    Customer_ID,
    Customer_Gender,
    Customer_Segment
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimLocation (
    Region,
    Country
)
SELECT DISTINCT
    Region,
    Country
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimProduct (
    Category,
    Sub_Category,
    Product_Name
)
SELECT DISTINCT
    Category,
    Sub_Category,
    Product_Name
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimShipping (
    Shipping_Method,
    Delivery_Level
)
SELECT DISTINCT
    Shipping_Method,
    Delivery_Level
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimPayment (
    Payment_Method
)
SELECT DISTINCT
    Payment_Method
FROM dbo.Landing_EcommerceFinal;

INSERT INTO dbo.DimOrderStatus (
    Order_Status,
    Is_Returned,
    Is_Cancelled
)
SELECT DISTINCT
    Order_Status,
    Is_Returned,
    Is_Cancelled
FROM dbo.Landing_EcommerceFinal;
GO


/* ============================================================
   BƯỚC 8: ĐƯA DỮ LIỆU VÀO BẢNG FACTSALES
   ============================================================ */

INSERT INTO dbo.FactSales (
    Order_ID,
    DateKey,
    CustomerKey,
    LocationKey,
    ProductKey,
    ShippingKey,
    PaymentKey,
    StatusKey,
    Unit_Price,
    Quantity,
    Discount,
    Revenue,
    Cost,
    Profit,
    Profit_Margin_Percent,
    Shipping_Cost,
    Shipping_Days,
    Revenue_Per_Order,
    Net_Profit
)
SELECT
    l.Order_ID,

    d.DateKey,
    c.CustomerKey,
    loc.LocationKey,
    p.ProductKey,
    sh.ShippingKey,
    pay.PaymentKey,
    os.StatusKey,

    l.Unit_Price,
    l.Quantity,
    l.Discount,
    l.Revenue,
    l.Cost,
    l.Profit,
    l.Profit_Margin_Percent,
    l.Shipping_Cost,
    l.Shipping_Days,
    l.Revenue_Per_Order,
    l.Net_Profit
FROM dbo.Landing_EcommerceFinal l
INNER JOIN dbo.DimDate d
    ON d.FullDate = l.Order_Date
INNER JOIN dbo.DimCustomer c
    ON c.Customer_ID = l.Customer_ID
    AND c.Customer_Gender = l.Customer_Gender
    AND c.Customer_Segment = l.Customer_Segment
INNER JOIN dbo.DimLocation loc
    ON loc.Region = l.Region
    AND loc.Country = l.Country
INNER JOIN dbo.DimProduct p
    ON p.Category = l.Category
    AND p.Sub_Category = l.Sub_Category
    AND p.Product_Name = l.Product_Name
INNER JOIN dbo.DimShipping sh
    ON sh.Shipping_Method = l.Shipping_Method
    AND sh.Delivery_Level = l.Delivery_Level
INNER JOIN dbo.DimPayment pay
    ON pay.Payment_Method = l.Payment_Method
INNER JOIN dbo.DimOrderStatus os
    ON os.Order_Status = l.Order_Status
    AND os.Is_Returned = l.Is_Returned
    AND os.Is_Cancelled = l.Is_Cancelled;
GO


/* ============================================================
   BƯỚC 9: TẠO INDEX

   Index giúp SQL Server tìm dữ liệu nhanh hơn.
   Khi Power BI lấy dữ liệu từ FactSales,
   các cột khóa như DateKey, ProductKey, CustomerKey sẽ được truy vấn nhiều.
   Vì vậy tạo index cho các cột này.
   ============================================================ */

CREATE INDEX IX_FactSales_DateKey ON dbo.FactSales(DateKey);
CREATE INDEX IX_FactSales_CustomerKey ON dbo.FactSales(CustomerKey);
CREATE INDEX IX_FactSales_LocationKey ON dbo.FactSales(LocationKey);
CREATE INDEX IX_FactSales_ProductKey ON dbo.FactSales(ProductKey);
CREATE INDEX IX_FactSales_ShippingKey ON dbo.FactSales(ShippingKey);
CREATE INDEX IX_FactSales_PaymentKey ON dbo.FactSales(PaymentKey);
CREATE INDEX IX_FactSales_StatusKey ON dbo.FactSales(StatusKey);
CREATE INDEX IX_FactSales_OrderID ON dbo.FactSales(Order_ID);
GO


/* ============================================================
   BƯỚC 10: KIỂM TRA KẾT QUẢ SAU KHI LÀM DATA WAREHOUSE

   Mục đích:
   - Xem mỗi bảng có bao nhiêu dòng
   - Kiểm tra FactSales có đủ dòng như Landing không
   - Nếu Landing = FactSales thì dữ liệu đã nạp thành công
   ============================================================ */

SELECT 'Landing_EcommerceFinal' AS TableName, COUNT(*) AS TotalRows FROM dbo.Landing_EcommerceFinal
UNION ALL
SELECT 'FactSales', COUNT(*) FROM dbo.FactSales
UNION ALL
SELECT 'DimDate', COUNT(*) FROM dbo.DimDate
UNION ALL
SELECT 'DimCustomer', COUNT(*) FROM dbo.DimCustomer
UNION ALL
SELECT 'DimLocation', COUNT(*) FROM dbo.DimLocation
UNION ALL
SELECT 'DimProduct', COUNT(*) FROM dbo.DimProduct
UNION ALL
SELECT 'DimShipping', COUNT(*) FROM dbo.DimShipping
UNION ALL
SELECT 'DimPayment', COUNT(*) FROM dbo.DimPayment
UNION ALL
SELECT 'DimOrderStatus', COUNT(*) FROM dbo.DimOrderStatus;
GO

-- Kiểm tra có bị mất dòng khi đưa dữ liệu từ Landing sang FactSales không.
-- Với file hiện tại, Landing và FactSales đều phải là 10000 dòng.
SELECT
    (SELECT COUNT(*) FROM dbo.Landing_EcommerceFinal) AS LandingRows,
    (SELECT COUNT(*) FROM dbo.FactSales) AS FactRows,
    (SELECT COUNT(*) FROM dbo.Landing_EcommerceFinal)
        - (SELECT COUNT(*) FROM dbo.FactSales) AS MissingRows;
GO
