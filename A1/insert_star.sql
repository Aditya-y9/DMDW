-- Insert into Dim_Customers
INSERT INTO Dim_Customers (customer_sk, customer_id, first_name, last_name, email, residential_location, customer_segment) VALUES
(1, 'C001', 'Alice', 'Smith', 'alice.smith@example.com', 'New York', 'Premium'),
(2, 'C002', 'Bob', 'Jones', 'bob.jones@example.com', 'Los Angeles', 'Standard'),
(3, 'C003', 'Charlie', 'Brown', 'charlie.brown@example.com', 'Chicago', 'Basic'),
(4, 'C004', 'Diana', 'Evans', 'diana.evans@example.com', 'Houston', 'Premium'),
(5, 'C005', 'Evan', 'Foster', 'evan.foster@example.com', 'Phoenix', 'Standard'),
(6, 'C006', 'Fiona', 'Garcia', 'fiona.garcia@example.com', 'Philadelphia', 'Basic'),
(7, 'C007', 'George', 'Harris', 'george.harris@example.com', 'San Antonio', 'Premium'),
(8, 'C008', 'Hannah', 'Ivers', 'hannah.ivers@example.com', 'San Diego', 'Standard'),
(9, 'C009', 'Ian', 'Jackson', 'ian.jackson@example.com', 'Dallas', 'Basic'),
(10, 'C010', 'Julia', 'King', 'julia.king@example.com', 'San Jose', 'Premium');

-- Insert into Dim_Products
INSERT INTO Dim_Products (product_sk, product_id, product_name, category, brand, origin_location) VALUES
(1, 'P001', 'Laptop', 'Electronics', 'BrandX', 'USA'),
(2, 'P002', 'Smartphone', 'Electronics', 'BrandY', 'China'),
(3, 'P003', 'Tablet', 'Electronics', 'BrandZ', 'South Korea'),
(4, 'P004', 'Jeans', 'Apparel', 'FashionCo', 'Bangladesh'),
(5, 'P005', 'T-shirt', 'Apparel', 'StyleInc', 'Vietnam'),
(6, 'P006', 'Sneakers', 'Footwear', 'RunFast', 'Indonesia'),
(7, 'P007', 'Backpack', 'Accessories', 'CarryAll', 'USA'),
(8, 'P008', 'Wristwatch', 'Accessories', 'TimeMaster', 'Switzerland'),
(9, 'P009', 'Desk Chair', 'Furniture', 'ComfortSeat', 'Poland'),
(10, 'P010', 'Coffee Maker', 'Appliances', 'BrewBest', 'Germany');

-- Insert into Dim_Stores
INSERT INTO Dim_Stores (store_sk, store_id, store_name, store_type, store_location, store_manager_sk) VALUES
(1, 'S001', 'Downtown Store', 'Retail', 'New York', 101),
(2, 'S002', 'Mall Store', 'Retail', 'Los Angeles', 102),
(3, 'S003', 'Outlet Store', 'Outlet', 'Chicago', 103),
(4, 'S004', 'City Store', 'Retail', 'Houston', 104),
(5, 'S005', 'Suburb Store', 'Retail', 'Phoenix', 105),
(6, 'S006', 'Airport Store', 'Retail', 'Philadelphia', 106),
(7, 'S007', 'Campus Store', 'Retail', 'San Antonio', 107),
(8, 'S008', 'Mall Kiosk', 'Kiosk', 'San Diego', 108),
(9, 'S009', 'Downtown Outlet', 'Outlet', 'Dallas', 109),
(10, 'S010', 'Warehouse Store', 'Warehouse', 'San Jose', 110);

-- Insert into Dim_Dates
INSERT INTO Dim_Dates (date_sk, full_date, year, month, day, day_of_week, quarter) VALUES
(1, '2024-01-01', 2024, 1, 1, 'Monday', 1),
(2, '2024-02-01', 2024, 2, 1, 'Thursday', 1),
(3, '2024-03-01', 2024, 3, 1, 'Friday', 1),
(4, '2024-04-01', 2024, 4, 1, 'Monday', 2),
(5, '2024-05-01', 2024, 5, 1, 'Wednesday', 2),
(6, '2024-06-01', 2024, 6, 1, 'Saturday', 2),
(7, '2024-07-01', 2024, 7, 1, 'Monday', 3),
(8, '2024-08-01', 2024, 8, 1, 'Thursday', 3),
(9, '2024-09-01', 2024, 9, 1, 'Sunday', 3),
(10, '2024-10-01', 2024, 10, 1, 'Tuesday', 4);

-- Insert into Dim_Salesperson
INSERT INTO Dim_Salesperson (salesperson_sk, salesperson_id, salesperson_name, salesperson_role) VALUES
(1, 'SP001', 'John Smith', 'Manager'),
(2, 'SP002', 'Jane Doe', 'Representative'),
(3, 'SP003', 'Jim Brown', 'Manager'),
(4, 'SP004', 'Jill Taylor', 'Representative'),
(5, 'SP005', 'Joe Wilson', 'Manager'),
(6, 'SP006', 'Jenny Lee', 'Representative'),
(7, 'SP007', 'Jack White', 'Manager'),
(8, 'SP008', 'Julie Green', 'Representative'),
(9, 'SP009', 'Jason Moore', 'Manager'),
(10, 'SP010', 'Jasmine King', 'Representative');

-- Insert into Dim_Campaign
INSERT INTO Dim_Campaign (campaign_sk, campaign_id, campaign_name, start_date_sk, end_date_sk, campaign_budget) VALUES
(1, 'CAM001', 'Summer Sale', 1, 3, 50000.00),
(2, 'CAM002', 'Back to School', 4, 6, 75000.00),
(3, 'CAM003', 'Holiday Special', 7, 10, 100000.00),
(4, 'CAM004', 'Clearance', 1, 5, 30000.00),
(5, 'CAM005', 'Anniversary', 3, 9, 60000.00),
(6, 'CAM006', 'Black Friday', 8, 10, 120000.00),
(7, 'CAM007', 'Cyber Monday', 9, 10, 90000.00),
(8, 'CAM008', 'Spring Sale', 2, 4, 40000.00),
(9, 'CAM009', 'New Year Sale', 10, 10, 55000.00),
(10, 'CAM010', 'Weekend Deals', 5, 7, 35000.00);

-- Insert into Fact_Sales
INSERT INTO Fact_Sales (sales_sk, sales_id, customer_sk, product_sk, store_sk, campaign_sk, salesperson_sk, sales_date, total_amount) VALUES
(1, 'S001', 1, 1, 1, 1, 1, '2024-01-01', 150.00),
(2, 'S002', 2, 3, 2, 2, 2, '2024-02-15', 200.00),
(3, 'S003', 3, 2, 3, 3, 3, '2024-03-10', 175.00),
(4, 'S004', 4, 5, 4, 4, 4, '2024-04-05', 300.00),
(5, 'S005', 5, 4, 5, 5, 5, '2024-05-20', 125.00),
(6, 'S006', 6, 6, 6, 6, 6, '2024-06-25', 400.00),
(7, 'S007', 7, 7, 7, 7, 7, '2024-07-30', 275.00),
(8, 'S008', 8, 8, 8, 8, 8, '2024-08-18', 350.00),
(9, 'S009', 9, 9, 9, 9, 9, '2024-09-22', 450.00),
(10, 'S010', 10, 10, 10, 10, 10, '2024-10-10', 500.00);
