-- Example Inserts for Dim_Customers
INSERT INTO Dim_Customers (customer_sk, customer_id, first_name, last_name, email, residential_location, customer_segment)
SELECT 
    seq,
    CONCAT('CUST', LPAD(seq::text, 3, '0')),
    CONCAT('First', seq),
    CONCAT('Last', seq),
    CONCAT('customer', seq, '@example.com'),
    CONCAT('Loc', (seq % 10) + 1),
    CASE WHEN seq % 3 = 0 THEN 'Premium'
         WHEN seq % 3 = 1 THEN 'Standard'
         ELSE 'Basic' END
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Dim_Products
INSERT INTO Dim_Products (product_sk, product_id, product_name, category, brand, origin_location)
SELECT 
    seq,
    CONCAT('PROD', LPAD(seq::text, 3, '0')),
    CONCAT('Product', seq),
    CASE WHEN seq % 2 = 0 THEN 'Electronics' ELSE 'Apparel' END,
    CASE WHEN seq % 3 = 0 THEN 'BrandA' ELSE 'BrandB' END,
    CONCAT('Country', (seq % 5) + 1)
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Dim_Stores
INSERT INTO Dim_Stores (store_sk, store_id, store_name, store_type, store_location, store_manager_sk)
SELECT 
    seq,
    CONCAT('STORE', LPAD(seq::text, 3, '0')),
    CONCAT('Store', seq),
    CASE WHEN seq % 2 = 0 THEN 'Retail' ELSE 'Outlet' END,
    CONCAT('City', (seq % 7) + 1),
    ((seq % 10) + 1)
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Dim_Dates
INSERT INTO Dim_Dates (date_sk, full_date, year, month, day, day_of_week, quarter)
SELECT 
    seq,
    CURRENT_DATE - (50 - seq),
    EXTRACT(YEAR FROM CURRENT_DATE - (50 - seq)),
    EXTRACT(MONTH FROM CURRENT_DATE - (50 - seq)),
    EXTRACT(DAY FROM CURRENT_DATE - (50 - seq)),
    TO_CHAR(CURRENT_DATE - (50 - seq), 'Day'),
    ((EXTRACT(MONTH FROM CURRENT_DATE - (50 - seq))::int - 1) / 3 + 1)::int
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Dim_Salesperson
INSERT INTO Dim_Salesperson (salesperson_sk, salesperson_id, salesperson_name, salesperson_role)
SELECT 
    seq,
    CONCAT('SALESP', LPAD(seq::text, 3, '0')),
    CONCAT('Salesperson', seq),
    CASE WHEN seq % 2 = 0 THEN 'Manager' ELSE 'Representative' END
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Dim_Campaign
INSERT INTO Dim_Campaign (campaign_sk, campaign_id, campaign_name, start_date_sk, end_date_sk, campaign_budget)
SELECT 
    seq,
    CONCAT('COMP', LPAD(seq::text, 3, '0')),
    CONCAT('Campaign', seq),
    ((seq % 40) + 1),
    ((seq % 50) + 1),
    (seq * 1000)::DECIMAL(12,2)
FROM generate_series(1, 50) AS seq;

-- Example Inserts for Fact_Sales
INSERT INTO Fact_Sales (sales_sk, sales_id, customer_sk, product_sk, store_sk, campaign_sk, salesperson_sk, sales_date, total_amount)
SELECT 
    seq,
    CONCAT('SALE', LPAD(seq::text, 4, '0')),
    (seq % 50) + 1,
    ((seq + 10) % 50) + 1,
    ((seq + 20) % 50) + 1,
    ((seq + 30) % 50) + 1,
    ((seq + 40) % 50) + 1,
    CURRENT_DATE - (50 - seq),
    (seq * 100.00)::DECIMAL(16,2)
FROM generate_series(1, 50) AS seq;
