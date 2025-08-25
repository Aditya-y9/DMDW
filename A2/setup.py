import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Create sample retail data warehouse
def create_sample_database():
    conn = sqlite3.connect('olap_retail_warehouse.db')
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_products")
    cursor.execute("DROP TABLE IF EXISTS dim_stores")
    cursor.execute("DROP TABLE IF EXISTS dim_customers")
    cursor.execute("DROP TABLE IF EXISTS dim_dates")
    cursor.execute("DROP TABLE IF EXISTS dim_regions")
    
    # Create dimension tables
    cursor.execute('''
    CREATE TABLE dim_products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        subcategory TEXT,
        brand TEXT,
        unit_price REAL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_stores (
        store_id INTEGER PRIMARY KEY,
        store_name TEXT,
        store_type TEXT,
        city TEXT,
        state TEXT,
        region_id INTEGER,
        FOREIGN KEY (region_id) REFERENCES dim_regions(region_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name TEXT,
        segment TEXT,
        city TEXT,
        state TEXT,
        age_group TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_dates (
        date_id INTEGER PRIMARY KEY,
        full_date DATE,
        day_of_week TEXT,
        month TEXT,
        quarter TEXT,
        year INTEGER,
        month_num INTEGER,
        quarter_num INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE dim_regions (
        region_id INTEGER PRIMARY KEY,
        region_name TEXT
    )
    ''')
    
    # Create fact table
    cursor.execute('''
    CREATE TABLE fact_sales (
        sales_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        store_id INTEGER,
        customer_id INTEGER,
        date_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        discount REAL,
        total_amount REAL,
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
        FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
    )
    ''')
    
    return conn, cursor

def populate_sample_data(conn, cursor):
    # Insert regions
    regions = [
        (1, 'North'),
        (2, 'South'), 
        (3, 'East'),
        (4, 'West'),
        (5, 'Central')
    ]
    cursor.executemany("INSERT INTO dim_regions VALUES (?, ?)", regions)
    
    # Insert products
    products = [
        (1, 'Laptop Pro', 'Electronics', 'Computers', 'TechBrand', 1200.00),
        (2, 'Smartphone X', 'Electronics', 'Mobile', 'PhoneCorp', 800.00),
        (3, 'Office Chair', 'Furniture', 'Seating', 'ComfortCorp', 250.00),
        (4, 'Desk Lamp', 'Furniture', 'Lighting', 'BrightLight', 80.00),
        (5, 'Coffee Maker', 'Appliances', 'Kitchen', 'BrewMaster', 150.00),
        (6, 'Tablet Mini', 'Electronics', 'Tablets', 'TechBrand', 400.00),
        (7, 'Bookshelf', 'Furniture', 'Storage', 'WoodWorks', 300.00),
        (8, 'Microwave', 'Appliances', 'Kitchen', 'CookFast', 200.00)
    ]
    cursor.executemany("INSERT INTO dim_products VALUES (?, ?, ?, ?, ?, ?)", products)
    
    # Insert stores
    stores = [
        (1, 'Downtown Store', 'Flagship', 'New York', 'NY', 3),
        (2, 'Mall Outlet', 'Outlet', 'Los Angeles', 'CA', 4),
        (3, 'City Center', 'Regular', 'Chicago', 'IL', 5),
        (4, 'Suburban Store', 'Regular', 'Houston', 'TX', 2),
        (5, 'Metro Plaza', 'Flagship', 'Miami', 'FL', 2),
        (6, 'Shopping Complex', 'Outlet', 'Seattle', 'WA', 4),
        (7, 'Town Square', 'Regular', 'Boston', 'MA', 3),
        (8, 'Market Street', 'Regular', 'Atlanta', 'GA', 2)
    ]
    cursor.executemany("INSERT INTO dim_stores VALUES (?, ?, ?, ?, ?, ?)", stores)
    
    # Insert customers
    customers = [
        (1, 'John Smith', 'Consumer', 'New York', 'NY', '25-35'),
        (2, 'Alice Johnson', 'Corporate', 'Los Angeles', 'CA', '35-45'),
        (3, 'Bob Wilson', 'Home Office', 'Chicago', 'IL', '45-55'),
        (4, 'Carol Davis', 'Consumer', 'Houston', 'TX', '25-35'),
        (5, 'David Brown', 'Corporate', 'Miami', 'FL', '35-45'),
        (6, 'Emma Miller', 'Home Office', 'Seattle', 'WA', '25-35'),
        (7, 'Frank Taylor', 'Consumer', 'Boston', 'MA', '45-55'),
        (8, 'Grace Anderson', 'Corporate', 'Atlanta', 'GA', '35-45')
    ]
    cursor.executemany("INSERT INTO dim_customers VALUES (?, ?, ?, ?, ?, ?)", customers)
    
    # Insert dates (for 2023-2024)
    dates = []
    start_date = datetime(2023, 1, 1)
    for i in range(730):  # 2 years of data
        current_date = start_date + timedelta(days=i)
        date_id = int(current_date.strftime('%Y%m%d'))
        dates.append((
            date_id,
            current_date.strftime('%Y-%m-%d'),
            current_date.strftime('%A'),
            current_date.strftime('%B'),
            f'Q{(current_date.month-1)//3 + 1}',
            current_date.year,
            current_date.month,
            (current_date.month-1)//3 + 1
        ))
    
    cursor.executemany("INSERT INTO dim_dates VALUES (?, ?, ?, ?, ?, ?, ?, ?)", dates)
    
    # Generate sample sales data
    sales_data = []
    for i in range(1, 1001):  # 1000 sales records
        product_id = random.randint(1, 8)
        store_id = random.randint(1, 8)
        customer_id = random.randint(1, 8)
        
        # Get a random date from our date dimension
        random_days = random.randint(0, 729)
        sale_date = start_date + timedelta(days=random_days)
        date_id = int(sale_date.strftime('%Y%m%d'))
        
        quantity = random.randint(1, 10)
        unit_price = random.uniform(50, 1500)
        discount = random.uniform(0, 0.2)
        total_amount = quantity * unit_price * (1 - discount)
        
        sales_data.append((
            i, product_id, store_id, customer_id, date_id,
            quantity, unit_price, discount, total_amount
        ))
    
    cursor.executemany("""
    INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, sales_data)
    
    conn.commit()
    print("Sample data created successfully!")

# Initialize database
conn, cursor = create_sample_database()
populate_sample_data(conn, cursor)
