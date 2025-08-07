import sqlite3
import csv
import os
import random
import datetime

# Database file path
db_path = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\fact_constellation.db'

# CSV files directory
csv_dir = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\Dataset\\'

# Delete existing database if it exists
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Removed existing database: {db_path}")

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Creating fact constellation schema tables...")

# ---------------------- SHARED DIMENSION TABLES ----------------------
# Create dimension tables (shared between multiple fact tables)
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_code TEXT,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    location TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id TEXT PRIMARY KEY,
    store_code TEXT,
    store_name TEXT,
    store_type TEXT,
    city TEXT,
    store_manager_sk TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_code TEXT,
    first_name TEXT,
    last_name TEXT,
    customer_email TEXT,
    residential_location TEXT,
    customer_segment TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_salespersons (
    salesperson_id TEXT PRIMARY KEY,
    salesperson_code TEXT,
    salesperson_name TEXT,
    salesperson_email TEXT,
    salesperson_phone TEXT,
    designation TEXT,
    region TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    quarter INTEGER,
    is_holiday INTEGER,
    is_weekend INTEGER
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id TEXT PRIMARY KEY,
    campaign_code TEXT,
    campaign_name TEXT,
    campaign_start_date TEXT,
    campaign_end_date TEXT,
    campaign_type TEXT,
    campaign_channel TEXT,
    campaign_budget REAL
)''')

# ---------------------- MULTIPLE FACT TABLES ----------------------
# 1. Sales Fact Table (Transaction level)
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id TEXT PRIMARY KEY,
    product_id TEXT,
    store_id TEXT,
    customer_id TEXT,
    salesperson_id TEXT,
    date_id INTEGER,
    campaign_id TEXT,
    quantity INTEGER,
    unit_price REAL,
    discount_pct REAL,
    total_amount REAL,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (salesperson_id) REFERENCES dim_salespersons(salesperson_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
    FOREIGN KEY (campaign_id) REFERENCES dim_campaigns(campaign_id)
)''')

# 2. Campaign Performance Fact Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_campaign_performance (
    campaign_perf_id INTEGER PRIMARY KEY,
    campaign_id TEXT,
    date_id INTEGER,
    store_id TEXT,
    product_id TEXT,
    impressions INTEGER,
    clicks INTEGER,
    conversion_rate REAL,
    cost_per_acquisition REAL,
    revenue_generated REAL,
    FOREIGN KEY (campaign_id) REFERENCES dim_campaigns(campaign_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
)''')

# 3. Inventory Fact Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_id INTEGER PRIMARY KEY,
    product_id TEXT,
    store_id TEXT,
    date_id INTEGER,
    beginning_inventory INTEGER,
    inventory_received INTEGER,
    inventory_sold INTEGER,
    inventory_on_hand INTEGER,
    inventory_value REAL,
    days_of_supply INTEGER,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
)''')

# 4. Customer Activity Fact Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_customer_activity (
    activity_id INTEGER PRIMARY KEY,
    customer_id TEXT,
    store_id TEXT,
    date_id INTEGER,
    visit_count INTEGER,
    browse_time_minutes INTEGER,
    items_viewed INTEGER,
    cart_abandonment_count INTEGER,
    purchase_count INTEGER,
    total_spend REAL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
)''')

# Function to import data from CSV
def import_csv_to_table(file_name, table_name, column_mapping=None):
    csv_path = os.path.join(csv_dir, file_name)
    if not os.path.exists(csv_path):
        print(f"Warning: CSV file {csv_path} not found.")
        return
    
    print(f"Importing data from {csv_path} into {table_name}...")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # If column_mapping not provided, try to match headers with table columns
        if not column_mapping:
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = [column[1] for column in cursor.fetchall()]
            column_mapping = {}
            
            for i, header in enumerate(headers):
                header_lower = header.lower().replace(' ', '_')
                if header_lower in table_columns:
                    column_mapping[i] = header_lower
                elif header_lower + '_id' in table_columns:
                    column_mapping[i] = header_lower + '_id'
                elif header_lower + '_sk' in headers:
                    sk_index = headers.index(header_lower + '_sk')
                    column_mapping[sk_index] = header_lower + '_id'
        
        # Build SQL query based on column mapping
        columns = list(column_mapping.values())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Process and insert rows
        for row in reader:
            values = [row[i] for i in column_mapping.keys()]
            
            # Convert types
            for i, col in enumerate(columns):
                # Only convert to int/float if NOT an ID field and contains a numeric value
                if not col.endswith('_id') and values[i]:
                    if col in ['quantity', 'impressions', 'clicks', 'beginning_inventory', 
                             'inventory_received', 'inventory_sold', 'inventory_on_hand', 
                             'days_of_supply', 'visit_count', 'browse_time_minutes', 'items_viewed',
                             'cart_abandonment_count', 'purchase_count'] and values[i]:
                        try:
                            values[i] = int(values[i])
                        except ValueError:
                            print(f"Warning: Could not convert '{values[i]}' to integer for column {col}")
                    elif col in ['unit_price', 'discount_pct', 'total_amount', 'conversion_rate',
                               'cost_per_acquisition', 'revenue_generated', 'inventory_value',
                               'total_spend', 'campaign_budget'] and values[i]:
                        try:
                            values[i] = float(values[i])
                        except ValueError:
                            print(f"Warning: Could not convert '{values[i]}' to float for column {col}")
            
            try:
                cursor.execute(sql, values)
            except sqlite3.Error as e:
                print(f"Error inserting row: {e}")
                print(f"SQL: {sql}")
                print(f"Values: {values}")

# ---------------------- POPULATE DIMENSION TABLES ----------------------
# Import Products
import_csv_to_table('dim_products.csv', 'dim_products', {
    0: 'product_id',
    1: 'product_code',
    2: 'product_name',
    3: 'category',
    4: 'brand',
    5: 'location'
})

# Import Stores
import_csv_to_table('dim_stores.csv', 'dim_stores', {
    0: 'store_id',
    1: 'store_code',
    2: 'store_name',
    3: 'store_type',
    4: 'city',
    5: 'store_manager_sk'
})

# Import Customers
import_csv_to_table('dim_customers.csv', 'dim_customers', {
    0: 'customer_id',
    1: 'customer_code',
    2: 'first_name',
    3: 'last_name',
    4: 'customer_email',
    5: 'residential_location',
    6: 'customer_segment'
})

# Import Salespersons
salespersons_csv_path = os.path.join(csv_dir, 'dim_salespersons.csv')
if os.path.exists(salespersons_csv_path):
    import_csv_to_table('dim_salespersons.csv', 'dim_salespersons')

# Import Dates
dates_csv_path = os.path.join(csv_dir, 'dim_dates.csv')
if os.path.exists(dates_csv_path):
    with open(dates_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        for row in reader:
            full_date = row[0]
            date_id = int(row[1])
            year = int(row[2])
            month = int(row[3])
            day = int(row[4])
            weekday = int(row[5])
            quarter = int(row[6])
            
            # Determine if weekend
            is_weekend = 1 if weekday in [6, 7] else 0
            
            # Determine if holiday (simplified example)
            is_holiday = 1 if (month == 1 and day == 1) or (month == 12 and day == 25) else 0
            
            cursor.execute("""
                INSERT INTO dim_dates 
                (date_id, full_date, day, month, year, weekday, quarter, is_holiday, is_weekend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date_id, full_date, day, month, year, weekday, quarter, is_holiday, is_weekend))

# Import Campaigns
campaigns_csv_path = os.path.join(csv_dir, 'dim_campaigns.csv')
if os.path.exists(campaigns_csv_path):
    import_csv_to_table('dim_campaigns.csv', 'dim_campaigns')
else:
    # Generate sample campaign data if no CSV
    campaign_types = ["Promotional", "Seasonal", "Holiday", "Product Launch", "Clearance"]
    campaign_channels = ["Email", "Social Media", "TV", "Radio", "Print", "In-Store"]
    
    for i in range(1, 21):
        campaign_code = f"CAM_{i:05}"
        campaign_name = f"Campaign {i}"
        campaign_type = campaign_types[i % len(campaign_types)]
        campaign_channel = campaign_channels[i % len(campaign_channels)]
        
        # Generate dates
        year = 2024
        start_month = ((i-1) % 12) + 1
        end_month = min(start_month + 2, 12)
        
        start_date = f"{year}-{start_month:02}-01"
        end_date = f"{year}-{end_month:02}-28"
        
        budget = i * 5000.0
        
        cursor.execute("""
            INSERT INTO dim_campaigns
            (campaign_id, campaign_code, campaign_name, campaign_start_date, campaign_end_date,
            campaign_type, campaign_channel, campaign_budget)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (i, campaign_code, campaign_name, start_date, end_date, 
             campaign_type, campaign_channel, budget))

# ---------------------- POPULATE FACT TABLES ----------------------
# 1. Import Sales Fact
print("Loading fact_sales data...")
sales_csv_path = os.path.join(csv_dir, 'fact_sales_normalized.csv')
if os.path.exists(sales_csv_path):
    with open(sales_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Create a more robust header mapping
        header_map = {}
        for i, header in enumerate(headers):
            header_lower = header.lower()
            header_map[header_lower] = i
        
        print(f"CSV headers: {headers}")
        
        # Process each row with safer extraction of values
        for row in reader:
            try:
                # Extract values - keep IDs as strings
                sales_id = row[header_map['sales_sk']] if 'sales_sk' in header_map and header_map['sales_sk'] < len(row) else None
                product_id = row[header_map['product_sk']] if 'product_sk' in header_map and header_map['product_sk'] < len(row) else None
                store_id = row[header_map['store_sk']] if 'store_sk' in header_map and header_map['store_sk'] < len(row) else None
                customer_id = row[header_map['customer_sk']] if 'customer_sk' in header_map and header_map['customer_sk'] < len(row) else None
                salesperson_id = row[header_map['salesperson_sk']] if 'salesperson_sk' in header_map and header_map['salesperson_sk'] < len(row) else None
                
                # Handle date carefully - it could be a date_sk or sales_date field
                date_id = None
                if 'date_sk' in header_map and header_map['date_sk'] < len(row) and row[header_map['date_sk']]:
                    try:
                        date_id = int(row[header_map['date_sk']])
                    except ValueError:
                        print(f"Warning: Invalid date_id value: {row[header_map['date_sk']]}")
                elif 'sales_date' in header_map and header_map['sales_date'] < len(row) and row[header_map['sales_date']]:
                    # Extract date from timestamp if needed
                    date_str = row[header_map['sales_date']]
                    if 'T' in date_str:
                        date_str = date_str.split('T')[0]
                    cursor.execute("SELECT date_id FROM dim_dates WHERE full_date = ?", (date_str,))
                    result = cursor.fetchone()
                    if result:
                        date_id = result[0]
                
                campaign_id = row[header_map['campaign_sk']] if 'campaign_sk' in header_map and header_map['campaign_sk'] < len(row) and row[header_map['campaign_sk']] else None
                
                # Extract numeric fields carefully
                quantity = 1  # Default value
                if 'quantity' in header_map and header_map['quantity'] < len(row) and row[header_map['quantity']]:
                    try:
                        quantity = int(row[header_map['quantity']])
                    except ValueError:
                        print(f"Warning: Invalid quantity value: {row[header_map['quantity']]}, using default 1")
                
                unit_price = 0.0
                if 'unit_price' in header_map and header_map['unit_price'] < len(row) and row[header_map['unit_price']]:
                    try:
                        unit_price = float(row[header_map['unit_price']])
                    except ValueError:
                        print(f"Warning: Invalid unit price: {row[header_map['unit_price']]}, using default 0.0")
                
                discount_pct = 0.0
                if 'discount_pct' in header_map and header_map['discount_pct'] < len(row) and row[header_map['discount_pct']]:
                    try:
                        discount_pct = float(row[header_map['discount_pct']])
                    except ValueError:
                        print(f"Warning: Invalid discount percentage: {row[header_map['discount_pct']]}, using default 0.0")
                
                total_amount = 0.0
                if 'total_amount' in header_map and header_map['total_amount'] < len(row) and row[header_map['total_amount']]:
                    try:
                        total_amount = float(row[header_map['total_amount']])
                    except ValueError:
                        print(f"Warning: Invalid total amount: {row[header_map['total_amount']]}, using default 0.0")
                
                # Skip if essential fields are missing
                if sales_id is None or product_id is None or store_id is None:
                    print(f"Warning: Skipping row with missing essential fields: {row}")
                    continue
                
                cursor.execute("""
                    INSERT INTO fact_sales 
                    (sales_id, product_id, store_id, customer_id, salesperson_id, date_id, campaign_id,
                    quantity, unit_price, discount_pct, total_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (sales_id, product_id, store_id, customer_id, salesperson_id, date_id, campaign_id,
                     quantity, unit_price, discount_pct, total_amount))
            
            except Exception as e:
                print(f"Error processing row: {e}")
                print(f"Row data: {row}")
                continue

print("Loaded fact_sales data")

# ---------------------- GENERATE ADDITIONAL FACT TABLES ----------------------

# 2. Generate Campaign Performance Facts (derived from sales and campaigns)
print("Generating campaign performance facts...")
cursor.execute("SELECT campaign_id FROM dim_campaigns")
campaign_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT date_id FROM dim_dates ORDER BY full_date")
date_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT store_id FROM dim_stores")
store_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT product_id FROM dim_products")
product_ids = [row[0] for row in cursor.fetchall()]

# Generate campaign performance data
campaign_perf_id = 1
for campaign_id in campaign_ids:
    # For each campaign, generate data for multiple dates, stores, products
    for date_index in range(0, len(date_ids), max(1, len(date_ids) // 10)):  # Sample dates
        date_id = date_ids[date_index]
        
        for store_index in range(0, len(store_ids), max(1, len(store_ids) // 5)):  # Sample stores
            store_id = store_ids[store_index]
            
            for product_index in range(0, len(product_ids), max(1, len(product_ids) // 5)):  # Sample products
                product_id = product_ids[product_index]
                
                # Generate random metrics
                impressions = int(random.uniform(500, 10000))
                clicks = int(impressions * random.uniform(0.01, 0.2))
                conversion_rate = random.uniform(0.001, 0.1)
                cost_per_acquisition = random.uniform(5, 100)
                
                # Get revenue from sales if possible
                cursor.execute("""
                    SELECT SUM(total_amount) FROM fact_sales 
                    WHERE campaign_id = ? AND date_id = ? AND store_id = ? AND product_id = ?
                """, (campaign_id, date_id, store_id, product_id))
                
                result = cursor.fetchone()
                revenue_generated = result[0] if result and result[0] else random.uniform(100, 50000)
                
                cursor.execute("""
                    INSERT INTO fact_campaign_performance
                    (campaign_perf_id, campaign_id, date_id, store_id, product_id,
                    impressions, clicks, conversion_rate, cost_per_acquisition, revenue_generated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (campaign_perf_id, campaign_id, date_id, store_id, product_id,
                     impressions, clicks, conversion_rate, cost_per_acquisition, revenue_generated))
                
                campaign_perf_id += 1

print(f"Generated {campaign_perf_id-1} campaign performance facts")

# 3. Generate Inventory Facts
print("Generating inventory facts...")
inventory_id = 1

# Get unique product-store combinations from sales
cursor.execute("SELECT DISTINCT product_id, store_id FROM fact_sales")
product_store_pairs = cursor.fetchall()

# Generate inventory data for each product-store pair across time
for product_id, store_id in product_store_pairs:
    # Get all dates for which we have sales for this product-store
    cursor.execute("""
        SELECT DISTINCT date_id 
        FROM fact_sales 
        WHERE product_id = ? AND store_id = ?
        ORDER BY date_id
    """, (product_id, store_id))
    
    # Get all dates for consistent inventory tracking
    sale_dates = [row[0] for row in cursor.fetchall()]
    
    # Initialize inventory tracking
    beginning_inventory = int(random.uniform(50, 500))
    inventory_on_hand = beginning_inventory
    
    # For each date, generate inventory movement
    for date_id in date_ids:
        # If we have sales for this date, use that quantity sold
        cursor.execute("""
            SELECT SUM(quantity) FROM fact_sales 
            WHERE product_id = ? AND store_id = ? AND date_id = ?
        """, (product_id, store_id, date_id))
        
        result = cursor.fetchone()
        inventory_sold = result[0] if result and result[0] else 0
        
        # Generate random inventory received
        inventory_received = int(random.uniform(0, inventory_sold * 1.5 + 10))
        
        # Calculate ending inventory
        inventory_on_hand = max(0, beginning_inventory + inventory_received - inventory_sold)
        
        # Calculate inventory value ($) and days of supply
        avg_unit_price = random.uniform(10, 100)
        inventory_value = inventory_on_hand * avg_unit_price
        days_of_supply = int(inventory_on_hand / max(1, inventory_sold) * 30) if inventory_sold > 0 else 90
        
        cursor.execute("""
            INSERT INTO fact_inventory
            (inventory_id, product_id, store_id, date_id, beginning_inventory,
            inventory_received, inventory_sold, inventory_on_hand, inventory_value, days_of_supply)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (inventory_id, product_id, store_id, date_id, beginning_inventory,
             inventory_received, inventory_sold, inventory_on_hand, inventory_value, days_of_supply))
        
        inventory_id += 1
        
        # Next cycle's beginning inventory is this cycle's ending inventory
        beginning_inventory = inventory_on_hand
        
        # Stop after generating 20 days of data per product-store
        if inventory_id % 20 == 0:
            break

print(f"Generated {inventory_id-1} inventory facts")

# 4. Generate Customer Activity Facts
print("Generating customer activity facts...")
activity_id = 1

# Get unique customer-store combinations from sales
cursor.execute("SELECT DISTINCT customer_id, store_id FROM fact_sales LIMIT 1000")  # Limit to avoid generating too much data
customer_store_pairs = cursor.fetchall()

# Generate customer activity data for each customer-store pair
for customer_id, store_id in customer_store_pairs:
    # Get dates for which this customer made purchases
    cursor.execute("""
        SELECT date_id, SUM(total_amount) 
        FROM fact_sales 
        WHERE customer_id = ? AND store_id = ?
        GROUP BY date_id
        ORDER BY date_id
    """, (customer_id, store_id))
    
    customer_purchases = cursor.fetchall()
    
    # Generate activity data for purchase dates and some non-purchase dates
    for date_id, purchase_amount in customer_purchases:
        # For purchase dates, we know they visited and bought something
        visit_count = 1
        purchase_count = 1
        total_spend = purchase_amount
        
        # Generate other metrics
        browse_time_minutes = int(random.uniform(15, 120))
        items_viewed = int(random.uniform(3, 20))
        cart_abandonment_count = 0  # They completed their purchase
        
        cursor.execute("""
            INSERT INTO fact_customer_activity
            (activity_id, customer_id, store_id, date_id, visit_count,
            browse_time_minutes, items_viewed, cart_abandonment_count, purchase_count, total_spend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (activity_id, customer_id, store_id, date_id, visit_count,
             browse_time_minutes, items_viewed, cart_abandonment_count, purchase_count, total_spend))
        
        activity_id += 1
        
        # Also generate some non-purchase visits (browsing only)
        if random.random() < 0.3:  # 30% chance of having additional non-purchase visit
            # Find a date close to purchase date
            visit_offset = random.randint(-10, -1)  # Visit before purchase
            
            cursor.execute("SELECT date_id FROM dim_dates WHERE date_id = ?", (date_id + visit_offset,))
            visit_date = cursor.fetchone()
            
            if visit_date:
                visit_date_id = visit_date[0]
                
                visit_count = 1
                purchase_count = 0
                total_spend = 0
                
                browse_time_minutes = int(random.uniform(5, 60))
                items_viewed = int(random.uniform(1, 10))
                cart_abandonment_count = int(random.random() < 0.4)  # 40% chance of cart abandonment
                
                cursor.execute("""
                    INSERT INTO fact_customer_activity
                    (activity_id, customer_id, store_id, date_id, visit_count,
                    browse_time_minutes, items_viewed, cart_abandonment_count, purchase_count, total_spend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (activity_id, customer_id, store_id, visit_date_id, visit_count,
                     browse_time_minutes, items_viewed, cart_abandonment_count, purchase_count, total_spend))
                
                activity_id += 1

print(f"Generated {activity_id-1} customer activity facts")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Fact constellation schema creation and data import completed!")
              