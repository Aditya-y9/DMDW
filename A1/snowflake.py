import sqlite3
import csv
import os
import re

# Database file path
db_path = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\snowflake.db'

# CSV files directory
csv_dir = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\Dataset\\'

# Delete existing database if it exists
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Removed existing database: {db_path}")

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Creating snowflake schema tables...")

# ---------------------- PRODUCT DIMENSION HIERARCHY ----------------------
# Create normalized Product dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_product_categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_product_brands (
    brand_id INTEGER PRIMARY KEY,
    brand_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_product_locations (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_code TEXT,
    product_name TEXT,
    category_id INTEGER,
    brand_id INTEGER,
    location_id INTEGER,
    FOREIGN KEY (category_id) REFERENCES dim_product_categories(category_id),
    FOREIGN KEY (brand_id) REFERENCES dim_product_brands(brand_id),
    FOREIGN KEY (location_id) REFERENCES dim_product_locations(location_id)
)''')

# ---------------------- STORE DIMENSION HIERARCHY ----------------------
# Create normalized Store dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_store_types (
    store_type_id INTEGER PRIMARY KEY,
    store_type_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_store_cities (
    city_id INTEGER PRIMARY KEY,
    city_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id INTEGER PRIMARY KEY,
    store_code TEXT,
    store_name TEXT,
    store_type_id INTEGER,
    city_id INTEGER,
    store_manager_sk INTEGER,
    FOREIGN KEY (store_type_id) REFERENCES dim_store_types(store_type_id),
    FOREIGN KEY (city_id) REFERENCES dim_store_cities(city_id)
)''')

# ---------------------- CUSTOMER DIMENSION HIERARCHY ----------------------
# Create normalized Customer dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_customer_cities (
    city_id INTEGER PRIMARY KEY,
    city_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_customer_segments (
    segment_id INTEGER PRIMARY KEY,
    segment_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_code TEXT,
    first_name TEXT,
    last_name TEXT,
    customer_email TEXT,
    city_id INTEGER,
    segment_id INTEGER,
    FOREIGN KEY (city_id) REFERENCES dim_customer_cities(city_id),
    FOREIGN KEY (segment_id) REFERENCES dim_customer_segments(segment_id)
)''')

# ---------------------- SALESPERSON DIMENSION HIERARCHY ----------------------
# Create normalized Salesperson dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_salesperson_regions (
    region_id INTEGER PRIMARY KEY,
    region_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_salesperson_designations (
    designation_id INTEGER PRIMARY KEY,
    designation_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_salespersons (
    salesperson_id INTEGER PRIMARY KEY,
    salesperson_code TEXT,
    salesperson_name TEXT,
    salesperson_email TEXT,
    salesperson_phone TEXT,
    designation_id INTEGER,
    region_id INTEGER,
    FOREIGN KEY (designation_id) REFERENCES dim_salesperson_designations(designation_id),
    FOREIGN KEY (region_id) REFERENCES dim_salesperson_regions(region_id)
)''')

# ---------------------- DATE DIMENSION HIERARCHY ----------------------
# Create normalized Date dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_years (
    year_id INTEGER PRIMARY KEY,
    year_value INTEGER
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_quarters (
    quarter_id INTEGER PRIMARY KEY,
    year_id INTEGER,
    quarter_value INTEGER,
    FOREIGN KEY (year_id) REFERENCES dim_years(year_id)
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_months (
    month_id INTEGER PRIMARY KEY,
    quarter_id INTEGER,
    month_value INTEGER,
    month_name TEXT,
    FOREIGN KEY (quarter_id) REFERENCES dim_quarters(quarter_id)
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT,
    day_number INTEGER,
    month_id INTEGER,
    weekday INTEGER,
    is_holiday INTEGER,
    is_weekend INTEGER,
    FOREIGN KEY (month_id) REFERENCES dim_months(month_id)
)''')

# ---------------------- CAMPAIGN DIMENSION HIERARCHY ----------------------
# Create normalized Campaign dimension tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_campaign_types (
    campaign_type_id INTEGER PRIMARY KEY,
    campaign_type_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_campaign_channels (
    campaign_channel_id INTEGER PRIMARY KEY,
    campaign_channel_name TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id INTEGER PRIMARY KEY,
    campaign_code TEXT,
    campaign_name TEXT,
    campaign_start_date TEXT,
    campaign_end_date TEXT,
    campaign_type_id INTEGER,
    campaign_channel_id INTEGER,
    campaign_budget REAL,
    FOREIGN KEY (campaign_type_id) REFERENCES dim_campaign_types(campaign_type_id),
    FOREIGN KEY (campaign_channel_id) REFERENCES dim_campaign_channels(campaign_channel_id)
)''')

# ---------------------- FACT TABLE ----------------------
# Create fact table
cursor.execute('''
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    store_id INTEGER,
    customer_id INTEGER,
    salesperson_id INTEGER,
    date_id INTEGER,
    campaign_id INTEGER,
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

# ---------------------- ETL FUNCTIONS ----------------------
# Function to extract unique values from a CSV column
def extract_unique_values(file_name, column_index, header=True):
    csv_path = os.path.join(csv_dir, file_name)
    unique_values = set()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        if header:
            next(reader)  # Skip header row
        for row in reader:
            if len(row) > column_index:
                unique_values.add(row[column_index])
    
    return sorted(list(unique_values))

# Function to load normalized data into dimension tables
def load_normalized_dimension(source_file, source_column_index, dim_table, value_column, header=True):
    values = extract_unique_values(source_file, source_column_index, header)
    
    # Insert values into dimension table
    for i, value in enumerate(values, 1):
        cursor.execute(f"INSERT INTO {dim_table} (rowid, {value_column}) VALUES (?, ?)", (i, value))
    
    # Create a mapping from value to ID
    value_to_id = {value: i for i, value in enumerate(values, 1)}
    return value_to_id

# ---------------------- PRODUCT DIMENSION ETL ----------------------
print("Processing Product dimension...")

# Load normalized product categories
category_mapping = load_normalized_dimension('dim_products.csv', 3, 'dim_product_categories', 'category_name')
print(f"Loaded {len(category_mapping)} product categories")

# Load normalized product brands
brand_mapping = load_normalized_dimension('dim_products.csv', 4, 'dim_product_brands', 'brand_name')
print(f"Loaded {len(brand_mapping)} product brands")

# Load normalized product locations
location_mapping = load_normalized_dimension('dim_products.csv', 5, 'dim_product_locations', 'location_name')
print(f"Loaded {len(location_mapping)} product locations")

# Load products with foreign keys
products_csv_path = os.path.join(csv_dir, 'dim_products.csv')
with open(products_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    for row in reader:
        product_id = int(row[0])
        product_code = row[1]
        product_name = row[2]
        category = row[3]
        brand = row[4]
        location = row[5]
        
        category_id = category_mapping.get(category, None)
        brand_id = brand_mapping.get(brand, None)
        location_id = location_mapping.get(location, None)
        
        cursor.execute("""
            INSERT INTO dim_products 
            (product_id, product_code, product_name, category_id, brand_id, location_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, product_code, product_name, category_id, brand_id, location_id))

print("Loaded products with normalized dimensions")

# ---------------------- STORE DIMENSION ETL ----------------------
print("Processing Store dimension...")

# Load normalized store types
store_type_mapping = load_normalized_dimension('dim_stores.csv', 3, 'dim_store_types', 'store_type_name')
print(f"Loaded {len(store_type_mapping)} store types")

# Load normalized store cities
store_city_mapping = load_normalized_dimension('dim_stores.csv', 4, 'dim_store_cities', 'city_name')
print(f"Loaded {len(store_city_mapping)} store cities")

# Load stores with foreign keys
stores_csv_path = os.path.join(csv_dir, 'dim_stores.csv')
with open(stores_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    for row in reader:
        store_id = int(row[0])
        store_code = row[1]
        store_name = row[2]
        store_type = row[3]
        city = row[4]
        store_manager_sk = int(row[5]) if row[5].strip() else None
        
        store_type_id = store_type_mapping.get(store_type, None)
        city_id = store_city_mapping.get(city, None)
        
        cursor.execute("""
            INSERT INTO dim_stores 
            (store_id, store_code, store_name, store_type_id, city_id, store_manager_sk)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (store_id, store_code, store_name, store_type_id, city_id, store_manager_sk))

print("Loaded stores with normalized dimensions")

# ---------------------- CUSTOMER DIMENSION ETL ----------------------
print("Processing Customer dimension...")

# Load normalized customer cities
customer_city_mapping = load_normalized_dimension('dim_customers.csv', 5, 'dim_customer_cities', 'city_name')
print(f"Loaded {len(customer_city_mapping)} customer cities")

# Load normalized customer segments
customer_segment_mapping = load_normalized_dimension('dim_customers.csv', 6, 'dim_customer_segments', 'segment_name')
print(f"Loaded {len(customer_segment_mapping)} customer segments")

# Load customers with foreign keys
customers_csv_path = os.path.join(csv_dir, 'dim_customers.csv')
with open(customers_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    for row in reader:
        customer_id = int(row[0])
        customer_code = row[1]
        first_name = row[2]
        last_name = row[3]
        email = row[4]
        city = row[5]
        segment = row[6]
        
        city_id = customer_city_mapping.get(city, None)
        segment_id = customer_segment_mapping.get(segment, None)
        
        cursor.execute("""
            INSERT INTO dim_customers 
            (customer_id, customer_code, first_name, last_name, customer_email, city_id, segment_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (customer_id, customer_code, first_name, last_name, email, city_id, segment_id))

print("Loaded customers with normalized dimensions")

# ---------------------- DATE DIMENSION ETL ----------------------
print("Processing Date dimension...")

# Load normalized years
dates_csv_path = os.path.join(csv_dir, 'dim_dates.csv')
year_mapping = {}
quarter_mapping = {}
month_mapping = {}

with open(dates_csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    # First pass: collect all years, quarters, months
    years = set()
    quarters_by_year = {}
    months_by_quarter = {}
    
    for row in reader:
        year = int(row[2])
        quarter = int(row[6])
        month = int(row[3])
        
        years.add(year)
        
        year_quarter = (year, quarter)
        if year_quarter not in quarters_by_year:
            quarters_by_year[year_quarter] = True
        
        quarter_month = (year, quarter, month)
        if quarter_month not in months_by_quarter:
            months_by_quarter[quarter_month] = True

    # Reset file pointer to beginning
    f.seek(0)
    next(reader)  # Skip header again

    # Insert years
    for i, year in enumerate(sorted(years), 1):
        cursor.execute("INSERT INTO dim_years (year_id, year_value) VALUES (?, ?)", 
                      (i, year))
        year_mapping[year] = i
    
    # Insert quarters
    quarter_id = 1
    for year, quarter in sorted(quarters_by_year.keys()):
        year_id = year_mapping[year]
        cursor.execute("INSERT INTO dim_quarters (quarter_id, year_id, quarter_value) VALUES (?, ?, ?)", 
                      (quarter_id, year_id, quarter))
        quarter_mapping[(year, quarter)] = quarter_id
        quarter_id += 1
    
    # Insert months
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
        7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    
    month_id = 1
    for year, quarter, month in sorted(months_by_quarter.keys()):
        quarter_id = quarter_mapping[(year, quarter)]
        month_name = month_names.get(month, f'Month {month}')
        
        cursor.execute("""
            INSERT INTO dim_months (month_id, quarter_id, month_value, month_name) 
            VALUES (?, ?, ?, ?)
        """, (month_id, quarter_id, month, month_name))
        
        month_mapping[(year, quarter, month)] = month_id
        month_id += 1
    
    # Reset file pointer to beginning again
    f.seek(0)
    next(reader)  # Skip header again
    
    # Insert dates
    for row in reader:
        date_id = int(row[1])
        full_date = row[0]
        year = int(row[2])
        month = int(row[3])
        day = int(row[4])
        weekday = int(row[5])
        quarter = int(row[6])
        
        month_id = month_mapping.get((year, quarter, month))
        is_weekend = 1 if weekday in [6, 7] else 0  # Assume 6=Saturday, 7=Sunday
        
        cursor.execute("""
            INSERT INTO dim_dates 
            (date_id, full_date, day_number, month_id, weekday, is_holiday, is_weekend)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (date_id, full_date, day, month_id, weekday, 0, is_weekend))

print("Loaded dates with normalized dimensions")

# ---------------------- CAMPAIGN DIMENSION ETL ----------------------
print("Processing Campaign dimension...")

# For simplicity, we'll synthesize campaign types and channels
campaign_types = [
    "Promotional", "Seasonal", "Holiday", "Product Launch", "Clearance",
    "Brand Awareness", "Customer Retention", "New Customer Acquisition"
]

campaign_channels = [
    "Email", "Social Media", "TV", "Radio", "Print", "In-Store",
    "Direct Mail", "SMS", "Web", "Mobile App"
]

# Insert campaign types
for i, campaign_type in enumerate(campaign_types, 1):
    cursor.execute("INSERT INTO dim_campaign_types (campaign_type_id, campaign_type_name) VALUES (?, ?)",
                  (i, campaign_type))

# Insert campaign channels
for i, campaign_channel in enumerate(campaign_channels, 1):
    cursor.execute("INSERT INTO dim_campaign_channels (campaign_channel_id, campaign_channel_name) VALUES (?, ?)",
                  (i, campaign_channel))

# Create mappings for campaign types and channels
campaign_type_mapping = {name: i for i, name in enumerate(campaign_types, 1)}
campaign_channel_mapping = {name: i for i, name in enumerate(campaign_channels, 1)}

# Load campaigns with foreign keys (using a pattern to assign types and channels)
campaigns_csv_path = os.path.join(csv_dir, 'dim_campaigns.csv')
if os.path.exists(campaigns_csv_path):
    with open(campaigns_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        for row in reader:
            # Use modulo to assign types and channels for demonstration
            campaign_id = int(row[0])
            campaign_code = row[1] if len(row) > 1 else f"CAM_{campaign_id:05}"
            campaign_name = row[2] if len(row) > 2 else f"Campaign {campaign_id}"
            
            # For start and end dates, use default values if not provided
            campaign_start_date = row[3] if len(row) > 3 and row[3] else "2024-01-01"
            campaign_end_date = row[4] if len(row) > 4 and row[4] else "2024-12-31"
            
            # Assign campaign type and channel based on id
            campaign_type_id = (campaign_id % len(campaign_types)) + 1
            campaign_channel_id = (campaign_id % len(campaign_channels)) + 1
            
            # Budget (use value if provided, otherwise generate)
            campaign_budget = float(row[5]) if len(row) > 5 and row[5] else campaign_id * 1000.0
            
            cursor.execute("""
                INSERT INTO dim_campaigns 
                (campaign_id, campaign_code, campaign_name, campaign_start_date, campaign_end_date, 
                campaign_type_id, campaign_channel_id, campaign_budget)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (campaign_id, campaign_code, campaign_name, campaign_start_date, campaign_end_date,
                 campaign_type_id, campaign_channel_id, campaign_budget))

print("Loaded campaigns with normalized dimensions")

# ---------------------- SALESPERSON DIMENSION ETL ----------------------
print("Processing Salesperson dimension...")

# Load normalized salesperson regions and designations if data is available
salesperson_regions = [
    "North", "South", "East", "West", "Central", "Northeast", "Southeast", 
    "Northwest", "Southwest", "International"
]

salesperson_designations = [
    "Junior Sales Representative", "Sales Representative", "Senior Sales Representative",
    "Sales Manager", "Regional Sales Manager", "Sales Director", "VP of Sales"
]

# Insert salesperson regions
for i, region in enumerate(salesperson_regions, 1):
    cursor.execute("INSERT INTO dim_salesperson_regions (region_id, region_name) VALUES (?, ?)",
                  (i, region))

# Insert salesperson designations
for i, designation in enumerate(salesperson_designations, 1):
    cursor.execute("INSERT INTO dim_salesperson_designations (designation_id, designation_name) VALUES (?, ?)",
                  (i, designation))

# Create mappings
region_mapping = {name: i for i, name in enumerate(salesperson_regions, 1)}
designation_mapping = {name: i for i, name in enumerate(salesperson_designations, 1)}

# Load salespersons with foreign keys
salespersons_csv_path = os.path.join(csv_dir, 'dim_salespersons.csv')
if os.path.exists(salespersons_csv_path):
    with open(salespersons_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        for row in reader:
            # Extract available fields
            salesperson_id = int(row[0])
            salesperson_code = row[1] if len(row) > 1 else f"SP_{salesperson_id:05}"
            salesperson_name = row[2] if len(row) > 2 else f"Salesperson {salesperson_id}"
            
            # Email and phone fields if available
            salesperson_email = row[3] if len(row) > 3 else f"salesperson{salesperson_id}@example.com"
            salesperson_phone = row[4] if len(row) > 4 else f"555-{salesperson_id:04}"
            
            # Assign region and designation based on id
            designation_id = (salesperson_id % len(salesperson_designations)) + 1
            region_id = (salesperson_id % len(salesperson_regions)) + 1
            
            cursor.execute("""
                INSERT INTO dim_salespersons 
                (salesperson_id, salesperson_code, salesperson_name, salesperson_email, 
                salesperson_phone, designation_id, region_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (salesperson_id, salesperson_code, salesperson_name, salesperson_email,
                 salesperson_phone, designation_id, region_id))

print("Loaded salespersons with normalized dimensions")

# ---------------------- FACT TABLE ETL ----------------------
print("Processing Fact Sales...")

# Load fact_sales
fact_sales_csv_path = os.path.join(csv_dir, 'fact_sales_normalized.csv')
if os.path.exists(fact_sales_csv_path):
    with open(fact_sales_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Create a better header mapping - case insensitive and checking for variations
        header_map = {}
        for i, header in enumerate(headers):
            header_lower = header.lower()
            header_map[header_lower] = i
            # Add common variations
            if header_lower == 'sales_sk':
                header_map['sales_id'] = i
            elif header_lower == 'product_sk':
                header_map['product_id'] = i
            elif header_lower == 'store_sk':
                header_map['store_id'] = i
            elif header_lower == 'customer_sk':
                header_map['customer_id'] = i
            elif header_lower == 'salesperson_sk':
                header_map['salesperson_id'] = i
            elif header_lower == 'date_sk':
                header_map['date_id'] = i
            elif header_lower == 'campaign_sk':
                header_map['campaign_id'] = i
        
        print(f"Detected columns: {headers}")
        
        # Process each row with safer access to columns
        for row in reader:
            try:
                # Extract ID fields safely
                sales_id = int(row[header_map['sales_sk']]) if 'sales_sk' in header_map and header_map['sales_sk'] < len(row) else None
                product_id = int(row[header_map['product_sk']]) if 'product_sk' in header_map and header_map['product_sk'] < len(row) else None
                store_id = int(row[header_map['store_sk']]) if 'store_sk' in header_map and header_map['store_sk'] < len(row) else None
                customer_id = int(row[header_map['customer_sk']]) if 'customer_sk' in header_map and header_map['customer_sk'] < len(row) else None
                salesperson_id = int(row[header_map['salesperson_sk']]) if 'salesperson_sk' in header_map and header_map['salesperson_sk'] < len(row) else None
                
                # Handle date_id safely
                date_id = None
                if 'date_sk' in header_map and header_map['date_sk'] < len(row) and row[header_map['date_sk']]:
                    date_id = int(row[header_map['date_sk']])
                elif 'sales_date' in header_map and header_map['sales_date'] < len(row) and row[header_map['sales_date']]:
                    # Extract date from timestamp if needed
                    date_str = row[header_map['sales_date']]
                    if 'T' in date_str:
                        date_str = date_str.split('T')[0]
                    cursor.execute("SELECT date_id FROM dim_dates WHERE full_date = ?", (date_str,))
                    result = cursor.fetchone()
                    if result:
                        date_id = result[0]
                
                campaign_id = int(row[header_map['campaign_sk']]) if 'campaign_sk' in header_map and header_map['campaign_sk'] < len(row) and row[header_map['campaign_sk']] else None
                
                # Extract numeric values safely
                quantity = None
                if 'quantity' in header_map and header_map['quantity'] < len(row) and row[header_map['quantity']]:
                    try:
                        quantity = int(row[header_map['quantity']])
                    except ValueError:
                        # If we can't convert to int, use default
                        print(f"Warning: Invalid quantity value: {row[header_map['quantity']]}, using default")
                        quantity = 1
                else:
                    quantity = 1  # Default quantity
                
                # Handle other numeric fields similarly
                unit_price = 0.0
                if 'unit_price' in header_map and header_map['unit_price'] < len(row) and row[header_map['unit_price']]:
                    try:
                        unit_price = float(row[header_map['unit_price']])
                    except ValueError:
                        print(f"Warning: Invalid unit price value: {row[header_map['unit_price']]}, using default")
                
                discount_pct = 0.0
                if 'discount_pct' in header_map and header_map['discount_pct'] < len(row) and row[header_map['discount_pct']]:
                    try:
                        discount_pct = float(row[header_map['discount_pct']])
                    except ValueError:
                        print(f"Warning: Invalid discount value: {row[header_map['discount_pct']]}, using default")
                
                total_amount = 0.0
                if 'total_amount' in header_map and header_map['total_amount'] < len(row) and row[header_map['total_amount']]:
                    try:
                        total_amount = float(row[header_map['total_amount']])
                    except ValueError:
                        print(f"Warning: Invalid total amount value: {row[header_map['total_amount']]}, using default")
                
                # Skip if we're missing key IDs
                if sales_id is None or product_id is None or store_id is None:
                    print(f"Warning: Skipping row with missing required IDs: {row}")
                    continue
                
                cursor.execute("""
                    INSERT INTO fact_sales 
                    (sales_id, product_id, store_id, customer_id, salesperson_id, date_id, campaign_id,
                    quantity, unit_price, discount_pct, total_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (sales_id, product_id, store_id, customer_id, salesperson_id, date_id, campaign_id,
                     quantity, unit_price, discount_pct, total_amount))
            
            except Exception as e:
                # Print detailed error info but continue processing
                print(f"Error processing row: {e}")
                print(f"Row data: {row}")
                print(f"Header map: {header_map}")
                continue

print("Loaded fact_sales data")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Snowflake schema creation and data import completed!")
