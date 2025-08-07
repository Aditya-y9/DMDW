import sqlite3
import csv
import os


db_path = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\star.db'


csv_dir = 'c:\\OneDrive - it.vjti.ac.in\\InITtoWinIT\\Acads\\DMDW\\A1\\Dataset\\'


if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Removed existing database: {db_path}")


conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Creating star schema tables...")


cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_code TEXT,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    location TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id INTEGER PRIMARY KEY,
    store_code TEXT,
    store_name TEXT,
    store_type TEXT,
    city TEXT,
    size_or_value INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_code TEXT,
    customer_name TEXT,
    customer_email TEXT,
    customer_phone TEXT,
    city TEXT,
    state TEXT,
    country TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_salespersons (
    salesperson_id INTEGER PRIMARY KEY,
    salesperson_code TEXT,
    salesperson_name TEXT,
    salesperson_email TEXT,
    salesperson_phone TEXT,
    designation TEXT,
    region TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT,
    day_of_week TEXT,
    day_number INTEGER,
    month TEXT,
    month_number INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_holiday INTEGER,
    is_weekend INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id INTEGER PRIMARY KEY,
    campaign_code TEXT,
    campaign_name TEXT,
    campaign_start_date TEXT,
    campaign_end_date TEXT,
    campaign_type TEXT,
    campaign_channel TEXT,
    campaign_budget REAL
)
''')


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
)
''')


def import_csv_to_table(file_name, table_name):
    csv_path = os.path.join(csv_dir, file_name)
    print(f"Importing data from {csv_path} into {table_name}...")
    
    
    mappings = {
        'dim_products': {
            'product_sk': 'product_id',
            'product_id': 'product_code',
            'product_name': 'product_name',
            'category': 'category',
            'brand': 'brand',
            'origin_location': 'location'
        },
        'dim_stores': {
            'store_sk': 'store_id',
            'store_id': 'store_code',
            'store_name': 'store_name',
            'store_type': 'store_type',
            'store_location': 'city',
            
        },
        'dim_customers': {
            'customer_sk': 'customer_id',
            'customer_id': 'customer_code',
            
            'email': 'customer_email',
            
        },
        'dim_salespersons': {
            'salesperson_sk': 'salesperson_id',
            'salesperson_id': 'salesperson_code',
            'salesperson_name': 'salesperson_name',
            'salesperson_role': 'designation',
        },
        'dim_dates': {
            'date_sk': 'date_id',
            'full_date': 'full_date',
            'weekday': 'day_of_week',
            'day': 'day_number',
            'month': 'month',
            'year': 'year',
            'quarter': 'quarter',
        },
        'dim_campaigns': {
            'campaign_sk': 'campaign_id',
            'campaign_id': 'campaign_code',
            'campaign_name': 'campaign_name',
            'campaign_budget': 'campaign_budget',
        },
        'fact_sales': {
            'sales_sk': 'sales_id',
            'product_sk': 'product_id',
            'store_sk': 'store_id',
            'customer_sk': 'customer_id',
            'salesperson_sk': 'salesperson_id',
            'campaign_sk': 'campaign_id',
            'total_amount': 'total_amount',
        }
    }
    
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        csv_headers = next(reader)
        
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        table_columns = [column[1] for column in cursor.fetchall()]
        table_types = {column[1]: column[2] for column in cursor.fetchall()}
        
        
        print(f"CSV headers ({len(csv_headers)}): {csv_headers}")
        print(f"Table columns ({len(table_columns)}): {table_columns}")
        
        
        column_mapping = []
        table_mapping = mappings.get(table_name, {})
        
        for i, header in enumerate(csv_headers):
            
            if header in table_mapping:
                db_column = table_mapping[header]
                if db_column in table_columns:
                    column_mapping.append((i, db_column))
        
        if not column_mapping:
            print(f"ERROR: No matching columns found between CSV and table")
            return
            
        
        mapped_table_cols = [mapping[1] for mapping in column_mapping]
        mapped_col_str = ', '.join(mapped_table_cols)
        placeholders = ', '.join(['?' for _ in mapped_table_cols])
        
        sql = f"INSERT INTO {table_name} ({mapped_col_str}) VALUES ({placeholders})"
        print(f"Using SQL: {sql}")
        print(f"Column mapping: {column_mapping}")
        
        
        data_to_insert = []
        for row in reader:
            
            mapped_values = []
            
            for csv_idx, db_col in column_mapping:
                value = row[csv_idx]
                
                
                if db_col.endswith('_id') and value:  
                    try:
                        value = int(value)
                    except ValueError:
                        print(f"Warning: Could not convert '{value}' to integer for column {db_col}")
                elif db_col in ['quantity', 'is_holiday', 'is_weekend', 'day_number', 'month_number', 'quarter', 'year', 'size_or_value'] and value:
                    try:
                        value = int(value)
                    except ValueError:
                        print(f"Warning: Could not convert '{value}' to integer for column {db_col}")
                elif db_col in ['unit_price', 'total_amount', 'discount_pct', 'campaign_budget'] and value:
                    try:
                        value = float(value)
                    except ValueError:
                        print(f"Warning: Could not convert '{value}' to float for column {db_col}")
                
                mapped_values.append(value)
            
            data_to_insert.append(mapped_values)
        
        try:
            cursor.executemany(sql, data_to_insert)
            print(f"Successfully imported {len(data_to_insert)} rows into {table_name}")
        except Exception as e:
            print(f"Error importing data into {table_name}: {e}")
            
            if data_to_insert:
                print(f"Sample row: {data_to_insert[0]}")


import_csv_to_table('dim_products.csv', 'dim_products')
import_csv_to_table('dim_stores.csv', 'dim_stores')
import_csv_to_table('dim_customers.csv', 'dim_customers')
import_csv_to_table('dim_salespersons.csv', 'dim_salespersons')
import_csv_to_table('dim_dates.csv', 'dim_dates')
import_csv_to_table('dim_campaigns.csv', 'dim_campaigns')


def import_fact_sales():
    csv_path = os.path.join(csv_dir, 'fact_sales_normalized.csv')
    print(f"Importing data from {csv_path} into fact_sales...")
    
    
    cursor.execute("SELECT date_id, full_date FROM dim_dates")
    date_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        
        sales_date_idx = headers.index('sales_date') if 'sales_date' in headers else -1
        
        
        column_indices = {
            'sales_id': headers.index('sales_sk') if 'sales_sk' in headers else -1,
            'product_id': headers.index('product_sk') if 'product_sk' in headers else -1,
            'store_id': headers.index('store_sk') if 'store_sk' in headers else -1,
            'customer_id': headers.index('customer_sk') if 'customer_sk' in headers else -1,
            'salesperson_id': headers.index('salesperson_sk') if 'salesperson_sk' in headers else -1,
            'campaign_id': headers.index('campaign_sk') if 'campaign_sk' in headers else -1,
            'total_amount': headers.index('total_amount') if 'total_amount' in headers else -1
        }
        
        
        columns = ['sales_id', 'product_id', 'store_id', 'customer_id', 
                   'salesperson_id', 'date_id', 'campaign_id', 'total_amount']
        sql = f"INSERT INTO fact_sales ({', '.join(columns)}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        
        data_to_insert = []
        for row in reader:
            values = []
            
            for col in columns:
                if col == 'date_id' and sales_date_idx != -1:
                    
                    timestamp = row[sales_date_idx]
                    
                    date_value = timestamp.split('T')[0] if 'T' in timestamp else timestamp
                    
                    date_id = date_mapping.get(date_value)
                    if date_id is None:
                        print(f"Warning: No date_id found for date: {date_value} (from {timestamp})")
                        date_id = 0  
                    values.append(date_id)
                elif col != 'date_id' and column_indices[col] != -1:
                    value = row[column_indices[col]]
                    
                    if col.endswith('_id'):
                        try:
                            value = int(value)
                        except ValueError:
                            print(f"Warning: Could not convert '{value}' to integer for column {col}")
                            value = 0
                    elif col == 'total_amount':
                        try:
                            value = float(value)
                        except ValueError:
                            print(f"Warning: Could not convert '{value}' to float for column {col}")
                            value = 0.0
                    values.append(value)
                else:
                    
                    if col.endswith('_id'):
                        values.append(0)
                    elif col == 'total_amount':
                        values.append(0.0)
                    else:
                        values.append(None)
            
            data_to_insert.append(values)
        
        try:
            cursor.executemany(sql, data_to_insert)
            print(f"Successfully imported {len(data_to_insert)} rows into fact_sales")
        except Exception as e:
            print(f"Error importing data into fact_sales: {e}")
            if data_to_insert:
                print(f"Sample row: {data_to_insert[0]}")


import_fact_sales()


conn.commit()
conn.close()

print("Star schema creation and data import completed!")