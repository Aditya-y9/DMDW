import sqlite3
import pandas as pd
def slice_operation(conn, dimension, value):
    """
    Slice operation: Select data for a specific dimension value
    """
    print(f"\n=== SLICE OPERATION: {dimension} = {value} ===")
    
    if dimension.upper() == 'CATEGORY':
        query = """
        SELECT 
            p.category,
            p.product_name,
            s.store_name,
            d.year,
            SUM(f.quantity) as total_quantity,
            SUM(f.total_amount) as total_sales
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        JOIN dim_stores s ON f.store_id = s.store_id
        JOIN dim_dates d ON f.date_id = d.date_id
        WHERE p.category = ?
        GROUP BY p.category, p.product_name, s.store_name, d.year
        ORDER BY total_sales DESC
        LIMIT 10
        """
    elif dimension.upper() == 'YEAR':
        query = """
        SELECT 
            d.year,
            p.category,
            COUNT(*) as transaction_count,
            SUM(f.quantity) as total_quantity,
            SUM(f.total_amount) as total_sales,
            AVG(f.total_amount) as avg_sale
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        JOIN dim_dates d ON f.date_id = d.date_id
        WHERE d.year = ?
        GROUP BY d.year, p.category
        ORDER BY total_sales DESC
        """
    
    df = pd.read_sql_query(query, conn, params=[value])
    print(df.to_string(index=False))
    return df

conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
# Execute slice operations
slice_electronics = slice_operation(conn, 'CATEGORY', 'Electronics')
slice_2024 = slice_operation(conn, 'YEAR', 2024)
