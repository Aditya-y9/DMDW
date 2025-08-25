def dice_operation(conn, conditions):
    """
    Dice operation: Select data based on multiple dimension conditions
    """
    print(f"\n=== DICE OPERATION: Multiple Conditions ===")
    
    query = """
    SELECT 
        p.category,
        p.subcategory,
        s.store_type,
        c.segment,
        d.quarter,
        d.year,
        COUNT(*) as transaction_count,
        SUM(f.quantity) as total_quantity,
        SUM(f.total_amount) as total_sales,
        AVG(f.total_amount) as avg_transaction
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_stores s ON f.store_id = s.store_id
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_dates d ON f.date_id = d.date_id
    WHERE p.category IN ('Electronics', 'Furniture')
    AND s.store_type IN ('Flagship', 'Regular')
    AND d.year = 2024
    AND c.segment = 'Corporate'
    GROUP BY p.category, p.subcategory, s.store_type, c.segment, d.quarter, d.year
    ORDER BY total_sales DESC
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    return df

import sqlite3
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
# Execute dice operation
dice_result = dice_operation(conn, {})
