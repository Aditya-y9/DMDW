def pivot_operation(conn, rows, columns, values, aggregation='SUM'):
    """
    Pivot operation: Rotate data cube for different perspectives
    """
    print(f"\n=== PIVOT OPERATION: {rows} vs {columns} ({aggregation} of {values}) ===")
    
    # Create base query for pivot
    base_query = """
    SELECT 
        CASE 
            WHEN ? = 'category' THEN p.category
            WHEN ? = 'quarter' THEN d.quarter
            WHEN ? = 'year' THEN CAST(d.year AS TEXT)
            WHEN ? = 'segment' THEN c.segment
            WHEN ? = 'store_type' THEN s.store_type
        END as row_dim,
        CASE 
            WHEN ? = 'category' THEN p.category
            WHEN ? = 'quarter' THEN d.quarter  
            WHEN ? = 'year' THEN CAST(d.year AS TEXT)
            WHEN ? = 'segment' THEN c.segment
            WHEN ? = 'store_type' THEN s.store_type
        END as col_dim,
        CASE 
            WHEN ? = 'total_amount' THEN f.total_amount
            WHEN ? = 'quantity' THEN CAST(f.quantity AS REAL)
        END as value_measure
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_stores s ON f.store_id = s.store_id  
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_dates d ON f.date_id = d.date_id
    """
    
    # Execute query and create pivot table
    params = [rows, rows, rows, rows, rows, columns, columns, columns, columns, columns, values, values]
    df = pd.read_sql_query(base_query, conn, params=params)
    
    # Create pivot table
    if aggregation.upper() == 'SUM':
        pivot_df = df.pivot_table(
            index='row_dim', 
            columns='col_dim', 
            values='value_measure', 
            aggfunc='sum', 
            fill_value=0
        ).round(2)
    elif aggregation.upper() == 'AVG':
        pivot_df = df.pivot_table(
            index='row_dim', 
            columns='col_dim', 
            values='value_measure', 
            aggfunc='mean', 
            fill_value=0
        ).round(2)
    elif aggregation.upper() == 'COUNT':
        pivot_df = df.pivot_table(
            index='row_dim', 
            columns='col_dim', 
            values='value_measure', 
            aggfunc='count', 
            fill_value=0
        )
    
    print(pivot_df.to_string())
    return pivot_df

# Execute pivot operations
import sqlite3
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
pivot1 = pivot_operation(conn, 'category', 'quarter', 'total_amount', 'SUM')
pivot2 = pivot_operation(conn, 'segment', 'year', 'quantity', 'AVG')
pivot3 = pivot_operation(conn, 'store_type', 'category', 'total_amount', 'SUM')
