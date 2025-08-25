def rollup_operation(conn, from_level, to_level):
    """
    Roll-up operation: Aggregate data by moving up the hierarchy
    """
    print(f"\n=== ROLL-UP OPERATION: {from_level} to {to_level} ===")
    
    if from_level == 'product' and to_level == 'category':
        # Detailed view (product level)
        query_detail = """
        SELECT 
            p.product_name,
            p.category,
            SUM(f.quantity) as total_quantity,
            SUM(f.total_amount) as total_sales
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY p.category, total_sales DESC
        """
        
        # Rolled-up view (category level)
        query_rollup = """
        SELECT 
            p.category,
            COUNT(DISTINCT p.product_id) as product_count,
            SUM(f.quantity) as total_quantity,
            SUM(f.total_amount) as total_sales,
            AVG(f.total_amount) as avg_sale
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_sales DESC
        """
        
        print("DETAILED VIEW (Product Level):")
        df_detail = pd.read_sql_query(query_detail, conn)
        print(df_detail.to_string(index=False))
        
        print("\nROLLED-UP VIEW (Category Level):")
        df_rollup = pd.read_sql_query(query_rollup, conn)
        print(df_rollup.to_string(index=False))
        
        return df_detail, df_rollup
    
    elif from_level == 'month' and to_level == 'quarter':
        # Monthly view
        query_detail = """
        SELECT 
            d.year,
            d.month,
            d.quarter,
            SUM(f.total_amount) as monthly_sales
        FROM fact_sales f
        JOIN dim_dates d ON f.date_id = d.date_id
        GROUP BY d.year, d.month_num, d.month, d.quarter
        ORDER BY d.year, d.month_num
        """
        
        # Quarterly view
        query_rollup = """
        SELECT 
            d.year,
            d.quarter,
            COUNT(DISTINCT d.month_num) as months_count,
            SUM(f.total_amount) as quarterly_sales,
            AVG(f.total_amount) as avg_monthly_sales
        FROM fact_sales f
        JOIN dim_dates d ON f.date_id = d.date_id
        GROUP BY d.year, d.quarter_num, d.quarter
        ORDER BY d.year, d.quarter_num
        """
        
        print("DETAILED VIEW (Monthly Level):")
        df_detail = pd.read_sql_query(query_detail, conn)
        print(df_detail.to_string(index=False))
        
        print("\nROLLED-UP VIEW (Quarterly Level):")
        df_rollup = pd.read_sql_query(query_rollup, conn)
        print(df_rollup.to_string(index=False))
        
        return df_detail, df_rollup


import sqlite3
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
# Execute roll-up operations
product_to_category = rollup_operation(conn, 'product', 'category')
month_to_quarter = rollup_operation(conn, 'month', 'quarter')
