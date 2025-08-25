def advanced_olap_analysis(conn):
    """
    Advanced OLAP analysis combining multiple operations
    """
    print("\n=== ADVANCED OLAP ANALYSIS ===")
    
    # 1. Time-based analysis with multiple dimensions
    print("\n1. QUARTERLY SALES TREND BY CATEGORY AND SEGMENT:")
    query1 = """
    SELECT 
        d.year,
        d.quarter,
        p.category,
        c.segment,
        COUNT(*) as transactions,
        SUM(f.quantity) as total_quantity,
        SUM(f.total_amount) as total_sales,
        AVG(f.total_amount) as avg_transaction,
        SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (PARTITION BY d.year, d.quarter) * 100 as sales_percentage
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_dates d ON f.date_id = d.date_id
    GROUP BY d.year, d.quarter_num, d.quarter, p.category, c.segment
    ORDER BY d.year, d.quarter_num, total_sales DESC
    """
    
    df1 = pd.read_sql_query(query1, conn)
    print(df1.to_string(index=False))
    
    # 2. Store performance analysis
    print("\n\n2. STORE PERFORMANCE ANALYSIS:")
    query2 = """
    SELECT 
        r.region_name,
        s.store_name,
        s.store_type,
        s.city,
        s.state,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        COUNT(*) as total_transactions,
        SUM(f.total_amount) as total_sales,
        AVG(f.total_amount) as avg_transaction,
        MAX(f.total_amount) as highest_sale,
        RANK() OVER (ORDER BY SUM(f.total_amount) DESC) as sales_rank
    FROM fact_sales f
    JOIN dim_stores s ON f.store_id = s.store_id
    JOIN dim_regions r ON s.region_id = r.region_id
    GROUP BY r.region_name, s.store_id, s.store_name, s.store_type, s.city, s.state
    ORDER BY total_sales DESC
    """
    
    df2 = pd.read_sql_query(query2, conn)
    print(df2.to_string(index=False))
    
    # 3. Customer segment analysis
    print("\n\n3. CUSTOMER SEGMENT ANALYSIS:")
    query3 = """
    SELECT 
        c.segment,
        c.age_group,
        COUNT(DISTINCT c.customer_id) as customer_count,
        COUNT(*) as total_purchases,
        SUM(f.total_amount) as total_spent,
        AVG(f.total_amount) as avg_purchase,
        SUM(f.quantity) as total_items,
        AVG(f.quantity) as avg_items_per_purchase,
        ROUND(SUM(f.total_amount) / COUNT(DISTINCT c.customer_id), 2) as avg_customer_value
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.segment, c.age_group
    ORDER BY total_spent DESC
    """
    
    df3 = pd.read_sql_query(query3, conn)
    print(df3.to_string(index=False))
    
    return df1, df2, df3

# Execute advanced analysis
import sqlite3
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
advanced_results = advanced_olap_analysis(conn)
