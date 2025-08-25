def drilldown_operation(conn, from_level, to_level, focus_value=None):
    """
    Drill-down operation: Disaggregate data by moving down the hierarchy
    """
    print(f"\n=== DRILL-DOWN OPERATION: {from_level} to {to_level} ===")
    
    if from_level == 'category' and to_level == 'subcategory':
        # Category level view
        query_summary = """
        SELECT 
            p.category,
            SUM(f.total_amount) as category_sales,
            COUNT(DISTINCT p.product_id) as product_count
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category
        ORDER BY category_sales DESC
        """
        
        print("SUMMARY VIEW (Category Level):")
        df_summary = pd.read_sql_query(query_summary, conn)
        print(df_summary.to_string(index=False))
        
        # Drill-down to subcategory
        if focus_value:
            query_drilldown = """
            SELECT 
                p.category,
                p.subcategory,
                p.brand,
                COUNT(*) as transaction_count,
                SUM(f.quantity) as total_quantity,
                SUM(f.total_amount) as subcategory_sales,
                AVG(f.total_amount) as avg_sale
            FROM fact_sales f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE p.category = ?
            GROUP BY p.category, p.subcategory, p.brand
            ORDER BY subcategory_sales DESC
            """
            
            print(f"\nDRILLED-DOWN VIEW (Subcategory Level for {focus_value}):")
            df_drilldown = pd.read_sql_query(query_drilldown, conn, params=[focus_value])
            print(df_drilldown.to_string(index=False))
            
            return df_summary, df_drilldown
        else:
            # Show all subcategories
            query_drilldown = """
            SELECT 
                p.category,
                p.subcategory,
                COUNT(*) as transaction_count,
                SUM(f.total_amount) as subcategory_sales
            FROM fact_sales f
            JOIN dim_products p ON f.product_id = p.product_id
            GROUP BY p.category, p.subcategory
            ORDER BY p.category, subcategory_sales DESC
            """
            
            print("\nDRILLED-DOWN VIEW (All Subcategories):")
            df_drilldown = pd.read_sql_query(query_drilldown, conn)
            print(df_drilldown.to_string(index=False))
            
            return df_summary, df_drilldown
    
    elif from_level == 'region' and to_level == 'state':
        # Region level view
        query_summary = """
        SELECT 
            r.region_name,
            COUNT(DISTINCT s.store_id) as store_count,
            SUM(f.total_amount) as region_sales
        FROM fact_sales f
        JOIN dim_stores s ON f.store_id = s.store_id
        JOIN dim_regions r ON s.region_id = r.region_id
        GROUP BY r.region_id, r.region_name
        ORDER BY region_sales DESC
        """
        
        print("SUMMARY VIEW (Region Level):")
        df_summary = pd.read_sql_query(query_summary, conn)
        print(df_summary.to_string(index=False))
        
        # Drill-down to state level
        query_drilldown = """
        SELECT 
            r.region_name,
            s.state,
            s.city,
            s.store_name,
            s.store_type,
            SUM(f.total_amount) as store_sales,
            COUNT(*) as transaction_count
        FROM fact_sales f
        JOIN dim_stores s ON f.store_id = s.store_id
        JOIN dim_regions r ON s.region_id = r.region_id
        GROUP BY r.region_name, s.state, s.city, s.store_name, s.store_type
        ORDER BY r.region_name, store_sales DESC
        """
        
        print("\nDRILLED-DOWN VIEW (State/City Level):")
        df_drilldown = pd.read_sql_query(query_drilldown, conn)
        print(df_drilldown.to_string(index=False))
        
        return df_summary, df_drilldown

# Execute drill-down operations

import sqlite3 
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
category_to_subcategory = drilldown_operation(conn, 'category', 'subcategory', 'Electronics')
region_to_state = drilldown_operation(conn, 'region', 'state')
