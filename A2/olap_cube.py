def create_olap_cube(conn):
    """
    Create a comprehensive OLAP cube with all dimensions
    """
    print("\n=== OLAP CUBE CREATION ===")
    
    query = """
    SELECT 
        -- Dimension attributes
        p.category,
        p.subcategory,
        p.brand,
        s.store_type,
        r.region_name,
        s.state,
        s.city,
        c.segment,
        c.age_group,
        d.year,
        d.quarter,
        d.month,
        
        -- Measures
        COUNT(*) as transaction_count,
        SUM(f.quantity) as total_quantity,
        SUM(f.total_amount) as total_sales,
        AVG(f.total_amount) as avg_transaction,
        SUM(f.discount * f.quantity * f.unit_price) as total_discount,
        
        -- Calculated measures
        SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0) as revenue_per_item,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.customer_id), 0) as revenue_per_customer
        
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    JOIN dim_stores s ON f.store_id = s.store_id
    JOIN dim_regions r ON s.region_id = r.region_id
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_dates d ON f.date_id = d.date_id
    GROUP BY CUBE(
        p.category, p.subcategory, p.brand,
        s.store_type, r.region_name, s.state, s.city,
        c.segment, c.age_group,
        d.year, d.quarter, d.month
    )
    ORDER BY total_sales DESC
    LIMIT 50
    """
    
    # Note: SQLite doesn't support CUBE, so we'll simulate it
    print("Creating OLAP cube simulation...")
    
    # Alternative approach - create multiple GROUP BY combinations
    cube_queries = [
        # Single dimension rollups
        "SELECT p.category, NULL as subcategory, NULL as region, NULL as year, SUM(f.total_amount) as total_sales FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id GROUP BY p.category",
        "SELECT NULL as category, NULL as subcategory, r.region_name as region, NULL as year, SUM(f.total_amount) as total_sales FROM fact_sales f JOIN dim_stores s ON f.store_id = s.store_id JOIN dim_regions r ON s.region_id = r.region_id GROUP BY r.region_name",
        "SELECT NULL as category, NULL as subcategory, NULL as region, d.year, SUM(f.total_amount) as total_sales FROM fact_sales f JOIN dim_dates d ON f.date_id = d.date_id GROUP BY d.year",
        
        # Two dimension combinations  
        "SELECT p.category, NULL as subcategory, r.region_name as region, NULL as year, SUM(f.total_amount) as total_sales FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id JOIN dim_stores s ON f.store_id = s.store_id JOIN dim_regions r ON s.region_id = r.region_id GROUP BY p.category, r.region_name",
        "SELECT p.category, NULL as subcategory, NULL as region, d.year, SUM(f.total_amount) as total_sales FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id JOIN dim_dates d ON f.date_id = d.date_id GROUP BY p.category, d.year"
    ]
    
    print("OLAP Cube Results (Simulated):")
    for i, query in enumerate(cube_queries):
        print(f"\nCube Level {i+1}:")
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    
    return cube_queries

# Create OLAP cube
import sqlite3  
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()
cube_results = create_olap_cube(conn)
