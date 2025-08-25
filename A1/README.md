# Experiment Report: Multidimensional Data Modeling Using SQL (Star Schema, Snowflake, and Fact Constellation)

---

## Aim

To design and implement multidimensional data models using SQL, specifically the star schema, snowflake schema, and fact constellation schema, and to import data from CSV files into these schemas for analytical processing.

---

## Introduction

Data warehousing is a critical component of modern business intelligence systems, enabling organizations to store, manage, and analyze large volumes of historical data. Multidimensional data modeling is the foundation of data warehousing, providing structures that support complex analytical queries and reporting. This experiment explores three major schema designs—star, snowflake, and fact constellation—using Python and SQLite, and demonstrates the ETL (Extract, Transform, Load) process for populating these schemas from raw CSV datasets.

---

## Theory

### Multidimensional Data Modeling

Multidimensional data modeling organizes data into facts and dimensions, facilitating efficient querying and analysis. The main schemas used are:

#### 1. **Star Schema**
- **Structure:** Central fact table surrounded by denormalized dimension tables.
- **Dimension Tables:** Contain descriptive attributes (e.g., product details, customer info).
- **Fact Table:** Stores measurable, quantitative data (e.g., sales amount, quantity).
- **Advantages:** Simple structure, fast query performance, easy to understand and maintain.
- **Disadvantages:** Data redundancy in dimension tables, potential for update anomalies.

#### 2. **Snowflake Schema**
- **Structure:** Extension of star schema with normalized dimension tables (dimensions split into sub-dimensions).
- **Advantages:** Reduces data redundancy, improves data integrity, supports more granular analysis.
- **Disadvantages:** More complex queries due to additional joins, slightly slower query performance.

#### 3. **Fact Constellation Schema (Galaxy Schema)**
- **Structure:** Multiple fact tables share dimension tables, supporting multiple business processes.
- **Advantages:** Supports complex business scenarios and multiple subject areas, flexible for enterprise data warehousing.
- **Disadvantages:** Increased schema complexity, more challenging to maintain and query.

### Key Concepts

- **Fact Table:** Stores transactional or event data, referencing dimension tables via foreign keys.
- **Dimension Table:** Stores descriptive attributes, often denormalized in star schema and normalized in snowflake schema.
- **Surrogate Keys:** Unique identifiers used in dimension tables for efficient joins and referential integrity.
- **ETL Process:** Extract, Transform, Load operations used to populate schemas from raw data sources, ensuring data quality and consistency.
- **OLAP Operations:** Slicing, dicing, drilling down, and rolling up for multidimensional analytical queries.

---

## Experimentation

### Overview

This experiment involves:
- Creating star, snowflake, and fact constellation schemas in SQLite using Python scripts.
- Defining dimension and fact tables for each schema.
- Importing data from CSV files into the respective tables.
- Handling data type conversions, referential integrity, and error management during import.

### Steps

#### 1. **Database Initialization**
   - Remove existing database files if present to ensure a clean setup.
   - Create new SQLite databases for each schema type.

#### 2. **Table Creation**
   - **Star Schema:** Define dimension tables (`dim_products`, `dim_stores`, `dim_customers`, `dim_salespersons`, `dim_dates`, `dim_campaigns`) and a central fact table (`fact_sales`).
   - **Snowflake Schema:** Normalize dimension tables into sub-dimensions (e.g., product categories, brands, locations) and create foreign key relationships.
   - **Fact Constellation Schema:** Create multiple fact tables (`fact_sales`, `fact_campaign_performance`, `fact_inventory`, `fact_customer_activity`) sharing dimension tables.

#### 3. **Data Import**
   - Read CSV files for each dimension and fact table.
   - Map CSV columns to database columns using predefined mappings and handle missing or mismatched columns.
   - Convert data types as needed (e.g., integers, floats, dates).
   - Insert data into tables, handling errors and reporting issues.
   - Ensure referential integrity by checking foreign key relationships.

#### 4. **Fact Table Population**
   - For the fact constellation schema, generate additional facts such as campaign performance, inventory, and customer activity using synthetic data and aggregations.

#### 5. **Finalization**
   - Commit changes and close the database connection.
   - Print summary statistics and error logs for verification.

---

## Diagrams

### Star Schema Diagram

```
         +------------------+
         |   dim_products   |
         +------------------+
                 |
+----------------+----------------+----------------+----------------+----------------+
|                |                |                |                |                |
|         +------------------+    |         +------------------+    |         +------------------+
|         |   dim_stores     |    |         |  dim_customers   |    |         | dim_salespersons |
|         +------------------+    |         +------------------+    |         +------------------+
|                |                |                |                |                |
+----------------+----------------+----------------+----------------+----------------+
                 |
         +------------------+
         |    fact_sales    |
         +------------------+
                 |
         +------------------+
         |   dim_dates      |
         +------------------+
                 |
         +------------------+
         |  dim_campaigns   |
         +------------------+
```

### Snowflake Schema Diagram

```
         +------------------+
         |   dim_products   |
         +------------------+
           /     |      \
+---------+   +-----+   +------+
| dim_product_categories | dim_product_brands | dim_product_locations |
+-----------------------+--------------------+----------------------+
```
*(Other dimensions similarly normalized)*

### Fact Constellation Schema Diagram

```
         +------------------+      +------------------+      +------------------+
         |   dim_products   |      |   dim_stores     |      |  dim_customers   |
         +------------------+      +------------------+      +------------------+
                 |                        |                        |
         +------------------+      +------------------+      +------------------+
         |   fact_sales     |      | fact_inventory   |      | fact_customer_activity |
         +------------------+      +------------------+      +------------------+
                 |                        |                        |
         +------------------+      +------------------+      +------------------+
         |   dim_dates      |      | dim_campaigns    |      | fact_campaign_performance |
         +------------------+      +------------------+      +------------------+
```

---

## Problems Faced

### 1. **Data Mapping Issues**
- Ensuring correct mapping between CSV columns and database schema columns.
- Handling mismatches in column names and missing columns, especially when normalizing dimensions.

### 2. **Referential Integrity**
- Maintaining foreign key relationships during data import.
- Ensuring that referenced dimension records exist before inserting fact records.
- Handling missing foreign keys by providing default values or skipping problematic rows.

### 3. **Data Type Conversion**
- Handling conversion errors between string, integer, and float types.
- Managing missing or malformed data in source files.
- Logging warnings for conversion issues and providing fallback values.

### 4. **Missing or Inconsistent Data**
- Dealing with incomplete or mismatched data in CSV files.
- Providing default values or skipping problematic rows.
- Generating synthetic data for missing facts in the constellation schema.

### 5. **Performance**
- Importing large datasets efficiently using batch inserts.
- Optimizing query performance for analytical operations by indexing foreign keys.

### 6. **Error Handling**
- Reporting errors during data import.
- Logging warnings for data conversion issues.
- Printing sample problematic rows for debugging.

### 7. **Schema Complexity**
- Managing increased complexity in snowflake and constellation schemas.
- Ensuring consistency and integrity across multiple fact tables and shared dimensions.

---

## Results and Analysis

- Successfully created and populated star, snowflake, and fact constellation schemas in SQLite.
- Demonstrated the ETL process for importing and transforming raw CSV data into analytical schemas.
- Verified referential integrity and data consistency through foreign key constraints.
- Generated additional analytical facts (campaign performance, inventory, customer activity) for richer analysis.
- Identified and addressed common data warehousing challenges such as mapping, conversion, and error handling.

---

## Conclusion

Multidimensional data modeling using SQL is essential for building scalable and efficient data warehouses. The star schema offers simplicity and speed for analytical queries, while the snowflake schema improves data integrity through normalization. The fact constellation schema supports complex business scenarios by allowing multiple fact tables to share dimensions. Proper schema design, data mapping, and error handling are crucial for successful implementation. This experiment demonstrates the practical steps involved in creating these schemas, importing data, and addressing common challenges in real-world scenarios. The resulting database structures enable fast, reliable analytics and reporting, supporting informed decision-making in organizations.

---

## References

- Kimball, R., & Ross, M. (2013). The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling.
- Inmon, W. H. (2005). Building the Data Warehouse.
- SQLite Documentation: https://sqlite.org/docs.html
- Python CSV Module: https://docs.python.org/3/library/csv.html
- Kaggle Retail Store Star Schema Dataset: https://www.kaggle.com/datasets/shrinivasv/retail-store-star-schema-dataset