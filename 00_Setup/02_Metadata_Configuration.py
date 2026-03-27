# Databricks notebook source
# DBTITLE 1,Framework Title
# MAGIC %md
# MAGIC # Metadata Configuration: RetailPulse Tables
# MAGIC
# MAGIC **RetailPulse Project - Metadata Catalog Setup**
# MAGIC
# MAGIC This notebook populates the `metadata_catalog` table with comprehensive metadata for all RetailPulse tables including:
# MAGIC - **Table definitions** across Bronze, Silver, and Gold layers
# MAGIC - **Validation rules** for data quality checks
# MAGIC - **Maintenance policies** for OPTIMIZE and VACUUM operations
# MAGIC - **Column-level metadata** with data types and constraints
# MAGIC - **SLA and refresh configurations**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration Overview
# MAGIC %md
# MAGIC ## 📋 Configuration Strategy
# MAGIC
# MAGIC ### **Gold Layer Tables**
# MAGIC Production-ready, business-facing tables with strict SLAs and optimization:
# MAGIC - `fact_sales` - Sales transactions (optimized daily with ZORDER)
# MAGIC - `dim_customer` - Customer dimension
# MAGIC - `dim_product` - Product dimension
# MAGIC - `dim_date` - Date dimension
# MAGIC
# MAGIC ### **Silver Layer Tables**
# MAGIC Cleaned and validated data:
# MAGIC - `orders` - Validated orders with DQ checks enabled
# MAGIC
# MAGIC ### **Bronze Layer Tables**
# MAGIC Raw ingested data:
# MAGIC - `orders_raw` - Raw order data from source systems
# MAGIC
# MAGIC ### **Maintenance Policies**
# MAGIC - **Daily OPTIMIZE**: High-volume transaction tables (fact_sales, orders)
# MAGIC - **Weekly OPTIMIZE**: Dimension tables (dim_customer, dim_product)
# MAGIC - **Monthly OPTIMIZE**: Static tables (dim_date)
# MAGIC - **Default VACUUM**: 168 hours (7 days) retention
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Clear Existing Configuration
# MAGIC %sql
# MAGIC -- Clear existing metadata for RetailPulse catalog (safe for re-runs)
# MAGIC DELETE FROM retailpulse.ops.metadata_catalog 
# MAGIC WHERE catalog_name = 'retailpulse';
# MAGIC
# MAGIC SELECT 'Existing metadata cleared - ready for fresh configuration!' as status;

# COMMAND ----------

# DBTITLE 1,Insert Gold Layer Tables
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- GOLD LAYER: Business-Ready Tables
# MAGIC -- ============================================
# MAGIC
# MAGIC -- 1. FACT_SALES - Sales Transaction Fact Table
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'gold', 'fact_sales', 'MANAGED', 'GOLD',
# MAGIC     'Sales transaction fact table containing all retail sales with customer, product, and date foreign keys',
# MAGIC     'Sales', 'Data Engineering Team', ARRAY('sales', 'transactions', 'fact'),
# MAGIC     TRUE, 'DAILY', 2, 
# MAGIC     'DAILY', ARRAY('sale_date', 'customer_id'), 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC -- 2. DIM_CUSTOMER - Customer Dimension
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'gold', 'dim_customer', 'MANAGED', 'GOLD',
# MAGIC     'Customer dimension table with customer demographics and attributes',
# MAGIC     'Customer', 'Data Engineering Team', ARRAY('customer', 'dimension'),
# MAGIC     TRUE, 'DAILY', 24,
# MAGIC     'WEEKLY', ARRAY('customer_id'), 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC -- 3. DIM_PRODUCT - Product Dimension
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'gold', 'dim_product', 'MANAGED', 'GOLD',
# MAGIC     'Product dimension table with product details, categories, and pricing',
# MAGIC     'Product', 'Data Engineering Team', ARRAY('product', 'dimension'),
# MAGIC     TRUE, 'DAILY', 24,
# MAGIC     'WEEKLY', ARRAY('product_id'), 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC -- 4. DIM_DATE - Date Dimension
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'gold', 'dim_date', 'MANAGED', 'GOLD',
# MAGIC     'Date dimension table with calendar attributes, fiscal periods, and holidays',
# MAGIC     'Date', 'Data Engineering Team', ARRAY('date', 'dimension', 'calendar'),
# MAGIC     FALSE, 'MONTHLY', NULL,
# MAGIC     'MONTHLY', ARRAY('date_key'), 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC SELECT 'Gold layer tables configured successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Insert Silver Layer Tables
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- SILVER LAYER: Cleaned and Validated Tables
# MAGIC -- ============================================
# MAGIC
# MAGIC -- ORDERS - Validated Orders Table
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'silver', 'orders', 'MANAGED', 'SILVER',
# MAGIC     'Validated and cleansed orders from bronze layer with DQ checks applied',
# MAGIC     'Sales', 'Data Engineering Team', ARRAY('orders', 'silver', 'validated'),
# MAGIC     TRUE, 'DAILY', 4,
# MAGIC     'DAILY', ARRAY('order_date', 'customer_id'), 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC SELECT 'Silver layer tables configured successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Insert Bronze Layer Tables
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- BRONZE LAYER: Raw Ingested Data
# MAGIC -- ============================================
# MAGIC
# MAGIC -- ORDERS_RAW - Raw Orders from Source
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, table_type, layer,
# MAGIC     description, business_domain, owner, tags,
# MAGIC     dq_checks_enabled, refresh_frequency, sla_hours,
# MAGIC     optimize_frequency, zorder_columns, vacuum_retention_hours, auto_optimize_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES (
# MAGIC     'retailpulse', 'bronze', 'orders_raw', 'MANAGED', 'BRONZE',
# MAGIC     'Raw orders ingested from source systems without transformation',
# MAGIC     'Sales', 'Data Engineering Team', ARRAY('orders', 'bronze', 'raw'),
# MAGIC     FALSE, 'STREAMING', 1,
# MAGIC     'DAILY', NULL, 168, FALSE,
# MAGIC     TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()
# MAGIC );
# MAGIC
# MAGIC SELECT 'Bronze layer tables configured successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Insert Column-Level Metadata with Validation Rules
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- COLUMN-LEVEL METADATA & VALIDATION RULES
# MAGIC -- ============================================
# MAGIC
# MAGIC -- FACT_SALES Columns
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, column_name, data_type,
# MAGIC     is_required, is_primary_key, column_description,
# MAGIC     validation_rule, rule_description, dq_checks_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES 
# MAGIC -- Sale ID
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'sale_id', 'BIGINT', 
# MAGIC  TRUE, TRUE, 'Primary key for sales transactions',
# MAGIC  'NOT_NULL', 'Sale ID must not be null', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Customer ID (Foreign Key)
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'customer_id', 'BIGINT',
# MAGIC  TRUE, FALSE, 'Foreign key to dim_customer',
# MAGIC  'NOT_NULL', 'Customer ID must not be null', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Product ID (Foreign Key)
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'product_id', 'BIGINT',
# MAGIC  TRUE, FALSE, 'Foreign key to dim_product',
# MAGIC  'NOT_NULL', 'Product ID must not be null', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Sale Date
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'sale_date', 'DATE',
# MAGIC  TRUE, FALSE, 'Date of the sale transaction',
# MAGIC  'DATE_VALID', 'Sale date must be a valid date and not in the future', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Quantity
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'quantity', 'INT',
# MAGIC  TRUE, FALSE, 'Quantity of items sold',
# MAGIC  'POSITIVE', 'Quantity must be greater than 0', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Unit Price
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'unit_price', 'DECIMAL(10,2)',
# MAGIC  TRUE, FALSE, 'Unit price of the product',
# MAGIC  'POSITIVE', 'Unit price must be greater than 0', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Sale Amount
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'sale_amount', 'DECIMAL(10,2)',
# MAGIC  TRUE, FALSE, 'Total sale amount (quantity * unit_price)',
# MAGIC  'POSITIVE', 'Sale amount must be greater than 0', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC -- Discount Amount
# MAGIC ('retailpulse', 'gold', 'fact_sales', 'discount_amount', 'DECIMAL(10,2)',
# MAGIC  FALSE, FALSE, 'Discount applied to the sale',
# MAGIC  'NON_NEGATIVE', 'Discount amount must be >= 0', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER());
# MAGIC
# MAGIC -- DIM_CUSTOMER Columns
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, column_name, data_type,
# MAGIC     is_required, is_primary_key, column_description,
# MAGIC     validation_rule, rule_description, dq_checks_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES 
# MAGIC ('retailpulse', 'gold', 'dim_customer', 'customer_id', 'BIGINT',
# MAGIC  TRUE, TRUE, 'Primary key for customer dimension',
# MAGIC  'NOT_NULL', 'Customer ID must not be null', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC ('retailpulse', 'gold', 'dim_customer', 'customer_name', 'STRING',
# MAGIC  TRUE, FALSE, 'Full name of the customer',
# MAGIC  'NOT_NULL', 'Customer name must not be null or empty', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC ('retailpulse', 'gold', 'dim_customer', 'email', 'STRING',
# MAGIC  FALSE, FALSE, 'Customer email address',
# MAGIC  'EMAIL_FORMAT', 'Email must be in valid format', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER());
# MAGIC
# MAGIC -- DIM_PRODUCT Columns
# MAGIC INSERT INTO retailpulse.ops.metadata_catalog 
# MAGIC (
# MAGIC     catalog_name, schema_name, table_name, column_name, data_type,
# MAGIC     is_required, is_primary_key, column_description,
# MAGIC     validation_rule, rule_description, dq_checks_enabled,
# MAGIC     is_active, created_date, updated_date, updated_by
# MAGIC )
# MAGIC VALUES 
# MAGIC ('retailpulse', 'gold', 'dim_product', 'product_id', 'BIGINT',
# MAGIC  TRUE, TRUE, 'Primary key for product dimension',
# MAGIC  'NOT_NULL', 'Product ID must not be null', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC ('retailpulse', 'gold', 'dim_product', 'product_name', 'STRING',
# MAGIC  TRUE, FALSE, 'Name of the product',
# MAGIC  'NOT_NULL', 'Product name must not be null or empty', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER()),
# MAGIC
# MAGIC ('retailpulse', 'gold', 'dim_product', 'price', 'DECIMAL(10,2)',
# MAGIC  TRUE, FALSE, 'Current price of the product',
# MAGIC  'POSITIVE', 'Price must be greater than 0', TRUE,
# MAGIC  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER());
# MAGIC
# MAGIC SELECT 'Column-level metadata and validation rules configured successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Verify Metadata Configuration - Summary View
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- VERIFICATION: Summary of Configured Tables
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT 
# MAGIC     full_table_name,
# MAGIC     layer,
# MAGIC     business_domain,
# MAGIC     optimize_frequency,
# MAGIC     dq_checks_enabled,
# MAGIC     refresh_frequency,
# MAGIC     sla_hours,
# MAGIC     CASE 
# MAGIC         WHEN zorder_columns IS NOT NULL THEN CONCAT('ZORDER: ', ARRAY_JOIN(zorder_columns, ', '))
# MAGIC         ELSE 'No ZORDER'
# MAGIC     END as optimization_strategy
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse'
# MAGIC     AND column_name IS NULL  -- Table-level metadata only
# MAGIC ORDER BY 
# MAGIC     CASE layer 
# MAGIC         WHEN 'BRONZE' THEN 1 
# MAGIC         WHEN 'SILVER' THEN 2 
# MAGIC         WHEN 'GOLD' THEN 3 
# MAGIC     END,
# MAGIC     table_name;

# COMMAND ----------

# DBTITLE 1,Verify Column-Level Metadata
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- VERIFICATION: Column-Level Metadata with Validation Rules
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT 
# MAGIC     full_table_name,
# MAGIC     column_name,
# MAGIC     data_type,
# MAGIC     is_required,
# MAGIC     is_primary_key,
# MAGIC     validation_rule,
# MAGIC     rule_description
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse'
# MAGIC     AND column_name IS NOT NULL  -- Column-level metadata only
# MAGIC ORDER BY full_table_name, 
# MAGIC     CASE WHEN is_primary_key THEN 0 ELSE 1 END,
# MAGIC     column_name;

# COMMAND ----------

# DBTITLE 1,Configuration Statistics
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- STATISTICS: Metadata Configuration Summary
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT 
# MAGIC     'Table Count by Layer' as metric_category,
# MAGIC     layer as metric_name,
# MAGIC     CAST(COUNT(DISTINCT table_name) AS STRING) as metric_value
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse' AND column_name IS NULL
# MAGIC GROUP BY layer
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Tables with DQ Checks Enabled' as metric_category,
# MAGIC     CAST(dq_checks_enabled AS STRING) as metric_name,
# MAGIC     CAST(COUNT(DISTINCT table_name) AS STRING) as metric_value
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse' AND column_name IS NULL
# MAGIC GROUP BY dq_checks_enabled
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Column-Level Validation Rules' as metric_category,
# MAGIC     'Total Columns Configured' as metric_name,
# MAGIC     CAST(COUNT(*) AS STRING) as metric_value
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse' AND column_name IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Optimization Frequency' as metric_category,
# MAGIC     optimize_frequency as metric_name,
# MAGIC     CAST(COUNT(DISTINCT table_name) AS STRING) as metric_value
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE catalog_name = 'retailpulse' AND column_name IS NULL
# MAGIC GROUP BY optimize_frequency
# MAGIC
# MAGIC ORDER BY metric_category, metric_name;