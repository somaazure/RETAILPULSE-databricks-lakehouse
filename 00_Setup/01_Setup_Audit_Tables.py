# Databricks notebook source
# DBTITLE 1,Framework Title
# MAGIC %md
# MAGIC # Metadata-Driven Framework: Audit Tables Setup
# MAGIC
# MAGIC **RetailPulse Project - Data Engineering Framework**
# MAGIC
# MAGIC This notebook sets up the foundational audit tables for a metadata-driven data engineering framework. These tables enable:
# MAGIC - **Automated tracking** of data quality, ETL jobs, and maintenance operations
# MAGIC - **Operational visibility** into pipeline health and performance
# MAGIC - **Metadata-driven orchestration** using centralized configuration
# MAGIC - **Historical auditing** for compliance and troubleshooting
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Audit Tables Overview
# MAGIC %md
# MAGIC ## 📋 Audit Tables Overview
# MAGIC
# MAGIC ### 1. **metadata_catalog**
# MAGIC Central registry of all tables, columns, validation rules, and maintenance policies. Drives dynamic framework behavior.
# MAGIC
# MAGIC ### 2. **dq_validation_audit**
# MAGIC Logs every data quality check execution with results, violations, and performance metrics.
# MAGIC
# MAGIC ### 3. **etl_job_audit**
# MAGIC Tracks ETL pipeline runs including duration, row counts, and success/failure status.
# MAGIC
# MAGIC ### 4. **maintenance_audit**
# MAGIC Records all table maintenance operations (OPTIMIZE, VACUUM, ANALYZE) with before/after metrics.
# MAGIC
# MAGIC ### 5. **table_change_audit**
# MAGIC Captures data lineage by logging INSERT, UPDATE, DELETE, and MERGE operations on tracked tables.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Create OPS Schema
# MAGIC %sql
# MAGIC -- Create the operations schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS retailpulse.ops
# MAGIC COMMENT 'Operational and audit tables for RetailPulse metadata-driven framework';
# MAGIC
# MAGIC SELECT 'Schema retailpulse.ops is ready!' as status;

# COMMAND ----------

# DBTITLE 1,Create Metadata Catalog Table
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- METADATA CATALOG TABLE
# MAGIC -- Central registry for all tables and their metadata
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retailpulse.ops.metadata_catalog (
# MAGIC     catalog_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Table Identification
# MAGIC     catalog_name STRING NOT NULL COMMENT 'Unity Catalog name (e.g., retailpulse)',
# MAGIC     schema_name STRING NOT NULL COMMENT 'Schema name (e.g., bronze, silver, gold)',
# MAGIC     table_name STRING NOT NULL COMMENT 'Table name without schema prefix',
# MAGIC     full_table_name STRING GENERATED ALWAYS AS (CONCAT(catalog_name, '.', schema_name, '.', table_name)) COMMENT 'Fully qualified table name',
# MAGIC     table_type STRING COMMENT 'MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW',
# MAGIC     layer STRING COMMENT 'Data layer: BRONZE, SILVER, GOLD',
# MAGIC     
# MAGIC     -- Table Metadata
# MAGIC     description STRING COMMENT 'Business description of the table',
# MAGIC     business_domain STRING COMMENT 'Business domain (Sales, Customer, Product, etc.)',
# MAGIC     owner STRING COMMENT 'Table owner or responsible team',
# MAGIC     tags ARRAY<STRING> COMMENT 'Tags for categorization',
# MAGIC     
# MAGIC     -- Column Metadata
# MAGIC     column_name STRING COMMENT 'Column name for column-level metadata',
# MAGIC     data_type STRING COMMENT 'Column data type',
# MAGIC     is_required BOOLEAN COMMENT 'Whether column is required (NOT NULL)',
# MAGIC     is_primary_key BOOLEAN COMMENT 'Whether column is part of primary key',
# MAGIC     column_description STRING COMMENT 'Column business description',
# MAGIC     
# MAGIC     -- Data Quality Configuration
# MAGIC     validation_rule STRING COMMENT 'Validation rule name (NOT_NULL, POSITIVE, DATE_VALID, etc.)',
# MAGIC     rule_description STRING COMMENT 'Detailed description of the validation rule',
# MAGIC     dq_checks_enabled BOOLEAN COMMENT 'Whether DQ checks are active for this table (default: true)',
# MAGIC     
# MAGIC     -- Refresh & SLA Configuration
# MAGIC     refresh_frequency STRING COMMENT 'STREAMING, HOURLY, DAILY, WEEKLY, MONTHLY',
# MAGIC     refresh_schedule STRING COMMENT 'Cron expression for scheduled refreshes',
# MAGIC     sla_hours INT COMMENT 'Data freshness SLA in hours',
# MAGIC     
# MAGIC     -- Maintenance Policy
# MAGIC     optimize_frequency STRING COMMENT 'DAILY, WEEKLY, MONTHLY',
# MAGIC     zorder_columns ARRAY<STRING> COMMENT 'Columns for ZORDER optimization',
# MAGIC     vacuum_retention_hours INT COMMENT 'VACUUM retention period (default: 168 hours = 7 days)',
# MAGIC     auto_optimize_enabled BOOLEAN COMMENT 'Enable AUTO OPTIMIZE (default: false)',
# MAGIC     
# MAGIC     -- Audit Fields
# MAGIC     is_active BOOLEAN COMMENT 'Whether this metadata entry is active (default: true)',
# MAGIC     created_date TIMESTAMP COMMENT 'When entry was created',
# MAGIC     updated_date TIMESTAMP COMMENT 'Last update timestamp',
# MAGIC     updated_by STRING COMMENT 'User who last updated the metadata',
# MAGIC     
# MAGIC     CONSTRAINT pk_metadata PRIMARY KEY (catalog_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Central metadata catalog for all tables in RetailPulse project';
# MAGIC
# MAGIC SELECT 'Metadata Catalog table created successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Create DQ Validation Audit Table
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- DQ VALIDATION AUDIT TABLE
# MAGIC -- Logs all data quality check executions
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retailpulse.ops.dq_validation_audit (
# MAGIC     audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Check Identification
# MAGIC     check_category STRING COMMENT 'Category: Freshness, Referential Integrity, Quality Rules, Completeness, Accuracy',
# MAGIC     check_name STRING NOT NULL COMMENT 'Name of the specific check',
# MAGIC     check_type STRING COMMENT 'Type: SQL, PYTHON, GREAT_EXPECTATIONS',
# MAGIC     table_name STRING NOT NULL COMMENT 'Fully qualified table name being validated',
# MAGIC     column_name STRING COMMENT 'Specific column being validated (if applicable)',
# MAGIC     
# MAGIC     -- Execution Details
# MAGIC     run_timestamp TIMESTAMP NOT NULL COMMENT 'When the check was executed',
# MAGIC     run_date DATE GENERATED ALWAYS AS (CAST(run_timestamp AS DATE)) COMMENT 'Date partition key',
# MAGIC     execution_time_sec DECIMAL(10,2) COMMENT 'Execution duration in seconds',
# MAGIC     
# MAGIC     -- Results
# MAGIC     status STRING NOT NULL COMMENT 'PASS, FAIL, WARNING, ERROR, SKIPPED',
# MAGIC     violation_count BIGINT COMMENT 'Number of violations/failures found',
# MAGIC     total_records BIGINT COMMENT 'Total records evaluated',
# MAGIC     pass_rate DECIMAL(5,2) GENERATED ALWAYS AS (
# MAGIC         CAST(
# MAGIC             CASE 
# MAGIC                 WHEN total_records > 0 THEN ((total_records - violation_count) * 100.0 / total_records)
# MAGIC                 ELSE NULL 
# MAGIC             END AS DECIMAL(5,2)
# MAGIC         )
# MAGIC     ) COMMENT 'Pass rate percentage',
# MAGIC     
# MAGIC     -- Metrics
# MAGIC     metric STRING COMMENT 'Specific metric value (e.g., "freshness: 2.5 hours", "null_rate: 0.05%")',
# MAGIC     threshold STRING COMMENT 'Threshold/expected value for the check',
# MAGIC     message STRING COMMENT 'Detailed message, description, or error details',
# MAGIC     
# MAGIC     -- Traceability
# MAGIC     job_run_id STRING COMMENT 'Job/workflow run ID for traceability',
# MAGIC     job_name STRING COMMENT 'Name of the job that ran the check',
# MAGIC     execution_user STRING COMMENT 'User who executed the check',
# MAGIC     notebook_path STRING COMMENT 'Path to notebook if run from notebook',
# MAGIC     
# MAGIC     CONSTRAINT pk_dq_audit PRIMARY KEY (audit_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Audit log for all data quality validation checks';
# MAGIC
# MAGIC SELECT 'DQ Validation Audit table created successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Create ETL Job Audit Table
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- ETL JOB AUDIT TABLE
# MAGIC -- Tracks all ETL pipeline executions
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retailpulse.ops.etl_job_audit (
# MAGIC     audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Job Identification
# MAGIC     job_name STRING NOT NULL COMMENT 'Name of the ETL job/pipeline',
# MAGIC     job_type STRING COMMENT 'Type: DLT, NOTEBOOK, WORKFLOW, SPARK_SUBMIT, DBT',
# MAGIC     job_layer STRING COMMENT 'Data layer: BRONZE, SILVER, GOLD',
# MAGIC     pipeline_name STRING COMMENT 'Parent pipeline name if part of larger workflow',
# MAGIC     
# MAGIC     -- Source & Target
# MAGIC     source_type STRING COMMENT 'Source type: TABLE, FILE, API, KAFKA, etc.',
# MAGIC     source_tables ARRAY<STRING> COMMENT 'List of source tables',
# MAGIC     source_location STRING COMMENT 'Source file path or connection details',
# MAGIC     target_table STRING NOT NULL COMMENT 'Target table name (fully qualified)',
# MAGIC     
# MAGIC     -- Execution Timeline
# MAGIC     start_time TIMESTAMP NOT NULL COMMENT 'Job start timestamp',
# MAGIC     end_time TIMESTAMP COMMENT 'Job end timestamp',
# MAGIC     run_date DATE GENERATED ALWAYS AS (CAST(start_time AS DATE)) COMMENT 'Date partition key',
# MAGIC     duration_seconds BIGINT GENERATED ALWAYS AS (
# MAGIC         CASE 
# MAGIC             WHEN end_time IS NOT NULL THEN BIGINT(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time))
# MAGIC             ELSE NULL 
# MAGIC         END
# MAGIC     ) COMMENT 'Job duration in seconds',
# MAGIC     
# MAGIC     -- Status & Results
# MAGIC     status STRING NOT NULL COMMENT 'Status: RUNNING, SUCCESS, FAILED, TIMEOUT, CANCELLED',
# MAGIC     rows_read BIGINT COMMENT 'Number of rows read from source',
# MAGIC     rows_written BIGINT COMMENT 'Number of rows written to target',
# MAGIC     rows_updated BIGINT COMMENT 'Number of rows updated (MERGE)',
# MAGIC     rows_deleted BIGINT COMMENT 'Number of rows deleted (MERGE)',
# MAGIC     bytes_processed BIGINT COMMENT 'Total bytes processed',
# MAGIC     
# MAGIC     -- Error Handling
# MAGIC     error_message STRING COMMENT 'Error message if job failed',
# MAGIC     error_type STRING COMMENT 'Error type/category',
# MAGIC     retry_count INT COMMENT 'Number of retries attempted',
# MAGIC     
# MAGIC     -- Traceability
# MAGIC     job_run_id STRING COMMENT 'Unique job run identifier (Databricks run_id)',
# MAGIC     cluster_id STRING COMMENT 'Cluster ID where job ran',
# MAGIC     execution_user STRING COMMENT 'User who executed the job',
# MAGIC     notebook_path STRING COMMENT 'Notebook path if applicable',
# MAGIC     
# MAGIC     -- Additional Metadata
# MAGIC     parameters MAP<STRING, STRING> COMMENT 'Job parameters as key-value pairs',
# MAGIC     tags ARRAY<STRING> COMMENT 'Tags for categorization',
# MAGIC     
# MAGIC     CONSTRAINT pk_etl_audit PRIMARY KEY (audit_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Audit log for ETL pipeline executions';
# MAGIC
# MAGIC SELECT 'ETL Job Audit table created successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Create Maintenance Audit Table
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- MAINTENANCE AUDIT TABLE
# MAGIC -- Records all table maintenance operations
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retailpulse.ops.maintenance_audit (
# MAGIC     audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Table & Operation
# MAGIC     table_name STRING NOT NULL COMMENT 'Fully qualified table name',
# MAGIC     operation STRING NOT NULL COMMENT 'Operation: OPTIMIZE, VACUUM, ANALYZE, ZORDER, REORG',
# MAGIC     operation_mode STRING COMMENT 'Mode: FULL, INCREMENTAL, PARTITION',
# MAGIC     
# MAGIC     -- Timeline
# MAGIC     start_time TIMESTAMP NOT NULL COMMENT 'Operation start time',
# MAGIC     end_time TIMESTAMP COMMENT 'Operation end time',
# MAGIC     run_date DATE GENERATED ALWAYS AS (CAST(start_time AS DATE)) COMMENT 'Date partition key',
# MAGIC     duration_seconds BIGINT GENERATED ALWAYS AS (
# MAGIC         CASE 
# MAGIC             WHEN end_time IS NOT NULL THEN BIGINT(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time))
# MAGIC             ELSE NULL 
# MAGIC         END
# MAGIC     ) COMMENT 'Operation duration in seconds',
# MAGIC     
# MAGIC     -- Status
# MAGIC     status STRING NOT NULL COMMENT 'Status: SUCCESS, FAILED, RUNNING, SKIPPED',
# MAGIC     
# MAGIC     -- File Metrics (OPTIMIZE/VACUUM)
# MAGIC     num_files_before INT COMMENT 'Number of files before operation',
# MAGIC     num_files_after INT COMMENT 'Number of files after operation',
# MAGIC     files_added INT GENERATED ALWAYS AS (num_files_after - num_files_before) COMMENT 'Number of files added (negative if removed)',
# MAGIC     files_removed INT COMMENT 'Number of files removed by VACUUM',
# MAGIC     
# MAGIC     -- Size Metrics
# MAGIC     size_before_bytes BIGINT COMMENT 'Table size before operation in bytes',
# MAGIC     size_after_bytes BIGINT COMMENT 'Table size after operation in bytes',
# MAGIC     size_before_mb DECIMAL(18,2) GENERATED ALWAYS AS (
# MAGIC         CAST(size_before_bytes / 1024.0 / 1024.0 AS DECIMAL(18,2))
# MAGIC     ) COMMENT 'Size before in MB',
# MAGIC     size_after_mb DECIMAL(18,2) GENERATED ALWAYS AS (
# MAGIC         CAST(size_after_bytes / 1024.0 / 1024.0 AS DECIMAL(18,2))
# MAGIC     ) COMMENT 'Size after in MB',
# MAGIC     space_saved_mb DECIMAL(18,2) GENERATED ALWAYS AS (
# MAGIC         CAST((size_before_bytes - size_after_bytes) / 1024.0 / 1024.0 AS DECIMAL(18,2))
# MAGIC     ) COMMENT 'Space saved in MB',
# MAGIC     
# MAGIC     -- Operation-Specific Details
# MAGIC     zorder_columns STRING COMMENT 'Columns used for ZORDER (comma-separated)',
# MAGIC     partition_filter STRING COMMENT 'Partition filter if incremental',
# MAGIC     vacuum_retention_hours INT COMMENT 'Retention hours for VACUUM operation',
# MAGIC     
# MAGIC     -- Statistics (ANALYZE)
# MAGIC     stats_collected ARRAY<STRING> COMMENT 'Statistics collected (for ANALYZE)',
# MAGIC     
# MAGIC     -- Performance Metrics
# MAGIC     rows_processed BIGINT COMMENT 'Number of rows processed',
# MAGIC     partitions_affected INT COMMENT 'Number of partitions affected',
# MAGIC     
# MAGIC     -- Results & Messages
# MAGIC     message STRING COMMENT 'Additional details, warnings, or recommendations',
# MAGIC     metrics MAP<STRING, STRING> COMMENT 'Additional metrics as key-value pairs',
# MAGIC     
# MAGIC     -- Traceability
# MAGIC     job_run_id STRING COMMENT 'Job run ID if part of automated workflow',
# MAGIC     job_name STRING COMMENT 'Name of the maintenance job',
# MAGIC     execution_user STRING COMMENT 'User who executed the operation',
# MAGIC     is_automated BOOLEAN COMMENT 'Whether operation was automated or manual',
# MAGIC     
# MAGIC     CONSTRAINT pk_maintenance_audit PRIMARY KEY (audit_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Audit log for table maintenance operations (OPTIMIZE, VACUUM, ANALYZE)';
# MAGIC
# MAGIC SELECT 'Maintenance Audit table created successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Create Table Change Audit Table
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- TABLE CHANGE AUDIT TABLE
# MAGIC -- Captures data lineage and change tracking
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retailpulse.ops.table_change_audit (
# MAGIC     audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Table & Operation
# MAGIC     table_name STRING NOT NULL COMMENT 'Fully qualified table name',
# MAGIC     operation STRING NOT NULL COMMENT 'Operation: INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, DROP',
# MAGIC     operation_type STRING COMMENT 'Type: DML, DDL, METADATA',
# MAGIC     
# MAGIC     -- Timeline
# MAGIC     change_timestamp TIMESTAMP NOT NULL COMMENT 'When the change occurred',
# MAGIC     change_date DATE GENERATED ALWAYS AS (CAST(change_timestamp AS DATE)) COMMENT 'Date partition key',
# MAGIC     
# MAGIC     -- Delta Version Info
# MAGIC     version BIGINT COMMENT 'Delta table version after this operation',
# MAGIC     version_before BIGINT COMMENT 'Delta table version before this operation',
# MAGIC     
# MAGIC     -- Change Metrics
# MAGIC     rows_affected BIGINT COMMENT 'Number of rows affected by the operation',
# MAGIC     rows_inserted BIGINT COMMENT 'Rows inserted (MERGE)',
# MAGIC     rows_updated BIGINT COMMENT 'Rows updated (MERGE)',
# MAGIC     rows_deleted BIGINT COMMENT 'Rows deleted (MERGE/DELETE)',
# MAGIC     bytes_written BIGINT COMMENT 'Bytes written to storage',
# MAGIC     files_added INT COMMENT 'Number of files added',
# MAGIC     files_removed INT COMMENT 'Number of files removed',
# MAGIC     
# MAGIC     -- Operation Details
# MAGIC     operation_metrics MAP<STRING, STRING> COMMENT 'Detailed operation metrics from Delta history',
# MAGIC     operation_parameters MAP<STRING, STRING> COMMENT 'Operation parameters (e.g., WHERE clause, MERGE condition)',
# MAGIC     
# MAGIC     -- Partition Information
# MAGIC     partition_values MAP<STRING, STRING> COMMENT 'Partition values affected (key-value pairs)',
# MAGIC     partitions_affected INT COMMENT 'Number of partitions affected',
# MAGIC     
# MAGIC     -- User & Job Context
# MAGIC     user_name STRING COMMENT 'User who made the change',
# MAGIC     user_email STRING COMMENT 'User email address',
# MAGIC     job_name STRING COMMENT 'Job or notebook that made the change',
# MAGIC     job_run_id STRING COMMENT 'Job run ID for traceability',
# MAGIC     notebook_path STRING COMMENT 'Notebook path if applicable',
# MAGIC     cluster_id STRING COMMENT 'Cluster ID where change was made',
# MAGIC     
# MAGIC     -- Change Description
# MAGIC     change_details STRING COMMENT 'Additional details about the change',
# MAGIC     change_reason STRING COMMENT 'Business reason for the change',
# MAGIC     is_automated BOOLEAN COMMENT 'Whether change was automated',
# MAGIC     
# MAGIC     -- Lineage
# MAGIC     source_tables ARRAY<STRING> COMMENT 'Source tables that contributed to this change',
# MAGIC     downstream_tables ARRAY<STRING> COMMENT 'Known downstream tables affected',
# MAGIC     
# MAGIC     CONSTRAINT pk_change_audit PRIMARY KEY (audit_id)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (change_date)
# MAGIC COMMENT 'Audit log for data changes and lineage tracking';
# MAGIC
# MAGIC SELECT 'Table Change Audit table created successfully!' as status;

# COMMAND ----------

# DBTITLE 1,Verify All Audit Tables
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- VERIFICATION: List all created audit tables
# MAGIC -- ============================================
# MAGIC
# MAGIC SHOW TABLES IN retailpulse.ops;

# COMMAND ----------

# DBTITLE 1,Get Table Details and Row Counts
# MAGIC %sql
# MAGIC -- Get detailed information about each audit table
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     table_type,
# MAGIC     CASE 
# MAGIC         WHEN table_name = 'metadata_catalog' THEN 'Central registry for tables, columns, validation rules, and maintenance policies'
# MAGIC         WHEN table_name = 'dq_validation_audit' THEN 'Logs all data quality check executions with results and metrics'
# MAGIC         WHEN table_name = 'etl_job_audit' THEN 'Tracks ETL pipeline runs, performance, and success/failure status'
# MAGIC         WHEN table_name = 'maintenance_audit' THEN 'Records OPTIMIZE, VACUUM, and ANALYZE operations with metrics'
# MAGIC         WHEN table_name = 'table_change_audit' THEN 'Captures data lineage and tracks INSERT/UPDATE/DELETE operations'
# MAGIC         ELSE 'Other operational table'
# MAGIC     END as description,
# MAGIC     created_by,
# MAGIC     CAST(created AS STRING) as created_at
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_catalog = 'retailpulse' 
# MAGIC     AND table_schema = 'ops'
# MAGIC     AND table_name IN ('metadata_catalog', 'dq_validation_audit', 'etl_job_audit', 'maintenance_audit', 'table_change_audit')
# MAGIC ORDER BY table_name;

# COMMAND ----------

# DBTITLE 1,Framework Complete
# MAGIC %md
# MAGIC ## ✅ Metadata-Driven Framework Complete!
# MAGIC
# MAGIC All audit tables have been created successfully. The complete framework consists of:
# MAGIC
# MAGIC ### 📁 Framework Notebooks:
# MAGIC 1. **[01_Setup_Audit_Tables](#notebook-452130069342023)** - Foundation audit tables (this notebook) ✅
# MAGIC 2. **[02_Metadata_Configuration](#notebook-452130069342024)** - Populate metadata catalog with table definitions ✅
# MAGIC 3. **[03_DQ_Framework](#notebook-452130069342025)** - Automated data quality validation ✅
# MAGIC 4. **[04_Maintenance_Framework](#notebook-452130069342026)** - OPTIMIZE, VACUUM, ANALYZE operations ✅
# MAGIC 5. **[05_Audit_Reporting](#notebook-452130069342027)** - Monitoring dashboards and reports ✅
# MAGIC
# MAGIC ### 📊 Audit Tables Created:
# MAGIC * `retailpulse.ops.metadata_catalog` - Central metadata registry
# MAGIC * `retailpulse.ops.dq_validation_audit` - DQ check results
# MAGIC * `retailpulse.ops.etl_job_audit` - ETL pipeline tracking
# MAGIC * `retailpulse.ops.maintenance_audit` - Maintenance operations log
# MAGIC * `retailpulse.ops.table_change_audit` - Data change history
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC 1. Run notebook 02 to populate metadata catalog with RetailPulse table definitions
# MAGIC 2. Run notebook 03 to execute DQ validations
# MAGIC 3. Schedule notebook 04 for regular maintenance operations (daily/weekly)
# MAGIC 4. Use notebook 05 queries to build operational dashboards
# MAGIC 5. Schedule automated jobs for continuous monitoring
# MAGIC
# MAGIC ---
# MAGIC **Framework Status**: Fully Implemented & Ready for Use 🎉