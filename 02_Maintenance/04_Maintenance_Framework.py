# Databricks notebook source
# DBTITLE 1,Maintenance Framework Title
# MAGIC %md
# MAGIC # Table Maintenance Framework: Automated Operations
# MAGIC
# MAGIC **RetailPulse Project - Metadata-Driven Maintenance**
# MAGIC
# MAGIC This notebook implements automated table maintenance operations:
# MAGIC - **OPTIMIZE**: Compact small files for better query performance
# MAGIC - **VACUUM**: Remove old data files to reclaim storage
# MAGIC - **ANALYZE**: Collect table statistics for query optimization
# MAGIC - **ZORDER**: Optimize data layout for frequently filtered columns
# MAGIC
# MAGIC All operations are driven by policies defined in metadata_catalog.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import time

# Initialize variables
current_timestamp = datetime.now()
current_user = spark.sql("SELECT current_user()").collect()[0][0]

print(f"Maintenance Framework initialized")
print(f"Timestamp: {current_timestamp}")
print(f"User: {current_user}")

# COMMAND ----------

# DBTITLE 1,Helper Function: Log Maintenance Operation
def log_maintenance_operation(table_name, operation, status, 
                               num_files_before=None, num_files_after=None,
                               size_before_bytes=None, size_after_bytes=None,
                               duration_seconds=None, zorder_columns=None,
                               vacuum_retention_hours=None, message=None):
    """
    Log maintenance operation to audit table
    """
    insert_sql = f"""
    INSERT INTO retailpulse.ops.maintenance_audit (
        table_name, operation, operation_mode, start_time, end_time,
        status, num_files_before, num_files_after, files_removed,
        size_before_bytes, size_after_bytes,
        zorder_columns, vacuum_retention_hours, message,
        execution_user, is_automated
    ) VALUES (
        '{table_name}',
        '{operation}',
        'FULL',
        timestamp_add(current_timestamp(), -{duration_seconds if duration_seconds else 0}),
        current_timestamp(),
        '{status}',
        {num_files_before if num_files_before else 'NULL'},
        {num_files_after if num_files_after else 'NULL'},
        NULL,
        {size_before_bytes if size_before_bytes else 'NULL'},
        {size_after_bytes if size_after_bytes else 'NULL'},
        {f"'{zorder_columns}'" if zorder_columns else 'NULL'},
        {vacuum_retention_hours if vacuum_retention_hours else 'NULL'},
        {f"'{message}'" if message else 'NULL'},
        '{current_user}',
        TRUE
    )
    """
    spark.sql(insert_sql)
    print(f"✓ Logged: {operation} on {table_name} [{status}]")

# COMMAND ----------

# DBTITLE 1,Helper Function: Get Table Metrics
def get_table_metrics(table_name):
    """
    Get current file count and size for a table
    """
    try:
        detail_query = f"DESCRIBE DETAIL {table_name}"
        detail = spark.sql(detail_query).collect()[0]
        
        num_files = detail['numFiles'] if 'numFiles' in detail.asDict() else None
        size_bytes = detail['sizeInBytes'] if 'sizeInBytes' in detail.asDict() else None
        
        return num_files, size_bytes
    except Exception as e:
        print(f"  ⚠ Could not get metrics for {table_name}: {str(e)}")
        return None, None

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Operations Section
# MAGIC %md
# MAGIC ## 1️⃣ OPTIMIZE Operations
# MAGIC
# MAGIC Compact small files to improve query performance.

# COMMAND ----------

# DBTITLE 1,Run OPTIMIZE Operations
# Get tables that need OPTIMIZE from metadata catalog
tables_to_optimize = spark.sql("""
    SELECT DISTINCT 
        full_table_name,
        zorder_columns,
        optimize_frequency
    FROM retailpulse.ops.metadata_catalog
    WHERE optimize_frequency IS NOT NULL
        AND is_active = TRUE
        AND column_name IS NULL
        AND layer IN ('SILVER', 'GOLD')
""").collect()

print(f"Found {len(tables_to_optimize)} tables to optimize")
print("\nRunning OPTIMIZE operations...\n")

for row in tables_to_optimize:
    table_name = row['full_table_name']
    zorder_cols = row['zorder_columns']
    frequency = row['optimize_frequency']
    
    print(f"Optimizing {table_name}...")
    start_time = time.time()
    
    try:
        # Get metrics before
        num_files_before, size_before = get_table_metrics(table_name)
        
        # Run OPTIMIZE with ZORDER if specified
        if zorder_cols and len(zorder_cols) > 0:
            zorder_str = ', '.join(zorder_cols)
            optimize_sql = f"OPTIMIZE {table_name} ZORDER BY ({zorder_str})"
            print(f"  Using ZORDER on: {zorder_str}")
        else:
            optimize_sql = f"OPTIMIZE {table_name}"
            zorder_str = None
        
        # Execute OPTIMIZE
        spark.sql(optimize_sql)
        
        # Get metrics after
        num_files_after, size_after = get_table_metrics(table_name)
        
        duration = int(time.time() - start_time)
        
        # Calculate improvements
        files_saved = (num_files_before - num_files_after) if num_files_before and num_files_after else 0
        size_saved_mb = ((size_before - size_after) / 1024 / 1024) if size_before and size_after else 0
        
        message = f'Compacted {files_saved} files. Saved {size_saved_mb:.2f} MB'
        
        # Log success
        log_maintenance_operation(
            table_name=table_name,
            operation='OPTIMIZE',
            status='SUCCESS',
            num_files_before=num_files_before,
            num_files_after=num_files_after,
            size_before_bytes=size_before,
            size_after_bytes=size_after,
            duration_seconds=duration,
            zorder_columns=zorder_str,
            message=message
        )
        
        print(f"  ✅ Completed in {duration}s - {message}\n")
        
    except Exception as e:
        duration = int(time.time() - start_time)
        error_msg = str(e)[:500]  # Truncate long errors
        
        log_maintenance_operation(
            table_name=table_name,
            operation='OPTIMIZE',
            status='FAILED',
            duration_seconds=duration,
            message=f'Error: {error_msg}'
        )
        
        print(f"  ❌ Failed: {error_msg}\n")

print("✅ OPTIMIZE operations completed")

# COMMAND ----------

# DBTITLE 1,VACUUM Operations Section
# MAGIC %md
# MAGIC ## 2️⃣ VACUUM Operations
# MAGIC
# MAGIC Remove old data files to reclaim storage space.

# COMMAND ----------

# DBTITLE 1,Run VACUUM Operations
# Get tables with vacuum policies from metadata catalog
tables_to_vacuum = spark.sql("""
    SELECT DISTINCT 
        full_table_name,
        vacuum_retention_hours
    FROM retailpulse.ops.metadata_catalog
    WHERE vacuum_retention_hours IS NOT NULL
        AND is_active = TRUE
        AND column_name IS NULL
        AND layer IN ('SILVER', 'GOLD')
""").collect()

print(f"Found {len(tables_to_vacuum)} tables to vacuum")
print("\nRunning VACUUM operations...\n")

for row in tables_to_vacuum:
    table_name = row['full_table_name']
    retention_hours = row['vacuum_retention_hours']
    
    print(f"Vacuuming {table_name} (retention: {retention_hours} hours)...")
    start_time = time.time()
    
    try:
        # Get size before
        _, size_before = get_table_metrics(table_name)
        
        # Run VACUUM
        vacuum_sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        spark.sql(vacuum_sql)
        
        # Get size after
        _, size_after = get_table_metrics(table_name)
        
        duration = int(time.time() - start_time)
        
        # Calculate space saved
        space_saved_mb = ((size_before - size_after) / 1024 / 1024) if size_before and size_after else 0
        
        message = f'Reclaimed {space_saved_mb:.2f} MB of storage'
        
        # Log success
        log_maintenance_operation(
            table_name=table_name,
            operation='VACUUM',
            status='SUCCESS',
            size_before_bytes=size_before,
            size_after_bytes=size_after,
            duration_seconds=duration,
            vacuum_retention_hours=retention_hours,
            message=message
        )
        
        print(f"  ✅ Completed in {duration}s - {message}\n")
        
    except Exception as e:
        duration = int(time.time() - start_time)
        error_msg = str(e)[:500]
        
        log_maintenance_operation(
            table_name=table_name,
            operation='VACUUM',
            status='FAILED',
            duration_seconds=duration,
            vacuum_retention_hours=retention_hours,
            message=f'Error: {error_msg}'
        )
        
        print(f"  ❌ Failed: {error_msg}\n")

print("✅ VACUUM operations completed")

# COMMAND ----------

# DBTITLE 1,ANALYZE Operations Section
# MAGIC %md
# MAGIC ## 3️⃣ ANALYZE TABLE Operations
# MAGIC
# MAGIC Collect table statistics for query optimization.

# COMMAND ----------

# DBTITLE 1,Run ANALYZE Operations
# Get all active tables for statistics collection
tables_to_analyze = spark.sql("""
    SELECT DISTINCT full_table_name
    FROM retailpulse.ops.metadata_catalog
    WHERE is_active = TRUE
        AND column_name IS NULL
        AND layer IN ('SILVER', 'GOLD')
""").collect()

print(f"Found {len(tables_to_analyze)} tables to analyze")
print("\nRunning ANALYZE operations...\n")

for row in tables_to_analyze:
    table_name = row['full_table_name']
    
    print(f"Analyzing {table_name}...")
    start_time = time.time()
    
    try:
        # Run ANALYZE TABLE to collect statistics
        analyze_sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"
        spark.sql(analyze_sql)
        
        duration = int(time.time() - start_time)
        message = 'Table statistics collected successfully'
        
        # Log success
        log_maintenance_operation(
            table_name=table_name,
            operation='ANALYZE',
            status='SUCCESS',
            duration_seconds=duration,
            message=message
        )
        
        print(f"  ✅ Completed in {duration}s\n")
        
    except Exception as e:
        duration = int(time.time() - start_time)
        error_msg = str(e)[:500]
        
        log_maintenance_operation(
            table_name=table_name,
            operation='ANALYZE',
            status='FAILED',
            duration_seconds=duration,
            message=f'Error: {error_msg}'
        )
        
        print(f"  ❌ Failed: {error_msg}\n")

print("✅ ANALYZE operations completed")

# COMMAND ----------

# DBTITLE 1,Results Summary Section
# MAGIC %md
# MAGIC ## 📊 Maintenance Operations Summary
# MAGIC
# MAGIC View the results of all maintenance operations run in this session.

# COMMAND ----------

# DBTITLE 1,View Recent Maintenance Results
# MAGIC %sql
# MAGIC -- View all maintenance operations from the last 24 hours
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     operation,
# MAGIC     status,
# MAGIC     duration_seconds,
# MAGIC     num_files_before,
# MAGIC     num_files_after,
# MAGIC     files_added,
# MAGIC     size_before_mb,
# MAGIC     size_after_mb,
# MAGIC     space_saved_mb,
# MAGIC     message,
# MAGIC     start_time
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ORDER BY start_time DESC, operation;