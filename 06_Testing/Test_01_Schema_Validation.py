# Databricks notebook source
# DBTITLE 1,Test Suite 1: Schema Validation
# MAGIC %md
# MAGIC # Test Suite 1: Audit Tables Schema Validation
# MAGIC
# MAGIC ## Overview
# MAGIC This test suite validates that all 5 audit tables exist with correct schema structure in the retailpulse.ops schema.
# MAGIC
# MAGIC **Test Cases:** UTC-001 to UTC-005
# MAGIC
# MAGIC **Tables Validated:**
# MAGIC * metadata_catalog - Central configuration registry
# MAGIC * dq_validation_audit - Data quality check results
# MAGIC * etl_job_audit - ETL job execution logs
# MAGIC * maintenance_audit - Table maintenance operations log
# MAGIC * table_change_audit - Schema and data change tracking
# MAGIC
# MAGIC **Purpose:**
# MAGIC Ensure the foundational audit infrastructure is properly configured before running any data quality checks.

# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries and Configure Test Environment
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import traceback

# Initialize test tracking
test_results = []

def log_test_result(test_id, test_name, status, message=""):
    """Log test results for summary reporting"""
    result = {
        "test_id": test_id,
        "test_name": test_name,
        "status": status,
        "message": message,
        "timestamp": datetime.now()
    }
    test_results.append(result)
    
    status_symbol = "✓" if status == "PASS" else "✗"
    print(f"{status_symbol} {test_id}: {test_name} - {status}")
    if message:
        print(f"  Details: {message}")
    print("-" * 80)

# Define test configuration
TEST_CATALOG = "retailpulse"
TEST_SCHEMA = "ops"

print("=" * 80)
print("Test Suite 1: Audit Tables Schema Validation")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Test Cases: UTC-001 to UTC-005
# MAGIC %md
# MAGIC ## Test Cases: UTC-001 to UTC-005
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - All 5 tables exist in retailpulse.ops
# MAGIC - Each table has the required columns with correct data types
# MAGIC - Tables are accessible and queryable

# COMMAND ----------

# DBTITLE 1,UTC-001: Validate metadata_catalog Table Schema
# UTC-001: Metadata Catalog Schema Validation
test_id = "UTC-001"
test_name = "Validate metadata_catalog table exists with correct schema"

try:
    # Check if table exists and get schema
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.metadata_catalog"
    
    # Query table to verify existence and schema
    df = spark.sql(f"DESCRIBE TABLE {table_name}")
    columns = [row.col_name for row in df.collect()]
    
    # Required columns for metadata_catalog (core columns from actual schema)
    required_columns = [
        "catalog_id",        # Primary key (identity column)
        "catalog_name",      # Unity Catalog name
        "schema_name",       # Schema name
        "table_name",        # Table name
        "table_type",        # MANAGED, EXTERNAL, VIEW, etc.
        "is_active",         # Active status flag
        "sla_hours",         # Data freshness SLA
        "validation_rule",   # Validation rule name (singular)
        "created_date",      # Creation timestamp
        "updated_date"       # Last update timestamp
    ]
    
    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in columns]
    
    if len(missing_columns) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(required_columns)} required columns found")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing columns: {missing_columns}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-002: Validate dq_validation_audit Table Schema
# UTC-002: DQ Validation Audit Schema Validation
test_id = "UTC-002"
test_name = "Validate dq_validation_audit table exists with correct schema"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.dq_validation_audit"
    
    df = spark.sql(f"DESCRIBE TABLE {table_name}")
    columns = [row.col_name for row in df.collect()]
    
    # Core columns from actual schema
    required_columns = [
        "audit_id",              # Primary key (identity)
        "check_category",        # Freshness, Referential Integrity, Quality Rules, Completeness, Accuracy
        "check_name",            # Name of the specific check
        "check_type",            # SQL, PYTHON, GREAT_EXPECTATIONS
        "table_name",            # Fully qualified table name
        "column_name",           # Specific column being validated
        "run_timestamp",         # When the check was executed
        "run_date",              # Date partition key
        "execution_time_sec",    # Execution duration
        "status",                # PASS, FAIL, WARNING, ERROR, SKIPPED
        "violation_count",       # Number of violations
        "total_records",         # Total records evaluated
        "pass_rate",             # Pass rate percentage (generated)
        "metric",                # Specific metric value
        "threshold",             # Threshold/expected value
        "message"                # Detailed message or error details
    ]
    
    missing_columns = [col for col in required_columns if col not in columns]
    
    if len(missing_columns) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(required_columns)} required columns found")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing columns: {missing_columns}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-003: Validate etl_job_audit Table Schema
# UTC-003: ETL Job Audit Schema Validation
test_id = "UTC-003"
test_name = "Validate etl_job_audit table exists with correct schema"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.etl_job_audit"
    
    df = spark.sql(f"DESCRIBE TABLE {table_name}")
    columns = [row.col_name for row in df.collect()]
    
    # Core columns from actual schema
    required_columns = [
        "audit_id",          # Primary key (identity)
        "job_name",          # Name of the ETL job/pipeline
        "job_type",          # DLT, NOTEBOOK, WORKFLOW, SPARK_SUBMIT, DBT
        "job_layer",         # BRONZE, SILVER, GOLD
        "pipeline_name",     # Parent pipeline name
        "source_type",       # TABLE, FILE, API, KAFKA, etc.
        "target_table",      # Target table name (fully qualified)
        "start_time",        # Job start timestamp
        "end_time",          # Job end timestamp
        "run_date",          # Date partition key
        "duration_seconds",  # Job duration (generated)
        "status",            # RUNNING, SUCCESS, FAILED, TIMEOUT, CANCELLED
        "rows_read",         # Rows read from source
        "rows_written",      # Rows written to target
        "rows_updated",      # Rows updated (MERGE)
        "rows_deleted"       # Rows deleted (MERGE)
    ]
    
    missing_columns = [col for col in required_columns if col not in columns]
    
    if len(missing_columns) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(required_columns)} required columns found")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing columns: {missing_columns}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-004: Validate maintenance_audit Table Schema
# UTC-004: Maintenance Audit Schema Validation
test_id = "UTC-004"
test_name = "Validate maintenance_audit table exists with correct schema"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.maintenance_audit"
    
    df = spark.sql(f"DESCRIBE TABLE {table_name}")
    columns = [row.col_name for row in df.collect()]
    
    # Core columns from actual schema
    required_columns = [
        "audit_id",          # Primary key (identity)
        "table_name",        # Fully qualified table name
        "operation",         # OPTIMIZE, VACUUM, ANALYZE, ZORDER, REORG
        "operation_mode",    # FULL, INCREMENTAL, PARTITION
        "start_time",        # Operation start time
        "end_time",          # Operation end time
        "run_date",          # Date partition key
        "duration_seconds",  # Operation duration (generated)
        "status",            # SUCCESS, FAILED, RUNNING, SKIPPED
        "num_files_before", # Files before operation
        "num_files_after",   # Files after operation
        "files_added",       # Files added (generated)
        "files_removed",     # Files removed by VACUUM
        "size_before_bytes", # Table size before
        "size_after_bytes"   # Table size after
    ]
    
    missing_columns = [col for col in required_columns if col not in columns]
    
    if len(missing_columns) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(required_columns)} required columns found")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing columns: {missing_columns}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-005: Validate table_change_audit Table Schema
# UTC-005: Table Change Audit Schema Validation
test_id = "UTC-005"
test_name = "Validate table_change_audit table exists with correct schema"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.table_change_audit"
    
    df = spark.sql(f"DESCRIBE TABLE {table_name}")
    columns = [row.col_name for row in df.collect()]
    
    # Core columns from actual schema
    required_columns = [
        "audit_id",           # Primary key (identity)
        "table_name",         # Fully qualified table name
        "operation",          # INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, DROP
        "operation_type",     # DML, DDL, METADATA
        "change_timestamp",   # When the change occurred
        "change_date",        # Date partition key
        "version",            # Delta table version after operation
        "version_before",     # Delta table version before operation
        "rows_affected",      # Number of rows affected
        "rows_inserted",      # Rows inserted (MERGE)
        "rows_updated",       # Rows updated (MERGE)
        "rows_deleted",       # Rows deleted (MERGE/DELETE)
        "bytes_written",      # Bytes written to storage
        "files_added",        # Number of files added
        "files_removed"       # Number of files removed
    ]
    
    missing_columns = [col for col in required_columns if col not in columns]
    
    if len(missing_columns) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(required_columns)} required columns found")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing columns: {missing_columns}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Test Summary
# Display test summary
print("\n" + "="*80)
print("TEST SUITE 1: SCHEMA VALIDATION - SUMMARY")
print("="*80)

if len(test_results) > 0:
    total_tests = len(test_results)
    passed_tests = len([r for r in test_results if r["status"] == "PASS"])
    failed_tests = len([r for r in test_results if r["status"] == "FAIL"])
    pass_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"\nTotal Tests: {total_tests}")
    print(f"Passed: {passed_tests} ✓")
    print(f"Failed: {failed_tests} ✗")
    print(f"Pass Rate: {pass_rate:.1f}%")
    
    if failed_tests > 0:
        print("\nFailed Tests:")
        for result in test_results:
            if result["status"] == "FAIL":
                print(f"  ✗ {result['test_id']}: {result['message']}")
    
    if failed_tests == 0:
        print("\n✅ All schema validation tests passed!")
    else:
        print(f"\n⚠️  {failed_tests} schema validation test(s) failed.")
else:
    print("\n⚠️  No tests were run.")

print("\n" + "="*80)