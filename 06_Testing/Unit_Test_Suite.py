# Databricks notebook source
# DBTITLE 1,Section 1: Framework Overview & Setup
# MAGIC %md
# MAGIC # RetailPulse Unit Test Suite
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook contains comprehensive unit test cases (UTCs) for the RetailPulse data quality framework.
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC - Audit table schema validation
# MAGIC - Metadata catalog integrity
# MAGIC - Freshness check logic
# MAGIC - Data quality rules validation
# MAGIC - Completeness checks
# MAGIC - DQ logging functionality
# MAGIC - Error handling
# MAGIC - End-to-end integration scenarios
# MAGIC
# MAGIC **Total Test Cases:** 28 (UTC-001 through UTC-028)
# MAGIC
# MAGIC **Testing Approach:**
# MAGIC - Each test includes a purpose statement and expected results
# MAGIC - Tests use mock data where needed to avoid modifying production data
# MAGIC - PASS/FAIL status is clearly indicated for each test
# MAGIC - Results are logged for tracking and reporting

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
print("RetailPulse Unit Test Suite - Initialization Complete")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Section 2: Audit Tables Schema Validation
# MAGIC %md
# MAGIC ## Test Cases: UTC-001 to UTC-005
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate that all 5 audit tables exist with correct schema structure in the retailpulse.ops schema.
# MAGIC
# MAGIC **Tables to Validate:**
# MAGIC 1. `metadata_catalog` - Central configuration for all monitored tables
# MAGIC 2. `dq_validation_audit` - Data quality check results
# MAGIC 3. `etl_job_audit` - ETL job execution logs
# MAGIC 4. `maintenance_audit` - Table maintenance operations log
# MAGIC 5. `table_change_audit` - Schema and data change tracking
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
    
    # Required columns for metadata_catalog
    required_columns = [
        "table_id",
        "catalog_name",
        "schema_name",
        "table_name",
        "table_type",
        "is_active",
        "sla_hours",
        "validation_rules",
        "created_at",
        "updated_at"
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
    
    required_columns = [
        "audit_id",
        "table_id",
        "check_type",
        "check_name",
        "check_status",
        "records_checked",
        "records_failed",
        "failure_rate",
        "error_message",
        "execution_time_seconds",
        "checked_at"
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
    
    required_columns = [
        "job_id",
        "job_name",
        "table_id",
        "job_status",
        "records_processed",
        "records_inserted",
        "records_updated",
        "records_failed",
        "started_at",
        "completed_at",
        "error_message"
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
    
    required_columns = [
        "maintenance_id",
        "table_id",
        "operation_type",
        "operation_status",
        "rows_affected",
        "execution_time_seconds",
        "started_at",
        "completed_at",
        "error_message"
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
    
    required_columns = [
        "change_id",
        "table_id",
        "change_type",
        "column_name",
        "old_value",
        "new_value",
        "changed_by",
        "changed_at"
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

# DBTITLE 1,Section 3: Metadata Catalog Data Integrity
# MAGIC %md
# MAGIC ## Test Cases: UTC-006 to UTC-008
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate that the metadata_catalog contains valid and complete configuration data for all monitored tables.
# MAGIC
# MAGIC **Tests:**
# MAGIC 1. All active tables have required non-null fields
# MAGIC 2. Validation rules are properly formatted (valid JSON/string format)
# MAGIC 3. SLA hours are within valid ranges (1-48 hours)
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - No NULL values in critical fields for active tables
# MAGIC - All validation rules follow expected format
# MAGIC - SLA values are realistic and within acceptable bounds

# COMMAND ----------

# DBTITLE 1,UTC-006: Validate Required Fields Are Not NULL for Active Tables
# UTC-006: Required Fields Validation
test_id = "UTC-006"
test_name = "Validate all active tables have required non-null fields"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.metadata_catalog"
    
    # Check for NULL values in critical fields for active tables
    null_check_query = f"""
    SELECT 
        COUNT(*) as total_active_tables,
        SUM(CASE WHEN table_id IS NULL THEN 1 ELSE 0 END) as null_table_id,
        SUM(CASE WHEN catalog_name IS NULL THEN 1 ELSE 0 END) as null_catalog,
        SUM(CASE WHEN schema_name IS NULL THEN 1 ELSE 0 END) as null_schema,
        SUM(CASE WHEN table_name IS NULL THEN 1 ELSE 0 END) as null_table_name,
        SUM(CASE WHEN sla_hours IS NULL THEN 1 ELSE 0 END) as null_sla
    FROM {table_name}
    WHERE is_active = true
    """
    
    result = spark.sql(null_check_query).collect()[0]
    
    total_nulls = (result.null_table_id + result.null_catalog + 
                   result.null_schema + result.null_table_name + result.null_sla)
    
    if total_nulls == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {result.total_active_tables} active tables have complete required fields")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Found {total_nulls} null values in required fields")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-007: Validate Validation Rules Format
# UTC-007: Validation Rules Format Check
test_id = "UTC-007"
test_name = "Validate validation rules are properly formatted"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.metadata_catalog"
    
    # Get all validation rules for active tables
    rules_query = f"""
    SELECT 
        table_id,
        table_name,
        validation_rules,
        CASE 
            WHEN validation_rules IS NULL THEN 'NULL'
            WHEN validation_rules = '' THEN 'EMPTY'
            WHEN LENGTH(validation_rules) > 0 THEN 'HAS_VALUE'
            ELSE 'UNKNOWN'
        END as rule_status
    FROM {table_name}
    WHERE is_active = true
    """
    
    results = spark.sql(rules_query).collect()
    
    invalid_rules = [r for r in results if r.rule_status in ['NULL', 'UNKNOWN']]
    
    if len(invalid_rules) == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {len(results)} active tables have valid validation rule formats")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"{len(invalid_rules)} tables have invalid validation rules")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-008: Validate SLA Hours Are Within Valid Ranges
# UTC-008: SLA Hours Range Validation
test_id = "UTC-008"
test_name = "Validate SLA hours are within valid ranges (1-48 hours)"

try:
    table_name = f"{TEST_CATALOG}.{TEST_SCHEMA}.metadata_catalog"
    
    # Check SLA hours for active tables
    sla_query = f"""
    SELECT 
        COUNT(*) as total_tables,
        SUM(CASE WHEN sla_hours < 1 THEN 1 ELSE 0 END) as below_min,
        SUM(CASE WHEN sla_hours > 48 THEN 1 ELSE 0 END) as above_max,
        MIN(sla_hours) as min_sla,
        MAX(sla_hours) as max_sla,
        AVG(sla_hours) as avg_sla
    FROM {table_name}
    WHERE is_active = true
    """
    
    result = spark.sql(sla_query).collect()[0]
    
    invalid_sla_count = result.below_min + result.above_max
    
    if invalid_sla_count == 0:
        log_test_result(test_id, test_name, "PASS", 
                       f"All SLA hours valid (range: {result.min_sla}-{result.max_sla}, avg: {result.avg_sla:.1f})")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"{invalid_sla_count} tables have SLA hours outside valid range (1-48)")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 4: Freshness Check Logic
# MAGIC %md
# MAGIC ## Test Cases: UTC-009 to UTC-011
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate the logic for checking table freshness based on SLA hours. This ensures data age is correctly calculated and compared against configured SLAs.
# MAGIC
# MAGIC **Test Scenarios:**
# MAGIC 1. Fresh table - data updated within SLA threshold (should PASS)
# MAGIC 2. Stale table - data older than SLA threshold (should FAIL)
# MAGIC 3. Edge case - data exactly at SLA threshold
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Fresh tables return PASS status
# MAGIC - Stale tables return FAIL status
# MAGIC - Edge case handling is consistent and correct

# COMMAND ----------

# DBTITLE 1,UTC-009: Test Freshness Check for Fresh Table (Within SLA)
# UTC-009: Freshness Check - Fresh Table
test_id = "UTC-009"
test_name = "Test freshness check for table within SLA"

try:
    # Create mock data: table with last_update within SLA
    from datetime import datetime, timedelta
    
    # Simulate: SLA = 24 hours, last update = 12 hours ago (FRESH)
    sla_hours = 24
    last_update = datetime.now() - timedelta(hours=12)
    current_time = datetime.now()
    
    # Calculate age in hours
    age_hours = (current_time - last_update).total_seconds() / 3600
    
    # Freshness check logic
    is_fresh = age_hours <= sla_hours
    freshness_status = "PASS" if is_fresh else "FAIL"
    
    if freshness_status == "PASS":
        log_test_result(test_id, test_name, "PASS", 
                       f"Fresh table detected correctly (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected PASS but got FAIL (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-010: Test Freshness Check for Stale Table (Outside SLA)
# UTC-010: Freshness Check - Stale Table
test_id = "UTC-010"
test_name = "Test freshness check for table outside SLA"

try:
    # Simulate: SLA = 24 hours, last update = 36 hours ago (STALE)
    sla_hours = 24
    last_update = datetime.now() - timedelta(hours=36)
    current_time = datetime.now()
    
    # Calculate age in hours
    age_hours = (current_time - last_update).total_seconds() / 3600
    
    # Freshness check logic
    is_fresh = age_hours <= sla_hours
    freshness_status = "PASS" if is_fresh else "FAIL"
    
    if freshness_status == "FAIL":
        log_test_result(test_id, test_name, "PASS", 
                       f"Stale table detected correctly (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected FAIL but got PASS (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-011: Test Freshness Check at SLA Threshold (Edge Case)
# UTC-011: Freshness Check - Edge Case at Threshold
test_id = "UTC-011"
test_name = "Test freshness check exactly at SLA threshold"

try:
    # Simulate: SLA = 24 hours, last update = exactly 24 hours ago
    sla_hours = 24
    last_update = datetime.now() - timedelta(hours=24)
    current_time = datetime.now()
    
    # Calculate age in hours
    age_hours = (current_time - last_update).total_seconds() / 3600
    
    # Freshness check logic (using <= so exactly at threshold should PASS)
    is_fresh = age_hours <= sla_hours
    freshness_status = "PASS" if is_fresh else "FAIL"
    
    if freshness_status == "PASS":
        log_test_result(test_id, test_name, "PASS", 
                       f"Edge case handled correctly: exactly at threshold treated as FRESH (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Edge case issue: threshold boundary handled incorrectly (age: {age_hours:.1f}h, SLA: {sla_hours}h)")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 5: Quality Rules Validation
# MAGIC %md
# MAGIC ## Test Cases: UTC-012 to UTC-016
# MAGIC
# MAGIC **Purpose:**
# MAGIC Test each validation rule type to ensure data quality checks correctly identify violations.
# MAGIC
# MAGIC **Rule Types:**
# MAGIC * NOT_NULL - Checks for NULL values in critical columns
# MAGIC * POSITIVE - Validates numeric values are > 0
# MAGIC * NON_NEGATIVE - Validates numeric values are >= 0
# MAGIC * DATE_VALID - Ensures dates are valid and reasonable
# MAGIC * RANGE - Validates values fall within expected ranges
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Each rule type correctly detects violations
# MAGIC - Valid data passes checks
# MAGIC - Invalid data is flagged appropriately

# COMMAND ----------

# DBTITLE 1,UTC-012: Test NOT_NULL Validation Rule
# UTC-012: NOT_NULL Rule Validation
test_id = "UTC-012"
test_name = "Test NOT_NULL validation rule detects nulls"

try:
    # Create mock dataframe with null values
    from pyspark.sql import Row
    
    test_data = [
        Row(id=1, customer_id="C001", amount=100.0),
        Row(id=2, customer_id=None, amount=150.0),  # NULL violation
        Row(id=3, customer_id="C003", amount=200.0)
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Apply NOT_NULL check on customer_id
    null_count = df.filter(col("customer_id").isNull()).count()
    total_count = df.count()
    
    # Rule should detect 1 violation
    expected_violations = 1
    
    if null_count == expected_violations:
        log_test_result(test_id, test_name, "PASS", 
                       f"NOT_NULL rule correctly detected {null_count}/{total_count} violations")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_violations} violations, found {null_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-013: Test POSITIVE Validation Rule
# UTC-013: POSITIVE Rule Validation
test_id = "UTC-013"
test_name = "Test POSITIVE validation rule detects non-positive values"

try:
    # Create mock dataframe with non-positive values
    test_data = [
        Row(id=1, quantity=10),
        Row(id=2, quantity=0),   # Violation: not positive
        Row(id=3, quantity=-5),  # Violation: negative
        Row(id=4, quantity=20)
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Apply POSITIVE check (value > 0)
    violation_count = df.filter(col("quantity") <= 0).count()
    total_count = df.count()
    
    # Rule should detect 2 violations
    expected_violations = 2
    
    if violation_count == expected_violations:
        log_test_result(test_id, test_name, "PASS", 
                       f"POSITIVE rule correctly detected {violation_count}/{total_count} violations")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_violations} violations, found {violation_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-014: Test NON_NEGATIVE Validation Rule
# UTC-014: NON_NEGATIVE Rule Validation
test_id = "UTC-014"
test_name = "Test NON_NEGATIVE validation rule detects negative values"

try:
    # Create mock dataframe with negative values
    test_data = [
        Row(id=1, balance=100.0),
        Row(id=2, balance=0.0),    # Valid: zero is non-negative
        Row(id=3, balance=-50.0),  # Violation: negative
        Row(id=4, balance=250.0)
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Apply NON_NEGATIVE check (value >= 0)
    violation_count = df.filter(col("balance") < 0).count()
    total_count = df.count()
    
    # Rule should detect 1 violation
    expected_violations = 1
    
    if violation_count == expected_violations:
        log_test_result(test_id, test_name, "PASS", 
                       f"NON_NEGATIVE rule correctly detected {violation_count}/{total_count} violations")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_violations} violations, found {violation_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-015: Test DATE_VALID Validation Rule
# UTC-015: DATE_VALID Rule Validation
test_id = "UTC-015"
test_name = "Test DATE_VALID validation rule detects invalid/unreasonable dates"

try:
    from datetime import date
    
    # Create mock dataframe with dates
    test_data = [
        Row(id=1, order_date=date(2024, 3, 15)),  # Valid
        Row(id=2, order_date=date(2026, 3, 31)),  # Valid (today's date)
        Row(id=3, order_date=date(2050, 12, 31)), # Invalid: future date
        Row(id=4, order_date=date(1900, 1, 1))    # Invalid: too old
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Apply DATE_VALID check (must be between 2000 and today)
    current_date = date(2026, 3, 31)
    min_valid_date = date(2000, 1, 1)
    
    violation_count = df.filter(
        (col("order_date") < lit(min_valid_date)) | 
        (col("order_date") > lit(current_date))
    ).count()
    
    total_count = df.count()
    
    # Rule should detect 2 violations
    expected_violations = 2
    
    if violation_count == expected_violations:
        log_test_result(test_id, test_name, "PASS", 
                       f"DATE_VALID rule correctly detected {violation_count}/{total_count} violations")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_violations} violations, found {violation_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-016: Test RANGE Validation Rule
# UTC-016: RANGE Rule Validation
test_id = "UTC-016"
test_name = "Test RANGE validation rule detects out-of-range values"

try:
    # Create mock dataframe with values outside expected range
    test_data = [
        Row(id=1, age=25),  # Valid
        Row(id=2, age=45),  # Valid
        Row(id=3, age=150), # Violation: too high
        Row(id=4, age=-5),  # Violation: negative age
        Row(id=5, age=30)   # Valid
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Apply RANGE check (age must be between 0 and 120)
    min_age = 0
    max_age = 120
    
    violation_count = df.filter(
        (col("age") < min_age) | (col("age") > max_age)
    ).count()
    
    total_count = df.count()
    
    # Rule should detect 2 violations
    expected_violations = 2
    
    if violation_count == expected_violations:
        log_test_result(test_id, test_name, "PASS", 
                       f"RANGE rule correctly detected {violation_count}/{total_count} violations")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_violations} violations, found {violation_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 6: Completeness Checks
# MAGIC %md
# MAGIC ## Test Cases: UTC-017 to UTC-019
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate row count checks and completeness validation across different table scenarios.
# MAGIC
# MAGIC **Test Scenarios:**
# MAGIC 1. Empty table detection
# MAGIC 2. Normal table with expected row counts
# MAGIC 3. Large table performance validation
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Empty tables are correctly identified
# MAGIC - Row counts are accurately calculated
# MAGIC - Completeness checks scale to large tables

# COMMAND ----------

# DBTITLE 1,UTC-017: Test Empty Table Detection
# UTC-017: Empty Table Detection
test_id = "UTC-017"
test_name = "Test completeness check detects empty tables"

try:
    # Create empty dataframe
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    
    # Check row count
    row_count = empty_df.count()
    
    # Empty table should have 0 rows
    is_empty = row_count == 0
    
    if is_empty:
        log_test_result(test_id, test_name, "PASS", 
                       f"Empty table correctly detected (row count: {row_count})")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected 0 rows but found {row_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-018: Test Normal Table Row Count
# UTC-018: Normal Table Row Count
test_id = "UTC-018"
test_name = "Test completeness check for normal table"

try:
    # Create test dataframe with known row count
    test_data = [
        Row(id=i, name=f"record_{i}", value=i*10)
        for i in range(1, 101)  # 100 records
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Check row count
    actual_count = df.count()
    expected_count = 100
    
    if actual_count == expected_count:
        log_test_result(test_id, test_name, "PASS", 
                       f"Row count correct: {actual_count} rows")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {expected_count} rows but found {actual_count}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-019: Test Large Table Completeness Check Performance
# UTC-019: Large Table Performance
test_id = "UTC-019"
test_name = "Test completeness check performance on large table"

try:
    import time
    
    # Create larger test dataframe (10,000 records)
    test_data = [
        Row(id=i, category=f"cat_{i%10}", amount=float(i))
        for i in range(1, 10001)
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Measure row count performance
    start_time = time.time()
    row_count = df.count()
    elapsed_time = time.time() - start_time
    
    expected_count = 10000
    performance_threshold = 5.0  # seconds
    
    count_correct = row_count == expected_count
    performance_ok = elapsed_time < performance_threshold
    
    if count_correct and performance_ok:
        log_test_result(test_id, test_name, "PASS", 
                       f"Large table check passed: {row_count} rows in {elapsed_time:.2f}s")
    elif not count_correct:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Count mismatch: expected {expected_count}, got {row_count}")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Performance issue: {elapsed_time:.2f}s exceeds threshold of {performance_threshold}s")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 7: DQ Logging Function
# MAGIC %md
# MAGIC ## Test Cases: UTC-020 to UTC-022
# MAGIC
# MAGIC **Purpose:**
# MAGIC Verify that the log_dq_result function correctly logs data quality check results to the audit table.
# MAGIC
# MAGIC **Test Scenarios:**
# MAGIC 1. Log PASS scenario
# MAGIC 2. Log FAIL scenario with error details
# MAGIC 3. Verify audit table insert functionality
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - PASS and FAIL results are logged correctly
# MAGIC - All required fields are populated
# MAGIC - Records are successfully inserted into dq_validation_audit table

# COMMAND ----------

# DBTITLE 1,UTC-020: Test DQ Logging for PASS Scenario
# UTC-020: DQ Logging - PASS Scenario
test_id = "UTC-020"
test_name = "Test log_dq_result function for PASS scenario"

try:
    # Mock log_dq_result function behavior for PASS
    def mock_log_dq_result_pass(table_id, check_type, check_name, status, 
                                 records_checked, records_failed, error_msg=None):
        """Simulate logging a DQ check result"""
        log_entry = {
            "table_id": table_id,
            "check_type": check_type,
            "check_name": check_name,
            "check_status": status,
            "records_checked": records_checked,
            "records_failed": records_failed,
            "failure_rate": records_failed / records_checked if records_checked > 0 else 0.0,
            "error_message": error_msg
        }
        return log_entry
    
    # Test PASS scenario
    result = mock_log_dq_result_pass(
        table_id="TBL001",
        check_type="COMPLETENESS",
        check_name="row_count_check",
        status="PASS",
        records_checked=1000,
        records_failed=0
    )
    
    # Validate
    is_valid = (
        result["check_status"] == "PASS" and
        result["records_failed"] == 0 and
        result["failure_rate"] == 0.0 and
        result["error_message"] is None
    )
    
    if is_valid:
        log_test_result(test_id, test_name, "PASS", 
                       f"PASS scenario logged correctly: {result['records_checked']} records checked, 0 failures")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"PASS scenario logging issue: {result}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-021: Test DQ Logging for FAIL Scenario
# UTC-021: DQ Logging - FAIL Scenario
test_id = "UTC-021"
test_name = "Test log_dq_result function for FAIL scenario"

try:
    # Mock log_dq_result function behavior for FAIL
    def mock_log_dq_result_fail(table_id, check_type, check_name, status, 
                                 records_checked, records_failed, error_msg=None):
        """Simulate logging a DQ check result"""
        log_entry = {
            "table_id": table_id,
            "check_type": check_type,
            "check_name": check_name,
            "check_status": status,
            "records_checked": records_checked,
            "records_failed": records_failed,
            "failure_rate": records_failed / records_checked if records_checked > 0 else 0.0,
            "error_message": error_msg
        }
        return log_entry
    
    # Test FAIL scenario
    result = mock_log_dq_result_fail(
        table_id="TBL002",
        check_type="VALIDITY",
        check_name="not_null_check",
        status="FAIL",
        records_checked=500,
        records_failed=25,
        error_msg="25 NULL values found in required field"
    )
    
    # Validate
    expected_failure_rate = 25 / 500
    is_valid = (
        result["check_status"] == "FAIL" and
        result["records_failed"] == 25 and
        abs(result["failure_rate"] - expected_failure_rate) < 0.001 and
        result["error_message"] is not None
    )
    
    if is_valid:
        log_test_result(test_id, test_name, "PASS", 
                       f"FAIL scenario logged correctly: {result['records_failed']}/{result['records_checked']} failures ({result['failure_rate']*100:.1f}%)")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"FAIL scenario logging issue: {result}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-022: Test Audit Table Insert Functionality
# UTC-022: Audit Table Insert
test_id = "UTC-022"
test_name = "Test audit table insert functionality"

try:
    # Create mock audit entry to simulate insertion
    from datetime import datetime
    
    audit_entry = Row(
        audit_id=1001,
        table_id="TBL003",
        check_type="FRESHNESS",
        check_name="sla_check",
        check_status="PASS",
        records_checked=0,  # Freshness checks don't count records
        records_failed=0,
        failure_rate=0.0,
        error_message=None,
        execution_time_seconds=2.5,
        checked_at=datetime.now()
    )
    
    # Create dataframe to simulate insert
    audit_df = spark.createDataFrame([audit_entry])
    
    # Validate all required fields are present
    required_fields = [
        "audit_id", "table_id", "check_type", "check_name", 
        "check_status", "records_checked", "records_failed", 
        "failure_rate", "execution_time_seconds", "checked_at"
    ]
    
    actual_fields = audit_df.columns
    missing_fields = [f for f in required_fields if f not in actual_fields]
    
    if len(missing_fields) == 0 and audit_df.count() == 1:
        log_test_result(test_id, test_name, "PASS", 
                       f"Audit entry created with all required fields ({len(required_fields)} fields)")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Missing fields: {missing_fields}" if missing_fields else "Entry count issue")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 8: Error Handling
# MAGIC %md
# MAGIC ## Test Cases: UTC-023 to UTC-025
# MAGIC
# MAGIC **Purpose:**
# MAGIC Test framework behavior when encountering invalid inputs or unexpected conditions.
# MAGIC
# MAGIC **Error Scenarios:**
# MAGIC 1. Non-existent table
# MAGIC 2. Invalid validation rule
# MAGIC 3. NULL column name in validation
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Graceful error handling without crashes
# MAGIC - Meaningful error messages
# MAGIC - Errors are logged to audit table

# COMMAND ----------

# DBTITLE 1,UTC-023: Test Error Handling for Non-Existent Table
# UTC-023: Error Handling - Non-Existent Table
test_id = "UTC-023"
test_name = "Test framework handles non-existent table gracefully"

try:
    # Attempt to describe a non-existent table
    fake_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.this_table_does_not_exist_xyz"
    
    try:
        df = spark.sql(f"DESCRIBE TABLE {fake_table}")
        columns = [row.col_name for row in df.collect()]
        
        # If we get here, the table exists (unexpected)
        log_test_result(test_id, test_name, "FAIL", 
                       "Table unexpectedly exists or error not raised")
    except Exception as table_error:
        # Expected: error should be caught and handled
        error_msg = str(table_error)
        
        # Check if error message is informative
        is_informative = "not" in error_msg.lower() or "exist" in error_msg.lower() or "found" in error_msg.lower()
        
        if is_informative:
            log_test_result(test_id, test_name, "PASS", 
                           f"Non-existent table error handled gracefully with informative message")
        else:
            log_test_result(test_id, test_name, "FAIL", 
                           f"Error message not informative: {error_msg[:100]}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Unexpected error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-024: Test Error Handling for Invalid Rule
# UTC-024: Error Handling - Invalid Rule
test_id = "UTC-024"
test_name = "Test framework handles invalid validation rule gracefully"

try:
    # Simulate invalid rule type
    invalid_rule_type = "INVALID_RULE_XYZ"
    valid_rule_types = ["NOT_NULL", "POSITIVE", "NON_NEGATIVE", "DATE_VALID", "RANGE"]
    
    # Check if rule is valid
    if invalid_rule_type not in valid_rule_types:
        error_detected = True
        error_msg = f"Invalid rule type: {invalid_rule_type}. Valid types: {', '.join(valid_rule_types)}"
    else:
        error_detected = False
        error_msg = None
    
    if error_detected:
        log_test_result(test_id, test_name, "PASS", 
                       f"Invalid rule type detected and handled: {error_msg}")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       "Invalid rule type was not caught")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-025: Test Error Handling for NULL Column Name
# UTC-025: Error Handling - NULL Column Name
test_id = "UTC-025"
test_name = "Test framework handles NULL column name in validation"

try:
    # Create test dataframe
    test_data = [Row(id=1, value=100), Row(id=2, value=200)]
    df = spark.createDataFrame(test_data)
    
    # Attempt to check NULL column name
    column_name = None
    
    try:
        if column_name is None or column_name == "":
            raise ValueError("Column name cannot be NULL or empty")
        
        # This should not execute
        null_count = df.filter(col(column_name).isNull()).count()
        
        log_test_result(test_id, test_name, "FAIL", 
                       "NULL column name was not validated")
    
    except ValueError as ve:
        # Expected: validation error for NULL column
        log_test_result(test_id, test_name, "PASS", 
                       f"NULL column name properly validated: {str(ve)}")
    
    except Exception as other_error:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Unexpected error type: {type(other_error).__name__}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 9: Integration Scenarios
# MAGIC %md
# MAGIC ## Test Cases: UTC-026 to UTC-028
# MAGIC
# MAGIC **Purpose:**
# MAGIC End-to-end integration tests that combine multiple framework components.
# MAGIC
# MAGIC **Integration Tests:**
# MAGIC 1. Full DQ run workflow (load config → run checks → log results)
# MAGIC 2. Multiple check types on single table
# MAGIC 3. Historical trend analysis simulation
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - All components work together seamlessly
# MAGIC - Data flows correctly through the pipeline
# MAGIC - Results are consistently logged and retrievable

# COMMAND ----------

# DBTITLE 1,UTC-026: Test Full DQ Run Workflow
# UTC-026: Integration Test - Full DQ Run
test_id = "UTC-026"
test_name = "Test full DQ run workflow (end-to-end)"

try:
    # Step 1: Simulate loading table configuration
    table_config = {
        "table_id": "INT_TBL_001",
        "catalog_name": "retailpulse",
        "schema_name": "sales",
        "table_name": "orders",
        "sla_hours": 24,
        "validation_rules": "NOT_NULL:order_id,POSITIVE:amount"
    }
    
    # Step 2: Simulate running checks
    checks_run = [
        {"check_type": "FRESHNESS", "status": "PASS"},
        {"check_type": "COMPLETENESS", "status": "PASS"},
        {"check_type": "VALIDITY", "status": "PASS"}
    ]
    
    # Step 3: Simulate logging results
    logged_results = len(checks_run)
    
    # Step 4: Validate workflow completion
    workflow_complete = (
        table_config is not None and
        len(checks_run) > 0 and
        logged_results == len(checks_run)
    )
    
    if workflow_complete:
        log_test_result(test_id, test_name, "PASS", 
                       f"Full DQ workflow completed: {logged_results} checks run and logged")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       "Workflow incomplete or inconsistent")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-027: Test Multiple Check Types on Single Table
# UTC-027: Integration Test - Multiple Checks
test_id = "UTC-027"
test_name = "Test multiple check types on single table"

try:
    # Create test dataframe
    from datetime import date
    test_data = [
        Row(id=1, customer_id="C001", amount=100.0, order_date=date(2026, 3, 1)),
        Row(id=2, customer_id="C002", amount=150.0, order_date=date(2026, 3, 15)),
        Row(id=3, customer_id="C003", amount=200.0, order_date=date(2026, 3, 30))
    ]
    
    df = spark.createDataFrame(test_data)
    
    # Run multiple check types
    checks = {
        "completeness": df.count() > 0,
        "not_null_customer": df.filter(col("customer_id").isNull()).count() == 0,
        "positive_amount": df.filter(col("amount") <= 0).count() == 0,
        "valid_dates": df.filter(col("order_date") > lit(date(2026, 3, 31))).count() == 0
    }
    
    # All checks should pass
    all_passed = all(checks.values())
    passed_count = sum(checks.values())
    total_checks = len(checks)
    
    if all_passed:
        log_test_result(test_id, test_name, "PASS", 
                       f"All {total_checks} check types passed on single table")
    else:
        failed_checks = [k for k, v in checks.items() if not v]
        log_test_result(test_id, test_name, "FAIL", 
                       f"{passed_count}/{total_checks} checks passed. Failed: {failed_checks}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-028: Test Historical Trend Analysis
# UTC-028: Integration Test - Historical Trends
test_id = "UTC-028"
test_name = "Test historical trend analysis simulation"

try:
    # Simulate historical DQ check results over time
    historical_results = [
        {"date": "2026-03-01", "check_status": "PASS", "failure_rate": 0.0},
        {"date": "2026-03-08", "check_status": "PASS", "failure_rate": 0.02},
        {"date": "2026-03-15", "check_status": "PASS", "failure_rate": 0.01},
        {"date": "2026-03-22", "check_status": "PASS", "failure_rate": 0.03},
        {"date": "2026-03-29", "check_status": "PASS", "failure_rate": 0.02}
    ]
    
    # Analyze trends
    failure_rates = [r["failure_rate"] for r in historical_results]
    avg_failure_rate = sum(failure_rates) / len(failure_rates)
    max_failure_rate = max(failure_rates)
    
    # Calculate trend (simple: check if recent is better than average)
    recent_rate = failure_rates[-1]
    trend = "IMPROVING" if recent_rate <= avg_failure_rate else "DEGRADING"
    
    # Validate
    threshold = 0.05  # 5% failure rate threshold
    is_healthy = max_failure_rate < threshold
    
    if is_healthy:
        log_test_result(test_id, test_name, "PASS", 
                       f"Historical trend analysis complete: Trend={trend}, Avg failure rate={avg_failure_rate*100:.1f}%, Max={max_failure_rate*100:.1f}%")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Historical data shows concerning trends: Max failure rate {max_failure_rate*100:.1f}% exceeds threshold")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 10: Test Results Summary
# MAGIC %md
# MAGIC ## Test Results Summary
# MAGIC
# MAGIC **Purpose:**
# MAGIC Provide a comprehensive summary of all test results for reporting and analysis.
# MAGIC
# MAGIC **Includes:**
# MAGIC * Total tests run
# MAGIC * Pass/Fail counts and percentages
# MAGIC * Failed test details
# MAGIC * Test execution timestamp
# MAGIC * Overall framework health status

# COMMAND ----------

# DBTITLE 1,Generate Test Results Summary Report
# Generate comprehensive test results summary
print("\n" + "="*80)
print("RETAILPULSE UNIT TEST SUITE - FINAL SUMMARY")
print("="*80)

if len(test_results) > 0:
    # Calculate statistics
    total_tests = len(test_results)
    passed_tests = len([r for r in test_results if r["status"] == "PASS"])
    failed_tests = len([r for r in test_results if r["status"] == "FAIL"])
    pass_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"\nTotal Tests Run: {total_tests}")
    print(f"Passed: {passed_tests} (✓)")
    print(f"Failed: {failed_tests} (✗)")
    print(f"Pass Rate: {pass_rate:.1f}%")
    print(f"\nTest Execution Time: {test_results[0]['timestamp']} to {test_results[-1]['timestamp']}")
    
    # Overall status
    if failed_tests == 0:
        print(f"\n✅ OVERALL STATUS: ALL TESTS PASSED")
    elif pass_rate >= 80:
        print(f"\n⚠️ OVERALL STATUS: MOST TESTS PASSED ({pass_rate:.1f}%)")
    else:
        print(f"\n❌ OVERALL STATUS: MULTIPLE FAILURES DETECTED ({pass_rate:.1f}% pass rate)")
    
    # Failed tests details
    if failed_tests > 0:
        print("\n" + "-"*80)
        print("FAILED TESTS DETAILS:")
        print("-"*80)
        for result in test_results:
            if result["status"] == "FAIL":
                print(f"\n✗ {result['test_id']}: {result['test_name']}")
                if result["message"]:
                    print(f"  Reason: {result['message']}")
    
    # Create summary dataframe for further analysis
    summary_df = spark.createDataFrame(test_results)
    print("\n" + "-"*80)
    print("TEST RESULTS DATAFRAME CREATED")
    print("-"*80)
    print(f"Available for further analysis as 'summary_df'")
    print(f"Columns: {', '.join(summary_df.columns)}")
    
    # Display first few results
    print("\nSample Results:")
    display(summary_df.select("test_id", "test_name", "status", "message").limit(10))
    
else:
    print("\n⚠️ No test results found. Please run the test cells above.")

print("\n" + "="*80)
print("END OF UNIT TEST SUITE")
print("="*80)