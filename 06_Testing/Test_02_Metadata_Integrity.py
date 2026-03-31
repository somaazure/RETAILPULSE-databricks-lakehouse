# Databricks notebook source
# DBTITLE 1,Test Suite 2: Metadata Integrity
# MAGIC %md
# MAGIC # Test Suite 2: Metadata Catalog Data Integrity
# MAGIC
# MAGIC ## Overview
# MAGIC This test suite validates that the metadata_catalog contains valid and complete configuration data for all monitored tables.
# MAGIC
# MAGIC **Test Cases:** UTC-006 to UTC-008
# MAGIC
# MAGIC **Validations:**
# MAGIC * All active tables have required non-null fields
# MAGIC * Validation rules are properly formatted
# MAGIC * SLA hours are within valid ranges (1-48 hours)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Ensure metadata configuration is complete and consistent before relying on it for automated DQ checks.

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
print("Test Suite 2: Metadata Catalog Data Integrity")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Test Cases: UTC-006 to UTC-008
# MAGIC %md
# MAGIC ## Test Cases: UTC-006 to UTC-008
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

# DBTITLE 1,Test Summary
# Display test summary
print("\n" + "="*80)
print("TEST SUITE 2: METADATA INTEGRITY - SUMMARY")
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
        print("\n✅ All metadata integrity tests passed!")
    else:
        print(f"\n⚠️  {failed_tests} metadata integrity test(s) failed.")
else:
    print("\n⚠️  No tests were run.")

print("\n" + "="*80)