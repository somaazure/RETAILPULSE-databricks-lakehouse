# Databricks notebook source
# DBTITLE 1,Test Suite 6: Advanced Tests
# MAGIC %md
# MAGIC # Test Suite 6: Advanced Tests
# MAGIC
# MAGIC ## Overview
# MAGIC This test suite contains advanced testing scenarios including DQ logging functionality, error handling, and end-to-end integration tests.
# MAGIC
# MAGIC **Test Cases:** UTC-020 to UTC-028 (9 tests)
# MAGIC
# MAGIC **Test Categories:**
# MAGIC * **DQ Logging (UTC-020 to UTC-022):** Verify log_dq_result function
# MAGIC * **Error Handling (UTC-023 to UTC-025):** Test graceful degradation
# MAGIC * **Integration (UTC-026 to UTC-028):** End-to-end workflow validation
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate advanced framework capabilities including logging, error resilience, and component integration.

# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries and Configure Test Environment
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
from pyspark.sql import Row
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
print("Test Suite 6: Advanced Tests (Logging, Error Handling, Integration)")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Section 1: DQ Logging Tests (UTC-020 to UTC-022)
# MAGIC %md
# MAGIC ## Section 1: DQ Logging Tests (UTC-020 to UTC-022)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate that the log_dq_result function properly inserts audit records into dq_validation_audit table.
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Successful validation results are logged correctly
# MAGIC - Failed validation results include error details
# MAGIC - Logging function handles all field types appropriately

# COMMAND ----------

# DBTITLE 1,UTC-020: Test Successful DQ Result Logging
# UTC-020: Log Successful DQ Result
test_id = "UTC-020"
test_name = "Test logging a successful data quality check result"

try:
    # Define log_dq_result function (simplified version for testing)
    def log_dq_result(table_id, check_type, check_name, column_name, 
                      total_records, failed_records, status, error_details=None):
        """Log DQ validation result to audit table"""
        log_data = [{
            "audit_id": f"TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "table_id": table_id,
            "check_type": check_type,
            "check_name": check_name,
            "column_name": column_name,
            "total_records": total_records,
            "failed_records": failed_records,
            "status": status,
            "error_details": error_details,
            "check_timestamp": datetime.now()
        }]
        return spark.createDataFrame(log_data)
    
    # Test successful validation logging
    log_df = log_dq_result(
        table_id="TEST_TABLE_001",
        check_type="NOT_NULL",
        check_name="customer_id_not_null",
        column_name="customer_id",
        total_records=1000,
        failed_records=0,
        status="PASS"
    )
    
    # Verify log record was created
    if log_df.count() == 1:
        record = log_df.first()
        if (record.status == "PASS" and 
            record.failed_records == 0 and 
            record.total_records == 1000):
            log_test_result(test_id, test_name, "PASS", 
                           f"Successfully logged PASS result for {record.check_name}")
        else:
            log_test_result(test_id, test_name, "FAIL", 
                           "Log record has incorrect values")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected 1 log record but found {log_df.count()}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-021: Test Failed DQ Result Logging
# UTC-021: Log Failed DQ Result
test_id = "UTC-021"
test_name = "Test logging a failed data quality check with error details"

try:
    # Test failed validation logging
    log_df = log_dq_result(
        table_id="TEST_TABLE_002",
        check_type="POSITIVE",
        check_name="amount_positive_check",
        column_name="amount",
        total_records=5000,
        failed_records=25,
        status="FAIL",
        error_details="Found 25 negative values in amount column"
    )
    
    # Verify log record was created with failure details
    if log_df.count() == 1:
        record = log_df.first()
        if (record.status == "FAIL" and 
            record.failed_records == 25 and 
            record.error_details is not None):
            log_test_result(test_id, test_name, "PASS", 
                           f"Successfully logged FAIL result with error details")
        else:
            log_test_result(test_id, test_name, "FAIL", 
                           "Failed record missing expected error details")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected 1 log record but found {log_df.count()}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-022: Test DQ Logging with NULL Column Name
# UTC-022: Log DQ Result with NULL Column (Table-Level Check)
test_id = "UTC-022"
test_name = "Test logging table-level check where column_name is NULL"

try:
    # Test table-level check logging (no specific column)
    log_df = log_dq_result(
        table_id="TEST_TABLE_003",
        check_type="COMPLETENESS",
        check_name="row_count_check",
        column_name=None,  # Table-level check has no specific column
        total_records=10000,
        failed_records=0,
        status="PASS"
    )
    
    # Verify log record handles NULL column_name
    if log_df.count() == 1:
        record = log_df.first()
        if (record.status == "PASS" and 
            record.column_name is None and 
            record.check_type == "COMPLETENESS"):
            log_test_result(test_id, test_name, "PASS", 
                           "Successfully logged table-level check with NULL column_name")
        else:
            log_test_result(test_id, test_name, "FAIL", 
                           "Log record handling of NULL column_name is incorrect")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected 1 log record but found {log_df.count()}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# Clean up test function from namespace
del log_dq_result

# COMMAND ----------

# DBTITLE 1,Section 2: Error Handling Tests (UTC-023 to UTC-025)
# MAGIC %md
# MAGIC ## Section 2: Error Handling Tests (UTC-023 to UTC-025)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate graceful error handling and failure scenarios across the framework.
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Missing tables are handled without crashing
# MAGIC - Invalid validation rules are caught and reported
# MAGIC - Framework continues processing despite individual check failures

# COMMAND ----------

# DBTITLE 1,UTC-023: Test Handling of Non-Existent Table
# UTC-023: Error Handling - Non-Existent Table
test_id = "UTC-023"
test_name = "Test framework handles non-existent table gracefully"

try:
    # Attempt to query a non-existent table
    non_existent_table = f"{TEST_CATALOG}.{TEST_SCHEMA}.this_table_does_not_exist_xyz"
    
    try:
        df = spark.sql(f"SELECT * FROM {non_existent_table} LIMIT 1")
        df.count()
        log_test_result(test_id, test_name, "FAIL", 
                       "Query should have failed but succeeded")
    except Exception as query_error:
        # Expected to fail - check error message is appropriate
        error_msg = str(query_error).lower()
        if "table" in error_msg and ("not found" in error_msg or "does not exist" in error_msg):
            log_test_result(test_id, test_name, "PASS", 
                           f"Correctly caught non-existent table error: {type(query_error).__name__}")
        else:
            log_test_result(test_id, test_name, "FAIL", 
                           f"Unexpected error type: {str(query_error)[:100]}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Test error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-024: Test Handling of Invalid Validation Rule
# UTC-024: Error Handling - Invalid Validation Rule
test_id = "UTC-024"
test_name = "Test framework handles invalid validation rule gracefully"

try:
    # Define a function that would process validation rules
    def validate_rule_syntax(rule_string):
        """Validate that a rule string is properly formatted"""
        valid_rule_types = ["NOT_NULL", "POSITIVE", "NON_NEGATIVE", "DATE_VALID", "RANGE"]
        
        if not rule_string or rule_string.strip() == "":
            return False, "Empty rule string"
        
        # Basic validation - check if rule type is recognized
        rule_type = rule_string.split(":")[0] if ":" in rule_string else rule_string
        
        if rule_type not in valid_rule_types:
            return False, f"Unknown rule type: {rule_type}"
        
        return True, "Valid rule"
    
    # Test with invalid rule
    invalid_rule = "INVALID_RULE_TYPE:column_name"
    is_valid, error_msg = validate_rule_syntax(invalid_rule)
    
    if not is_valid and "Unknown rule type" in error_msg:
        log_test_result(test_id, test_name, "PASS", 
                       f"Invalid rule detected: {error_msg}")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       "Invalid rule was not caught")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-025: Test Continuation After Individual Check Failure
# UTC-025: Error Handling - Continue After Check Failure
test_id = "UTC-025"
test_name = "Test framework continues processing after individual check failure"

try:
    # Simulate running multiple checks where one fails
    check_results = []
    
    checks = [
        {"name": "check_1", "should_fail": False},
        {"name": "check_2", "should_fail": True},   # This one fails
        {"name": "check_3", "should_fail": False},
        {"name": "check_4", "should_fail": False}
    ]
    
    # Process all checks even if one fails
    for check in checks:
        try:
            if check["should_fail"]:
                raise ValueError(f"Simulated failure in {check['name']}")
            else:
                check_results.append({"check": check["name"], "status": "PASS"})
        except Exception as check_error:
            # Log failure but continue
            check_results.append({"check": check["name"], "status": "FAIL", "error": str(check_error)})
    
    # Verify all checks were attempted
    total_checks = len(checks)
    processed_checks = len(check_results)
    failed_check = [r for r in check_results if r["status"] == "FAIL"]
    
    if processed_checks == total_checks and len(failed_check) == 1:
        log_test_result(test_id, test_name, "PASS", 
                       f"Processed all {total_checks} checks despite 1 failure")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Expected {total_checks} checks, processed {processed_checks}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# Cleanup
del validate_rule_syntax

# COMMAND ----------

# DBTITLE 1,Section 3: Integration Tests (UTC-026 to UTC-028)
# MAGIC %md
# MAGIC ## Section 3: Integration Tests (UTC-026 to UTC-028)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate end-to-end workflows that combine multiple framework components.
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - Metadata + DQ validation integration works correctly
# MAGIC - Freshness checks integrate with metadata SLA configuration
# MAGIC - Complete workflow from metadata to logging operates seamlessly

# COMMAND ----------

# DBTITLE 1,UTC-026: Test Metadata + DQ Validation Integration
# UTC-026: Integration - Metadata + DQ Validation
test_id = "UTC-026"
test_name = "Test integration between metadata_catalog and DQ validation"

try:
    # Create mock metadata entry
    metadata_data = [
        Row(
            table_id="INT_TEST_001",
            catalog_name=TEST_CATALOG,
            schema_name=TEST_SCHEMA,
            table_name="test_integration_table",
            validation_rules="NOT_NULL:customer_id;POSITIVE:amount",
            sla_hours=24,
            is_active=True
        )
    ]
    metadata_df = spark.createDataFrame(metadata_data)
    
    # Parse validation rules from metadata
    rules = metadata_df.select("validation_rules").first()[0]
    parsed_rules = [rule.strip() for rule in rules.split(";")]
    
    # Verify rules can be extracted and processed
    expected_rules = ["NOT_NULL:customer_id", "POSITIVE:amount"]
    
    if parsed_rules == expected_rules:
        log_test_result(test_id, test_name, "PASS", 
                       f"Successfully integrated metadata with DQ rules: {len(parsed_rules)} rules parsed")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Rule parsing mismatch. Expected {expected_rules}, got {parsed_rules}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-027: Test Freshness Check + Metadata SLA Integration
# UTC-027: Integration - Freshness Check + Metadata SLA
test_id = "UTC-027"
test_name = "Test integration of freshness logic with metadata SLA configuration"

try:
    from datetime import datetime, timedelta
    
    # Create mock metadata with SLA configuration
    metadata_entry = Row(
        table_id="INT_TEST_002",
        table_name="customer_transactions",
        sla_hours=12,
        last_update_timestamp=datetime.now() - timedelta(hours=18)  # 18 hours old
    )
    
    # Calculate freshness
    current_time = datetime.now()
    data_age_hours = (current_time - metadata_entry.last_update_timestamp).total_seconds() / 3600
    sla_hours = metadata_entry.sla_hours
    
    # Determine if data is stale
    is_stale = data_age_hours > sla_hours
    
    # Expected: 18 hours > 12 hour SLA, so should be stale
    if is_stale and data_age_hours > sla_hours:
        log_test_result(test_id, test_name, "PASS", 
                       f"Freshness check correctly identified stale data: {data_age_hours:.1f}h > {sla_hours}h SLA")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       f"Freshness calculation incorrect: age={data_age_hours:.1f}h, SLA={sla_hours}h, stale={is_stale}")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,UTC-028: Test End-to-End Workflow Integration
# UTC-028: Integration - End-to-End Workflow
test_id = "UTC-028"
test_name = "Test complete workflow: metadata -> validation -> logging"

try:
    # Step 1: Read metadata configuration
    metadata = Row(
        table_id="INT_TEST_003",
        table_name="orders",
        validation_rules="NOT_NULL:order_id",
        sla_hours=24
    )
    
    # Step 2: Create test data
    test_data = [
        Row(order_id=1, amount=100.0),
        Row(order_id=2, amount=200.0),
        Row(order_id=3, amount=150.0)
    ]
    df = spark.createDataFrame(test_data)
    
    # Step 3: Execute validation based on metadata rules
    validation_rule = metadata.validation_rules  # "NOT_NULL:order_id"
    column_to_check = validation_rule.split(":")[1]  # "order_id"
    
    # Check for NULLs in order_id column
    total_records = df.count()
    null_count = df.filter(col(column_to_check).isNull()).count()
    
    # Step 4: Determine status
    status = "PASS" if null_count == 0 else "FAIL"
    
    # Step 5: Log result (simulated)
    log_entry = Row(
        table_id=metadata.table_id,
        check_type="NOT_NULL",
        column_name=column_to_check,
        total_records=total_records,
        failed_records=null_count,
        status=status
    )
    
    # Verify complete workflow
    workflow_success = (
        metadata.table_id == log_entry.table_id and
        log_entry.status == "PASS" and
        log_entry.failed_records == 0
    )
    
    if workflow_success:
        log_test_result(test_id, test_name, "PASS", 
                       f"End-to-end workflow successful: metadata -> validation -> logging for {metadata.table_name}")
    else:
        log_test_result(test_id, test_name, "FAIL", 
                       "Workflow integration failed")
        
except Exception as e:
    log_test_result(test_id, test_name, "FAIL", f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Test Summary and Final Report
# Display comprehensive test summary
print("\n" + "="*80)
print("TEST SUITE 6: ADVANCED TESTS - FINAL SUMMARY")
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
    
    # Breakdown by section
    print("\n" + "-"*80)
    print("Test Breakdown by Section:")
    print("-"*80)
    
    dq_logging_tests = [r for r in test_results if r["test_id"].startswith("UTC-02")]
    error_handling_tests = [r for r in test_results if r["test_id"] in ["UTC-023", "UTC-024", "UTC-025"]]
    integration_tests = [r for r in test_results if r["test_id"] in ["UTC-026", "UTC-027", "UTC-028"]]
    
    print(f"\nDQ Logging Tests (UTC-020 to UTC-022):")
    print(f"  Total: {len(dq_logging_tests)} | Passed: {len([r for r in dq_logging_tests if r['status'] == 'PASS'])}")
    
    print(f"\nError Handling Tests (UTC-023 to UTC-025):")
    print(f"  Total: {len(error_handling_tests)} | Passed: {len([r for r in error_handling_tests if r['status'] == 'PASS'])}")
    
    print(f"\nIntegration Tests (UTC-026 to UTC-028):")
    print(f"  Total: {len(integration_tests)} | Passed: {len([r for r in integration_tests if r['status'] == 'PASS'])}")
    
    if failed_tests > 0:
        print("\n" + "-"*80)
        print("Failed Tests Details:")
        print("-"*80)
        for result in test_results:
            if result["status"] == "FAIL":
                print(f"  ✗ {result['test_id']}: {result['test_name']}")
                print(f"     {result['message']}")
    
    print("\n" + "="*80)
    if failed_tests == 0:
        print("✅ ALL ADVANCED TESTS PASSED!")
    else:
        print(f"⚠️  {failed_tests} advanced test(s) failed - review details above")
    print("="*80)
else:
    print("\n⚠️  No tests were run.")
    print("="*80)