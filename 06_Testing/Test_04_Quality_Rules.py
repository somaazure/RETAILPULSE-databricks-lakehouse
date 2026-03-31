# Databricks notebook source
# DBTITLE 1,Test Suite 4: Quality Rules Validation
# MAGIC %md
# MAGIC # Test Suite 4: Quality Rules Validation
# MAGIC
# MAGIC ## Overview
# MAGIC This test suite validates each validation rule type to ensure data quality checks correctly identify violations.
# MAGIC
# MAGIC **Test Cases:** UTC-012 to UTC-016
# MAGIC
# MAGIC **Rule Types Tested:**
# MAGIC * NOT_NULL - Checks for NULL values in critical columns
# MAGIC * POSITIVE - Validates numeric values are > 0
# MAGIC * NON_NEGATIVE - Validates numeric values are >= 0
# MAGIC * DATE_VALID - Ensures dates are valid and reasonable
# MAGIC * RANGE - Validates values fall within expected ranges
# MAGIC
# MAGIC **Purpose:**
# MAGIC Validate that each rule type correctly detects violations and appropriately flags invalid data.

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
print("Test Suite 4: Quality Rules Validation")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Test Cases: UTC-012 to UTC-016
# MAGIC %md
# MAGIC ## Test Cases: UTC-012 to UTC-016
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

# DBTITLE 1,Test Summary
# Display test summary
print("\n" + "="*80)
print("TEST SUITE 4: QUALITY RULES VALIDATION - SUMMARY")
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
        print("\n✅ All quality rules validation tests passed!")
    else:
        print(f"\n⚠️  {failed_tests} quality rules validation test(s) failed.")
else:
    print("\n⚠️  No tests were run.")

print("\n" + "="*80)