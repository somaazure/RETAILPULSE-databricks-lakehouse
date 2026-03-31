# Databricks notebook source
# DBTITLE 1,Test Suite 3: Freshness Check Logic
# MAGIC %md
# MAGIC # Test Suite 3: Freshness Check Logic
# MAGIC
# MAGIC ## Overview
# MAGIC This test suite validates the logic for checking table freshness based on SLA hours.
# MAGIC
# MAGIC **Test Cases:** UTC-009 to UTC-011
# MAGIC
# MAGIC **Test Scenarios:**
# MAGIC * Fresh table - data updated within SLA threshold (should PASS)
# MAGIC * Stale table - data older than SLA threshold (should FAIL)
# MAGIC * Edge case - data exactly at SLA threshold
# MAGIC
# MAGIC **Purpose:**
# MAGIC Ensure data age is correctly calculated and compared against configured SLAs for accurate freshness detection.

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
print("Test Suite 3: Freshness Check Logic")
print("=" * 80)
print(f"Test Catalog: {TEST_CATALOG}")
print(f"Test Schema: {TEST_SCHEMA}")
print(f"Test Start Time: {datetime.now()}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Test Cases: UTC-009 to UTC-011
# MAGIC %md
# MAGIC ## Test Cases: UTC-009 to UTC-011
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

# DBTITLE 1,Test Summary
# Display test summary
print("\n" + "="*80)
print("TEST SUITE 3: FRESHNESS CHECK LOGIC - SUMMARY")
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
        print("\n✅ All freshness check logic tests passed!")
    else:
        print(f"\n⚠️  {failed_tests} freshness check logic test(s) failed.")
else:
    print("\n⚠️  No tests were run.")

print("\n" + "="*80)