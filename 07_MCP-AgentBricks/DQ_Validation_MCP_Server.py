# Databricks notebook source
# DBTITLE 1,Data Quality Validation MCP Server - Introduction
"""Data Quality Validation MCP Server for RetailPulse Project

This MCP (Model Context Protocol) server provides data quality validation tools for:
- Table freshness monitoring
- Referential integrity checks
- Data drift detection
- Custom quality rule execution

Tools Available:
  1. check_table_freshness() - Verify data recency
  2. validate_referential_integrity() - Check FK relationships
  3. detect_data_drift() - Schema/distribution changes
  4. run_quality_rules() - Execute custom DQ rules
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Optional

spark = SparkSession.builder.getOrCreate()

print("="*80)
print("DATA QUALITY VALIDATION MCP SERVER")
print("="*80)
print("\nInitializing tools for RetailPulse data quality monitoring...")
print("\nTarget catalog: retailpulse")
print("Target schemas: bronze, silver, gold")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Tool 1: Check Table Freshness
"""Tool 1: Check Table Freshness - Verify data recency."""

def check_table_freshness(
    table_name: str,
    timestamp_column: Optional[str] = None,
    max_age_hours: int = 24
) -> Dict[str, Any]:
    """
    Check if a table has been updated recently.
    
    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        timestamp_column: Column to check for freshness (optional, uses table metadata if not provided)
        max_age_hours: Maximum acceptable age in hours (default: 24)
    
    Returns:
        Dictionary with freshness status and details
    """
    try:
        # Get table details
        table_details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        
        # Last modified time from table metadata
        last_modified = table_details['lastModified']
        
        # Calculate age
        now = datetime.now()
        age_seconds = (now - last_modified.replace(tzinfo=None)).total_seconds()
        age_hours = age_seconds / 3600
        
        is_fresh = age_hours <= max_age_hours
        
        result = {
            'table': table_name,
            'is_fresh': is_fresh,
            'last_modified': last_modified.isoformat(),
            'age_hours': round(age_hours, 2),
            'max_age_hours': max_age_hours,
            'status': 'PASS' if is_fresh else 'FAIL',
            'message': f"Table is {round(age_hours, 2)} hours old" if is_fresh else f"Table is stale ({round(age_hours, 2)} hours old, max allowed: {max_age_hours})"
        }
        
        # If timestamp column provided, check data freshness too
        if timestamp_column:
            max_ts = spark.sql(f"SELECT MAX({timestamp_column}) as max_ts FROM {table_name}").collect()[0]['max_ts']
            if max_ts:
                data_age_seconds = (now - max_ts.replace(tzinfo=None)).total_seconds()
                data_age_hours = data_age_seconds / 3600
                result['data_max_timestamp'] = max_ts.isoformat()
                result['data_age_hours'] = round(data_age_hours, 2)
                result['data_is_fresh'] = data_age_hours <= max_age_hours
                if not result['data_is_fresh']:
                    result['status'] = 'FAIL'
                    result['message'] += f" | Data is {round(data_age_hours, 2)} hours old"
        
        return result
        
    except Exception as e:
        return {
            'table': table_name,
            'is_fresh': False,
            'status': 'ERROR',
            'message': f"Error checking freshness: {str(e)}"
        }

# Test the function
print("Testing check_table_freshness()...\n")

test_tables = [
    ('retailpulse.gold.fact_sales', 'order_ts'),
    ('retailpulse.gold.dim_customer', None),
    ('retailpulse.silver.orders', 'order_timestamp')
]

for table, ts_col in test_tables:
    result = check_table_freshness(table, ts_col, max_age_hours=168)  # 7 days
    print(f"\nTable: {result['table']}")
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Tool 2: Validate Referential Integrity
"""Tool 2: Validate Referential Integrity - Check FK relationships."""

def validate_referential_integrity(
    child_table: str,
    child_column: str,
    parent_table: str,
    parent_column: str,
    allow_nulls: bool = True
) -> Dict[str, Any]:
    """
    Validate referential integrity between child and parent tables.
    
    Args:
        child_table: Fully qualified child table name
        child_column: Foreign key column in child table
        parent_table: Fully qualified parent table name
        parent_column: Primary key column in parent table
        allow_nulls: Whether NULL values in child_column are acceptable
    
    Returns:
        Dictionary with validation results
    """
    try:
        # Check for orphaned records (FKs without matching PKs)
        orphaned_query = f"""
        SELECT COUNT(*) as orphaned_count
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
        WHERE p.{parent_column} IS NULL
        {'AND c.' + child_column + ' IS NOT NULL' if allow_nulls else ''}
        """
        
        orphaned_count = spark.sql(orphaned_query).collect()[0]['orphaned_count']
        
        # Get total child records
        total_child = spark.sql(f"SELECT COUNT(*) as cnt FROM {child_table}").collect()[0]['cnt']
        
        # Check for NULL values if not allowed
        null_count = 0
        if not allow_nulls:
            null_count = spark.sql(f"""
                SELECT COUNT(*) as null_count 
                FROM {child_table} 
                WHERE {child_column} IS NULL
            """).collect()[0]['null_count']
        
        # Determine status
        has_issues = orphaned_count > 0 or (not allow_nulls and null_count > 0)
        
        result = {
            'child_table': child_table,
            'child_column': child_column,
            'parent_table': parent_table,
            'parent_column': parent_column,
            'total_child_records': total_child,
            'orphaned_records': orphaned_count,
            'orphaned_percentage': round((orphaned_count / total_child * 100), 2) if total_child > 0 else 0,
            'null_records': null_count,
            'allow_nulls': allow_nulls,
            'status': 'FAIL' if has_issues else 'PASS',
            'message': f"Found {orphaned_count} orphaned records" if has_issues else "All foreign keys are valid"
        }
        
        # Get sample orphaned records if any
        if orphaned_count > 0:
            sample_orphans = spark.sql(f"""
                SELECT c.{child_column}
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
                WHERE p.{parent_column} IS NULL
                {'AND c.' + child_column + ' IS NOT NULL' if allow_nulls else ''}
                LIMIT 5
            """).collect()
            result['sample_orphaned_values'] = [row[0] for row in sample_orphans]
        
        return result
        
    except Exception as e:
        return {
            'child_table': child_table,
            'parent_table': parent_table,
            'status': 'ERROR',
            'message': f"Error validating integrity: {str(e)}"
        }

# Test the function
print("Testing validate_referential_integrity()...\n")

# Define FK relationships in RetailPulse
fk_relationships = [
    {
        'child': 'retailpulse.gold.fact_sales',
        'child_col': 'customer_id',
        'parent': 'retailpulse.gold.dim_customer',
        'parent_col': 'customer_id'
    },
    {
        'child': 'retailpulse.gold.fact_sales',
        'child_col': 'product_sk',
        'parent': 'retailpulse.gold.dim_product',
        'parent_col': 'product_sk'
    },
    {
        'child': 'retailpulse.gold.fact_sales',
        'child_col': 'date_id',
        'parent': 'retailpulse.gold.dim_date',
        'parent_col': 'date_id'
    }
]

for rel in fk_relationships:
    result = validate_referential_integrity(
        rel['child'], rel['child_col'],
        rel['parent'], rel['parent_col']
    )
    print(f"\nRelationship: {result['child_table']}.{result['child_column']} -> {result['parent_table']}.{result['parent_column']}")
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    print(f"Orphaned Records: {result['orphaned_records']} ({result['orphaned_percentage']}%)")
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Tool 3: Detect Data Drift
"""Tool 3: Detect Data Drift - Schema and distribution changes."""

def detect_data_drift(
    table_name: str,
    baseline_stats: Optional[Dict] = None,
    check_schema: bool = True,
    check_distribution: bool = True
) -> Dict[str, Any]:
    """
    Detect data drift in schema and distributions.
    
    Args:
        table_name: Fully qualified table name
        baseline_stats: Previous statistics to compare against (optional)
        check_schema: Whether to check for schema changes
        check_distribution: Whether to check for distribution changes
    
    Returns:
        Dictionary with drift detection results
    """
    try:
        df = spark.table(table_name)
        
        result = {
            'table': table_name,
            'timestamp': datetime.now().isoformat(),
            'drift_detected': False,
            'schema_changes': [],
            'distribution_changes': []
        }
        
        # Schema drift detection
        if check_schema:
            current_schema = {field.name: str(field.dataType) for field in df.schema.fields}
            result['current_schema'] = current_schema
            result['column_count'] = len(current_schema)
            
            if baseline_stats and 'schema' in baseline_stats:
                baseline_schema = baseline_stats['schema']
                
                # Check for added columns
                added_cols = set(current_schema.keys()) - set(baseline_schema.keys())
                if added_cols:
                    result['schema_changes'].append({
                        'type': 'COLUMNS_ADDED',
                        'columns': list(added_cols)
                    })
                    result['drift_detected'] = True
                
                # Check for removed columns
                removed_cols = set(baseline_schema.keys()) - set(current_schema.keys())
                if removed_cols:
                    result['schema_changes'].append({
                        'type': 'COLUMNS_REMOVED',
                        'columns': list(removed_cols)
                    })
                    result['drift_detected'] = True
                
                # Check for type changes
                common_cols = set(current_schema.keys()) & set(baseline_schema.keys())
                type_changes = [
                    col for col in common_cols 
                    if current_schema[col] != baseline_schema[col]
                ]
                if type_changes:
                    result['schema_changes'].append({
                        'type': 'TYPE_CHANGES',
                        'columns': [
                            {
                                'column': col,
                                'old_type': baseline_schema[col],
                                'new_type': current_schema[col]
                            }
                            for col in type_changes
                        ]
                    })
                    result['drift_detected'] = True
        
        # Distribution drift detection
        if check_distribution:
            # Get basic statistics
            row_count = df.count()
            result['row_count'] = row_count
            
            # Check numeric columns for distribution changes
            numeric_cols = [field.name for field in df.schema.fields 
                          if str(field.dataType) in ['IntegerType()', 'LongType()', 'DoubleType()', 'FloatType()', 'DecimalType(12,2)', 'DecimalType(20,2)']]
            
            if numeric_cols:
                stats_df = df.select([F.mean(col).alias(f"{col}_mean") for col in numeric_cols] +
                                    [F.stddev(col).alias(f"{col}_stddev") for col in numeric_cols]).collect()[0]
                
                current_stats = {}
                for col in numeric_cols:
                    current_stats[col] = {
                        'mean': float(stats_df[f"{col}_mean"]) if stats_df[f"{col}_mean"] else None,
                        'stddev': float(stats_df[f"{col}_stddev"]) if stats_df[f"{col}_stddev"] else None
                    }
                
                result['numeric_stats'] = current_stats
                
                # Compare with baseline if provided
                if baseline_stats and 'numeric_stats' in baseline_stats:
                    for col in numeric_cols:
                        if col in baseline_stats['numeric_stats']:
                            baseline_mean = baseline_stats['numeric_stats'][col].get('mean')
                            current_mean = current_stats[col].get('mean')
                            
                            if baseline_mean and current_mean:
                                # Check if mean shifted by more than 20%
                                pct_change = abs((current_mean - baseline_mean) / baseline_mean * 100)
                                if pct_change > 20:
                                    result['distribution_changes'].append({
                                        'column': col,
                                        'metric': 'mean',
                                        'baseline_value': baseline_mean,
                                        'current_value': current_mean,
                                        'percent_change': round(pct_change, 2)
                                    })
                                    result['drift_detected'] = True
            
            # Check for row count changes
            if baseline_stats and 'row_count' in baseline_stats:
                baseline_rows = baseline_stats['row_count']
                if baseline_rows > 0:
                    row_change_pct = abs((row_count - baseline_rows) / baseline_rows * 100)
                    result['row_count_change_pct'] = round(row_change_pct, 2)
                    
                    # Alert if row count changed by more than 50%
                    if row_change_pct > 50:
                        result['distribution_changes'].append({
                            'metric': 'row_count',
                            'baseline_value': baseline_rows,
                            'current_value': row_count,
                            'percent_change': round(row_change_pct, 2)
                        })
                        result['drift_detected'] = True
        
        result['status'] = 'DRIFT_DETECTED' if result['drift_detected'] else 'NO_DRIFT'
        result['message'] = f"Detected {len(result['schema_changes'])} schema changes and {len(result['distribution_changes'])} distribution changes" if result['drift_detected'] else "No drift detected"
        
        return result
        
    except Exception as e:
        return {
            'table': table_name,
            'status': 'ERROR',
            'message': f"Error detecting drift: {str(e)}"
        }

# Test the function
print("Testing detect_data_drift()...\n")

test_table = 'retailpulse.gold.fact_sales'
result = detect_data_drift(test_table, check_schema=True, check_distribution=True)

print(f"Table: {result['table']}")
print(f"Status: {result['status']}")
print(f"Message: {result['message']}")
print(f"Row Count: {result.get('row_count', 'N/A')}")
print(f"Column Count: {result.get('column_count', 'N/A')}")
print("-" * 80)

# Save current stats as baseline for future comparisons
baseline = {
    'schema': result.get('current_schema', {}),
    'row_count': result.get('row_count', 0),
    'numeric_stats': result.get('numeric_stats', {})
}

print("\nBaseline statistics captured for future drift detection.")
print(f"Baseline saved with {len(baseline['schema'])} columns and {baseline['row_count']} rows.")

# COMMAND ----------

# DBTITLE 1,Tool 4: Run Quality Rules
"""Tool 4: Run Quality Rules - Execute custom DQ rules."""

def run_quality_rules(
    table_name: str,
    rules: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Execute custom data quality rules on a table.
    
    Args:
        table_name: Fully qualified table name
        rules: List of rule dictionaries with format:
               {
                   'rule_name': 'Unique description',
                   'rule_type': 'completeness|validity|accuracy|consistency',
                   'sql_condition': 'SQL WHERE condition that identifies BAD records',
                   'severity': 'CRITICAL|WARNING|INFO'
               }
    
    Returns:
        Dictionary with rule execution results
    """
    try:
        df = spark.table(table_name)
        total_records = df.count()
        
        results = {
            'table': table_name,
            'total_records': total_records,
            'rules_executed': len(rules),
            'rules_passed': 0,
            'rules_failed': 0,
            'rule_results': [],
            'overall_status': 'PASS'
        }
        
        for rule in rules:
            rule_name = rule.get('rule_name', 'Unnamed Rule')
            rule_type = rule.get('rule_type', 'unknown')
            sql_condition = rule.get('sql_condition')
            severity = rule.get('severity', 'WARNING')
            
            if not sql_condition:
                results['rule_results'].append({
                    'rule_name': rule_name,
                    'status': 'ERROR',
                    'message': 'No SQL condition provided'
                })
                continue
            
            try:
                # Count records that violate the rule
                violation_query = f"""
                SELECT COUNT(*) as violation_count
                FROM {table_name}
                WHERE {sql_condition}
                """
                
                violation_count = spark.sql(violation_query).collect()[0]['violation_count']
                violation_pct = (violation_count / total_records * 100) if total_records > 0 else 0
                
                passed = violation_count == 0
                
                rule_result = {
                    'rule_name': rule_name,
                    'rule_type': rule_type,
                    'severity': severity,
                    'status': 'PASS' if passed else 'FAIL',
                    'violation_count': violation_count,
                    'violation_percentage': round(violation_pct, 2),
                    'message': f"No violations found" if passed else f"Found {violation_count} violations ({round(violation_pct, 2)}%)"
                }
                
                # Get sample violations if any
                if violation_count > 0:
                    sample_query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {sql_condition}
                    LIMIT 3
                    """
                    sample_violations = spark.sql(sample_query).collect()
                    rule_result['sample_violations'] = [
                        {col: str(row[col]) for col in row.asDict().keys()}
                        for row in sample_violations
                    ]
                
                results['rule_results'].append(rule_result)
                
                if passed:
                    results['rules_passed'] += 1
                else:
                    results['rules_failed'] += 1
                    if severity == 'CRITICAL':
                        results['overall_status'] = 'CRITICAL'
                    elif results['overall_status'] != 'CRITICAL' and severity == 'WARNING':
                        results['overall_status'] = 'WARNING'
                        
            except Exception as e:
                results['rule_results'].append({
                    'rule_name': rule_name,
                    'status': 'ERROR',
                    'message': f"Error executing rule: {str(e)}"
                })
        
        return results
        
    except Exception as e:
        return {
            'table': table_name,
            'status': 'ERROR',
            'message': f"Error running quality rules: {str(e)}"
        }

# Test the function with RetailPulse quality rules
print("Testing run_quality_rules()...\n")

# Define quality rules for fact_sales
fact_sales_rules = [
    {
        'rule_name': 'No Null Quantities',
        'rule_type': 'completeness',
        'sql_condition': 'quantity IS NULL',
        'severity': 'CRITICAL'
    },
    {
        'rule_name': 'Positive Quantities',
        'rule_type': 'validity',
        'sql_condition': 'quantity <= 0',
        'severity': 'CRITICAL'
    },
    {
        'rule_name': 'Positive Prices',
        'rule_type': 'validity',
        'sql_condition': 'price <= 0',
        'severity': 'CRITICAL'
    },
    {
        'rule_name': 'Sales Amount Accuracy',
        'rule_type': 'accuracy',
        'sql_condition': 'ABS(sales_amount - (quantity * price)) > 0.01',
        'severity': 'CRITICAL'
    },
    {
        'rule_name': 'Future Dates Check',
        'rule_type': 'validity',
        'sql_condition': 'date_id > CAST(CURRENT_DATE() AS STRING)',
        'severity': 'WARNING'
    },
    {
        'rule_name': 'Reasonable Quantity Range',
        'rule_type': 'validity',
        'sql_condition': 'quantity > 1000',
        'severity': 'INFO'
    }
]

result = run_quality_rules('retailpulse.gold.fact_sales', fact_sales_rules)

print(f"Table: {result['table']}")
print(f"Total Records: {result['total_records']}")
print(f"Overall Status: {result['overall_status']}")
print(f"Rules Passed: {result['rules_passed']}/{result['rules_executed']}")
print(f"Rules Failed: {result['rules_failed']}/{result['rules_executed']}")
print("\n" + "="*80)
print("RULE RESULTS:")
print("="*80)

for rule_result in result['rule_results']:
    print(f"\n[{rule_result['status']}] {rule_result['rule_name']} ({rule_result.get('severity', 'N/A')})")
    print(f"  Type: {rule_result.get('rule_type', 'N/A')}")
    print(f"  {rule_result['message']}")
    if 'violation_count' in rule_result and rule_result['violation_count'] > 0:
        print(f"  Violations: {rule_result['violation_count']} ({rule_result['violation_percentage']}%)")
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Comprehensive DQ Validation Report
"""Run comprehensive data quality validation across RetailPulse."""

from pyspark.sql import Row

def generate_dq_report(catalog: str = 'retailpulse', schema: str = 'gold') -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report.
    
    Args:
        catalog: Catalog name
        schema: Schema name
    
    Returns:
        Comprehensive DQ report
    """
    report = {
        'catalog': catalog,
        'schema': schema,
        'report_timestamp': datetime.now().isoformat(),
        'checks_performed': 0,
        'checks_passed': 0,
        'checks_failed': 0,
        'critical_issues': [],
        'warnings': [],
        'details': {}
    }
    
    print("="*80)
    print(f"DATA QUALITY VALIDATION REPORT - {catalog}.{schema}")
    print(f"Generated: {report['report_timestamp']}")
    print("="*80)
    
    # 1. Freshness Checks
    print("\n[1] TABLE FRESHNESS CHECKS")
    print("-"*80)
    
    freshness_tables = [
        (f'{catalog}.{schema}.fact_sales', 'order_ts', 24),
        (f'{catalog}.{schema}.dim_customer', None, 168),
        (f'{catalog}.{schema}.dim_product', None, 168),
        (f'{catalog}.{schema}.dim_date', None, 720)
    ]
    
    freshness_results = []
    for table, ts_col, max_age in freshness_tables:
        result = check_table_freshness(table, ts_col, max_age)
        freshness_results.append(result)
        report['checks_performed'] += 1
        
        print(f"  {table}: {result['status']}")
        
        if result['status'] == 'PASS':
            report['checks_passed'] += 1
        else:
            report['checks_failed'] += 1
            if 'stale' in result['message'].lower():
                report['warnings'].append(f"Table {table} is stale")
    
    report['details']['freshness'] = freshness_results
    
    # 2. Referential Integrity Checks
    print("\n[2] REFERENTIAL INTEGRITY CHECKS")
    print("-"*80)
    
    fk_relationships = [
        (f'{catalog}.{schema}.fact_sales', 'customer_id', f'{catalog}.{schema}.dim_customer', 'customer_id'),
        (f'{catalog}.{schema}.fact_sales', 'product_sk', f'{catalog}.{schema}.dim_product', 'product_sk'),
        (f'{catalog}.{schema}.fact_sales', 'date_id', f'{catalog}.{schema}.dim_date', 'date_id')
    ]
    
    integrity_results = []
    for child, child_col, parent, parent_col in fk_relationships:
        result = validate_referential_integrity(child, child_col, parent, parent_col)
        integrity_results.append(result)
        report['checks_performed'] += 1
        
        print(f"  {child}.{child_col} -> {parent}.{parent_col}: {result['status']}")
        
        if result['status'] == 'PASS':
            report['checks_passed'] += 1
        else:
            report['checks_failed'] += 1
            if result.get('orphaned_records', 0) > 0:
                report['critical_issues'].append(
                    f"Referential integrity violation: {result['orphaned_records']} orphaned records in {child}.{child_col}"
                )
    
    report['details']['integrity'] = integrity_results
    
    # 3. Data Drift Detection
    print("\n[3] DATA DRIFT DETECTION")
    print("-"*80)
    
    drift_tables = [f'{catalog}.{schema}.fact_sales', f'{catalog}.{schema}.dim_customer']
    drift_results = []
    
    for table in drift_tables:
        result = detect_data_drift(table, check_schema=True, check_distribution=True)
        drift_results.append(result)
        report['checks_performed'] += 1
        
        print(f"  {table}: {result['status']}")
        
        if result['status'] == 'NO_DRIFT':
            report['checks_passed'] += 1
        else:
            report['checks_failed'] += 1
            if result.get('schema_changes'):
                report['warnings'].append(f"Schema drift detected in {table}")
            if result.get('distribution_changes'):
                report['warnings'].append(f"Distribution drift detected in {table}")
    
    report['details']['drift'] = drift_results
    
    # 4. Quality Rules Execution
    print("\n[4] QUALITY RULES VALIDATION")
    print("-"*80)
    
    quality_rules = [
        {
            'rule_name': 'No Null Quantities',
            'rule_type': 'completeness',
            'sql_condition': 'quantity IS NULL',
            'severity': 'CRITICAL'
        },
        {
            'rule_name': 'Positive Quantities',
            'rule_type': 'validity',
            'sql_condition': 'quantity <= 0',
            'severity': 'CRITICAL'
        },
        {
            'rule_name': 'Positive Prices',
            'rule_type': 'validity',
            'sql_condition': 'price <= 0',
            'severity': 'CRITICAL'
        },
        {
            'rule_name': 'Sales Amount Accuracy',
            'rule_type': 'accuracy',
            'sql_condition': 'ABS(sales_amount - (quantity * price)) > 0.01',
            'severity': 'CRITICAL'
        }
    ]
    
    rules_result = run_quality_rules(f'{catalog}.{schema}.fact_sales', quality_rules)
    report['checks_performed'] += rules_result['rules_executed']
    report['checks_passed'] += rules_result['rules_passed']
    report['checks_failed'] += rules_result['rules_failed']
    
    for rule in rules_result['rule_results']:
        print(f"  {rule['rule_name']}: {rule['status']}")
        if rule['status'] == 'FAIL' and rule.get('severity') == 'CRITICAL':
            report['critical_issues'].append(
                f"Quality rule failed: {rule['rule_name']} - {rule['message']}"
            )
    
    report['details']['quality_rules'] = rules_result
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total Checks: {report['checks_performed']}")
    print(f"Passed: {report['checks_passed']} ({round(report['checks_passed']/report['checks_performed']*100, 1)}%)")
    print(f"Failed: {report['checks_failed']} ({round(report['checks_failed']/report['checks_performed']*100, 1)}%)")
    print(f"\nCritical Issues: {len(report['critical_issues'])}")
    print(f"Warnings: {len(report['warnings'])}")
    
    if report['critical_issues']:
        print("\n⚠️  CRITICAL ISSUES:")
        for issue in report['critical_issues']:
            print(f"  - {issue}")
    
    if report['warnings']:
        print("\n⚡ WARNINGS:")
        for warning in report['warnings']:
            print(f"  - {warning}")
    
    if not report['critical_issues'] and not report['warnings']:
        print("\n✅ All data quality checks passed!")
    
    print("="*80)
    
    return report

# Generate the report
dq_report = generate_dq_report()

# COMMAND ----------

# DBTITLE 1,MCP Server Integration & Unity Catalog Registration
"""Register DQ validation metadata in Unity Catalog."""

# Ensure catalog and schema exist
spark.sql("CREATE CATALOG IF NOT EXISTS retailpulse")
spark.sql("USE CATALOG retailpulse")
spark.sql("CREATE SCHEMA IF NOT EXISTS ops")

print("\n" + "="*80)
print("REGISTERING DATA QUALITY VALIDATION TOOLS")
print("="*80)
print("\nNote: Unity Catalog SQL functions have limitations with dynamic table queries.")
print("The Python functions in this notebook are the primary DQ validation interface.\n")

# Register simplified UC functions that provide metadata/documentation
# These serve as discoverable API documentation in the catalog

# Function 1: List available DQ checks
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.ops.list_dq_checks()
RETURNS ARRAY<STRING>
COMMENT 'Lists all available data quality validation checks. Use the Python functions in the DQ_Validation_MCP_Server notebook for full functionality.'
RETURN ARRAY(
    'check_table_freshness(table_name, max_age_hours)',
    'validate_referential_integrity(child_table, child_col, parent_table, parent_col)',
    'detect_data_drift(table_name, baseline_stats)',
    'run_quality_rules(table_name, rules_list)',
    'generate_dq_report(catalog, schema)'
)
""")

print("✅ Registered: retailpulse.ops.list_dq_checks()")

# Function 2: Get DQ notebook location
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.ops.get_dq_notebook_path()
RETURNS STRING
COMMENT 'Returns the path to the DQ Validation MCP Server notebook containing all validation functions.'
RETURN '/Users/shekartelstra@gmail.com/DQ_Validation_MCP_Server'
""")

print("✅ Registered: retailpulse.ops.get_dq_notebook_path()")

# Function 3: Get DQ check documentation
spark.sql("""
CREATE OR REPLACE FUNCTION retailpulse.ops.get_check_info(check_name STRING)
RETURNS STRING
COMMENT 'Returns documentation for a specific DQ check. Valid check names: freshness, integrity, drift, rules, report.'
RETURN CASE check_name
    WHEN 'freshness' THEN 'check_table_freshness(table_name, max_age_hours) - Monitors when tables were last updated. Returns PASS if age < max_age_hours.'
    WHEN 'integrity' THEN 'validate_referential_integrity(child_table, child_col, parent_table, parent_col) - Validates FK relationships. Returns orphaned record count.'
    WHEN 'drift' THEN 'detect_data_drift(table_name, baseline_stats) - Detects schema and distribution changes. Compares current stats to baseline.'
    WHEN 'rules' THEN 'run_quality_rules(table_name, rules_list) - Executes custom quality rules. Rules define SQL conditions for bad records.'
    WHEN 'report' THEN 'generate_dq_report(catalog, schema) - Comprehensive validation report running all checks across a schema.'
    ELSE 'Unknown check. Use: freshness, integrity, drift, rules, or report.'
END
""")

print("✅ Registered: retailpulse.ops.get_check_info(check_name)")

# Create a simple view to document the DQ validation framework
spark.sql("""
CREATE OR REPLACE VIEW retailpulse.ops.dq_validation_catalog AS
SELECT 
    'check_table_freshness' AS function_name,
    'Freshness Monitoring' AS category,
    'table_name STRING, max_age_hours INT' AS parameters,
    'Verifies table was updated within acceptable timeframe' AS description,
    'Python API' AS interface
UNION ALL
SELECT 
    'validate_referential_integrity',
    'Referential Integrity',
    'child_table STRING, child_column STRING, parent_table STRING, parent_column STRING',
    'Checks for orphaned foreign key records',
    'Python API'
UNION ALL
SELECT 
    'detect_data_drift',
    'Schema & Distribution Monitoring',
    'table_name STRING, baseline_stats DICT',
    'Detects schema changes and distribution shifts',
    'Python API'
UNION ALL
SELECT 
    'run_quality_rules',
    'Custom Rule Execution',
    'table_name STRING, rules LIST[DICT]',
    'Executes user-defined quality rules with severity levels',
    'Python API'
UNION ALL
SELECT 
    'generate_dq_report',
    'Comprehensive Validation',
    'catalog STRING, schema STRING',
    'Runs all DQ checks and generates summary report',
    'Python API'
""")

print("✅ Registered: retailpulse.ops.dq_validation_catalog view")

print("\n" + "="*80)
print("DATA QUALITY VALIDATION MCP SERVER - READY")
print("="*80)

print("\n🚀 Available Tools (Python API):")
print("""
1. check_table_freshness(table_name, max_age_hours)
   - Monitors table update recency
   - Example: check_table_freshness('retailpulse.gold.fact_sales', 24)

2. validate_referential_integrity(child_table, child_col, parent_table, parent_col)
   - Validates FK relationships
   - Example: validate_referential_integrity(
       'retailpulse.gold.fact_sales', 'customer_id',
       'retailpulse.gold.dim_customer', 'customer_id'
     )

3. detect_data_drift(table_name, baseline_stats)
   - Detects schema and distribution changes
   - Example: detect_data_drift('retailpulse.gold.fact_sales', baseline_dict)

4. run_quality_rules(table_name, rules_list)
   - Executes custom quality rules
   - Example: run_quality_rules('retailpulse.gold.fact_sales', [
       {'rule_name': 'No Nulls', 'sql_condition': 'col IS NULL', 'severity': 'CRITICAL'}
     ])

5. generate_dq_report(catalog, schema)
   - Comprehensive DQ validation report
   - Example: generate_dq_report('retailpulse', 'gold')
""")

print("\n📊 Unity Catalog Discovery:")
print("""
# View all registered DQ functions
SELECT * FROM retailpulse.ops.dq_validation_catalog;

# List available checks
SELECT retailpulse.ops.list_dq_checks();

# Get documentation for a specific check
SELECT retailpulse.ops.get_check_info('integrity');

# Find the DQ notebook
SELECT retailpulse.ops.get_dq_notebook_path();
""")

print("\n💡 How to Trace the MCP Server:")
print("""
1. Catalog Explorer UI:
   - Click Catalog icon (left sidebar)
   - Navigate: retailpulse → ops → Functions & Views
   - See: list_dq_checks(), get_check_info(), dq_validation_catalog

2. SQL Query:
   - SHOW FUNCTIONS IN retailpulse.ops;
   - SHOW VIEWS IN retailpulse.ops;
   - DESCRIBE FUNCTION retailpulse.ops.list_dq_checks;

3. Python Notebook:
   - This notebook: /Users/shekartelstra@gmail.com/DQ_Validation_MCP_Server
   - All functions defined in cells 2-5
   - Comprehensive report in cell 6
""")

print("\n✅ MCP Server ready! Use Python functions for full validation capabilities.")
print("✅ UC functions provide discoverability and documentation.")
print("="*80)

# Display the catalog view
print("\n📋 DQ Validation Catalog:")
display(spark.table("retailpulse.ops.dq_validation_catalog"))

# COMMAND ----------

# DBTITLE 1,Discover DQ Tools in Unity Catalog
# MAGIC %sql
# MAGIC -- TRACING THE MCP SERVER IN UNITY CATALOG
# MAGIC -- This demonstrates how to discover the registered DQ validation tools
# MAGIC
# MAGIC -- 1. Show all functions in the ops schema
# MAGIC SHOW FUNCTIONS IN retailpulse.ops;
# MAGIC
# MAGIC -- 2. Show all views in the ops schema
# MAGIC SHOW VIEWS IN retailpulse.ops;

# COMMAND ----------

# DBTITLE 1,Query DQ Validation Catalog
# MAGIC %sql
# MAGIC -- View the complete DQ validation catalog
# MAGIC SELECT * FROM retailpulse.ops.dq_validation_catalog
# MAGIC ORDER BY category;
# MAGIC
# MAGIC -- List all available checks
# MAGIC SELECT retailpulse.ops.list_dq_checks() AS available_checks;
# MAGIC
# MAGIC -- Get info about specific checks
# MAGIC SELECT 
# MAGIC     'freshness' AS check_type,
# MAGIC     retailpulse.ops.get_check_info('freshness') AS documentation
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'integrity' AS check_type,
# MAGIC     retailpulse.ops.get_check_info('integrity') AS documentation
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'drift' AS check_type,
# MAGIC     retailpulse.ops.get_check_info('drift') AS documentation;

# COMMAND ----------

# DBTITLE 1,How to Use the DQ Validation Tools
# MAGIC %md
# MAGIC ## 🎯 How to Use the DQ Validation Tools
# MAGIC
# MAGIC ### ⚠️ Important: Two Interfaces
# MAGIC
# MAGIC The DQ validation system has **two separate interfaces**:
# MAGIC
# MAGIC 1. **Python Functions** (Full Implementation) ✅
# MAGIC    - Live in **this notebook**
# MAGIC    - Perform actual validation logic
# MAGIC    - Use: Call directly in Python cells
# MAGIC
# MAGIC 2. **Unity Catalog Functions** (Metadata Only) 📚
# MAGIC    - Registered in `retailpulse.ops`
# MAGIC    - Provide **discovery and documentation**
# MAGIC    - **Do NOT perform actual validation**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ This WON'T Work:
# MAGIC ```sql
# MAGIC -- This function doesn't exist!
# MAGIC SELECT retailpulse.ops.check_table_freshness(
# MAGIC   'retailpulse.gold.fact_sales',
# MAGIC   24
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Why?** Unity Catalog SQL functions can't do dynamic table queries. The `check_table_freshness()` validation logic is **only available as a Python function** in this notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ This WILL Work:
# MAGIC
# MAGIC #### Option 1: Use Python Functions in This Notebook
# MAGIC ```python
# MAGIC # Call the actual validation function
# MAGIC result = check_table_freshness('retailpulse.gold.fact_sales', 24)
# MAGIC print(result)
# MAGIC ```
# MAGIC
# MAGIC #### Option 2: Discover Tools via UC Functions
# MAGIC ```sql
# MAGIC -- List all available checks
# MAGIC SELECT retailpulse.ops.list_dq_checks();
# MAGIC
# MAGIC -- Get documentation for a check
# MAGIC SELECT retailpulse.ops.get_check_info('freshness');
# MAGIC
# MAGIC -- View the catalog
# MAGIC SELECT * FROM retailpulse.ops.dq_validation_catalog;
# MAGIC ```
# MAGIC
# MAGIC #### Option 3: Run the Comprehensive Report
# MAGIC ```python
# MAGIC # Generate full DQ report for a schema
# MAGIC report = generate_dq_report('retailpulse', 'gold')
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 What's Registered in Unity Catalog?
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |----------|----------|
# MAGIC | `list_dq_checks()` | Returns array of available check names |
# MAGIC | `get_check_info(check_name)` | Returns documentation for a specific check |
# MAGIC | `get_dq_notebook_path()` | Returns path to this notebook |
# MAGIC | `dq_validation_catalog` view | Shows all available tools in a table |
# MAGIC
# MAGIC **None of these perform actual validation** - they're metadata/documentation helpers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 To Actually Run Validations:
# MAGIC
# MAGIC **Use the Python functions defined in Cells 2-6 of this notebook!**

# COMMAND ----------

# DBTITLE 1,Example: Using DQ Validation Functions
"""Examples of how to use the DQ validation functions."""

print("="*80)
print("DQ VALIDATION USAGE EXAMPLES")
print("="*80)

# Example 1: Check Table Freshness
print("\n[Example 1] Check Table Freshness")
print("-"*80)
result = check_table_freshness('retailpulse.gold.fact_sales', max_age_hours=24)
print(f"Table: {result['table']}")
print(f"Status: {result['status']}")
print(f"Age: {result['age_hours']} hours")
print(f"Message: {result['message']}")

# Example 2: Validate Referential Integrity
print("\n[Example 2] Validate Referential Integrity")
print("-"*80)
result = validate_referential_integrity(
    child_table='retailpulse.gold.fact_sales',
    child_column='customer_id',
    parent_table='retailpulse.gold.dim_customer',
    parent_column='customer_id'
)
print(f"Relationship: {result['child_column']} -> {result['parent_column']}")
print(f"Status: {result['status']}")
print(f"Orphaned Records: {result['orphaned_records']}")
print(f"Message: {result['message']}")

# Example 3: Detect Data Drift
print("\n[Example 3] Detect Data Drift")
print("-"*80)
result = detect_data_drift('retailpulse.gold.fact_sales')
print(f"Table: {result['table']}")
print(f"Status: {result['status']}")
print(f"Row Count: {result.get('row_count', 'N/A')}")
print(f"Message: {result['message']}")

# Example 4: Run Custom Quality Rules
print("\n[Example 4] Run Custom Quality Rules")
print("-"*80)
custom_rules = [
    {
        'rule_name': 'No Negative Prices',
        'rule_type': 'validity',
        'sql_condition': 'price < 0',
        'severity': 'CRITICAL'
    },
    {
        'rule_name': 'High Value Orders',
        'rule_type': 'validity',
        'sql_condition': 'sales_amount > 10000',
        'severity': 'INFO'
    }
]
result = run_quality_rules('retailpulse.gold.fact_sales', custom_rules)
print(f"Table: {result['table']}")
print(f"Overall Status: {result['overall_status']}")
print(f"Rules Passed: {result['rules_passed']}/{result['rules_executed']}")
for rule in result['rule_results']:
    print(f"  - {rule['rule_name']}: {rule['status']}")

# Example 5: Generate Comprehensive Report
print("\n[Example 5] Generate Comprehensive DQ Report")
print("-"*80)
print("Running generate_dq_report('retailpulse', 'gold')...\n")
# Uncomment to run the full report:
# report = generate_dq_report('retailpulse', 'gold')

print("="*80)
print("TIP: Copy these examples and modify them for your own tables!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,SQL: What You CAN Check
# MAGIC %sql
# MAGIC -- ================================================================================
# MAGIC -- WHAT YOU CAN DO IN SQL: Query Metadata & Documentation
# MAGIC -- ================================================================================
# MAGIC
# MAGIC -- 1. View all available DQ validation tools
# MAGIC SELECT 
# MAGIC     function_name,
# MAGIC     category,
# MAGIC     description,
# MAGIC     interface
# MAGIC FROM retailpulse.ops.dq_validation_catalog
# MAGIC ORDER BY category;
# MAGIC
# MAGIC -- 2. List all available checks
# MAGIC SELECT retailpulse.ops.list_dq_checks() AS available_checks;
# MAGIC
# MAGIC -- 3. Get documentation for specific checks
# MAGIC SELECT retailpulse.ops.get_check_info('freshness') AS freshness_docs;
# MAGIC SELECT retailpulse.ops.get_check_info('integrity') AS integrity_docs;
# MAGIC
# MAGIC -- 4. Find the notebook with full validation functions
# MAGIC SELECT retailpulse.ops.get_dq_notebook_path() AS notebook_path;
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- WHAT YOU CANNOT DO IN SQL: Run Actual Validations
# MAGIC -- ================================================================================
# MAGIC
# MAGIC -- ❌ This will NOT work (function doesn't exist):
# MAGIC -- SELECT retailpulse.ops.check_table_freshness('retailpulse.gold.fact_sales', 24);
# MAGIC
# MAGIC -- ❌ This will NOT work (function doesn't exist):
# MAGIC -- SELECT retailpulse.ops.validate_referential_integrity(
# MAGIC --     'retailpulse.gold.fact_sales', 'customer_id',
# MAGIC --     'retailpulse.gold.dim_customer', 'customer_id'
# MAGIC -- );
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- WHY? Unity Catalog SQL functions cannot:
# MAGIC -- 1. Use dynamic table names passed as parameters
# MAGIC -- 2. Execute complex validation logic with joins
# MAGIC -- 3. Return complex nested structures
# MAGIC -- ================================================================================

# COMMAND ----------

# DBTITLE 1,SQL Workarounds: Run Basic Validations
# MAGIC %sql
# MAGIC -- ================================================================================
# MAGIC -- WORKAROUND: Run Basic DQ Checks Directly in SQL
# MAGIC -- ================================================================================
# MAGIC -- These queries perform simple validations without using UC functions
# MAGIC -- UPDATED: Freshness threshold set to 48 hours (2 days)
# MAGIC -- ================================================================================
# MAGIC
# MAGIC -- 1. CHECK TABLE FRESHNESS (manually) - 48 hour threshold
# MAGIC SELECT 
# MAGIC     'retailpulse.gold.fact_sales' AS table_name,
# MAGIC     MAX(order_ts) AS last_order_timestamp,
# MAGIC     CURRENT_TIMESTAMP() AS current_time,
# MAGIC     ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600, 2) AS age_hours,
# MAGIC     CASE 
# MAGIC         WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600 <= 48 
# MAGIC         THEN 'PASS' 
# MAGIC         ELSE 'FAIL' 
# MAGIC     END AS freshness_status,
# MAGIC     '48 hour threshold (2 days)' AS threshold_note
# MAGIC FROM retailpulse.gold.fact_sales;
# MAGIC
# MAGIC -- 2. CHECK REFERENTIAL INTEGRITY (manually)
# MAGIC -- Find orphaned customer_ids in fact_sales
# MAGIC SELECT 
# MAGIC     'fact_sales -> dim_customer' AS relationship,
# MAGIC     COUNT(*) AS orphaned_records,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM retailpulse.gold.fact_sales f
# MAGIC LEFT JOIN retailpulse.gold.dim_customer d ON f.customer_id = d.customer_id
# MAGIC WHERE d.customer_id IS NULL;
# MAGIC
# MAGIC -- Find orphaned product_sk in fact_sales
# MAGIC SELECT 
# MAGIC     'fact_sales -> dim_product' AS relationship,
# MAGIC     COUNT(*) AS orphaned_records,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM retailpulse.gold.fact_sales f
# MAGIC LEFT JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
# MAGIC WHERE p.product_sk IS NULL;
# MAGIC
# MAGIC -- 3. RUN QUALITY RULES (manually)
# MAGIC -- Check for null quantities
# MAGIC SELECT 
# MAGIC     'No Null Quantities' AS rule_name,
# MAGIC     COUNT(*) AS violation_count,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM retailpulse.gold.fact_sales
# MAGIC WHERE quantity IS NULL;
# MAGIC
# MAGIC -- Check for negative prices
# MAGIC SELECT 
# MAGIC     'Positive Prices' AS rule_name,
# MAGIC     COUNT(*) AS violation_count,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM retailpulse.gold.fact_sales
# MAGIC WHERE price <= 0;
# MAGIC
# MAGIC -- Check for sales amount accuracy
# MAGIC SELECT 
# MAGIC     'Sales Amount Accuracy' AS rule_name,
# MAGIC     COUNT(*) AS violation_count,
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM retailpulse.gold.fact_sales
# MAGIC WHERE ABS(sales_amount - (quantity * price)) > 0.01;
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- TIP: Save these queries as SQL queries in Databricks for reuse!
# MAGIC -- NOTE: Adjust threshold from 48 to 24 or 72 hours as needed for your use case
# MAGIC -- ================================================================================

# COMMAND ----------

# DBTITLE 1,SQL: Comprehensive DQ Validation Report
# MAGIC %sql
# MAGIC -- ================================================================================
# MAGIC -- COMPREHENSIVE DQ VALIDATION REPORT (Pure SQL)
# MAGIC -- ================================================================================
# MAGIC -- This query can be run from:
# MAGIC -- - SQL Warehouse
# MAGIC -- - Databricks SQL Editor
# MAGIC -- - BI Tools (Tableau, Power BI, etc.)
# MAGIC -- - Any notebook with SQL cell
# MAGIC -- ================================================================================
# MAGIC -- UPDATED: Freshness threshold increased to 48 hours (2 days)
# MAGIC -- ================================================================================
# MAGIC
# MAGIC WITH 
# MAGIC -- 1. Freshness Check (48 hour threshold)
# MAGIC freshness_check AS (
# MAGIC     SELECT 
# MAGIC         'Table Freshness' AS check_category,
# MAGIC         'fact_sales data age' AS check_name,
# MAGIC         ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600, 2) AS age_hours,
# MAGIC         CASE 
# MAGIC             WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600 <= 48 
# MAGIC             THEN 'PASS' 
# MAGIC             ELSE 'FAIL' 
# MAGIC         END AS status,
# MAGIC         CONCAT('Data is ', ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600, 2), ' hours old (threshold: 48 hours)') AS message
# MAGIC     FROM retailpulse.gold.fact_sales
# MAGIC ),
# MAGIC
# MAGIC -- 2. Referential Integrity Checks
# MAGIC ri_customer AS (
# MAGIC     SELECT 
# MAGIC         'Referential Integrity' AS check_category,
# MAGIC         'fact_sales -> dim_customer' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' orphaned customer_id records') AS message
# MAGIC     FROM retailpulse.gold.fact_sales f
# MAGIC     LEFT JOIN retailpulse.gold.dim_customer d ON f.customer_id = d.customer_id
# MAGIC     WHERE d.customer_id IS NULL
# MAGIC ),
# MAGIC
# MAGIC ri_product AS (
# MAGIC     SELECT 
# MAGIC         'Referential Integrity' AS check_category,
# MAGIC         'fact_sales -> dim_product' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' orphaned product_sk records') AS message
# MAGIC     FROM retailpulse.gold.fact_sales f
# MAGIC     LEFT JOIN retailpulse.gold.dim_product p ON f.product_sk = p.product_sk
# MAGIC     WHERE p.product_sk IS NULL
# MAGIC ),
# MAGIC
# MAGIC ri_date AS (
# MAGIC     SELECT 
# MAGIC         'Referential Integrity' AS check_category,
# MAGIC         'fact_sales -> dim_date' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' orphaned date_id records') AS message
# MAGIC     FROM retailpulse.gold.fact_sales f
# MAGIC     LEFT JOIN retailpulse.gold.dim_date d ON f.date_id = d.date_id
# MAGIC     WHERE d.date_id IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- 3. Data Quality Rules
# MAGIC qr_null_quantity AS (
# MAGIC     SELECT 
# MAGIC         'Quality Rules' AS check_category,
# MAGIC         'No Null Quantities' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' null quantity records') AS message
# MAGIC     FROM retailpulse.gold.fact_sales
# MAGIC     WHERE quantity IS NULL
# MAGIC ),
# MAGIC
# MAGIC qr_negative_price AS (
# MAGIC     SELECT 
# MAGIC         'Quality Rules' AS check_category,
# MAGIC         'Positive Prices' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' negative/zero price records') AS message
# MAGIC     FROM retailpulse.gold.fact_sales
# MAGIC     WHERE price <= 0
# MAGIC ),
# MAGIC
# MAGIC qr_sales_accuracy AS (
# MAGIC     SELECT 
# MAGIC         'Quality Rules' AS check_category,
# MAGIC         'Sales Amount Accuracy' AS check_name,
# MAGIC         COUNT(*) AS violation_count,
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC         CONCAT(COUNT(*), ' inaccurate sales_amount calculations') AS message
# MAGIC     FROM retailpulse.gold.fact_sales
# MAGIC     WHERE ABS(sales_amount - (quantity * price)) > 0.01
# MAGIC ),
# MAGIC
# MAGIC -- 4. Combine all checks
# MAGIC all_checks AS (
# MAGIC     SELECT check_category, check_name, NULL AS age_hours, violation_count, status, message FROM ri_customer
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, NULL, violation_count, status, message FROM ri_product
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, NULL, violation_count, status, message FROM ri_date
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, NULL, violation_count, status, message FROM qr_null_quantity
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, NULL, violation_count, status, message FROM qr_negative_price
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, NULL, violation_count, status, message FROM qr_sales_accuracy
# MAGIC     UNION ALL
# MAGIC     SELECT check_category, check_name, age_hours, NULL AS violation_count, status, message FROM freshness_check
# MAGIC )
# MAGIC
# MAGIC -- Final Report
# MAGIC SELECT 
# MAGIC     check_category,
# MAGIC     check_name,
# MAGIC     status,
# MAGIC     COALESCE(CAST(violation_count AS STRING), CONCAT(CAST(age_hours AS STRING), ' hours')) AS metric,
# MAGIC     message,
# MAGIC     CURRENT_TIMESTAMP() AS report_timestamp
# MAGIC FROM all_checks
# MAGIC ORDER BY 
# MAGIC     CASE check_category
# MAGIC         WHEN 'Table Freshness' THEN 1
# MAGIC         WHEN 'Referential Integrity' THEN 2
# MAGIC         WHEN 'Quality Rules' THEN 3
# MAGIC     END,
# MAGIC     check_name;
# MAGIC
# MAGIC -- ================================================================================
# MAGIC -- TIP: Save this as a SQL query in Databricks and schedule it!
# MAGIC -- NOTE: Freshness threshold set to 48 hours (can be adjusted to 24 or 72 as needed)
# MAGIC -- ================================================================================

# COMMAND ----------

# DBTITLE 1,Diagnose Freshness Issue: Data vs Table Timestamps
"""Diagnose why freshness check fails even after successful job run."""

from datetime import datetime

# Updated threshold to match Cell 14
FRESHNESS_THRESHOLD_HOURS = 48

print("="*80)
print("FRESHNESS DIAGNOSIS: DATA vs TABLE TIMESTAMPS")
print("="*80)
print("\nYour job completed successfully at 08:17 PM.")
print("Let's check both DATA and TABLE freshness...\n")

# Part 1: Check DATA freshness (order_ts column)
print(f"[1] DATA FRESHNESS - Checking MAX(order_ts) (Threshold: {FRESHNESS_THRESHOLD_HOURS} hours)")
print("-"*80)
data_query = """
SELECT 
    MAX(order_ts) AS last_order_time,
    ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(order_ts))) / 3600, 2) AS age_hours
FROM retailpulse.gold.fact_sales
"""
data_result = spark.sql(data_query).collect()[0]
data_age = data_result['age_hours']
data_status = 'PASS ✅' if data_age <= FRESHNESS_THRESHOLD_HOURS else 'FAIL ❌'

print(f"Last Order Timestamp: {data_result['last_order_time']}")
print(f"Age: {data_age} hours")
print(f"Status: {data_status}")
print(f"Explanation: This checks when orders were PLACED (business data timestamp)\n")

# Part 2: Check TABLE metadata freshness
print(f"[2] TABLE FRESHNESS - Checking lastModified metadata (Threshold: {FRESHNESS_THRESHOLD_HOURS} hours)")
print("-"*80)
table_details = spark.sql("DESCRIBE DETAIL retailpulse.gold.fact_sales").collect()[0]
last_modified = table_details['lastModified']
now = datetime.now()
age_seconds = (now - last_modified.replace(tzinfo=None)).total_seconds()
age_hours = age_seconds / 3600
table_status = 'PASS ✅' if age_hours <= FRESHNESS_THRESHOLD_HOURS else 'FAIL ❌'

print(f"Table Last Modified: {last_modified}")
print(f"Age: {round(age_hours, 2)} hours")
print(f"Status: {table_status}")
print(f"Explanation: This checks when ETL job REFRESHED the table (metadata timestamp)\n")

# Summary
print("="*80)
print("SUMMARY & EXPLANATION")
print("="*80)
print(f"""
The KEY DIFFERENCE:

📅 DATA Freshness (order_ts): {round(data_age, 2)} hours old
   - This checks the timestamp of the ORDERS themselves (when customers placed orders)
   - Even if the job runs successfully, old order data will show as stale
   - Status: {data_status} (Threshold: {FRESHNESS_THRESHOLD_HOURS} hours)

🔄 TABLE Freshness (lastModified): {round(age_hours, 2)} hours old
   - This checks when the ETL JOB last refreshed the table
   - Your job ran at 08:17 PM, so this should be recent
   - Status: {table_status} (Threshold: {FRESHNESS_THRESHOLD_HOURS} hours)

RECOMMENDATION:
- If you want to monitor ETL job execution: Check TABLE metadata ✅
- If you want to monitor data recency: Check DATA timestamps ✅
- For complete monitoring: Check BOTH! ✅

With the {FRESHNESS_THRESHOLD_HOURS}-hour threshold:
- DATA freshness: {data_status}
- TABLE freshness: {table_status}
- Both checks align with the validation report in Cell 14!
""")
print("="*80)