# RetailPulse Operational Runbook

This runbook documents the complete operational procedures for the RetailPulse enterprise data pipeline and metadata-driven governance framework.

---

## 📍 Workspace Information

**Workspace Path**: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse`  
**Catalog**: `retailpulse`  
**Schemas**: `bronze`, `silver`, `gold`, `ops`  
**Primary Job**: RetailPulse Enterprise Orchestrator (ID: 869046484148482)

---

## 🔄 Complete Execution Order

### STEP 1: Data Pipeline (Every 4 hours)
**What**: RetailPulse Enterprise Orchestrator Job  
**When**: Every 4 hours (0, 4, 8, 12, 16, 20)  
**Duration**: ~15-20 minutes  
**Status**: PRODUCTION - Automated

#### Task 1: bronze_silver_dlt
**Type**: DLT Pipeline  
**Config**: `config/dlt_bronze_silver_pipeline.json`  
**Notebook**: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/08_dlt_e2e_main_refresh`

**Purpose**:
- DLT owns Bronze ingestion with Auto Loader
- DLT owns Silver curation and quarantine
- Handles schema evolution and rescue data

**Outputs**:
- `retailpulse.bronze.orders` - Raw ingested orders
- `retailpulse.silver.orders` - Validated and deduplicated orders
- `retailpulse.ops.silver_orders_quarantine` - Invalid Silver records with dq_reason

**Success Criteria**:
- Pipeline completes without errors
- New records appear in Bronze and Silver
- Invalid records captured in quarantine

#### Task 2: gold_dims_facts
**Type**: Databricks Job  
**Config**: `config/job_gold_dims_facts.json`  
**Dependency**: Runs only after bronze_silver_dlt succeeds

**Notebooks** (in execution order):
1. `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/09_product_master`
2. `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/10_dim_product`
3. `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/11_dim_customer`
4. `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/12_dim_date`
5. `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/13_fact_sales`

**Purpose**:
- Keep Gold logic modular and independently rerunnable
- Separate product mastering, SCD2 maintenance, dimensions, and fact loads
- Keep fact quarantine in Gold job layer

**Outputs**:
- `retailpulse.silver.products` - Product master data
- `retailpulse.gold.dim_product` - Product dimension (SCD Type 2)
- `retailpulse.gold.dim_customer` - Customer dimension
- `retailpulse.gold.dim_date` - Date dimension
- `retailpulse.gold.fact_sales` - Sales fact table
- `retailpulse.ops.fact_sales_quarantine` - Unresolved facts with dq_reason

**Success Criteria**:
- All 5 notebooks complete successfully
- Dimensions and fact table updated
- SCD2 logic maintains product history correctly
- Unresolved records captured in quarantine

---

### STEP 2: Data Quality Validation (After data load)
**What**: DQ Framework Notebook  
**When**: After STEP 1 completes successfully  
**Duration**: ~5-10 minutes  
**Status**: PRODUCTION - Should be automated

**Notebook**: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/01_DQ_Framework/03_DQ_Framework`

**Purpose**:
- Validate data freshness (SLA compliance)
- Validate column-level quality rules
- Validate table completeness (row counts)
- Log all results to audit table

**Operations Performed**:
1. **Freshness Checks**
   - Query `information_schema.tables` for last_altered timestamp
   - Compare against SLA hours from metadata_catalog
   - PASS if freshness_hours <= sla_hours

2. **Quality Rules Checks**
   - Read column-level validation rules from metadata_catalog
   - Supported rules: NOT_NULL, POSITIVE, NON_NEGATIVE, DATE_VALID, EMAIL_FORMAT
   - Count violations for each rule
   - Calculate pass_rate = (total - violations) / total

3. **Completeness Checks**
   - Query row count for each active table
   - PASS if total_records > 0

**Audit Output**: `retailpulse.ops.dq_validation_audit`

**Success Criteria**:
- All checks execute without errors
- Pass rate >= 95% (configurable threshold)
- No FAIL status for critical checks
- Results logged with timestamp, status, violation counts

**Troubleshooting**:
- Check metadata_catalog for correct column names
- Review dq_validation_audit for recent failures
- Investigate tables with FAIL status

---

### STEP 3: Table Maintenance (Weekly)
**What**: Maintenance Framework Notebook  
**When**: Weekly (Sundays at 1 AM)  
**Duration**: ~30-60 minutes  
**Status**: PRODUCTION - Should be automated

**Notebook**: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/02_Maintenance/04_Maintenance_Framework`

**Purpose**:
- Compact small files for query performance (OPTIMIZE)
- Reclaim storage space (VACUUM)
- Update table statistics (ANALYZE)
- All operations driven by metadata_catalog policies

**Operations Performed**:
1. **OPTIMIZE**
   - Compacts small files into larger files
   - Applies ZORDER on configured columns (e.g., order_date, customer_id)
   - Tracks before/after file counts and sizes
   - Expected: 50-90% reduction in file count

2. **VACUUM**
   - Removes old data files beyond retention period
   - Minimum retention: 168 hours (7 days)
   - Tracks before/after sizes
   - Expected: 20-70% storage savings for high-churn tables

3. **ANALYZE**
   - Collects table statistics for query optimizer
   - Updates row counts, distinct values, min/max values
   - Improves query plan generation

**Audit Output**: `retailpulse.ops.maintenance_audit`

**Success Criteria**:
- All operations complete successfully
- OPTIMIZE reduces file count by 50%+
- VACUUM reclaims storage space
- No data loss or corruption
- Metrics logged for trending

**Troubleshooting**:
- If OPTIMIZE shows no file reduction: table already optimized (normal)
- If VACUUM fails: check retention hours >= 168
- If concurrent write errors: reschedule during low-activity period

---

### STEP 4: Health Reporting (Daily / On-Demand)
**What**: Audit Reporting Notebook  
**When**: Daily at 9 AM or on-demand  
**Duration**: ~2-5 minutes  
**Status**: PRODUCTION - Manual or automated

**Notebook**: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/03_Reporting/05_Audit_Reporting`

**Purpose**:
- Generate health dashboards
- Review DQ check trends
- Monitor ETL performance
- Track maintenance effectiveness

**Reports Available**:
- DQ check pass rates over time
- Failed DQ checks detail
- ETL job success/failure rates
- Maintenance operation metrics
- Executive summary dashboard

**Data Sources**: All audit tables in `retailpulse.ops.*`

**Success Criteria**:
- Reports run without errors
- Metrics reflect current system state
- Trends visible for decision-making

---

## 📋 Metadata-Driven Framework Components

### Audit Tables (retailpulse.ops schema)

All created by: `/Workspace/Users/shekartelstra@gmail.com/RetailPulse/00_Setup/01_Setup_Audit_Tables`

| Table | Purpose | Populated By |
|-------|---------|-------------|
| `metadata_catalog` | Central registry for tables, columns, validation rules, maintenance policies | Manual configuration |
| `dq_validation_audit` | Logs DQ check results with pass/fail status, violation counts | 03_DQ_Framework |
| `etl_job_audit` | Tracks ETL pipeline runs with duration, row counts | Orchestrator Job |
| `maintenance_audit` | Records OPTIMIZE/VACUUM/ANALYZE operations with metrics | 04_Maintenance_Framework |
| `table_change_audit` | Captures data lineage and change tracking | Trigger-based (future) |

### Framework Notebooks

**Setup** (Run once):
- `00_Setup/01_Setup_Audit_Tables` - Create audit tables
- `00_Setup/02_Metadata_Configuration` - Populate metadata catalog

**Active** (Run regularly):
- `01_DQ_Framework/03_DQ_Framework` - Data quality validation
- `02_Maintenance/04_Maintenance_Framework` - Table maintenance
- `03_Reporting/05_Audit_Reporting` - Health reporting

**Documentation**:
- `05_Documentation/RetailPulse Framework Architecture` - Complete architecture diagrams
- `README - Framework Guide` - Quick reference guide

---

## 🚨 Operational Procedures

### Manual Execution (For Testing)

#### Run Complete Pipeline End-to-End
```bash
# Step 1: Run data pipeline
Run Job: RetailPulse Enterprise Orchestrator (ID: 869046484148482)

# Step 2: Wait for completion, then run DQ validation
Run Notebook: RetailPulse/01_DQ_Framework/03_DQ_Framework

# Step 3: Review DQ results
Run Notebook: RetailPulse/03_Reporting/05_Audit_Reporting

# Step 4: Run maintenance (weekly only)
Run Notebook: RetailPulse/02_Maintenance/04_Maintenance_Framework
```

#### Generate Sample Data (If Needed)
```bash
Run Notebook: RetailPulse/notebooks/00_generate_orders_once
# This generates 250 sample order records in CSV format
# Files written to: /Volumes/retailpulse/bronze/orders_files/orders_business_ts/
```

### Monitoring and Alerting

#### Check Data Pipeline Health
```sql
-- Verify record counts
SELECT 'bronze.orders' AS layer, COUNT(*) AS record_count FROM retailpulse.bronze.orders
UNION ALL
SELECT 'silver.orders', COUNT(*) FROM retailpulse.silver.orders
UNION ALL
SELECT 'gold.fact_sales', COUNT(*) FROM retailpulse.gold.fact_sales;

-- Check quarantine counts (should be low)
SELECT 'silver_quarantine' AS table_name, COUNT(*) AS quarantine_count 
FROM retailpulse.ops.silver_orders_quarantine
UNION ALL
SELECT 'fact_quarantine', COUNT(*) 
FROM retailpulse.ops.fact_sales_quarantine;
```

#### Check DQ Health
```sql
-- DQ pass rate (last 24 hours)
SELECT 
  ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate_pct,
  COUNT(*) AS total_checks,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks
FROM retailpulse.ops.dq_validation_audit
WHERE run_timestamp >= current_timestamp() - INTERVAL 24 HOURS;

-- Failed DQ checks detail
SELECT 
  table_name,
  check_name,
  column_name,
  violation_count,
  message,
  run_timestamp
FROM retailpulse.ops.dq_validation_audit
WHERE status = 'FAIL'
  AND run_timestamp >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY run_timestamp DESC;
```

#### Check Maintenance Effectiveness
```sql
-- Recent maintenance operations
SELECT 
  table_name,
  operation,
  status,
  num_files_before,
  num_files_after,
  ROUND(space_saved_mb, 2) AS space_saved_mb,
  duration_seconds,
  start_time
FROM retailpulse.ops.maintenance_audit
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY start_time DESC;
```

### Troubleshooting Guide

#### Issue: Data pipeline job fails
**Symptoms**: Orchestrator job shows FAILED status  
**Investigation**:
1. Check job run logs in Databricks UI
2. Identify which task failed (DLT or Gold)
3. Review task-specific logs

**Common Causes**:
- DLT task fails: CSV file format issues, schema changes
- Gold task fails: Missing dimension records, constraint violations

**Resolution**:
- Fix data quality issues in source files
- Update schema if intentional schema evolution
- Check quarantine tables for rejected records

#### Issue: DQ checks fail
**Symptoms**: dq_validation_audit shows FAIL status  
**Investigation**:
1. Query dq_validation_audit for failed checks
2. Review violation_count and message fields
3. Query the affected table to see actual violations

**Common Causes**:
- Column name mismatch in metadata_catalog
- Business rule changes (e.g., negative values now allowed)
- Data quality degradation at source

**Resolution**:
- Update metadata_catalog with correct column names
- Adjust validation rules to match business requirements
- Work with data providers to improve source quality

#### Issue: Maintenance operations slow
**Symptoms**: Maintenance notebook runs >60 minutes  
**Investigation**:
1. Check maintenance_audit for duration_seconds per operation
2. Identify which tables take longest
3. Check table sizes with DESCRIBE DETAIL

**Common Causes**:
- Very large tables (>100GB)
- Many small files accumulated
- Concurrent writes during maintenance

**Resolution**:
- Schedule maintenance during low-activity periods
- Consider incremental OPTIMIZE for large tables
- Increase cluster size for maintenance job

#### Issue: Quarantine tables growing
**Symptoms**: High record counts in quarantine tables  
**Investigation**:
1. Query quarantine tables for dq_reason field
2. Group by dq_reason to identify patterns
3. Sample quarantine records to see examples

**Common Causes**:
- Source data quality issues
- Validation rules too strict
- Schema evolution not handled

**Resolution**:
- Work with data providers to fix source issues
- Review and adjust DQ expectations if needed
- Update schema handling in DLT pipeline

---

## 📅 Recommended Scheduling

### Job 1: RetailPulse Enterprise Orchestrator
```yaml
Job ID: 869046484148482
Schedule: 0 0 0,4,8,12,16,20 * * ? *  # Every 4 hours
Timeout: 30 minutes
Retries: 2
Alerts: Email on failure
Status: ACTIVE - PRODUCTION
```

### Job 2: RetailPulse DQ Validation
```yaml
Notebook: /RetailPulse/01_DQ_Framework/03_DQ_Framework
Schedule: 0 0 6,18 * * ? *  # Twice daily at 6 AM and 6 PM
Dependency: Runs after Orchestrator succeeds
Timeout: 30 minutes
Retries: 1
Alerts: Email on failure
Status: RECOMMENDED - Not yet scheduled
```

### Job 3: RetailPulse Maintenance
```yaml
Notebook: /RetailPulse/02_Maintenance/04_Maintenance_Framework
Schedule: 0 0 1 ? * SUN *  # Sundays at 1 AM
Timeout: 2 hours
Retries: 0 (manual rerun if needed)
Alerts: Email on failure
Status: RECOMMENDED - Not yet scheduled
```

### Job 4: RetailPulse Health Report
```yaml
Notebook: /RetailPulse/03_Reporting/05_Audit_Reporting
Schedule: 0 0 9 * * ? *  # Daily at 9 AM
Timeout: 15 minutes
Retries: 1
Output: Email summary to leadership
Status: OPTIONAL - Manual on-demand
```

---

## 🔧 Configuration Files

**DLT Pipeline**:
- `config/dlt_bronze_silver_pipeline.json`
- Creates: Bronze and Silver layers

**Gold Job**:
- `config/job_gold_dims_facts.json`
- Creates: Gold dimensions and facts

**Orchestrator Job**:
- `config/job_enterprise_orchestrator.json`
- Coordinates: DLT → Gold execution

---

## 🏗️ One-Time Setup / Cutover

### Initial Setup (New Workspace)

1. **Create Audit Tables**
   ```
   Run: RetailPulse/00_Setup/01_Setup_Audit_Tables
   Creates: 5 audit tables in retailpulse.ops schema
   ```

2. **Configure Metadata**
   ```
   Run: RetailPulse/00_Setup/02_Metadata_Configuration
   Populates: metadata_catalog with tables, columns, rules
   ```

3. **Create DLT Pipeline**
   ```powershell
   databricks pipelines create --json @config/dlt_bronze_silver_pipeline.json --profile <profile>
   ```

4. **Create Orchestrator Job**
   ```powershell
   databricks jobs create --json @config/job_enterprise_orchestrator.json --profile <profile>
   ```

5. **Test End-to-End**
   ```
   Run: RetailPulse Enterprise Orchestrator Job (manual)
   Verify: All tables created and populated
   ```

### Cutover from Old DLT-Managed Branch

If workspace has DLT-managed Gold tables from older one-click branch:

**Tables to Check**:
- `retailpulse.silver.products`
- `retailpulse.gold.dim_product`
- `retailpulse.gold.dim_customer`
- `retailpulse.gold.dim_date`
- `retailpulse.gold.fact_sales`
- `retailpulse.ops.fact_sales_quarantine`

**Cutover Steps**:
1. Backup current tables (if needed)
2. Stop older full-DLT refresh flow
3. Drop DLT-managed Gold objects
4. Run enterprise Gold notebooks to recreate as standard Delta tables

**Verification**:
```sql
DESCRIBE EXTENDED retailpulse.gold.dim_product;
-- Should show "Type: MANAGED" not "Type: MATERIALIZED_VIEW"
```

---

## 📂 Archive

**Older Notebook Variants**:
`/Workspace/Users/shekartelstra@gmail.com/RetailPulse/notebooks/archive`

**Legacy Config Files**:
`config/archive`

**Archived Utilities**:
`RetailPulse/99_Archive/src/utils/`
- `delta_utils.py` (empty placeholder)
- `validation_utils.py` (empty placeholder)

---

## 🔗 Additional Resources

**Documentation**:
- README.MD - Complete project overview
- README - Framework Guide - Quick reference notebook
- RetailPulse Framework Architecture - Detailed architecture notebook

**AI Context**:
- `.ai/context/architecture.md` - Architecture documentation
- `.ai/skills/dq_framework.md` - DQ validation skill
- `.ai/skills/maintenance_framework.md` - Maintenance operations skill

**Repository**:
`https://github.com/somaazure/RETAILPULSE-databricks-lakehouse`

---

## 📞 Support and Escalation

**Runbook Owner**: Data Engineering Team  
**Last Updated**: March 26, 2026  
**Version**: 2.0 (Metadata-Driven Framework)

**For Issues**:
1. Check troubleshooting guide above
2. Review audit tables for error details
3. Check job logs in Databricks UI
4. Escalate to data engineering team if unresolved

**Success Metrics**:
- Data pipeline: 95%+ success rate
- DQ checks: 95%+ pass rate
- Maintenance: Completes within 2 hours
- Quarantine: <5% of total records

---

## ✅ Pre-Flight Checklist

Before running production workloads:

- [ ] Audit tables created (retailpulse.ops.*)
- [ ] Metadata catalog populated with validation rules
- [ ] DLT pipeline deployed and tested
- [ ] Orchestrator job created with correct task dependencies
- [ ] Sample data generated and pipeline tested end-to-end
- [ ] DQ Framework tested with metadata-driven checks
- [ ] Maintenance Framework tested on dev tables
- [ ] Monitoring queries validated
- [ ] Alert recipients configured
- [ ] Backup and disaster recovery plan documented

---

*This runbook is a living document. Update after significant changes to the pipeline or framework.*
