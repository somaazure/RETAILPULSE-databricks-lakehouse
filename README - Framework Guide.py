# Databricks notebook source
# DBTITLE 1,RetailPulse Framework Guide
# MAGIC %md
# MAGIC # RetailPulse Metadata-Driven Framework
# MAGIC
# MAGIC **Comprehensive Data Quality, Maintenance, and Audit Framework**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Overview
# MAGIC
# MAGIC This framework provides:
# MAGIC - **Audit Infrastructure**: 5 audit tables tracking DQ, ETL, maintenance, and changes
# MAGIC - **Metadata-Driven Operations**: Centralized configuration in `metadata_catalog`
# MAGIC - **Automated DQ Validation**: Freshness, quality rules, and completeness checks
# MAGIC - **Table Maintenance**: OPTIMIZE, VACUUM, and ANALYZE operations
# MAGIC - **Comprehensive Reporting**: Dashboards and monitoring queries
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Folder Structure
# MAGIC %md
# MAGIC ## 📁 Folder Structure
# MAGIC
# MAGIC Notebooks are organized by function for easy navigation:
# MAGIC
# MAGIC ```
# MAGIC RetailPulse/
# MAGIC ├── 00_Setup/                    (One-time configuration)
# MAGIC │   ├── 01_Setup_Audit_Tables
# MAGIC │   └── 02_Metadata_Configuration
# MAGIC │
# MAGIC ├── 01_DQ_Framework/             (Daily data quality)
# MAGIC │   └── 03_DQ_Framework
# MAGIC │
# MAGIC ├── 02_Maintenance/              (Weekly maintenance)
# MAGIC │   └── 04_Maintenance_Framework
# MAGIC │
# MAGIC ├── 03_Reporting/                (Ad-hoc reporting)
# MAGIC │   └── 05_Audit_Reporting
# MAGIC │
# MAGIC ├── 04_Orchestration/            (Job configs - future)
# MAGIC │
# MAGIC ├── 05_Documentation/            (Architecture docs)
# MAGIC │   └── RetailPulse Framework Architecture
# MAGIC │
# MAGIC └── 99_Archive/                  (Old versions - future)
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Notebook Descriptions
# MAGIC %md
# MAGIC ## 📝 Notebook Descriptions
# MAGIC
# MAGIC ### 00_Setup/ - Initial Configuration
# MAGIC
# MAGIC | Notebook | Purpose | Run Frequency |
# MAGIC |----------|---------|---------------|
# MAGIC | **01_Setup_Audit_Tables** | Creates 5 audit tables in `retailpulse.ops` | Once (initial setup) |
# MAGIC | **02_Metadata_Configuration** | Populates metadata catalog with table definitions and rules | Once + updates |
# MAGIC
# MAGIC **Status**: ✅ Completed - All 5 audit tables created, 6 tables configured
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 01_DQ_Framework/ - Data Quality Validation
# MAGIC
# MAGIC | Notebook | Purpose | Run Frequency |
# MAGIC |----------|---------|---------------|
# MAGIC | **03_DQ_Framework** | Executes metadata-driven DQ validations (freshness, quality rules, completeness) | Daily (automated) |
# MAGIC
# MAGIC **Current Status**: ✅ 13/13 checks passing
# MAGIC
# MAGIC **DQ Checks Performed**:
# MAGIC - **Freshness**: SLA compliance validation (4 tables)
# MAGIC - **Quality Rules**: Column-level validations (13 rules)
# MAGIC   - NOT_NULL, POSITIVE, NON_NEGATIVE, DATE_VALID
# MAGIC - **Completeness**: Row count validation (4 tables)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 02_Maintenance/ - Table Optimization
# MAGIC
# MAGIC | Notebook | Purpose | Run Frequency |
# MAGIC |----------|---------|---------------|
# MAGIC | **04_Maintenance_Framework** | OPTIMIZE (with ZORDER), VACUUM, ANALYZE operations | Weekly (automated) |
# MAGIC
# MAGIC **Operations**:
# MAGIC - **OPTIMIZE**: Compact small files, apply ZORDER
# MAGIC - **VACUUM**: Reclaim storage space
# MAGIC - **ANALYZE**: Update table statistics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 03_Reporting/ - Monitoring & Analytics
# MAGIC
# MAGIC | Notebook | Purpose | Run Frequency |
# MAGIC |----------|---------|---------------|
# MAGIC | **05_Audit_Reporting** | 20+ queries for DQ, ETL, maintenance monitoring | Ad-hoc |
# MAGIC
# MAGIC **Reports Available**:
# MAGIC - DQ monitoring (pass rates, trends, failures)
# MAGIC - ETL monitoring (success rates, performance)
# MAGIC - Maintenance effectiveness
# MAGIC - Executive dashboard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 05_Documentation/ - Architecture & Guides
# MAGIC
# MAGIC | Notebook | Purpose | Updates |
# MAGIC |----------|---------|----------|
# MAGIC | **RetailPulse Framework Architecture** | Complete framework documentation with Mermaid diagrams | As needed |
# MAGIC
# MAGIC **Contents**: 12 sections with 8 Mermaid diagrams covering architecture, flows, and operations
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Execution Order
# MAGIC %md
# MAGIC ## ⏩ Execution Order
# MAGIC
# MAGIC ### First-Time Setup (Run Once)
# MAGIC
# MAGIC 1. **Run Orchestrator Job**
# MAGIC    - Job: `RetailPulse Enterprise Orchestrator`
# MAGIC    - Purpose: Populate Bronze/Silver/Gold tables
# MAGIC    - Duration: ~15 minutes
# MAGIC
# MAGIC 2. **Run Setup Notebooks** (in order)
# MAGIC    ```
# MAGIC    00_Setup/01_Setup_Audit_Tables          → Creates audit infrastructure
# MAGIC    00_Setup/02_Metadata_Configuration      → Configures metadata
# MAGIC    ```
# MAGIC
# MAGIC ### Regular Operations
# MAGIC
# MAGIC #### Daily - Data Quality Checks
# MAGIC ```
# MAGIC 01_DQ_Framework/03_DQ_Framework
# MAGIC Recommended time: 6:00 AM and 6:00 PM (after data refresh)
# MAGIC ```
# MAGIC
# MAGIC #### Weekly - Maintenance Operations
# MAGIC ```
# MAGIC 02_Maintenance/04_Maintenance_Framework
# MAGIC Recommended time: Sunday 1:00 AM (off-peak hours)
# MAGIC ```
# MAGIC
# MAGIC #### Ad-hoc - Health Monitoring
# MAGIC ```
# MAGIC 03_Reporting/05_Audit_Reporting
# MAGIC Run anytime to check framework health
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Quick Links
# MAGIC %md
# MAGIC ## 🔗 Quick Links
# MAGIC
# MAGIC ### Setup Notebooks
# MAGIC - [01_Setup_Audit_Tables](#notebook/452130069342023)
# MAGIC - [02_Metadata_Configuration](#notebook/452130069342024)
# MAGIC
# MAGIC ### Operational Notebooks
# MAGIC - [03_DQ_Framework](#notebook/452130069342025)
# MAGIC - [04_Maintenance_Framework](#notebook/452130069342026)
# MAGIC - [05_Audit_Reporting](#notebook/452130069342027)
# MAGIC
# MAGIC ### Documentation
# MAGIC - [RetailPulse Framework Architecture](#notebook/452130069342028)
# MAGIC
# MAGIC ### Jobs
# MAGIC - [RetailPulse Enterprise Orchestrator](#job/869046484148482)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Tables
# MAGIC %md
# MAGIC ## 🗄️ Key Tables
# MAGIC
# MAGIC ### Audit Tables (retailpulse.ops)
# MAGIC - [metadata_catalog](#table/retailpulse.ops.metadata_catalog) - Central configuration registry
# MAGIC - [dq_validation_audit](#table/retailpulse.ops.dq_validation_audit) - Quality check logs
# MAGIC - [etl_job_audit](#table/retailpulse.ops.etl_job_audit) - Pipeline execution logs
# MAGIC - [maintenance_audit](#table/retailpulse.ops.maintenance_audit) - Maintenance operation logs
# MAGIC - [table_change_audit](#table/retailpulse.ops.table_change_audit) - Data lineage tracking
# MAGIC
# MAGIC ### Data Tables
# MAGIC - **Bronze**: [orders_raw](#table/retailpulse.bronze.orders_raw)
# MAGIC - **Silver**: [orders](#table/retailpulse.silver.orders), [products](#table/retailpulse.silver.products)
# MAGIC - **Gold**: [fact_sales](#table/retailpulse.gold.fact_sales), [dim_customer](#table/retailpulse.gold.dim_customer), [dim_product](#table/retailpulse.gold.dim_product), [dim_date](#table/retailpulse.gold.dim_date)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Framework Status
# MAGIC %md
# MAGIC ## ✅ Framework Status
# MAGIC
# MAGIC | Component | Status | Details |
# MAGIC |---|---|---|
# MAGIC | **Audit Tables** | ✅ Ready | All 5 tables in retailpulse.ops |
# MAGIC | **Metadata Catalog** | ✅ Configured | 6 tables, 14 columns with rules |
# MAGIC | **Data Population** | ✅ Complete | Orchestrator job ran successfully |
# MAGIC | **DQ Framework** | ✅ Operational | 13/13 checks passing |
# MAGIC | **Maintenance** | ✅ Ready | Framework implemented |
# MAGIC | **Reporting** | ✅ Ready | 20+ queries available |
# MAGIC | **Documentation** | ✅ Complete | Architecture diagrams created |
# MAGIC | **Folder Organization** | ✅ Complete | All notebooks organized by function |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Scheduling Recommendations
# MAGIC %md
# MAGIC ## 🗓️ Scheduling Recommendations
# MAGIC
# MAGIC ### Databricks Jobs to Create
# MAGIC
# MAGIC #### 1. DQ Validation Job (Daily)
# MAGIC ```yaml
# MAGIC Name: RetailPulse DQ Validation
# MAGIC Notebook: RetailPulse/01_DQ_Framework/03_DQ_Framework
# MAGIC Schedule: 0 0 6,18 * * ? *  # Twice daily at 6 AM and 6 PM
# MAGIC Timeout: 30 minutes
# MAGIC Alerts: Email on failure
# MAGIC ```
# MAGIC
# MAGIC #### 2. Maintenance Job (Weekly)
# MAGIC ```yaml
# MAGIC Name: RetailPulse Weekly Maintenance
# MAGIC Notebook: RetailPulse/02_Maintenance/04_Maintenance_Framework
# MAGIC Schedule: 0 0 1 ? * SUN *  # Sundays at 1 AM
# MAGIC Timeout: 2 hours
# MAGIC Alerts: Email on failure
# MAGIC ```
# MAGIC
# MAGIC #### 3. Health Report Job (Daily)
# MAGIC ```yaml
# MAGIC Name: RetailPulse Health Report
# MAGIC Notebook: RetailPulse/03_Reporting/05_Audit_Reporting
# MAGIC Schedule: 0 0 9 * * ? *  # Daily at 9 AM
# MAGIC Timeout: 15 minutes
# MAGIC Output: Email summary to leadership
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Troubleshooting
# MAGIC %md
# MAGIC ## 🔧 Troubleshooting
# MAGIC
# MAGIC ### DQ Check Failures
# MAGIC 1. Check [dq_validation_audit](#table/retailpulse.ops.dq_validation_audit) for failure details
# MAGIC 2. Query affected table directly to verify data
# MAGIC 3. Re-run [RetailPulse Enterprise Orchestrator](#job/869046484148482) if data is missing
# MAGIC 4. Update [metadata_catalog](#table/retailpulse.ops.metadata_catalog) if schema changed
# MAGIC
# MAGIC ### Empty Tables
# MAGIC 1. Run [RetailPulse Enterprise Orchestrator](#job/869046484148482)
# MAGIC 2. Check [etl_job_audit](#table/retailpulse.ops.etl_job_audit) for pipeline failures
# MAGIC 3. Verify source data exists in cloud storage
# MAGIC
# MAGIC ### Maintenance Issues
# MAGIC 1. Check [maintenance_audit](#table/retailpulse.ops.maintenance_audit) for errors
# MAGIC 2. Ensure MODIFY privileges on tables
# MAGIC 3. Avoid running during peak query hours
# MAGIC 4. Ensure VACUUM retention >= 7 days
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC ### Immediate (Week 1)
# MAGIC - [ ] Create scheduled jobs for DQ and maintenance
# MAGIC - [ ] Set up email alerts for DQ failures
# MAGIC - [ ] Share framework documentation with team
# MAGIC
# MAGIC ### Short-term (Month 1)
# MAGIC - [ ] Add more tables to metadata_catalog
# MAGIC - [ ] Implement additional validation rules (e.g., EMAIL_FORMAT)
# MAGIC - [ ] Create Databricks SQL dashboards from reporting queries
# MAGIC - [ ] Set up Slack notifications
# MAGIC
# MAGIC ### Long-term (Quarter 1)
# MAGIC - [ ] Expand to additional data domains
# MAGIC - [ ] Implement custom DQ check types
# MAGIC - [ ] Build executive dashboard with auto-refresh
# MAGIC - [ ] Create runbooks for common scenarios
# MAGIC - [ ] Integrate with incident management (PagerDuty)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📞 Support
# MAGIC
# MAGIC For questions or issues with the framework:
# MAGIC 1. Review [RetailPulse Framework Architecture](#notebook/452130069342028) for detailed diagrams
# MAGIC 2. Check [05_Audit_Reporting](#notebook/452130069342027) for monitoring queries
# MAGIC 3. Contact the Data Engineering team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Last Updated**: March 26, 2026  
# MAGIC **Framework Version**: 1.0  
# MAGIC **Status**: Production Ready ✅