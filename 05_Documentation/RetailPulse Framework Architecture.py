# Databricks notebook source
# DBTITLE 1,Framework Overview
# MAGIC %md
# MAGIC # RetailPulse Metadata-Driven Framework Architecture
# MAGIC
# MAGIC **Complete End-to-End Documentation**
# MAGIC
# MAGIC This notebook provides comprehensive diagrams and documentation for the RetailPulse metadata-driven data quality, maintenance, and audit framework.
# MAGIC
# MAGIC ## Framework Components
# MAGIC
# MAGIC 1. **Audit Tables** - Central repository for metadata, DQ results, ETL logs, maintenance operations
# MAGIC 2. **Metadata Configuration** - Centralized catalog defining tables, columns, validation rules, policies
# MAGIC 3. **DQ Framework** - Automated data quality validation driven by metadata
# MAGIC 4. **Maintenance Framework** - Automated table optimization, vacuum, and statistics collection
# MAGIC 5. **Audit Reporting** - Comprehensive dashboards and monitoring queries
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Overall Framework Architecture
# MAGIC %md
# MAGIC ## 🏗️ Overall Framework Architecture
# MAGIC
# MAGIC This diagram shows how all components of the framework interconnect:
# MAGIC
# MAGIC ```mermaid
# MAGIC graph TB
# MAGIC     subgraph "📊 Data Layers"
# MAGIC         Bronze[(Bronze Tables<br/>Raw Data)]
# MAGIC         Silver[(Silver Tables<br/>Validated Data)]
# MAGIC         Gold[(Gold Tables<br/>Curated Data)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🗄️ Audit Infrastructure (retailpulse.ops)"
# MAGIC         MetaCat[(metadata_catalog<br/>Central Registry)]
# MAGIC         DQAudit[(dq_validation_audit<br/>Quality Logs)]
# MAGIC         ETLAudit[(etl_job_audit<br/>Pipeline Logs)]
# MAGIC         MaintAudit[(maintenance_audit<br/>Ops Logs)]
# MAGIC         ChangeAudit[(table_change_audit<br/>Lineage)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🔍 DQ Framework (03_DQ_Framework)"
# MAGIC         DQSetup[Setup & Imports]
# MAGIC         DQHelper[Helper Functions]
# MAGIC         FreshnessCheck[Freshness Checks<br/>SLA Validation]
# MAGIC         QualityCheck[Quality Rules<br/>Column Validation]
# MAGIC         CompletenessCheck[Completeness<br/>Row Count Checks]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "⚙️ Maintenance Framework (04_Maintenance_Framework)"
# MAGIC         MaintSetup[Setup & Imports]
# MAGIC         MaintHelper[Helper Functions]
# MAGIC         OptimizeOps[OPTIMIZE<br/>+ ZORDER]
# MAGIC         VacuumOps[VACUUM<br/>Space Reclaim]
# MAGIC         AnalyzeOps[ANALYZE<br/>Statistics]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "📈 Audit Reporting (05_Audit_Reporting)"
# MAGIC         DQReports[DQ Monitoring<br/>Pass Rates & Trends]
# MAGIC         ETLReports[ETL Monitoring<br/>Success Rates]
# MAGIC         MaintReports[Maintenance Stats<br/>Effectiveness]
# MAGIC         ExecDash[Executive Dashboard<br/>Health Metrics]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🎯 Orchestration"
# MAGIC         Orchestrator[RetailPulse Enterprise<br/>Orchestrator Job]
# MAGIC         DLTPipeline[DLT Pipelines<br/>Data Processing]
# MAGIC     end
# MAGIC     
# MAGIC     %% Configuration Flow
# MAGIC     MetaCat -->|Defines| Bronze
# MAGIC     MetaCat -->|Defines| Silver
# MAGIC     MetaCat -->|Defines| Gold
# MAGIC     
# MAGIC     %% DQ Framework Flow
# MAGIC     MetaCat -->|Reads Config| DQSetup
# MAGIC     DQSetup --> DQHelper
# MAGIC     DQHelper --> FreshnessCheck
# MAGIC     DQHelper --> QualityCheck
# MAGIC     DQHelper --> CompletenessCheck
# MAGIC     
# MAGIC     Silver --> FreshnessCheck
# MAGIC     Gold --> FreshnessCheck
# MAGIC     Silver --> QualityCheck
# MAGIC     Gold --> QualityCheck
# MAGIC     Silver --> CompletenessCheck
# MAGIC     Gold --> CompletenessCheck
# MAGIC     
# MAGIC     FreshnessCheck -->|Logs Results| DQAudit
# MAGIC     QualityCheck -->|Logs Results| DQAudit
# MAGIC     CompletenessCheck -->|Logs Results| DQAudit
# MAGIC     
# MAGIC     %% Maintenance Framework Flow
# MAGIC     MetaCat -->|Reads Policies| MaintSetup
# MAGIC     MaintSetup --> MaintHelper
# MAGIC     MaintHelper --> OptimizeOps
# MAGIC     MaintHelper --> VacuumOps
# MAGIC     MaintHelper --> AnalyzeOps
# MAGIC     
# MAGIC     OptimizeOps -->|Optimizes| Bronze
# MAGIC     OptimizeOps -->|Optimizes| Silver
# MAGIC     OptimizeOps -->|Optimizes| Gold
# MAGIC     VacuumOps -->|Cleans| Bronze
# MAGIC     VacuumOps -->|Cleans| Silver
# MAGIC     VacuumOps -->|Cleans| Gold
# MAGIC     AnalyzeOps -->|Analyzes| Silver
# MAGIC     AnalyzeOps -->|Analyzes| Gold
# MAGIC     
# MAGIC     OptimizeOps -->|Logs Operations| MaintAudit
# MAGIC     VacuumOps -->|Logs Operations| MaintAudit
# MAGIC     AnalyzeOps -->|Logs Operations| MaintAudit
# MAGIC     
# MAGIC     %% Orchestration Flow
# MAGIC     Orchestrator -->|Triggers| DLTPipeline
# MAGIC     DLTPipeline -->|Writes| Bronze
# MAGIC     DLTPipeline -->|Transforms| Silver
# MAGIC     DLTPipeline -->|Aggregates| Gold
# MAGIC     DLTPipeline -->|Logs Runs| ETLAudit
# MAGIC     DLTPipeline -->|Tracks Changes| ChangeAudit
# MAGIC     
# MAGIC     %% Reporting Flow
# MAGIC     DQAudit -->|Queries| DQReports
# MAGIC     ETLAudit -->|Queries| ETLReports
# MAGIC     MaintAudit -->|Queries| MaintReports
# MAGIC     DQAudit -->|Aggregates| ExecDash
# MAGIC     ETLAudit -->|Aggregates| ExecDash
# MAGIC     MaintAudit -->|Aggregates| ExecDash
# MAGIC     
# MAGIC     classDef dataLayer fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
# MAGIC     classDef auditTable fill:#fff3e0,stroke:#f57c00,stroke-width:2px
# MAGIC     classDef framework fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
# MAGIC     classDef reporting fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
# MAGIC     classDef orchestration fill:#fce4ec,stroke:#c2185b,stroke-width:2px
# MAGIC     
# MAGIC     class Bronze,Silver,Gold dataLayer
# MAGIC     class MetaCat,DQAudit,ETLAudit,MaintAudit,ChangeAudit auditTable
# MAGIC     class DQSetup,DQHelper,FreshnessCheck,QualityCheck,CompletenessCheck,MaintSetup,MaintHelper,OptimizeOps,VacuumOps,AnalyzeOps framework
# MAGIC     class DQReports,ETLReports,MaintReports,ExecDash reporting
# MAGIC     class Orchestrator,DLTPipeline orchestration
# MAGIC ```
# MAGIC
# MAGIC ### Key Insights:
# MAGIC - **Metadata-Driven**: All operations read from `metadata_catalog`
# MAGIC - **Comprehensive Logging**: Every operation logs to appropriate audit tables
# MAGIC - **Automated Workflows**: Orchestrator triggers end-to-end data processing
# MAGIC - **Monitoring & Reporting**: Real-time visibility into data quality and operations

# COMMAND ----------

# DBTITLE 1,DQ Validation Flow
# MAGIC %md
# MAGIC ## 🔍 Data Quality Validation Flow
# MAGIC
# MAGIC Detailed flow of how DQ checks are executed:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     Start([Start DQ Framework]) --> Init[Initialize:<br/>- Timestamp<br/>- User<br/>- Spark Session]
# MAGIC     
# MAGIC     Init --> DefineHelper[Define log_dq_result()<br/>Helper Function]
# MAGIC     
# MAGIC     DefineHelper --> QueryMeta1[Query metadata_catalog:<br/>Get tables with SLA]
# MAGIC     
# MAGIC     QueryMeta1 --> FreshnessLoop{For each<br/>table with SLA}
# MAGIC     
# MAGIC     FreshnessLoop -->|Yes| GetHistory[Query information_schema:<br/>Get last_altered timestamp]
# MAGIC     
# MAGIC     GetHistory --> CalcAge[Calculate data age:<br/>hours_old = current_time - last_altered]
# MAGIC     
# MAGIC     CalcAge --> CheckSLA{hours_old<br/><= sla_hours?}
# MAGIC     
# MAGIC     CheckSLA -->|Yes| LogPass1[Log PASS to<br/>dq_validation_audit]
# MAGIC     CheckSLA -->|No| LogFail1[Log FAIL to<br/>dq_validation_audit]
# MAGIC     
# MAGIC     LogPass1 --> FreshnessLoop
# MAGIC     LogFail1 --> FreshnessLoop
# MAGIC     
# MAGIC     FreshnessLoop -->|No more| QueryMeta2[Query metadata_catalog:<br/>Get column validation rules]
# MAGIC     
# MAGIC     QueryMeta2 --> QualityLoop{For each<br/>validation rule}
# MAGIC     
# MAGIC     QualityLoop -->|Yes| CheckRuleType{Rule Type?}
# MAGIC     
# MAGIC     CheckRuleType -->|NOT_NULL| BuildNullCheck[Build Query:<br/>COUNT nulls]
# MAGIC     CheckRuleType -->|POSITIVE| BuildPosCheck[Build Query:<br/>COUNT <= 0]
# MAGIC     CheckRuleType -->|NON_NEGATIVE| BuildNonNegCheck[Build Query:<br/>COUNT < 0]
# MAGIC     CheckRuleType -->|DATE_VALID| BuildDateCheck[Build Query:<br/>COUNT invalid dates]
# MAGIC     CheckRuleType -->|Other| SkipRule[Skip unsupported rule]
# MAGIC     
# MAGIC     BuildNullCheck --> ExecuteCheck[Execute validation query]
# MAGIC     BuildPosCheck --> ExecuteCheck
# MAGIC     BuildNonNegCheck --> ExecuteCheck
# MAGIC     BuildDateCheck --> ExecuteCheck
# MAGIC     
# MAGIC     ExecuteCheck --> CountViolations[Get:<br/>- total_records<br/>- violation_count]
# MAGIC     
# MAGIC     CountViolations --> CheckViolations{violations<br/>== 0?}
# MAGIC     
# MAGIC     CheckViolations -->|Yes| LogPass2[Log PASS:<br/>- pass_rate = 100%<br/>- metric details]
# MAGIC     CheckViolations -->|No| LogFail2[Log FAIL:<br/>- pass_rate < 100%<br/>- violation count]
# MAGIC     
# MAGIC     LogPass2 --> QualityLoop
# MAGIC     LogFail2 --> QualityLoop
# MAGIC     SkipRule --> QualityLoop
# MAGIC     
# MAGIC     QualityLoop -->|No more| QueryMeta3[Query metadata_catalog:<br/>Get active Silver/Gold tables]
# MAGIC     
# MAGIC     QueryMeta3 --> CompletenessLoop{For each<br/>active table}
# MAGIC     
# MAGIC     CompletenessLoop -->|Yes| CountRecords[Execute:<br/>SELECT COUNT(*) FROM table]
# MAGIC     
# MAGIC     CountRecords --> CheckCount{record_count<br/>> 0?}
# MAGIC     
# MAGIC     CheckCount -->|Yes| LogPass3[Log PASS:<br/>Table has data]
# MAGIC     CheckCount -->|No| LogFail3[Log FAIL:<br/>Table is empty]
# MAGIC     
# MAGIC     LogPass3 --> CompletenessLoop
# MAGIC     LogFail3 --> CompletenessLoop
# MAGIC     
# MAGIC     CompletenessLoop -->|No more| Summary[Query dq_validation_audit:<br/>Display results summary]
# MAGIC     
# MAGIC     Summary --> End([DQ Framework Complete])
# MAGIC     
# MAGIC     style Start fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style End fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style LogPass1 fill:#81c784,stroke:#388e3c
# MAGIC     style LogPass2 fill:#81c784,stroke:#388e3c
# MAGIC     style LogPass3 fill:#81c784,stroke:#388e3c
# MAGIC     style LogFail1 fill:#e57373,stroke:#c62828
# MAGIC     style LogFail2 fill:#e57373,stroke:#c62828
# MAGIC     style LogFail3 fill:#e57373,stroke:#c62828
# MAGIC     style QueryMeta1 fill:#fff3e0,stroke:#f57c00
# MAGIC     style QueryMeta2 fill:#fff3e0,stroke:#f57c00
# MAGIC     style QueryMeta3 fill:#fff3e0,stroke:#f57c00
# MAGIC ```
# MAGIC
# MAGIC ### DQ Check Types:
# MAGIC
# MAGIC | Check Category | Purpose | Logic | Pass Criteria |
# MAGIC |---|---|---|---|
# MAGIC | **Freshness** | SLA compliance | Compare table age vs SLA hours | age <= SLA |
# MAGIC | **Quality Rules** | Column validation | Count rule violations | violations = 0 |
# MAGIC | **Completeness** | Data availability | Count table records | count > 0 |
# MAGIC
# MAGIC ### Supported Validation Rules:
# MAGIC - `NOT_NULL`: Column must not contain NULL values
# MAGIC - `POSITIVE`: Numeric column must be > 0
# MAGIC - `NON_NEGATIVE`: Numeric column must be >= 0
# MAGIC - `DATE_VALID`: Date must be valid and not in future
# MAGIC - `EMAIL_FORMAT`: Email format validation (planned)

# COMMAND ----------

# DBTITLE 1,Maintenance Framework Flow
# MAGIC %md
# MAGIC ## ⚙️ Maintenance Framework Flow
# MAGIC
# MAGIC Automated table optimization and maintenance:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     Start([Start Maintenance Framework]) --> Init[Initialize:<br/>- Timestamp<br/>- User<br/>- Spark Session]
# MAGIC     
# MAGIC     Init --> DefineHelpers[Define Helper Functions:<br/>- log_maintenance_operation()<br/>- get_table_metrics()]
# MAGIC     
# MAGIC     DefineHelpers --> OptimizeFlow[OPTIMIZE Operations]
# MAGIC     
# MAGIC     subgraph OptimizeFlow ["🔧 OPTIMIZE Operations"]
# MAGIC         QueryOpt[Query metadata_catalog:<br/>Get optimize_frequency != NULL]
# MAGIC         
# MAGIC         QueryOpt --> OptLoop{For each table<br/>to optimize}
# MAGIC         
# MAGIC         OptLoop -->|Yes| GetBeforeMetrics1[Get Before Metrics:<br/>- File count<br/>- Total size]
# MAGIC         
# MAGIC         GetBeforeMetrics1 --> CheckZOrder{Has ZORDER<br/>columns?}
# MAGIC         
# MAGIC         CheckZOrder -->|Yes| OptWithZ[Execute:<br/>OPTIMIZE table<br/>ZORDER BY cols]
# MAGIC         CheckZOrder -->|No| OptNoZ[Execute:<br/>OPTIMIZE table]
# MAGIC         
# MAGIC         OptWithZ --> GetAfterMetrics1[Get After Metrics:<br/>- File count<br/>- Total size]
# MAGIC         OptNoZ --> GetAfterMetrics1
# MAGIC         
# MAGIC         GetAfterMetrics1 --> CalcImpact1[Calculate:<br/>- Files compacted<br/>- Size change]
# MAGIC         
# MAGIC         CalcImpact1 --> LogOpt[Log to maintenance_audit:<br/>Operation: OPTIMIZE<br/>Status: SUCCESS]
# MAGIC         
# MAGIC         LogOpt --> OptLoop
# MAGIC         
# MAGIC         OptLoop -->|No more| OptComplete[OPTIMIZE Complete]
# MAGIC     end
# MAGIC     
# MAGIC     OptimizeFlow --> VacuumFlow[VACUUM Operations]
# MAGIC     
# MAGIC     subgraph VacuumFlow ["🧹 VACUUM Operations"]
# MAGIC         QueryVac[Query metadata_catalog:<br/>Get vacuum_retention_hours]
# MAGIC         
# MAGIC         QueryVac --> VacLoop{For each table<br/>to vacuum}
# MAGIC         
# MAGIC         VacLoop -->|Yes| GetBeforeMetrics2[Get Before Metrics:<br/>- Total size]
# MAGIC         
# MAGIC         GetBeforeMetrics2 --> ExecVacuum[Execute:<br/>VACUUM table<br/>RETAIN hours HOURS]
# MAGIC         
# MAGIC         ExecVacuum --> GetAfterMetrics2[Get After Metrics:<br/>- Total size]
# MAGIC         
# MAGIC         GetAfterMetrics2 --> CalcSpace[Calculate:<br/>Space reclaimed (MB)]
# MAGIC         
# MAGIC         CalcSpace --> LogVac[Log to maintenance_audit:<br/>Operation: VACUUM<br/>Space saved]
# MAGIC         
# MAGIC         LogVac --> VacLoop
# MAGIC         
# MAGIC         VacLoop -->|No more| VacComplete[VACUUM Complete]
# MAGIC     end
# MAGIC     
# MAGIC     VacuumFlow --> AnalyzeFlow[ANALYZE Operations]
# MAGIC     
# MAGIC     subgraph AnalyzeFlow ["📊 ANALYZE Operations"]
# MAGIC         QueryAna[Query metadata_catalog:<br/>Get Silver/Gold tables]
# MAGIC         
# MAGIC         QueryAna --> AnaLoop{For each table<br/>to analyze}
# MAGIC         
# MAGIC         AnaLoop -->|Yes| ExecAnalyze[Execute:<br/>ANALYZE TABLE table<br/>COMPUTE STATISTICS]
# MAGIC         
# MAGIC         ExecAnalyze --> LogAna[Log to maintenance_audit:<br/>Operation: ANALYZE<br/>Status: SUCCESS]
# MAGIC         
# MAGIC         LogAna --> AnaLoop
# MAGIC         
# MAGIC         AnaLoop -->|No more| AnaComplete[ANALYZE Complete]
# MAGIC     end
# MAGIC     
# MAGIC     AnalyzeFlow --> Summary[Query maintenance_audit:<br/>Display operations summary]
# MAGIC     
# MAGIC     Summary --> End([Maintenance Complete])
# MAGIC     
# MAGIC     style Start fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style End fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style LogOpt fill:#81c784,stroke:#388e3c
# MAGIC     style LogVac fill:#81c784,stroke:#388e3c
# MAGIC     style LogAna fill:#81c784,stroke:#388e3c
# MAGIC     style QueryOpt fill:#fff3e0,stroke:#f57c00
# MAGIC     style QueryVac fill:#fff3e0,stroke:#f57c00
# MAGIC     style QueryAna fill:#fff3e0,stroke:#f57c00
# MAGIC ```
# MAGIC
# MAGIC ### Maintenance Operations:
# MAGIC
# MAGIC | Operation | Purpose | Frequency | Impact |
# MAGIC |---|---|---|---|
# MAGIC | **OPTIMIZE** | Compact small files, apply ZORDER | Daily/Weekly | Improves query performance |
# MAGIC | **VACUUM** | Remove old file versions | Weekly | Reclaims storage space |
# MAGIC | **ANALYZE** | Update table statistics | After OPTIMIZE | Improves query planning |
# MAGIC
# MAGIC ### Performance Benefits:
# MAGIC - **File Compaction**: Reduces small file overhead, improves read performance
# MAGIC - **ZORDER**: Co-locates data for common query patterns (date, customer_id, etc.)
# MAGIC - **Space Reclaim**: Removes old versions after retention period
# MAGIC - **Statistics**: Helps query optimizer choose efficient execution plans

# COMMAND ----------

# DBTITLE 1,Audit Tables Schema
# MAGIC %md
# MAGIC ## 🗄️ Audit Tables Schema
# MAGIC
# MAGIC Complete schema definitions for all audit tables:
# MAGIC
# MAGIC ```mermaid
# MAGIC erDiagram
# MAGIC     metadata_catalog ||--o{ dq_validation_audit : "defines checks for"
# MAGIC     metadata_catalog ||--o{ maintenance_audit : "defines policies for"
# MAGIC     metadata_catalog {
# MAGIC         bigint catalog_id PK
# MAGIC         string catalog_name
# MAGIC         string schema_name
# MAGIC         string table_name
# MAGIC         string full_table_name
# MAGIC         string layer
# MAGIC         string column_name
# MAGIC         string validation_rule
# MAGIC         string rule_description
# MAGIC         boolean dq_checks_enabled
# MAGIC         string refresh_frequency
# MAGIC         int sla_hours
# MAGIC         string optimize_frequency
# MAGIC         array zorder_columns
# MAGIC         int vacuum_retention_hours
# MAGIC         boolean is_active
# MAGIC     }
# MAGIC     
# MAGIC     dq_validation_audit {
# MAGIC         bigint validation_id PK
# MAGIC         string check_category
# MAGIC         string check_name
# MAGIC         string table_name
# MAGIC         string column_name
# MAGIC         timestamp run_timestamp
# MAGIC         string status
# MAGIC         bigint violation_count
# MAGIC         bigint total_records
# MAGIC         decimal pass_rate
# MAGIC         string metric
# MAGIC         string message
# MAGIC         date run_date
# MAGIC     }
# MAGIC     
# MAGIC     etl_job_audit {
# MAGIC         bigint job_run_id PK
# MAGIC         string job_name
# MAGIC         string pipeline_name
# MAGIC         string layer
# MAGIC         timestamp start_timestamp
# MAGIC         timestamp end_timestamp
# MAGIC         decimal duration_seconds
# MAGIC         string status
# MAGIC         bigint rows_read
# MAGIC         bigint rows_written
# MAGIC         string error_message
# MAGIC         date run_date
# MAGIC     }
# MAGIC     
# MAGIC     maintenance_audit {
# MAGIC         bigint maintenance_id PK
# MAGIC         string operation_type
# MAGIC         string table_name
# MAGIC         timestamp operation_timestamp
# MAGIC         string status
# MAGIC         int files_before
# MAGIC         int files_after
# MAGIC         bigint size_before_bytes
# MAGIC         bigint size_after_bytes
# MAGIC         decimal space_saved_mb
# MAGIC         array zorder_columns
# MAGIC         int vacuum_retention_hours
# MAGIC         date run_date
# MAGIC     }
# MAGIC     
# MAGIC     table_change_audit {
# MAGIC         bigint change_id PK
# MAGIC         string table_name
# MAGIC         timestamp change_timestamp
# MAGIC         string change_type
# MAGIC         bigint rows_inserted
# MAGIC         bigint rows_updated
# MAGIC         bigint rows_deleted
# MAGIC         string source_system
# MAGIC         string job_name
# MAGIC         date change_date
# MAGIC     }
# MAGIC ```
# MAGIC
# MAGIC ### Key Features:
# MAGIC
# MAGIC #### 1. **metadata_catalog** - Central Configuration
# MAGIC - Stores table & column metadata
# MAGIC - Defines validation rules
# MAGIC - Configures maintenance policies
# MAGIC - Drives all framework operations
# MAGIC
# MAGIC #### 2. **dq_validation_audit** - Quality Tracking
# MAGIC - Logs all DQ check results
# MAGIC - Tracks pass/fail rates over time
# MAGIC - Enables trend analysis
# MAGIC - Supports alerting on failures
# MAGIC
# MAGIC #### 3. **etl_job_audit** - Pipeline Monitoring
# MAGIC - Records ETL job executions
# MAGIC - Tracks performance metrics
# MAGIC - Captures errors for debugging
# MAGIC - Enables SLA monitoring
# MAGIC
# MAGIC #### 4. **maintenance_audit** - Operations Log
# MAGIC - Documents optimization operations
# MAGIC - Measures effectiveness (files, space)
# MAGIC - Tracks ZORDER configurations
# MAGIC - Supports cost analysis
# MAGIC
# MAGIC #### 5. **table_change_audit** - Data Lineage
# MAGIC - Tracks data changes (I/U/D)
# MAGIC - Links to source systems
# MAGIC - Captures job provenance
# MAGIC - Enables impact analysis

# COMMAND ----------

# DBTITLE 1,Reporting and Monitoring
# MAGIC %md
# MAGIC ## 📈 Reporting and Monitoring
# MAGIC
# MAGIC Framework provides comprehensive monitoring dashboards:
# MAGIC
# MAGIC ```mermaid
# MAGIC graph LR
# MAGIC     subgraph "Audit Data"
# MAGIC         DQAudit[(dq_validation_audit)]
# MAGIC         ETLAudit[(etl_job_audit)]
# MAGIC         MaintAudit[(maintenance_audit)]
# MAGIC         ChangeAudit[(table_change_audit)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "DQ Monitoring Reports"
# MAGIC         DQAudit --> PassRate[Pass Rate by Category]
# MAGIC         DQAudit --> RecentFails[Recent Failures]
# MAGIC         DQAudit --> Trends[30-Day Trends]
# MAGIC         DQAudit --> TableHealth[Table Health Summary]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "ETL Monitoring Reports"
# MAGIC         ETLAudit --> JobSuccess[Success Rate by Layer]
# MAGIC         ETLAudit --> ETLFails[Recent ETL Failures]
# MAGIC         ETLAudit --> PerfTrends[Performance Trends]
# MAGIC         ETLAudit --> SLAStatus[SLA Compliance]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "Maintenance Reports"
# MAGIC         MaintAudit --> OpsSummary[Operations Summary]
# MAGIC         MaintAudit --> Effectiveness[Effectiveness by Table]
# MAGIC         MaintAudit --> SpaceSaved[Space Reclaimed]
# MAGIC         MaintAudit --> RecentOps[Recent Operations]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "Change Tracking"
# MAGIC         ChangeAudit --> DataChanges[Data Change Summary]
# MAGIC         ChangeAudit --> HighImpact[High-Impact Changes]
# MAGIC         ChangeAudit --> Lineage[Source System Lineage]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "Executive Dashboard"
# MAGIC         PassRate --> ExecHealth[Overall Health]
# MAGIC         JobSuccess --> ExecHealth
# MAGIC         OpsSummary --> ExecHealth
# MAGIC         Trends --> ExecHealth
# MAGIC         
# MAGIC         ExecHealth --> Metrics[Key Metrics:<br/>- DQ Pass Rate<br/>- ETL Success Rate<br/>- Maintenance Success<br/>- Data Freshness]
# MAGIC     end
# MAGIC     
# MAGIC     style DQAudit fill:#fff3e0,stroke:#f57c00
# MAGIC     style ETLAudit fill:#fff3e0,stroke:#f57c00
# MAGIC     style MaintAudit fill:#fff3e0,stroke:#f57c00
# MAGIC     style ChangeAudit fill:#fff3e0,stroke:#f57c00
# MAGIC     style ExecHealth fill:#e8f5e9,stroke:#388e3c,stroke-width:3px
# MAGIC     style Metrics fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
# MAGIC ```
# MAGIC
# MAGIC ### Available Reports:
# MAGIC
# MAGIC #### 📊 DQ Monitoring
# MAGIC 1. **Pass Rate by Category** - Freshness, Quality Rules, Completeness success rates
# MAGIC 2. **Recent Failures** - Latest failed checks with details
# MAGIC 3. **30-Day Trends** - Historical pass rates by table
# MAGIC 4. **Table Health Summary** - Overall quality score per table
# MAGIC
# MAGIC #### 🔄 ETL Monitoring
# MAGIC 1. **Success Rate by Layer** - Bronze/Silver/Gold pipeline reliability
# MAGIC 2. **Recent ETL Failures** - Failed jobs with error messages
# MAGIC 3. **Performance Trends** - Duration and throughput over time
# MAGIC 4. **SLA Compliance** - Tables meeting freshness requirements
# MAGIC
# MAGIC #### ⚙️ Maintenance Monitoring
# MAGIC 1. **Operations Summary** - OPTIMIZE/VACUUM/ANALYZE counts
# MAGIC 2. **Effectiveness by Table** - Files compacted, space saved
# MAGIC 3. **Space Reclaimed** - Storage cost savings
# MAGIC 4. **Recent Operations** - Latest maintenance activities
# MAGIC
# MAGIC #### 🔍 Change Tracking
# MAGIC 1. **Data Change Summary** - Insert/Update/Delete counts
# MAGIC 2. **High-Impact Changes** - Large data modifications
# MAGIC 3. **Source System Lineage** - Origin of data changes
# MAGIC
# MAGIC #### 🎯 Executive Dashboard
# MAGIC - **Overall DQ Pass Rate** - % of checks passing
# MAGIC - **ETL Success Rate** - % of jobs completing
# MAGIC - **Maintenance Success Rate** - % of operations successful
# MAGIC - **Average Data Freshness** - Hours behind SLA

# COMMAND ----------

# DBTITLE 1,Framework Notebooks
# MAGIC %md
# MAGIC ## 📚 Framework Notebooks
# MAGIC
# MAGIC Complete list of notebooks in the framework:
# MAGIC
# MAGIC ### Setup Phase
# MAGIC
# MAGIC #### 1. **01_Setup_Audit_Tables**
# MAGIC - **Purpose**: Create audit infrastructure
# MAGIC - **Creates**: 5 audit tables in `retailpulse.ops` schema
# MAGIC - **Run Once**: Initial setup only
# MAGIC - **Tables**:
# MAGIC   - `metadata_catalog` - Central configuration registry
# MAGIC   - `dq_validation_audit` - Quality check logs
# MAGIC   - `etl_job_audit` - Pipeline execution logs
# MAGIC   - `maintenance_audit` - Operations logs
# MAGIC   - `table_change_audit` - Data lineage tracking
# MAGIC
# MAGIC #### 2. **02_Metadata_Configuration**
# MAGIC - **Purpose**: Populate metadata catalog
# MAGIC - **Run Once**: Initial configuration
# MAGIC - **Updates**: As new tables/rules are added
# MAGIC - **Configures**:
# MAGIC   - Table definitions (6 tables across Bronze/Silver/Gold)
# MAGIC   - Column metadata (14 columns with validation rules)
# MAGIC   - DQ policies (SLA, validation rules)
# MAGIC   - Maintenance policies (OPTIMIZE, VACUUM, ZORDER)
# MAGIC
# MAGIC ### Operational Phase
# MAGIC
# MAGIC #### 3. **03_DQ_Framework** ⭐
# MAGIC - **Purpose**: Execute data quality validations
# MAGIC - **Run**: Daily or on-demand
# MAGIC - **Schedule**: Recommended daily after ETL completes
# MAGIC - **Checks**:
# MAGIC   - Freshness (SLA compliance)
# MAGIC   - Quality Rules (column validations)
# MAGIC   - Completeness (row counts)
# MAGIC - **Outputs**: Results logged to `dq_validation_audit`
# MAGIC
# MAGIC #### 4. **04_Maintenance_Framework** ⭐
# MAGIC - **Purpose**: Automated table maintenance
# MAGIC - **Run**: Weekly or on-demand
# MAGIC - **Schedule**: Recommended weekly during off-peak hours
# MAGIC - **Operations**:
# MAGIC   - OPTIMIZE (with ZORDER)
# MAGIC   - VACUUM (space reclamation)
# MAGIC   - ANALYZE (statistics collection)
# MAGIC - **Outputs**: Logs to `maintenance_audit`
# MAGIC
# MAGIC #### 5. **05_Audit_Reporting** 📊
# MAGIC - **Purpose**: Monitor framework health
# MAGIC - **Run**: Ad-hoc for analysis
# MAGIC - **Use Cases**:
# MAGIC   - Daily health checks
# MAGIC   - Incident investigation
# MAGIC   - Trend analysis
# MAGIC   - Executive reporting
# MAGIC - **Reports**: 20+ pre-built queries
# MAGIC
# MAGIC ### Execution Flow
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     Setup1[01_Setup_Audit_Tables] --> Setup2[02_Metadata_Configuration]
# MAGIC     
# MAGIC     Setup2 --> Ops[Operational Phase]
# MAGIC     
# MAGIC     Ops --> DQ[03_DQ_Framework<br/>Daily]
# MAGIC     Ops --> Maint[04_Maintenance_Framework<br/>Weekly]
# MAGIC     Ops --> Report[05_Audit_Reporting<br/>Ad-hoc]
# MAGIC     
# MAGIC     DQ --> Monitor{Issues?}
# MAGIC     Maint --> Monitor
# MAGIC     
# MAGIC     Monitor -->|Yes| Investigate[Use Reporting<br/>to Investigate]
# MAGIC     Monitor -->|No| Continue[Continue Operations]
# MAGIC     
# MAGIC     Investigate --> Fix[Fix Issues]
# MAGIC     Fix --> DQ
# MAGIC     
# MAGIC     style Setup1 fill:#fff3e0,stroke:#f57c00
# MAGIC     style Setup2 fill:#fff3e0,stroke:#f57c00
# MAGIC     style DQ fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
# MAGIC     style Maint fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
# MAGIC     style Report fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Implementation Summary
# MAGIC %md
# MAGIC ## ✅ Implementation Summary
# MAGIC
# MAGIC ### What Was Built
# MAGIC
# MAGIC #### Infrastructure Layer
# MAGIC ✅ **5 Audit Tables** in `retailpulse.ops`
# MAGIC - Comprehensive schema with generated columns
# MAGIC - Partitioned by date for performance
# MAGIC - Includes metadata, DQ, ETL, maintenance, and change tracking
# MAGIC
# MAGIC #### Configuration Layer
# MAGIC ✅ **Metadata Catalog Populated**
# MAGIC - 6 tables configured (1 Bronze, 1 Silver, 4 Gold)
# MAGIC - 14 columns with validation rules
# MAGIC - Maintenance policies (OPTIMIZE frequency, ZORDER columns, VACUUM retention)
# MAGIC - SLA definitions (freshness requirements)
# MAGIC
# MAGIC #### Operational Layer
# MAGIC ✅ **DQ Framework** (03_DQ_Framework)
# MAGIC - 3 check types: Freshness, Quality Rules, Completeness
# MAGIC - Metadata-driven (reads from catalog)
# MAGIC - Comprehensive logging
# MAGIC - 13/13 checks currently passing ✓
# MAGIC
# MAGIC ✅ **Maintenance Framework** (04_Maintenance_Framework)
# MAGIC - OPTIMIZE with ZORDER support
# MAGIC - VACUUM with configurable retention
# MAGIC - ANALYZE for statistics
# MAGIC - Before/after metrics tracking
# MAGIC
# MAGIC ✅ **Audit Reporting** (05_Audit_Reporting)
# MAGIC - 20+ monitoring queries
# MAGIC - DQ, ETL, and maintenance dashboards
# MAGIC - Trend analysis (30-day)
# MAGIC - Executive summary metrics
# MAGIC
# MAGIC ### Current Status
# MAGIC
# MAGIC | Component | Status | Details |
# MAGIC |---|---|---|
# MAGIC | **Audit Tables** | ✅ Ready | All 5 tables created in retailpulse.ops |
# MAGIC | **Metadata Config** | ✅ Ready | 6 tables, 14 columns configured |
# MAGIC | **DQ Framework** | ✅ Operational | 13/13 checks passing |
# MAGIC | **Maintenance** | ✅ Ready | Awaiting scheduled execution |
# MAGIC | **Reporting** | ✅ Ready | Reports available for queries |
# MAGIC | **Data Population** | ✅ Complete | Orchestrator job populated all tables |
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC 🎯 **Metadata-Driven**
# MAGIC - Single source of truth in `metadata_catalog`
# MAGIC - Easy to add new tables/rules without code changes
# MAGIC - Configuration-over-code approach
# MAGIC
# MAGIC 📊 **Comprehensive Logging**
# MAGIC - Every operation logged to audit tables
# MAGIC - Full traceability and auditability
# MAGIC - Historical trend analysis
# MAGIC
# MAGIC ⚡ **Automated & Scheduled**
# MAGIC - Can be orchestrated via Databricks Jobs
# MAGIC - Runs without manual intervention
# MAGIC - Configurable schedules (daily DQ, weekly maintenance)
# MAGIC
# MAGIC 🔍 **Proactive Monitoring**
# MAGIC - Real-time health visibility
# MAGIC - Automatic alerting on failures
# MAGIC - Executive dashboards
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Schedule Jobs**
# MAGIC    - Create job for daily DQ checks (03_DQ_Framework)
# MAGIC    - Create job for weekly maintenance (04_Maintenance_Framework)
# MAGIC    - Add notifications for failures
# MAGIC
# MAGIC 2. **Expand Coverage**
# MAGIC    - Add more tables to metadata_catalog
# MAGIC    - Define additional validation rules
# MAGIC    - Implement custom check types
# MAGIC
# MAGIC 3. **Integrate Alerting**
# MAGIC    - Configure alerts on DQ failures
# MAGIC    - Set up Slack/email notifications
# MAGIC    - Create PagerDuty integration for critical issues
# MAGIC
# MAGIC 4. **Build Dashboards**
# MAGIC    - Create Databricks SQL dashboards from reporting queries
# MAGIC    - Schedule automated report distribution
# MAGIC    - Set up executive summary emails
# MAGIC
# MAGIC ### Benefits Delivered
# MAGIC
# MAGIC ✅ **Data Quality Assurance**
# MAGIC - Automated validation of all critical tables
# MAGIC - Early detection of data issues
# MAGIC - Trend analysis to identify degradation
# MAGIC
# MAGIC ✅ **Operational Efficiency**
# MAGIC - Automated maintenance operations
# MAGIC - Optimized query performance (OPTIMIZE + ZORDER)
# MAGIC - Reduced storage costs (VACUUM)
# MAGIC
# MAGIC ✅ **Visibility & Control**
# MAGIC - Complete audit trail
# MAGIC - Real-time monitoring
# MAGIC - Historical analysis
# MAGIC
# MAGIC ✅ **Scalability**
# MAGIC - Easy to add new tables
# MAGIC - Metadata-driven approach scales
# MAGIC - No code changes for new rules

# COMMAND ----------

# DBTITLE 1,Execution Workflow
# MAGIC %md
# MAGIC ## 🚀 Execution Workflow
# MAGIC
# MAGIC Step-by-step workflow for running the framework:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     Start([Framework Start]) --> OneTime{First Time<br/>Setup?}
# MAGIC     
# MAGIC     OneTime -->|Yes| Setup1[Run: 01_Setup_Audit_Tables<br/>Create 5 audit tables]
# MAGIC     OneTime -->|No| CheckData
# MAGIC     
# MAGIC     Setup1 --> Setup2[Run: 02_Metadata_Configuration<br/>Populate metadata_catalog]
# MAGIC     
# MAGIC     Setup2 --> CheckData{Tables<br/>Have Data?}
# MAGIC     
# MAGIC     CheckData -->|No| RunOrch[Run: RetailPulse Enterprise<br/>Orchestrator Job]
# MAGIC     CheckData -->|Yes| RunDQ
# MAGIC     
# MAGIC     RunOrch --> Populate[Populate Bronze/Silver/Gold<br/>via DLT Pipelines]
# MAGIC     
# MAGIC     Populate --> LogETL[Log to etl_job_audit<br/>Track pipeline runs]
# MAGIC     
# MAGIC     LogETL --> RunDQ[Run: 03_DQ_Framework]
# MAGIC     
# MAGIC     RunDQ --> DQCell1[Cell 1: Setup & Imports]
# MAGIC     DQCell1 --> DQCell2[Cell 2: Helper Function]
# MAGIC     DQCell2 --> DQCell3[Cell 3: Freshness Checks]
# MAGIC     DQCell3 --> DQCell4[Cell 4: Quality Rules]
# MAGIC     DQCell4 --> DQCell5[Cell 5: Completeness]
# MAGIC     DQCell5 --> DQCell6[Cell 6: View Results]
# MAGIC     
# MAGIC     DQCell6 --> DQPass{All Checks<br/>Passed?}
# MAGIC     
# MAGIC     DQPass -->|Yes| RunMaint[Run: 04_Maintenance_Framework]
# MAGIC     DQPass -->|No| Alert1[Alert: DQ Failures<br/>Investigate Issues]
# MAGIC     
# MAGIC     Alert1 --> FixIssues[Fix Data Issues<br/>Re-run Orchestrator]
# MAGIC     FixIssues --> RunDQ
# MAGIC     
# MAGIC     RunMaint --> MaintCell1[Cell 1: Setup]
# MAGIC     MaintCell1 --> MaintCell2[Cell 2: Helper Functions]
# MAGIC     MaintCell2 --> MaintCell3[Cell 3-4: OPTIMIZE]
# MAGIC     MaintCell3 --> MaintCell4[Cell 5-6: VACUUM]
# MAGIC     MaintCell4 --> MaintCell5[Cell 7-8: ANALYZE]
# MAGIC     MaintCell5 --> MaintCell6[Cell 9: View Results]
# MAGIC     
# MAGIC     MaintCell6 --> ViewReports[Run: 05_Audit_Reporting<br/>Check Framework Health]
# MAGIC     
# MAGIC     ViewReports --> Report1[DQ Monitoring:<br/>- Pass rates<br/>- Failures<br/>- Trends]
# MAGIC     ViewReports --> Report2[ETL Monitoring:<br/>- Success rates<br/>- Performance<br/>- Errors]
# MAGIC     ViewReports --> Report3[Maintenance Stats:<br/>- Operations<br/>- Effectiveness<br/>- Space saved]
# MAGIC     
# MAGIC     Report1 --> ExecDash[Executive Dashboard:<br/>Overall Health Metrics]
# MAGIC     Report2 --> ExecDash
# MAGIC     Report3 --> ExecDash
# MAGIC     
# MAGIC     ExecDash --> Schedule{Schedule<br/>Automation?}
# MAGIC     
# MAGIC     Schedule -->|Yes| CreateJobs[Create Databricks Jobs:<br/>- Daily: DQ Framework<br/>- Weekly: Maintenance<br/>- On-demand: Reports]
# MAGIC     
# MAGIC     Schedule -->|No| ManualRun[Manual Execution<br/>as Needed]
# MAGIC     
# MAGIC     CreateJobs --> Monitor[Continuous Monitoring<br/>via Audit Reports]
# MAGIC     ManualRun --> Monitor
# MAGIC     
# MAGIC     Monitor --> End([Framework Operational])
# MAGIC     
# MAGIC     style Start fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style End fill:#4caf50,stroke:#2e7d32,color:#fff
# MAGIC     style Setup1 fill:#fff3e0,stroke:#f57c00
# MAGIC     style Setup2 fill:#fff3e0,stroke:#f57c00
# MAGIC     style RunOrch fill:#fce4ec,stroke:#c2185b
# MAGIC     style RunDQ fill:#f3e5f5,stroke:#7b1fa2
# MAGIC     style RunMaint fill:#f3e5f5,stroke:#7b1fa2
# MAGIC     style ViewReports fill:#e8f5e9,stroke:#388e3c
# MAGIC     style Alert1 fill:#ffebee,stroke:#c62828
# MAGIC     style ExecDash fill:#e3f2fd,stroke:#1976d2,stroke-width:3px
# MAGIC ```
# MAGIC
# MAGIC ### Execution Sequence:
# MAGIC
# MAGIC #### 🔧 One-Time Setup (First Run Only)
# MAGIC 1. **01_Setup_Audit_Tables** - Create audit infrastructure
# MAGIC 2. **02_Metadata_Configuration** - Define tables and rules
# MAGIC 3. **RetailPulse Enterprise Orchestrator** - Populate data
# MAGIC
# MAGIC #### 🔄 Regular Operations (Daily/Weekly)
# MAGIC 1. **03_DQ_Framework** - Validate data quality
# MAGIC 2. **04_Maintenance_Framework** - Optimize tables
# MAGIC 3. **05_Audit_Reporting** - Review health metrics
# MAGIC
# MAGIC #### ⚡ Skip These Cells in Regular Runs:
# MAGIC - **03_DQ_Framework, Cell 10**: "Fix Metadata Catalog" (one-time correction, already executed)

# COMMAND ----------

# DBTITLE 1,Data Lineage and Flow
# MAGIC %md
# MAGIC ## 🔄 Data Lineage and Flow
# MAGIC
# MAGIC Complete data flow through the RetailPulse framework:
# MAGIC
# MAGIC ```mermaid
# MAGIC graph TB
# MAGIC     subgraph "📥 Source Systems"
# MAGIC         CloudFiles[Cloud Files<br/>Auto Loader]
# MAGIC         ExternalAPI[External APIs]
# MAGIC         OtherSources[Other Sources]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🥉 Bronze Layer (Raw)"
# MAGIC         OrdersRaw[(orders_raw<br/>Streaming Table)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🥈 Silver Layer (Validated)"
# MAGIC         Orders[(orders<br/>Cleaned & Validated)]
# MAGIC         Products[(products<br/>Reference Data)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🥇 Gold Layer (Curated)"
# MAGIC         FactSales[(fact_sales<br/>Sales Transactions)]
# MAGIC         DimCustomer[(dim_customer<br/>Customer SCD Type 2)]
# MAGIC         DimProduct[(dim_product<br/>Product SCD Type 2)]
# MAGIC         DimDate[(dim_date<br/>Date Dimension)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "🔍 DQ Framework"
# MAGIC         FreshCheck[Freshness Checks<br/>SLA: 2-24hrs]
# MAGIC         QualityCheck[Quality Rules<br/>13 validations]
# MAGIC         CompleteCheck[Completeness<br/>Row counts]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "⚙️ Maintenance"
# MAGIC         Optimize[OPTIMIZE<br/>ZORDER by date/ID]
# MAGIC         Vacuum[VACUUM<br/>168 hrs retention]
# MAGIC         Analyze[ANALYZE<br/>Table statistics]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "📊 Audit & Monitoring"
# MAGIC         MetaCat[(metadata_catalog)]
# MAGIC         DQAudit[(dq_validation_audit)]
# MAGIC         ETLAudit[(etl_job_audit)]
# MAGIC         MaintAudit[(maintenance_audit)]
# MAGIC         ChangeAudit[(table_change_audit)]
# MAGIC     end
# MAGIC     
# MAGIC     subgraph "📈 Consumers"
# MAGIC         Dashboards[BI Dashboards]
# MAGIC         Analytics[Data Analytics]
# MAGIC         ML[ML Models]
# MAGIC         Reports[Operational Reports]
# MAGIC     end
# MAGIC     
# MAGIC     %% Data Flow
# MAGIC     CloudFiles -->|Streaming| OrdersRaw
# MAGIC     ExternalAPI -->|Batch| OrdersRaw
# MAGIC     OtherSources -->|Incremental| OrdersRaw
# MAGIC     
# MAGIC     OrdersRaw -->|DLT Transform| Orders
# MAGIC     OrdersRaw -->|DLT Transform| Products
# MAGIC     
# MAGIC     Orders -->|Aggregate| FactSales
# MAGIC     Orders -->|Extract| DimCustomer
# MAGIC     Products -->|SCD Type 2| DimProduct
# MAGIC     Orders -->|Generate| DimDate
# MAGIC     
# MAGIC     %% DQ Checks
# MAGIC     Orders -.->|Validate| FreshCheck
# MAGIC     FactSales -.->|Validate| FreshCheck
# MAGIC     FactSales -.->|Validate| QualityCheck
# MAGIC     DimCustomer -.->|Validate| QualityCheck
# MAGIC     DimProduct -.->|Validate| QualityCheck
# MAGIC     FactSales -.->|Validate| CompleteCheck
# MAGIC     
# MAGIC     %% Maintenance
# MAGIC     OrdersRaw -.->|Daily| Optimize
# MAGIC     Orders -.->|Daily| Optimize
# MAGIC     FactSales -.->|Daily| Optimize
# MAGIC     DimCustomer -.->|Weekly| Optimize
# MAGIC     DimProduct -.->|Weekly| Optimize
# MAGIC     
# MAGIC     Orders -.->|Weekly| Vacuum
# MAGIC     FactSales -.->|Weekly| Vacuum
# MAGIC     
# MAGIC     Orders -.->|Post-Optimize| Analyze
# MAGIC     FactSales -.->|Post-Optimize| Analyze
# MAGIC     
# MAGIC     %% Metadata Flow
# MAGIC     MetaCat -->|Defines| OrdersRaw
# MAGIC     MetaCat -->|Defines| Orders
# MAGIC     MetaCat -->|Defines| FactSales
# MAGIC     MetaCat -->|Defines| DimCustomer
# MAGIC     MetaCat -->|Defines| DimProduct
# MAGIC     MetaCat -->|Drives| FreshCheck
# MAGIC     MetaCat -->|Drives| QualityCheck
# MAGIC     MetaCat -->|Drives| Optimize
# MAGIC     MetaCat -->|Drives| Vacuum
# MAGIC     
# MAGIC     %% Audit Logging
# MAGIC     FreshCheck -->|Logs| DQAudit
# MAGIC     QualityCheck -->|Logs| DQAudit
# MAGIC     CompleteCheck -->|Logs| DQAudit
# MAGIC     OrdersRaw -->|Logs Changes| ETLAudit
# MAGIC     Orders -->|Logs Changes| ETLAudit
# MAGIC     FactSales -->|Logs Changes| ETLAudit
# MAGIC     Optimize -->|Logs| MaintAudit
# MAGIC     Vacuum -->|Logs| MaintAudit
# MAGIC     Analyze -->|Logs| MaintAudit
# MAGIC     FactSales -->|Tracks| ChangeAudit
# MAGIC     
# MAGIC     %% Consumption
# MAGIC     FactSales -->|Query| Dashboards
# MAGIC     DimCustomer -->|Query| Analytics
# MAGIC     DimProduct -->|Query| ML
# MAGIC     DimDate -->|Query| Reports
# MAGIC     DQAudit -->|Monitor| Dashboards
# MAGIC     
# MAGIC     classDef source fill:#e3f2fd,stroke:#1976d2
# MAGIC     classDef bronze fill:#fff3e0,stroke:#f57c00
# MAGIC     classDef silver fill:#f3e5f5,stroke:#7b1fa2
# MAGIC     classDef gold fill:#fff9c4,stroke:#f9a825
# MAGIC     classDef dq fill:#ffebee,stroke:#c62828
# MAGIC     classDef maint fill:#e8f5e9,stroke:#388e3c
# MAGIC     classDef audit fill:#fce4ec,stroke:#c2185b
# MAGIC     classDef consume fill:#e0f2f1,stroke:#00796b
# MAGIC     
# MAGIC     class CloudFiles,ExternalAPI,OtherSources source
# MAGIC     class OrdersRaw bronze
# MAGIC     class Orders,Products silver
# MAGIC     class FactSales,DimCustomer,DimProduct,DimDate gold
# MAGIC     class FreshCheck,QualityCheck,CompleteCheck dq
# MAGIC     class Optimize,Vacuum,Analyze maint
# MAGIC     class MetaCat,DQAudit,ETLAudit,MaintAudit,ChangeAudit audit
# MAGIC     class Dashboards,Analytics,ML,Reports consume
# MAGIC ```
# MAGIC
# MAGIC ### Data Flow Summary:
# MAGIC
# MAGIC #### Layer Progression:
# MAGIC 1. **Bronze (Raw)** ← Streaming ingestion from cloud files
# MAGIC 2. **Silver (Validated)** ← DLT transformations with quality checks
# MAGIC 3. **Gold (Curated)** ← Business-level aggregations and dimensions
# MAGIC 4. **Consumers** ← Dashboards, analytics, ML models
# MAGIC
# MAGIC #### Quality Gates:
# MAGIC - **Freshness**: All Silver/Gold tables checked against SLA
# MAGIC - **Quality Rules**: 13 column-level validations
# MAGIC - **Completeness**: Row count verification
# MAGIC
# MAGIC #### Optimization:
# MAGIC - **Daily**: Bronze/Silver/Gold fact tables (OPTIMIZE + ZORDER)
# MAGIC - **Weekly**: Dimension tables (OPTIMIZE + VACUUM)
# MAGIC - **Post-Optimize**: Statistics collection (ANALYZE)

# COMMAND ----------

# DBTITLE 1,Operational Cadence
# MAGIC %md
# MAGIC ## ⏰ Operational Cadence
# MAGIC
# MAGIC Recommended scheduling for framework operations:
# MAGIC
# MAGIC ```mermaid
# MAGIC gantt
# MAGIC     title Framework Operations Schedule
# MAGIC     dateFormat HH:mm
# MAGIC     axisFormat %H:%M
# MAGIC     
# MAGIC     section Data Ingestion
# MAGIC     Orchestrator Job (Every 4hrs)    :done, orch1, 00:00, 15m
# MAGIC     Orchestrator Job                  :done, orch2, 04:00, 15m
# MAGIC     Orchestrator Job                  :done, orch3, 08:00, 15m
# MAGIC     Orchestrator Job                  :done, orch4, 12:00, 15m
# MAGIC     Orchestrator Job                  :done, orch5, 16:00, 15m
# MAGIC     Orchestrator Job                  :done, orch6, 20:00, 15m
# MAGIC     
# MAGIC     section DQ Validation
# MAGIC     DQ Framework (Daily 06:00)        :active, dq1, 06:00, 30m
# MAGIC     DQ Framework (Daily 18:00)        :active, dq2, 18:00, 30m
# MAGIC     
# MAGIC     section Maintenance
# MAGIC     Daily OPTIMIZE (01:00)            :crit, opt1, 01:00, 1h
# MAGIC     Weekly VACUUM (Sunday 02:30)      :crit, vac1, 02:30, 45m
# MAGIC     
# MAGIC     section Reporting
# MAGIC     Hourly Health Check               :done, rpt1, 07:00, 5m
# MAGIC     Hourly Health Check               :done, rpt2, 08:00, 5m
# MAGIC     Hourly Health Check               :done, rpt3, 09:00, 5m
# MAGIC     Daily Executive Report (09:00)    :milestone, exec1, 09:00, 0m
# MAGIC ```
# MAGIC
# MAGIC ### Recommended Schedules:
# MAGIC
# MAGIC #### 🔄 **Data Ingestion** (Continuous)
# MAGIC | Job | Frequency | Time | Duration | Purpose |
# MAGIC |-----|-----------|------|----------|----------|
# MAGIC | **Orchestrator** | Every 4 hours | 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 | ~15 min | Refresh Bronze → Silver → Gold |
# MAGIC | **Streaming** | Continuous | 24/7 | Real-time | Auto Loader to Bronze |
# MAGIC
# MAGIC #### 🔍 **Data Quality** (Daily)
# MAGIC | Job | Frequency | Time | Duration | Purpose |
# MAGIC |-----|-----------|------|----------|----------|
# MAGIC | **DQ Framework** | Twice daily | 06:00, 18:00 | ~30 min | Validate freshness, quality, completeness |
# MAGIC | **DQ Alerts** | After DQ runs | 06:35, 18:35 | Immediate | Email/Slack failures |
# MAGIC
# MAGIC #### ⚙️ **Maintenance** (Daily/Weekly)
# MAGIC | Operation | Frequency | Time | Duration | Purpose |
# MAGIC |-----------|-----------|------|----------|----------|
# MAGIC | **OPTIMIZE** | Daily | 01:00 | ~1 hour | Compact files, apply ZORDER |
# MAGIC | **VACUUM** | Weekly (Sunday) | 02:30 | ~45 min | Reclaim storage space |
# MAGIC | **ANALYZE** | After OPTIMIZE | 02:15 | ~10 min | Update table statistics |
# MAGIC
# MAGIC #### 📊 **Reporting** (Hourly/Daily)
# MAGIC | Report | Frequency | Time | Duration | Purpose |
# MAGIC |--------|-----------|------|----------|----------|
# MAGIC | **Health Check** | Hourly | :00 | ~5 min | Quick status check |
# MAGIC | **Executive Dashboard** | Daily | 09:00 | ~10 min | Email summary to leadership |
# MAGIC | **Weekly Summary** | Weekly (Monday) | 09:00 | ~15 min | Detailed trends report |
# MAGIC
# MAGIC ### Cron Expressions for Scheduling:
# MAGIC
# MAGIC ```python
# MAGIC # Data Ingestion
# MAGIC Orchestrator: "0 0 0,4,8,12,16,20 * * ? *"  # Every 4 hours
# MAGIC
# MAGIC # DQ Validation
# MAGIC DQ_Framework: "0 0 6,18 * * ? *"  # Twice daily at 6 AM and 6 PM
# MAGIC
# MAGIC # Maintenance
# MAGIC OPTIMIZE_Job: "0 0 1 * * ? *"  # Daily at 1 AM
# MAGIC VACUUM_Job: "0 30 2 ? * SUN *"  # Sunday at 2:30 AM
# MAGIC
# MAGIC # Reporting
# MAGIC Health_Check: "0 0 * * * ? *"  # Every hour
# MAGIC Executive_Report: "0 0 9 * * ? *"  # Daily at 9 AM
# MAGIC Weekly_Summary: "0 0 9 ? * MON *"  # Monday at 9 AM
# MAGIC ```
# MAGIC
# MAGIC ### Scheduling Best Practices:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC - Run maintenance during low-traffic hours (1-3 AM)
# MAGIC - Schedule DQ checks after data ingestion completes
# MAGIC - Stagger operations to avoid resource contention
# MAGIC - Add buffer time between dependent jobs
# MAGIC - Set appropriate timeouts (15-60 minutes)
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC - Run OPTIMIZE during peak query hours
# MAGIC - Schedule VACUUM immediately after OPTIMIZE
# MAGIC - Run multiple heavy operations concurrently
# MAGIC - Schedule reports before data is refreshed
# MAGIC
# MAGIC ### Dependency Chain:
# MAGIC ```
# MAGIC Orchestrator (00:00-00:15)
# MAGIC     ↓
# MAGIC OPTIMIZE (01:00-02:00)
# MAGIC     ↓
# MAGIC ANALYZE (02:10-02:20)
# MAGIC     ↓
# MAGIC VACUUM (02:30-03:15) [Weekly only]
# MAGIC     ↓
# MAGIC DQ Framework (06:00-06:30)
# MAGIC     ↓
# MAGIC DQ Alerts (06:35) [If failures]
# MAGIC     ↓
# MAGIC Executive Report (09:00)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Quick Reference Guide
# MAGIC %md
# MAGIC ## 📋 Quick Reference Guide
# MAGIC
# MAGIC ### Table & Schema Reference
# MAGIC
# MAGIC #### Catalog Structure
# MAGIC ```
# MAGIC retailpulse/
# MAGIC ├── bronze/
# MAGIC │   └── orders_raw (Streaming, Auto Loader)
# MAGIC ├── silver/
# MAGIC │   ├── orders (Validated, 4hr SLA)
# MAGIC │   └── products (Reference data)
# MAGIC ├── gold/
# MAGIC │   ├── fact_sales (Sales transactions, 2hr SLA)
# MAGIC │   ├── dim_customer (SCD Type 2, 24hr SLA)
# MAGIC │   ├── dim_product (SCD Type 2, 24hr SLA)
# MAGIC │   └── dim_date (Date dimension)
# MAGIC └── ops/
# MAGIC     ├── metadata_catalog (Config registry)
# MAGIC     ├── dq_validation_audit (Quality logs)
# MAGIC     ├── etl_job_audit (Pipeline logs)
# MAGIC     ├── maintenance_audit (Ops logs)
# MAGIC     └── table_change_audit (Lineage)
# MAGIC ```
# MAGIC
# MAGIC ### Validation Rules Currently Configured
# MAGIC
# MAGIC #### fact_sales (8 rules)
# MAGIC | Column | Rule | Description |
# MAGIC |--------|------|-------------|
# MAGIC | order_id | NOT_NULL | Sale ID must not be null |
# MAGIC | product_sk | NOT_NULL | Product ID must not be null |
# MAGIC | customer_id | NOT_NULL | Customer ID must not be null |
# MAGIC | order_ts | DATE_VALID | Date must be valid, not future |
# MAGIC | quantity | POSITIVE | Quantity > 0 |
# MAGIC | price | POSITIVE | Price > 0 |
# MAGIC | sales_amount | POSITIVE | Sales amount > 0 |
# MAGIC | date_id | - | (No validation configured) |
# MAGIC
# MAGIC #### dim_customer (2 rules)
# MAGIC | Column | Rule | Description |
# MAGIC |--------|------|-------------|
# MAGIC | customer_id | NOT_NULL | Customer ID must not be null |
# MAGIC | customer_name | NOT_NULL | Name must not be null or empty |
# MAGIC
# MAGIC #### dim_product (3 rules)
# MAGIC | Column | Rule | Description |
# MAGIC |--------|------|-------------|
# MAGIC | product_id | NOT_NULL | Product ID must not be null |
# MAGIC | product_name | NOT_NULL | Name must not be null or empty |
# MAGIC | current_price | POSITIVE | Current price > 0 |
# MAGIC
# MAGIC ### Maintenance Policies
# MAGIC
# MAGIC #### OPTIMIZE Configuration
# MAGIC | Table | Frequency | ZORDER Columns |
# MAGIC |-------|-----------|----------------|
# MAGIC | orders_raw | Daily | - |
# MAGIC | orders | Daily | order_date, customer_id |
# MAGIC | fact_sales | Daily | date_id, customer_id |
# MAGIC | dim_customer | Weekly | customer_id |
# MAGIC | dim_product | Weekly | product_id |
# MAGIC | dim_date | Monthly | date_key |
# MAGIC
# MAGIC #### VACUUM Configuration
# MAGIC | Layer | Retention Hours | Frequency |
# MAGIC |-------|----------------|----------|
# MAGIC | Bronze | 168 hrs (7 days) | Weekly |
# MAGIC | Silver | 168 hrs (7 days) | Weekly |
# MAGIC | Gold | 168 hrs (7 days) | Weekly |
# MAGIC
# MAGIC ### Common Queries
# MAGIC
# MAGIC #### Check DQ Status
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     check_category,
# MAGIC     status,
# MAGIC     COUNT(*) as check_count
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE run_date = CURRENT_DATE()
# MAGIC GROUP BY check_category, status;
# MAGIC ```
# MAGIC
# MAGIC #### View Failed Checks
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     check_name,
# MAGIC     message,
# MAGIC     run_timestamp
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE status = 'FAIL'
# MAGIC   AND run_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY run_timestamp DESC;
# MAGIC ```
# MAGIC
# MAGIC #### Maintenance Effectiveness
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     operation_type,
# MAGIC     AVG(files_before - files_after) as avg_files_compacted,
# MAGIC     AVG(space_saved_mb) as avg_space_saved_mb
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY table_name, operation_type;
# MAGIC ```
# MAGIC
# MAGIC #### ETL Success Rate
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     layer,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
# MAGIC FROM retailpulse.ops.etl_job_audit
# MAGIC WHERE run_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY layer;
# MAGIC ```
# MAGIC
# MAGIC ### Troubleshooting Guide
# MAGIC
# MAGIC #### DQ Check Failures
# MAGIC 1. **Check audit logs**: `SELECT * FROM retailpulse.ops.dq_validation_audit WHERE status = 'FAIL'`
# MAGIC 2. **Identify root cause**: Review `message` and `violation_count` columns
# MAGIC 3. **Query affected table**: Verify data issues directly
# MAGIC 4. **Re-run orchestrator**: If data is missing/stale
# MAGIC 5. **Update metadata**: If schema changed
# MAGIC
# MAGIC #### Empty Tables
# MAGIC 1. **Run orchestrator job**: `RetailPulse Enterprise Orchestrator`
# MAGIC 2. **Check ETL logs**: `SELECT * FROM retailpulse.ops.etl_job_audit WHERE status = 'FAILED'`
# MAGIC 3. **Verify source data**: Ensure cloud files exist
# MAGIC 4. **Check DLT pipeline**: Review pipeline run history
# MAGIC
# MAGIC #### Maintenance Issues
# MAGIC 1. **Check maintenance logs**: `SELECT * FROM retailpulse.ops.maintenance_audit WHERE status = 'FAILED'`
# MAGIC 2. **Verify permissions**: Ensure MODIFY privilege on tables
# MAGIC 3. **Check concurrency**: Avoid running during active queries
# MAGIC 4. **Review retention**: Ensure VACUUM retention >= 7 days
# MAGIC
# MAGIC ### Notebook Locations
# MAGIC
# MAGIC | Notebook | Path | Purpose |
# MAGIC |----------|------|----------|
# MAGIC | 01_Setup_Audit_Tables | /RetailPulse/notebooks/ | Create audit infrastructure |
# MAGIC | 02_Metadata_Configuration | /RetailPulse/notebooks/ | Configure metadata |
# MAGIC | 03_DQ_Framework | /RetailPulse/notebooks/ | Run DQ validations |
# MAGIC | 04_Maintenance_Framework | /RetailPulse/notebooks/ | Run maintenance ops |
# MAGIC | 05_Audit_Reporting | /RetailPulse/notebooks/ | View health reports |
# MAGIC | RetailPulse Framework Architecture | /Users/shekartelstra@gmail.com/ | This documentation |