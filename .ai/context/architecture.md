# RetailPulse Architecture

## Overview

RetailPulse is a retail analytics platform built on Databricks, implementing a medallion architecture (Bronze → Silver → Gold) for data processing. The platform provides real-time and batch data ingestion, comprehensive data quality validation, operational monitoring, and AI-powered analytics capabilities.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  Cloud Files (S3/ADLS/GCS)  │  Kafka/Event Hubs  │  JDBC/APIs          │
└──────────────────┬──────────────────────┬─────────────────┬─────────────┘
                   │                      │                 │
                   ▼                      ▼                 ▼
         ┌─────────────────────────────────────────────────────────┐
         │              BRONZE LAYER (Raw Ingestion)               │
         ├─────────────────────────────────────────────────────────┤
         │  • Auto Loader (cloudFiles)                             │
         │  • Streaming ingestion with schema inference            │
         │  • Minimal transformation                               │
         │  • Preserve source data integrity                       │
         │  • Add technical metadata (ingestion_timestamp, etc.)   │
         └────────────────────┬────────────────────────────────────┘
                              │
                              ▼
         ┌─────────────────────────────────────────────────────────┐
         │          SILVER LAYER (Cleaned & Validated)             │
         ├─────────────────────────────────────────────────────────┤
         │  • Data cleansing & standardization                     │
         │  • Deduplication                                        │
         │  • Data quality expectations (DQ Framework)             │
         │  • CDC for dimension tables (SCD Type 1/2)              │
         │  • Business logic application                           │
         │  • Enrichment with lookups                              │
         └────────────────────┬────────────────────────────────────┘
                              │
                              ▼
         ┌─────────────────────────────────────────────────────────┐
         │        GOLD LAYER (Business-Ready Aggregates)           │
         ├─────────────────────────────────────────────────────────┤
         │  • Aggregated metrics & KPIs                            │
         │  • Denormalized for analytics                           │
         │  • Customer 360 views                                   │
         │  • Sales/inventory dashboards                           │
         │  • ML feature engineering                               │
         └────────────────────┬────────────────────────────────────┘
                              │
                              ▼
         ┌─────────────────────────────────────────────────────────┐
         │                 OPERATIONAL LAYER                       │
         ├─────────────────────────────────────────────────────────┤
         │  • metadata_catalog (schema registry)                   │
         │  • dq_validation_audit (quality metrics)                │
         │  • etl_job_audit (pipeline runs)                        │
         │  • maintenance_audit (optimization logs)                │
         │  • table_change_audit (schema evolution)                │
         └─────────────────────────────────────────────────────────┘
```

## Medallion Architecture Layers

### Bronze Layer (Raw Data)
**Purpose**: Ingest raw data with minimal transformation

**Characteristics**:
* Schema-on-read with automatic inference
* Append-only streaming tables
* Preserve source format and structure
* Add technical metadata for lineage

**Technologies**:
* Auto Loader for cloud file ingestion
* Kafka/Event Hubs for streaming events
* JDBC for database replication
* Delta Live Tables streaming tables

**Naming Convention**: `bronze_{domain}_{entity}`
* Example: `bronze_sales_transactions`, `bronze_inventory_stock`

### Silver Layer (Cleaned Data)
**Purpose**: Cleansed, validated, and conformed data

**Characteristics**:
* Schema enforcement with expectations
* Data quality validation
* Deduplication and cleansing
* Business rule application
* Slowly Changing Dimensions (SCD)

**Technologies**:
* Delta Live Tables with expectations
* Auto CDC for change data capture
* Streaming and batch processing
* Data quality framework

**Naming Convention**: `silver_{domain}_{entity}`
* Example: `silver_sales_orders`, `silver_customer_profiles`

### Gold Layer (Business Aggregates)
**Purpose**: Business-ready, aggregated datasets

**Characteristics**:
* Denormalized for query performance
* Pre-computed aggregations
* Optimized for analytics workloads
* Liquid clustering for multi-dimensional queries

**Technologies**:
* Materialized views for aggregations
* Liquid clustering
* Scheduled refreshes
* BI tool integration

**Naming Convention**: `gold_{business_domain}_{metric}`
* Example: `gold_sales_daily_summary`, `gold_customer_360`

## Data Flow Patterns

### 1. Streaming Ingestion Flow
```
Cloud Storage → Auto Loader → Bronze Table → Silver Table (streaming)
                                                    ↓
                                            Gold MV (batch read)
```

**Use Cases**:
* Real-time sales transactions
* IoT sensor data
* Application logs
* Event streams

### 2. CDC Flow (Change Data Capture)
```
Database Snapshot → Bronze → Auto CDC → Silver (SCD Type 2)
                                              ↓
                                      Gold Customer 360
```

**Use Cases**:
* Customer dimension updates
* Product catalog changes
* Employee records
* Reference data sync

### 3. Batch Aggregation Flow
```
Silver Tables (streaming) → Gold MV (batch aggregation) → Dashboards
```

**Use Cases**:
* Daily sales summaries
* Inventory reports
* Customer segmentation
* KPI calculations

## Project Structure

```
/Workspace/Users/shekartelstra@gmail.com/RetailPulse/
│
├── .ai/                                    # AI assistant context
│   ├── context/
│   │   ├── architecture.md                # This file - system architecture
│   │   ├── coding_standards.md            # Code style and conventions
│   │   └── data_model.md                  # Data schemas and relationships
│   ├── prompts/
│   │   ├── bronze_pipeline.md             # Bronze ingestion prompt
│   │   ├── generate_pipeline.md           # Full DLT pipeline creation
│   │   ├── gold_job.md                    # Gold layer job creation
│   │   ├── optimize_delta.md              # Delta optimization guide
│   │   └── orchestration_job.md           # E2E orchestration workflow
│   └── skills/
│       ├── bronze_pipeline.md             # Bronze layer patterns
│       ├── fact_table.md                  # Fact table design
│       ├── orchestration.md               # Job orchestration
│       ├── scd2_merge.md                  # SCD Type 2 implementation
│       ├── silver_transform.md            # Silver transformation logic
│       ├── streaming_pipeline.md          # Streaming patterns
│       └── README.md                      # Skills documentation
│
├── 01_Setup/                              # Initial configuration
│   ├── 01_configure_env.py                # Environment setup
│   └── 02_metadata_tables.sql             # Audit table schemas
│
├── 02_Bronze/                             # Raw ingestion layer
│   ├── bronze_sales_transactions.py       # Sales data ingestion
│   ├── bronze_inventory_stock.py          # Inventory ingestion
│   └── bronze_customer_events.py          # Customer event stream
│
├── 03_Silver/                             # Cleaned data layer
│   ├── silver_sales_orders.py             # Cleaned sales
│   ├── silver_inventory_levels.py         # Cleaned inventory
│   ├── cdc_customers.py                   # Customer CDC
│   └── enrich_orders.py                   # Order enrichment
│
├── 04_Gold/                               # Business aggregates
│   ├── gold_sales_daily_summary.py        # Daily sales metrics
│   ├── gold_inventory_summary.py          # Inventory KPIs
│   └── gold_customer_360.py               # Customer profiles
│
├── 05_Operational/                        # Monitoring & maintenance
│   ├── dq_validation_functions.py         # DQ framework functions
│   ├── delta_maintenance.py               # Table optimization
│   └── pipeline_monitor.py                # Health checks
│
├── 06_Testing/                            # Test suite
│   ├── Unit_Test_Suite.py                 # Test runner
│   └── test_cases/
│       ├── Test_01_Schema_Validation.py
│       ├── Test_02_Metadata_Integrity.py
│       ├── Test_03_Freshness_Logic.py
│       ├── Test_04_Quality_Rules.py
│       ├── Test_05_Completeness_Checks.py
│       └── Test_06_Advanced_Tests.py
│
└── 07_MCP-AgentBricks/                    # MCP integration
    ├── DQ_Validation_MCP_Server.py        # MCP server
    └── mcp_config.json                    # MCP configuration
```

## Testing Framework Architecture

### Test Suite Organization

The testing framework provides comprehensive validation of the RetailPulse platform across 6 test suites with 28 test cases (UTC-001 to UTC-028).

| Test Suite | ID | Test Cases | Coverage Area |
|------------|----|-----------:|---------------|
| Test_01_Schema_Validation | 782317743298624 | UTC-001 to UTC-005 | Schema integrity, column existence, data types |
| Test_02_Metadata_Integrity | 782317743298625 | UTC-006 to UTC-008 | Metadata completeness, catalog consistency |
| Test_03_Freshness_Logic | 782317743298626 | UTC-009 to UTC-011 | Data freshness detection, SLA validation |
| Test_04_Quality_Rules | 782317743298627 | UTC-012 to UTC-016 | DQ rule execution, validation logic |
| Test_05_Completeness_Checks | 782317743298628 | UTC-017 to UTC-019 | Null checks, record count validation |
| Test_06_Advanced_Tests | 782317743298629 | UTC-020 to UTC-028 | Cross-table consistency, business logic |

### Test Coverage Areas

**Schema Validation** (UTC-001 to UTC-005):
* Table existence verification
* Column name and type validation
* Primary key constraints
* Generated column logic
* Schema evolution tracking

**Metadata Integrity** (UTC-006 to UTC-008):
* Metadata catalog completeness
* Business domain coverage
* Owner assignment validation
* Tag consistency

**Freshness Logic** (UTC-009 to UTC-011):
* Data freshness calculation accuracy
* SLA threshold validation
* Timestamp logic verification
* Late-arriving data detection

**Quality Rules** (UTC-012 to UTC-016):
* Validation rule execution
* NOT_NULL rule enforcement
* POSITIVE/NON_NEGATIVE checks
* DATE_VALID validation
* RANGE boundary testing

**Completeness Checks** (UTC-017 to UTC-019):
* Required field population
* Record count validation
* Expected row presence
* Data gap detection

**Advanced Tests** (UTC-020 to UTC-028):
* Cross-table referential integrity
* Business rule validation
* Aggregate accuracy
* End-to-end data flow
* Performance benchmarks

### Test Execution Model

**Test Framework Functions**:
```python
def log_test_result(test_id, test_name, passed, details=""):
    """
    Logs test execution results with visual indicators
    
    Args:
        test_id: Unique test identifier (UTC-XXX)
        test_name: Human-readable test description
        passed: Boolean test result
        details: Additional context on failure
    """
```

**Execution Pattern**:
1. Setup: Create mock data or reference production tables
2. Execute: Run validation logic
3. Assert: Compare results against expectations
4. Log: Record pass/fail with details
5. Cleanup: Remove temporary test data
6. Summary: Display aggregate statistics

**Test Isolation**:
* Use mock data generators for unit tests
* No impact on production tables
* Rollback-safe temporary tables
* Independent test execution

## MCP Integration Architecture

### Overview

The Model Context Protocol (MCP) integration provides AI agents with programmatic access to RetailPulse data quality functions through standardized tools.

### MCP Server Components

**Server Location**: `/RetailPulse/07_MCP-AgentBricks/DQ_Validation_MCP_Server`

**Exposed Tools**:

1. **validate_table_schema**
   * Validates table exists with expected columns and types
   * Parameters:
     - `catalog`: Unity Catalog name
     - `schema`: Schema name
     - `table_name`: Table name
     - `expected_columns`: List of {"name": str, "type": str}
   * Returns: Validation result with missing/mismatched columns

2. **check_data_freshness**
   * Checks if data meets freshness SLA requirements
   * Parameters:
     - `catalog`: Unity Catalog name
     - `schema`: Schema name
     - `table_name`: Table name
     - `sla_hours`: Maximum age threshold
   * Returns: Freshness status and hours since last update

3. **validate_data_quality**
   * Executes data quality validation rules
   * Parameters:
     - `catalog`: Unity Catalog name
     - `schema`: Schema name
     - `table_name`: Table name
     - `rules`: List of {"column": str, "rule_type": str, "rule_value": any}
   * Returns: Validation results per rule with pass/fail counts

4. **get_table_metadata**
   * Retrieves comprehensive table metadata
   * Parameters:
     - `catalog`: Unity Catalog name
     - `schema`: Schema name
     - `table_name`: Table name
   * Returns: Schema, owner, tags, refresh schedule, SLA

5. **check_pipeline_health**
   * Monitors pipeline execution health
   * Parameters:
     - `pipeline_name`: Optional specific pipeline filter
     - `hours`: Lookback window (default 24)
   * Returns: Success/failure counts, error summaries

### MCP Python API

Direct function access for Python clients:

```python
from retailpulse.dq import (
    validate_table_schema_uc,
    check_data_freshness_uc,
    validate_data_quality_uc,
    get_table_metadata_uc,
    check_pipeline_health_uc
)

# Example: Validate schema
result = validate_table_schema_uc(
    catalog="retailpulse",
    schema="silver",
    table_name="sales_orders",
    expected_columns=[
        {"name": "order_id", "type": "bigint"},
        {"name": "customer_id", "type": "bigint"},
        {"name": "order_date", "type": "date"}
    ]
)
```

### Discovery Patterns

**AI Agent Usage**:
```
Agent: "Check if the silver_sales_orders table is up to date"
→ Uses check_data_freshness tool
→ Returns: "Table is fresh (updated 2 hours ago, SLA: 6 hours)"

Agent: "Validate that customer_id column has no nulls"
→ Uses validate_data_quality tool with NOT_NULL rule
→ Returns: "Validation passed: 0 null values found"
```

**Integration Benefits**:
* Automated DQ checks in CI/CD pipelines
* Real-time validation in notebook workflows
* Proactive alerting on quality degradation
* Self-service data quality monitoring

## Data Quality Framework

### Validation Rule Types

| Rule Type | Description | Example |
|-----------|-------------|---------|
| NOT_NULL | Column must not contain null values | `customer_id IS NOT NULL` |
| POSITIVE | Numeric column must be > 0 | `amount > 0` |
| NON_NEGATIVE | Numeric column must be >= 0 | `quantity >= 0` |
| DATE_VALID | Date must be valid and within range | `order_date <= current_date()` |
| RANGE | Value must fall within specified range | `age BETWEEN 0 AND 120` |

### Expectation Strategies

**Fail Fast** (expect_or_fail):
```python
@dp.expect_or_fail("critical_field", "customer_id IS NOT NULL")
```
* Pipeline stops on validation failure
* Use for critical business rules

**Drop Invalid** (expect_or_drop):
```python
@dp.expect_or_drop("valid_amount", "amount > 0")
```
* Invalid rows excluded from output
* Continue processing valid data

**Track Only** (expect):
```python
@dp.expect("completeness", "email IS NOT NULL")
```
* Log violations but continue
* Monitor data quality trends

### Quality Metrics Tracking

All validation results stored in `retailpulse.ops.dq_validation_audit`:
* Table/column validated
* Rule executed
* Pass/fail counts
* Execution timestamp
* Error details

**Query Example**:
```sql
SELECT 
  table_name,
  validation_rule,
  SUM(records_validated) as total_records,
  SUM(records_failed) as failed_records,
  ROUND(100.0 * SUM(records_failed) / SUM(records_validated), 2) as failure_rate
FROM retailpulse.ops.dq_validation_audit
WHERE validation_date >= current_date() - INTERVAL 7 DAYS
GROUP BY table_name, validation_rule
HAVING failure_rate > 1.0
ORDER BY failure_rate DESC;
```

## Operational Components

### Audit Tables

**1. metadata_catalog**
* Central schema registry
* 30 columns tracking schema, validation rules, SLA, optimization settings
* Single source of truth for table metadata

**2. dq_validation_audit**
* Data quality validation history
* Tracks rule execution, pass/fail counts, error messages
* Enables DQ trend analysis

**3. etl_job_audit**
* Pipeline execution logs
* Job start/end times, status, row counts, error tracking
* Performance monitoring

**4. maintenance_audit**
* Table optimization history
* OPTIMIZE, VACUUM, ANALYZE operations
* Storage and performance metrics

**5. table_change_audit**
* Schema evolution tracking
* Column additions, type changes, table renames
* Impact analysis for downstream dependencies

### Monitoring & Alerting

**System Metrics**:
* Pipeline success/failure rates
* Data freshness SLA compliance
* Data quality rule violations
* Table size and growth trends

**Alert Triggers**:
* Pipeline failures (immediate)
* SLA breaches (within 1 hour)
* Quality degradation (>5% failure rate)
* Schema changes (notification)

**Dashboards**:
* Pipeline health (last 24 hours)
* DQ scorecard (by table/domain)
* Data freshness status
* Storage and compute costs

## Technology Stack

**Compute**:
* Databricks Runtime 14.3 LTS
* Photon acceleration enabled
* Unity Catalog for governance

**Storage**:
* Delta Lake (ACID transactions)
* Cloud object storage (S3/ADLS/GCS)
* Liquid clustering for query optimization

**Processing**:
* Delta Live Tables (streaming & batch)
* Auto Loader (cloud file ingestion)
* Auto CDC (change data capture)

**Languages**:
* Python (PySpark, Delta Live Tables API)
* SQL (Delta SQL, DLT SQL)

**Orchestration**:
* Databricks Workflows
* DLT pipelines (continuous/scheduled)

**Monitoring**:
* System tables (query history, lineage)
* Custom audit tables
* MCP integration for AI agents

## Security & Governance

**Unity Catalog**:
* Catalog: `retailpulse`
* Schemas: `bronze`, `silver`, `gold`, `ops`
* Fine-grained access control (GRANT/REVOKE)

**Row/Column Security**:
* Row filters for PII protection
* Column masking for sensitive data
* Dynamic views based on user context

**Data Classification**:
* Tags: `PII`, `PHI`, `Confidential`, `Public`
* Automated tagging via metadata catalog
* Compliance reporting

## Performance Optimization

**Table Optimization**:
* Liquid clustering on gold tables
* Auto-optimize for streaming tables
* Z-ordering for specific query patterns
* Regular VACUUM for storage efficiency

**Caching**:
* Delta cache for hot data
* Materialized views for expensive aggregations
* Query result caching in BI tools

**Partitioning**:
* Date-based partitioning for large tables
* Avoid over-partitioning (<1GB partitions)
* Liquid clustering preferred for multi-dimensional queries

## Scalability Considerations

**Current Scale**:
* ~100GB data volume
* ~1M rows/day ingestion rate
* 5-10 concurrent users

**Scale Targets**:
* 10TB+ data volume
* 100M+ rows/day ingestion
* 100+ concurrent users

**Scaling Strategy**:
* Horizontal scaling via autoscaling clusters
* Partition large tables (>100GB)
* Optimize medallion layer boundaries
* Implement data lifecycle policies (archive cold data)

## Future Enhancements

**Planned Features**:
1. ML model integration for predictive analytics
2. Real-time dashboard streaming
3. Advanced CDC patterns (merge strategies)
4. Data mesh architecture (domain-driven ownership)
5. Cross-region replication for HA/DR
6. Advanced security (encryption, tokenization)

**Technical Debt**:
* Standardize error handling across pipelines
* Implement comprehensive integration tests
* Enhance monitoring dashboard granularity
* Document runbook procedures
* Automate disaster recovery testing

---

**Last Updated**: 2024-01-15  
**Version**: 1.0  
**Maintained By**: Data Engineering Team
