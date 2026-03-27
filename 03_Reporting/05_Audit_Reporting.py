# Databricks notebook source
# DBTITLE 1,Audit Reporting Title
# MAGIC %md
# MAGIC # Audit Reporting & Monitoring Dashboard
# MAGIC
# MAGIC **RetailPulse Project - Operational Intelligence**
# MAGIC
# MAGIC This notebook provides comprehensive reporting and monitoring views for:
# MAGIC - **Data Quality Trends**: Track DQ check pass rates over time
# MAGIC - **ETL Job Performance**: Monitor pipeline execution and failures
# MAGIC - **Maintenance Operations**: Review OPTIMIZE/VACUUM effectiveness
# MAGIC - **Table Change Tracking**: Audit data modifications and lineage
# MAGIC
# MAGIC Use these queries to build dashboards and alerting mechanisms.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,DQ Monitoring Section
# MAGIC %md
# MAGIC ## 1️⃣ Data Quality Monitoring

# COMMAND ----------

# DBTITLE 1,DQ Check Summary - Last 7 Days
# MAGIC %sql
# MAGIC -- Overall DQ health summary by category
# MAGIC SELECT 
# MAGIC     check_category,
# MAGIC     COUNT(*) as total_checks,
# MAGIC     SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
# MAGIC     SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
# MAGIC     SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as errors,
# MAGIC     ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pass_rate_pct,
# MAGIC     MAX(run_timestamp) as last_run
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY check_category
# MAGIC ORDER BY check_category;

# COMMAND ----------

# DBTITLE 1,DQ Failures - Recent Issues
# MAGIC %sql
# MAGIC -- Recent DQ check failures requiring attention
# MAGIC SELECT 
# MAGIC     run_timestamp,
# MAGIC     check_category,
# MAGIC     check_name,
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     violation_count,
# MAGIC     total_records,
# MAGIC     pass_rate,
# MAGIC     message
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE status IN ('FAIL', 'ERROR')
# MAGIC     AND run_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY run_timestamp DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,DQ Trends by Table
# MAGIC %sql
# MAGIC -- DQ pass rate trends by table over last 30 days
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     DATE(run_timestamp) as check_date,
# MAGIC     COUNT(*) as checks_run,
# MAGIC     ROUND(AVG(pass_rate), 2) as avg_pass_rate,
# MAGIC     SUM(violation_count) as total_violations
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 30 DAYS
# MAGIC     AND pass_rate IS NOT NULL
# MAGIC GROUP BY table_name, DATE(run_timestamp)
# MAGIC ORDER BY table_name, check_date DESC;

# COMMAND ----------

# DBTITLE 1,ETL Monitoring Section
# MAGIC %md
# MAGIC ## 2️⃣ ETL Job Performance Monitoring

# COMMAND ----------

# DBTITLE 1,ETL Job Summary - Last 7 Days
# MAGIC %sql
# MAGIC -- ETL job execution summary
# MAGIC SELECT 
# MAGIC     job_layer,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
# MAGIC     SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
# MAGIC     ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
# MAGIC     ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_min,
# MAGIC     SUM(rows_written) as total_rows_written
# MAGIC FROM retailpulse.ops.etl_job_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY job_layer
# MAGIC ORDER BY job_layer;

# COMMAND ----------

# DBTITLE 1,ETL Failures - Recent Issues
# MAGIC %sql
# MAGIC -- Recent ETL job failures
# MAGIC SELECT 
# MAGIC     start_time,
# MAGIC     job_name,
# MAGIC     job_type,
# MAGIC     target_table,
# MAGIC     duration_seconds,
# MAGIC     error_message,
# MAGIC     error_type,
# MAGIC     execution_user
# MAGIC FROM retailpulse.ops.etl_job_audit
# MAGIC WHERE status = 'FAILED'
# MAGIC     AND run_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,ETL Performance Trends
# MAGIC %sql
# MAGIC -- Daily ETL job performance trends
# MAGIC SELECT 
# MAGIC     run_date,
# MAGIC     COUNT(*) as jobs_run,
# MAGIC     ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_min,
# MAGIC     MAX(duration_seconds / 60.0) as max_duration_min,
# MAGIC     SUM(rows_written) as total_rows_processed,
# MAGIC     ROUND(SUM(bytes_processed) / 1024.0 / 1024.0 / 1024.0, 2) as total_gb_processed
# MAGIC FROM retailpulse.ops.etl_job_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 30 DAYS
# MAGIC     AND status = 'SUCCESS'
# MAGIC GROUP BY run_date
# MAGIC ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Maintenance Monitoring Section
# MAGIC %md
# MAGIC ## 3️⃣ Table Maintenance Monitoring

# COMMAND ----------

# DBTITLE 1,Maintenance Operations Summary
# MAGIC %sql
# MAGIC -- Summary of all maintenance operations
# MAGIC SELECT 
# MAGIC     operation,
# MAGIC     COUNT(*) as total_ops,
# MAGIC     SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
# MAGIC     SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
# MAGIC     ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_min,
# MAGIC     SUM(ABS(files_added)) as total_files_compacted,
# MAGIC     ROUND(SUM(space_saved_mb) / 1024.0, 2) as total_space_saved_gb,
# MAGIC     MAX(start_time) as last_run
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY operation
# MAGIC ORDER BY operation;

# COMMAND ----------

# DBTITLE 1,Maintenance Effectiveness by Table
# MAGIC %sql
# MAGIC -- OPTIMIZE effectiveness by table
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     COUNT(*) as optimize_runs,
# MAGIC     SUM(num_files_before) as total_files_before,
# MAGIC     SUM(num_files_after) as total_files_after,
# MAGIC     SUM(ABS(files_added)) as files_compacted,
# MAGIC     ROUND(SUM(space_saved_mb), 2) as space_saved_mb,
# MAGIC     ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_min,
# MAGIC     MAX(start_time) as last_optimize
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE operation = 'OPTIMIZE'
# MAGIC     AND run_date >= current_date() - INTERVAL 30 DAYS
# MAGIC     AND status = 'SUCCESS'
# MAGIC GROUP BY table_name
# MAGIC ORDER BY space_saved_mb DESC;

# COMMAND ----------

# DBTITLE 1,Recent Maintenance Operations
# MAGIC %sql
# MAGIC -- Recent maintenance operations log
# MAGIC SELECT 
# MAGIC     start_time,
# MAGIC     table_name,
# MAGIC     operation,
# MAGIC     status,
# MAGIC     duration_seconds,
# MAGIC     num_files_before,
# MAGIC     num_files_after,
# MAGIC     space_saved_mb,
# MAGIC     message
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Change Tracking Section
# MAGIC %md
# MAGIC ## 4️⃣ Table Change Tracking & Lineage

# COMMAND ----------

# DBTITLE 1,Data Change Summary by Table
# MAGIC %sql
# MAGIC -- Summary of data changes by table
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     operation,
# MAGIC     COUNT(*) as change_count,
# MAGIC     SUM(rows_affected) as total_rows_affected,
# MAGIC     MAX(change_timestamp) as last_change,
# MAGIC     COUNT(DISTINCT user_name) as unique_users
# MAGIC FROM retailpulse.ops.table_change_audit
# MAGIC WHERE change_date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY table_name, operation
# MAGIC ORDER BY table_name, operation;

# COMMAND ----------

# DBTITLE 1,Recent High-Impact Changes
# MAGIC %sql
# MAGIC -- Recent changes affecting large numbers of rows
# MAGIC SELECT 
# MAGIC     change_timestamp,
# MAGIC     table_name,
# MAGIC     operation,
# MAGIC     rows_affected,
# MAGIC     user_name,
# MAGIC     job_name,
# MAGIC     change_details
# MAGIC FROM retailpulse.ops.table_change_audit
# MAGIC WHERE change_date >= current_date() - INTERVAL 7 DAYS
# MAGIC     AND rows_affected > 1000
# MAGIC ORDER BY change_timestamp DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Metadata Catalog View Section
# MAGIC %md
# MAGIC ## 5️⃣ Metadata Catalog Overview

# COMMAND ----------

# DBTITLE 1,Active Tables Configuration
# MAGIC %sql
# MAGIC -- View current metadata configuration for all active tables
# MAGIC SELECT 
# MAGIC     full_table_name,
# MAGIC     layer,
# MAGIC     business_domain,
# MAGIC     refresh_frequency,
# MAGIC     sla_hours,
# MAGIC     optimize_frequency,
# MAGIC     zorder_columns,
# MAGIC     vacuum_retention_hours,
# MAGIC     dq_checks_enabled,
# MAGIC     owner
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE is_active = TRUE
# MAGIC     AND column_name IS NULL  -- Table-level entries only
# MAGIC ORDER BY layer, table_name;

# COMMAND ----------

# DBTITLE 1,Column-Level Validation Rules
# MAGIC %sql
# MAGIC -- View all column-level validation rules
# MAGIC SELECT 
# MAGIC     full_table_name,
# MAGIC     column_name,
# MAGIC     validation_rule,
# MAGIC     rule_description,
# MAGIC     dq_checks_enabled
# MAGIC FROM retailpulse.ops.metadata_catalog
# MAGIC WHERE column_name IS NOT NULL
# MAGIC     AND validation_rule IS NOT NULL
# MAGIC     AND is_active = TRUE
# MAGIC ORDER BY full_table_name, column_name;

# COMMAND ----------

# DBTITLE 1,Executive Dashboard Section
# MAGIC %md
# MAGIC ## 📊 Executive Dashboard - Overall Health

# COMMAND ----------

# DBTITLE 1,Framework Health Overview
# MAGIC %sql
# MAGIC -- Overall framework health metrics
# MAGIC SELECT 
# MAGIC     'Data Quality' as metric_category,
# MAGIC     CONCAT(
# MAGIC         ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
# MAGIC         '% Pass Rate'
# MAGIC     ) as current_status,
# MAGIC     COUNT(*) as total_checks_24h
# MAGIC FROM retailpulse.ops.dq_validation_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 1 DAY
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'ETL Jobs' as metric_category,
# MAGIC     CONCAT(
# MAGIC         ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
# MAGIC         '% Success Rate'
# MAGIC     ) as current_status,
# MAGIC     COUNT(*) as total_jobs_24h
# MAGIC FROM retailpulse.ops.etl_job_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 1 DAY
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Maintenance Ops' as metric_category,
# MAGIC     CONCAT(
# MAGIC         ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
# MAGIC         '% Success Rate'
# MAGIC     ) as current_status,
# MAGIC     COUNT(*) as total_ops_7d
# MAGIC FROM retailpulse.ops.maintenance_audit
# MAGIC WHERE run_date >= current_date() - INTERVAL 7 DAYS;