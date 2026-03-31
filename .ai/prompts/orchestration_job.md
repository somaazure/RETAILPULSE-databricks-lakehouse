# Orchestration Job Creation Prompt

Create a Databricks orchestration job to coordinate the end-to-end RetailPulse data pipeline: Bronze/Silver DLT → Gold Jobs.

## Job Configuration

* **Job Name**: `retailpulse_enterprise_orchestrator` or `retailpulse_e2e_workflow`
* **Job Type**: Multi-task workflow with mixed task types
* **Purpose**: Sequence DLT pipeline updates and downstream Gold processing

## Orchestration Pattern

**Task 1: Bronze/Silver DLT Pipeline Update**
* Task Type: `pipeline_task` (DLT pipeline update)
* Target: `retailpulse_bronze_silver_pipeline` (pipeline ID)
* Full Refresh: `false` (incremental by default)
* Wait for completion before proceeding

**Task 2: Gold Dimensions & Facts Job**
* Task Type: `run_job_task` (trigger another job)
* Target: `retailpulse_gold_dims_facts` (job ID)
* Depends on: Task 1 completion
* Wait for completion before proceeding

**Task 3: Gold Aggregates (Optional)**
* Task Type: `notebook_task` or nested job
* Execute additional Gold layer aggregations
* Depends on: Task 2 completion

## Task Dependency Configuration

```json
{
  "tasks": [
    {
      "task_key": "bronze_silver_pipeline",
      "pipeline_task": {
        "pipeline_id": "<dlt_pipeline_id>"
      }
    },
    {
      "task_key": "gold_dims_facts",
      "depends_on": [{"task_key": "bronze_silver_pipeline"}],
      "run_job_task": {
        "job_id": "<gold_job_id>"
      }
    }
  ]
}
```

## Trigger Options

**Option 1: File Arrival Trigger**
* Use file event notification service
* Trigger orchestrator when new files land in `/Volumes/retailpulse/bronze/`
* Ensures timely processing of new data

**Option 2: Scheduled Trigger**
* Cron schedule: `0 0 2 * * ?` (daily at 2 AM)
* Adjust based on data arrival patterns
* Add timezone specification

**Option 3: API Trigger**
* REST API call to start orchestrator job
* Integrate with external scheduler (Airflow, Control-M, etc.)
* Use for complex cross-system dependencies

## Error Handling

* Configure max retries per task (e.g., 2 retries)
* Set timeout limits per task to prevent runaway jobs
* Enable email notifications on failure
* Use job run logs and lineage for troubleshooting

## Best Practices

* Use descriptive task keys for readability
* Add task-level comments/descriptions
* Configure appropriate cluster sizing per task
* Enable cost management tags
* Set up alerting for long-running or failed tasks
