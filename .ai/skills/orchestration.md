# Skill: Orchestration

Create orchestration patterns for Databricks pipelines and jobs in modular lakehouse workflows.

## Objective

Coordinate DLT and batch jobs cleanly so ingestion, transformation, and modeling layers run in the correct order with clear ownership.

## Core Patterns

- Use DLT for Bronze and Silver when streaming ingestion, schema evolution, and data quality are required.
- Use Databricks Workflow jobs for Gold dimensions and facts.
- Use a lightweight orchestrator job when you want one manual or scheduled entry point.

## Task Dependency Pattern

Preferred order:

1. DLT pipeline task
2. Gold job task with `depends_on` success

Pattern:

```json
{
  "tasks": [
    {
      "task_key": "bronze_silver_dlt",
      "pipeline_task": {
        "pipeline_id": "<PIPELINE_ID>"
      }
    },
    {
      "task_key": "gold_job",
      "depends_on": [
        { "task_key": "bronze_silver_dlt" }
      ],
      "run_job_task": {
        "job_id": <GOLD_JOB_ID>
      }
    }
  ]
}
```

## Orchestrator Job Design

Use an orchestrator job when:

- the platform design is split across DLT and Gold jobs
- users want one button to run the pipeline manually
- scheduling should happen at a controller level instead of on every component independently

## Retry And Failure Guidance

- Let downstream Gold tasks run only after upstream DLT success.
- Retry transient infrastructure failures at the task level when supported.
- Keep Bronze/Silver failures isolated from Gold modeling failures.
- Prefer separate quarantine outputs for recoverable data issues instead of aborting the full pipeline.
- Keep DLT and Gold ownership boundaries clear so reruns are targeted and safe.

## Reusable Recommendation

For enterprise-scale pipelines:

- DLT = ingestion, schema evolution, Silver curation, quarantine
- Gold jobs = dimensions, facts, batch modeling
- Orchestrator = dependency control and single entry point
