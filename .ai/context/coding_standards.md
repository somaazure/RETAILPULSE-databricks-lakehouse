# Coding Standards

* Use PySpark DataFrame API
* Use Delta format for all tables
* Use saveAsTable with Unity Catalog
* Always include checkpointing in streaming
* Use MERGE INTO for upserts
* Avoid hardcoding paths, use parameters
* Use snake_case naming
* Use Delta Live Tables (DLT) for Bronze and Silver layers where streaming and data quality are required
* Use Databricks Workflows for Gold layer transformations such as dimensions and fact tables
* Avoid combining Bronze, Silver, and Gold logic in a single pipeline in production setups
* Ensure modular notebook design for each layer: Bronze, Silver, Gold
* Use `@dlt.expect` and related expectation patterns for data quality validation and quarantine handling in DLT pipelines
* Gold layer transformations should use batch processing and support independent execution
* Maintain separation of concerns between ingestion, transformation, and modeling layers
