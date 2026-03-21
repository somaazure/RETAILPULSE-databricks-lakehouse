Project Name: RetailPulse: Real-Time Retail Analytics Platform on Databricks Lakehouse
RetailPulse is an enterprise-grade, real-time retail analytics platform built on Databricks Lakehouse using Unity Catalog and Medallion Architecture.

The platform simulates a multi-channel retail system by ingesting streaming and batch data related to orders, products, customers, inventory, and returns.

It implements a scalable Bronze-Silver-Gold data pipeline using Delta Lake, Auto Loader, and Structured Streaming, ensuring data quality, governance, and performance optimization.

The Gold layer is modeled using a Star Schema with dimension tables (Product, Category, Customer, Store, Date) and fact tables (Sales, Inventory, Returns) to support advanced analytics and reporting.

The project also incorporates Slowly Changing Dimensions (SCD Type 2), late-arriving data handling, and Delta Lake features like MERGE, Time Travel, and Z-Ordering.

Development is accelerated using AI-assisted coding (Codex in VS Code) and modular skill-based plugins to generate reusable PySpark pipelines, enforce best practices, and enable rapid prototyping of enterprise-grade data engineering workflows.
