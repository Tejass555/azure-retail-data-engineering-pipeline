# End-to-End Azure Retail Data Engineering Pipeline

An end-to-end retail data engineering project on Microsoft Azure. It ingests data from Azure SQL Database with watermark-based incremental loading in Azure Data Factory, lands it in ADLS Gen2 as Parquet, and processes it in Azure Databricks through a Medallion Architecture (Bronze, Silver, Gold) on Delta Lake, governed by Unity Catalog.

> Educational and portfolio project built on a small sample dataset. It is not a production deployment. See [Project Status](#project-status) and [Known Limitations](#known-limitations).

## Table of Contents

- [Project Overview](#project-overview)
- [Technology Stack](#technology-stack)
- [Source Data](#source-data)
- [Azure Data Factory Implementation](#azure-data-factory-implementation)
- [Storage Layer: ADLS Gen2](#storage-layer-adls-gen2)
- [Medallion Architecture in Databricks](#medallion-architecture-in-databricks)
- [Data Governance: Unity Catalog](#data-governance-unity-catalog)
- [Incremental Loading Explained](#incremental-loading-explained)
- [Validation and Testing](#validation-and-testing)
- [Sample Results](#sample-results)
- [Project Status](#project-status)
- [Known Limitations](#known-limitations)
- [Repository Structure](#repository-structure)
- [Setup and Reproduction](#setup-and-reproduction)
- [Future Improvements](#future-improvements)
- [Skills Demonstrated](#skills-demonstrated)
- [Author](#author)

## Project Overview

Retail businesses generate data from sales transactions, stores and products. This project builds a pipeline that:

- Ingests only new or updated records from Azure SQL Database using watermarks.
- Stores raw extracts in Azure Data Lake Storage Gen2 as Parquet files.
- Loads them into Delta tables with Databricks Auto Loader (Bronze).
- Cleanses, deduplicates and validates the data, quarantining bad records (Silver).
- Produces business KPI tables for analytical reporting (Gold).
- Organises all tables under Unity Catalog.

## Technology Stack

| Technology | Purpose |
|---|---|
| Azure SQL Database | Source data and watermark table |
| Azure Data Factory | Ingestion and orchestration |
| Azure Data Lake Storage Gen2 | Landing storage |
| Azure Databricks | Data processing |
| PySpark | Transformation and cleansing |
| Delta Lake | Reliable table storage and MERGE operations |
| Unity Catalog | Data governance and organisation |
| Parquet (Snappy compression) | Landing file format |
| SQL | Source queries, stored procedure, watermark management |
| Git and GitHub | Version control |

## Source Data

Three retail tables in Azure SQL Database:

| Table | Description |
|---|---|
| `raw_sales` | Sales transactions: sale ID, sale time, store ID, product ID, quantity, unit price, discount, amount, payment mode, sales channel |
| `dim_stores` | Store information |
| `dim_products` | Product information |

## Azure Data Factory Implementation

Three pipelines were developed.

### 1. `data_ingestion_pipeline`

Metadata-driven, watermark-based incremental ingestion from Azure SQL Database to ADLS Gen2.

| Activity | Purpose |
|---|---|
| Lookup (watermark) | Reads the last processed watermark for each table from a SQL metadata table |
| ForEach | Iterates over the source tables so one pipeline serves all of them |
| Lookup (upper watermark) | Reads the latest `updated_at` value from the source table |
| If Condition | Runs the copy only if new or updated records exist |
| Copy Data | Copies the incremental range to ADLS Gen2 as timestamped, Snappy-compressed Parquet |
| Stored Procedure | Updates the watermark after a successful copy |

Dynamic SQL queries use lower and upper watermark values, and destination folder and file names are parameterised per table.

The If Condition compares source and stored watermarks:

```
@greater(
    ticks(activity('Lookup_Upper_Watermark').output.firstRow.upper_watermark),
    ticks(item().watermark_value)
)
```

### 2. `retail_master_pipeline`

Runs three Databricks Notebook activities in order, controlled by activity dependencies:

1. Bronze ingestion
2. Silver cleansing and transformation
3. Gold business aggregations

Failure paths are configured. See [Known Limitations](#known-limitations).

### 3. `orchestration_pipeline`

Uses Execute Pipeline activities to coordinate the full workflow:

1. Run `data_ingestion_pipeline`.
2. Wait for it to complete.
3. Run `retail_master_pipeline`.

A time-based Schedule Trigger was configured and tested (five-minute interval). It is now stopped.

## Storage Layer: ADLS Gen2

```text
landing/
├── raw_sales/
│   └── sales_data.parquet
├── dim_stores/
│   └── stores_data.parquet
└── dim_products/
    └── products_data.parquet
```

## Medallion Architecture in Databricks

### Bronze: raw ingestion

Notebook: `01_Bronze_Ingestion`

- Ingests Parquet files from ADLS Gen2 using Databricks Auto Loader (`cloudFiles`) with `availableNow=True`.
- Uses separate schema and checkpoint locations per source table.
- Adds metadata columns such as `_ingested_at` and `_source_file`.
- Writes Delta tables to Unity Catalog:
  - `retail_catalog.bronze.raw_sales`
  - `retail_catalog.bronze.dim_stores`
  - `retail_catalog.bronze.dim_products`

### Silver: cleansing and transformation

Notebook: `02_Silver_Data_Cleansing`

- Removes duplicate sales records using window functions.
- Handles nulls, invalid amounts and missing payment modes.
- Writes rejected records to a quarantine table with rejection reasons.
- Cleanses store and product dimensions and joins them with sales.
- Applies incremental changes with Delta MERGE.
- Demonstrates SCD Type 1 and Type 2 concepts for store dimensions (not a production-hardened dimension pipeline).
- Tables:
  - `retail_catalog.silver.sales`
  - `retail_catalog.silver.dim_stores`
  - `retail_catalog.silver.quarantine_sales`

### Gold: business aggregations

Notebook: `03_Gold_Business_Aggregations`

| Gold table | Business purpose |
|---|---|
| `retail_catalog.gold.overall_business_kpis` | Overall retail performance |
| `retail_catalog.gold.revenue_by_category` | Revenue by product category |
| `retail_catalog.gold.revenue_by_city` | Revenue by city |
| `retail_catalog.gold.revenue_by_payment_mode` | Revenue by payment method |
| `retail_catalog.gold.revenue_by_region` | Revenue by region |

## Data Governance: Unity Catalog

- Catalog: `retail_catalog`, with schemas `bronze`, `silver` and `gold`.
- ADLS Gen2 access is integrated through an Azure Databricks Access Connector using a managed identity, a storage credential and an external location.
- Bronze, Silver and Gold tables are registered in Unity Catalog.

## Incremental Loading Explained

Each source table has an `updated_at` timestamp. A watermark table stores the last processed timestamp per table.

1. **Lookup** reads the stored (lower) watermark.
2. A second **Lookup** reads the latest source timestamp (upper watermark).
3. **If Condition** checks whether the upper watermark is newer.
4. **Copy Data** extracts only records between the two watermarks.
5. A **Stored Procedure** saves the upper watermark as the new stored value.

Example: if the stored watermark is November 2 and the latest source timestamp is November 4, the copy runs for that range. If there is no newer timestamp, the copy is skipped.

## Validation and Testing

- Validated Bronze ingestion with 33 sales records.
- Tested incremental ingestion with a new transaction (`TXN-10033`) and confirmed one new record was ingested and the watermark updated.
- Validated Silver cleansing and duplicate handling.
- Tested the incremental MERGE with a synthetic test record.
- Verified the empty incremental batch scenario.
- Validated the final Gold KPI results.

## Sample Results

Recorded during a test run on the sample dataset:

| Metric | Result |
|---|---:|
| Sales records | 33 |
| Unique sales records | 33 |
| Total orders | 33 |
| Total units sold | 77 |
| Total revenue | 789,404.85 |
| Average order value | 23,921.36 |
| UPI revenue percentage | 40.02% |

These are results from a recorded test run, not a guarantee of the current state of the Azure environment.

## Project Status

| Component | Status |
|---|---|
| Azure SQL source tables | Implemented |
| ADF incremental ingestion (`data_ingestion_pipeline`) | Implemented and tested |
| ADLS Gen2 landing layer | Implemented |
| Bronze Auto Loader ingestion | Implemented |
| Silver cleansing, quarantine and MERGE | Developed and tested individually |
| Gold aggregation tables | Developed; results recorded |
| Unity Catalog configuration | Implemented |
| `retail_master_pipeline` and `orchestration_pipeline` | Configured; final end-to-end run not verified |
| Schedule Trigger | Configured and tested; now stopped |
| Failure paths | Configured; Fail activity execution not confirmed |
| Retry logic, alerting, production monitoring | Not implemented |

## Known Limitations

- The final recorded ADF master pipeline run ended with a Silver notebook failure. The complete workflow needs to be revalidated before it is described as verified end to end.
- Full incremental propagation from ADF through Silver and Gold needs further validation.
- Failure paths were configured, but execution of the Fail activity was not conclusively verified.
- Automated failure notifications, retry and recovery strategy, and production monitoring are not implemented.
- The Schedule Trigger is stopped.
- The Azure environment was temporary (free trial). The repository contains source definitions and notebook exports, not a running deployment.
- The dataset is small (33 sales records) and meant for demonstrating patterns, not performance at scale.

## Repository Structure

```text
azure-retail-data-engineering-pipeline/
│
├── ADF/
│   ├── data_ingestion_pipeline.json
│   ├── retail_master_pipeline.json
│   └── orchestration_pipeline.json
│
├── Databricks/
│   ├── 01_Bronze_Ingestion.py
│   ├── 02_Silver_Data_Cleansing.py
│   └── 03_Gold_Business_Aggregations.py
│
├── SQL/
│   ├── create_tables.sql
│   ├── watermark_procedure.sql
│   └── test_data.sql
│
├── Screenshots/
├── Documentation/
│
└── README.md
```

## Setup and Reproduction

Requirements: an Azure subscription, Azure SQL Database, Azure Data Factory, ADLS Gen2, and a Unity Catalog-enabled Azure Databricks workspace.

1. Create the Azure resources and the three source tables, plus the watermark table and stored procedure (`SQL/`).
2. Configure ADF linked services and datasets.
3. Configure ADLS Gen2 storage and permissions.
4. Import the ADF pipeline definitions.
5. Create the Unity Catalog objects: catalog, schemas, Access Connector, storage credential and external location.
6. Import the three Databricks notebooks.
7. Set environment-specific paths, credentials and checkpoint locations securely. Do not commit secrets.
8. Run the ingestion pipeline, then the Databricks notebooks.
9. Validate the Silver and Gold tables.

## Future Improvements

- Fix the Silver failure and complete a verified end-to-end run.
- Add retry logic, failure notifications and a run-level audit table.
- Add pipeline monitoring and alerting.
- Add advanced data quality checks.
- Add CI/CD for ADF and Databricks.

## Skills Demonstrated

Azure Data Factory, ETL pipeline development, metadata-driven pipelines, incremental loading, watermark management, dynamic SQL and expressions, Lookup / ForEach / If Condition / Copy / Stored Procedure / Execute Pipeline / Notebook activities, pipeline dependencies, schedule triggers, ADLS Gen2, Parquet, Databricks Auto Loader, PySpark, Delta Lake (MERGE), Medallion Architecture, data cleansing and quarantine handling, SCD Type 1 and 2 concepts, Unity Catalog, managed identity, Git and GitHub.

## Author

**Tejas Waghmare**
Computer Engineering Graduate | Aspiring Data Engineer
Pune, Maharashtra

[LinkedIn](https://linkedin.com/in/tejas-waghmare15) | [GitHub](https://github.com/Tejass555)
