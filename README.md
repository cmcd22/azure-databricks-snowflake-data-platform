🚀 End-to-End Azure Data Platform (Bronze–Silver–Gold)

📌 Overview

This project implements a modern end-to-end data platform using Azure services and Databricks, following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline ingests data from a public API, processes it using Apache Spark in Databricks, and produces analytics-ready datasets.

⸻

🧱 Architecture
	•	Ingestion: Azure Data Factory (ADF)
	•	Storage: Azure Data Lake Storage Gen2 (ADLS)
	•	Processing: Azure Databricks (PySpark)
	•	Governance: Unity Catalog (Managed Identity)
	•	Orchestration: ADF Pipelines + Scheduled Trigger

⸻

🔄 Pipeline Flow
	1.	ADF extracts data from DummyJSON API
	2.	Raw data is stored in Bronze layer (JSON)
	3.	Databricks notebook transforms data into structured Silver layer (Parquet)
	4.	Further transformations create Gold layer (star schema)
	5.	ADF orchestrates the full pipeline and runs on a schedule

⸻

🏗️ Medallion Architecture

🥉 Bronze
	•	Raw API data (JSON)
	•	Partitioned by ingestion_date

🥈 Silver
	•	Cleaned and structured data
	•	Partitioned by ingestion_date
	•	Data quality checks applied

🥇 Gold
	•	Analytics-ready star schema:
	•	dim_products
	•	dim_customers
	•	fact_orders
	•	fact_order_items

⸻

⚙️ Data Quality Checks

Implemented in Databricks notebooks:
	•	Null checks on key fields
	•	Uniqueness constraints (primary keys)
	•	Non-negative validation (prices, quantities)
	•	Referential integrity between tables

Pipeline fails automatically if checks do not pass.

⸻

📅 Incremental Processing
	•	Pipeline is parameterised by ingestion_date
	•	ADF passes the run date into Databricks notebooks
	•	Only the relevant partition is processed per run

⸻

🔐 Security
	•	Azure Databricks uses Managed Identity
	•	Access to ADLS controlled via Unity Catalog
	•	No storage keys or secrets exposed

⸻

⏱️ Automation
	•	ADF trigger runs pipeline daily
	•	Fully automated ingestion → transformation → output

⸻

📊 Sample Output

Gold layer provides:
	•	Customer-level data
	•	Product catalog
	•	Order and order item facts

⸻

🚀 How to Run
	1.	Trigger ADF pipeline manually OR wait for scheduled trigger
	2.	Pipeline ingests data from API
	3.	Databricks notebooks process data
	4.	Output available in ADLS Gold layer

⸻

🛠️ Tech Stack
	•	Azure Data Factory
	•	Azure Data Lake Storage Gen2
	•	Azure Databricks (PySpark)
	•	Unity Catalog
	•	Python

⸻

📌 Key Highlights
	•	End-to-end cloud data platform
	•	Medallion architecture implementation
	•	Parameter-driven incremental processing
	•	Data quality validation
	•	Fully automated pipeline
