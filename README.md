# 🚀 End-to-End Azure + Snowflake Data Platform (Bronze–Silver–Gold)

## 📌 Overview
This project implements a modern, end-to-end data platform using Azure services, Databricks, and Snowflake, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests data from a public API, processes it using Apache Spark, applies data quality validation, and publishes analytics-ready data into Snowflake for downstream use.

---

## 🧱 Architecture

- **Ingestion & Orchestration**: Azure Data Factory (ADF)
- **Storage**: Azure Data Lake Storage Gen2 (ADLS)
- **Processing**: Azure Databricks (PySpark)
- **Governance**: Unity Catalog (Managed Identity)
- **Data Warehouse**: Snowflake
- **Automation**: ADF Scheduled Trigger

---

## 🔄 End-to-End Pipeline Flow

1. ADF extracts data from the DummyJSON API  
2. Raw data is stored in **Bronze layer (ADLS)**  
3. Databricks transforms data into **Silver layer (cleaned Parquet)**  
4. Databricks builds **Gold layer (star schema)**  
5. Gold data is written to **Snowflake**  
6. Snowflake serves analytics-ready tables and views  
7. ADF orchestrates and schedules the entire workflow  

---

## 🏗️ Medallion Architecture

### 🥉 Bronze Layer
- Raw API data (JSON)
- Stored in ADLS
- Partitioned by `ingestion_date`

### 🥈 Silver Layer
- Cleaned and structured data
- Stored as Parquet
- Partitioned by `ingestion_date`
- Data quality checks applied

### 🥇 Gold Layer (ADLS + Snowflake)
- Analytics-ready star schema:
  - `dim_products`
  - `dim_customers`
  - `fact_orders`
  - `fact_order_items`
- Stored in ADLS and loaded into Snowflake

---

## ❄️ Snowflake Analytics Layer

Gold tables are loaded into Snowflake:

- `FAKESTORE_DB.GOLD.DIM_PRODUCTS`
- `FAKESTORE_DB.GOLD.DIM_CUSTOMERS`
- `FAKESTORE_DB.GOLD.FACT_ORDERS`
- `FAKESTORE_DB.GOLD.FACT_ORDER_ITEMS`

Example analytics view:

```sql
CREATE VIEW gold.v_customer_orders AS
SELECT
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_total
FROM gold.fact_orders o
JOIN gold.dim_customers c
    ON o.customer_id = c.customer_id;
```

---

## ⚙️ Data Quality Checks

Implemented in Databricks notebooks:

- Null checks on key columns
- Primary key uniqueness validation
- Non-negative checks (price, quantity, totals)
- Referential integrity between fact and dimension tables

The pipeline fails automatically if any check does not pass.

---

## 📅 Incremental Processing

- Pipeline is parameterised by `ingestion_date`
- ADF passes the run date into Databricks notebooks
- Data is processed per partition
- Silver layer is partitioned by ingestion date

---

## 🔐 Security

- Azure Databricks uses **Managed Identity**
- ADLS access controlled via **Unity Catalog**
- Snowflake credentials managed securely (recommended via secrets)

---

## ⏱️ Automation

- ADF trigger runs the pipeline daily
- Fully automated ingestion → transformation → load to Snowflake

---

## 🚀 How to Run

1. Trigger ADF pipeline manually or wait for scheduled trigger  
2. ADF ingests data into Bronze layer  
3. Databricks processes data into Silver and Gold layers  
4. Gold data is written to Snowflake  
5. Query results in Snowflake  

---

## 🛠️ Tech Stack

- Azure Data Factory  
- Azure Data Lake Storage Gen2  
- Azure Databricks (PySpark)  
- Unity Catalog  
- Snowflake  
- Python  

---

## 🏆 Key Features

- End-to-end cloud data platform  
- Medallion architecture implementation  
- Parameter-driven incremental processing  
- Data quality validation layer  
- Fully automated pipeline  
- Integration with Snowflake data warehouse  
