# 🚀 E-Commerce Hybrid ELT Lakehouse Pipeline on Google Cloud

![GCP](https://img.shields.io/badge/Cloud-GCP-4285F4?logo=googlecloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Spark](<https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white>)
![dbt](https://img.shields.io/badge/Transform-dbt-FF694B?logo=dbt&logoColor=white)
![Delta Lake](<https://img.shields.io/badge/Table%20Format-Delta%20Lake-00ADD8>)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A complete, production-ready Hybrid ELT Lakehouse architecture built natively on Google Cloud Platform (GCP). This repository demonstrates an enterprise-grade data engineering pipeline that processes streaming E-Commerce data using a **Multi-Layered Medallion Architecture (Bronze → Silver → Gold)**, featuring built-in Data Quality controls, Change Data Capture (CDC), and a Dead Letter Queue (DLQ) mechanism.

## 📑 Table of Contents

- [Technical Architecture Diagram](#️-technical-architecture-diagram)
- [Component Breakdown &amp; Data Flow](#️-component-breakdown--data-flow)
- [Multi-Layer Defense in Action](#️-multi-layer-defense-in-action-error-handling)
- [Technology Stack](#️-technology-stack)
- [Project Structure](#-project-structure)
- [How to Run](#-how-to-run)
- [License](#-license)

## 🏗️ Technical Architecture Diagram

```mermaid
flowchart TD
    SCR["Script generate data - Faker - E-commerce Olist"] -->|"orders, customers, products, sellers..."| PUB["Cloud Pub/Sub Topic"]
    PUB -->|T-Branching| BEAM["Apache Beam (Google Cloud Dataflow)"]
  
    %% Branch 1: Speed Layer (Real-time)
    BEAM -->|Branch 1: 30s Window| RTAGG["Real-time Aggregation - Count/Revenue"]
    RTAGG -->|continuous write| RTFS[("Cloud Firestore - Live Dashboard")]
    RTFS -.|onSnapshot listener.-> RTDASH["Live Dashboard - Web/Mobile realtime UI"]
  
    %% Branch 2: Batch Layer (Master Data)
    BEAM -->|Branch 2: Raw Dump| STG[("GCS Staging Layer - Landing Zone - Raw JSON")]
    STG -->|Trigger AvailableNow| SPARK1["PySpark Batch Job - Dataproc - Staging to Bronze"]
    SPARK1 --> CHK{"Validate Schema"}
    CHK -->|pass| BRONZE[("GCS Bronze Layer - Delta Lake, cleaned raw data")]
    CHK -->|schema error / null| DLQ[("GCS Dead Letter Queue")]
    BRONZE --> SPARK2["Spark Structured Streaming - Continuous Bronze to Silver"]
    SPARK2 --> SILVER[("GCS Silver Layer - Delta Lake, deduplicated + normalized")]
    SILVER -->|BigLake external table| BQ["BigQuery"]
    BQ --> DBT["dbt Transformations ELT"]
    DBT --> GOLD[("BigQuery Gold - Fact + Dimension table")]
    GOLD --> IAM["Cloud IAM - RBAC + Audit Log"]
    IAM --> BI["Power BI"]

    subgraph OBS["System Observability and Monitoring"]
        MON1["Raw Data Monitor"]
        MON2["Bronze Quality Monitor"]
        MON3["Silver Performance Monitor"]
        LOGAGG["Log Aggregation Job - Cloud Logging"]
        MON1 --> LOGAGG
        MON2 --> LOGAGG
        MON3 --> LOGAGG
    end
    SCR -.log input.-> MON1
    SPARK1 -.log QC.-> MON2
    SPARK2 -.log throughput.-> MON3
    LOGAGG --> ALERT["Slack / Discord Alerts"]

    AF(("Cloud Composer Airflow")) -.trigger.-> SPARK2
    AF -.trigger.-> DBT
    AF -.scheduled trigger.-> LOGAGG

    classDef ingest fill:#f1efe8,stroke:#5f5e5a,stroke-width:0.5px;
    classDef job fill:#e6f1fb,stroke:#185fa5,stroke-width:0.5px;
    classDef storage fill:#faeeda,stroke:#854f0b,stroke-width:0.5px;
    classDef check fill:#eeedfe,stroke:#534ab7,stroke-width:0.5px;
    classDef error fill:#fcebeb,stroke:#a32d2d,stroke-width:0.5px;
    classDef monitor fill:#faece7,stroke:#993c1d,stroke-width:0.5px,stroke-dasharray: 4 2;
    classDef orch fill:#eaf3de,stroke:#3b6d11,stroke-width:0.5px;
    classDef bi fill:#e6f1fb,stroke:#185fa5,stroke-width:0.5px;
    classDef realtime fill:#e8f7ee,stroke:#1a7a41,stroke-width:0.5px;

    class SCR,PUB ingest
    class BEAM,SPARK1,SPARK2,DBT,LOGAGG job
    class STG,BRONZE,SILVER,GOLD,BQ storage
    class CHK check
    class DLQ error
    class MON1,MON2,MON3 monitor
    class AF orch
    class IAM,BI,ALERT bi
    class RTAGG realtime
    class RTFS realtime
    class RTDASH bi
```

---

## ⚙️ Component Breakdown & Data Flow

### 1. Ingestion Layer (Pub/Sub & Dataflow)

- **Data Generator:** A custom Python script (`stream_to_pubsub.py`) uses the Faker library to simulate an Olist-style E-commerce system (8 entities including Customers, Orders, CDC Shipments, etc.).
- **Message Broker (Pub/Sub):** Receives the high-throughput, real-time raw data stream.
- **Staging / Landing Zone (GCS):** Dataflow (or local puller script) consumes messages from Pub/Sub and buffers them into NDJSON files stored in GCS `staging/`. This solves the "Small Files Problem" early in the pipeline.

### 2. Processing Layer: Dataproc (Apache Spark & Delta Lake)

- **Bronze Layer (Schema Validation):**
  - `stream_raw_to_bronze.py` leverages Spark Structured Streaming with `Trigger.AvailableNow` to process Staging data.
  - 🛡️ **Schema Defense:** Uses Spark's `PERMISSIVE` mode. Records failing schema enforcement (e.g., passing a String into a Float column) are caught via `_corrupt_record` and isolated into the **Dead Letter Queue (DLQ)**. Valid records are appended to the Bronze Delta tables.
- **Silver Layer (Data Quality & CDC Deduplication):**
  - `stream_bronze_to_silver.py` streams from Bronze to Silver.
  - 🛡️ **Data Quality Defense:** Imputes missing values (e.g., filling `Null` emails with a default string) and performs deduplication on CDC events using `Window` functions to retain only the latest state (e.g., Shipment statuses).
  - Uses Delta Lake `MERGE` (Upsert) to maintain an accurate Silver state.

### 3. Analytics Layer: dbt & BigQuery (Gold)

- **BigLake Integration:** BigQuery accesses the Silver Delta Lake files on GCS via External Tables, achieving a true zero-copy architecture.
- **dbt (Data Build Tool):** Executes ELT transformations directly inside BigQuery.
  - 🛡️ **Business Logic Defense:** `stg_order_items.sql` enforces business rules (e.g., `WHERE price > 0`) to filter out anomalies that were structurally valid but logically incorrect.
  - Generates the final Star Schema (Facts and Dimensions) optimized for BI consumption.

---

## 🛡️ Multi-Layer Defense in Action (Error Handling)

This project intentionally generates a small percentage of corrupted data to demonstrate the robustness of the Medallion architecture:

1. **Schema Errors (1-2%)**: Data Generator produces Type Mismatches (e.g., Dict instead of String).
   - **Result**: Caught by **Bronze Layer** (PySpark) -> Routed to **DLQ** as raw JSON.
2. **Missing Values (1-2%)**: Data Generator omits required fields (e.g., Null Emails).
   - **Result**: Passed by Bronze, caught by **Silver Layer** (PySpark) -> Automatically imputed/cleaned via Data Quality rules.
3. **Business Logic Errors (1-2%)**: Data Generator produces negative prices (`-999.99`).
   - **Result**: Passed by Bronze & Silver, caught by **Gold Layer** (dbt) -> Filtered out via SQL `WHERE` clauses before analytical aggregations.

---

## 🛠️ Technology Stack

| Category                        | Technology                     |
| ------------------------------- | ------------------------------ |
| **Cloud Provider**        | Google Cloud Platform (GCP)    |
| **Message Broker**        | Cloud Pub/Sub                  |
| **Stream Processing**     | Cloud Dataflow (Apache Beam)   |
| **Data Lake Storage**     | Google Cloud Storage (GCS)     |
| **Data Processing (ETL)** | Dataproc Serverless (PySpark)  |
| **Table Format**          | Delta Lake (ACID Transactions) |
| **Data Warehouse**        | Google BigQuery + BigLake      |
| **Data Transformation**   | dbt (Data Build Tool)          |
| **Language**              | Python 3.10+, SQL              |

## 📂 Project Structure

```text
├── data_generator/         # Custom E-commerce Data Generator (8 Entities + CDC)
│   ├── config.py           # Generator configuration and schemas
│   ├── generators.py       # Entity generation logic (includes dirty data injection)
│   └── stream_to_pubsub.py # Publishes generated events to GCP Pub/Sub
│
├── spark_jobs/             # PySpark ETL Jobs for Dataproc Serverless
│   ├── config.py           # Spark configurations and table schemas
│   ├── stream_raw_to_bronze.py    # Staging (JSON) -> Bronze (Delta) + DLQ Routing
│   ├── stream_bronze_to_silver.py # Bronze -> Silver (Delta MERGE + Data Cleaning)
│   ├── pull_pubsub_to_raw.py      # Local script to pull Pub/Sub to Staging (Fallback)
│   └── deploy_to_dataproc.py      # Automates submitting PySpark batches
│
├── dbt_transform/          # dbt project for Gold Layer analytics
│   ├── dbt_project.yml
│   ├── profiles.yml        # Configured to target BigQuery (`lakehouse_gold`)
│   └── models/
│       ├── staging/        # Views on top of Silver External Tables (Business Filters)
│       └── marts/          # Fact & Dimension tables (Star Schema)
```

## 📊 Cloud Operations & Observability

To demonstrate the cloud-native capabilities of this pipeline, the system is fully integrated with Google Cloud operations suite. Below are the key operational views (Add your screenshots here):

### 1. Centralized Logging & Metrics (Cloud Monitoring)

The pipeline implements a **Zero-Overhead custom logging mechanism** in PySpark to track Data Quality (Bronze) and Processing Performance (Silver) in real-time.

### 2. Stream Processing (Dataflow)

<!-- ![Monitoring Dashboard](docs/monitoring_dashboard.png) -->

*![1786246510541](image/README/1786246510541.png)3. Serverless Spark Execution (Dataproc)*


<!-- ![Dataproc Batches](docs/dataproc_jobs.png) -->

### 4. Data Warehouse & Transformation (BigQuery + dbt)


<!-- ![BigQuery/dbt](docs/dbt_lineage.png) -->

---

## 🚀 How to Run

1. **Setup GCP & Infrastructure**: Ensure Pub/Sub, GCS bucket, and BigQuery datasets (`lakehouse_silver`, `lakehouse_gold`) are created.
2. **Start the Data Generator**:
   `python data_generator/stream_to_pubsub.py`
3. **Run Dataproc Spark Jobs**:
   `python spark_jobs/deploy_to_dataproc.py`
4. **Build Gold Layer (dbt)**:
   `cd dbt_transform && dbt run`
