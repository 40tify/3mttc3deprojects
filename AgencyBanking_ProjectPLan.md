# Agency Banking & Mobile Money Lakehouse Data Pipeline - Project Plan

> [!IMPORTANT]
> **Project Goal**: Build a governed, end-to-end Lakehouse pipeline using Apache Airflow, Google Cloud Storage (GCS), BigQuery, Apache Iceberg, and OpenMetadata based on [`Project-Guide.md`](./docs/Project-Guide.md).

---

## 1. Project Overview & Architecture

### Business Problem Statement
This project simulates an agency banking and mobile money platform coordinating a nationwide network of POS agents processing financial transactions for underbanked populations. The fintech organization faces challenges with daily liquidity tracking, agent commission settlement accuracy, network performance monitoring, and compliance auditing.

The Lakehouse pipeline ingests raw transactional flat files, performs ELT transformations in BigQuery, exposes serving data marts to business intelligence tools, logs execution audits, offloads aged data to open-format Apache Iceberg tables, and maintains end-to-end governance via OpenMetadata.

### End-to-End Lakehouse Architecture

```mermaid
flowchart TD
    subgraph Orchestration["⚡ Airflow Orchestration Engine"]
        DAG0["DAG 0: One-Time Dimension Loader (@once)"]
        DAG1["DAG 1: Landing Data Generator (@daily)"]
        DAG2["DAG 2: Core ELT Pipeline (@daily)"]
        DAG3["DAG 3: Iceberg Archival (@monthly)"]
    end

    subgraph GCS["☁️ GCS Lakehouse Storage"]
        GCS_TMP["📁 temp/<br><code>dim_*.csv</code>"]
        GCS_LND["📁 landing/<br><code>lnd_YYYYMMDD_1..3.csv</code>"]
        GCS_ARC["📁 archival/<br><code>Processed CSV files</code>"]
        GCS_ICE["📁 iceberg/<br><code>Iceberg Parquet + Metadata</code>"]
    end

    subgraph BQ["🏢 BigQuery Data Warehouse Layer"]
        BQ_DIM["🗂️ john_dw_core_dataset.dim_*<br><i>(Dimensions)</i>"]
        BQ_LND["📥 john_lnd_stg_dataset.lnd_*<br><i>(Raw Landing, 7-Day TTL)</i>"]
        BQ_STG["⚙️ john_lnd_stg_dataset.stg_*<br><i>(Enriched Staging, 7-Day TTL)</i>"]
        BQ_FACT["📊 john_dw_core_dataset.fact_*<br><i>(Partitioned & Clustered Fact)</i>"]
        BQ_VIEWS["📈 john_dw_analytics_dataset.vw_*<br><i>(Serving Views)</i>"]
        BQ_LOG["📜 john_dw_core_dataset.pipeline_execution_logs<br><i>(Audit Logs)</i>"]
    end

    subgraph ARCH["❄️ Iceberg Archival Layer"]
        ICE_REST["Lakekeeper REST Catalog"]
        ICE_TAB["Iceberg Archival Tables"]
    end

    subgraph GOV["🛡️ Data Governance"]
        OMD["OpenMetadata Hub<br><i>(Lineage • Data Dictionary • DQ Suites)</i>"]
    end

    DAG0 -->|Generate & Upload| GCS_TMP
    GCS_TMP -->|One-Time Load| BQ_DIM

    DAG1 -->|Daily 3 CSV Files| GCS_LND

    GCS_LND -->|1. Load Flat Files| BQ_LND
    DAG2 -->|Orchestrates ELT| BQ_LND
    BQ_LND -->|2. SQL Transform & Enrich| BQ_STG
    BQ_DIM -.->|Join Lookups| BQ_STG
    BQ_STG -->|3. Merge & Upsert| BQ_FACT
    BQ_FACT -->|4. Refresh BI Views| BQ_VIEWS
    BQ_FACT & BQ_STG & BQ_LND -->|5. Write Audit Events| BQ_LOG
    DAG2 -->|6. Move Processed CSVs| GCS_ARC

    DAG3 -->|7. Query Aged Rows| BQ_FACT
    DAG3 -->|8. Export & Write| ICE_REST
    ICE_REST -->|Write Parquet| GCS_ICE
    ICE_REST -->|Metadata| ICE_TAB

    BQ & ICE_TAB & Airflow -->|Ingest Metadata & Lineage| OMD
```

---

## 2. Configuration Parameters

| Parameter | Value | Description |
| :--- | :--- | :--- |
| **GCP Project ID** | `your-gcp-project-id` | Target Google Cloud Project |
| **GCS Bucket** | `gs://3mtt-lakehouse-agencybanking/` | Primary Lakehouse Storage Bucket |
| **BigQuery Dataset (Landing/Staging)**| `john_lnd_stg_dataset` (7-day TTL) | Temporary ingestion & staging dataset |
| **BigQuery Dataset (Core Fact/Dims)**| `john_dw_core_dataset` | Permanent warehouse storage |
| **BigQuery Dataset (Serving Views)** | `john_dw_analytics_dataset` | Business intelligence layer views |
| **Audit Log Table** | `john_dw_core_dataset.pipeline_execution_logs` | Central execution tracking log |
| **Iceberg Catalog** | Lakekeeper REST Catalog | Archival catalog manager |
| **Governance Platform** | OpenMetadata Instance | Lineage, data dictionary & DQ monitor |

---

## 3. Data Model & Schemas

### Dimension Tables (`john_dw_core_dataset`)
All dimension tables include `insert_timestamp` and `update_timestamp` columns (defaulting to `CURRENT_TIMESTAMP()`) to support backups, delta loading, and incremental updates.

1. **`dim_agents`**:
   - `agent_id` (INT64, PK), `agent_name` (STRING), `business_name` (STRING), `terminal_id` (STRING), `tier_level` (STRING), `signup_date` (DATE), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)
2. **`dim_customers`**:
   - `customer_id` (INT64, PK), `customer_phone` (STRING), `kyc_status` (STRING), `account_type` (STRING), `registration_date` (DATE), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)
3. **`dim_transaction_types`**:
   - `txn_type_id` (INT64, PK), `txn_name` (STRING), `direction` (STRING), `is_financial` (BOOLEAN), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)
4. **`dim_geography`**:
   - `geo_id` (INT64, PK), `location_cluster` (STRING), `lga` (STRING), `state` (STRING), `region` (STRING), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)

### Landing Table (`john_lnd_stg_dataset.lnd_daily_transactions`)
- Raw ingestion table populated directly from GCS flat files (`lnd_YYYYMMDD_1..3.csv`).
- Schema: `txnid` (STRING), `createdat` (STRING), `terminalid` (STRING), `custphone` (STRING), `txntypecode` (INT64), `amount` (NUMERIC), `fee_charged` (NUMERIC), `status` (STRING)

### Staging Table (`john_lnd_stg_dataset.stg_daily_transactions`)
- Created via `CREATE OR REPLACE TABLE` in SQL during DAG 2 ELT execution.
- Captures both successful and failed transactions (previously discarded).
- Performs dimension lookups, string parsing, type casting, and conditionally calculates `agent_commission` (70% of transaction fee ONLY for successful transactions; 0 for failed transactions).

### Successful Fact Table (`john_dw_core_dataset.fact_daily_transactions`)
- Stores successful financial events. Partitioned by `transaction_date` (DATE) and clustered by `agent_id`, `geo_id`.
- Schema: `transaction_id` (STRING, PK), `transaction_timestamp` (TIMESTAMP), `transaction_date` (DATE), `agent_id` (INT64, FK), `geo_id` (INT64, FK), `customer_id` (INT64, FK), `txn_type_id` (INT64, FK), `transaction_amount` (NUMERIC), `fee_charged` (NUMERIC), `agent_commission` (NUMERIC), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)

### Failed Fact Table (`john_dw_core_dataset.fact_daily_failed_transactions`)
- Stores failed operational events for debugging and reliability tracking. Partitioned by `transaction_date` (DATE) and clustered by `agent_id`, `geo_id`.
- Schema: `transaction_id` (STRING, PK), `transaction_timestamp` (TIMESTAMP), `transaction_date` (DATE), `agent_id` (INT64, FK), `geo_id` (INT64, FK), `customer_id` (INT64, FK), `txn_type_id` (INT64, FK), `transaction_amount` (NUMERIC), `fee_charged` (NUMERIC), `transaction_status` (STRING), `insert_timestamp` (TIMESTAMP), `update_timestamp` (TIMESTAMP)

### Serving Views (`john_dw_analytics_dataset`)
1. **`vw_agent_performance`**: Summarizes total transactions, transaction volume, gross fees, and agent commission earnings by agent tier and cluster.
2. **`vw_daily_liquidity_summary`**: Analyzes cash deposit (`IN`) vs cash withdrawal (`OUT`) volumes by LGA/state for daily cash rebalancing.
3. **`vw_kyc_compliance_risk`**: Flags high-value transactions conducted by `UNREGISTERED` or `PENDING` KYC accounts.

---

## 4. Pipeline Execution Workflow

### Configuration & Secret Management (Best Practice)
All DAGs connect to Google Cloud Storage using an Airflow Connection `gcp_hmac_conn` (storing the GCP HMAC access key and secret key) and an Airflow Variable `gcs_bucket_name` (storing the GCS bucket name). These are automatically seeded upon container startup using `hmac_credentials.env` inside the `airflow-init` container.

### DAG 0: One-Time Dimension Loader (`@once`)
- Generates static/seed dimension CSVs (`dim_agents`, `dim_customers`, `dim_transaction_types`, `dim_geography`).
- Uploads CSVs to `gs://.../temp/` via `gcp_hmac_conn`.
- Executes one-time load into BigQuery `john_dw_core_dataset.dim_*` tables.

### DAG 1: Daily Landing Data Generator (`@daily`)
- **Staging Sync & Dimension Dependency**: Prior to generating daily operational logs, it runs `ensure_dimension_files_local()` to verify that all seed dimension CSVs (`dim_customers.csv`, `dim_agents.csv`, `dim_transaction_types.csv`, `dim_geography.csv`) are present locally (downloading them automatically from GCS `temp/` if needed). If any prerequisite dimension file is missing, DAG 1 raises an error enforcing that DAG 0 (`@once`) has completed first.
- **Strict Referential Integrity**: Samples customer phone numbers from `dim_customers.csv`, terminal IDs from `dim_agents.csv`, and transaction types from `dim_transaction_types.csv` with zero random fallback phone numbers.
- Simulates daily POS operational logs (injecting 10% bad data for staging schema validation).
- Outputs exactly 3 deterministic CSV files per daily logical date: `landing/lnd_YYYYMMDD_1.csv`, `landing/lnd_YYYYMMDD_2.csv`, `landing/lnd_YYYYMMDD_3.csv`.
- Uploads them to GCS `landing/` using `gcp_hmac_conn`.
- Guarantees idempotency (re-running overwrites exact 3 files).

### DAG 2: Core ELT Pipeline (`@daily`)
- **Step 1: Direct Load**: Reads GCS `landing/` CSVs for `logical_date` into BigQuery `john_lnd_stg_dataset.lnd_daily_transactions`.
- **Step 2: SQL Staging Transform**: Runs `CREATE OR REPLACE TABLE stg_daily_transactions` staging all transactions (both `SUCCESS` and `FAILED`). Derives `agent_commission` conditionally (only successful transactions earn commission).
- **Step 3: Double Fact Upsert/Merge**: Runs parallel SQL `MERGE` statements:
  * Successful staging transactions are merged into `john_dw_core_dataset.fact_daily_transactions`.
  * Failed staging transactions are merged into `john_dw_core_dataset.fact_daily_failed_transactions`.
  * Audit columns `insert_timestamp` and `update_timestamp` are updated accordingly.
- **Step 4: Audit Metrics Logging**: Appends task execution row to `john_dw_core_dataset.pipeline_execution_logs`.
- **Step 5: Refresh Serving Views**: Refreshes serving views in `john_dw_analytics_dataset`.
- **Step 6: File Hygiene & Archival**: Moves ingested flat files from GCS `landing/` to `archival/` using `gcp_hmac_conn`.

### DAG 3: Iceberg Archival Pipeline (`@monthly`)
- Queries fact table records older than retention period ($N$ months).
- Overwrites target monthly Apache Iceberg partitions in GCS `iceberg/` via Lakekeeper REST Catalog.

---

## 5. Audit Logging Specification

Every pipeline execution task appends audit events to `john_dw_core_dataset.pipeline_execution_logs`:

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `run_id` | `STRING` | Airflow DAG run execution ID |
| `logical_date` | `DATE` | Pipeline logical date timestamp |
| `task_id` | `STRING` | Name of the executed Airflow task |
| `target_table` | `STRING` | Target dataset/table updated |
| `rows_processed` | `INT64` | Number of inserted / modified rows |
| `execution_status` | `STRING` | `SUCCESS` \| `FAILED` \| `RETRY` |
| `created_at` | `TIMESTAMP` | Logging event timestamp |

---

## 6. Data Governance with OpenMetadata

- **Visual Lineage**: GCS Flat Files -> Airflow DAG Tasks -> BQ Landing/Staging/Fact -> Serving Views & Iceberg Archives.
- **Unified Catalog**: Tagged schemas, column descriptions, and metadata across BigQuery and Iceberg open formats.
- **Data Quality Suites**: Automated DQ tests checking null constraints, primary key uniqueness, and value range assertions.
