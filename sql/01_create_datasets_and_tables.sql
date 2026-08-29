-- =============================================================================
-- SQL 01: Create Datasets and Table Schemas for Agency Banking Lakehouse
-- =============================================================================

-- 1. Create BigQuery Datasets
CREATE SCHEMA IF NOT EXISTS `john_lnd_stg_dataset`
OPTIONS(
  location="us-east1",
  description="Temporary ingestion and staging dataset with 7-day default table expiration",
  default_table_expiration_days=7
);

CREATE SCHEMA IF NOT EXISTS `john_dw_core_dataset`
OPTIONS(
  location="us-east1",
  description="Permanent core data warehouse dataset storing dimensions, fact tables, and pipeline execution audit logs"
);

CREATE SCHEMA IF NOT EXISTS `john_dw_analytics_dataset`
OPTIONS(
  location="us-east1",
  description="Business intelligence serving dataset containing analytical views"
);

-- -----------------------------------------------------------------------------
-- 2. Create Dimension Tables in john_dw_core_dataset
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.dim_geography` (
  geo_id INT64 NOT NULL,
  location_cluster STRING NOT NULL,
  lga STRING NOT NULL,
  state STRING NOT NULL,
  region STRING NOT NULL,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(description="Geographic region, state, LGA, and location cluster lookup dimension");

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.dim_agents` (
  agent_id INT64 NOT NULL,
  agent_name STRING NOT NULL,
  business_name STRING,
  terminal_id STRING NOT NULL,
  tier_level STRING NOT NULL,
  signup_date DATE,
  geo_id INT64,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(description="Banking agent profiles, assigned terminals, tier levels, and assigned geographic IDs");

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.dim_customers` (
  customer_id INT64 NOT NULL,
  customer_phone STRING NOT NULL,
  kyc_status STRING NOT NULL,
  account_type STRING NOT NULL,
  registration_date DATE,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(description="Registered customer details, KYC compliance status, and account types");

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.dim_transaction_types` (
  txn_type_id INT64 NOT NULL,
  txn_name STRING NOT NULL,
  direction STRING NOT NULL,
  is_financial BOOLEAN NOT NULL,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(description="Transaction classification lookup table defining payment direction and financial status");

-- -----------------------------------------------------------------------------
-- 3. Create Raw Landing Table in john_lnd_stg_dataset (7-day TTL)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `john_lnd_stg_dataset.lnd_daily_transactions` (
  txnid STRING,
  createdat STRING,
  terminalid STRING,
  custphone STRING,
  txntypecode INT64,
  amount NUMERIC,
  fee_charged NUMERIC,
  status STRING,
  batch_id STRING,
  loaded_at TIMESTAMP
)
OPTIONS(description="Raw landing table populated directly from GCS flat CSV files");

-- -----------------------------------------------------------------------------
-- 4. Create Core Fact Table in john_dw_core_dataset (Partitioned & Clustered)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.fact_daily_transactions` (
  transaction_id STRING NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  transaction_date DATE NOT NULL,
  agent_id INT64,
  agent_name STRING,
  terminal_id STRING NOT NULL,
  location_cluster STRING,
  lga STRING,
  state STRING,
  region STRING,
  customer_phone STRING,
  kyc_status STRING,
  txn_type_id INT64,
  transaction_name STRING,
  direction STRING,
  transaction_amount NUMERIC NOT NULL,
  fee_charged NUMERIC NOT NULL,
  agent_commission NUMERIC NOT NULL,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY transaction_date
CLUSTER BY agent_id, state
OPTIONS(description="Core transactional fact table partitioned by transaction date and clustered by agent ID and state");

-- -----------------------------------------------------------------------------
-- 5. Create Core Failed Fact Table in john_dw_core_dataset (Partitioned & Clustered)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.fact_daily_failed_transactions` (
  transaction_id STRING NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  transaction_date DATE NOT NULL,
  agent_id INT64,
  agent_name STRING,
  terminal_id STRING NOT NULL,
  location_cluster STRING,
  lga STRING,
  state STRING,
  region STRING,
  customer_phone STRING,
  kyc_status STRING,
  txn_type_id INT64,
  transaction_name STRING,
  direction STRING,
  transaction_amount NUMERIC NOT NULL,
  fee_charged NUMERIC NOT NULL,
  transaction_status STRING NOT NULL,
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY transaction_date
CLUSTER BY agent_id, state
OPTIONS(description="Core transactional failed fact table partitioned by transaction date and clustered by agent ID and state");

-- -----------------------------------------------------------------------------
-- 6. Create Pipeline Execution Audit Log Table in john_dw_core_dataset
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `john_dw_core_dataset.pipeline_execution_logs` (
  run_id STRING NOT NULL,
  logical_date DATE NOT NULL,
  task_id STRING NOT NULL,
  target_table STRING NOT NULL,
  rows_processed INT64,
  execution_status STRING NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(description="Central execution log tracking Airflow task runs, status, and row counts across pipeline steps");
