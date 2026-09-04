-- =============================================================================
-- SQL 02: ELT Staging Transformation (Landing ➔ Staging)
-- =============================================================================

CREATE OR REPLACE TABLE `john_lnd_stg_dataset.stg_daily_transactions` AS
SELECT
  l.txnid AS transaction_id,
  SAFE_CAST(l.createdat AS TIMESTAMP) AS transaction_timestamp,
  DATE(SAFE_CAST(l.createdat AS TIMESTAMP)) AS transaction_date,
  a.agent_id,
  a.geo_id,
  c.customer_id,
  t.txn_type_id,
  CAST(l.amount AS NUMERIC) AS transaction_amount,
  CAST(l.fee_charged AS NUMERIC) AS fee_charged,
  -- Business logic rule: Agents earn 70% of fee charged for successful transactions
  CASE 
    WHEN UPPER(l.status) = 'SUCCESS' THEN ROUND(CAST(l.fee_charged AS NUMERIC) * 0.70, 2)
    ELSE CAST(0.0 AS NUMERIC)
  END AS agent_commission,
  l.status AS transaction_status,
  CURRENT_TIMESTAMP() AS transformed_at
FROM `john_lnd_stg_dataset.lnd_daily_transactions` l
LEFT JOIN `john_dw_core_dataset.dim_agents` a 
  ON CAST(l.terminalid AS STRING) = CAST(a.terminal_id AS STRING)
LEFT JOIN `john_dw_core_dataset.dim_customers` c 
  ON CAST(l.custphone AS STRING) = CAST(c.customer_phone AS STRING)
  OR REGEXP_REPLACE(CAST(l.custphone AS STRING), r'\.0$', '') = CAST(c.customer_phone AS STRING)
LEFT JOIN `john_dw_core_dataset.dim_transaction_types` t 
  ON CAST(l.txntypecode AS STRING) = CAST(t.txn_type_id AS STRING)
WHERE UPPER(l.status) IN ('SUCCESS', 'FAILED')
  AND l.txnid IS NOT NULL
  AND l.createdat IS NOT NULL
  AND l.terminalid IS NOT NULL
  AND l.amount >= 0;
