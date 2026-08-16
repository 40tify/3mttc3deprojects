-- =============================================================================
-- SQL 02: ELT Staging Transformation (Landing ➔ Staging)
-- =============================================================================

CREATE OR REPLACE TABLE `john_lnd_stg_dataset.stg_daily_transactions` AS
SELECT
  l.txnid AS transaction_id,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', l.createdat) AS transaction_timestamp,
  DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', l.createdat)) AS transaction_date,
  a.agent_id,
  a.agent_name,
  l.terminalid AS terminal_id,
  g.location_cluster,
  g.lga,
  g.state,
  g.region,
  l.custphone AS customer_phone,
  COALESCE(c.kyc_status, 'UNREGISTERED') AS kyc_status,
  t.txn_type_id,
  COALESCE(t.txn_name, 'UNKNOWN') AS transaction_name,
  COALESCE(t.direction, 'UNKNOWN') AS direction,
  CAST(l.amount AS NUMERIC) AS transaction_amount,
  CAST(l.fee_charged AS NUMERIC) AS fee_charged,
  -- Business logic rule: Agents earn 70% of fee charged for successful transactions
  ROUND(CAST(l.fee_charged AS NUMERIC) * 0.70, 2) AS agent_commission,
  l.status AS transaction_status,
  CURRENT_TIMESTAMP() AS transformed_at
FROM `john_lnd_stg_dataset.lnd_daily_transactions` l
LEFT JOIN `john_dw_core_dataset.dim_agents` a 
  ON l.terminalid = a.terminal_id
LEFT JOIN `john_dw_core_dataset.dim_geography` g 
  ON a.geo_id = g.geo_id
LEFT JOIN `john_dw_core_dataset.dim_customers` c 
  ON l.custphone = c.customer_phone
LEFT JOIN `john_dw_core_dataset.dim_transaction_types` t 
  ON l.txntypecode = t.txn_type_id
WHERE UPPER(l.status) = 'SUCCESS';
