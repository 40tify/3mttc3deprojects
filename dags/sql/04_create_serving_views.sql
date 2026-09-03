-- =============================================================================
-- SQL 04: Serving Layer Business Intelligence Views (dw_analytics_dataset)
-- =============================================================================

-- 1. View: Agent Performance Metrics
CREATE OR REPLACE VIEW `john_dw_analytics_dataset.vw_agent_performance` AS
SELECT
  a.agent_id,
  a.agent_name,
  a.business_name,
  a.tier_level,
  g.location_cluster,
  g.state,
  g.region,
  COUNT(f.transaction_id) AS total_transactions,
  SUM(f.transaction_amount) AS total_volume,
  SUM(f.fee_charged) AS total_fees_collected,
  SUM(f.agent_commission) AS total_agent_payout,
  MIN(f.transaction_timestamp) AS first_seen_tx,
  MAX(f.transaction_timestamp) AS last_seen_tx
FROM `john_dw_core_dataset.fact_daily_transactions` f
LEFT JOIN `john_dw_core_dataset.dim_agents` a
  ON f.agent_id = a.agent_id
LEFT JOIN `john_dw_core_dataset.dim_geography` g
  ON f.geo_id = g.geo_id
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 2. View: Daily Regional Liquidity Summary
CREATE OR REPLACE VIEW `john_dw_analytics_dataset.vw_daily_liquidity_summary` AS
SELECT
  f.transaction_date,
  g.region,
  g.state,
  g.lga,
  g.location_cluster,
  SUM(CASE WHEN UPPER(t.direction) = 'IN' THEN f.transaction_amount ELSE 0 END) AS cash_in_volume,
  SUM(CASE WHEN UPPER(t.direction) = 'OUT' THEN f.transaction_amount ELSE 0 END) AS cash_out_volume,
  SUM(CASE WHEN UPPER(t.direction) = 'IN' THEN f.transaction_amount ELSE -f.transaction_amount END) AS net_liquidity_flow,
  COUNT(f.transaction_id) AS total_transactions
FROM `john_dw_core_dataset.fact_daily_transactions` f
LEFT JOIN `john_dw_core_dataset.dim_geography` g
  ON f.geo_id = g.geo_id
LEFT JOIN `john_dw_core_dataset.dim_transaction_types` t
  ON f.txn_type_id = t.txn_type_id
GROUP BY 1, 2, 3, 4, 5;

-- 3. View: KYC Compliance Risk Analysis
CREATE OR REPLACE VIEW `john_dw_analytics_dataset.vw_kyc_compliance_risk` AS
SELECT
  f.transaction_date,
  c.customer_phone,
  c.kyc_status,
  COUNT(f.transaction_id) AS transaction_count,
  SUM(f.transaction_amount) AS total_amount,
  AVG(f.transaction_amount) AS avg_amount,
  MAX(f.transaction_amount) AS max_single_transaction
FROM `john_dw_core_dataset.fact_daily_transactions` f
LEFT JOIN `john_dw_core_dataset.dim_customers` c
  ON f.customer_id = c.customer_id
WHERE UPPER(c.kyc_status) IN ('UNREGISTERED', 'PENDING')
GROUP BY 1, 2, 3
HAVING SUM(f.transaction_amount) > 50000;
