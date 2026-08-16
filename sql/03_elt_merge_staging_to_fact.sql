-- =============================================================================
-- SQL 03: ELT Merge Staging to Core Fact Table
-- =============================================================================

MERGE INTO `john_dw_core_dataset.fact_daily_transactions` AS target
USING `john_lnd_stg_dataset.stg_daily_transactions` AS source
ON target.transaction_id = source.transaction_id
   AND target.transaction_date = source.transaction_date
WHEN MATCHED THEN
  UPDATE SET
    target.agent_id = source.agent_id,
    target.agent_name = source.agent_name,
    target.terminal_id = source.terminal_id,
    target.location_cluster = source.location_cluster,
    target.lga = source.lga,
    target.state = source.state,
    target.region = source.region,
    target.customer_phone = source.customer_phone,
    target.kyc_status = source.kyc_status,
    target.txn_type_id = source.txn_type_id,
    target.transaction_name = source.transaction_name,
    target.direction = source.direction,
    target.transaction_amount = source.transaction_amount,
    target.fee_charged = source.fee_charged,
    target.agent_commission = source.agent_commission
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id,
    transaction_timestamp,
    transaction_date,
    agent_id,
    agent_name,
    terminal_id,
    location_cluster,
    lga,
    state,
    region,
    customer_phone,
    kyc_status,
    txn_type_id,
    transaction_name,
    direction,
    transaction_amount,
    fee_charged,
    agent_commission
  )
  VALUES (
    source.transaction_id,
    source.transaction_timestamp,
    source.transaction_date,
    source.agent_id,
    source.agent_name,
    source.terminal_id,
    source.location_cluster,
    source.lga,
    source.state,
    source.region,
    source.customer_phone,
    source.kyc_status,
    source.txn_type_id,
    source.transaction_name,
    source.direction,
    source.transaction_amount,
    source.fee_charged,
    source.agent_commission
  );
