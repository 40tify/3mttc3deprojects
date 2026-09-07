import os
from google.cloud import bigquery
from google.oauth2 import service_account

credentials_path = "/opt/airflow/credentials/john-big-query-cluster.json"
if not os.path.exists(credentials_path):
    credentials_path = os.path.join(os.path.dirname(__file__), "../credentials/john-big-query-cluster.json")

creds = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=creds, project=creds.project_id)

queries = [
    # 1. Seed dim_geography -1
    f"""
    MERGE INTO `{client.project}.john_dw_core_dataset.dim_geography` AS target
    USING (SELECT -1 AS geo_id, 'Unknown Cluster' AS location_cluster, 'Unknown' AS lga, 'Unknown' AS state, 'Unknown' AS region) AS source
    ON target.geo_id = source.geo_id
    WHEN NOT MATCHED THEN
      INSERT (geo_id, location_cluster, lga, state, region)
      VALUES (source.geo_id, source.location_cluster, source.lga, source.state, source.region);
    """,
    
    # 2. Seed dim_agents -1
    f"""
    MERGE INTO `{client.project}.john_dw_core_dataset.dim_agents` AS target
    USING (SELECT -1 AS agent_id, 'Unknown Agent' AS agent_name, 'Unknown Business' AS business_name, 'UNKNOWN' AS terminal_id, 'Unknown' AS tier_level, DATE('1970-01-01') AS signup_date, -1 AS geo_id) AS source
    ON target.agent_id = source.agent_id
    WHEN NOT MATCHED THEN
      INSERT (agent_id, agent_name, business_name, terminal_id, tier_level, signup_date, geo_id)
      VALUES (source.agent_id, source.agent_name, source.business_name, source.terminal_id, source.tier_level, source.signup_date, source.geo_id);
    """,

    # 3. Seed dim_customers -1
    f"""
    MERGE INTO `{client.project}.john_dw_core_dataset.dim_customers` AS target
    USING (SELECT -1 AS customer_id, 0 AS customer_phone, 'UNKNOWN' AS kyc_status, 'UNKNOWN' AS account_type, DATE('1970-01-01') AS registration_date) AS source
    ON target.customer_id = source.customer_id
    WHEN NOT MATCHED THEN
      INSERT (customer_id, customer_phone, kyc_status, account_type, registration_date)
      VALUES (source.customer_id, source.customer_phone, source.kyc_status, source.account_type, source.registration_date);
    """,

    # 4. Seed dim_transaction_types -1
    f"""
    MERGE INTO `{client.project}.john_dw_core_dataset.dim_transaction_types` AS target
    USING (SELECT -1 AS txn_type_id, 'Unknown Transaction Type' AS txn_name, 'UNKNOWN' AS direction, FALSE AS is_financial) AS source
    ON target.txn_type_id = source.txn_type_id
    WHEN NOT MATCHED THEN
      INSERT (txn_type_id, txn_name, direction, is_financial)
      VALUES (source.txn_type_id, source.txn_name, source.direction, source.is_financial);
    """,

    # 5. Fix existing NULLs in fact_daily_transactions
    f"""
    UPDATE `{client.project}.john_dw_core_dataset.fact_daily_transactions`
    SET txn_type_id = COALESCE(txn_type_id, -1),
        customer_id = COALESCE(customer_id, -1),
        agent_id = COALESCE(agent_id, -1),
        geo_id = COALESCE(geo_id, -1)
    WHERE txn_type_id IS NULL OR customer_id IS NULL OR agent_id IS NULL OR geo_id IS NULL;
    """,

    # 6. Fix existing NULLs in fact_daily_failed_transactions
    f"""
    UPDATE `{client.project}.john_dw_core_dataset.fact_daily_failed_transactions`
    SET txn_type_id = COALESCE(txn_type_id, -1),
        customer_id = COALESCE(customer_id, -1),
        agent_id = COALESCE(agent_id, -1),
        geo_id = COALESCE(geo_id, -1)
    WHERE txn_type_id IS NULL OR customer_id IS NULL OR agent_id IS NULL OR geo_id IS NULL;
    """
]

for idx, q in enumerate(queries, 1):
    job = client.query(q)
    job.result()
    print(f"Executed step {idx}/{len(queries)} successfully.")

print("All dimension -1 placeholders seeded and existing fact tables updated successfully!")
