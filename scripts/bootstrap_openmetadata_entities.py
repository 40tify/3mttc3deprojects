import json
import os
import requests

def bootstrap_openmetadata():
    token = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3ODg1NDU4NjgsImV4cCI6bnVsbH0.mlPOTJj_wXsvHjyzHwxYDaSaYsR_dTFoL_GwE-rQUxWry8Q7DBZfi1amUqp4vtm4JwmSh3ay5Lg11XF3TTqzlZmD6YB6rn_0TBrihLSfm4hZZQWXM-VYmEb8aYjuCTkmNl2OvXNPJI0gowxgxKMEfZHyrpuwoFP6CIbAQfWXZwTTHKmcViAwlfz1eSpwX4iY5PoDa7vHIYy04TDu4y4KGzakkx7fngyua3G4Kvhh7VOSIiWqbx8HTAxah4xizV91JfOFuZJi5jegmtBzP9c6rGlqMR11F2y-gVYOjyX8OhXe7nYUq29e64r6uhOJ7o6br_abrHtdjvX4zYDG2jwlGA"
    base_url = "http://openmetadata-server:8585/api/v1"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    session = requests.Session()
    session.headers.update(headers)

    print("1. Creating Database Service: BigQuery-Agency-Banking...")
    bq_svc_payload = {
        "name": "BigQuery-Agency-Banking",
        "serviceType": "BigQuery",
        "connection": {
            "config": {
                "type": "BigQuery"
            }
        }
    }
    r = session.put(f"{base_url}/services/databaseServices", json=bq_svc_payload)
    print("Database Service PUT:", r.status_code)

    print("2. Creating Database: big-query-cluster...")
    db_payload = {
        "name": "big-query-cluster",
        "service": "BigQuery-Agency-Banking"
    }
    r = session.put(f"{base_url}/databases", json=db_payload)
    print("Database PUT:", r.status_code)

    schemas = ["john_lnd_stg_dataset", "john_dw_core_dataset", "john_dw_analytics_dataset"]
    for schema_name in schemas:
        print(f"3. Creating Schema: {schema_name}...")
        schema_payload = {
            "name": schema_name,
            "database": "BigQuery-Agency-Banking.big-query-cluster"
        }
        r = session.put(f"{base_url}/databaseSchemas", json=schema_payload)
        print(f"Schema {schema_name} PUT:", r.status_code)

    tables = [
        ("john_lnd_stg_dataset", "lnd_daily_transactions", ["txnid", "createdat", "terminalid", "custphone", "txntypecode", "amount", "fee_charged", "status"]),
        ("john_lnd_stg_dataset", "stg_daily_transactions", ["transaction_id", "transaction_timestamp", "transaction_date", "agent_id", "customer_id", "transaction_type_id", "geography_id", "channel_id", "transaction_amount", "fee_charged", "agent_commission", "transaction_status", "device_type", "error_code", "created_at"]),
        ("john_dw_core_dataset", "dim_agents", ["agent_id", "agent_name", "phone_number", "geo_id", "agent_status", "onboarding_date"]),
        ("john_dw_core_dataset", "dim_customers", ["customer_id", "phone_number", "customer_name", "kyc_status", "kyc_tier", "registration_date"]),
        ("john_dw_core_dataset", "dim_transaction_types", ["transaction_type_id", "type_code", "type_description", "fee_rate", "is_revenue_generating"]),
        ("john_dw_core_dataset", "dim_geography", ["geo_id", "state", "region", "lga", "zone_classification"]),
        ("john_dw_core_dataset", "fact_daily_transactions", ["transaction_id", "transaction_date", "transaction_timestamp", "agent_id", "customer_id", "transaction_type_id", "geography_id", "channel_id", "transaction_amount", "fee_charged", "agent_commission", "transaction_status", "device_type", "created_at"]),
        ("john_dw_core_dataset", "fact_daily_failed_transactions", ["transaction_id", "transaction_date", "transaction_timestamp", "agent_id", "customer_id", "transaction_type_id", "geography_id", "channel_id", "transaction_amount", "fee_charged", "error_code", "created_at"]),
        ("john_dw_analytics_dataset", "vw_agent_performance", ["agent_id", "agent_name", "state", "total_volume", "total_revenue", "commission_earned"]),
        ("john_dw_analytics_dataset", "vw_daily_liquidity_summary", ["transaction_date", "state", "total_inflow", "total_outflow", "net_liquidity"]),
        ("john_dw_analytics_dataset", "vw_kyc_compliance_risk", ["customer_id", "customer_name", "kyc_tier", "suspicious_volume_flag", "daily_turnover"]),
    ]

    for schema_name, table_name, cols in tables:
        print(f"4. Creating Table: {schema_name}.{table_name}...")
        col_payloads = [{"name": c, "dataType": "STRING"} for c in cols]
        tbl_payload = {
            "name": table_name,
            "databaseSchema": f"BigQuery-Agency-Banking.big-query-cluster.{schema_name}",
            "columns": col_payloads
        }
        r = session.put(f"{base_url}/tables", json=tbl_payload)
        print(f"Table {table_name} PUT:", r.status_code)

    print("5. Creating Storage Service: GCS-Agency-Banking...")
    storage_svc_payload = {
        "name": "GCS-Agency-Banking",
        "serviceType": "GCS",
        "connection": {
            "config": {
                "type": "GCS"
            }
        }
    }
    r = session.put(f"{base_url}/services/storageServices", json=storage_svc_payload)
    print("Storage Service PUT:", r.status_code)

    containers = [
        "3mtt-mentees-bucket.john/landing",
        "3mtt-mentees-bucket.john/iceberg/fact_daily_transactions"
    ]
    for c_name in containers:
        print(f"6. Creating Container: {c_name}...")
        container_payload = {
            "name": c_name,
            "service": "GCS-Agency-Banking"
        }
        r = session.put(f"{base_url}/containers", json=container_payload)
        print(f"Container {c_name} PUT:", r.status_code)

    print("\nSuccessfully bootstrapped all Lakehouse Star Schema entities into OpenMetadata!")

if __name__ == "__main__":
    bootstrap_openmetadata()
