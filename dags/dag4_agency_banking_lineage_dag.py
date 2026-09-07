"""
DAG 4: Agency Banking OpenMetadata Automated Lineage Generator

Emits the complete end-to-end data lineage for the Agency Banking Lakehouse into OpenMetadata:

    GCS landing zone (container)
        -> lnd_daily_transactions        (BigQuery Landing, 7-day TTL)
        -> stg_daily_transactions        (BigQuery Staging)
        -> fact_daily_transactions       (BigQuery Fact Table, joined with 4 dimensions)
        -> fact_daily_failed_transactions(BigQuery Failed Transactions Fact)
        -> Serving Views                 (vw_agent_performance, vw_daily_liquidity_summary, vw_kyc_compliance_risk)
        -> GCS Iceberg Archive           (gs://3mtt-mentees-bucket/john/iceberg/fact_daily_transactions)

    dim_agents            ->|
    dim_customers         ->|
    dim_transaction_types ->|  fact_daily_transactions
    dim_geography         ->|
"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from lineage.lineage_emitter_om import emit_lineage_to_om

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "lineage/config/agency_banking_lineage_config.json")
if not os.path.exists(CONFIG_FILE_PATH):
    CONFIG_FILE_PATH = "/opt/airflow/dags/lineage/config/agency_banking_lineage_config.json"

default_args = {
    "owner": "agency_banking_ops",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "depends_on_past": False,
}

def load_lineage_config():
    """Load lineage config JSON file."""
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(f"Lineage config JSON not found at: {CONFIG_FILE_PATH}")

    with open(CONFIG_FILE_PATH, "r") as f:
        return json.load(f)

def emit_with_resolved_fqns(
    source_fqn: str,
    target_fqn: str,
    source_type: str,
    target_type: str,
    column_mappings: list,
):
    """
    Resolves {TOKEN} placeholders in the FQNs at runtime, then invokes the lineage emitter.
    """
    tokens = {
        "{BQ}": Variable.get("omd_bq_service", default_var="BigQuery-Agency-Banking"),
        "{GCS}": Variable.get("omd_gcs_service", default_var="GCS-Agency-Banking"),
        "{PROJECT}": Variable.get("bq_project_id", default_var="big-query-cluster"),
    }

    def resolve(fqn: str) -> str:
        for token, value in tokens.items():
            fqn = fqn.replace(token, value)
        if "{" in fqn or "}" in fqn:
            raise ValueError(f"Unresolved token left in FQN: {fqn}")
        return fqn

    resolved_source = resolve(source_fqn)
    resolved_target = resolve(target_fqn)
    print(f"Emitting OpenMetadata Lineage: {resolved_source} ({source_type}) -> {resolved_target} ({target_type})")

    return emit_lineage_to_om(
        source_fqn=resolved_source,
        target_fqn=resolved_target,
        source_type=source_type,
        target_type=target_type,
        column_mappings=column_mappings,
    )

with DAG(
    dag_id="dag4_agency_banking_lineage_dag",
    default_args=default_args,
    schedule=None,  # Manual / On-Demand: Lineage changes with architecture, not daily data runs
    catchup=False,
    tags=["agency_banking", "dag4", "openmetadata", "lineage", "governance"],
    description="DAG 4: Emits complete Star Schema data lineage into OpenMetadata (GCS -> Landing -> Staging -> Fact -> BI Views -> Iceberg)",
) as dag:

    config = load_lineage_config()
    lineage_items = config.get("lineages", [])

    for item in lineage_items:
        column_map_list = [
            (col["source"], col["target"]) for col in item.get("column_mappings", [])
        ]

        PythonOperator(
            task_id=item["task_id"],
            python_callable=emit_with_resolved_fqns,
            op_kwargs={
                "source_fqn": item["source_fqn"],
                "target_fqn": item["target_fqn"],
                "source_type": item["source_type"],
                "target_type": item["target_type"],
                "column_mappings": column_map_list,
            },
        )
