import os
from datetime import datetime, timedelta
import boto3
from botocore.config import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

LOCAL_DATA_DIR = "/opt/airflow/data"

def get_gcs_client_and_bucket():
    """
    Retrieves GCS HMAC credentials from the Airflow connection 'gcp_hmac_conn'
    and bucket name from the Airflow Variable 'gcs_bucket_name'.
    Falls back to environment variables if they are not defined.
    """
    # 1. Retrieve connection
    try:
        conn = BaseHook.get_connection('gcp_hmac_conn')
        access_key = conn.login
        secret_key = conn.password
    except Exception as e:
        print(f"Connection 'gcp_hmac_conn' not found: {e}. Falling back to env variables.")
        access_key = os.getenv("GCP_HMAC_ACCESS_KEY")
        secret_key = os.getenv("GCP_HMAC_SECRET_KEY")

    # 2. Retrieve bucket name
    bucket_name = Variable.get("gcs_bucket_name", default_var=os.getenv("GCP_BUCKET_NAME", "3mtt-lakehouse-agencybanking"))

    # 3. Create s3 client if credentials exist
    if access_key and secret_key:
        s3_client = boto3.client(
            's3',
            region_name='auto',
            endpoint_url='https://storage.googleapis.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        return s3_client, bucket_name
    else:
        print("GCP HMAC credentials not configured.")
        return None, bucket_name

default_args = {
    'owner': 'agency_banking_ops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials_path = "/opt/airflow/credentials/john-big-query-cluster.json"
    if not os.path.exists(credentials_path):
        local_creds = os.path.join(os.path.dirname(__file__), "../credentials/john-big-query-cluster.json")
        if os.path.exists(local_creds):
            credentials_path = local_creds
        else:
            print(f"BigQuery credentials not found at {credentials_path}. Simulating client...")
            return None

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def log_audit_event(client, run_id, logical_date, task_id, target_table, rows_processed, status):
    from google.cloud import bigquery
    query = f"""
    INSERT INTO `{client.project}.john_dw_core_dataset.pipeline_execution_logs`
    (run_id, logical_date, task_id, target_table, rows_processed, execution_status, created_at)
    VALUES (@run_id, DATE(@logical_date), @task_id, @target_table, @rows_processed, @status, CURRENT_TIMESTAMP())
    """
    safe_rows = int(rows_processed) if rows_processed is not None else 0
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("logical_date", "STRING", logical_date),
            bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
            bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            bigquery.ScalarQueryParameter("rows_processed", "INT64", safe_rows),
            bigquery.ScalarQueryParameter("status", "STRING", status),
        ]
    )
    client.query(query, job_config=job_config).result()
    print(f"Logged audit event: {task_id} -> {status} (rows: {safe_rows})")

def load_landing_to_bigquery(run_id, logical_date, ds_nodash, **kwargs):
    """
    Step 1: Direct Load raw landing flat files (lnd_YYYYMMDD_1..3.csv) into john_lnd_stg_dataset.lnd_daily_transactions.
    Logs execution task metrics to john_dw_core_dataset.pipeline_execution_logs.
    """
    print(f"DAG 2 ELT Step 1: Ingesting GCS landing files lnd_{ds_nodash}_1..3.csv into BigQuery...")
    client = get_bq_client()
    if client is None:
        print("Simulating load: Direct loaded 1500 raw records into BigQuery.")
        return

    _, bucket_name = get_gcs_client_and_bucket()
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''
    
    table_id = f"{client.project}.john_lnd_stg_dataset.lnd_daily_transactions"
    uri = f"gs://{actual_bucket}/{key_prefix}landing/lnd_{ds_nodash}_*.csv"
    
    from google.cloud import bigquery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    
    try:
        print(f"Loading GCS URI: {uri} into BigQuery table: {table_id}...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for completion
        
        destination_table = client.get_table(table_id)
        rows_loaded = destination_table.num_rows
        print(f"Direct loaded {rows_loaded} raw records into {table_id}.")
        
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="load_landing_to_bigquery",
            target_table="john_lnd_stg_dataset.lnd_daily_transactions",
            rows_processed=rows_loaded,
            status="SUCCESS"
        )
    except Exception as e:
        print(f"Error loading landing files to BigQuery: {e}")
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="load_landing_to_bigquery",
            target_table="john_lnd_stg_dataset.lnd_daily_transactions",
            rows_processed=0,
            status="FAILED"
        )
        raise e

def transform_landing_to_staging(run_id, logical_date, **kwargs):
    """
    Step 2: Execute CREATE OR REPLACE TABLE john_lnd_stg_dataset.stg_daily_transactions via SQL.
    Enriches raw landing records with john_dw_core_dataset.dim_* lookups and derives agent_commission.
    """
    print("DAG 2 ELT Step 2: Executing BigQuery SQL transform to staging...")
    client = get_bq_client()
    if client is None:
        print("Simulating transform: Transformed 1350 valid records into staging.")
        return

    sql_path = os.path.join(os.path.dirname(__file__), "sql/02_elt_transform_landing_to_staging.sql")
    if not os.path.exists(sql_path):
        sql_path = os.path.join(os.path.dirname(__file__), "../sql/02_elt_transform_landing_to_staging.sql")
        if not os.path.exists(sql_path):
            sql_path = "/opt/airflow/dags/sql/02_elt_transform_landing_to_staging.sql"

    try:
        with open(sql_path, "r") as f:
            query = f.read()
        
        print("Running staging transformation SQL...")
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        destination_table = client.get_table(f"{client.project}.john_lnd_stg_dataset.stg_daily_transactions")
        rows_staged = destination_table.num_rows
        print(f"Transformed {rows_staged} valid records into john_lnd_stg_dataset.stg_daily_transactions.")
        
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="transform_landing_to_staging",
            target_table="john_lnd_stg_dataset.stg_daily_transactions",
            rows_processed=rows_staged,
            status="SUCCESS"
        )
    except Exception as e:
        print(f"Error running staging transform: {e}")
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="transform_landing_to_staging",
            target_table="john_lnd_stg_dataset.stg_daily_transactions",
            rows_processed=0,
            status="FAILED"
        )
        raise e

def merge_staging_to_fact(run_id, logical_date, **kwargs):
    """
    Step 3: MERGE valid staging records into john_dw_core_dataset.fact_daily_transactions (success)
    and john_dw_core_dataset.fact_daily_failed_transactions (failed).
    """
    print("DAG 2 ELT Step 3: Merging staging records into core fact tables...")
    client = get_bq_client()
    if client is None:
        print("Simulating merge: Merged 1350 records into fact tables.")
        return

    sql_path = os.path.join(os.path.dirname(__file__), "sql/03_elt_merge_staging_to_fact.sql")
    if not os.path.exists(sql_path):
        sql_path = os.path.join(os.path.dirname(__file__), "../sql/03_elt_merge_staging_to_fact.sql")
        if not os.path.exists(sql_path):
            sql_path = "/opt/airflow/dags/sql/03_elt_merge_staging_to_fact.sql"

    try:
        with open(sql_path, "r") as f:
            query = f.read()
            
        print("Running merge to fact tables SQL...")
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        rows_merged = query_job.num_dml_affected_rows
        print(f"Merged {rows_merged} records into core fact tables (success and failed).")
        
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="merge_staging_to_fact",
            target_table="john_dw_core_dataset.fact_daily_transactions",
            rows_processed=rows_merged,
            status="SUCCESS"
        )
    except Exception as e:
        print(f"Error running merge to fact tables: {e}")
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="merge_staging_to_fact",
            target_table="john_dw_core_dataset.fact_daily_transactions",
            rows_processed=0,
            status="FAILED"
        )
        raise e

def refresh_serving_views(run_id, logical_date, **kwargs):
    """
    Step 4: Re-create / refresh business intelligence views in john_dw_analytics_dataset.
    """
    print("DAG 2 ELT Step 4: Refreshing business intelligence serving views in john_dw_analytics_dataset...")
    client = get_bq_client()
    if client is None:
        print("Simulating view refresh: Views refreshed successfully.")
        return

    sql_path = os.path.join(os.path.dirname(__file__), "sql/04_create_serving_views.sql")
    if not os.path.exists(sql_path):
        sql_path = os.path.join(os.path.dirname(__file__), "../sql/04_create_serving_views.sql")
        if not os.path.exists(sql_path):
            sql_path = "/opt/airflow/dags/sql/04_create_serving_views.sql"

    try:
        with open(sql_path, "r") as f:
            query = f.read()
            
        print("Running view creation script...")
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        print("Refreshed vw_agent_performance, vw_daily_liquidity_summary, and vw_kyc_compliance_risk.")
        
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="refresh_serving_views",
            target_table="john_dw_analytics_dataset.*",
            rows_processed=0,
            status="SUCCESS"
        )
    except Exception as e:
        print(f"Error refreshing serving views: {e}")
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="refresh_serving_views",
            target_table="john_dw_analytics_dataset.*",
            rows_processed=0,
            status="FAILED"
        )
        raise e

def log_pipeline_audit_metrics(run_id, logical_date, **kwargs):
    """
    Step 5: Appends task run metrics into john_dw_core_dataset.pipeline_execution_logs.
    """
    print(f"DAG 2 ELT Step 5: Logging audit execution metrics to john_dw_core_dataset.pipeline_execution_logs for run_id={run_id}...")
    client = get_bq_client()
    if client is None:
        print("Simulating summary: Logged SUCCESS status to audit table.")
        return

    try:
        log_audit_event(
            client=client,
            run_id=run_id,
            logical_date=logical_date,
            task_id="dag2_core_elt_pipeline_dag",
            target_table="john_dw_core_dataset.fact_daily_transactions",
            rows_processed=0,
            status="SUCCESS"
        )
        print("Logged overall run success to audit logs table.")
    except Exception as e:
        print(f"Error logging pipeline summary audit: {e}")
        raise e

def archive_processed_landing_files(ds_nodash, **kwargs):
    """
    Step 6: File Hygiene - Automatically move ingested CSV files from landing/ to archival/ folder in GCS.
    """
    s3_client, bucket_name = get_gcs_client_and_bucket()

    print(f"DAG 2 ELT Step 6: Archiving processed CSV files for {ds_nodash} from landing/ to archival/...")

    if not s3_client:
        print("HMAC credentials not supplied. File move simulated locally.")
        return

    # Handle bucket names configured with subdirectories/prefixes (e.g. 'bucket-name/prefix')
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''

    for file_index in range(1, 4):
        filename = f"lnd_{ds_nodash}_{file_index}.csv"
        src_key = f"{key_prefix}landing/{filename}"
        dest_key = f"{key_prefix}archival/{filename}"
        
        try:
            print(f"Copying gs://{actual_bucket}/{src_key} -> gs://{actual_bucket}/{dest_key}")
            s3_client.copy_object(
                Bucket=actual_bucket,
                CopySource={'Bucket': actual_bucket, 'Key': src_key},
                Key=dest_key
            )
            print(f"Deleting source file gs://{actual_bucket}/{src_key}")
            s3_client.delete_object(Bucket=actual_bucket, Key=src_key)
        except Exception as e:
            print(f"Notice during file archive for {filename}: {e}")

with DAG(
    'dag2_core_elt_pipeline_dag',
    default_args=default_args,
    description='DAG 2: Core ELT Pipeline (Landing -> Staging -> Fact -> Views -> Audit Log -> Archival)',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['agency_banking', 'dag2', 'elt', 'core_pipeline'],
) as dag:

    load_landing_task = PythonOperator(
        task_id='load_landing_to_bigquery',
        python_callable=load_landing_to_bigquery,
        op_kwargs={'run_id': '{{ run_id }}', 'logical_date': '{{ ds }}', 'ds_nodash': '{{ ds_nodash }}'},
    )

    transform_staging_task = PythonOperator(
        task_id='transform_landing_to_staging',
        python_callable=transform_landing_to_staging,
        op_kwargs={'run_id': '{{ run_id }}', 'logical_date': '{{ ds }}'},
    )

    merge_fact_task = PythonOperator(
        task_id='merge_staging_to_fact',
        python_callable=merge_staging_to_fact,
        op_kwargs={'run_id': '{{ run_id }}', 'logical_date': '{{ ds }}'},
    )

    refresh_views_task = PythonOperator(
        task_id='refresh_serving_views',
        python_callable=refresh_serving_views,
        op_kwargs={'run_id': '{{ run_id }}', 'logical_date': '{{ ds }}'},
    )

    log_audit_task = PythonOperator(
        task_id='log_pipeline_audit_metrics',
        python_callable=log_pipeline_audit_metrics,
        op_kwargs={'run_id': '{{ run_id }}', 'logical_date': '{{ ds }}'},
    )

    archive_files_task = PythonOperator(
        task_id='archive_processed_landing_files',
        python_callable=archive_processed_landing_files,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    load_landing_task >> transform_staging_task >> merge_fact_task >> refresh_views_task >> log_audit_task >> archive_files_task
