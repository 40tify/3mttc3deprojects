import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
import boto3
from botocore.config import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Initialize Faker with Nigerian context
fake = Faker(['en_NG'])

LOCAL_DATA_DIR = "/opt/airflow/data"
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

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

def ensure_dim_customers_local(s3_client, bucket_name, local_dir):
    """Ensures dim_customers.csv is present in local_dir by downloading it from GCS if missing."""
    local_path = os.path.join(local_dir, "dim_customers.csv")
    if os.path.exists(local_path):
        print(f"dim_customers.csv found locally at {local_path}.")
        return

    # Handle bucket names configured with subdirectories/prefixes (e.g. 'bucket-name/prefix')
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''
    gcs_key = f"{key_prefix}temp/dim_customers.csv"

    print(f"dim_customers.csv not found locally. Attempting download from gs://{actual_bucket}/{gcs_key}...")
    try:
        os.makedirs(local_dir, exist_ok=True)
        s3_client.download_file(actual_bucket, gcs_key, local_path)
        print(f"Successfully downloaded dim_customers.csv from GCS to {local_path}.")
    except Exception as e:
        print(f"Warning: Failed to download dim_customers.csv from GCS: {e}")

default_args = {
    'owner': 'agency_banking_ops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_daily_landing_files(ds, ds_nodash, **kwargs):
    """
    Generates 3 deterministic daily transaction landing CSV files for logical date ds.
    Files generated:
      - lnd_YYYYMMDD_1.csv
      - lnd_YYYYMMDD_2.csv
      - lnd_YYYYMMDD_3.csv
    Idempotent: Re-running for a logical date reproduces the exact same 3 files.
    """
    print(f"DAG 1: Generating 3 deterministic landing files for logical date: {ds}")
    
    # 1. Retrieve credentials and bucket name
    s3_client, bucket_name = get_gcs_client_and_bucket()
    
    # 2. Ensure dim_customers.csv is local by downloading from GCS temp/ if needed
    if s3_client:
        ensure_dim_customers_local(s3_client, bucket_name, LOCAL_DATA_DIR)
    
    # Deterministic seeding per logical date
    seed_val = int(ds_nodash)
    random.seed(seed_val)
    Faker.seed(seed_val)
    
    # Load or fallback to customer phone numbers
    customers_path = os.path.join(LOCAL_DATA_DIR, "dim_customers.csv")
    if os.path.exists(customers_path):
        customer_df = pd.read_csv(customers_path)
        customer_phones = customer_df["customer_phone"].tolist()
    else:
        customer_phones = [f"234803{random.randint(1000000, 9999999)}" for _ in range(80)]

    terminals = [f"TERM-{i}" for i in range(7002, 7051)]
    txn_types = [101, 102, 103, 104]
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]

    generated_files = []

    # Split daily transactions across 3 deterministic files
    for file_index in range(1, 4):
        file_seed = seed_val + file_index
        random.seed(file_seed)
        
        txn_rows = []
        num_txns = 500  # 500 records per file -> 1500 total daily records

        for i in range(num_txns):
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            timestamp = f"{ds} {hour:02d}:{minute:02d}:{second:02d}"
            
            txid = f"TXN-{random.randint(100000, 999999)}"
            terminal_id = random.choice(terminals)
            cust_phone = random.choice(customer_phones)
            txn_type = random.choice(txn_types)
            amount = round(random.uniform(500.0, 75000.0), 2)
            status_val = random.choice(statuses)
            
            # Calculate fee
            if txn_type == 101:
                fee_charged = min(amount * 0.005, 500.0)
            elif txn_type == 102:
                fee_charged = min(amount * 0.01, 1000.0)
            elif txn_type == 103:
                fee_charged = 100.0
            elif txn_type == 104:
                fee_charged = 0.0
            else:
                fee_charged = 0.0
            fee_charged = round(fee_charged, 2)
            
            # Inject 10% corrupted records for staging quality filtering
            is_corrupt = (random.random() < 0.10)
            if is_corrupt:
                corruption_type = random.choice([
                    "missing_phone", "missing_terminal", "negative_amount", "invalid_txn_type"
                ])
                if corruption_type == "missing_phone":
                    cust_phone = None
                elif corruption_type == "missing_terminal":
                    terminal_id = None
                elif corruption_type == "negative_amount":
                    amount = -1 * amount
                elif corruption_type == "invalid_txn_type":
                    txn_type = 999

            txn_rows.append({
                "txnid": txid,
                "createdat": timestamp,
                "terminalid": terminal_id,
                "custphone": cust_phone,
                "txntypecode": txn_type,
                "amount": amount,
                "fee_charged": fee_charged,
                "status": status_val
            })
        
        filename = f"lnd_{ds_nodash}_{file_index}.csv"
        file_path = os.path.join(LOCAL_DATA_DIR, filename)
        df = pd.DataFrame(txn_rows)
        df.to_csv(file_path, index=False)
        generated_files.append(filename)
        print(f"Generated landing file: {file_path}")

    return generated_files

def upload_landing_files_to_gcs(ds_nodash, **kwargs):
    """Uploads the 3 daily landing CSV files to gs://.../landing/ in GCS."""
    s3_client, bucket_name = get_gcs_client_and_bucket()

    if not s3_client:
        print("HMAC credentials not supplied. Landing files stored in local container directory.")
        return

    # Handle bucket names configured with subdirectories/prefixes (e.g. 'bucket-name/prefix')
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''

    for file_index in range(1, 4):
        filename = f"lnd_{ds_nodash}_{file_index}.csv"
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        gcs_key = f"{key_prefix}landing/{filename}"
        
        if os.path.exists(local_path):
            print(f"Uploading {local_path} -> gs://{actual_bucket}/{gcs_key}")
            s3_client.upload_file(local_path, actual_bucket, gcs_key)

with DAG(
    'dag1_landing_data_generator_dag',
    default_args=default_args,
    description='DAG 1: Daily Landing Data Generator (Outputs 3 deterministic CSV files)',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['agency_banking', 'dag1', 'generator', 'daily'],
) as dag:

    generate_files_task = PythonOperator(
        task_id='generate_daily_landing_files',
        python_callable=generate_daily_landing_files,
        op_kwargs={'ds': '{{ ds }}', 'ds_nodash': '{{ ds_nodash }}'},
    )

    upload_landing_gcs_task = PythonOperator(
        task_id='upload_landing_files_to_gcs',
        python_callable=upload_landing_files_to_gcs,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    generate_files_task >> upload_landing_gcs_task
