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
if not os.path.exists(LOCAL_DATA_DIR):
    local_dir_fallback = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
    if os.path.exists(local_dir_fallback):
        LOCAL_DATA_DIR = local_dir_fallback
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

def ensure_dimension_files_local(s3_client, bucket_name, local_dir):
    """
    Ensures that all prerequisite seed dimension CSV files exist locally in local_dir.
    If any file is missing locally, attempts to download it from gs://.../temp/.
    """
    dim_files = [
        "dim_customers.csv",
        "dim_agents.csv",
        "dim_transaction_types.csv",
        "dim_geography.csv"
    ]
    
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''

    for filename in dim_files:
        local_path = os.path.join(local_dir, filename)
        if os.path.exists(local_path):
            print(f"Dimension file '{filename}' found locally at {local_path}.")
            continue
        
        if s3_client:
            gcs_key = f"{key_prefix}temp/{filename}"
            print(f"Dimension file '{filename}' missing locally. Attempting download from gs://{actual_bucket}/{gcs_key}...")
            try:
                os.makedirs(local_dir, exist_ok=True)
                s3_client.download_file(actual_bucket, gcs_key, local_path)
                print(f"Successfully downloaded '{filename}' from GCS to {local_path}.")
            except Exception as e:
                print(f"Notice: Could not download '{filename}' from GCS ({e}).")

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
    Enforces strict referential integrity by loading customer phones, terminal IDs,
    and transaction types directly from the seed dimension CSV files produced by DAG 0.
    """
    print(f"DAG 1: Generating 3 deterministic landing files for logical date: {ds}")
    
    # 1. Retrieve credentials and bucket name
    s3_client, bucket_name = get_gcs_client_and_bucket()
    
    # 2. Ensure dimension files are local by downloading from GCS temp/ if needed
    ensure_dimension_files_local(s3_client, bucket_name, LOCAL_DATA_DIR)
    
    # 3. Load and validate prerequisite dimension datasets (no random fallbacks)
    customers_path = os.path.join(LOCAL_DATA_DIR, "dim_customers.csv")
    if not os.path.exists(customers_path):
        raise FileNotFoundError(
            f"Required dimension file '{customers_path}' not found. "
            "DAG 0 (dag0_dimension_loader_dag) must be executed first to generate seed dimensions."
        )
    customer_df = pd.read_csv(customers_path)
    if "customer_phone" not in customer_df.columns or customer_df.empty:
        raise ValueError("dim_customers.csv is empty or missing 'customer_phone' column.")
    customer_phones = customer_df["customer_phone"].astype(str).tolist()

    agents_path = os.path.join(LOCAL_DATA_DIR, "dim_agents.csv")
    if not os.path.exists(agents_path):
        raise FileNotFoundError(
            f"Required dimension file '{agents_path}' not found. "
            "DAG 0 (dag0_dimension_loader_dag) must be executed first to generate seed dimensions."
        )
    agents_df = pd.read_csv(agents_path)
    if "terminal_id" not in agents_df.columns or agents_df.empty:
        raise ValueError("dim_agents.csv is empty or missing 'terminal_id' column.")
    terminals = agents_df["terminal_id"].astype(str).tolist()

    txn_types_path = os.path.join(LOCAL_DATA_DIR, "dim_transaction_types.csv")
    if not os.path.exists(txn_types_path):
        raise FileNotFoundError(
            f"Required dimension file '{txn_types_path}' not found. "
            "DAG 0 (dag0_dimension_loader_dag) must be executed first to generate seed dimensions."
        )
    txn_types_df = pd.read_csv(txn_types_path)
    if "txn_type_id" not in txn_types_df.columns or txn_types_df.empty:
        raise ValueError("dim_transaction_types.csv is empty or missing 'txn_type_id' column.")
    txn_types = txn_types_df["txn_type_id"].astype(int).tolist()

    print(f"Referential lookup ready: {len(customer_phones)} customers, {len(terminals)} terminals, {len(txn_types)} txn types.")

    # Deterministic seeding per logical date
    seed_val = int(ds_nodash)
    random.seed(seed_val)
    Faker.seed(seed_val)

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
