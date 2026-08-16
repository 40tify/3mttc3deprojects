import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
import boto3
from botocore.config import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

# Initialize Faker with Nigerian context
fake = Faker(['en_NG'])

LOCAL_DATA_DIR = "/opt/airflow/data"
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

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
    access_key = os.getenv("GCP_HMAC_ACCESS_KEY")
    secret_key = os.getenv("GCP_HMAC_SECRET_KEY")
    bucket_name = os.getenv("GCP_BUCKET_NAME", "3mtt-lakehouse-agencybanking")

    if not access_key or not secret_key:
        print("HMAC credentials not supplied. Landing files stored in local container directory.")
        return

    s3_client = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version='s3v4'
        )
    )

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
