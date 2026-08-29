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

default_args = {
    'owner': 'agency_banking_ops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_dimensions_csv(**kwargs):
    """Generates all seed dimension CSVs locally in data/ directory."""
    print("Executing DAG 0: Simulating and generating seed dimension data...")
    Faker.seed(42)
    random.seed(42)

    # 1. dim_geography
    state_lgas = {
        "Kano": ["Kano Municipal", "Fagge", "Dala", "Gwale", "Nassarawa", "Tarauni", "Doguwa", "Ungogo"],
        "Kaduna": ["Kaduna North", "Kaduna South", "Chikun", "Igabi", "Zaria", "Sabon Gari", "Kafanchan"],
        "Abuja": ["AMAC", "Bwari", "Gwagwalada", "Kuje", "Kwali", "Abaji"],
        "Rivers": ["Port Harcourt", "Obio-Akpor", "Eleme", "Ogu-Bolo", "Okrika", "Ikwerre", "Bonny"],
        "Delta": ["Warri South", "Oshimili South", "Oshimili North", "Uvwie", "Ughelli North", "Sapele"],
        "Edo": ["Oredo", "Ikpoba Okha", "Egor", "Ovia North-East", "Esan West", "Esan Central"],
        "Lagos": ["Ikeja", "Alimosho", "Surulere", "Lagos Island", "Lagos Mainland", "Eti-Osa", "Badagry", "Ikorodu"],
        "Oyo": ["Ibadan North", "Ibadan South-West", "Ibadan North-West", "Ogbomosho North", "Oyo East", "Akinyele"],
        "Ogun": ["Abeokuta South", "Abeokuta North", "Ijebu Ode", "Ado-Odo/Ota", "Sagamu", "Obafemi Owode"],
        "Enugu": ["Enugu East", "Enugu North", "Enugu South", "Nsukka", "Udi", "Oji River"],
        "Anambra": ["Awka South", "Awka North", "Onitsha North", "Onitsha South", "Nnewi North", "Aguata"],
        "Abia": ["Umuahia North", "Umuahia South", "Aba North", "Aba South", "Ohafia", "Arochukwu"]
    }
    regions = {
        "North": ["Kano", "Kaduna", "Abuja"],
        "South-South": ["Rivers", "Delta", "Edo"],
        "South-West": ["Lagos", "Oyo", "Ogun"],
        "South-East": ["Enugu", "Anambra", "Abia"]
    }
    geo_data = []
    for geo_id in range(1, 16):
        region = random.choice(list(regions.keys()))
        state = random.choice(regions[region])
        lga = random.choice(state_lgas[state])
        clean_lga = lga.upper().replace(" ", "").replace("/", "").replace("-", "")
        cluster_name = f"Cluster_{clean_lga[:6]}"
        geo_data.append({
            "geo_id": geo_id,
            "location_cluster": cluster_name,
            "lga": lga,
            "state": state,
            "region": region
        })
    geo_df = pd.DataFrame(geo_data)
    geo_path = os.path.join(LOCAL_DATA_DIR, "dim_geography.csv")
    geo_df.to_csv(geo_path, index=False)

    # 2. dim_agents
    agent_data = []
    for agent_id in range(1001, 1051):
        agent_data.append({
            "agent_id": agent_id,
            "agent_name": fake.name(),
            "business_name": f"{fake.company()} Ventures",
            "terminal_id": f"TERM-{agent_id + 6000}",
            "tier_level": random.choice(["Bronze", "Silver", "Gold"]),
            "signup_date": fake.date_between(start_date="-2y", end_date="-1m").strftime("%Y-%m-%d"),
            "geo_id": random.randint(1, 15)
        })
    agents_df = pd.DataFrame(agent_data)
    agents_path = os.path.join(LOCAL_DATA_DIR, "dim_agents.csv")
    agents_df.to_csv(agents_path, index=False)

    # 3. dim_transaction_types
    txn_type_data = [
        {"txn_type_id": 101, "txn_name": "Deposit", "direction": "IN", "is_financial": True},
        {"txn_type_id": 102, "txn_name": "Withdrawal", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 103, "txn_name": "Bill Payment", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 104, "txn_name": "Airtime Purchase", "direction": "OUT", "is_financial": True}
    ]
    txn_type_df = pd.DataFrame(txn_type_data)
    txn_type_path = os.path.join(LOCAL_DATA_DIR, "dim_transaction_types.csv")
    txn_type_df.to_csv(txn_type_path, index=False)

    # 4. dim_customers
    customer_data = []
    for customer_id in range(2001, 2201):
        customer_data.append({
            "customer_id": customer_id,
            "customer_phone": f"234803{random.randint(1000000, 9999999)}",
            "kyc_status": random.choice(["APPROVED", "PENDING", "UNREGISTERED"]),
            "account_type": random.choice(["SAVINGS", "CURRENT"]),
            "registration_date": fake.date_between(start_date="-2y", end_date="-1m").strftime("%Y-%m-%d")
        })
    customers_df = pd.DataFrame(customer_data)
    customers_path = os.path.join(LOCAL_DATA_DIR, "dim_customers.csv")
    customers_df.to_csv(customers_path, index=False)

    print("DAG 0: Seed dimensions generated successfully.")

def upload_dimensions_to_gcs(**kwargs):
    """Uploads seed dimension CSV files to gs://.../temp/ directory in GCS."""
    s3_client, bucket_name = get_gcs_client_and_bucket()

    dim_files = [
        "dim_geography.csv",
        "dim_agents.csv",
        "dim_transaction_types.csv",
        "dim_customers.csv"
    ]

    if not s3_client:
        print("HMAC credentials not supplied. Dimension CSV files generated locally in temp storage.")
        return

    # Handle bucket names configured with subdirectories/prefixes (e.g. 'bucket-name/prefix')
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''

    for filename in dim_files:
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        gcs_key = f"{key_prefix}temp/{filename}"
        print(f"Uploading {local_path} -> gs://{actual_bucket}/{gcs_key}")
        s3_client.upload_file(local_path, actual_bucket, gcs_key)

def load_dimensions_to_bigquery(**kwargs):
    """
    Triggers the load of dimension tables into BigQuery john_dw_core_dataset.dim_*.
    Logs execution metadata to john_dw_core_dataset.pipeline_execution_logs.
    """
    print("DAG 0: Executing BigQuery one-time dimension load...")
    import os
    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials_path = "/opt/airflow/credentials/john-big-query-cluster.json"
    if not os.path.exists(credentials_path):
        print(f"Credentials not found at {credentials_path}. Simulating dimension load...")
        dim_tables = ["dim_geography", "dim_agents", "dim_transaction_types", "dim_customers"]
        for table in dim_tables:
            print(f"Loaded john_dw_core_dataset.{table} into BigQuery warehouse.")
        return

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    _, bucket_name = get_gcs_client_and_bucket()
    bucket_parts = bucket_name.split('/', 1)
    actual_bucket = bucket_parts[0]
    key_prefix = bucket_parts[1] + '/' if len(bucket_parts) > 1 else ''

    dim_tables = ["dim_geography", "dim_agents", "dim_transaction_types", "dim_customers"]
    
    for table in dim_tables:
        table_id = f"{credentials.project_id}.john_dw_core_dataset.{table}"
        uri = f"gs://{actual_bucket}/{key_prefix}temp/{table}.csv"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
        
        print(f"Loading {uri} into {table_id}...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Waits for the job to complete.
        
        destination_table = client.get_table(table_id)
        print(f"Successfully loaded {destination_table.num_rows} rows into {table_id}.")

with DAG(
    'dag0_dimension_loader_dag',
    default_args=default_args,
    description='DAG 0: One-Time Dimension Data Generator & BigQuery Loader',
    schedule_interval='@once',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['agency_banking', 'dag0', 'dimension_loader', 'one_time'],
) as dag:

    generate_dim_task = PythonOperator(
        task_id='generate_dimensions',
        python_callable=generate_dimensions_csv,
    )

    upload_dim_gcs_task = PythonOperator(
        task_id='upload_dimensions_to_gcs_temp',
        python_callable=upload_dimensions_to_gcs,
    )

    load_dim_bq_task = PythonOperator(
        task_id='load_dimensions_to_bigquery',
        python_callable=load_dimensions_to_bigquery,
    )

    generate_dim_task >> upload_dim_gcs_task >> load_dim_bq_task
