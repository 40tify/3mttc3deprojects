import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

# Initialize Faker with Nigerian context
fake = Faker(['en_NG'])

# Configuration variables
# Credentials are loaded from environment variables (populated via credentials/hmac_credentials.env)
LOCAL_DATA_DIR = "/opt/airflow/data"

# Ensure local data directory exists inside the container
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

default_args = {
    'owner': 'agency_banking_ops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_dimensions_csv(**kwargs):
    """Generates dim_geography and dim_agents CSVs and saves them to local data directory."""
    print("Generating dimension data...")
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
    print(f"Saved dim_geography.csv to {geo_path}")

    # 2. dim_agents
    agent_data = []
    for agent_id in range(1001, 1051):
        agent_data.append({
            "agent_id": agent_id,
            "agent_name": fake.name(),
            "business_name": f"{fake.company()} Ventures",
            "terminal_id": f"TERM-{agent_id + 6000}",
            "tier_level": random.choice(["Bronze", "Silver", "Gold"]),
            "signup_date": fake.date_between(start_date="-2y", end_date="-1m").strftime("%Y-%m-%d")
        })
    
    agents_df = pd.DataFrame(agent_data)
    agents_path = os.path.join(LOCAL_DATA_DIR, "dim_agents.csv")
    agents_df.to_csv(agents_path, index=False)
    print(f"Saved dim_agents.csv to {agents_path}")

    # 3. dim_transaction_types (Static lookup dimension)
    txn_type_data = [
        {"txn_type_id": 101, "txn_name": "Deposit", "direction": "IN", "is_financial": True},
        {"txn_type_id": 102, "txn_name": "Withdrawal", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 103, "txn_name": "Bill Payment", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 104, "txn_name": "Airtime Purchase", "direction": "OUT", "is_financial": True}
    ]
    txn_type_df = pd.DataFrame(txn_type_data)
    txn_type_path = os.path.join(LOCAL_DATA_DIR, "dim_transaction_types.csv")
    txn_type_df.to_csv(txn_type_path, index=False)
    print(f"Saved dim_transaction_types.csv to {txn_type_path}")

    # 4. dim_customers (200 customer profiles)
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
    print(f"Saved dim_customers.csv to {customers_path}")


def generate_daily_transactions_csv(execution_date, **kwargs):
    """Generates daily transactions CSV incorporating 10% simulated bad data."""
    print(f"Generating daily transaction data for logical date: {execution_date}")
    
    seed_val = int(execution_date.replace("-", ""))
    random.seed(seed_val)
    Faker.seed(seed_val)
    
    terminals = [f"TERM-{i}" for i in range(7002, 7051)]
    
    customers_path = os.path.join(LOCAL_DATA_DIR, "dim_customers.csv")
    if os.path.exists(customers_path):
        customer_df = pd.read_csv(customers_path)
        customer_phones = customer_df["customer_phone"].tolist()
    else:
        customer_phones = [f"234803{random.randint(1000000, 9999999)}" for _ in range(80)]
    txn_types = [101, 102, 103, 104]
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]

    txn_rows = []
    num_txns = 1500

    for i in range(num_txns):
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        timestamp = f"{execution_date} {hour:02d}:{minute:02d}:{second:02d}"
        
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
        
        # Inject 10% bad data
        is_corrupt = (random.random() < 0.10)
        if is_corrupt:
            corruption_type = random.choice([
                "missing_phone",
                "missing_terminal",
                "negative_amount",
                "invalid_txn_type",
                "duplicate_txid",
                "malformed_timestamp",
                "missing_txid",
                "invalid_amount_type"
            ])
            
            if corruption_type == "missing_phone":
                cust_phone = None
            elif corruption_type == "missing_terminal":
                terminal_id = None
            elif corruption_type == "negative_amount":
                amount = -1 * amount
            elif corruption_type == "invalid_txn_type":
                txn_type = 999
            elif corruption_type == "duplicate_txid" and len(txn_rows) > 0:
                prev_row = random.choice(txn_rows)
                txid = prev_row["txid"]
            elif corruption_type == "malformed_timestamp":
                if random.choice([True, False]):
                    timestamp = "INVALID_TIMESTAMP"
                else:
                    timestamp = f"2035-12-31 {hour:02d}:{minute:02d}:{second:02d}"
            elif corruption_type == "missing_txid":
                txid = None
            elif corruption_type == "invalid_amount_type":
                amount = "NaN"
        
        txn_rows.append({
            "txid": txid,
            "createdat": timestamp,
            "terminalid": terminal_id,
            "custphone": cust_phone,
            "txntypecode": txn_type,
            "amount": amount,
            "fee_charged": fee_charged,
            "status": status_val
        })
        
    df = pd.DataFrame(txn_rows)
    date_nodash = execution_date.replace("-", "")
    file_path = os.path.join(LOCAL_DATA_DIR, f"lnd_{date_nodash}.csv")
    df.to_csv(file_path, index=False)
    print(f"Successfully generated {num_txns} transactions (with bad data injected) and saved to {file_path}")


def upload_to_gcs_hmac(local_path, remote_path, **kwargs):
    """Uploads a local file to GCS using HMAC credentials via S3-compatible Interoperability API."""
    access_key = os.environ.get("GCP_HMAC_ACCESS_KEY")
    secret_key = os.environ.get("GCP_HMAC_SECRET_KEY")
    bucket_name = os.environ.get("GCP_BUCKET_NAME")

    if not access_key or access_key == "your_access_key_here":
        raise ValueError("GCP_HMAC_ACCESS_KEY is not configured or holds a placeholder.")
    if not secret_key or secret_key == "your_secret_key_here":
        raise ValueError("GCP_HMAC_SECRET_KEY is not configured or holds a placeholder.")
    if not bucket_name or bucket_name == "your_gcs_bucket_name_here":
        raise ValueError("GCP_BUCKET_NAME is not configured or holds a placeholder.")

    # Strip quotes if they were added in the env file
    access_key = access_key.strip("'\"")
    secret_key = secret_key.strip("'\"")
    bucket_name = bucket_name.strip("'\"")

    # Handle bucket names that include a path prefix (e.g., '3mtt-mentees-bucket/john')
    prefix = ""
    if "/" in bucket_name:
        parts = bucket_name.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1].strip("/") + "/"

    final_remote_path = prefix + remote_path

    print(f"Initializing GCS HMAC Client for upload: {local_path} -> gs://{bucket_name}/{final_remote_path}")
    
    # GCS interop is fully compatible with S3 endpoint
    from botocore.config import Config
    s3 = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version='s3v4',
            request_checksum_calculation='when_required',
            response_checksum_validation='when_required'
        )
    )
    
    s3.upload_file(local_path, bucket_name, final_remote_path)
    print("Upload completed successfully!")


with DAG(
    'agency_banking_data_generation',
    default_args=default_args,
    description='Generate daily agency banking transactions and dimensions, and upload to GCS via HMAC',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Dimension Tables Tasks
    generate_dimensions = PythonOperator(
        task_id='generate_dimensions',
        python_callable=generate_dimensions_csv,
    )

    upload_dim_geography = PythonOperator(
        task_id='upload_dim_geography',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_geography.csv',
            'remote_path': 'dimensions/dim_geography.csv'
        }
    )

    upload_dim_agents = PythonOperator(
        task_id='upload_dim_agents',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_agents.csv',
            'remote_path': 'dimensions/dim_agents.csv'
        }
    )

    upload_dim_geography_archive = PythonOperator(
        task_id='upload_dim_geography_archive',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_geography.csv',
            'remote_path': 'dimensions/archive/dim_geography_{{ ds_nodash }}.csv'
        }
    )

    upload_dim_agents_archive = PythonOperator(
        task_id='upload_dim_agents_archive',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_agents.csv',
            'remote_path': 'dimensions/archive/dim_agents_{{ ds_nodash }}.csv'
        }
    )

    # 2. Daily Transaction Tasks
    generate_transactions = PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_daily_transactions_csv,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    upload_transactions = PythonOperator(
        task_id='upload_transactions',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': LOCAL_DATA_DIR + '/lnd_{{ ds_nodash }}.csv',
            'remote_path': 'landing/lnd_{{ ds_nodash }}.csv'
        }
    )

    upload_dim_customers = PythonOperator(
        task_id='upload_dim_customers',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_customers.csv',
            'remote_path': 'dimensions/dim_customers.csv'
        }
    )

    upload_dim_customers_archive = PythonOperator(
        task_id='upload_dim_customers_archive',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_customers.csv',
            'remote_path': 'dimensions/archive/dim_customers_{{ ds_nodash }}.csv'
        }
    )

    upload_dim_transaction_types = PythonOperator(
        task_id='upload_dim_transaction_types',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_transaction_types.csv',
            'remote_path': 'dimensions/dim_transaction_types.csv'
        }
    )

    upload_dim_transaction_types_archive = PythonOperator(
        task_id='upload_dim_transaction_types_archive',
        python_callable=upload_to_gcs_hmac,
        op_kwargs={
            'local_path': f'{LOCAL_DATA_DIR}/dim_transaction_types.csv',
            'remote_path': 'dimensions/archive/dim_transaction_types_{{ ds_nodash }}.csv'
        }
    )

    # Dependencies
    generate_dimensions >> [
        upload_dim_geography, 
        upload_dim_agents, 
        upload_dim_geography_archive, 
        upload_dim_agents_archive,
        upload_dim_customers,
        upload_dim_customers_archive,
        upload_dim_transaction_types,
        upload_dim_transaction_types_archive
    ]
    generate_dimensions >> generate_transactions
    generate_transactions >> upload_transactions
