import os
import pandas as pd
import random
from faker import Faker

# Ensure the data directory exists
os.makedirs("data", exist_ok=True)

# Initialize Faker with localized Nigerian data context for realistic names/locations
fake = Faker(['en_NG'])
Faker.seed(42)
random.seed(42)

def generate_dimensions():
    print("Generating Dimension Table datasets...")
    
    # Real LGAs/Area Councils mapped to their respective states
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
    
    # 1. dim_geography (15 location clusters)
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
        
        # Clean LGA name for location cluster name (e.g. "Kano Municipal" -> "KANOMU")
        clean_lga = lga.upper().replace(" ", "").replace("/", "").replace("-", "")
        cluster_name = f"Cluster_{clean_lga[:6]}"
        
        geo_data.append({
            "geo_id": geo_id,
            "location_cluster": cluster_name,
            "lga": lga,
            "state": state,
            "region": region
        })
    pd.DataFrame(geo_data).to_csv("data/dim_geography.csv", index=False)

    # 2. dim_agents (50 agents linked to geography IDs)
    agent_data = []
    for agent_id in range(1001, 1051):
        agent_data.append({
            "agent_id": agent_id,
            "agent_name": fake.name(),
            "business_name": f"{fake.company()} Ventures",
            "terminal_id": f"TERM-{agent_id + 6000}", # TERM-7001 to TERM-7050
            "tier_level": random.choice(["Bronze", "Silver", "Gold"]),
            "signup_date": fake.date_between(start_date="-2y", end_date="-1m").strftime("%Y-%m-%d")
        })
    pd.DataFrame(agent_data).to_csv("data/dim_agents.csv", index=False)

    # 3. dim_transaction_types (Static lookup dimension)
    txn_type_data = [
        {"txn_type_id": 101, "txn_name": "Deposit", "direction": "IN", "is_financial": True},
        {"txn_type_id": 102, "txn_name": "Withdrawal", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 103, "txn_name": "Bill Payment", "direction": "OUT", "is_financial": True},
        {"txn_type_id": 104, "txn_name": "Airtime Purchase", "direction": "OUT", "is_financial": True}
    ]
    pd.DataFrame(txn_type_data).to_csv("data/dim_transaction_types.csv", index=False)

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
    pd.DataFrame(customer_data).to_csv("data/dim_customers.csv", index=False)
    print("[SUCCESS] Saved dim_geography.csv, dim_agents.csv, dim_transaction_types.csv, and dim_customers.csv to data/")


def generate_daily_landing_file(date_str="20260706"):
    print(f"Generating transaction landing file for {date_str}...")
    
    # Reload generated terminal IDs and pick customer phones from dim_customers.csv to ensure referential integrity
    terminals = [f"TERM-{i}" for i in range(7002, 7051)]
    
    customers_path = "data/dim_customers.csv"
    if os.path.exists(customers_path):
        customer_df = pd.read_csv(customers_path)
        customer_phones = customer_df["customer_phone"].tolist()
    else:
        # Fallback if file doesn't exist yet
        customer_phones = [f"234803{random.randint(1000000, 9999999)}" for _ in range(80)]
    
    # 101: Deposit, 102: Withdrawal, 103: Bill Pay, 104: Airtime
    txn_types = [101, 102, 103, 104] 
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"] # Heavily skewed toward SUCCESS

    txn_rows = []
    # Simulating 1,500 transactions captured in a single operational day
    for i in range(1500):
        # Format timestamp to mock hours throughout the chosen batch day
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        timestamp = f"2026-07-06 {hour:02d}:{minute:02d}:{second:02d}"
        
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(500.0, 75000.0), 2)
        
        # Calculate fee_charged based on transaction type and amount
        if txn_type == 101:    # Deposit (0.5% fee, capped at 500 NGN)
            fee_charged = min(amount * 0.005, 500.0)
        elif txn_type == 102:  # Withdrawal (1.0% fee, capped at 1,000 NGN)
            fee_charged = min(amount * 0.01, 1000.0)
        elif txn_type == 103:  # Bill Payment (Flat 100 NGN fee)
            fee_charged = 100.0
        elif txn_type == 104:  # Airtime Purchase (No fee charged to customer)
            fee_charged = 0.0
        else:
            fee_charged = 0.0
            
        fee_charged = round(fee_charged, 2)
        
        txn_rows.append({
            "txid": f"TXN-{random.randint(100000, 999999)}",
            "createdat": timestamp,
            "terminalid": random.choice(terminals),
            "custphone": random.choice(customer_phones),
            "txntypecode": txn_type,
            "amount": amount,
            "fee_charged": fee_charged,
            "status": random.choice(statuses)
        })
        
    file_name = f"data/lnd_{date_str}.csv"
    pd.DataFrame(txn_rows).to_csv(file_name, index=False)
    print(f"[SUCCESS] Saved {file_name} successfully!")

if __name__ == "__main__":
    generate_dimensions()
    generate_daily_landing_file()