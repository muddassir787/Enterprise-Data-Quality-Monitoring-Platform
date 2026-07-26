import os
import csv
import uuid
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Path to incoming data directory
INCOMING_DIR = "/data/incoming"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

def create_mock_dataset(file_type: str):
    """
    Generates a CSV file containing mock sales transactions.
    'clean' files contain 50 correct records.
    'dirty' files contain 45 correct records and 5 violating records.
    """
    os.makedirs(INCOMING_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{file_type}_sales_{timestamp}.csv"
    filepath = os.path.join(INCOMING_DIR, filename)
    
    categories = ["Electronics", "Apparel", "Home", "Books", "Beauty"]
    records = []
    
    # 1. Generate 45 clean records for both types
    for _ in range(45):
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"CUST_{random.randint(1000, 9999)}",
            "product_category": random.choice(categories),
            "amount": round(random.uniform(5.00, 1200.00), 2),
            "transaction_date": (datetime.now() - timedelta(minutes=random.randint(10, 1440))).strftime("%Y-%m-%d %H:%M:%S")
        })
        
    # 2. Add records based on file type
    if file_type == "dirty":
        # Violation 1: Duplicate transaction_id
        dup_id = str(uuid.uuid4())
        records.append({
            "transaction_id": dup_id,
            "customer_id": "CUST_8888",
            "product_category": "Apparel",
            "amount": 25.50,
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        records.append({
            "transaction_id": dup_id,  # Duplicate ID
            "customer_id": "CUST_8888",
            "product_category": "Apparel",
            "amount": 25.50,
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Violation 2: Missing customer_id (Null value)
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": "",  # Empty string/Null
            "product_category": "Books",
            "amount": 12.99,
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Violation 3: Negative amount
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": "CUST_1234",
            "product_category": "Home",
            "amount": -150.00,  # Negative
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Violation 4: Invalid product category
        records.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": "CUST_5678",
            "product_category": "Automotive",  # Non-approved category
            "amount": 450.00,
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    else:
        # Complete the clean set with 5 more valid records
        for _ in range(5):
            records.append({
                "transaction_id": str(uuid.uuid4()),
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "product_category": random.choice(categories),
                "amount": round(random.uniform(5.00, 1200.00), 2),
                "transaction_date": (datetime.now() - timedelta(minutes=random.randint(10, 1440))).strftime("%Y-%m-%d %H:%M:%S")
            })
            
    # Shuffle the records to make sure they are randomly ordered
    random.shuffle(records)
    
    # Write to CSV file
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["transaction_id", "customer_id", "product_category", "amount", "transaction_date"])
        writer.writeheader()
        writer.writerows(records)
        
    print(f"Successfully generated {len(records)} records in: {filepath}")

with DAG(
    "generate_mock_data",
    default_args=default_args,
    description="DAG to simulate file delivery by generating clean or dirty transactions",
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_clean = PythonOperator(
        task_id="generate_clean_data",
        python_callable=create_mock_dataset,
        op_kwargs={"file_type": "clean"},
    )

    generate_dirty = PythonOperator(
        task_id="generate_dirty_data",
        python_callable=create_mock_dataset,
        op_kwargs={"file_type": "dirty"},
    )
