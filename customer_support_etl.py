from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import timedelta, datetime

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator

from datetime import datetime
def extract_support_tickets():
    print("Extracting tickets from Zendesk...")
def extract_chat_logs():
    print("Extracting chat logs from Intercom...")
def normalize_records():
    print("Normalizing and joining support records...")
def load_to_warehouse():
    print(":arrows_counterclockwise: Preparing to load transformed data to finance.payment_logs warehouse...")
    print(":test_tube: Step 1: Validating schema compatibility... OK")
    print(":stopwatch: Step 2: Converting timestamps to UTC format... OK")
    print(":card_file_box: Step 3: Splitting data into uploadable chunks... OK")
    print(":truck: Step 4: Uploading batch 1 of 5... SUCCESS")
    print(":truck: Step 5: Uploading batch 2 of 5... SUCCESS")
    print(":truck: Step 6: Uploading batch 3 of 5... SUCCESS")
    print(":rotating_light: ERROR: Batch 4 upload failed during final commit phase.")
    print(":boom: Exception: [WarehouseCommitError] Write transaction could not be finalized. Error Code: WARE-9021")
    print("   Possible cause: Concurrent write conflict or quota exceeded on table `finance.payment_logs`.")
    print(":wood: Detailed error logs saved at:")
    print(" :Error log is written in s3 at location: s3://hackathon-smart-incident-manager-final/input-lambda/customer_support_20250423_1_batch_4_commit_error.json")
    print(":globe_with_meridians: View real-time ingestion monitor:")
    print("   :point_right: https://internal-tools.myorg.com/warehouse-monitor?table=finance.payment_logs&run_id=2025-04-23")
    raise Exception("WarehouseLoadFailure: Commit transaction failed for batch 4.")
def generate_summary_metrics():
    print("Generating summary metrics: CSAT, response time, ticket volume...")
with DAG(
    dag_id="customer_support_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "support"]
) as dag:
    task_1 = PythonOperator(
        task_id="extract_support_tickets",
        python_callable=extract_support_tickets
    )
    task_2 = PythonOperator(
        task_id="extract_chat_logs",
        python_callable=extract_chat_logs
    )
    task_3 = PythonOperator(
        task_id="normalize_records",
        python_callable=normalize_records
    )
    task_4 = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse
    )
    task_5 = PythonOperator(
        task_id="generate_summary_metrics",
        python_callable=generate_summary_metrics
    )
    task_1 >> task_2 >> task_3 >> task_4 >> task_5
