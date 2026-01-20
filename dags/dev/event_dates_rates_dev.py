from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# --- Ensure your repo folder is visible inside the container ---
# This matches your docker-compose volume mapping
sys.path.append("/opt/airflow/repos/gcp_martech_dev/extract")

# Import your existing extraction script
from event_dates_emea_latam_apac import main as event_dates_emea_latam_apac_extraction_main

# --- DAG Configuration ---
default_args = {
    "owner": "Prasanna",
    "depends_on_past": False,
    "email": ["prasanna@euromedicom.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="event_dates_rates_dag",
    default_args=default_args,
    description="Extract and load Event dates and rates data into BigQuery",
    schedule_interval=None,  # Manual trigger only in Dev
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["event_dates_rates", "bigquery", "dev"],
) as dag:

    run_event_dates_rates_emea_latam_apac_extraction = PythonOperator(
        task_id="extract_and_load_event_dates_rates_emea_latam_apac_data",
        python_callable=event_dates_emea_latam_apac_extraction_main,
        dag=dag,
    )

    run_event_dates_rates_emea_latam_apac_extraction
