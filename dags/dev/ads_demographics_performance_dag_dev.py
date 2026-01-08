from airflow import DAG
from airflow.operators.bash import BashOperator    # <--- for dbt CLI
from datetime import datetime, timedelta
import os
import sys

# ==============================================================================
# DAG CONFIGURATION (DEV ENVIRONMENT)
# ==============================================================================
# Goal: Test dbt transformations for Age & Gender performance (Google & Meta).
# Schedule: None (Manual Trigger Only)
# ==============================================================================

default_args = {
    'owner': 'Prasanna',
    'depends_on_past': False,
    'email': ['prasanna@euromedicom.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'ads_demographics_dag',
    default_args=default_args,
    description='Orchestrates Ads Demographics (Age/Gender) dbt Silver and Gold transformations',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'dev', 'demographics', 'age', 'gender'],
) as dag:

    # 1. Check Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views for Demographics ONLY)
    # We select the Google Demographics and Meta Demographics scripts
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select stg_google_ads_demographics stg_meta_ads_demographics --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Table for Demographics ONLY)
    # Target the unified ads_demographics_performance table
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select ads_demographics_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    # Run tests on the ads_demographics_performance model and its parents
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --select +ads_demographics_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test