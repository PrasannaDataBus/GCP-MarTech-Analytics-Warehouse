from airflow import DAG
from airflow.operators.bash import BashOperator    # <--- for dbt CLI
from datetime import datetime, timedelta
import os
import sys

# ==============================================================================
# DAG CONFIGURATION (DEV ENVIRONMENT)
# ==============================================================================
# Goal: Test dbt transformations manually.
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
    'ads_words_performance_dag',
    default_args=default_args,
    description='Orchestrates Ads Words Performance dbt Silver and Gold transformations',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'dev', 'words', 'creative', 'search_terms'],
) as dag:

    # 1. Check Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views for SPEND only)
    # Target: The NEW Google Search Term view AND the Meta Creative Performance view
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select stg_google_ads_search_terms stg_meta_ads_creative_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Table for SPEND only)
    # Target: The NEW ads_words_performance table
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select ads_words_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    # Tests the Gold table and its upstream parents (Silver & Raw)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --select +ads_words_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test