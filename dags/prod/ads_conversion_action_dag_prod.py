from airflow import DAG
from airflow.operators.bash import BashOperator    # <--- for dbt CLI
from datetime import datetime, timedelta
import os
import sys

# ==============================================================================
# 📋 DAG CONFIGURATION (PROD ENVIRONMENT)
# ==============================================================================
# Goal: Run dbt transformations (Silver -> Gold) for the Dashboard.
# Schedule: 11:00 AM Daily (Well after Google @ 6am and Meta @ 8am finish)
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
    'ads_conversion_dag',
    default_args=default_args,
    description='Orchestrates Ads Sales (Revenue) dbt Silver and Gold transformations',
    schedule_interval="0 11 * * *",  # <--- Runs at 11:00 AM Paris Time,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'prod', 'sales', 'revenue', 'conversion'],
) as dag:

    # 1. Check Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views for Conversions ONLY)
    # We explicitly select the two conversion scripts to avoid re-running Spend data
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select stg_google_ads_conversion_action stg_meta_ads_conversion_action --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Table for Sales ONLY)
    # Target specific ads_sales table
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select ads_conversion_action --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    # Run tests only on the ads_sales model and its direct parents
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --select +ads_conversion_action --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test