from airflow import DAG
from airflow.operators.bash import BashOperator     # <--- for dbt CLI
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
    'retries': 0, # No retries in Dev, we want to see errors immediately
}

with DAG(
    'ads_performance_dag',
    default_args=default_args,
    description='Orchestrates Ads Performance dbt Silver and Gold transformations (Dev)',
    schedule_interval=None, # <--- Manual Trigger Only for Dev
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'dev'],
) as dag:

    # 1. Check Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views)
    # Note: In Dev, this uses the 14-day "Cost Saver" logic automatically
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select tag:silver --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Tables)
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select tag:gold --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test