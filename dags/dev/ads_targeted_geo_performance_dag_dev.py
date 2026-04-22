from airflow import DAG
from airflow.operators.bash import BashOperator    # <--- for dbt CLI
from datetime import datetime, timedelta
import os
import sys

# ==============================================================================
# DAG CONFIGURATION (DEV ENVIRONMENT)
# ==============================================================================
# Goal: Test Geographic ROAS dbt transformations (Google & Meta).
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
    'ads_targeted_geo_dag',
    default_args=default_args,
    description='Orchestrates Ads Geography ROAS dbt Silver and Gold transformations',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'dev', 'geo roas', 'geography roas', 'location roas'],
) as dag:

    # 1. Check Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views for Geography ONLY)
    # We select the Google Targeted Geo (with lookup join) and Meta Region scripts
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select stg_google_ads_user_location stg_meta_ads_geo --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Table for Geography ONLY)
    # Target the unified ads_targeted_geo_performance table
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select ads_targeted_geo_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    # Run tests on the ads_targeted_geo_performance model and its direct parents
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --select +ads_targeted_geo_performance --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test