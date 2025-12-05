from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    'email_on_failure': True, # Requires SMTP setup in .env
    'email_on_retry': False,
    'retries': 1,             # Retry once if BigQuery blips (Best Practice for Prod)
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ads_performance_dag',
    default_args=default_args,
    description='Orchestrates Ads Performance dbt Silver and Gold transformations (Prod)',
    schedule_interval="0 11 * * *",  # <--- Runs at 11:00 AM Paris Time
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'prod'],
) as dag:

    # 1. Check Connection
    # Fails fast if credentials are wrong
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt debug --profiles-dir /opt/airflow/secrets
        """
    )

    # 2. Run Silver (Staging Views)
    # Note: In Prod, this loads ALL HISTORY (Cost Saver logic is ignored)
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select tag:silver --profiles-dir /opt/airflow/secrets
        """
    )

    # 3. Run Gold (Marts Tables)
    # Creates the physical table used by Looker
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --select tag:gold --profiles-dir /opt/airflow/secrets
        """
    )

    # 4. Test Data Quality
    # Ensures no duplicates or bad data in the final table
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt test --profiles-dir /opt/airflow/secrets
        """
    )

    # Execution Order
    dbt_debug >> dbt_silver >> dbt_gold >> dbt_test