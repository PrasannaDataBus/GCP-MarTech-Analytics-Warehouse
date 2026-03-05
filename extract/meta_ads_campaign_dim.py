# Process: Extract Meta Campaign Dimensions (Schedules & Status)
# Data Points: Campaign ID, Name, Status, Start Time, Stop Time
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Materialization: Dimension Table (Overwrites per Account)
# MarTech Dictionary: Refer SharePoint file - MarTech Data Dictionary

import os
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

# --- Meta SDK Imports ---
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User


# --- ENVIRONMENT LOADING LOGIC ---
def load_environment():
    """
    Load params.env dynamically:
      - If inside Airflow: /opt/airflow/secrets/params.env
      - Else (local Windows): infer env (Dev/Prod/…) from script path
        '...\\GCP MarTech Analytics Warehouse - <Env>\\...'
        and load '<base>\\GCP MarTech Analytics Warehouse - <Env>\\params.env'
    Returns the detected environment name in UPPERCASE (e.g., 'DEV', 'PROD').
    """
    # Airflow container
    airflow_env = Path("/opt/airflow/secrets/params.env")
    if airflow_env.exists():
        load_dotenv(airflow_env.as_posix(), override=True)
        env = (os.getenv("ENVIRONMENT_NAME") or os.getenv("ENVIRONMENT") or "AIRFLOW").strip().upper()
        print(f"Airflow detected. Loaded: {airflow_env}")
        print(f"Effective ENV: {env}")
        return env

    # Local path-based detection (Windows)
    # Use __file__ if available, else fall back to CWD (helps in REPL/tests)
    script_path = Path(__file__).resolve() if "__file__" in globals() else Path.cwd().resolve()
    script_str = str(script_path)

    # Match the folder pattern: GCP MarTech Analytics Warehouse - <Env>
    m = re.search(r"GCP MarTech Analytics Warehouse - ([A-Za-z]+)", script_str, flags=re.IGNORECASE)
    if not m:
        raise ValueError(
            "Unable to detect environment from path. Expected path segment like "
            "'GCP MarTech Analytics Warehouse - Dev' or '- Prod'. "
            f"Got: {script_str}"
        )

    env = m.group(1).strip().upper()  # e.g., DEV, PROD, UAT, etc.
    base_path = Path(r"C:\Users\prasa\Root")
    folder_name = f"GCP MarTech Analytics Warehouse - {env.title()}"
    env_file = (base_path / folder_name / "params.env")

    if not env_file.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file}")

    load_dotenv(env_file.as_posix(), override=True)

    # if ENVIRONMENT_NAME exists in params.env, ensure it matches
    file_env = (os.getenv("ENVIRONMENT_NAME") or env).strip().upper()
    if file_env != env:
        print(f"Mismatch: path env={env}, file ENVIRONMENT_NAME={file_env}")

    print(f"Local detected. Loaded: {env_file}")
    print(f"Effective ENV: {file_env}")
    return env


# usage
current_env = load_environment()
if __name__ == "__main__":
    print(f"Running in {current_env} environment")

# --- Set Google credentials dynamically (Safe String) ---
if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
META_CAMPAIGN_DIM_TABLE_NAME = os.getenv("META_CAMPAIGN_DIM_TABLE_NAME")


# ---------------------------------------------------------
# HELPER: LAZY LOAD CLIENTS (BQ Only for Meta Script)
# ---------------------------------------------------------
def get_bq_client():
    return bigquery.Client()


# --- HELPER: Get Ad Accounts ---
def get_ad_accounts():
    """Fetches all Ad Accounts attached to the System User."""
    me = User(fbid = 'me')
    my_accounts = me.get_ad_accounts(fields = ['name', 'account_id'])
    return [{'id': acc['id'], 'name': acc.get('name', 'Unnamed Account')} for acc in my_accounts]


# --- EXTRACTION FUNCTION ---
def extract_campaign_dimensions(account_id: str, account_name: str):
    """Extracts Campaign Metadata (Status, Start, Stop) without time-series limitations."""
    formatted_id = account_id if account_id.startswith('act_') else f"act_{account_id}"
    account = AdAccount(formatted_id)

    print(f"  -> Fetching campaign dimensions for {account_name}...")

    # We pull the metadata for ALL campaigns in the account
    fields = ['id', 'name', 'effective_status', 'start_time', 'stop_time']
    params = {'limit': 10000}

    all_data_rows = []

    try:
        campaigns = account.get_campaigns(fields = fields, params = params)

        for c in campaigns:
            row = {
                "account_id": formatted_id.replace('act_', ''),
                "campaign_id": c.get('id'),
                "campaign_name": c.get('name'),
                "effective_status": c.get('effective_status', 'UNKNOWN'),
                "start_time": c.get('start_time'),  # Meta returns ISO 8601 strings
                "stop_time": c.get('stop_time'),  # Can be None if no end date is set
                "_ingested_at": datetime.now()
            }
            all_data_rows.append(row)

    except Exception as e:
        print(f"  -> Error fetching campaigns for {account_name}: {e}")
        raise e

    df = pd.DataFrame(all_data_rows)

    # Convert Meta's ISO strings to true datetime objects for BigQuery
    if not df.empty:
        df['start_time'] = pd.to_datetime(df['start_time'], errors = 'coerce')
        df['stop_time'] = pd.to_datetime(df['stop_time'], errors = 'coerce')

    return df


# --- LOAD FUNCTION ---
def load_dimensions_to_bigquery(df: pd.DataFrame, account_id: str, bq_client):
    """Idempotent load: Deletes existing campaigns for this account, then inserts fresh data."""
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{META_CAMPAIGN_DIM_TABLE_NAME}"
    clean_acc_id = account_id.replace('act_', '')

    # 1. Delete old dimension records for this specific account
    delete_query = f"""
        DELETE FROM `{table_id}` WHERE account_id = '{clean_acc_id}'
    """
    try:
        bq_client.query(delete_query).result()
        print(f"  -> Cleared existing dimensions for {clean_acc_id}")
    except Exception:
        pass  # Table might not exist yet on first run

    # 2. Append fresh state
    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_APPEND",
        schema = [
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("effective_status", "STRING"),
            bigquery.SchemaField("start_time", "TIMESTAMP"),
            bigquery.SchemaField("stop_time", "TIMESTAMP"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ],
        # No time partitioning needed for a small dimension table, but clustering helps joins
        clustering_fields = ["account_id", "campaign_id"]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    print(f"  -> Loaded {len(df)} campaigns into {table_id}")


# --- MAIN EXECUTION ---
def main():
    load_environment()
    bq_client = get_bq_client()

    META_APP_ID = os.getenv("META_APP_ID")
    META_APP_SECRET = os.getenv("META_APP_SECRET")
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

    if not all([META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN]):
        print("Error: Missing Meta credentials. Check params.env.")
        return

    try:
        FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
        print("Meta API Initialized Successfully")
    except Exception as e:
        print(f"Failed to initialize Meta API: {e}")
        return

    print("--- Starting Meta Ads Campaign Dimension Extraction ---")

    try:
        accounts = get_ad_accounts()
        print(f"Found {len(accounts)} Ad Accounts.")
    except Exception as e:
        print(f"Error fetching ad accounts: {e}")
        return

    failed_accounts = []

    for acc in accounts:
        try:
            df = extract_campaign_dimensions(acc['id'], acc['name'])
            if not df.empty:
                load_dimensions_to_bigquery(df, acc['id'], bq_client)
            else:
                print(f"No campaigns found for {acc['name']}.")
        except Exception as e:
            error_msg = f"Failed to process {acc['name']}: {e}"
            print(error_msg)
            failed_accounts.append(error_msg)

    if failed_accounts:
        print("\nCRITICAL: The following accounts failed extraction:")
        for err in failed_accounts:
            print(f" - {err}")
        raise Exception(f"Script completed with errors in {len(failed_accounts)} accounts.")

    print("--- Meta Campaign Dimensions Load Complete ---")


if __name__ == "__main__":
    main()