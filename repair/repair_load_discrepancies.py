# ==============================================================================
# REPAIR SCRIPT: FORCE FULL HISTORY RELOAD FOR SPECIFIC GOOGLE ADS ACCOUNTS
# ==============================================================================
# Use this to fix accounts where BigQuery data is "Lower" than Google Ads UI
# because historical data (2021-2024) maybe missed during initial loads.
# ==============================================================================

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
from datetime import datetime, timezone, date, timedelta
import pandas as pd
from dotenv import load_dotenv
import re
import os
from pathlib import Path


# --- ENVIRONMENT LOADING LOGIC (Copied from your Main Script) --------------
def load_environment():
    """
    Load params.env dynamically based on your local folder structure.
    """
    # Airflow container check
    airflow_env = Path("/opt/airflow/secrets/params.env")
    if airflow_env.exists():
        load_dotenv(airflow_env.as_posix(), override = True)
        print(f"Airflow detected. Loaded: {airflow_env}")
        return

    # Local path-based detection (Windows)
    script_path = Path(__file__).resolve() if "__file__" in globals() else Path.cwd().resolve()
    script_str = str(script_path)

    # Match the folder pattern: GCP MarTech Analytics Warehouse - <Env>
    m = re.search(r"GCP MarTech Analytics Warehouse - ([A-Za-z]+)", script_str, flags = re.IGNORECASE)

    # Fallback if running from a temp folder (common with simple scripts)
    if not m:
        # HARDCODED FALLBACK: Point directly to your Dev environment file if detection fails
        # Update "Dev" to "Prod" if needed
        print("Warning: Path detection failed. Attempting hardcoded path for DEV.")
        base_path = Path(r"C:\Users\prasa\Root")
        env_file = base_path / "GCP MarTech Analytics Warehouse - Dev" / "params.env"
    else:
        env = m.group(1).strip().upper()
        base_path = Path(r"C:\Users\prasa\Root")
        folder_name = f"GCP MarTech Analytics Warehouse - {env.title()}"
        env_file = (base_path / folder_name / "params.env")

    if not env_file.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file}")

    load_dotenv(env_file.as_posix(), override = True)
    print(f"Local detected. Loaded: {env_file}")


# EXECUTE LOAD IMMEDIATELY
load_environment()

# --- CONFIGURATION ---------------------------------------------------------

GOOGLE_ADS_CONFIG = os.getenv("GOOGLE_ADS_CONFIG")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
PERFORMANCE_TABLE_NAME = os.getenv("PERFORMANCE_TABLE_NAME")

# *** CRITICAL: PUT BAD ACCOUNT IDs HERE ***
TARGET_BAD_ACCOUNT_IDS = ['']

# REPAIR DATE RANGE
START_DATE = "2022-11-12"
END_DATE = str(date.today())

# GAQL QUERY TEMPLATE (Same as your main script)
QUERY_TEMPLATE = """
SELECT
  segments.date,
  customer.id,
  customer.descriptive_name,
  customer.currency_code,
  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.bidding_strategy_type,
  ad_group.id,
  ad_group.name,
  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  segments.device,
  segments.ad_network_type,
  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.average_cpc,
  metrics.cost_micros,
  metrics.conversions,
  metrics.conversions_value,
  metrics.all_conversions,
  metrics.view_through_conversions,
  metrics.engagements
FROM ad_group_ad
WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
"""


# --- 2. CLIENT HELPER ---------------------------------------------------------
def get_clients():
    if not GOOGLE_ADS_CONFIG:
        raise ValueError("GOOGLE_ADS_CONFIG environment variable is missing")

    # Ensure Google Cloud credentials are visible
    if CREDENTIALS_PATH:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

    print("--- Connecting to Clients ---")
    ads_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG)
    bq_client = bigquery.Client()
    return ads_client, bq_client


# --- 3. EXTRACTION FUNCTION (Reused) ------------------------------------------
def extract_ads_data(customer_id: str, start_date: str, end_date: str, ads_client):
    """Extracts performance data for the full repair range."""
    print(f"   > API Call: Fetching {start_date} to {end_date}...")
    service = ads_client.get_service("GoogleAdsService")
    query = QUERY_TEMPLATE.format(start_date = start_date, end_date = end_date)

    # Search Stream is faster for large datasets
    response = service.search_stream(customer_id = customer_id, query = query)

    rows = []
    for batch in response:
        for row in batch.results:
            rows.append({
                "date": row.segments.date,
                "account_id": str(row.customer.id),
                "account_name": row.customer.descriptive_name,
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "campaign_status": row.campaign.status.name,
                "ad_group_id": str(row.ad_group.id),
                "ad_group_name": row.ad_group.name,
                "ad_id": str(row.ad_group_ad.ad.id) if row.ad_group_ad.ad else None,
                "ad_name": getattr(row.ad_group_ad.ad, "name", None),
                "ad_type": getattr(row.ad_group_ad.ad.type_, "name", None),
                "ad_network_type": getattr(row.segments.ad_network_type, "name", None),
                "device": getattr(row.segments.device, "name", None),
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "ctr": row.metrics.ctr,
                "average_cpc": (
                    float(row.metrics.average_cpc.micros) / 1_000_000
                    if getattr(row.metrics.average_cpc, "micros", None) is not None
                    else None
                ),
                "cost_micros": row.metrics.cost_micros,
                "conversions": row.metrics.conversions,
                "conversions_value": row.metrics.conversions_value,
                "all_conversions": row.metrics.all_conversions,
                "view_through_conversions": row.metrics.view_through_conversions,
                "engagements": row.metrics.engagements,
                "bidding_strategy_type": getattr(row.campaign.bidding_strategy_type, "name", None),
                "currency": row.customer.currency_code,
                "_ingested_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        # Data Cleaning
        df["date"] = pd.to_datetime(df["date"], errors = "coerce").dt.date
        df = df.astype({
            "impressions": "int64",
            "clicks": "int64",
            "engagements": "int64",
            "cost_micros": "int64",
            "ctr": "float64",
            "average_cpc": "float64",
            "conversions": "float64",
            "conversions_value": "float64",
            "all_conversions": "float64",
            "view_through_conversions": "float64",
        })
    return df


# --- 4. LOAD & REPAIR FUNCTION ------------------------------------------------
def repair_bigquery_data(df: pd.DataFrame, account_id: str, bq_client):
    """Deletes OLD fragmented data for this account and inserts NEW full history."""
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{PERFORMANCE_TABLE_NAME}"

    # 1. DELETE existing data for this account in the repair range
    # This prevents duplicates if you have 'some' data in 2024 but none in 2022.
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE account_id = '{account_id}'
        AND date BETWEEN '{START_DATE}' AND '{END_DATE}'
    """
    print(f"   > BigQuery: Deleting old fragmented data for {account_id}...")
    bq_client.query(delete_query).result()

    # 2. LOAD the new clean dataframe
    print(f"   > BigQuery: Loading {len(df)} rows of clean history...")
    job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_APPEND")
    # Schema matches your existing table automatically

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()  # Wait for completion
    print(f"   > SUCCESS: {account_id} is fully repaired.")


# --- 5. MAIN EXECUTION --------------------------------------------------------
def main():
    print(f"=== STARTING REPAIR JOB ===")
    print(f"Targets: {TARGET_BAD_ACCOUNT_IDS}")
    print(f"Range:   {START_DATE} to {END_DATE}")

    ads_client, bq_client = get_clients()

    # Get Account Names just for better logging (Optional)
    manager_id = ads_client.login_customer_id or ads_client.client_customer_id
    service = ads_client.get_service("GoogleAdsService")
    # Simple query to get names
    q = "SELECT customer_client.id, customer_client.descriptive_name FROM customer_client WHERE " \
        "customer_client.manager = FALSE"
    res = service.search(request = {"customer_id": manager_id, "query": q})
    id_map = {str(row.customer_client.id): row.customer_client.descriptive_name for row in res}

    # Loop through ONLY the bad accounts
    for customer_id in TARGET_BAD_ACCOUNT_IDS:
        acct_name = id_map.get(customer_id, "Unknown Account")
        print(f"\nProcessing Account: {acct_name} ({customer_id})")

        try:
            # Step 1: Extract Full History
            df = extract_ads_data(customer_id, START_DATE, END_DATE, ads_client)

            if df.empty:
                print(f"   > WARNING: Google Ads API returned ZERO data. Check if account existed in {START_DATE}.")
                continue

            print(f"   > Extracted {len(df)} rows.")

            # Step 2: Delete Old & Load New
            repair_bigquery_data(df, customer_id, bq_client)

        except Exception as e:
            print(f"   > ERROR: Failed to repair {customer_id}. Reason: {e}")

    print("\n=== REPAIR JOB COMPLETE ===")


if __name__ == "__main__":
    main()