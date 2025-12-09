# Process: Extract CRM Paid Conversions and Ingest into BigQuery
# Data Source: MySQL Database / Analytics Hub (events_analytics_hub_emea_latam_apac & events_analytics_hub_americas)
# Destination: BigQuery (marketing_raw.crm_conversions_raw)
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Strategy: Full Refresh (WRITE_TRUNCATE) - Daily Snapshot
# Partitioning: By 'date_submit' (Day)
# Clustering: By UTM Source, UTM Medium, UTM Campaign, Event Series

import os
import re
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from google.cloud import bigquery
from urllib.parse import quote_plus


# --- Detect environment ---

# You can set this with PowerShell: $env:ENVIRONMENT = "DEV" (temporary) or setx ENVIRONMENT "DEV" (permanent)
# Verify using: echo $env:ENVIRONMENT

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


# Initialize Env
current_env = load_environment()
if __name__ == "__main__":
    print(f"Running in {current_env} environment")

# --- LOAD CONFIG STRINGS (SAFE AT TOP LEVEL) ---
GOOGLE_ADS_CONFIG = os.getenv("GOOGLE_ADS_CONFIG")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Ensure env var is set for BigQuery (Safe)
if CREDENTIALS_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
CRM_CONVERSIONS_TABLE_NAME = os.getenv("CRM_CONVERSIONS_TABLE_NAME")

# Database Credentials
DB_USER = os.getenv("DB_USER_SG_EMCDB")
DB_PASSWORD = os.getenv("DB_PASSWORD_SG_EMCDB")
DB_HOST = os.getenv("DB_HOST_SG_EMCDB")
DB_NAME = os.getenv("DB_NAME_SG_EMCDB")

# --- SQL QUERY ---
CRM_QUERY = """
/* -------------------------------------------------------
   REGION 1: EMEA / LATAM / APAC
------------------------------------------------------- */
SELECT
    id_commande,
    year,
    date_submit,
    date_time_submit,
    time_submit,
    time_period,
    time_submit_converted,
    time_period_converted,
    conference_series,
    conference_editions,
    cut_off_rate,
    total_ht,
    currency,
    order_type,
    city,
    country,
    region,
    primary_language,
    specialty,
    utm_source,
    utm_medium,
    utm_campaign,
    orders,
    new_customer
FROM events_analytics_hub_emea_latam_apac
WHERE 
    -- 1. Explicitly Paid Sources
    utm_source IN ('AdWords', 'google_ads', 'meta-SiteLink', 'facebook_ads', 'instagram_ads')

    OR

    -- 2. Ambiguous Sources (Must have Paid Medium)
    (
        utm_source IN (
            'google', 'google.com', 'Youtube', 'youtube', 'youtube.com', 'gdn',
            'facebook', 'facebook.com', 'fb', 'instagram', 'instagram.com', 'ig', 'instragram', 'meta'
        )
        AND
        utm_medium IN (
            'cpc', 'ppc', 'cpm', 'cpv', 'cpa', 'paid', 'display', 'banner',
            'Paid Search', 'paid search', 'paid_search',
            'Paid Pmax', 'P Max', 'P+Max', 'pmax',
            'Social Ads', 'social ads', 'Paid Social', 'paid social', 'early bird ads'
        )
        AND utm_source != ''
    )
    
UNION ALL

/* -------------------------------------------------------
   REGION 2: NORTH AMERICA (Americas)
------------------------------------------------------- */
SELECT
    id_customer as id_commande,             -- Mapping ID
    year,
    registration_date_time as date_submit,  -- Mapping Date
    order_date_time as date_time_submit,    -- Mapping Timestamp
    time_submit,
    time_period,
    time_submit_converted,
    time_period_converted,
    conference_series,
    conference_editions,
    cut_off_rate,
    amount_received as total_ht,            -- Mapping Revenue
    'USD' as currency,                       -- Explicit NULL per requirement
    order_type,
    city,
    country,
    region,
    primary_language,
    speciality as specialty,                -- Fix spelling
    utm_source,
    utm_medium,
    utm_campaign,
    orders,
    new_customer
FROM events_analytics_hub_americas
WHERE 
    -- 1. Explicitly Paid Sources
    utm_source IN ('AdWords', 'google_ads', 'meta-SiteLink', 'facebook_ads', 'instagram_ads')
    OR
    (
        -- 2. Ambiguous Sources (Must have Paid Medium)
        utm_source IN (
            'google', 'google.com', 'Youtube', 'youtube', 'youtube.com', 'gdn',
            'facebook', 'facebook.com', 'fb', 'l.facebook.com', 'lm.facebook.com',
            'instagram', 'instagram.com', 'ig', 'instragram', 'meta'
        )
        AND
        -- Paid Mediums
        utm_medium IN (
            'cpc', 'ppc', 'cpm', 'cpv', 'cpa', 'paid', 'display', 'banner',
            'Paid Search', 'paid search', 'paid_search',
            'Paid Pmax', 'P Max', 'P+Max', 'pmax',
            'Social Ads', 'social ads', 'Paid Social', 'paid social', 'early bird ads'
        )
        AND utm_source != ''
    );
"""


# ---------------------------------------------------------
# HELPER: DATABASE CONNECTION
# ---------------------------------------------------------
def get_db_engine():
    """Create SQLAlchemy Engine for SiteGround MySQL."""
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        raise ValueError("Missing SiteGround DB credentials in environment variables.")

    # SAFEGUARD: Encode the password
    encoded_password = quote_plus(DB_PASSWORD)

    # Requires 'mysql-connector-python' in requirements.txt
    connection_string = f'mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}'
    return create_engine(connection_string)


def get_bq_client():
    """Initialize BigQuery Client."""
    # Auth is handled via GOOGLE_APPLICATION_CREDENTIALS env var set in Docker
    return bigquery.Client()


# ---------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------
def extract_crm_data(engine):
    """Executes SQL query against CRM and returns Pandas DataFrame."""
    print(f"Connecting to CRM Database: {DB_HOST}...")

    connection = None
    try:
        # FIX: Bypass SQLAlchemy Wrapper. Get the raw DBAPI connection.
        # This resolves the 'Engine object has no attribute cursor' error.
        connection = engine.raw_connection()

        df = pd.read_sql(CRM_QUERY, connection)

        print(f"Extracted {len(df)} rows matching Paid Media criteria.")
        return df

    except Exception as e:
        print(f"Database Extraction Failed: {e}")
        raise

    finally:
        # Clean up the raw connection explicitly
        if connection:
            connection.close()


# ---------------------------------------------------------
# LOAD (FULL REFRESH)
# ---------------------------------------------------------
def load_to_bigquery(df: pd.DataFrame, bq_client):
    """
    Loads DataFrame to BigQuery using WRITE_TRUNCATE (Full Refresh).
    Ensures Schema matching and Partitioning.
    """
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{CRM_CONVERSIONS_TABLE_NAME}"

    # 1. PRE-PROCESSING
    if not df.empty:
        # Convert date_submit to proper Date object for Partitioning
        df['date_submit'] = pd.to_datetime(df['date_submit'], errors = 'coerce').dt.date

        # Convert Timestamp
        df['date_time_submit'] = pd.to_datetime(df['date_time_submit'], errors = 'coerce')

        # Force id_commande to STRING to handle "47064zg2"
        if 'id_commande' in df.columns:
            df['id_commande'] = df['id_commande'].astype('str')

        # Add Ingestion Time
        df['_ingested_at'] = datetime.now(timezone.utc)

        # Handle NaNs for String columns (BQ doesn't like NaN in String fields sometimes)
        str_cols = [
            'conference_series',
            'conference_editions',
            'cut_off_rate',
            'currency',
            'order_type',
            'city',
            'country',
            'region',
            'primary_language',
            'specialty',
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'orders',
            'new_customer'
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna("Unknown")

    # 2. CONFIGURATION
    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_TRUNCATE",  # REPLACES TABLE CONTENT DAILY

        schema = [
            bigquery.SchemaField("id_commande", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("date_submit", "DATE"),  # Partition Key
            bigquery.SchemaField("date_time_submit", "TIMESTAMP"),
            bigquery.SchemaField("time_submit", "STRING"),
            bigquery.SchemaField("time_period", "STRING"),
            bigquery.SchemaField("time_submit_converted", "STRING"),
            bigquery.SchemaField("time_period_converted", "STRING"),
            bigquery.SchemaField("conference_series", "STRING"),
            bigquery.SchemaField("conference_editions", "STRING"),
            bigquery.SchemaField("cut_off_rate", "STRING"),
            bigquery.SchemaField("total_ht", "FLOAT"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("order_type", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("primary_language", "STRING"),
            bigquery.SchemaField("specialty", "STRING"),
            bigquery.SchemaField("utm_source", "STRING"),
            bigquery.SchemaField("utm_medium", "STRING"),
            bigquery.SchemaField("utm_campaign", "STRING"),
            bigquery.SchemaField("orders", "STRING"),
            bigquery.SchemaField("new_customer", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ],

        # Optimizes performance for Date filtering (Cost Saver)
        time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "date_submit",
        ),

        # Optimizes Join performance with Ads Data
        clustering_fields = [
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "conference_series"
        ],
    )

    # 3. EXECUTE LOAD
    print(f"Starting BigQuery Load: {table_id}")
    try:
        job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
        job.result()  # Wait for completion

        # Get Table Stats
        table = bq_client.get_table(table_id)
        print(f"Success! Loaded {table.num_rows} rows into {table_id}.")

    except Exception as e:
        print(f"BigQuery Load Failed: {e}")
        raise


# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
def main():
    print("------------------------------------------------")
    print("   STARTING CRM CONVERSIONS EXTRACTION")
    print("------------------------------------------------")

    # 1. Initialize Clients
    try:
        engine = get_db_engine()
        bq_client = get_bq_client()
    except Exception as e:
        print(f"Initialization Failed: {e}")
        return

    # 2. Extract
    try:
        df = extract_crm_data(engine)
    except Exception:
        # If extraction fails, we stop here (Airflow marks failed)
        raise

    # 3. Load
    if not df.empty:
        load_to_bigquery(df, bq_client)
    else:
        print("No data found in CRM. Skipping BigQuery load.")

    print("------------------------------------------------")
    print("   JOB COMPLETED SUCCESSFULLY")
    print("------------------------------------------------")


if __name__ == "__main__":
    main()