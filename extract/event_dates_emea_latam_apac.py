# Process: Extract event dates & rates data and Ingest into BigQuery
# Data Source: MySQL Database / CRM DB
# Destination: BigQuery (marketing_raw.crm_conversions_raw)
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Strategy: Full Refresh (WRITE_TRUNCATE) - Daily Snapshot
# Partitioning: By 'event_start_date' (Day)
# Clustering: By Conference Series, Year

import os
import sys
import re
import pandas as pd
import pycountry
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
EMEA_LATAM_APAC_EVENTS_DATES_TABLE_NAME = os.getenv("EMEA_LATAM_APAC_EVENTS_DATES_TABLE_NAME")

# Database Credentials
DB_USER = os.getenv("DB_USER_EMCDB")
DB_PASSWORD = os.getenv("DB_PASSWORD_EMCDB")
DB_HOST = os.getenv("DB_HOST_EMCDB")
DB_NAME = os.getenv("DB_NAME_EMCDB")

# --- SQL QUERIES ---
MANIFESTATION_QUERY = """
SELECT
 id_manifestation,
 date_begin as event_start_date,
 date_end as event_end_date,
 original_title,
 country as host_nation
FROM manifestation;
"""

RATES_QUERY = """
SELECT
 id_manifestation,
 date1, 
 date2 
FROM dates_congress;
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
def extract_manifestation_data(engine):
    print(f"Connecting to CRM Database: {DB_HOST}...")
    connection = None
    try:
        connection = engine.raw_connection()
        df = pd.read_sql(MANIFESTATION_QUERY, connection)
        print(f"Extracted {len(df)} rows from manifestation.")
        return df
    except Exception as e:
        print(f"Manifestation Extraction Failed: {e}")
        raise
    finally:
        if connection: connection.close()


def extract_rates_data(engine):
    print(f"Connecting to CRM Database for Rates...")
    connection = None
    try:
        connection = engine.raw_connection()
        df = pd.read_sql(RATES_QUERY, connection)
        print(f"Extracted {len(df)} rows from dates_congress.")
        return df
    except Exception as e:
        print(f"Rates Extraction Failed: {e}")
        raise
    finally:
        if connection: connection.close()


# ---------------------------------------------------------
# TRANSFORMATIONS:
# ---------------------------------------------------------

def run_transformations(df_manifestation, df_rateperiod):
    print("--- Starting Transformations ---")

    # 1. Manifestation Cleaning
    df_manifestation['original_title'] = df_manifestation['original_title'] \
        .str.replace('AMWC - LATIN AMERICA', 'AMWC LATAM', regex = False) \
        .str.replace('AMWC LATIN AMERICA', 'AMWC LATAM', regex = False) \
        .str.replace('AMWC SOUTHEAST ASIA', 'AMWC SEA', regex = False) \
        .str.replace('ICAD', 'AMWC SEA', regex = False)

    event_patterns = ['AMWC LATAM', 'AMWC SEA', 'AMWC ASIA', 'AMWC DUBAI', 'ICAD', 'FACE', 'TAS UK', 'EUROGIN', 'AMWC']
    regex_pattern = '|'.join(re.escape(pat) for pat in event_patterns)

    df_manifestation = df_manifestation[df_manifestation['original_title'].str.contains(regex_pattern, regex = True)]

    df_manifestation['year'] = pd.to_numeric(df_manifestation['original_title'].str.extract(r'(\d{4})')[0],
                                             errors = 'coerce').astype('Int64')

    df_manifestation['conference_series'] = df_manifestation['original_title'].str.replace(r' \d{4}.*', '',
                                                                                           regex = True)
    df_manifestation['conference_series'] = df_manifestation['conference_series'].replace('ICAD', 'AMWC SOUTHEAST ASIA')

    allowed_amwc = ['AMWC LATAM', 'AMWC SEA', 'AMWC ASIA', 'AMWC ASIA VIRTUAL', 'AMWC DUBAI', 'AMWC']
    df_manifestation = df_manifestation[~((df_manifestation['conference_series'].str.startswith('AMWC')) & (
        ~df_manifestation['conference_series'].isin(allowed_amwc)))]

    # Eurogin Logic
    eurogin_ids = [225, 266, 282, 306, 336]
    df_eurogin = df_manifestation[df_manifestation['original_title'].str.contains('EUROGIN')]
    df_eurogin_old = df_eurogin[(df_eurogin['year'] >= 2017) & (df_eurogin['year'] <= 2022) & (
        df_eurogin['id_manifestation'].isin(eurogin_ids))]
    df_eurogin_new = df_eurogin[df_eurogin['year'] >= 2023]
    df_eurogin_final = pd.concat([df_eurogin_old, df_eurogin_new])

    df_manifestation = pd.concat(
        [df_manifestation[~df_manifestation['original_title'].str.contains('EUROGIN')], df_eurogin_final])
    df_manifestation = df_manifestation[df_manifestation['year'] >= 2022]

    exclusions = ['ICAD BRAZIL', 'AMWC ASIA POST CONGRESS COURSE', 'FACE ASEAN', 'FACE VIRTUAL',
        'AMWC - LATIN AMERICA VIRTUAL', 'PRECEDING AND AS PART OF AMWC']
    df_manifestation = df_manifestation[~df_manifestation['conference_series'].isin(exclusions)]
    df_manifestation = df_manifestation[
        ~((df_manifestation['conference_series'] == 'AMWC ASIA VIRTUAL') & (df_manifestation['year'] != 2021))]

    # Validations
    valid_conferences = ['AMWC LATAM', 'AMWC SEA', 'AMWC ASIA', 'AMWC DUBAI', 'ICAD', 'FACE', 'TAS UK', 'EUROGIN',
        'AMWC']
    non_matching = df_manifestation[~df_manifestation['conference_series'].isin(valid_conferences)]['conference_series']
    if not non_matching.empty:
        print(colored("WARNING: Non-matching conference series found.", 'yellow'))
        # exit() # Commented out for safety in dev

    df_manifestation['conference_editions'] = df_manifestation['year'].astype(str) + ' ' + df_manifestation[
        'conference_series']

    def get_alpha_2(country_name):
        try:
            return pycountry.countries.get(name = country_name).alpha_2
        except:
            return ''

    # Ensure host_nation is handled for Eurogin
    df_manifestation.loc[df_manifestation['conference_series'] == 'EUROGIN', 'conference_editions'] += ' ' + \
                                                                                                       df_manifestation[
                                                                                                           'host_nation'].apply(
                                                                                                           get_alpha_2)

    df_manifestation = df_manifestation[
        ['id_manifestation', 'event_start_date', 'event_end_date', 'year', 'conference_series', 'conference_editions']]

    # 2. Rate Calculation
    df_rateperiod['date1'] = pd.to_datetime(df_rateperiod['date1'], errors = 'raise')
    df_rateperiod['date2'] = pd.to_datetime(df_rateperiod['date2'], errors = 'raise')
    df_manifestation['event_start_date'] = pd.to_datetime(df_manifestation['event_start_date'], errors = 'raise')
    df_manifestation['event_end_date'] = pd.to_datetime(df_manifestation['event_end_date'], errors = 'raise')

    df_rateperiod['seb_start_date'] = df_rateperiod.groupby('id_manifestation')['date1'].transform('min')
    df_rateperiod['seb_end_date'] = df_rateperiod.groupby('id_manifestation')['date2'].transform('min')
    df_rateperiod['eb_start_date'] = df_rateperiod.groupby('id_manifestation')['date1'].transform('max')
    df_rateperiod['eb_end_date'] = df_rateperiod.groupby('id_manifestation')['date2'].transform('max')

    # Merge
    df_ratescombined = df_rateperiod.merge(df_manifestation, on = 'id_manifestation', how = 'left')
    df_ratescombined = df_ratescombined[df_ratescombined['id_manifestation'].isin(df_manifestation['id_manifestation'])]

    # Logic for FP and Onsite
    df_ratescombined['fp_start_date'] = df_ratescombined['eb_end_date'] + pd.Timedelta(days = 1)
    df_ratescombined['fp_end_date'] = df_ratescombined['event_start_date'] - pd.Timedelta(days = 1)
    df_ratescombined['onsite_start_date'] = df_ratescombined['event_start_date']
    df_ratescombined['onsite_end_date'] = df_ratescombined['event_end_date']

    # Validation
    na_counts = df_ratescombined[[
        'seb_start_date',
        'seb_end_date',
        'eb_start_date',
        'eb_end_date',
        'fp_end_date',
        'onsite_start_date',
        'onsite_end_date'
    ]].isna().sum()
    if na_counts.sum() > 0:
        print(colored("WARNING: NaT values found (some events may rely on default logic).", 'yellow'))

    df_final = df_ratescombined.drop(columns = ['date1', 'date2'])

    # Added comma in drop_duplicates list
    df_final = df_final.drop_duplicates(subset = [
        'id_manifestation',
        'year',
        'event_start_date',
        'event_end_date',
        'conference_series',
        'conference_editions',
        'seb_start_date',
        'seb_end_date',
        'eb_start_date',
        'eb_end_date',
        'fp_start_date',
        'fp_end_date',
        'onsite_start_date',
        'onsite_end_date'
    ], keep = 'first')

    return df_final


# ---------------------------------------------------------
# LOAD (FULL REFRESH)
# ---------------------------------------------------------
def load_to_bigquery(df: pd.DataFrame, bq_client):
    """
    Loads Dates/Rates DataFrame to BigQuery.
    """
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{EMEA_LATAM_APAC_EVENTS_DATES_TABLE_NAME}"

    if df.empty:
        print("DataFrame is empty. Skipping load.")
        return

    # 1. Pre-Processing for BigQuery
    df['_ingested_at'] = datetime.now(timezone.utc)

    # Ensure Date columns are actually Dates (not Timestamps) for BQ DATE fields
    date_cols = [
        'event_start_date',
        'event_end_date',
        'seb_start_date',
        'seb_end_date',
        'eb_start_date',
        'eb_end_date',
        'fp_start_date',
        'fp_end_date',
        'onsite_start_date',
        'onsite_end_date'
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

    # 2. Config
    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_TRUNCATE",

        schema = [
            bigquery.SchemaField("id_manifestation", "INTEGER"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("conference_series", "STRING"),
            bigquery.SchemaField("conference_editions", "STRING"),

            # Critical Date Fields
            bigquery.SchemaField("event_start_date", "DATE"),
            bigquery.SchemaField("event_end_date", "DATE"),

            bigquery.SchemaField("seb_start_date", "DATE"),
            bigquery.SchemaField("seb_end_date", "DATE"),
            bigquery.SchemaField("eb_start_date", "DATE"),
            bigquery.SchemaField("eb_end_date", "DATE"),
            bigquery.SchemaField("fp_start_date", "DATE"),
            bigquery.SchemaField("fp_end_date", "DATE"),
            bigquery.SchemaField("onsite_start_date", "DATE"),
            bigquery.SchemaField("onsite_end_date", "DATE"),

            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ],

        # Partition by Event Start Date to optimize queries looking for specific years/events
        time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "event_start_date",
        ),

        # Cluster by Series and Year for faster filtering
        clustering_fields = [
            "conference_series",
            "year"
        ],
    )

    # 3. Execute
    print(f"Starting BigQuery Load: {table_id}")
    try:
        job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
        job.result()
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
    print("   STARTING EVENT DATES EXTRACTION")
    print("------------------------------------------------")

    try:
        engine = get_db_engine()
        bq_client = get_bq_client()

        # 1. Extract both tables
        df_man = extract_manifestation_data(engine)
        df_rates = extract_rates_data(engine)

        # 2. Transform & Merge
        df_final = run_transformations(df_man, df_rates)

        # 3. Load
        load_to_bigquery(df_final, bq_client)

    except Exception as e:
        print(f"Job Failed: {e}")
        raise

    print("------------------------------------------------")
    print("   JOB COMPLETED SUCCESSFULLY")
    print("------------------------------------------------")


if __name__ == "__main__":
    main()