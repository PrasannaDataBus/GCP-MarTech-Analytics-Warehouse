# Test 1: BigQuery connection test

# from google.cloud import bigquery
#
# client = bigquery.Client()
# print("Connected to project:", client.project)

#Test 2: Oauth test

# from google.ads.googleads.client import GoogleAdsClient
#
# # Load the configuration file
# client = GoogleAdsClient.load_from_storage("key/google-ads.yaml")
#
#
# def main():
#     ga_service = client.get_service("GoogleAdsService")
#
#     query = """
#         SELECT
#             customer.id,
#             customer.descriptive_name,
#             campaign.id,
#             campaign.name,
#             campaign.status
#         FROM campaign
#         ORDER BY campaign.id
#         LIMIT 10
#     """
#
#     # Get your client ID (the Ads account you want to query)
#     customer_id = client.login_customer_id or client.client_customer_id
#
#     # Execute query
#     response = ga_service.search(customer_id=customer_id, query=query)
#
#     print("\nConnection successful! Sample campaigns:\n")
#     for row in response:
#         print(f"- {row.campaign.name} (Status: {row.campaign.status.name})")
#
#
# if __name__ == "__main__":
#     main()

# Test 3: META ADS connection test

import os
import sys
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount

# --- 1. Load Environment Variables ---
# Try loading from params.env (your project structure) or .env
env_path = "params.env" if os.path.exists("params.env") else ".env"
if os.path.exists(env_path):
    load_dotenv(env_path, override = True)
    print(f"Loaded configuration from {env_path}")
else:
    print("Error: No .env or params.env file found.")
    sys.exit(1)

# --- 2. Get Credentials ---
my_app_id = os.getenv("META_APP_ID")
my_app_secret = os.getenv("META_APP_SECRET")
my_access_token = os.getenv("META_ACCESS_TOKEN")

if not all([my_app_id, my_app_secret, my_access_token]):
    print("Error: Missing Meta credentials in environment file.")
    print("Ensure META_APP_ID, META_APP_SECRET, and META_ACCESS_TOKEN are set.")
    sys.exit(1)


def main():
    print("\n--- Starting Meta API Connection Test ---")

    # --- 3. Initialize API ---
    try:
        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
        print("API Initialized successfully.")
    except Exception as e:
        print(f"API Initialization Failed: {e}")
        return

    # --- 4. Verify User (The System User) ---
    try:
        # 'me' refers to the System User linked to the token
        me = User(fbid = 'me').api_get(fields = ['name', 'id'])
        print(f"Connected as System User: {me['name']} (ID: {me['id']})")
    except Exception as e:
        print(f"Failed to fetch System User. Check your Access Token. Error: {e}")
        return

    # --- 5. List Ad Accounts ---
    print("\n--- Fetching Ad Accounts ---")
    try:
        # Fetch accounts connected to this System User
        my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])

        if not my_accounts:
            print("No Ad Accounts found. Did you assign assets to the System User in Business Settings?")
            return

        print(f"Found {len(my_accounts)} Ad Accounts:\n")

        # --- 6. List Campaigns for the First Account ---
        # We take the first account to test campaign permissions
        first_account = my_accounts[0]
        print(f"Testing Account: {first_account['name']} ({first_account['account_id']})")

        # Fetch 5 campaigns
        campaigns = first_account.get_campaigns(
            fields = ['name', 'status', 'objective'],
            params = {'limit': 5}
        )

        if campaigns:
            print("Campaigns fetched successfully:")
            for campaign in campaigns:
                print(f"  - {campaign['name']} (Status: {campaign['status']})")
        else:
            print("Connection successful, but no campaigns found in this account.")

    except Exception as e:
        print(f"Error fetching data: {e}")

    print("\n--- Test Complete ---")


if __name__ == "__main__":
    main()


################################################ SCRIPT TESTING ########################################################

# SCRIPT NAME: meta_ads_creative_dim
# Note: I keep the below script for future references incase if there is any missing data possibilities from the working script

# Process: Extract Creative Visuals & Copy (Headlines, Images, URLs) and Load into BigQuery
# Data Points: Meta Creative Library -> object_story_spec / asset_feed_spec via Graph API
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Partitioning: None (Dimension Table)
# Clustering: Not necessary for small dimension tables
# Loading Strategy: Full Refresh (WRITE_TRUNCATE) - Runs Weekly/Monthly
# MarTech Dictionary: Refer SharePoint file - MarTech Data Dictionary

import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

# --- Meta SDK Imports ---
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookRequestError


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
META_CREATIVE_DIM_TABLE_NAME = os.getenv("META_CREATIVE_DIM_TABLE_NAME")

# --- FIELDS TO EXTRACT ---
FIELDS = [
    'name',
    'id',
    'status',
    'thumbnail_url',
    'image_url',
    'object_story_spec',
    'asset_feed_spec',
    'call_to_action_type',
    'url_tags'
]


# ---------------------------------------------------------
# HELPER: LAZY LOAD CLIENTS (BQ Only for Meta Script)
# ---------------------------------------------------------
def get_bq_client():
    """Safe BQ Client initialization"""
    return bigquery.Client()


# --- HELPER: Get Ad Accounts ---
def get_ad_accounts():
    """Fetches all Ad Accounts attached to the System User with Retry Logic."""
    me = User(fbid = 'me')

    try:
        my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])
    except FacebookRequestError as e:
        # If blocked at start-up, wait and try again
        if e.api_error_code() in [17, 80004]:
            print("! Rate limit hit immediately. Sleeping 5 minutes before retrying account fetch...")
            time.sleep(300)  # Wait 5 mins
            my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])
        else:
            raise e

    accounts_list = []
    for acc in my_accounts:
        accounts_list.append({
            'id': acc['id'],
            'name': acc.get('name', 'Unnamed Account'),
            'currency': acc.get('currency', 'USD')
        })
    return accounts_list


# HELPER: SMART PARSING LOGIC (The Core Value)
# ---------------------------------------------------------
def parse_creative_details(creative):
    """
    Intelligently extracts Headline, Body, and URL from complex Meta JSON.
    Handles Standard Ads, Carousels, and Dynamic Creative (DCO).
    """
    data = {
        'headline': None,
        'body': None,
        'destination_url': creative.get('url_tags'),
        'call_to_action_type': creative.get('call_to_action_type'),
        'image_url': creative.get('image_url') or creative.get('thumbnail_url')
    }

    # 1. Standard Ads / Carousels (object_story_spec)
    oss = creative.get('object_story_spec')
    if oss:
        # Link Data (Single Image/Video link)
        link_data = oss.get('link_data')
        if link_data:
            data['headline'] = link_data.get('name') or data['headline']
            data['body'] = link_data.get('message') or data['body']
            data['destination_url'] = link_data.get('link') or data['destination_url']
            if not data['call_to_action_type']:
                data['call_to_action_type'] = link_data.get('call_to_action', {}).get('type')
            if not data['image_url']:
                data['image_url'] = link_data.get('picture')

        # Video Data
        video_data = oss.get('video_data')
        if video_data:
            data['headline'] = video_data.get('title') or data['headline']
            data['body'] = video_data.get('message') or data['body']
            if not data['call_to_action_type']:
                data['call_to_action_type'] = video_data.get('call_to_action', {}).get('type')
            if not data['image_url']:
                data['image_url'] = video_data.get('image_url')

    # 2. Dynamic Creative Optimization - DCO (asset_feed_spec)
    # DCO returns lists. We take the first asset to give context.
    afs = creative.get('asset_feed_spec')
    if afs:
        titles = afs.get('titles')
        bodies = afs.get('bodies')
        urls = afs.get('link_urls')
        images = afs.get('images')

        if titles and len(titles) > 0:
            data['headline'] = titles[0].get('text')
        if bodies and len(bodies) > 0:
            data['body'] = bodies[0].get('text')
        if urls and len(urls) > 0:
            data['destination_url'] = urls[0].get('website_url')
        if images and len(images) > 0 and not data['image_url']:
            data['image_url'] = images[0].get('url')

    return data


# --- EXTRACTION FUNCTION ---
def extract_creative_dim(account_id: str, account_name: str):
    """
    Hybrid Extraction with Rate Limit Handling:
    1. Fetches 'Library' Creatives (Limit 50 + Sleeps to avoid rate limits).
    2. Fetches 'Active Ad' Creatives (Retry logic if Step 1 triggered a block).
    """
    print(f"Querying Creative Dimensions for {account_name} ({account_id})...")
    formatted_id = account_id if account_id.startswith('act_') else f"act_{account_id}"
    account = AdAccount(formatted_id)

    # STORAGE
    all_creatives_map = {}

    # ---------------------------------------------------------
    # STEP 1: The "Dump" (Fetch Public Library)
    # ---------------------------------------------------------
    print("-> Step 1: Fetching Public Creative Library...")

    try:
        # Use Limit 50 to prevent "Reduce amount of data" error
        library_creatives = account.get_ad_creatives(fields = FIELDS, params = {'limit': 50})

        count = 0
        for item in library_creatives:
            count += 1
            if count % 100 == 0:
                print(f"...fetched {count} library creatives")
                # THROTTLING: Sleep 1s every 100 items to keep API bucket healthy
                time.sleep(1)

            parsed = parse_creative_details(item)
            all_creatives_map[item['id']] = {
                'creative_id': item['id'],
                'creative_name': item.get('name'),
                'account_id': account_id.replace("act_", ""),
                'account_name': account_name,
                'status': item.get('status'),
                'headline': parsed['headline'],
                'body': parsed['body'],
                'destination_url': parsed['destination_url'],
                'call_to_action_type': parsed['call_to_action_type'],
                'image_url': parsed['image_url'],
                '_ingested_at': datetime.now()
            }

    except FacebookRequestError as e:
        print(f"! Warning: Step 1 interrupted at item {count}. Meta Error: {e.api_error_message()}")

        # COOLDOWN LOGIC: If Rate Limit hit (Code 17 or 80004), Force Sleep
        if e.api_error_code() in [17, 80004]:
            print("! RATE LIMIT HIT. Sleeping 120 seconds to refill quota before Step 2...")
            time.sleep(120)

    except Exception as e:
        print(f"! Warning: Step 1 interrupted. Moving to Step 2. Error: {e}")

    # ---------------------------------------------------------
    # STEP 2: The "Gap Fill" (Find Missing Active Creatives)
    # ---------------------------------------------------------
    print("-> Step 2: Checking Active Ads for 'Shadow' Creatives...")

    try:
        # WRAPPER: Retry active_ads fetch if it fails immediately (Rate Limit)
        try:
            active_ads = account.get_ads(fields = ['creative'], params = {'status': ['ACTIVE'], 'limit': 1000})
        except FacebookRequestError as e:
            if e.api_error_code() in [17, 80004]:
                print("! Step 2 blocked by Rate Limit. Sleeping 60s and retrying...")
                time.sleep(60)
                active_ads = account.get_ads(fields = ['creative'], params = {'status': ['ACTIVE'], 'limit': 1000})
            else:
                raise e  # Re-raise other errors

        needed_ids = set()
        for ad in active_ads:
            if 'creative' in ad and 'id' in ad['creative']:
                needed_ids.add(ad['creative']['id'])

        # Identify missing IDs
        found_ids = set(all_creatives_map.keys())
        missing_ids = list(needed_ids - found_ids)

        if missing_ids:
            print(f"Found {len(missing_ids)} active creatives missing from library. Fetching explicitly...")

            # Batch Fetch
            chunk_size = 50
            for i in range(0, len(missing_ids), chunk_size):
                chunk = missing_ids[i:i + chunk_size]

                try:
                    ids_str = ",".join(chunk)
                    fields_str = ",".join(FIELDS)

                    # RAW API CALL
                    response = FacebookAdsApi.get_default_api().call(
                        'GET',
                        path = '/',
                        params = {'ids': ids_str, 'fields': fields_str}
                    )

                    fetched_objects = response.json()

                    for key, item in fetched_objects.items():
                        parsed = parse_creative_details(item)
                        all_creatives_map[item['id']] = {
                            'creative_id': item['id'],
                            'creative_name': item.get('name', 'System Generated Creative'),
                            'account_id': account_id.replace("act_", ""),
                            'account_name': account_name,
                            'status': item.get('status', 'ACTIVE'),
                            'headline': parsed['headline'],
                            'body': parsed['body'],
                            'destination_url': parsed['destination_url'],
                            'call_to_action_type': parsed['call_to_action_type'],
                            'image_url': parsed['image_url'],
                            '_ingested_at': datetime.now()
                        }

                    # THROTTLE: Tiny sleep between Gap Fill chunks
                    time.sleep(0.5)

                except Exception as e:
                    print(f"Warning: Failed to fetch chunk {i}. Error: {e}")
        else:
            print("No missing active creatives found.")

    except Exception as e:
        print(f"! Error in Step 2: {e}")

    # ---------------------------------------------------------
    # RETURN
    # ---------------------------------------------------------
    print(f"Final Total: {len(all_creatives_map)} creatives.")
    df = pd.DataFrame(list(all_creatives_map.values()))
    return df


# --- LOAD FUNCTION (Full Refresh) ---
def load_to_bigquery(df: pd.DataFrame, bq_client):
    if META_CREATIVE_DIM_TABLE_NAME is None:
        raise ValueError("META_CREATIVE_DIM_TABLE_NAME is not set in .env")

    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{META_CREATIVE_DIM_TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_TRUNCATE",  # Wipes the table clean and reloads
        schema = [
            bigquery.SchemaField("creative_id", "STRING"),
            bigquery.SchemaField("creative_name", "STRING"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("headline", "STRING"),
            bigquery.SchemaField("body", "STRING"),
            bigquery.SchemaField("destination_url", "STRING"),
            bigquery.SchemaField("call_to_action_type", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    print(f"  -> Loaded {len(df)} rows into {table_id} (Mode: TRUNCATE)")


# --- MAIN EXECUTION ---
def main():
    # 1. Load Environment & Init BQ
    load_environment()
    bq_client = get_bq_client()  # BQ needed here

    # 2. READ CREDENTIALS (AFTER load_environment has run)
    META_APP_ID = os.getenv("META_APP_ID")
    META_APP_SECRET = os.getenv("META_APP_SECRET")
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

    # 3. Check Credentials availability after loading
    if not all([META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN]):
        print("Error: Missing Meta credentials (APP_ID, SECRET, TOKEN). Check params.env.")
        return

    # 4. INITIALIZE META API (This was missing/misplaced in your error)
    try:
        FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
        print("Meta API Initialized Successfully")
    except Exception as e:
        print(f"Failed to initialize Meta API: {e}")
        return

    print("--- Starting Meta Creative dimensions Extraction ---")

    # 5. Get Accounts
    try:
        accounts = get_ad_accounts()
        print(f"Found {len(accounts)} Ad Accounts.")
    except Exception as e:
        print(f"Error fetching ad accounts: {e}")
        return

    all_creatives_dfs = []
    failed_accounts = []

    # 6. Loop and Extract
    for acc in accounts:
        acc_id = acc['id']
        acc_name = acc['name']

        try:
            df = extract_creative_dim(acc_id, acc_name)
            if not df.empty:
                all_creatives_dfs.append(df)
                print(f"     {acc_name}: Found {len(df)} Creative dimensions.")
            else:
                print(f"     {acc_name}: No Creative dimensions found.")
        except Exception as e:
            error_msg = f"Failed for {acc_name} ({acc_id}): {e}"
            print(error_msg)
            failed_accounts.append(error_msg)

    # 7. Consolidate and Load
    if all_creatives_dfs:
        print("\nConsolidating all Creative dimensions from all accounts...")
        master_df = pd.concat(all_creatives_dfs, ignore_index = True)

        # Deduplicate based on creative_id
        master_df_deduped = master_df.drop_duplicates(subset = ['creative_id']).reset_index(drop = True)
        print(f"Total Unique Creative dimensions to Load: {len(master_df_deduped)}")

        try:
            load_to_bigquery(master_df_deduped, bq_client)
            print("--- Meta Creative dimensions Dimension Load Complete ---")
        except Exception as e:
            print(f"Failed to load to BigQuery: {e}")
    else:
        print("No Creative dimensions extracted from any account.")

    # --- FINAL FAILURE CHECK ---
    # If there were ANY failures during the loop, raise an exception now.
    if failed_accounts:
        print("\nCRITICAL: The following accounts failed extraction:")
        for err in failed_accounts:
            print(f" - {err}")

        # This ensures Airflow marks the task as FAILED so you get the email/alert
        raise Exception(f"Script completed with errors in {len(failed_accounts)} accounts.")


if __name__ == "__main__":
    main()

################################################ SCRIPT TESTING ########################################################
