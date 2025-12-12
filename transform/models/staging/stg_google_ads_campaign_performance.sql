{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_campaign_performance_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

renamed AS (
    SELECT
        -- 1. Unique ID (Campaign Level)
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING))) as id,

        -- 2. Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,
        EXTRACT(ISOWEEK FROM CAST(date AS DATE)) as week_number,
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- Event Name Logic (Same as before)
        CASE
            WHEN account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            ELSE account_name
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,

        -- NEW: This distinguishes PMax from Search
        advertising_channel_type as channel_type,

        -- 4. Metrics
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,

        -- 5. Context
        bidding_strategy_type,
        currency

    FROM source
)

SELECT * FROM renamed