{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_conversion_action_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        -- Note: Google Conversion Raw is at CAMPAIGN level, not Ad level.
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING), CAST(conversion_action_id AS STRING))) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,
        EXTRACT(ISOWEEK FROM CAST(date AS DATE)) as week_number,

        -- "Card" for Dashboard: Always shows today's week number (e.g., 49)
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,

        -- Google "Account Name" IS the Event Name, so we keep it as is.
        account_name,

        -- CLEANUP LOGIC: Create the 'event_name' column here
        -- This maps "Inactive - AMWC Asia" -> "AMWC Asia-TDAC"
        CASE
            WHEN account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            ELSE account_name
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,

        -- 4. Conversion Specific Dimensions
        CAST(conversion_action_id AS STRING) as conversion_action_id,
        conversion_action_name,
        conversion_category, -- CRITICAL: Use this to filter 'PURCHASE' vs 'LEAD'

        -- 5. Metrics
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        SAFE_CAST(all_conversions_value AS FLOAT64) as all_conversions_value,

        -- 6. Context
        bidding_strategy_type,
        currency

    FROM source
)

SELECT * FROM renamed