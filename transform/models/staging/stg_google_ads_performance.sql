{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_performance_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(ad_id AS STRING))) as id,

        -- 2. Standardize Date & Time Components
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

        CAST(ad_group_id AS STRING) as ad_group_id,
        ad_group_name,

        CAST(ad_id AS STRING) as ad_id,
        ad_name,
        ad_type,

        -- 4. Financials
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,

        -- 5. Performance
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,

        -- 6. Google Specific
        SAFE_CAST(view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        SAFE_CAST(engagements AS INT64) as engagements,
        bidding_strategy_type, -- No cast needed, already string

        -- 7. Segments
        device,
        ad_network_type,
        currency

    FROM source
)

SELECT * FROM renamed

