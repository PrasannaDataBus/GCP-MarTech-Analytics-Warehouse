{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_user_location_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

geo_names AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_geo_dim') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        FARM_FINGERPRINT(CONCAT(
            CAST(s.date AS STRING),
            CAST(s.campaign_id AS STRING),
            CAST(s.user_geo_criterion_id AS STRING)
        )) as id,

        -- 2. Standardize Date & Time Components
        CAST(s.date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(s.date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(s.date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(s.date AS DATE)) as day,
        EXTRACT(ISOWEEK FROM CAST(s.date AS DATE)) as week_number,
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- 3. Dimensions
        CAST(s.account_id AS STRING) as account_id,
        s.account_name,

        CASE
            WHEN s.account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            ELSE s.account_name
        END as event_name,

        CAST(s.campaign_id AS STRING) as campaign_id,
        s.campaign_name,
        s.campaign_status,

        CAST(s.ad_group_id AS STRING) as ad_group_id,
        s.ad_group_name,

        -- 4. Geography Dimensions (New Columns Added Here)
        COALESCE(g.Name, 'Unknown') as region_name,
        COALESCE(g.`Canonical Name`, 'Unknown') as canonical_name,
        COALESCE(g.`Country Code`, 'Unknown') as country_code,
        COALESCE(g.`Target Type`, 'Unknown') as geo_target_type,

        -- 5. Financials
        (SAFE_CAST(s.cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(s.average_cpc AS FLOAT64) as average_cpc,

        -- 6. Performance
        SAFE_CAST(s.impressions AS INT64) as impressions,
        SAFE_CAST(s.clicks AS INT64) as clicks,
        SAFE_CAST(s.ctr AS FLOAT64) as ctr,
        SAFE_CAST(s.conversions AS FLOAT64) as conversions,
        SAFE_CAST(s.conversions_value AS FLOAT64) as conversion_value,

        -- 7. Google Specific
        SAFE_CAST(s.view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(s.all_conversions AS FLOAT64) as all_conversions,
        s.bidding_strategy_type,
        s.currency

    FROM source s
    LEFT JOIN geo_names g
        ON CAST(s.user_geo_criterion_id AS STRING) = CAST(g.`Criteria ID` AS STRING)
)

SELECT * FROM renamed