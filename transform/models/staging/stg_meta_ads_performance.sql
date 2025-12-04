{{ config(
    materialized='view',
    tags=['silver', 'meta', 'daily']
) }}

-- The Auto-Update Magic happens above because materialized='view'

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'meta_ads_performance_raw') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(ad_id AS STRING))) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,

        -- 3. Standardize Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,

        -- MAPPING: Meta "AdSet" = Google "AdGroup"
        CAST(adset_id AS STRING) as ad_group_id,
        adset_name as ad_group_name,

        CAST(ad_id AS STRING) as ad_id,
        ad_name,

        -- 4. Standardize Metrics
        -- Rename 'spend' to 'cost'. It is already a FLOAT (e.g. 10.50), so NO division needed.
        SAFE_CAST(spend AS FLOAT64) as cost,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,

        -- 5. Conversion Metrics
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversion_value AS FLOAT64) as conversion_value,

        -- 6. Extra Metrics
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        currency

    FROM source
)

SELECT * FROM renamed