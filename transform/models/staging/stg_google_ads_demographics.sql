{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily', 'demographics']
) }}

WITH age_source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_age_range_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

gender_source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_gender_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

-- 1. Process Age Data
google_age AS (
    SELECT
        -- Unique ID: Date + Campaign + AdGroup + 'age' + Range
        FARM_FINGERPRINT(CONCAT(
            CAST(date AS STRING),
            CAST(campaign_id AS STRING),
            CAST(ad_group_id AS STRING),
            'age',
            age_range
        )) as id,

        -- Standard Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,
        EXTRACT(ISOWEEK FROM CAST(date AS DATE)) as week_number,
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT MAPPING (Simple Google Logic)
        CASE
            WHEN account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            ELSE account_name
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,
        CAST(ad_group_id AS STRING) as ad_group_id,
        ad_group_name,

        -- DEMOGRAPHICS: Age is present, Gender is Unspecified
        age_range as age_group,
        'Unspecified' as gender,

        -- Financials & Metrics
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,

        -- Google Specific
        SAFE_CAST(view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        bidding_strategy_type,
        currency

    FROM age_source
),

-- 2. Process Gender Data
google_gender AS (
    SELECT
        -- Unique ID: Date + Campaign + AdGroup + 'gender' + GenderName
        FARM_FINGERPRINT(CONCAT(
            CAST(date AS STRING),
            CAST(campaign_id AS STRING),
            CAST(ad_group_id AS STRING),
            'gender',
            gender
        )) as id,

        -- Standard Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,
        EXTRACT(ISOWEEK FROM CAST(date AS DATE)) as week_number,
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT MAPPING (Simple Google Logic)
        CASE
            WHEN account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            ELSE account_name
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,
        CAST(ad_group_id AS STRING) as ad_group_id,
        ad_group_name,

        -- DEMOGRAPHICS: Age is Unspecified, Gender is present
        'Unspecified' as age_group,
        gender,

        -- Financials & Metrics
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,

        -- Google Specific
        SAFE_CAST(view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        bidding_strategy_type,
        currency

    FROM gender_source
)

-- 3. Combine Them
SELECT * FROM google_age
UNION ALL
SELECT * FROM google_gender