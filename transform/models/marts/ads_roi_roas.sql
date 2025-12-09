{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "campaign_name", "event_name"],
    tags=['gold', 'daily', 'roi', 'roas']
) }}

WITH ads_data AS (
    SELECT
        -- 1. Shared Dimensions
        date,
        year,
        platform,
        LOWER(TRIM(campaign_name)) as campaign_name,
        currency, -- Ads have currency too

        -- MAPPING: Use 'event_name' as the master Event column
        event_name,

        -- 2. CRM Dimensions (Ads don't have these -> Set to NULL)
        CAST(NULL AS TIMESTAMP) as conversion_timestamp,
        CAST(NULL AS STRING) as conference_series,
        CAST(NULL AS STRING) as conference_editions,
        CAST(NULL AS STRING) as city,
        CAST(NULL AS STRING) as country,
        CAST(NULL AS STRING) as region,
        CAST(NULL AS STRING) as orders,
        CAST(NULL AS STRING) as new_customer, -- Raw text (Acquisition/Retention)
        CAST(NULL AS STRING) as cut_off_rate,
        CAST(NULL AS STRING) as order_type,
        CAST(NULL AS STRING) as primary_language,
        CAST(NULL AS STRING) as specialty,
        CAST(NULL AS STRING) as utm_source,
        CAST(NULL AS STRING) as utm_medium,

        -- 3. Ad Metrics (Real Data)
        cost,
        impressions,
        clicks,

        -- 4. Sales Metrics (Set to 0)
        0.0 as revenue,
        0 as total_sales,
        0 as new_customers_count

    FROM {{ ref('ads_performance') }}
),

crm_data AS (
    SELECT
        -- 1. Shared Dimensions
        date,
        year,
        platform,
        campaign_name, -- Already lowered in Silver
        currency,

        -- MAPPING LOGIC: Align CRM 'conference_series' to Ads 'event_name'
        -- Adjust these 'WHEN' clauses to match your actual CRM values exactly
        CASE
            WHEN conference_series LIKE '%Americas%' THEN 'AMWC Americas'
            WHEN conference_series LIKE '%Asia%' OR conference_series LIKE '%TDAC%' THEN 'AMWC Asia-TDAC'
            WHEN conference_series LIKE '%Dubai%' OR conference_series LIKE '%Middle East%' THEN 'AMWC Dubai'
            WHEN conference_series LIKE '%SEA%' OR conference_series LIKE '%ICAD%' THEN 'AMWC SEA - ICAD'
            WHEN conference_series LIKE '%Latam%' OR conference_series LIKE '%Medellin%' THEN 'AMWC LATAM'
            WHEN conference_series LIKE '%FACE%' THEN 'FACE Conference'
            WHEN conference_series LIKE '%TAS%' OR conference_series LIKE '%Aesthetic Show%' THEN 'TAS'
            WHEN conference_series LIKE '%VCS%' OR conference_series LIKE '%Vegas%' THEN 'VCS'
            WHEN conference_series LIKE '%Eurogin%' THEN 'EUROGIN'
            WHEN conference_series LIKE '%AMS%' THEN 'AMS'
            WHEN conference_series LIKE '%Monaco%' OR conference_series = 'AMWC' THEN 'AMWC Conference'
            WHEN conference_series LIKE '%IM%' THEN 'IM AESTHETICS'
            -- Fallback: Use the raw value if no match found
            ELSE COALESCE(conference_series, 'Unmapped Event')
        END as event_name,

        -- 2. CRM Dimensions (Real Data)
        conversion_timestamp,
        conference_series, -- Keep raw column
        conference_editions,
        city,
        country,
        region,
        orders,
        new_customer, -- Keep the raw value for filtering
        cut_off_rate,
        order_type,
        primary_language,
        specialty,
        utm_source,
        utm_medium,

        -- 3. Ad Metrics (Set to 0)
        0.0 as cost,
        0 as impressions,
        0 as clicks,

        -- 4. Sales Metrics (Real Data)
        revenue,
        1 as total_sales,

        -- Calculated Metric: Count Acquisitions
        CASE
            WHEN LOWER(TRIM(new_customer)) IN ('acquisition', 'new', 'yes', 'true', '1') THEN 1
            ELSE 0
        END as new_customers_count

    FROM {{ ref('stg_crm_conversions') }}
)

-- STACK THEM TOGETHER
SELECT * FROM ads_data
UNION ALL
SELECT * FROM crm_data