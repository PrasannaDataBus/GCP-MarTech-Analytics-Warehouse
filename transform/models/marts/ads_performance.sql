{{ config(
    materialized='table',
    tags=['gold', 'daily'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_name", "campaign_name"]
) }}

WITH google_ads AS (
    SELECT
        id as unique_id,
        'Google Ads' as platform,
        date,
        year,
        month,
        day,
        week_number,
        current_week_number,

        -- Dimensions
        event_name,
        account_id,
        campaign_name,
        campaign_status,
        ad_group_name,
        ad_name,

        -- Metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc,

        -- GOOGLE SPECIFIC METRICS
        view_through_conversions,
        all_conversions,
        engagements,
        bidding_strategy_type,
        ad_network_type,

        -- META SPECIFIC METRICS (Fill with NULLs)
        CAST(NULL AS FLOAT64) as cpm,
        CAST(NULL AS INT64) as reach,
        CAST(NULL AS FLOAT64) as frequency,

        -- Extra Context
        currency,
        device as device_type

    FROM {{ ref('stg_google_ads_performance') }}
),

meta_ads AS (
    SELECT
        id as unique_id,
        'Meta Ads' as platform,
        date,
        year,
        month,
        day,
        week_number,
        current_week_number,

        -- Dimensions
        event_name,
        account_id,
        campaign_name,
        -- Explicitly cast NULL to STRING to match Google
        CAST(NULL AS STRING) as campaign_status,
        ad_group_name,
        ad_name,

        -- Metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc,

        -- GOOGLE SPECIFIC METRICS (Fill with NULLs)
        CAST(NULL AS FLOAT64) as view_through_conversions,
        CAST(NULL AS FLOAT64) as all_conversions,
        CAST(NULL AS INT64) as engagements,
        CAST(NULL AS STRING) as bidding_strategy_type,
        CAST(NULL AS STRING) as ad_network_type,

        -- META SPECIFIC METRICS
        cpm,
        reach,
        frequency,

        -- Extra Context
        currency,
        -- Explicitly cast NULL to STRING
        CAST(NULL AS STRING) as device_type

    FROM {{ ref('stg_meta_ads_performance') }}
)

-- UNION ALL stacks them on top of each other
SELECT * FROM google_ads
UNION ALL
SELECT * FROM meta_ads