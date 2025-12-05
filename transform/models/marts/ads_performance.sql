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

        -- Extra Context
        currency,
        device as device_type -- Exists in Google

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