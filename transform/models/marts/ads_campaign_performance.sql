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

WITH google_campaigns AS (
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
        account_name,        -- Added: Missing in previous version
        campaign_id,
        campaign_name,
        campaign_status,     -- Added: Critical for filtering

        -- Context
        channel_type,        -- e.g. 'PERFORMANCE_MAX'
        bidding_strategy_type, -- Added: Useful for optimization analysis

        -- Metrics
        cost,
        impressions,
        clicks,

        -- Calculated Metrics (Optional, but good for QA)
        ctr,
        SAFE_DIVIDE(cost, clicks) as average_cpc,

        currency

    FROM {{ ref('stg_google_ads_campaign_performance') }}
),

meta_campaigns AS (
    -- Meta is originally at Ad Level, so we GROUP BY Campaign
    SELECT
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING))) as unique_id,
        'Meta Ads' as platform,
        date,
        MAX(year) as year,
        MAX(month) as month,
        MAX(day) as day,
        MAX(week_number) as week_number,
        MAX(current_week_number) as current_week_number,

        -- Dimensions
        MAX(event_name) as event_name,
        MAX(account_id) as account_id,
        MAX(account_name) as account_name, -- Added
        campaign_id,
        MAX(campaign_name) as campaign_name,

        -- Meta Staging (Ad Level) usually doesn't have Campaign Status.
        -- We pass NULL or 'Unknown' to match the schema.
        CAST(NULL AS STRING) as campaign_status,

        -- Context
        'Social' as channel_type,
        CAST(NULL AS STRING) as bidding_strategy_type, -- Meta doesn't have this in performance report

        -- Metrics (SUM them up)
        SUM(cost) as cost,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,

        -- Re-Calculate Rates for the Aggregated Row
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as ctr,
        SAFE_DIVIDE(SUM(cost), SUM(clicks)) as average_cpc,

        MAX(currency) as currency

    FROM {{ ref('stg_meta_ads_performance') }}
    GROUP BY
        date, campaign_id
)

-- UNION BOTH
SELECT * FROM google_campaigns
UNION ALL
SELECT * FROM meta_campaigns