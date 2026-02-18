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
        -- Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- Event Cycle Dimensions (NEW)
        week as week_display,             -- e.g. "Week 10"
        week_number,                      -- e.g. 10 (Slicer Value)
        week_number_to_sort,              -- Sort order
        weeks_left,                       -- Card Value (Current status)

        -- Event Context
        event_name,
        event_edition,                    -- NEW: e.g. "AMWC 2025"

        account_id,
        account_name,
        campaign_id,
        campaign_name,
        campaign_status,

        -- Pricing / Rate Context (NEW)
        cut_off_rate,                     -- e.g. "EB", "FP"
        cut_off_rate_sort_order,          -- e.g. 1, 2, 3

        -- Channel Context
        channel_type,
        bidding_strategy_type,

        -- Metrics
        cost,
        impressions,
        clicks,
        unique_clicks,

        -- Calculated Metrics
        ctr,
        unique_ctr,
        SAFE_DIVIDE(cost, clicks) as average_cpc,

        currency

    FROM {{ ref('stg_google_ads_campaign_performance') }}
),

meta_campaigns AS (
    -- Meta is originally at Ad Level, so we GROUP BY Campaign
    SELECT
        -- Create a unique ID for the Campaign-Day grain
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING))) as unique_id,
        'Meta Ads' as platform,
        date,

        -- Date Parts (Uniform across the group)
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- Event Cycle Dimensions (Aggregated using MAX since they are identical per day)
        MAX(week) as week_display,
        MAX(week_number) as week_number,
        MAX(week_number_to_sort) as week_number_to_sort,
        MAX(weeks_left) as weeks_left,

        -- Event Context
        MAX(event_name) as event_name,
        MAX(event_edition) as event_edition,

        MAX(account_id) as account_id,
        MAX(account_name) as account_name,
        campaign_id,
        MAX(campaign_name) as campaign_name,
        MAX(campaign_status) as campaign_status,

        -- Pricing / Rate Context
        MAX(cut_off_rate) as cut_off_rate,
        MAX(cut_off_rate_sort_order) as cut_off_rate_sort_order,

        -- Channel Context
        'Social' as channel_type,
        CAST(NULL AS STRING) as bidding_strategy_type,

        -- Metrics (Summed from Ad Level)
        SUM(cost) as cost,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(unique_clicks) as unique_clicks,

        -- Re-Calculate Rates for the Aggregated Row
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as ctr,
        SAFE_DIVIDE(SUM(unique_clicks), SUM(impressions)) as unique_ctr,
        SAFE_DIVIDE(SUM(cost), SUM(clicks)) as average_cpc,

        MAX(currency) as currency

    FROM {{ ref('stg_meta_ads_performance') }}
    GROUP BY
        date, campaign_id
)

-- UNION BOTH PLATFORMS
SELECT * FROM google_campaigns
UNION ALL
SELECT * FROM meta_campaigns