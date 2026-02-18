{{ config(
    materialized='table',
    tags=['gold', 'daily', 'performance'],
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

        -- 1. Date Parts (Re-calculated here)
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- 2. New Event Logic
        week as week_display,             -- e.g. "Week 10"
        week_number,                      -- e.g. 10
        weeks_left,                       -- Scorecard Value (Replaces current_week_number)

        -- 3. Dimensions
        event_name,
        event_edition,                    -- New Column
        account_id,
        account_name,
        campaign_name,
        campaign_status,
        ad_group_id,
        ad_group_name,
        ad_id,
        ad_name,

        -- 4. Pricing / Rate Context (New)
        cut_off_rate,
        cut_off_rate_sort_order,

        -- 5. Metrics
        cost,
        impressions,
        clicks,
        unique_clicks,
        conversions,
        conversion_value,
        ctr,
        unique_ctr,
        average_cpc,

        -- 6. Google Specific Metrics
        view_through_conversions,
        all_conversions,
        engagements,
        bidding_strategy_type,
        ad_network_type,

        -- 7. Meta Specific Metrics (Fill with NULLs)
        CAST(NULL AS FLOAT64) as cpm,
        CAST(NULL AS INT64) as reach,
        CAST(NULL AS FLOAT64) as frequency,

        -- 8. Extra Context
        currency,
        device as device_type

    FROM {{ ref('stg_google_ads_performance') }}
),

meta_ads AS (
    SELECT
        id as unique_id,
        'Meta Ads' as platform,
        date,

        -- 1. Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- 2. New Event Logic
        week as week_display,
        week_number,
        weeks_left,

        -- 3. Dimensions
        event_name,
        event_edition,
        account_id,
        account_name,
        campaign_name,
        campaign_status,
        ad_group_id,
        ad_group_name,
        ad_id,
        ad_name,

        -- 4. Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- 5. Metrics
        cost,
        impressions,
        clicks,
        unique_clicks,
        conversions,
        conversion_value,
        ctr,
        unique_ctr,
        average_cpc,

        -- 6. Google Specific Metrics (Fill with NULLs)
        CAST(NULL AS FLOAT64) as view_through_conversions,
        CAST(NULL AS FLOAT64) as all_conversions,
        CAST(NULL AS INT64) as engagements,
        CAST(NULL AS STRING) as bidding_strategy_type,
        CAST(NULL AS STRING) as ad_network_type,

        -- 7. Meta Specific Metrics
        cpm,
        reach,
        frequency,

        -- 8. Extra Context
        currency,
        CAST(NULL AS STRING) as device_type -- Meta data usually doesn't provide device breakdown here

    FROM {{ ref('stg_meta_ads_performance') }}
)

-- UNION ALL stacks them on top of each other
SELECT * FROM google_ads
UNION ALL
SELECT * FROM meta_ads