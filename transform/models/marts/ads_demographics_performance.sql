{{ config(
    materialized='table',
    tags=['gold', 'daily', 'demographics'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["event_name", "platform"]
) }}

WITH google_ads AS (
    SELECT
        id as unique_id,
        'Google Ads' as platform,
        date,

        -- Standard Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- EVENT CYCLE DIMENSIONS (NEW)
        week as week_display,             -- e.g. "Week 10"
        week_number,                      -- e.g. 10 (Slicer Value)
        week_number_to_sort,              -- Sort Order
        weeks_left,                       -- Scorecard Value (e.g. "12")

        -- Dimensions
        event_name,
        event_edition,                    -- e.g. "AMWC 2025"

        account_id,
        account_name,
        campaign_id,
        campaign_name,

        -- Demographics
        age_group,
        gender,
        report_granularity,               -- 'Age Only', 'Gender Only', or 'Combined'

        -- Pricing / Rate Context (NEW)
        cut_off_rate,                     -- e.g. "EB", "FP"
        cut_off_rate_sort_order,          -- e.g. 1, 2, 3

        -- Metrics
        cost,
        average_cpc,
        impressions,
        clicks,
        ctr,
        conversions,
        conversion_value,
        currency

    FROM {{ ref('stg_google_ads_demographics') }}
),

meta_ads AS (
    SELECT
        id as unique_id,
        'Meta Ads' as platform,
        date,

        -- Standard Date Parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- EVENT CYCLE DIMENSIONS (NEW)
        week as week_display,
        week_number,
        week_number_to_sort,
        weeks_left,

        -- Dimensions
        event_name,
        event_edition,

        account_id,
        account_name,
        campaign_id,
        campaign_name,

        -- Demographics
        age_group,
        gender,
        report_granularity,

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Metrics
        cost,
        average_cpc,
        impressions,
        clicks,
        ctr,
        conversions,
        conversion_value,
        currency

    FROM {{ ref('stg_meta_ads_demographics') }}
)

SELECT * FROM google_ads
UNION ALL
SELECT * FROM meta_ads