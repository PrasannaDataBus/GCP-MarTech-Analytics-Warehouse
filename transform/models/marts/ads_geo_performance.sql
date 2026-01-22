{{ config(
    materialized='table',
    tags=['gold', 'daily', 'geo'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_name", "country_code"]
) }}

WITH country_ref AS (
    SELECT
        country_code,
        country_name
    FROM {{ source('marketing_raw', 'countries') }}
),

google_ads AS (
    SELECT
        id as unique_id,
        'Google Ads' as platform,
        date,

        -- Standard Date Parts (Re-calculated here for Gold)
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,

        -- EVENT CYCLE DIMENSIONS (New Logic from Staging)
        week as week_display,             -- e.g. "Week 10"
        week_number,                      -- e.g. 10 (Slicer Value)
        week_number_to_sort,              -- Sort Order
        weeks_left,                       -- Scorecard Value

        -- Dimensions
        event_name,
        event_edition,                    -- e.g. "AMWC 2025"

        account_id,
        account_name,
        campaign_id,
        campaign_name,

        -- Pricing / Rate Context
        cut_off_rate,                     -- e.g. "EB", "FP"
        cut_off_rate_sort_order,          -- e.g. 1, 2, 3

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,

        -- Metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc,
        currency

    FROM {{ ref('stg_google_ads_geo') }}
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

        -- EVENT CYCLE DIMENSIONS
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

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,

        -- Metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc,
        currency

    FROM {{ ref('stg_meta_ads_geo') }}
),

unioned_data AS (
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM meta_ads
)

SELECT
    u.*,

    -- MAPPING LOGIC:
    -- 1. Try to find the full name in your reference table.
    -- 2. If NULL (no match), fallback to the original 2-char code.
    COALESCE(c.country_name, u.country_code) as country_name

FROM unioned_data u
LEFT JOIN country_ref c
    ON u.country_code = c.country_code