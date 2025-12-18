{{ config(
    materialized='table',
    tags=['gold', 'daily'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_name", "country_code"]
) }}

WITH unioned_data AS (
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
        campaign_id,
        campaign_name,

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,

        -- All metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc
    FROM {{ ref('stg_google_ads_geo') }}

    UNION ALL

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
        campaign_id,
        campaign_name,

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,

        -- All metrics
        cost,
        impressions,
        clicks,
        conversions,
        conversion_value,
        ctr,
        average_cpc
    FROM {{ ref('stg_meta_ads_geo') }}
)

SELECT * FROM unioned_data