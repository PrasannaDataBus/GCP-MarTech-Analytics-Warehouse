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
        year,
        month,
        day,
        week_number,
        account_id,
        account_name,
        event_name,
        campaign_id,
        campaign_name,
        age_group,
        gender,
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
        year,
        month,
        day,
        week_number,
        account_id,
        account_name,
        event_name,
        campaign_id,
        campaign_name,
        age_group,
        gender,
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