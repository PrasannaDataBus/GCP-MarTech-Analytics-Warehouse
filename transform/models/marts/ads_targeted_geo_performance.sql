{{ config(
    materialized='table',
    tags=['gold', 'daily', 'targeted_geo'],
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
        campaign_status,

        -- Pricing / Rate Context
        cut_off_rate,                     -- e.g. "EB", "FP"
        cut_off_rate_sort_order,          -- e.g. 1, 2, 3

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,
        is_targeting_location,
        conversion_action_name,

        -- GOOGLE STRICT MAPPING LOGIC
        CASE
            WHEN conversion_action_name = 'NOTWORKING-Do NOT USE' THEN 'PURCHASE'
            WHEN conversion_action_name LIKE '%NOTWORKING%' THEN 'OTHER'
            WHEN conversion_action_name LIKE '%Do NOT USE%' THEN 'OTHER'
            WHEN conversion_action_name = 'Registration' THEN 'INITIATE_CHECKOUT'
            WHEN conversion_action_name LIKE '%Ticket%' THEN 'PURCHASE'
            WHEN conversion_action_name IN ('Registrations', 'Submit lead form', 'Newsletter') THEN 'LEAD'
            -- Add any other common strings here if needed, otherwise fallback to OTHER
            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- Metrics
        cost,
        impressions,
        clicks,
        unique_clicks,
        conversions,

        -- GOOGLE REVENUE SAFETY NET
        CASE
            WHEN conversion_action_name IN ('Registration', 'Registrations') THEN 0.0
            WHEN (
                conversion_action_name LIKE '%Ticket%' OR
                conversion_action_name = 'NOTWORKING-Do NOT USE'
            ) THEN conversion_value
            ELSE 0.0
        END as conversion_value,

        ctr,
        unique_ctr,
        average_cpc,
        currency

    FROM {{ ref('stg_google_ads_user_location') }}
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
        campaign_status,

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Geography specific
        country_code,
        region_name,
        canonical_name,
        geo_target_type,
        is_targeting_location,
        conversion_action_name,

        -- META STRICT MAPPING LOGIC
        CASE
            WHEN conversion_action_name = 'purchase' THEN 'PURCHASE'
            WHEN conversion_action_name = 'lead' THEN 'LEAD'
            WHEN conversion_action_name = 'complete_registration' THEN 'SIGNUP'
            WHEN conversion_action_name = 'add_to_cart' THEN 'ADD_TO_CART'
            -- We use LIKE for checkout/calls as there are no base versions in your raw data
            WHEN conversion_action_name LIKE '%initiate_checkout%' THEN 'INITIATE_CHECKOUT'
            WHEN conversion_action_name LIKE '%call_confirm%' THEN 'CONTACT'
            WHEN conversion_action_name LIKE '%call_connect%' THEN 'CONTACT'
            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- Metrics
        cost,
        impressions,
        clicks,
        unique_clicks,
        conversions,

        -- META REVENUE SAFETY NET
        CASE
            WHEN conversion_action_name = 'purchase' THEN conversion_value
            ELSE 0.0
        END as conversion_value,

        ctr,
        unique_ctr,
        average_cpc,
        currency

    FROM {{ ref('stg_meta_ads_geo_conversions') }}
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

-- THE SMART FILTER:
-- Google: Keeps all valid actions (matching the 60.41 in the UI), only drops explicitly 'JUNK' tags.
-- Meta: Hard-filters out all 'OTHER', 'PAGE_VIEW', and 'ENGAGEMENT' tags (preventing the 15,583 India spike).
WHERE
    (u.platform = 'Google Ads' AND u.standardized_conversion_type != 'OTHER')
    OR
    (u.platform = 'Meta Ads' AND u.standardized_conversion_type IN ('PURCHASE', 'LEAD', 'SIGNUP', 'INITIATE_CHECKOUT', 'ADD_TO_CART', 'CONTACT'))