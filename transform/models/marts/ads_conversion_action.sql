{{ config(
    materialized='table',
    tags=['gold', 'daily'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_name", "standardized_conversion_type"]
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
        campaign_id,
        campaign_name,
        campaign_status,
        -- Google doesn't have ad_group/ad level for this report, so we pass NULL or 'N/A'
        CAST(NULL AS STRING) as ad_group_name,
        CAST(NULL AS STRING) as ad_name,

        -- Conversion Details
        conversion_action_name as conversion_name,
        conversion_category as conversion_raw_category,

        -- GOOGLE MAPPING LOGIC
        -- We clean up the Google names to match the Standard List
        CASE
            WHEN conversion_category = 'PURCHASE' THEN 'PURCHASE'
            WHEN conversion_category = 'SUBMIT_LEAD_FORM' THEN 'LEAD'
            WHEN conversion_category = 'SIGNUP' THEN 'SIGNUP'
            WHEN conversion_category = 'PAGE_VIEW' THEN 'PAGE_VIEW'
            WHEN conversion_category = 'ENGAGEMENT' THEN 'ENGAGEMENT'
            -- Future proofing: If Google adds 'ADD_TO_CART' later, map it here
            WHEN conversion_category LIKE '%CART%' THEN 'ADD_TO_CART'
            WHEN conversion_category LIKE '%CHECKOUT%' THEN 'INITIATE_CHECKOUT'
            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- Metrics
        conversions,
        conversion_value,

        -- Context
        currency

    FROM {{ ref('stg_google_ads_conversion_action') }}
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
        -- Meta implies status is active if it's spending, but we can pass NULL
        CAST(NULL AS STRING) as campaign_status,
        ad_group_name,
        ad_name,

        -- Conversion Details
        conversion_action as conversion_name,
        -- Meta doesn't have a separate category column, so we use the name again
        conversion_action as conversion_raw_category,

        -- META MAPPING LOGIC (The Rosetta Stone)
        -- Order matters! Specific rules (Purchase) go before generic ones (View).
        CASE
            -- 1. REVENUE DRIVERS (High Priority)
            WHEN LOWER(conversion_action) LIKE '%purchase%' THEN 'PURCHASE'

            -- 2. LEADS & SIGNUPS
            WHEN LOWER(conversion_action) LIKE '%lead%' THEN 'LEAD'
            WHEN LOWER(conversion_action) LIKE '%complete_registration%' THEN 'SIGNUP'
            WHEN LOWER(conversion_action) LIKE '%subscribe%' THEN 'SIGNUP'

            -- 3. COMMERCE INTENT (Pre-Purchase)
            WHEN LOWER(conversion_action) LIKE '%add_to_cart%' THEN 'ADD_TO_CART'
            WHEN LOWER(conversion_action) LIKE '%checkout%' THEN 'INITIATE_CHECKOUT'
            WHEN LOWER(conversion_action) LIKE '%add_payment_info%' THEN 'INITIATE_CHECKOUT'

            -- 4. SITE ACTIVITY
            WHEN LOWER(conversion_action) LIKE '%view_content%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%landing_page_view%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%page_view%' THEN 'PAGE_VIEW'
            WHEN LOWER(conversion_action) LIKE '%app_site_visit%' THEN 'PAGE_VIEW'

            -- 5. CONTACT / SEARCH
            WHEN LOWER(conversion_action) LIKE '%search%' THEN 'SEARCH'
            WHEN LOWER(conversion_action) LIKE '%contact%' THEN 'CONTACT'
            WHEN LOWER(conversion_action) LIKE '%call%' THEN 'CONTACT'
            WHEN LOWER(conversion_action) LIKE '%messaging%' THEN 'CONTACT'

            -- 6. ENGAGEMENT (Social Actions)
            WHEN LOWER(conversion_action) LIKE '%engagement%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%like%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%comment%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%reaction%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%video_view%' THEN 'ENGAGEMENT'
            WHEN LOWER(conversion_action) LIKE '%post%' THEN 'ENGAGEMENT'

            ELSE 'OTHER'
        END as standardized_conversion_type,

        -- Metrics
        conversions,
        conversion_value,

        -- Context
        currency

    FROM {{ ref('stg_meta_ads_conversion_action') }}
)

-- COMBINE BOTH PLATFORMS
SELECT * FROM google_ads
UNION ALL
SELECT * FROM meta_ads