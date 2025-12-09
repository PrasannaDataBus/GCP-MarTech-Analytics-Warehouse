{{ config(
    materialized='view',
    tags=['silver', 'crm', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'crm_conversions_raw') }}
),

cleaned AS (
    SELECT
        -- 1. Primary Key
        CAST(id_commande AS STRING) as conversion_id,

        -- 2. Dates
        CAST(date_submit AS DATE) as date,
        -- Keep the timestamp for debugging/deep dives
        CAST(date_time_submit AS TIMESTAMP) as conversion_timestamp,

        -- 3. Financials
        -- Ensure revenue is a float and default to 0 if null
        COALESCE(SAFE_CAST(total_ht AS FLOAT64), 0) as revenue,
        TRIM(currency) as currency,

        -- 4. Dimensions (Cleaned)
        -- Normalizing to lowercase helps joins later
        LOWER(TRIM(utm_source)) as utm_source,
        LOWER(TRIM(utm_medium)) as utm_medium,
        LOWER(TRIM(utm_campaign)) as campaign_name,

        -- 5. Platform Normalization (The "Bridge" to Ads Data)
        CASE
            WHEN LOWER(utm_source) IN ('adwords', 'google', 'google_ads', 'youtube', 'gdn', 'waze') THEN 'Google Ads'
            WHEN LOWER(utm_source) IN ('facebook', 'instagram', 'meta', 'fb', 'ig', 'facebook_ads', 'instagram_ads', 'l.facebook.com', 'lm.facebook.com') THEN 'Meta Ads'
            ELSE 'Other' -- Should rarely happen given our extraction logic
        END as platform,

        -- 6. Business Context
        year,
        TRIM(conference_series) as conference_series,
        TRIM(conference_editions) as conference_editions,
        TRIM(city) as city,
        TRIM(country) as country,
        TRIM(region) as region,
        TRIM(orders) as orders,
        TRIM(new_customer) as new_customer,
        TRIM(cut_off_rate) as cut_off_rate,
        TRIM(order_type) as order_type,
        TRIM(primary_language) as primary_language,
        TRIM(specialty) as specialty

    FROM source
)

SELECT * FROM cleaned
-- Safety Filter: Only include rows that are actually paid/attributed to a platform
WHERE platform IN ('Google Ads', 'Meta Ads')
