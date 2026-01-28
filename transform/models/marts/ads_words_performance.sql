{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["platform", "event_edition", "word"],
    tags=['gold', 'words', 'daily']
) }}

WITH
-- 1. Google Source: Search Terms (What users TYPED)
google_source AS (
    SELECT
        date,
        'Google Ads' as platform, -- Matches campaign table
        account_name,
        account_id,
        campaign_name,
        campaign_id,
        event_name,
        event_edition,

        -- Event Cycle Dimensions (CRITICAL: Matches Campaign Table Slicers)
        week as week_display,
        week_number,
        week_number_to_sort,
        weeks_left,

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Text to Analyze
        search_term as raw_text,

        -- Metrics
        cost,
        clicks,
        impressions,
        conversions,
        conversion_value,
        currency

    FROM {{ ref('stg_google_ads_search_terms') }}
    WHERE search_term IS NOT NULL
),

-- 2. Meta Source: Creative Text (What we SHOWED)
meta_source AS (
    SELECT
        date,
        'Meta Ads' as platform, -- Matches campaign table
        account_name,
        account_id,
        campaign_name,
        campaign_id,
        event_name,
        event_edition,

        -- Event Cycle Dimensions
        week as week_display,
        week_number,
        week_number_to_sort,
        weeks_left,

        -- Pricing / Rate Context
        cut_off_rate,
        cut_off_rate_sort_order,

        -- Text to Analyze: Concatenate Headline + Body
        -- We add a space ' ' to ensure "HeadlineWord" doesn't merge with "BodyWord"
        CONCAT(IFNULL(ad_headline, ''), ' ', IFNULL(ad_primary_text, '')) as raw_text,

        -- Metrics
        cost,
        clicks,
        impressions,
        conversions,
        conversion_value,
        currency

    FROM {{ ref('stg_meta_ads_creative_performance') }}
    WHERE ad_headline IS NOT NULL OR ad_primary_text IS NOT NULL
),

-- 3. Combine Sources
combined_data AS (
    SELECT * FROM google_source
    UNION ALL
    SELECT * FROM meta_source
),

-- 4. Tokenization Engine (The Logic Layer)
tokenized AS (
    SELECT
        date,
        platform,
        account_name,
        account_id,
        campaign_name,
        campaign_id,
        event_name,
        event_edition,

        week_display,
        week_number,
        week_number_to_sort,
        weeks_left,

        cut_off_rate,
        cut_off_rate_sort_order,

        currency,

        -- CLEANING & SPLITTING:
        -- 1. LOWER(): Normalize case (Monaco = monaco)
        -- 2. REGEXP_REPLACE(): Remove symbols (e.g., "Wow!" -> "Wow")
        -- 3. SPLIT(): Turn string into Array ['best', 'conference']
        -- 4. UNNEST(): Turn Array into Rows
        word,

        cost,
        clicks,
        impressions,
        conversions,
        conversion_value

    FROM combined_data,
    UNNEST(SPLIT(REGEXP_REPLACE(LOWER(raw_text), r'[^\w\s]', ''), ' ')) as word
)

-- 5. Final Aggregation
SELECT
    -- Unique ID for PBI (Date + Event + Platform + Word)
    FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), event_edition, platform, word)) as id,

    date,
    platform,

    -- Date Parts (Consistent with Campaign Table)
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    EXTRACT(DAY FROM date) as day,

    event_edition,
    event_name,
    campaign_name,

    -- Slicer Columns
    week_display,
    week_number,
    week_number_to_sort,
    weeks_left,
    cut_off_rate,
    cut_off_rate_sort_order,

    -- The Atomic Unit
    word,

    -- Aggregated Metrics
    SUM(cost) as cost,
    SUM(impressions) as impressions,
    SUM(clicks) as clicks,

    -- Calculated Metrics (Safe Divide)
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) as ctr,
    SAFE_DIVIDE(SUM(cost), SUM(clicks)) as average_cpc,

    SUM(conversions) as conversions,
    SUM(conversion_value) as conversion_value,

    MAX(currency) as currency

FROM tokenized

WHERE
    -- NOISE FILTER:
    -- Remove words with 2 or fewer letters (e.g., 'a', 'to', 'in', 'of')
    LENGTH(word) > 2
    -- Remove specific stopwords if needed
    AND word NOT IN ('the', 'and', 'for', 'with', 'from', 'that', 'this', 'your', 'are')

GROUP BY
    date,
    platform,
    event_edition,
    event_name,
    campaign_name,
    week_display,
    week_number,
    week_number_to_sort,
    weeks_left,
    cut_off_rate,
    cut_off_rate_sort_order,
    word