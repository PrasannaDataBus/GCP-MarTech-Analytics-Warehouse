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

        -- PATCH: Handle missing creatives (Applied to META only)
        -- If headline/body are NULL, use a placeholder so the row is preserved
        CASE
            WHEN ad_headline IS NULL AND ad_primary_text IS NULL THEN '_unknown_creative_'
            ELSE CONCAT(IFNULL(ad_headline, ''), ' ', IFNULL(ad_primary_text, ''))
        END as raw_text,

        -- Metrics
        cost,
        clicks,
        impressions,
        conversions,
        conversion_value,
        currency

    FROM {{ ref('stg_meta_ads_creative_performance') }}
),

-- 3. Combine Sources
combined_data AS (
    SELECT * FROM google_source
    UNION ALL
    SELECT * FROM meta_source
),

-- 4. Tokenization Engine (Explode Words)
tokenized AS (
    SELECT
        *,
        -- Logic: Clean, Lowercase, Split, Unnest
        -- Output column is named 'word'
        token as word
    FROM combined_data,
    -- Internal alias is named 'token' to prevent ambiguity
    UNNEST(SPLIT(REGEXP_REPLACE(LOWER(raw_text), r'[^\w\s]', ''), ' ')) as token
    WHERE LENGTH(token) > 2
      AND token NOT IN ('the', 'and', 'for', 'with', 'from', 'that', 'this', 'your', 'are')
),

-- 5. NEW STEP: Pre-Aggregate Phrases (Deduplication Layer)
-- This ensures "Ad Copy A" is only listed ONCE per word, with total stats.
phrase_stats AS (
    SELECT
        -- Grouping Dimensions
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

        -- The Phrase itself (Truncated to 100 chars for readability)
        CASE
            WHEN LENGTH(raw_text) > 100 THEN CONCAT(LEFT(raw_text, 100), '...')
            ELSE raw_text
        END as clean_phrase,

        -- Aggregated Stats for this specific phrase
        SUM(cost) as phrase_cost,
        SUM(impressions) as phrase_impressions,
        SUM(clicks) as phrase_clicks,
        SUM(conversions) as phrase_conversions,
        SUM(conversion_value) as phrase_conversion_value

    FROM tokenized
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)

-- 5. Final Aggregation
SELECT
    -- Unique ID for PBI (Date + Event + Platform + Word)
    FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), event_edition, platform, CAST(campaign_id AS STRING), word)) as id,

    date,
    platform,

    -- Date Parts (Consistent with Campaign Table)
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    EXTRACT(DAY FROM date) as day,

    event_edition,
    event_name,
    campaign_id,
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

    -- PERFECT TOOLTIP:
    -- Aggregates the unique, pre-summed phrases. No duplicates.
    ARRAY_TO_STRING(
        ARRAY_AGG(
            CONCAT(clean_phrase, ' (', phrase_impressions, ')')
            ORDER BY phrase_impressions DESC LIMIT 5
        ),
        '\n'
    ) as top_search_combinations,

    -- Total Stats for the Word
    SUM(phrase_cost) as cost,
    SUM(phrase_impressions) as impressions,
    SUM(phrase_clicks) as clicks,
    SAFE_DIVIDE(SUM(phrase_clicks), SUM(phrase_impressions)) as ctr,
    SAFE_DIVIDE(SUM(phrase_cost), SUM(phrase_clicks)) as average_cpc,
    SUM(phrase_conversions) as conversions,
    SUM(phrase_conversion_value) as conversion_value,
    MAX(currency) as currency

FROM phrase_stats
GROUP BY
    date,
    platform,
    event_edition,
    event_name,
    campaign_id,
    campaign_name,
    week_display,
    week_number,
    week_number_to_sort,
    weeks_left,
    cut_off_rate,
    cut_off_rate_sort_order,
    word