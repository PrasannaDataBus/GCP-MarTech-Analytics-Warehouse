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
),

-- 6. Global Stats (Ignore Date to get Grand Totals)
global_phrase_agg AS (
    SELECT
        event_edition,
        platform,
        campaign_id,
        cut_off_rate,
        word,
        clean_phrase,
        -- Summing across ALL dates
        SUM(phrase_impressions) as total_global_impressions
    FROM phrase_stats
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- 7. Build the Global Tooltip String
global_tooltip_map AS (
    SELECT
        event_edition,
        platform,
        campaign_id,
        cut_off_rate,
        word,
        -- Create the list based on TOTAL volume, not daily
        ARRAY_TO_STRING(
            ARRAY_AGG(
                CONCAT(clean_phrase, ' (', total_global_impressions, ')')
                ORDER BY total_global_impressions DESC LIMIT 10
            ),
            '\n'
        ) as global_top_combinations
    FROM global_phrase_agg
    GROUP BY 1, 2, 3, 4, 5
)

-- 8. Final Join (Daily Data + Global Tooltip)
SELECT
    FARM_FINGERPRINT(CONCAT(CAST(p.date AS STRING), p.event_edition, p.platform, CAST(p.campaign_id AS STRING), p.word)) as id,

    p.date,
    p.platform,
    EXTRACT(YEAR FROM p.date) as year,
    EXTRACT(MONTH FROM p.date) as month,
    EXTRACT(DAY FROM p.date) as day,
    p.event_edition,
    p.event_name,
    p.campaign_id,
    p.campaign_name,
    p.week_display,
    p.week_number,
    p.week_number_to_sort,
    p.weeks_left,
    p.cut_off_rate,
    p.cut_off_rate_sort_order,

    p.word,

    -- USE THE GLOBAL STRING (Joined from Step 7)
    g.global_top_combinations as top_search_combinations,

    -- Daily Stats
    SUM(p.phrase_cost) as cost,
    SUM(p.phrase_impressions) as impressions,
    SUM(p.phrase_clicks) as clicks,
    SAFE_DIVIDE(SUM(p.phrase_clicks), SUM(p.phrase_impressions)) as ctr,
    SAFE_DIVIDE(SUM(p.phrase_cost), SUM(p.phrase_clicks)) as average_cpc,
    SUM(p.phrase_conversions) as conversions,
    SUM(p.phrase_conversion_value) as conversion_value,
    MAX(p.currency) as currency

FROM phrase_stats p
-- Join the global stats on Campaign + Word
LEFT JOIN global_tooltip_map g
    ON p.event_edition = g.event_edition
    AND p.platform = g.platform
    AND p.campaign_id = g.campaign_id
    AND p.cut_off_rate = g.cut_off_rate
    AND p.word = g.word

GROUP BY
    p.date,
    p.platform,
    p.event_edition,
    p.event_name,
    p.campaign_id,
    p.campaign_name,
    p.week_display,
    p.week_number,
    p.week_number_to_sort,
    p.weeks_left,
    p.cut_off_rate,
    p.cut_off_rate_sort_order,
    p.word,
    g.global_top_combinations