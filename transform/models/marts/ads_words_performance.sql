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
        campaign_status,
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
        unique_clicks,
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
        campaign_status,
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
        unique_clicks,
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
)

-- Note: Commenting the below logics since we dont need to explode words rather we stick with the platform report view

---- 4. Tokenization Engine (Explode Words)
--tokenized AS (
--    SELECT
--        *,
--        -- Logic: Clean, Lowercase, Split, Unnest
--        -- Output column is named 'word'
--        token as word
--    FROM combined_data,
--    -- Internal alias is named 'token' to prevent ambiguity
--    UNNEST(SPLIT(REGEXP_REPLACE(LOWER(raw_text), r'[^\w\s]', ''), ' ')) as token
--    WHERE LENGTH(token) > 2
--      AND token NOT IN ('the', 'and', 'for', 'with', 'from', 'that', 'this', 'your', 'are')
--),

---- 5. NEW STEP: Pre-Aggregate Phrases (Deduplication Layer)
---- This ensures "Ad Copy A" is only listed ONCE per word, with total stats.
--phrase_stats AS (
--    SELECT
--        -- Grouping Dimensions
--        date,
--        platform,
--        account_name,
--        account_id,
--        campaign_name,
--        campaign_status,
--        campaign_id,
--        event_name,
--        event_edition,
--
--        week_display,
--        week_number,
--        week_number_to_sort,
--        weeks_left,
--
--        cut_off_rate,
--        cut_off_rate_sort_order,
--
--        currency,
--
--        -- CLEANING & SPLITTING:
--        -- 1. LOWER(): Normalize case (Monaco = monaco)
--        -- 2. REGEXP_REPLACE(): Remove symbols (e.g., "Wow!" -> "Wow")
--        -- 3. SPLIT(): Turn string into Array ['best', 'conference']
--        -- 4. UNNEST(): Turn Array into Rows
--        word,
--
--        -- The Phrase itself (Truncated to 100 chars for readability)
--        CASE
--            WHEN LENGTH(raw_text) > 100 THEN CONCAT(LEFT(raw_text, 100), '...')
--            ELSE raw_text
--        END as clean_phrase,
--
--        -- Aggregated Stats for this specific phrase
--        SUM(cost) as phrase_cost,
--        SUM(impressions) as phrase_impressions,
--        SUM(clicks) as phrase_clicks,
--        SUM(unique_clicks) as phrase_unique_clicks,
--        SUM(conversions) as phrase_conversions,
--        SUM(conversion_value) as phrase_conversion_value
--
--    FROM tokenized
--    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
--),

---- 6. Global Stats (Ignore Date to get Grand Totals)
--global_phrase_agg AS (
--    SELECT
--        event_edition,
--        platform,
--        campaign_id,
--        campaign_status,
--        cut_off_rate,
--        word,
--        clean_phrase,
--        -- Summing across ALL dates
--        SUM(phrase_impressions) as total_global_impressions
--    FROM phrase_stats
--    GROUP BY 1, 2, 3, 4, 5, 6, 7
--),
--
---- 7. Build the Global Tooltip String
--global_tooltip_map AS (
--    SELECT
--        event_edition,
--        platform,
--        campaign_id,
--        campaign_status,
--        cut_off_rate,
--        word,
--        -- Create the list based on TOTAL volume, not daily
--        ARRAY_TO_STRING(
--            ARRAY_AGG(
--                CONCAT(clean_phrase, ' (', total_global_impressions, ')')
--                ORDER BY total_global_impressions DESC LIMIT 10
--            ),
--            '\n'
--        ) as global_top_combinations
--    FROM global_phrase_agg
--    GROUP BY 1, 2, 3, 4, 5, 6
--)

-- Note: Commenting the above logics since we dont need to explode words rather we stick with the platform report view

-- 8. Final Join (NO SPLITTING, KEEP EXACT PHRASE)
SELECT
    FARM_FINGERPRINT(CONCAT(CAST(c.date AS STRING), c.event_edition, c.platform, CAST(c.campaign_id AS STRING), c.raw_text)) as id,

    c.date,
    c.platform,
    EXTRACT(YEAR FROM c.date) as year,
    EXTRACT(MONTH FROM c.date) as month,
    EXTRACT(DAY FROM c.date) as day,
    c.event_edition,
    c.event_name,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.week_display,
    c.week_number,
    c.week_number_to_sort,
    c.weeks_left,
    c.cut_off_rate,
    c.cut_off_rate_sort_order,

    -- Output the exact phrase, just lowercased for clean grouping
    LOWER(c.raw_text) as word,

    -- We Keep this column so Power BI Tooltips don't break due to above changes, just pass the phrase
    c.raw_text as top_search_combinations,

-- Note: Commenting the below logics since we dont need to explode words rather we stick with the platform report view

--    -- Daily Stats
--    SUM(c.phrase_cost) as cost,
--    SUM(c.phrase_impressions) as impressions,
--    SUM(c.phrase_clicks) as clicks,
--    SUM(c.phrase_unique_clicks) as unique_clicks,
--    SAFE_DIVIDE(SUM(c.phrase_clicks), SUM(c.phrase_impressions)) as ctr,
--    SAFE_DIVIDE(SUM(c.phrase_unique_clicks), SUM(c.phrase_impressions)) as unique_ctr,
--    SAFE_DIVIDE(SUM(c.phrase_cost), SUM(c.phrase_clicks)) as average_cpc,
--    SUM(c.phrase_conversions) as conversions,
--    SUM(c.phrase_conversion_value) as conversion_value,
--    MAX(c.currency) as currency

    -- Daily Stats
    SUM(c.cost) as cost,
    SUM(c.impressions) as impressions,
    SUM(c.clicks) as clicks,
    SUM(c.unique_clicks) as unique_clicks,
    SAFE_DIVIDE(SUM(c.clicks), SUM(c.impressions)) as ctr,
    SAFE_DIVIDE(SUM(c.unique_clicks), SUM(c.impressions)) as unique_ctr,
    SAFE_DIVIDE(SUM(c.cost), SUM(c.clicks)) as average_cpc,
    SUM(c.conversions) as conversions,
    SUM(c.conversion_value) as conversion_value,
    MAX(c.currency) as currency

FROM combined_data c
-- Note: Commenting the below logics since we dont need to explode words rather we stick with the platform report view
-- Join the global stats on Campaign + Word
--LEFT JOIN global_tooltip_map g
--    ON p.event_edition = g.event_edition
--    AND p.platform = g.platform
--    AND p.campaign_id = g.campaign_id
--    AND p.campaign_status = g.campaign_status
--    AND p.cut_off_rate = g.cut_off_rate
--    AND p.word = g.word

GROUP BY
    c.date,
    c.platform,
    year,
    month,
    day,
    c.event_edition,
    c.event_name,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.week_display,
    c.week_number,
    c.week_number_to_sort,
    c.weeks_left,
    c.cut_off_rate,
    c.cut_off_rate_sort_order,
    word,
    top_search_combinations