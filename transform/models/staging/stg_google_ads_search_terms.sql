{{ config(
    materialized='view',
    tags=['silver', 'google', 'search_terms', 'daily']
) }}

WITH search_terms_source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_search_terms_raw') }}

    -- COST SAVER: Dev Limit (14 Days)
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION: Search Term Level
    -- Note: We assume 'search_term' + 'campaign_id' + 'date' is unique enough.
    -- If you see duplicates, we might need to add ad_group_id to the partition.
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, ad_group_id, search_term
        ORDER BY _ingested_at DESC
    ) = 1
),

-- 1. Load Calendar
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- Unique ID: Date + Campaign + Ad Group + Search Term
        FARM_FINGERPRINT(CONCAT(CAST(st.date AS STRING), CAST(st.campaign_id AS STRING), CAST(st.ad_group_id AS STRING), st.search_term)) as id,

        -- Date
        CAST(st.date AS DATE) as date,

        -- Dimensions (Directly from Raw Table - No Join Needed)
        CAST(st.account_id AS STRING) as account_id,
        st.account_name,

        -- Event Name Normalization (Standard Logic)
        CASE
            WHEN st.account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            WHEN st.account_name = 'The Aesthetic Show UK' THEN 'TAS UK'
            WHEN st.account_name = 'AMWC Conference' THEN 'AMWC Monaco'
            WHEN st.account_name LIKE '%Dubai%' THEN 'AMWC Dubai'
            ELSE st.account_name
        END as event_name,

        CAST(st.campaign_id AS STRING) as campaign_id,
        st.campaign_name,

        CAST(st.ad_group_id AS STRING) as ad_group_id,
        st.ad_group_name,

        -- Search Term Specifics
        st.search_term,
        -- REMOVED: search_term_match_type (Not in raw)
        -- REMOVED: status (Not in raw)

        CAST(st.keyword_id AS STRING) as keyword_id, -- Added this since it is in your list

        -- Metrics
        (SAFE_CAST(st.cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(st.impressions AS INT64) as impressions,
        SAFE_CAST(st.clicks AS INT64) as clicks,
        SAFE_CAST(st.conversions AS FLOAT64) as conversions,
        SAFE_CAST(st.conversions_value AS FLOAT64) as conversion_value, -- Added for value tracking

        -- Context
        st.currency

    FROM search_terms_source st
),

-- 2. Join Logic (Find the Edition)
joined_data AS (
    SELECT
        ads.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        -- Rank to find the CLOSEST future/past event
        ROW_NUMBER() OVER (
            PARTITION BY ads.id
            ORDER BY ABS(DATE_DIFF(ads.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed ads
    LEFT JOIN calendar cal
        ON (
            -- AMWC Asia-TDAC (Taiwan) -> Maps to 'AMWC ASIA' series
            (ads.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR

            -- AMWC SEA - ICAD (Thailand) -> Maps to 'AMWC SEA' series
            (ads.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR

            -- AMWC Americas (Miami) -> Maps to 'AMWC Americas' or old 'AMWC NA'
            (ads.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%'
                                                   OR cal.conference_editions LIKE 'AMWC North Americas%')) OR

            -- AMWC LATAM (Medellin) -> Maps to 'AMWC LATAM' series
            (ads.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR

            -- AMWC Dubai Logic
            (ads.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR

            -- TAS UK (London) -> Maps to 'TAS UK' series
            (ads.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR

            -- TAS (US - Vegas) -> EXCLUDE 'TAS UK' to avoid overlap
            (ads.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE 'TAS UK%') OR

            -- Standard Series
            (ads.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR
            (ads.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (ads.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR

            -- Main AMWC (Monaco) Catch-all
            -- Matches 'AMWC 2024', 'AMWC 2025' but excludes the regional ones above
            (ads.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        -- Must be within 400 days BEFORE the event ends
        AND ads.date <= cal.event_end_date
        AND ads.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 3. Logic Calculation
logic_layer AS (
    SELECT
        *,
        -- LOGIC 1: Conference Edition
        COALESCE(conference_editions, event_name) as event_edition,

        -- LOGIC 2: Cut-Off Rate
        CASE
            WHEN conference_editions IS NULL THEN 'No Rate'
            WHEN date <= seb_end_date THEN 'SEB'
            WHEN date <= eb_end_date THEN 'EB'
            WHEN date <= advance_end_date THEN 'Advance'
            WHEN date <= fp_end_date THEN 'FP'
            WHEN date <= event_start_date THEN 'Onsite'
            ELSE 'Onsite / Late'
        END as cut_off_rate,

        -- LOGIC 3: Week Number
        CASE
            WHEN conference_editions IS NOT NULL THEN
                DATE_DIFF(event_start_date, date, WEEK)
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

        -- Logic 4: Weeks Left Card
        CASE
            WHEN conference_editions IS NULL THEN CAST(52 - EXTRACT(ISOWEEK FROM CURRENT_DATE()) AS STRING)
            WHEN event_start_date < CURRENT_DATE() THEN 'Unknown'
            ELSE CAST(DATE_DIFF(event_start_date, CURRENT_DATE(), WEEK) AS STRING)
        END as weeks_left_card_value

    FROM joined_data
    WHERE match_rank = 1
)

SELECT
    id,
    date,
    account_id,
    account_name,
    event_name,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,

    -- Specific Search Term Columns
    search_term,
    keyword_id,

    cost,
    impressions,
    clicks,

    -- CTR Calculation (Safe Divide)
    SAFE_DIVIDE(clicks, impressions) as ctr,

    conversions,
    conversion_value,
    currency,
    event_edition,

    -- Final Output: Cut Off Rate
    cut_off_rate,
    CASE cut_off_rate
        WHEN 'SEB' THEN 1
        WHEN 'EB' THEN 2
        WHEN 'Advance' THEN 3
        WHEN 'FP' THEN 4
        WHEN 'Onsite' THEN 5
        WHEN 'Onsite / Late' THEN 6
        ELSE 7
    END as cut_off_rate_sort_order,

    -- Final Output: Week Display
    CONCAT('Week ', CAST(calculated_week_number AS STRING)) as week,
    calculated_week_number as week_number,
    calculated_week_number as week_number_to_sort,
    weeks_left_card_value as weeks_left

FROM logic_layer