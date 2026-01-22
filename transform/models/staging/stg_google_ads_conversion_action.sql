{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_conversion_action_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- ROI PROTECTION: Deduplication
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, conversion_action_id, conversion_category
        ORDER BY _ingested_at DESC
    ) = 1
),

-- 1. Load Calendar (The "Truth" Table)
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        -- Note: Google Conversion Raw is at CAMPAIGN level, not Ad level.
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING), CAST(conversion_action_id AS STRING))) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT NAME NORMALIZATION (Matches Campaign Performance Logic)
        -- Ensures consistency for joins and Power BI slicers
        CASE
            WHEN account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            WHEN account_name = 'The Aesthetic Show UK' THEN 'TAS UK'
            WHEN account_name = 'AMWC Conference' THEN 'AMWC Monaco'
            WHEN account_name LIKE '%Dubai%' THEN 'AMWC Dubai'

            ELSE account_name
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,

        -- 4. Conversion Specific Dimensions
        CAST(conversion_action_id AS STRING) as conversion_action_id,
        conversion_action_name,
        conversion_category, -- CRITICAL: Use this to filter 'PURCHASE' vs 'LEAD'

        -- 5. Metrics
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        SAFE_CAST(all_conversions_value AS FLOAT64) as all_conversions_value,

        -- 6. Context
        bidding_strategy_type,
        currency

    FROM source
),

-- 2. Join Logic (Find the Edition)
-- Connects the normalized Event Name to the correct Calendar Year/Edition
joined_data AS (
    SELECT
        conv.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        -- Rank to find the CLOSEST future/past event
        -- Handles cycle overlaps (e.g., 2024 vs 2025)
        ROW_NUMBER() OVER (
            PARTITION BY conv.id
            ORDER BY ABS(DATE_DIFF(conv.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed conv
    LEFT JOIN calendar cal
        ON (
            -- AMWC Asia-TDAC (Taiwan) -> Maps to 'AMWC ASIA' series
            (conv.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR

            -- AMWC SEA - ICAD (Thailand) -> Maps to 'AMWC SEA' series
            (conv.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR

            -- AMWC Americas (Miami) -> Maps to 'AMWC Americas' or old 'AMWC NA'
            (conv.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%'
                                                   OR cal.conference_editions LIKE 'AMWC North Americas%')) OR

            -- AMWC LATAM (Medellin) -> Maps to 'AMWC LATAM' series
            (conv.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR

            -- AMWC Dubai Logic
            (conv.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR

            -- TAS UK (London) -> Maps to 'TAS UK' series
            (conv.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR

            -- TAS (US - Vegas) -> EXCLUDE 'TAS UK' to avoid overlap
            (conv.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE 'TAS UK%') OR

            -- Standard Series
            (conv.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR
            (conv.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (conv.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR

            -- Main AMWC (Monaco) Catch-all
            -- Matches 'AMWC 2024', 'AMWC 2025' but excludes the regional ones above
            (conv.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        -- Must be within 400 days BEFORE the event ends
        AND conv.date <= cal.event_end_date
        AND conv.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 3. Logic Calculation
logic_layer AS (
    SELECT
        *,
        -- LOGIC 1: Conference Edition (Fallback to Event Name if not found)
        COALESCE(conference_editions, event_name) as event_edition,

        -- LOGIC 2: Cut-Off Rate (Pricing Tiers)
        CASE
            WHEN conference_editions IS NULL THEN 'No Rate'
            WHEN date <= seb_end_date THEN 'SEB'
            WHEN date <= eb_end_date THEN 'EB'
            WHEN date <= advance_end_date THEN 'Advance'
            WHEN date <= fp_end_date THEN 'FP'
            WHEN date <= event_start_date THEN 'Onsite'
            ELSE 'Onsite / Late'
        END as cut_off_rate,

        -- LOGIC 3: Week Number (COUNTDOWN Style)
        -- Positive number = Weeks Remaining. (10 = 10 weeks to go)
        CASE
            WHEN conference_editions IS NOT NULL THEN
                DATE_DIFF(event_start_date, date, WEEK)
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

        -- LOGIC 4: Weeks Left (For PBI Scorecard)
        -- Calculates current status relative to Today
        CASE
            -- If No Edition: Use current Countdown to EOY
            WHEN conference_editions IS NULL THEN CAST(52 - EXTRACT(ISOWEEK FROM CURRENT_DATE()) AS STRING)

            -- If Event is Past: Unknown
            WHEN event_start_date < CURRENT_DATE() THEN 'Unknown'

            -- If Event is Future: Simple Diff (e.g., 10)
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
    campaign_status,

    -- Conversion Details
    conversion_action_id,
    conversion_action_name,
    conversion_category,

    -- Metrics
    conversions,
    conversion_value,
    all_conversions,
    all_conversions_value,

    bidding_strategy_type,
    currency,

    -- Event Context (New)
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

    -- Final Output: Week Slicer Value
    calculated_week_number as week_number,
    calculated_week_number as week_number_to_sort,

    -- Card Value
    weeks_left_card_value as weeks_left

FROM logic_layer