{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_campaign_performance_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION:
    -- Ensures we take the freshest row and remove duplicates BEFORE joining
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id
        ORDER BY _ingested_at DESC
    ) = 1
),

-- 1. Load Calendar
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    -- Unique ID (Campaign Level)
    SELECT
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING))) as id,

        -- Date
        CAST(date AS DATE) as date,

        -- Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- Event Name Normalization
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

        -- NEW: This distinguishes PMax from Search
        advertising_channel_type as channel_type,

        -- Metrics
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,

        -- Context
        bidding_strategy_type,
        currency
    FROM source
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
        -- LOGIC 1: Conference Edition (Fallback to Event Name)
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

        -- LOGIC 3: Week Number (COUNTDOWN Style)
        CASE
            -- A. Event Exists: Countdown (10 weeks before = 10)
            WHEN conference_editions IS NOT NULL THEN
                DATE_DIFF(event_start_date, date, WEEK)

            -- B. No Event: Countdown to End of Year (Dec 31)
            -- Week to match the "Countdown" direction
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

        -- Weeks Left (For PBI Card)
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
    channel_type,
    cost,
    impressions,
    clicks,
    ctr,
    bidding_strategy_type,
    currency,
    event_edition,

    -- Final Output: Cut Off Rate
    cut_off_rate,

    -- Sort Order for Cut Off Rate (1=SEB, 2=EB, etc.)
    CASE cut_off_rate
        WHEN 'SEB' THEN 1
        WHEN 'EB' THEN 2
        WHEN 'Advance' THEN 3
        WHEN 'FP' THEN 4
        WHEN 'Onsite' THEN 5
        WHEN 'Onsite / Late' THEN 6
        ELSE 7 -- No Rate
    END as cut_off_rate_sort_order,

    -- Final Output: Week Display
    CONCAT('Week ', CAST(calculated_week_number AS STRING)) as week,

    -- Final Output: Week Slicer Value
    calculated_week_number as week_number,

    -- Final Output: Sort Order (Same as number for simplicity)
    calculated_week_number as week_number_to_sort,

    -- Card Value
    weeks_left_card_value as weeks_left

FROM logic_layer