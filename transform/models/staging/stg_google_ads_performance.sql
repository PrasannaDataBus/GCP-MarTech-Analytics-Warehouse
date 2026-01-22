{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_performance_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

-- 1. Load Calendar (The "Truth" Table)
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID (Ad Level)
        FARM_FINGERPRINT(CONCAT(
            CAST(date AS STRING),
            CAST(ad_id AS STRING),
            IFNULL(device, ''),
            IFNULL(ad_network_type, '')
        )) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT NAME NORMALIZATION
        -- Matches logic from 'stg_google_ads_campaign_performance'
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

        CAST(ad_group_id AS STRING) as ad_group_id,
        ad_group_name,

        CAST(ad_id AS STRING) as ad_id,
        ad_name,
        ad_type,

        -- 4. Financials
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        -- REMOVED: average_cpc (Calculated later)

        -- 5. Performance
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,

        -- REMOVED: conversions/values (Not in raw data)
        -- Only specific metrics available:
        SAFE_CAST(view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(all_conversions AS FLOAT64) as all_conversions,
        SAFE_CAST(engagements AS INT64) as engagements,

        bidding_strategy_type,

        -- 7. Segments
        device,
        ad_network_type,
        currency

    FROM source
),

-- 2. Join Logic (Connect Ad Data to Calendar)
joined_data AS (
    SELECT
        ads.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        -- RANKING LOGIC
        ROW_NUMBER() OVER (
            PARTITION BY ads.id
            ORDER BY ABS(DATE_DIFF(ads.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed ads
    LEFT JOIN calendar cal
        ON (
            (ads.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR
            (ads.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR
            (ads.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%' OR cal.conference_editions LIKE 'AMWC North Americas%')) OR
            (ads.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR
            (ads.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR
            (ads.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR
            (ads.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE 'TAS UK%') OR
            (ads.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR
            (ads.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (ads.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR
            (ads.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        AND ads.date <= cal.event_end_date
        AND ads.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 3. Business Logic Calculation
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

        -- LOGIC 4: Weeks Left
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
    campaign_status,
    ad_group_id,
    ad_group_name,
    ad_id,
    ad_name,
    ad_type,
    cost,

    -- CALCULATE CPC HERE (Since raw column is missing)
    SAFE_DIVIDE(cost, clicks) as average_cpc,

    impressions,
    clicks,
    ctr,

    -- Conversions missing in Google Raw, passing NULLs or alternate metrics
    -- If you want standard conversions to be 0/NULL for now to match schema:
    CAST(NULL AS FLOAT64) as conversions,
    CAST(NULL AS FLOAT64) as conversion_value,

    view_through_conversions,
    all_conversions,
    engagements,
    bidding_strategy_type,
    device,
    ad_network_type,
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