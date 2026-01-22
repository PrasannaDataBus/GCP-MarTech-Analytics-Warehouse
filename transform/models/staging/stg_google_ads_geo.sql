{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily', 'geo']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_user_location_raw') }}

    -- COST SAVER: Runs only in Dev.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION: Removes duplicate raw rows
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, ad_group_id, user_geo_criterion_id
        ORDER BY _ingested_at DESC
    ) = 1
),

geo_names AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_geo_dim') }}
),

-- 1. Load Calendar
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        FARM_FINGERPRINT(CONCAT(
            CAST(s.date AS STRING),
            CAST(s.campaign_id AS STRING),
            CAST(s.ad_group_id AS STRING),
            CAST(s.user_geo_criterion_id AS STRING)
        )) as id,

        -- 2. Standardize Date
        CAST(s.date AS DATE) as date,
        CAST(s.account_id AS STRING) as account_id,
        s.account_name,

        -- EVENT NORMALIZATION (Matched to Reference Script)
        CASE
            WHEN s.account_name = 'Inactive - AMWC Asia' THEN 'AMWC Asia-TDAC'
            WHEN s.account_name = 'The Aesthetic Show UK' THEN 'TAS UK'
            WHEN s.account_name = 'AMWC Conference' THEN 'AMWC Monaco'
            WHEN s.account_name LIKE '%Dubai%' THEN 'AMWC Dubai'
            ELSE s.account_name
        END as event_name,

        CAST(s.campaign_id AS STRING) as campaign_id,
        s.campaign_name,
        s.campaign_status,
        CAST(s.ad_group_id AS STRING) as ad_group_id,
        s.ad_group_name,

        -- 3. Geography Dimensions
        COALESCE(g.Name, 'Unknown') as region_name,
        COALESCE(g.`Canonical Name`, 'Unknown') as canonical_name,
        COALESCE(g.`Country Code`, 'Unknown') as country_code,
        COALESCE(g.`Target Type`, 'Unknown') as geo_target_type,

        -- 4. Metrics
        (SAFE_CAST(s.cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(s.average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(s.impressions AS INT64) as impressions,
        SAFE_CAST(s.clicks AS INT64) as clicks,
        SAFE_CAST(s.ctr AS FLOAT64) as ctr,
        SAFE_CAST(s.conversions AS FLOAT64) as conversions,
        SAFE_CAST(s.conversions_value AS FLOAT64) as conversion_value,

        -- Google Specific
        SAFE_CAST(s.view_through_conversions AS FLOAT64) as view_through_conversions,
        SAFE_CAST(s.all_conversions AS FLOAT64) as all_conversions,
        s.bidding_strategy_type,
        s.currency

    FROM source s
    LEFT JOIN geo_names g
        ON CAST(s.user_geo_criterion_id AS STRING) = CAST(g.`Criteria ID` AS STRING)
),

-- 5. Join Logic (Connect to Calendar)
joined_data AS (
    SELECT
        base.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        ROW_NUMBER() OVER (
            PARTITION BY base.id
            ORDER BY ABS(DATE_DIFF(base.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed base
    LEFT JOIN calendar cal
        ON (
            -- YEAR-FIRST MATCHING
            (base.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR
            (base.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR
            (base.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR
            (base.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR
            (base.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (base.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR

            -- YEAR-LAST MATCHING
            (base.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%' OR cal.conference_editions LIKE 'AMWC North Americas%')) OR
            (base.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR
            (base.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE '%TAS UK%') OR
            (base.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR

            -- MAIN AMWC (Monaco)
            (base.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        AND base.date <= cal.event_end_date
        AND base.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 6. Logic Calculation
logic_layer AS (
    SELECT
        *,
        COALESCE(conference_editions, event_name) as event_edition,

        CASE
            WHEN conference_editions IS NULL THEN 'No Rate'
            WHEN date <= seb_end_date THEN 'SEB'
            WHEN date <= eb_end_date THEN 'EB'
            WHEN date <= advance_end_date THEN 'Advance'
            WHEN date <= fp_end_date THEN 'FP'
            WHEN date <= event_start_date THEN 'Onsite'
            ELSE 'Onsite / Late'
        END as cut_off_rate,

        CASE
            WHEN conference_editions IS NOT NULL THEN DATE_DIFF(event_start_date, date, WEEK)
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

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

    -- Geo Dimensions
    region_name,
    canonical_name,
    country_code,
    geo_target_type,

    -- Metrics
    cost,
    average_cpc,
    impressions,
    clicks,
    ctr,
    conversions,
    conversion_value,
    view_through_conversions,
    all_conversions,
    bidding_strategy_type,
    currency,

    -- Logic Output
    event_edition,
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

    CONCAT('Week ', CAST(calculated_week_number AS STRING)) as week,
    calculated_week_number as week_number,
    calculated_week_number as week_number_to_sort,
    weeks_left_card_value as weeks_left

FROM logic_layer