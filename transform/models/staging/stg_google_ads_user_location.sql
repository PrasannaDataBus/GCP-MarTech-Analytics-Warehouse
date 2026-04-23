{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily', 'user_location', 'conversions']
) }}

WITH source_standard AS (
    SELECT
        date,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        CAST(user_geo_criterion_id AS STRING) AS user_geo_criterion_id,
        is_targeting_location, -- This boolean tells us if it was explicitly targeted
        CAST(conversion_action_name AS STRING) AS conversion_action_name,
        conversions,
        conversions_value,
        _ingested_at

    FROM {{ source('marketing_raw', 'google_ads_user_location_raw') }}

    WHERE 1=1 -- This ensures the 'AND' below doesn't break syntax

    -- COST SAVER: Runs only in Dev.
    {% if target.name == 'dev' %}
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION: Removes duplicate raw rows
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, user_geo_criterion_id, conversion_action_name
        ORDER BY _ingested_at DESC
    ) = 1
),

geo_names AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_geo_dim') }}
),

-- Load Campaign Dimensions (Schedules & Status)
campaign_dims AS (
    SELECT
        CAST(campaign_id AS STRING) as dim_campaign_id,
        status,
        serving_status,
        start_date,
        end_date
    FROM {{ source('marketing_raw', 'google_ads_campaign_dim') }}
    -- Ensure we only grab the latest state per campaign
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY campaign_id
        ORDER BY _ingested_at DESC
    ) = 1
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
            CAST(s.user_geo_criterion_id AS STRING),
            CAST(s.conversion_action_name AS STRING)
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

        -- UI DELIVERY STATUS CALCULATION
        CASE
            WHEN dim.serving_status = 'ENDED' OR (dim.end_date IS NOT NULL AND dim.end_date < CURRENT_DATE()) THEN 'Completed'
            WHEN dim.status = 'PAUSED' THEN 'Off'
            WHEN dim.status = 'REMOVED' THEN 'Deleted'
            WHEN dim.status = 'ENABLED' AND dim.serving_status = 'SERVING' THEN 'Active'
            ELSE COALESCE(INITCAP(dim.serving_status), INITCAP(dim.status), 'Not delivering')
        END as campaign_status,

        -- Ad Groups are not in the conversion API, so we hardcode them to keep the schema intact
        'N/A' as ad_group_id,
        'Unknown' as ad_group_name,

        s.is_targeting_location,
        s.conversion_action_name,

        -- 3. Geography Dimensions
        COALESCE(g.Name, 'Unknown') as region_name,
        COALESCE(g.`Canonical Name`, 'Unknown') as canonical_name,
        COALESCE(g.`Country Code`, 'Unknown') as country_code,
        COALESCE(g.`Target Type`, 'Unknown') as geo_target_type,

        -- 4. Metrics
        0.0 as cost,
        0.0 as average_cpc,
        0 as impressions,
        0 as clicks,
        0 as unique_clicks,
        0.0 as ctr,
        0.0 as unique_ctr,
        SAFE_CAST(s.conversions AS FLOAT64) as conversions,
        SAFE_CAST(s.conversions_value AS FLOAT64) as conversion_value,

        -- Google Specific
        0.0 as view_through_conversions,
        0.0 as all_conversions,
        'N/A' as bidding_strategy_type,
        'N/A' as currency

    FROM source_standard s
    LEFT JOIN geo_names g
        ON CAST(s.user_geo_criterion_id AS STRING) = CAST(g.`Criteria ID` AS STRING)
    LEFT JOIN campaign_dims dim
        ON CAST(s.campaign_id AS STRING) = dim.dim_campaign_id
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
    is_targeting_location,
    conversion_action_name,

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
    unique_clicks,
    ctr,
    unique_ctr,
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