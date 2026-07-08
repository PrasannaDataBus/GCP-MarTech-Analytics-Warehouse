{{ config(
    materialized='view',
    tags=['silver', 'meta', 'daily', 'geo', 'conversions']
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('marketing_raw', 'meta_ads_country_raw') }}

    -- COST SAVER: Runs only in Dev.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

-- 1. UNNEST FIRST: Explode the JSON array into separate rows per action
unnested_source AS (
    SELECT
        r.* EXCEPT(actions, action_values),
        JSON_EXTRACT_SCALAR(act, '$.action_type') AS conversion_action_name,
        CAST(JSON_EXTRACT_SCALAR(act, '$.value') AS FLOAT64) AS extracted_conversions,
        (
            SELECT CAST(JSON_EXTRACT_SCALAR(val, '$.value') AS FLOAT64)
            FROM UNNEST(JSON_EXTRACT_ARRAY(r.action_values)) AS val
            WHERE JSON_EXTRACT_SCALAR(val, '$.action_type') = JSON_EXTRACT_SCALAR(act, '$.action_type')
            LIMIT 1
        ) AS extracted_conversion_value
    FROM raw_data r
    INNER JOIN UNNEST(JSON_EXTRACT_ARRAY(r.actions)) AS act
    WHERE JSON_EXTRACT_SCALAR(act, '$.action_type') IS NOT NULL
),

-- 2. DEDUP SECOND: Now partition by the conversion_action_name as well!
source AS (
    SELECT *
    FROM unnested_source
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, adset_id, ad_id, country, conversion_action_name
        ORDER BY _ingested_at DESC
    ) = 1
),

-- Load New Campaign Dimensions (Schedules & Status)
campaign_dims AS (
    SELECT
        CAST(campaign_id AS STRING) as dim_campaign_id,
        effective_status,
        start_time,
        stop_time
    FROM {{ source('marketing_raw', 'meta_ads_campaign_dim') }}
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
            CAST(source.date AS STRING),
            CAST(source.campaign_id AS STRING),
            CAST(source.adset_id AS STRING),
            CAST(source.ad_id AS STRING),
            CAST(source.country AS STRING),
            CAST(source.conversion_action_name AS STRING)
        )) as id,

        -- 2. Standardize Date
        CAST(source.date AS DATE) as date,
        CAST(source.account_id AS STRING) as account_id,
        source.account_name,

        -- EVENT MATCHING LOGIC (Updated to match Google Logic)
        CASE
            -- 1. AMWC REGIONALS
            WHEN UPPER(source.campaign_name) LIKE '%MIAMI%'
              OR UPPER(source.campaign_name) LIKE '%MCS%'
              OR UPPER(source.campaign_name) LIKE '%AMERICAS%'
              OR UPPER(source.campaign_name) LIKE '%AMWC NA%'
              OR UPPER(source.campaign_name) LIKE '%AMWCNA%'
              OR UPPER(source.campaign_name) LIKE '%NORTH AMERICA%'
              OR UPPER(source.campaign_name) LIKE '%NORTHAMERICA%'
              OR UPPER(source.campaign_name) LIKE '%AMWCNORTHAMERICA%'
              OR UPPER(source.campaign_name) LIKE '%AMWC NORTH AMERICA%'
              THEN 'AMWC Americas'

            WHEN UPPER(source.campaign_name) LIKE '%TAIPEI%'
              OR UPPER(source.campaign_name) LIKE '%TAIWAN%'
              OR UPPER(source.campaign_name) LIKE '%TDAC%'
              OR UPPER(source.campaign_name) LIKE '%AMWC ASIA%'
              OR UPPER(source.campaign_name) LIKE '%AMWCASIA%'
              THEN 'AMWC Asia-TDAC'

            WHEN UPPER(source.campaign_name) LIKE '%DUBAI%'
              OR UPPER(source.campaign_name) LIKE '%GCC%'
              OR UPPER(source.campaign_name) LIKE '%UAE%'
              OR UPPER(source.campaign_name) LIKE '%MIDDLE-EAST%'
              OR UPPER(source.campaign_name) LIKE '%AMWCUAE%'
              THEN 'AMWC Dubai'

            WHEN UPPER(source.campaign_name) LIKE '%BANGKOK%'
              OR UPPER(source.campaign_name) LIKE '%THAILAND%'
              OR UPPER(source.campaign_name) LIKE '%ICAD%'
              OR UPPER(source.campaign_name) LIKE '%SEA%'
              OR UPPER(source.campaign_name) LIKE '%AMWC SOUTH%'
              OR UPPER(source.campaign_name) LIKE '%SOUTHEAST ASIA%'
              OR UPPER(source.campaign_name) LIKE '%SOUTHEASTASIA%'
              OR UPPER(source.campaign_name) LIKE '%AMWCSEA%'
              THEN 'AMWC SEA - ICAD'

            WHEN UPPER(source.campaign_name) LIKE '%LATAM%'
              OR UPPER(source.campaign_name) LIKE '%AMWCLATAM%'
              OR UPPER(source.campaign_name) LIKE '%AMWC-LATAM%'
              OR UPPER(source.campaign_name) LIKE '%AMWC LATAM%'
              OR UPPER(source.campaign_name) LIKE '%AMWC LATIN AMERICA%'
              OR UPPER(source.campaign_name) LIKE '%LATIN AMERICA%'
              OR UPPER(source.campaign_name) LIKE '%AMWC MEDELLIN%'
              THEN 'AMWC LATAM'

            -- 2. SPECIFIC CONFERENCES
            -- UPDATED: Maps to 'TAS UK' explicitly (previously mapped to FACE)
            WHEN UPPER(source.campaign_name) LIKE '%TAS UK%'
              OR UPPER(source.campaign_name) LIKE '%TASUK%'
              OR UPPER(source.campaign_name) LIKE '%TAS-UK%'
              OR UPPER(source.campaign_name) LIKE '%THE AESTHETIC SHOW UK%'
              THEN 'TAS UK'

            WHEN UPPER(source.campaign_name) LIKE '%FACE%'
              OR UPPER(source.campaign_name) LIKE '%FACIAL AESTHETIC%'
              THEN 'FACE Conference'

            -- Check Generic TAS after TAS UK
            WHEN UPPER(source.campaign_name) LIKE '%TAS%'
              OR UPPER(source.campaign_name) LIKE '%THE AESTHETIC SHOW%'
              THEN 'TAS'

            WHEN UPPER(source.campaign_name) LIKE '%VCS%'
              OR UPPER(source.campaign_name) LIKE '%VEGAS COSMETIC SURGERY%'
              THEN 'VCS'

            WHEN UPPER(source.campaign_name) LIKE '%EUROGIN%'
              OR UPPER(source.campaign_name) LIKE '%HPV%'
              OR UPPER(source.campaign_name) LIKE '%GYN%'
              OR UPPER(source.campaign_name) LIKE '%PAPILLOMA%'
              THEN 'EUROGIN'

            WHEN UPPER(source.campaign_name) LIKE '%AMS%'
              OR UPPER(source.campaign_name) LIKE '%AESTHETIC MULTISPECIALITY SOCIETY%'
              THEN 'AMS'

            -- 3. GENERIC AMWC (The Monaco Event)
            -- UPDATED: Renamed to 'AMWC Monaco'
            WHEN UPPER(source.campaign_name) LIKE '%MONACO%'
              OR UPPER(source.campaign_name) LIKE '%MONTE CARLO%'
              OR UPPER(source.campaign_name) LIKE '%AMWC%'
              OR UPPER(source.campaign_name) LIKE '%AMWCMC%'
              THEN 'AMWC Monaco'

            WHEN UPPER(source.campaign_name) LIKE '%IM AESTHETICS%'
              OR UPPER(source.campaign_name) LIKE '%IMAESTHETICS%'
              OR UPPER(source.campaign_name) LIKE '%IM-AESTHETICS%'
              OR UPPER(source.campaign_name) LIKE '%IMA%'
              THEN 'IM AESTHETICS'

            ELSE 'Other/Unmapped'
        END as event_name,

        CAST(source.campaign_id AS STRING) as campaign_id,
        source.campaign_name,
        -- Aligning Meta Adsets to Google Ad Groups format
        CAST(source.adset_id AS STRING) as ad_group_id,
        source.adset_name as ad_group_name,

        -- UI DELIVERY STATUS CALCULATION (Replaces raw status)
        CASE
            -- If the campaign has an end date and it is in the past, it is Completed
            WHEN dim.stop_time IS NOT NULL AND EXTRACT(DATE FROM dim.stop_time) < CURRENT_DATE() THEN 'Completed'
            -- If it was manually turned off, it is Off
            WHEN dim.effective_status = 'PAUSED' THEN 'Off'
            -- If it is actively running, it is Active
            WHEN dim.effective_status = 'ACTIVE' THEN 'Active'
            -- Fallback for pending, deleted, etc.
            ELSE COALESCE(INITCAP(dim.effective_status), 'Not delivering')
        END as campaign_status,

        'TARGETED_LOCATION' as location_type,
        source.conversion_action_name,

        -- 4. Geography Dimensions
        'Unknown' as region_name,
        -- Generate canonical name to match Google format
        source.country as canonical_name,
        source.country as country_code,
        'Country' as geo_target_type,

        -- Metrics Zeroed Out
        0.0 as cost,
        0.0 as average_cpc,
        0 as impressions,
        0 as clicks,
        0 as unique_clicks,
        0.0 as ctr,
        0.0 as unique_ctr,
        COALESCE(source.extracted_conversions, 0.0) as conversions,
        COALESCE(source.extracted_conversion_value, 0.0) as conversion_value,

        -- Meta specific fallback
        0.0 as view_through_conversions,
        0.0 as all_conversions,
        'N/A' as bidding_strategy_type,
        source.currency

    FROM source
    LEFT JOIN campaign_dims dim
        ON CAST(source.campaign_id AS STRING) = dim.dim_campaign_id
),

-- 2. Join Logic (Connect to Calendar)
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

-- 3. Logic Calculation
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
    location_type,
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