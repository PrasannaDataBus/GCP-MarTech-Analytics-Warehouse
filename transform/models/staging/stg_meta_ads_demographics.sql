{{ config(
    materialized='view',
    tags=['silver', 'meta', 'daily', 'demographics']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'meta_ads_age_gender_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION
    -- Prevents the inflation bug.
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, adset_id, age, gender
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
            CAST(date AS STRING),
            CAST(campaign_id AS STRING),
            CAST(adset_id AS STRING),
            CAST(age AS STRING),
            CAST(gender AS STRING)
        )) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT MATCHING LOGIC (Maps Meta Campaigns to Event Series)
        CASE
            -- 1. AMWC REGIONALS
            WHEN UPPER(campaign_name) LIKE '%MIAMI%'
              OR UPPER(campaign_name) LIKE '%MCS%'
              OR UPPER(campaign_name) LIKE '%AMERICAS%'
              OR UPPER(campaign_name) LIKE '%AMWC NA%'
              OR UPPER(campaign_name) LIKE '%AMWCNA%'
              OR UPPER(campaign_name) LIKE '%NORTH AMERICA%'
              OR UPPER(campaign_name) LIKE '%NORTHAMERICA%'
              OR UPPER(campaign_name) LIKE '%AMWCNORTHAMERICA%'
              OR UPPER(campaign_name) LIKE '%AMWC NORTH AMERICA%'
              THEN 'AMWC Americas'

            WHEN UPPER(campaign_name) LIKE '%TAIPEI%'
              OR UPPER(campaign_name) LIKE '%TAIWAN%'
              OR UPPER(campaign_name) LIKE '%TDAC%'
              OR UPPER(campaign_name) LIKE '%AMWC ASIA%'
              OR UPPER(campaign_name) LIKE '%AMWCASIA%'
              THEN 'AMWC Asia-TDAC'

            WHEN UPPER(campaign_name) LIKE '%DUBAI%'
              OR UPPER(campaign_name) LIKE '%GCC%'
              OR UPPER(campaign_name) LIKE '%UAE%'
              OR UPPER(campaign_name) LIKE '%MIDDLE-EAST%'
              OR UPPER(campaign_name) LIKE '%AMWCUAE%'
              THEN 'AMWC Dubai'

            WHEN UPPER(campaign_name) LIKE '%BANGKOK%'
              OR UPPER(campaign_name) LIKE '%THAILAND%'
              OR UPPER(campaign_name) LIKE '%ICAD%'
              OR UPPER(campaign_name) LIKE '%SEA%'
              OR UPPER(campaign_name) LIKE '%AMWC SOUTH%'
              OR UPPER(campaign_name) LIKE '%SOUTHEAST ASIA%'
              OR UPPER(campaign_name) LIKE '%SOUTHEASTASIA%'
              OR UPPER(campaign_name) LIKE '%AMWCSEA%'
              THEN 'AMWC SEA - ICAD'

            WHEN UPPER(campaign_name) LIKE '%LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWCLATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC-LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC LATIN AMERICA%'
              OR UPPER(campaign_name) LIKE '%LATIN AMERICA%'
              OR UPPER(campaign_name) LIKE '%AMWC MEDELLIN%'
              THEN 'AMWC LATAM'

            -- 2. SPECIFIC CONFERENCES
            -- UPDATED: Split TAS UK from FACE
            WHEN UPPER(campaign_name) LIKE '%TAS UK%'
              OR UPPER(campaign_name) LIKE '%TASUK%'
              OR UPPER(campaign_name) LIKE '%TAS-UK%'
              OR UPPER(campaign_name) LIKE '%THE AESTHETIC SHOW UK%'
              THEN 'TAS UK'

            WHEN UPPER(campaign_name) LIKE '%FACE%'
              OR UPPER(campaign_name) LIKE '%FACIAL AESTHETIC%'
              THEN 'FACE Conference'

            -- Check Generic TAS after TAS UK
            WHEN UPPER(campaign_name) LIKE '%TAS%'
              OR UPPER(campaign_name) LIKE '%THE AESTHETIC SHOW%'
              THEN 'TAS'

            WHEN UPPER(campaign_name) LIKE '%VCS%'
              OR UPPER(campaign_name) LIKE '%VEGAS COSMETIC SURGERY%'
              THEN 'VCS'

            WHEN UPPER(campaign_name) LIKE '%EUROGIN%'
              OR UPPER(campaign_name) LIKE '%HPV%'
              OR UPPER(campaign_name) LIKE '%GYN%'
              OR UPPER(campaign_name) LIKE '%PAPILLOMA%'
              THEN 'EUROGIN'

            WHEN UPPER(campaign_name) LIKE '%AMS%'
              OR UPPER(campaign_name) LIKE '%AESTHETIC MULTISPECIALITY SOCIETY%'
              THEN 'AMS'

            -- 3. GENERIC AMWC (The Monaco Event)
            -- UPDATED: Renamed to 'AMWC Monaco'
            WHEN UPPER(campaign_name) LIKE '%MONACO%'
              OR UPPER(campaign_name) LIKE '%MONTE CARLO%'
              OR UPPER(campaign_name) LIKE '%AMWC%'
              OR UPPER(campaign_name) LIKE '%AMWCMC%'
              THEN 'AMWC Monaco'

            WHEN UPPER(campaign_name) LIKE '%IM AESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IMAESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IM-AESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IMA%'
              THEN 'IM AESTHETICS'

            ELSE 'Other/Unmapped'
        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,
        campaign_status,
        CAST(adset_id AS STRING) as ad_group_id,
        adset_name as ad_group_name,

        -- 4. Demographics
        age as age_group,
        INITCAP(gender) as gender,
        'Combined' as report_granularity,

        -- 5. Metrics
        SAFE_CAST(spend AS FLOAT64) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(unique_clicks AS INT64) as unique_clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(unique_ctr AS FLOAT64) as unique_ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversion_value AS FLOAT64) as conversion_value,
        currency

    FROM source
),

-- 2. Join Logic (Connect Ad Data to Calendar)
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
            -- YEAR-FIRST MATCHING (Wildcards at start)
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

    -- Demographics
    age_group,
    gender,
    report_granularity,

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