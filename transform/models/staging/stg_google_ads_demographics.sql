{{ config(
    materialized='view',
    tags=['silver', 'google', 'daily', 'demographics']
) }}

WITH age_source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_age_range_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION: Keep latest ingestion
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, ad_group_id, age_range
        ORDER BY _ingested_at DESC
    ) = 1
),

gender_source AS (
    SELECT * FROM {{ source('marketing_raw', 'google_ads_gender_raw') }}

    -- COST SAVER: This block runs ONLY in Dev.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, ad_group_id, gender
        ORDER BY _ingested_at DESC
    ) = 1
),

calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

-- 1. Standardize Age Data
google_age AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING), CAST(ad_group_id AS STRING), 'age', age_range)) as id,
        CAST(date AS DATE) as date,
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT NORMALIZATION (Matches Campaign Logic)
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

        -- DEMOGRAPHICS
        CASE
            WHEN age_range = 'AGE_RANGE_18_24' THEN '18-24'
            WHEN age_range = 'AGE_RANGE_25_34' THEN '25-34'
            WHEN age_range = 'AGE_RANGE_35_44' THEN '35-44'
            WHEN age_range = 'AGE_RANGE_45_54' THEN '45-54'
            WHEN age_range = 'AGE_RANGE_55_64' THEN '55-64'
            WHEN age_range = 'AGE_RANGE_65_UP' THEN '65+'
            ELSE 'Unknown'
        END as age_group,
        'Unspecified' as gender,
        'Age Only' as report_granularity,

        -- METRICS
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,
        currency

    FROM age_source
),

-- 2. Standardize Gender Data
google_gender AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(campaign_id AS STRING), CAST(ad_group_id AS STRING), 'gender', gender)) as id,
        CAST(date AS DATE) as date,
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT NORMALIZATION
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

        -- DEMOGRAPHICS
        'Unspecified' as age_group,
        gender,
        'Gender Only' as report_granularity,

        -- METRICS
        (SAFE_CAST(cost_micros AS FLOAT64) / 1000000) as cost,
        SAFE_CAST(average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(impressions AS INT64) as impressions,
        SAFE_CAST(clicks AS INT64) as clicks,
        SAFE_CAST(ctr AS FLOAT64) as ctr,
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversions_value AS FLOAT64) as conversion_value,
        currency

    FROM gender_source
),

-- 3. Combine Base Data
unioned_base AS (
    SELECT * FROM google_age
    UNION ALL
    SELECT * FROM google_gender
),

-- 4. Join Logic (Connect to Calendar ONCE)
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

    FROM unioned_base base
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

-- 5. Logic Calculation
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
    ctr,
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