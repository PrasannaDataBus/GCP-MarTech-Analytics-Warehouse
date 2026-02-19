{{ config(
    materialized='view',
    tags=['silver', 'meta', 'creative', 'daily']
) }}

WITH performance_source AS (
    SELECT * FROM {{ source('marketing_raw', 'meta_ads_performance_raw') }}

    -- COST SAVER: Dev Limit (14 Days)
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- DEDUPLICATION: Granular Level (Ad + Date)
    -- Using 'date' instead of 'date_start' based on column list
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, ad_id
        ORDER BY _ingested_at DESC
    ) = 1
),

-- BRIDGE TABLE: Required to link Performance (Ad ID) -> Creative (Creative ID)
ad_dim_bridge AS (
    SELECT DISTINCT
        CAST(ad_id AS STRING) as ad_id,
        CAST(creative_id AS STRING) as creative_id
    FROM {{ source('marketing_raw', 'meta_ads_ad_dim') }}
),

-- CREATIVE CONTEXT:
creative_dim AS (
    SELECT DISTINCT
        CAST(creative_id AS STRING) as creative_id,
        creative_name,
        headline,
        body,
        destination_url,
        call_to_action_type,
        image_url
    FROM {{ source('marketing_raw', 'meta_ads_creative_dim') }}
),

-- LOAD CALENDAR
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        FARM_FINGERPRINT(CONCAT(CAST(perf.date AS STRING), CAST(perf.ad_id AS STRING))) as id,

        -- 2. Date Standardization
        CAST(perf.date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(perf.date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(perf.date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(perf.date AS DATE)) as day,

        -- 3. Dimensions
        CAST(perf.account_id AS STRING) as account_id,
        perf.account_name,

        -- EVENT MATCHING LOGIC (Preserved exactly from reference)
        CASE
            -- 1. AMWC REGIONALS
            WHEN UPPER(perf.campaign_name) LIKE '%MIAMI%'
              OR UPPER(perf.campaign_name) LIKE '%MCS%'
              OR UPPER(perf.campaign_name) LIKE '%AMERICAS%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC NA%'
              OR UPPER(perf.campaign_name) LIKE '%NORTH AMERICA%'
              OR UPPER(perf.campaign_name) LIKE '%NORTHAMERICA%'
              OR UPPER(perf.campaign_name) LIKE '%AMWCNORTHAMERICA%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC NORTH AMERICA%'
              THEN 'AMWC Americas'

            WHEN UPPER(perf.campaign_name) LIKE '%TAIPEI%'
              OR UPPER(perf.campaign_name) LIKE '%TAIWAN%'
              OR UPPER(perf.campaign_name) LIKE '%TDAC%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC ASIA%'
              OR UPPER(perf.campaign_name) LIKE '%AMWCASIA%'
              THEN 'AMWC Asia-TDAC'

            WHEN UPPER(perf.campaign_name) LIKE '%DUBAI%'
              OR UPPER(perf.campaign_name) LIKE '%GCC%'
              OR UPPER(perf.campaign_name) LIKE '%UAE%'
              OR UPPER(perf.campaign_name) LIKE '%MIDDLE-EAST%'
              THEN 'AMWC Dubai'

            WHEN UPPER(perf.campaign_name) LIKE '%BANGKOK%'
              OR UPPER(perf.campaign_name) LIKE '%THAILAND%'
              OR UPPER(perf.campaign_name) LIKE '%ICAD%'
              OR UPPER(perf.campaign_name) LIKE '%SEA%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC SOUTH%'
              OR UPPER(perf.campaign_name) LIKE '%SOUTHEAST ASIA%'
              OR UPPER(perf.campaign_name) LIKE '%SOUTHEASTASIA%'
              THEN 'AMWC SEA - ICAD'

            WHEN UPPER(perf.campaign_name) LIKE '%LATAM%'
              OR UPPER(perf.campaign_name) LIKE '%AMWCLATAM%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC-LATAM%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC LATAM%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC LATIN AMERICA%'
              OR UPPER(perf.campaign_name) LIKE '%LATIN AMERICA%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC MEDELLIN%'
              THEN 'AMWC LATAM'

            -- 2. SPECIFIC CONFERENCES
            WHEN UPPER(perf.campaign_name) LIKE '%TAS UK%'
              OR UPPER(perf.campaign_name) LIKE '%TASUK%'
              OR UPPER(perf.campaign_name) LIKE '%TAS-UK%'
              OR UPPER(perf.campaign_name) LIKE '%THE AESTHETIC SHOW UK%'
              THEN 'TAS UK'

            WHEN UPPER(perf.campaign_name) LIKE '%FACE%'
              OR UPPER(perf.campaign_name) LIKE '%FACIAL AESTHETIC%'
              THEN 'FACE Conference'

            WHEN UPPER(perf.campaign_name) LIKE '%TAS%'
              OR UPPER(perf.campaign_name) LIKE '%THE AESTHETIC SHOW%'
              THEN 'TAS'

            WHEN UPPER(perf.campaign_name) LIKE '%VCS%'
              OR UPPER(perf.campaign_name) LIKE '%VEGAS COSMETIC SURGERY%'
              THEN 'VCS'

            WHEN UPPER(perf.campaign_name) LIKE '%EUROGIN%'
              OR UPPER(perf.campaign_name) LIKE '%HPV%'
              OR UPPER(perf.campaign_name) LIKE '%GYN%'
              OR UPPER(perf.campaign_name) LIKE '%PAPILLOMA%'
              THEN 'EUROGIN'

            WHEN UPPER(perf.campaign_name) LIKE '%AMS%'
              OR UPPER(perf.campaign_name) LIKE '%AESTHETIC MULTISPECIALITY SOCIETY%'
              THEN 'AMS'

            -- 3. GENERIC AMWC
            WHEN UPPER(perf.campaign_name) LIKE '%MONACO%'
              OR UPPER(perf.campaign_name) LIKE '%MONTE CARLO%'
              OR UPPER(perf.campaign_name) LIKE '%AMWC%'
              THEN 'AMWC Monaco'

            WHEN UPPER(perf.campaign_name) LIKE '%IM AESTHETICS%'
              OR UPPER(perf.campaign_name) LIKE '%IMAESTHETICS%'
              OR UPPER(perf.campaign_name) LIKE '%IM-AESTHETICS%'
              THEN 'IM AESTHETICS'

            ELSE 'Other/Unmapped'
        END as event_name,

        CAST(perf.campaign_id AS STRING) as campaign_id,
        perf.campaign_name,
        perf.campaign_status,

        CAST(perf.adset_id AS STRING) as ad_group_id,
        perf.adset_name as ad_group_name,

        CAST(perf.ad_id AS STRING) as ad_id,
        perf.ad_name,

        -- CREATIVE CONTENT
        cre.headline as ad_headline,
        cre.body as ad_primary_text,
        cre.call_to_action_type,
        cre.destination_url as landing_page_url,
        cre.image_url,

        -- 4. Financials
        SAFE_CAST(perf.spend AS FLOAT64) as cost,
        SAFE_CAST(perf.average_cpc AS FLOAT64) as average_cpc,
        SAFE_CAST(perf.cpm AS FLOAT64) as cpm,

        -- 5. Performance
        SAFE_CAST(perf.impressions AS INT64) as impressions,
        SAFE_CAST(perf.clicks AS INT64) as clicks,
        SAFE_CAST(perf.unique_clicks AS INT64) as unique_clicks,
        SAFE_CAST(perf.ctr AS FLOAT64) as ctr,
        SAFE_CAST(perf.unique_ctr AS FLOAT64) as unique_ctr,
        SAFE_CAST(perf.conversions AS FLOAT64) as conversions,
        SAFE_CAST(perf.conversion_value AS FLOAT64) as conversion_value,
        SAFE_CAST(perf.reach AS INT64) as reach,
        SAFE_CAST(perf.frequency AS FLOAT64) as frequency,

        perf.currency

    FROM performance_source perf
    -- JOIN FLOW: Performance -> Bridge -> Creative
    LEFT JOIN ad_dim_bridge bridge
        ON CAST(perf.ad_id AS STRING) = bridge.ad_id
    LEFT JOIN creative_dim cre
        ON bridge.creative_id = cre.creative_id
),

-- 2. Join Logic (Calendar)
joined_data AS (
    SELECT
        ads.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        ROW_NUMBER() OVER (
            PARTITION BY ads.id
            ORDER BY ABS(DATE_DIFF(ads.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed ads
    LEFT JOIN calendar cal
        ON (
            -- MAPPING: Connects Normalized "Event Name" to Calendar "Conference Editions"

            -- AMWC Asia-TDAC (Taiwan)
            (ads.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR

            -- AMWC SEA - ICAD (Thailand)
            (ads.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR

            -- AMWC Americas (Miami)
            (ads.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%' OR cal.conference_editions LIKE 'AMWC North Americas%')) OR

            -- AMWC LATAM (Medellin)
            (ads.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR

            -- AMWC Dubai Logic
            (ads.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR

            -- TAS UK (London)
            (ads.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR

            -- TAS (US - Vegas) - Exclude UK to avoid overlap
            (ads.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE 'TAS UK%') OR

            -- Standard Series
            (ads.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR
            (ads.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (ads.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR

            -- AMWC Monaco (Flagship) - Matches 'AMWC 2024', 'AMWC 2025'
            (ads.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        -- TIME WINDOW: Ad must be within 400 days BEFORE the event (Approx 1 Year Cycle)
        AND ads.date <= cal.event_end_date
        AND ads.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 3. Logic Layer
logic_layer AS (
    SELECT
        *,
        -- LOGIC 1: Conference Edition (If no match found, use generic name)
        COALESCE(conference_editions, event_name) as event_edition,

        -- LOGIC 2: Cut-Off Rate (Pricing Tiers)
        -- Uses Short Codes (SEB, EB, FP) for clean charts
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
        -- A. Event Exists: Weeks Remaining (e.g., 10 weeks out = 10)
        -- B. No Event: Countdown to End of Year (aligns direction of charts)
        CASE
            WHEN conference_editions IS NOT NULL THEN
                DATE_DIFF(event_start_date, date, WEEK)
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

        -- LOGIC 4: Weeks Left Card Value (Current Status)
        -- Used for the big Scorecard in Power BI to guide Slicer selection
        CASE
            -- If No Edition: Use current Countdown to EOY
            WHEN conference_editions IS NULL THEN CAST(52 - EXTRACT(ISOWEEK FROM CURRENT_DATE()) AS STRING)

            -- If Event is Past: Unknown
            WHEN event_start_date < CURRENT_DATE() THEN 'Unknown'

            -- If Event is Future: Simple Diff (e.g., 10)
            ELSE CAST(DATE_DIFF(event_start_date, CURRENT_DATE(), WEEK) AS STRING)
        END as weeks_left_card_value

    FROM joined_data
    WHERE match_rank = 1 -- Keep only the single best event match
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

    -- Specific Creative Columns
    ad_headline,
    ad_primary_text,
    call_to_action_type,
    landing_page_url,
    image_url,

    cost,
    average_cpc,
    cpm,
    impressions,
    clicks,
    unique_clicks,
    ctr,
    unique_ctr,
    conversions,
    conversion_value,
    reach,
    frequency,
    currency,

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