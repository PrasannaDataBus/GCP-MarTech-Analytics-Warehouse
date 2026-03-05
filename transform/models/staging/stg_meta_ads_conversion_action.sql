{{ config(
    materialized='view',
    tags=['silver', 'meta', 'daily']
) }}

WITH source AS (
    SELECT * FROM {{ source('marketing_raw', 'meta_ads_conversion_action_raw') }}

    -- COST SAVER: Runs only in Dev. Ignored in Prod.
    -- When merged to Prod, dbt ignores it automatically.
    {% if target.name == 'dev' %}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

    -- ROI PROTECTION: Deduplication
    -- Guarantees uniqueness by Date + Ad + Action Type
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY date, campaign_id, adset_id, ad_id, conversion_action
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

-- 1. Load Calendar (The "Truth" Table)
-- We need this to enrich the ad data with Event Editions and Dates
calendar AS (
    SELECT * FROM {{ ref('stg_global_events_calendar') }}
),

renamed AS (
    SELECT
        -- 1. Generate Unique ID
        -- Meta Conversion Raw IS at AD level.
        FARM_FINGERPRINT(CONCAT(CAST(date AS STRING), CAST(ad_id AS STRING), conversion_action)) as id,

        -- 2. Standardize Date
        CAST(date AS DATE) as date,
        EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
        EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
        EXTRACT(DAY FROM CAST(date AS DATE)) as day,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT MATCHING LOGIC (Maps Meta Campaigns to Event Series)
        -- CRITICAL UPDATE: We separated 'TAS UK' and 'AMWC Monaco' to match the Calendar logic.
        CASE
            -- 1. AMWC REGIONALS (Check these first to avoid falling into generic AMWC)
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
            -- UPDATED: Split TAS UK from FACE to map to the new 'TAS UK' Calendar series
            WHEN UPPER(campaign_name) LIKE '%TAS UK%'
              OR UPPER(campaign_name) LIKE '%TASUK%'
              OR UPPER(campaign_name) LIKE '%TAS-UK%'
              OR UPPER(campaign_name) LIKE '%THE AESTHETIC SHOW UK%'
              THEN 'TAS UK'

            WHEN UPPER(campaign_name) LIKE '%FACE%'
              OR UPPER(campaign_name) LIKE '%FACIAL AESTHETIC%'
              THEN 'FACE Conference'

            -- Check Generic TAS after TAS UK to avoid overlap
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
            -- UPDATED: Renamed to 'AMWC Monaco' to match Google logic
            WHEN UPPER(campaign_name) LIKE '%MONACO%'
              OR UPPER(campaign_name) LIKE '%MONTE CARLO%'
              OR UPPER(campaign_name) LIKE '%AMWCMC%'
              OR UPPER(campaign_name) LIKE '%AMWC%'
              THEN 'AMWC Monaco'

            WHEN UPPER(campaign_name) LIKE '%IM AESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IMAESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IM-AESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IMA%'
              THEN 'IM AESTHETICS'

            ELSE 'Other/Unmapped'

        END as event_name,

        CAST(source.campaign_id AS STRING) as campaign_id,
        campaign_name,

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

        CAST(adset_id AS STRING) as ad_group_id,
        adset_name as ad_group_name,

        CAST(ad_id AS STRING) as ad_id,
        ad_name,

        -- 4. Conversion Specific Dimensions
        conversion_action, -- e.g., 'purchase' or 'lead'

        -- 5. Metrics
        SAFE_CAST(conversions AS FLOAT64) as conversions,
        SAFE_CAST(conversion_value AS FLOAT64) as conversion_value,

        -- 6. Context
        currency

    FROM source
    LEFT JOIN campaign_dims dim
        ON CAST(source.campaign_id AS STRING) = dim.dim_campaign_id
),

-- 2. Join Logic (Connect Ad Data to Calendar)
joined_data AS (
    SELECT
        conv.*,
        cal.conference_editions,
        cal.event_start_date,
        cal.seb_end_date,
        cal.eb_end_date,
        cal.advance_end_date,
        cal.fp_end_date,

        -- RANKING LOGIC: Handle Event Cycles (e.g., TAS 2024 vs TAS 2025)
        -- We pick the event with the Start Date closest to the Ad Date
        ROW_NUMBER() OVER (
            PARTITION BY conv.id
            ORDER BY ABS(DATE_DIFF(conv.date, cal.event_start_date, DAY)) ASC
        ) as match_rank

    FROM renamed conv
    LEFT JOIN calendar cal
        ON (
            -- MAPPING: Connects Normalized "Event Name" to Calendar "Conference Editions"

            -- AMWC Asia-TDAC (Taiwan)
            (conv.event_name = 'AMWC Asia-TDAC' AND cal.conference_editions LIKE '%AMWC ASIA%') OR

            -- AMWC SEA - ICAD (Thailand)
            (conv.event_name = 'AMWC SEA - ICAD' AND cal.conference_editions LIKE '%AMWC SEA%') OR

            -- AMWC Americas (Miami)
            (conv.event_name = 'AMWC Americas' AND (cal.conference_editions LIKE 'AMWC Americas%' OR cal.conference_editions LIKE 'AMWC NA%' OR cal.conference_editions LIKE 'AMWC North Americas%')) OR

            -- AMWC LATAM (Medellin)
            (conv.event_name = 'AMWC LATAM' AND cal.conference_editions LIKE '%AMWC LATAM%') OR

            -- AMWC Dubai Logic
            (conv.event_name = 'AMWC Dubai' AND cal.conference_editions LIKE '%AMWC DUBAI%') OR

            -- TAS UK (London)
            (conv.event_name = 'TAS UK' AND cal.conference_editions LIKE '%TAS UK%') OR

            -- TAS (US - Vegas) - Exclude UK to avoid overlap
            (conv.event_name = 'TAS' AND cal.conference_editions LIKE 'TAS%' AND cal.conference_editions NOT LIKE 'TAS UK%') OR

            -- Standard Series
            (conv.event_name = 'VCS' AND cal.conference_editions LIKE 'VCS%') OR
            (conv.event_name = 'FACE Conference' AND cal.conference_editions LIKE '%FACE%') OR
            (conv.event_name = 'EUROGIN' AND cal.conference_editions LIKE '%EUROGIN%') OR

            -- AMWC Monaco (Flagship) - Matches 'AMWC 2024', 'AMWC 2025'
            (conv.event_name = 'AMWC Monaco' AND cal.conference_editions LIKE '% AMWC')
        )
        -- TIME WINDOW: Ad must be within 400 days BEFORE the event (Approx 1 Year Cycle)
        AND conv.date <= cal.event_end_date
        AND conv.date >= DATE_SUB(cal.event_end_date, INTERVAL 400 DAY)
),

-- 3. Business Logic Calculation
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
        CASE
            WHEN conference_editions IS NOT NULL THEN
                DATE_DIFF(event_start_date, date, WEEK)
            ELSE 52 - EXTRACT(ISOWEEK FROM date)
        END as calculated_week_number,

        -- LOGIC 4: Weeks Left Card Value (Current Status)
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
    conversion_action,
    conversions,
    conversion_value,
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
        ELSE 7
    END as cut_off_rate_sort_order,

    -- Final Output: Week Display (e.g., "Week 10")
    CONCAT('Week ', CAST(calculated_week_number AS STRING)) as week,

    -- Final Output: Week Slicer Value (e.g., 10)
    calculated_week_number as week_number,

    -- Final Output: Sort Order
    calculated_week_number as week_number_to_sort,

    -- Card Value
    weeks_left_card_value as weeks_left

FROM logic_layer