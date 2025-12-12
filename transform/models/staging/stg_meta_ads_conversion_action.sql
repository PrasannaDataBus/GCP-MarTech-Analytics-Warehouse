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
        EXTRACT(ISOWEEK FROM CAST(date AS DATE)) as week_number,
        EXTRACT(ISOWEEK FROM CURRENT_DATE()) as current_week_number,

        -- 3. Dimensions
        CAST(account_id AS STRING) as account_id,
        account_name,

        -- EVENT MATCHING LOGIC (Maps Meta Campaigns to Google Account Names)
        -- We use UPPER() to make matching case-insensitive (e.g., "face" = "FACE")
        CASE
            -- 1. AMWC REGIONALS (Check these first to avoid falling into generic AMWC)
            WHEN UPPER(campaign_name) LIKE '%MIAMI%'
              OR UPPER(campaign_name) LIKE '%MCS%'
              OR UPPER(campaign_name) LIKE '%AMERICAS%'
              OR UPPER(campaign_name) LIKE '%AMWC NA%'
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

              THEN 'AMWC Dubai'

            WHEN UPPER(campaign_name) LIKE '%BANGKOK%'
              OR UPPER(campaign_name) LIKE '%THAILAND%'
              OR UPPER(campaign_name) LIKE '%ICAD%'
              OR UPPER(campaign_name) LIKE '%SEA%'
              OR UPPER(campaign_name) LIKE '%AMWC SOUTH%'
              OR UPPER(campaign_name) LIKE '%SOUTHEAST ASIA%'
              OR UPPER(campaign_name) LIKE '%SOUTHEASTASIA%'

              THEN 'AMWC SEA - ICAD'

            WHEN UPPER(campaign_name) LIKE '%LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWCLATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC-LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC LATAM%'
              OR UPPER(campaign_name) LIKE '%AMWC LATIN AMERICA%'
              OR UPPER(campaign_name) LIKE '%LATIN AMERICA%'
              OR UPPER(campaign_name) LIKE '%AMWC MEDELLIN%'

              THEN 'AMWC LATAM'

            -- 2. SPECIFIC CONFERENCES (Order matters for TAS vs TAS UK)
            -- "TAS UK" maps to FACE, so check this BEFORE checking for generic "TAS"
            WHEN UPPER(campaign_name) LIKE '%FACE%'
              OR UPPER(campaign_name) LIKE '%FACIAL AESTHETIC%'
              OR UPPER(campaign_name) LIKE '%TAS UK%'
              OR UPPER(campaign_name) LIKE '%TASUK%'
              OR UPPER(campaign_name) LIKE '%TAS-UK%'
              OR UPPER(campaign_name) LIKE '%THE AESTHETIC SHOW UK%'

              THEN 'FACE Conference'

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
            -- This acts as a catch-all for "AMWC" only if it didn't match the regions above
            WHEN UPPER(campaign_name) LIKE '%MONACO%'
              OR UPPER(campaign_name) LIKE '%MONTE CARLO%'
              OR UPPER(campaign_name) LIKE '%AMWC%'

              THEN 'AMWC Conference'

            WHEN UPPER(campaign_name) LIKE '%IM AESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IMAESTHETICS%'
              OR UPPER(campaign_name) LIKE '%IM-AESTHETICS%'

              THEN 'IM AESTHETICS'

            ELSE 'Other/Unmapped'

        END as event_name,

        CAST(campaign_id AS STRING) as campaign_id,
        campaign_name,

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
)

SELECT * FROM renamed