{{ config(
    materialized='view',
    tags=['silver', 'global', 'daily']
) }}

WITH emea_apac AS (
    -- 1. EMEA/APAC/LATAM (Automated Source)
    -- Already has clean DATE types.
    SELECT
        'EMEA/LATAM/APAC' as region,
        EXTRACT(YEAR FROM event_start_date) as year,
        conference_editions,
        event_start_date,
        event_end_date,

        -- Pricing Tiers
        seb_start_date,
        seb_end_date,
        eb_start_date,
        eb_end_date,

        -- EMEA doesn't have "Advance", so we set to NULL
        CAST(NULL AS DATE) as advance_start_date,
        CAST(NULL AS DATE) as advance_end_date,

        -- Map "Full Price" to global FP columns
        fp_start_date,
        fp_end_date,

        onsite_start_date,
        onsite_end_date

    FROM {{ source('marketing_raw', 'event_dates_emea_latm_apac') }}
),

north_america AS (
    -- 2. NORTH AMERICA (Manual Source)
    -- Needs parsing from String -> Date
    SELECT
        'North America' as region,
        EXTRACT(YEAR FROM SAFE_CAST(LEFT(event_start_date, 10) AS DATE)) as year,
        conference_editions,

        -- Parse Event Dates
        SAFE_CAST(LEFT(event_start_date, 10) AS DATE) as event_start_date,
        SAFE_CAST(LEFT(event_end_date, 10) AS DATE) as event_end_date,

        -- Parse Pricing Tiers
        -- Note: NA might not have 'seb_start', only 'seb_end'. We map what exists.
        SAFE_CAST(LEFT(seb_end_date, 10) AS DATE) as seb_end_date,

        -- Logic: If SEB Start is missing, we assume it started way back (optional)
        -- or leave NULL. Here we map NULL since it's not in the sheet.
        CAST(NULL AS DATE) as seb_start_date,

        SAFE_CAST(LEFT(eb_start_date, 10) AS DATE) as eb_start_date,
        SAFE_CAST(LEFT(eb_end_date, 10) AS DATE) as eb_end_date,

        SAFE_CAST(LEFT(advance_start_date, 10) AS DATE) as advance_start_date,
        SAFE_CAST(LEFT(advance_end_date, 10) AS DATE) as advance_end_date,

        -- MAP 'Standard' (NA) -> 'Full Price' (Global)
        SAFE_CAST(LEFT(standard_start_date, 10) AS DATE) as fp_start_date,
        SAFE_CAST(LEFT(standard_end_date, 10) AS DATE) as fp_end_date,

        -- NA doesn't have specific "Onsite" dates in the sheet (usually same as Event Date)
        -- We default Onsite = Event Start/End
        SAFE_CAST(LEFT(event_start_date, 10) AS DATE) as onsite_start_date,
        SAFE_CAST(LEFT(event_end_date, 10) AS DATE) as onsite_end_date

    FROM {{ source('marketing_raw', 'event_dates_north_america') }}

    -- FILTER: Remove the header row from the CSV upload
    WHERE conference_editions <> 'conference_editions'
)

-- 3. UNIFY
SELECT * FROM emea_apac
UNION ALL
SELECT * FROM north_america