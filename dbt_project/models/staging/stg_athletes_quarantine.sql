-- Quarantine for RAW_ATHLETES with explicit failure_reason codes
-- Purpose: capture athlete rows that failed silver-level validation.
-- Each row has `raw_athlete_id`, normalized `athlete_id` (if parseable),
-- duplicate-detection fields, and a `failure_reason` to help triage upstream data issues.
-- athlete_id_occurrences / athlete_record_rank come from stg_athletes (the typed
-- layer, single source of truth for dedup) so "duplicate" here always agrees with
-- what stg_athletes_clean.sql keeps as the rank-1 row.
WITH cleaned AS (
    SELECT * FROM {{ ref('stg_athletes') }}
)

SELECT
    c.*,
    CASE
        WHEN c.raw_athlete_id IS NULL OR TRIM(c.raw_athlete_id) = '' THEN 'MISSING_ATHLETE_ID'
        WHEN c.athlete_id IS NULL THEN 'MISSING_ATHLETE_ID'
        WHEN c.athlete_id_occurrences > 1 AND c.athlete_record_rank > 1 THEN 'DUPLICATE_ATHLETE_ID'
        WHEN c.full_name IS NULL THEN 'MISSING_FULL_NAME'
        WHEN c.squad IS NULL THEN 'MISSING_SQUAD'
        WHEN c.position IS NULL THEN 'MISSING_POSITION'
        WHEN c.date_of_birth IS NULL THEN 'INVALID_DATE_OF_BIRTH'
        WHEN c.height_cm IS NULL THEN 'MISSING_HEIGHT'
        WHEN c.height_cm < 150 OR c.height_cm > 230 THEN 'OUT_OF_RANGE_HEIGHT'
        ELSE 'OTHER_DQ_FAILURE'
    END AS failure_reason
FROM cleaned c
WHERE c.raw_athlete_id IS NULL
   OR TRIM(c.raw_athlete_id) = ''
   OR c.athlete_id IS NULL
   OR (c.athlete_id_occurrences > 1 AND c.athlete_record_rank > 1)
   OR c.full_name IS NULL
   OR c.squad IS NULL
   OR c.position IS NULL
   OR c.date_of_birth IS NULL
   OR c.height_cm IS NULL
   OR c.height_cm < 150
   OR c.height_cm > 230
