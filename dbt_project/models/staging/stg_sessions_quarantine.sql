-- Rows excluded from `stg_sessions` due to normalization/validation failures
-- This quarantine contains normalized session rows that did not pass silver-level
-- validation. Each row includes a `failure_reason` code to simplify downstream triage.
WITH cleaned AS (
    SELECT * FROM {{ ref('stg_sessions') }}
)
-- Select normalized fields plus the reason the row was quarantined.
SELECT
    c.*, -- normalized session fields
    a.athlete_id AS ref_athlete_id,
    CASE
        WHEN LOWER(TRIM(c.raw_session_id)) IN ('n/a','-','null','') OR c.raw_session_id IS NULL THEN 'MISSING_SESSION_ID'
        WHEN c.raw_session_id IS NOT NULL AND NOT REGEXP_LIKE(c.raw_session_id, '^S\\d+$') THEN 'INVALID_SESSION_ID_TOKEN'
        WHEN c.athlete_id IS NULL THEN 'MISSING_ATHLETE_ID'
        WHEN a.athlete_id IS NULL THEN 'UNMAPPED_ATHLETE_ID'
        WHEN c.session_athlete_occurrences > 1 AND c.session_athlete_rank > 1 THEN 'DUPLICATE_SESSION_ATHLETE_ID'
        WHEN c.session_date IS NULL THEN 'INVALID_DATE'
        WHEN c.duration_min IS NULL OR c.duration_min <= 0 OR c.duration_min >= 600 THEN 'OUT_OF_RANGE_DURATION'
        WHEN c.distance_unit IS NULL OR TRIM(c.distance_unit) = '' THEN 'MISSING_DISTANCE_UNIT'
        WHEN LOWER(c.distance_unit) NOT IN ('km','m') THEN 'INVALID_DISTANCE_UNIT'
        WHEN c.rpe IS NULL OR NOT (c.rpe BETWEEN 1 AND 10) THEN 'OUT_OF_RANGE_RPE'
        WHEN c.avg_heart_rate IS NULL OR NOT (c.avg_heart_rate BETWEEN 30 AND 220) THEN 'OUT_OF_RANGE_HEART_RATE'
        WHEN c.max_velocity IS NULL THEN 'MISSING_MAX_VELOCITY'
        WHEN NOT (c.max_velocity BETWEEN 0 AND 40) THEN 'OUT_OF_RANGE_MAX_VELOCITY'
        ELSE 'UNKNOWN'
    END AS failure_reason
FROM cleaned c
LEFT JOIN {{ ref('stg_athletes_clean') }} a
    ON c.athlete_id = a.athlete_id
WHERE (
    LOWER(TRIM(c.raw_session_id)) IN ('n/a','-','null','')
    OR c.raw_session_id IS NULL
    OR NOT REGEXP_LIKE(c.raw_session_id, '^S\\d+$')
    OR c.athlete_id IS NULL
    OR a.athlete_id IS NULL
    OR (c.session_athlete_occurrences > 1 AND c.session_athlete_rank > 1)
    OR c.session_date IS NULL
    OR c.duration_min IS NULL
    OR c.duration_min <= 0
    OR c.duration_min >= 600
    OR c.distance_unit IS NULL
    OR TRIM(c.distance_unit) = ''
    OR LOWER(c.distance_unit) NOT IN ('km','m')
    OR c.max_velocity IS NULL
    OR NOT (c.max_velocity BETWEEN 0 AND 40)
    OR c.avg_heart_rate IS NULL
    OR NOT (c.avg_heart_rate BETWEEN 30 AND 220)
    OR c.rpe IS NULL
    OR NOT (c.rpe BETWEEN 1 AND 10)
)
