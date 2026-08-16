-- Materialized as a view (not ephemeral): this typed layer is referenced by two
-- downstream models (stg_sessions_clean and stg_sessions_quarantine), and dbt's
-- unit-test fixture resolution has trouble disambiguating an ephemeral ref with
-- multiple consumers -- see unit_tests_sessions_quarantine.yml.
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_landing', 'RAW_SESSIONS') }}
),
typed AS (
SELECT
    CASE
        WHEN LOWER(TRIM(session_id)) IN ('n/a', '-', 'null', 'n/a', '') THEN NULL
        ELSE TRIM(session_id)
    END AS session_id,
    TRY_TO_NUMBER(NULLIF(TRIM(athlete_id), '')) AS athlete_id,
    CASE
        WHEN REGEXP_LIKE(TRIM(session_date), '^[0-9]{13}$') THEN
            TO_DATE(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(TRIM(session_date)) / 1000))
        WHEN REGEXP_LIKE(TRIM(session_date), '^[0-9]{10}$') THEN
            TO_DATE(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(TRIM(session_date))))
        ELSE
            COALESCE(
                TRY_TO_DATE(TRIM(session_date), 'YYYY-MM-DD'),
                TRY_TO_DATE(TRIM(session_date), 'DD-MON-YYYY'),
                TRY_TO_DATE(TRIM(session_date), 'DD/MM/YYYY')
            )
    END AS session_date,
    CASE 
        WHEN UPPER(TRIM(session_type)) IN ('SKL', 'SKILLS','SKILL SESSION') THEN 'Skills'
        WHEN UPPER(TRIM(session_type)) IN ('REC', 'RECOV', 'RECOVERY') THEN 'Recovery'
        WHEN UPPER(TRIM(session_type)) IN ('TRN', 'TRAIN', 'TRAINING') THEN 'Training'
        WHEN UPPER(TRIM(session_type)) IN ('MTC', 'MATCH', 'MATCH DAY') THEN 'Match'
        ELSE INITCAP(TRIM(session_type))
    END AS session_type,
    TRY_TO_NUMBER(NULLIF(TRIM(duration_min), '')) AS duration_min,
    CASE 
        WHEN LOWER(TRIM(distance_unit)) = 'km' OR (distance_unit IS NULL AND TRY_TO_DECIMAL(NULLIF(TRIM(distance), ''), 10, 2) < 50) 
            THEN TRY_TO_DECIMAL(NULLIF(TRIM(distance), ''), 10, 2) * 1000
        ELSE TRY_TO_DECIMAL(NULLIF(TRIM(distance), ''), 10, 2)
    END AS distance_m,
    TRY_TO_DECIMAL(NULLIF(TRIM(max_velocity), ''), 5, 2) AS max_velocity,
    TRY_TO_NUMBER(NULLIF(TRIM(avg_heart_rate), '')) AS avg_heart_rate,
    TRY_TO_NUMBER(NULLIF(TRIM(rpe), '')) AS rpe,
    TRY_TO_TIMESTAMP(NULLIF(TRIM(recorded_at), '')) AS recorded_at,
    TRIM(session_id) AS raw_session_id,
    TRIM(distance_unit) AS distance_unit,
    _source_file_name,
    _source_file_row,
    _loaded_at
FROM source
)

SELECT
    typed.*,
    -- Uniqueness is NOT on session_id alone -- many athletes legitimately share the
    -- same session_id (a squad training/match session has one row per attending
    -- athlete). The real key is (session_id, athlete_id).
    COUNT(*) OVER (PARTITION BY session_id, athlete_id) AS session_athlete_occurrences,
    -- Ranked by recorded_at DESC then _source_file_row (NOT _loaded_at, for the same
    -- determinism reason as stg_athletes: _loaded_at is a wall-clock value that
    -- differs between runs). Rank 1 is the row stg_sessions_clean keeps; occurrences > 1
    -- with rank > 1 is what stg_sessions_quarantine flags as DUPLICATE_SESSION_ATHLETE_ID.
    ROW_NUMBER() OVER (
        PARTITION BY session_id, athlete_id
        ORDER BY recorded_at DESC, _source_file_row
    ) AS session_athlete_rank
FROM typed
