-- Mart: clean_sessions
-- Purpose: produce one canonical row per session (silver->gold boundary) enriched
-- with athlete attributes. This model is materialized as a table for downstream
-- analytics and reporting. Keep joins as LEFT so sessions without athletes can
-- still be inspected, but the ETL pipeline should triage these into quarantine.
{{ config(materialized='table') }}

WITH sessions AS (
    SELECT *
    FROM {{ ref('stg_sessions_clean') }}
),
athletes AS (
    SELECT athlete_id, full_name, squad, position
    FROM {{ ref('stg_athletes_clean') }}
)

SELECT
    -- Surrogate primary key: session_id alone isn't unique (many athletes can
    -- share a session_id), so the real key -- and this hash -- is (session_id, athlete_id).
    MD5(s.session_id || '-' || s.athlete_id) AS session_pk,
    s.session_id,
    s.athlete_id,
    -- derive session_date from recorded_at when missing
    COALESCE(s.session_date, CAST(s.recorded_at AS DATE)) AS session_date,
    s.session_type,
    s.duration_min,
    s.distance_m,
    s.max_velocity,
    s.avg_heart_rate,
    s.rpe,
    s.recorded_at,
    a.full_name AS athlete_full_name,
    a.squad,
    a.position
FROM sessions s
LEFT JOIN athletes a
  ON s.athlete_id = a.athlete_id
