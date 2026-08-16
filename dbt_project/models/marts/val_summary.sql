-- Mart: val_summary
-- Purpose: run-level validation/observability summary across the athlete and
-- session pipelines, computed fresh every dbt run. This model is materialized
-- as a table -- a full CREATE OR REPLACE each run (the GOLD default), so it
-- reflects a single point-in-time snapshot, not an append-only history. For
-- a historical log of every dbt test's pass/fail across runs, see
-- src/run_dbt_pipeline.py's PERFORMANCE_ANALYTICS.OBSERVABILITY.DBT_TEST_RUNS
-- table instead.
{{ config(materialized='table') }}

WITH session_counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_sessions') }}) AS raw_sessions_count,
        (SELECT COUNT(*) FROM {{ ref('stg_sessions_clean') }}) AS clean_sessions_count,
        (SELECT COUNT(*) FROM {{ ref('stg_sessions_quarantine') }}) AS quarantined_sessions_count,
        -- Sessions quarantined specifically because their athlete_id didn't map to
        -- a valid clean athlete (as opposed to failing some other rule).
        (
            SELECT COUNT(*)
            FROM {{ ref('stg_sessions_quarantine') }}
            WHERE failure_reason = 'UNMAPPED_ATHLETE_ID'
        ) AS unjoined_unmapped_sessions_count
),
athlete_counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_athletes') }}) AS raw_athletes_count,
        (SELECT COUNT(*) FROM {{ ref('stg_athletes_clean') }}) AS clean_athletes_count,
        (SELECT COUNT(*) FROM {{ ref('stg_athletes_quarantine') }}) AS quarantined_athletes_count
)

SELECT
    s.raw_sessions_count,
    s.clean_sessions_count,
    s.quarantined_sessions_count,
    s.unjoined_unmapped_sessions_count,
    a.raw_athletes_count,
    a.clean_athletes_count,
    a.quarantined_athletes_count,
    CURRENT_TIMESTAMP() AS summary_generated_at
FROM session_counts s
CROSS JOIN athlete_counts a
