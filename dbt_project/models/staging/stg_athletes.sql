-- Materialized as a view (not ephemeral): this typed layer is referenced by two
-- downstream models (stg_athletes_clean and stg_athletes_quarantine). Kept
-- consistent with stg_sessions -- see that file for why ephemeral is avoided
-- here (dbt's unit-test fixture resolution has trouble with an ephemeral ref
-- that has multiple consumers).
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_landing', 'RAW_ATHLETES') }}
),
typed AS (
    SELECT
        TRIM(athlete_id) AS raw_athlete_id,
        TRY_TO_NUMBER(NULLIF(TRIM(athlete_id), '')) AS athlete_id,
        INITCAP(TRIM(full_name)) AS full_name,
        CASE
            WHEN UPPER(TRIM(squad)) IN ('SNR', 'SENIOR', 'SENIOR LIST') THEN 'Senior'
            WHEN UPPER(TRIM(squad)) IN ('DEV LIST', 'DEVELOPMENT','DEV') THEN 'Development'
            WHEN UPPER(TRIM(squad)) IN ('REHAB', 'REHABILITATION', 'REHAB LIST') THEN 'Rehab'
            ELSE 'Other'
        END AS squad,
        CASE
            WHEN UPPER(TRIM(position)) IN ('MID', 'MIDFIELD') THEN 'Midfield'
            WHEN UPPER(TRIM(position)) IN ('FWD', 'FORWARD') THEN 'Forward'
            WHEN UPPER(TRIM(position)) IN ('DEF', 'DEFENDER', 'BACK') THEN 'Defender'
            WHEN UPPER(TRIM(position)) IN ('RUC', 'RUCK', 'RUCKMAN') THEN 'Ruckman'
            ELSE INITCAP(TRIM(position))
        END AS position,
        COALESCE(
            TRY_TO_DATE(date_of_birth, 'YYYY-MM-DD'),
            TRY_TO_DATE(date_of_birth, 'DD/MM/YYYY'),
            TRY_TO_DATE(date_of_birth, 'DD-Mon-YYYY')
        ) AS date_of_birth,
        CASE
            WHEN TRY_TO_DECIMAL(NULLIF(TRIM(height), ''), 5, 2) < 3.0 THEN TRY_TO_DECIMAL(NULLIF(TRIM(height), ''), 5, 2) * 100
            ELSE TRY_TO_DECIMAL(NULLIF(TRIM(height), ''), 5, 2)
        END AS height_cm,
        _source_file_name,
        _source_file_row,
        _loaded_at
    FROM source
)

SELECT
    typed.*,
    COUNT(*) OVER (PARTITION BY athlete_id) AS athlete_id_occurrences,
    -- Ranked by date_of_birth then _source_file_row (NOT _loaded_at) so repeated runs
    -- against unchanged source data always pick the same surviving row. _loaded_at is a
    -- wall-clock value that differs between runs, which would make "which duplicate wins"
    -- nondeterministic if used as the sort key.
    ROW_NUMBER() OVER (
        PARTITION BY athlete_id
        ORDER BY date_of_birth, _source_file_row
    ) AS athlete_record_rank
FROM typed
