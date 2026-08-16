-- session_athlete_occurrences / session_athlete_rank are computed once in stg_sessions
-- (single source of truth for dedup, keyed on session_id + athlete_id -- NOT session_id
-- alone, since many athletes legitimately share a session_id) and simply consumed here.
WITH cleaned AS (
    SELECT * FROM {{ ref('stg_sessions') }}
)

-- SELECT c.* (not an explicit column list) so lineage columns
-- (_source_file_name, _source_file_row, _loaded_at) and the dedup metadata
-- flow through -- matching stg_athletes_clean's convention, and needed by
-- the clean/quarantine overlap checks in the README's QUARANTINE Validation
-- Deep Dive section.
SELECT c.*
FROM cleaned c
JOIN {{ ref('stg_athletes_clean') }} a
    ON c.athlete_id = a.athlete_id
WHERE c.session_id IS NOT NULL
    AND REGEXP_LIKE(c.session_id, '^S\\d+$')
    AND c.session_date IS NOT NULL
    AND c.duration_min IS NOT NULL
    AND c.duration_min > 0
    AND c.duration_min < 600
    AND c.distance_unit IS NOT NULL
    AND TRIM(c.distance_unit) != ''
    AND LOWER(c.distance_unit) IN ('km', 'm')
    AND c.max_velocity IS NOT NULL
    AND c.max_velocity BETWEEN 0 AND 40
    AND c.avg_heart_rate IS NOT NULL
    AND c.avg_heart_rate BETWEEN 30 AND 220
    AND c.rpe IS NOT NULL
    AND c.rpe BETWEEN 1 AND 10
    AND c.session_athlete_rank = 1