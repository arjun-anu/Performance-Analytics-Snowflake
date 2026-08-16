-- athlete_id_occurrences / athlete_record_rank are computed once in stg_athletes_clean
-- (single source of truth for dedup) and simply consumed here.
SELECT *
FROM {{ ref('stg_athletes') }}
WHERE athlete_id IS NOT NULL
  AND full_name IS NOT NULL
  AND squad IS NOT NULL
  AND position IS NOT NULL
  AND date_of_birth IS NOT NULL
  AND height_cm IS NOT NULL
  AND height_cm <= 230
  AND height_cm >= 150
  AND athlete_record_rank = 1