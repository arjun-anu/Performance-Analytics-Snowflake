## DECISIONS.md

### 1) What was wrong with the data, and how it was found

The dataset intentionally contains real-world quality issues. We identified these via:
- direct CSV inspection,
- RAW-to-local row count checks,
- dbt staging tests,
- quarantine model outputs.

Issues observed:
- Mixed date formats in both athletes and sessions (`YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MON-YYYY`).
- Epoch timestamps in `session_date` (for example `1778630400000`) requiring numeric-date parsing.
- Inconsistent categorical values (for example `session_type`, squad/position casing variants).
- Duplicate athlete IDs in source (failed `unique` test in staging before dedupe).
- Session rows with validation failures (missing IDs, invalid dates, out-of-range duration/RPE/heart-rate, invalid distance unit, missing max velocity).
- Athlete rows with missing critical attributes (for example position/height).
- Mixed height units in athlete source data (cm and m representations).


### 2) Landing strategy and RAW design decisions

Decision: Land both source files unchanged in RAW as VARCHAR, and attach lineage metadata.

Rationale:
- Prevents early load failures from malformed values.
- Preserves source truth for traceability and debugging.
- Supports strict reproducibility.

RAW lineage fields:
- `_SOURCE_FILE`
- `_SOURCE_ROW_NUMBER`
- `_LOADED_AT`

Validation check implemented in run output:
- `scripts/validate_raw_counts.py` ensures local file row counts equal RAW table row counts.


### 3) Validation strategy: fail vs quarantine

Decision: Fail for structural integrity issues in curated layers; quarantine record-level business quality failures.

Implemented behavior:
- dbt tests fail pipeline for critical model contracts (for example unique/not-null keys in curated models).
- `stg_sessions_quarantine` captures invalid session rows with `quarantine_reason`.
- `stg_athletes_quarantine` captures invalid/duplicate athlete rows with `quarantine_reason`.
- `stg_sessions_clean` holds records passing explicit filters.
- `stg_athletes_clean` holds records passing explicit filters.
- `val_summary` publishes run-time validation metrics as table output.

Why this split:
- Prevents silent corruption in published outputs.
- Preserves problematic rows for review instead of dropping them invisibly.


### 4) Clean joined table, grain, and row identity

Final table:
- `SPORTS_ANALYTICS_DEV.GOLD.FCT_ATHLETE_SESSIONS`

Grain:
- One row per accepted clean session record with a validated mapped athlete.

Row identifier:
- `session_pk`, a deterministic MD5 hash over business + lineage fields:
  - `session_id`, `athlete_id`, `session_date`, `recorded_at`, `_source_file`, `_source_row_number`

Why not trust vendor ID alone:
- Vendor identifiers can repeat or be inconsistent.
- We confirmed uniqueness via dbt test on `session_pk` after strengthening key composition.


### 5) Business rules adopted in staging

Athlete staging decisions:
- Normalize `athlete_id` using trim + leading-zero removal to align joins.
- Canonicalize `squad` and `position` into consistent controlled values.
- Parse `date_of_birth` using multiple expected formats.
- Normalize `height` so centimeter-like values (`100-260`) are converted to meters.
- Deduplicate by `athlete_id` using deterministic rank (`_loaded_at`, `_source_row_number`, `full_name`) and keep rank `1` as the clean candidate.
- Require `full_name`, `squad`, `position`, `date_of_birth`, and `height_m` for clean; quarantine failures with explicit reasons.

Session staging decisions:
- Normalize identifiers (`session_id`, `athlete_id`) via trim and leading-zero removal for athlete IDs.
- Parse `session_date` from epoch milliseconds/seconds and string date formats.
- Canonicalize `session_type` values to a standard set.
- Enforce clean-session rules: valid non-token session ID, mapped athlete, valid date, duration `(0, 600]`, valid distance unit (`M`/`KM`), RPE `[0,10]` when present, heart rate `[30,240]` when present, and positive non-null max velocity.
- Route failures to quarantine with explicit reason codes (for example `UNMAPPED_ATHLETE_ID`, `INVALID_DISTANCE_UNIT`, `OUT_OF_RANGE_RPE`, `MISSING_MAX_VELOCITY`).


### 6) Expected rows vs actual rows

Expectation logic:
- RAW sessions expected = source sessions row count (after header), validated by script.
- Clean sessions expected = rows passing validation rules in `stg_sessions_clean`.
- Quarantine expected = rows failing validation rules in `stg_sessions_quarantine`.

Latest reference run metrics:
- RAW sessions: `36458`
- Clean sessions: `28730`
- Quarantined sessions: `7728`
- Unjoined clean sessions: `0`
- GOLD fact row count: `28730`

Athlete reference counts from latest run:
- Staged athletes: `86`
- Clean athletes: `72`
- Quarantined athletes: `14`

Difference explanation:
- `raw_sessions_count != clean_sessions_count` because validation explicitly excludes invalid rows from clean output.
- `clean_sessions_count + quarantined_sessions_count = raw_sessions_count` via reconciliation tests.
- Unmapped athlete sessions are quarantined (not carried into clean/GOLD), which drives `unjoined_unmapped_sessions_count` to zero.


### 7) How unclean/non-join records are surfaced

- Invalid sessions are retained in `QUARANTINE.STG_SESSIONS_QUARANTINE` with explicit reason codes.
- Invalid or duplicate athletes are retained in `QUARANTINE.STG_ATHLETES_QUARANTINE` with explicit reason codes.
- Unmapped session-to-athlete joins are treated as a quality failure (`UNMAPPED_ATHLETE_ID`) and quarantined.
- `GOLD.VAL_SUMMARY` reports row counts and `unjoined_unmapped_sessions_count`.


### 8) Repeatability choices

- RAW ingest truncates and reloads from source files each run.
- dbt build is deterministic for fixed inputs.
- Pipeline was run repeatedly from clean-state logic with consistent pass behavior.


### 9) Scaling answer (hundreds of millions, morning query for one squad over last 28 days)

Snowflake has no indexes, so performance comes from storage layout, pruning, and serving patterns.

Recommended approach:
1. Keep fact table physically optimized for filter predicates:
   - Cluster by `(session_date, squad)` or `(squad, session_date)` depending on cardinality and query pattern.
   - This improves micro-partition pruning for squad + recent date windows.
2. Use incremental model strategy in dbt for fact growth:
   - Avoid full rebuilds as volume scales.
   - Insert only new/changed slices by load timestamp/date boundary.
3. Add a narrow serving table or dynamic table for the hot access pattern:
   - Materialize a recent-window aggregate or filtered table used by morning dashboards.
4. Monitor and tune:
   - Track query profile for partition pruning percentage and scan bytes.
   - Adjust clustering key order based on observed predicate selectivity.
5. Warehouse sizing/concurrency:
   - Right-size virtual warehouse for morning burst and use auto-suspend/auto-resume.

Why this works:
- It aligns physical layout with dominant filters (squad + 28-day range), minimizing scanned data and improving consistent latency.
- It complements observability-first operations, where run metadata and quality issue metrics are queryable by run, file, and issue type.


### 10) What I would do with another week

- Add full dbt docs and source freshness checks.
- Add persistent run-level observability tables (`run_id`, file-level counts, status, error metadata).
- Introduce richer domain rules (for example stricter optional-field expectations and domain-specific anomaly thresholds).
- Expand quarantine taxonomy and add remediation workflows.
- Add CI orchestration (GitHub Actions) running pipeline tests automatically.
- Add export automation for final mart CSV and evidence pack generation.


### 11) Known limitations / uncertainties

- Although row-level lineage is captured (`_source_file`, `_source_row_number`, `_loaded_at`), run-level metadata (`run_id`, file-level status/error tracking) is not yet persisted as first-class observability tables.
- Business definitions for some thresholds/rules should still be signed off by domain stakeholders.
- Clustering/search optimization in serving layers is future-state and should be enabled only after workload-based measurement.
