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
- `_SOURCE_FILE_NAME`
- `_SOURCE_FILE_ROW`
- `_LOADED_AT`

Validation check implemented in run output:
- `scripts/validate_raw_counts.py` ensures local file row counts equal RAW table row counts.


### 3) Validation strategy: fail vs quarantine

Decision: Quarantine Record-Level Issues (Non-fatal): Bad rows (e.g., unparseable heart rate, range violations, or missing foreign keys) are isolated into a quarantine with a clear error reason. Clean records continue down the pipeline into gold schemas.

Implemented behavior:
- dbt tests fail pipeline for critical model contracts (for example unique/not-null keys in curated models).
- `stg_sessions_quarantine` captures invalid session rows with `FAILURE_REASON`.
- `stg_athletes_quarantine` captures invalid/duplicate athlete rows with `FAILURE_REASON`.
- `stg_sessions_clean` holds records passing explicit filters.
- `stg_athletes_clean` holds records passing explicit filters.
- `val_summary` publishes run-time validation metrics as table output.

Why this split:
- Prevents silent corruption in published outputs.
- Preserves problematic rows for review instead of dropping them invisibly.


### 4) Clean joined table, grain, and row identity

Final table:
- `PERFORMANCE_ANALYTICS.GOLD.CLEAN_SESSIONS`

Grain:
- One row per accepted clean session record with a validated mapped athlete. 

Row identifier:
  - (`session_id`, `athlete_id`)

Why not trust vendor ID alone:
- Vendor identifiers can repeat or be inconsistent.
- We confirmed uniqueness via dbt test on `session_pk` after strengthening key composition.

Deduplication key: `PARTITION BY session_id, athlete_id` (not `session_id` alone)

Decision: dedupe session rows on the composite `(session_id, athlete_id)` pair,
computed once in `stg_sessions.sql` and reused everywhere downstream, instead
of on `session_id` by itself.

Why `session_id` alone doesn't work:
- A single session (e.g. a squad training day) legitimately has one row per
  attending athlete -- many rows sharing a `session_id` is normal, not a
  duplicate. Partitioning on `session_id` alone would have collapsed every
  athlete at a session down to a single surviving row, silently discarding
  everyone else who attended.
- `athlete_id` alone isn't sufficient either -- one athlete attends many
  sessions. The pair together is the actual natural key: at most one valid
  row should exist per `(session_id, athlete_id)` combination.

Implementation (`stg_sessions.sql`):
- `session_athlete_occurrences = COUNT(*) OVER (PARTITION BY session_id, athlete_id)`
  -- how many raw rows share this exact `(session_id, athlete_id)` pair.
- `session_athlete_rank = ROW_NUMBER() OVER (PARTITION BY session_id, athlete_id ORDER BY recorded_at DESC, _source_file_row)`
  -- orders rows within each pair, most-recently-recorded row first.
  `_source_file_row` is the tiebreaker for ties (or both-`NULL` `recorded_at`
  values) -- deliberately not `_loaded_at`, which is a run-time wall-clock
  value that would make "which row wins" non-reproducible across two
  otherwise-identical pipeline runs.
- Both columns are computed exactly once and consumed by both downstream
  models rather than each recomputing its own version:
  - `stg_sessions_clean.sql` keeps only `session_athlete_rank = 1`.
  - `stg_sessions_quarantine.sql` flags `session_athlete_occurrences > 1 AND session_athlete_rank > 1`
    as `DUPLICATE_SESSION_ATHLETE_ID`.
  - Because both read the same precomputed columns, "who wins" can never
    disagree between the clean and quarantine outputs.

Why this avoids duplication without false positives:
- True duplicates (same athlete, same session, resubmitted/re-recorded)
  collapse to one surviving row; the rest are quarantined with an explicit,
  auditable reason instead of silently vanishing.
- Legitimate multi-athlete sessions are never flagged as duplicates, since
  each athlete's rows fall into a different partition.
- The deterministic tiebreak means a from-scratch rerun of the pipeline
  picks the same winning row every time.


### 5) Business rules adopted in staging

Athlete staging decisions:
- Normalize `athlete_id` using trim + leading-zero removal to align joins.
- Canonicalize `squad` and `position` into consistent controlled values.
- Parse `date_of_birth` using multiple expected formats.
- Normalize `height` so centimeter-like values (`1.00-2.60`) are converted to centimeters.
- Deduplicate by `athlete_id` using deterministic rank (`_loaded_at`, `_source_row_number`, `full_name`) and keep rank `1` as the clean candidate.
- Require `full_name`, `squad`, `position`, `date_of_birth`, and `height_cm` for clean; quarantine failures with explicit reasons.

Session staging decisions:
- Normalize identifiers (`session_id`, `athlete_id`) via trim and leading-zero removal for athlete IDs.
- Parse `session_date` from epoch milliseconds/seconds and string date formats.
- Canonicalize `session_type` values to a standard set.
- Enforce clean-session rules: valid non-token session ID, mapped athlete, valid date, duration `(0, 600]`, valid distance unit (`M`), RPE `[0,10]` when present, heart rate `[30,220]` when present, and positive non-null max velocity.
- Route failures to quarantine with explicit reason codes 

### 6) Expected rows vs actual rows

Expectation logic:
- RAW sessions expected = source sessions row count (after header), validated by script.
- Clean sessions expected = rows passing validation rules in `stg_sessions_clean`.
- Quarantine expected = rows failing validation rules in `stg_sessions_quarantine`.

Latest reference run metrics:
- RAW sessions: `36458`
- Clean sessions: `29060`
- Quarantined sessions: `7398`
- Unjoined unmapped sessions: `2459`
- GOLD fact row count: `28730`

Athlete reference counts from latest run:
- RAW athletes: `86`
- Clean athletes: `75`
- Quarantined athletes: `11`

Difference explanation:
- `raw_sessions_count != clean_sessions_count` because validation explicitly excludes invalid rows from clean output.
- `clean_sessions_count + quarantined_sessions_count = raw_sessions_count` via reconciliation tests. (This test has only been written as an explicit sql test, and not internalized into the many enforcing dbt tests)

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
Full walkthrough with worked examples: README "Future-Stage Architecture (Snowflake Scale Strategy)".

Recommended approach:
1. Ingest/load: split large files into balanced chunks and run parallel `COPY`
   for higher throughput; keep RAW immutable with lineage columns attached.
2. Use an incremental dbt model strategy for fact growth:
   - Avoid full rebuilds as volume scales.
   - Process only new/changed slices by load timestamp/date boundary, plus
     periodic full reconciliations to catch drift.
3. Cluster the fact table on `(squad, session_date)` -- squad first, not
   session_date first:
   - `squad` is the equality-filtered, lower-cardinality dimension; putting
     it first co-locates each squad's rows, and `session_date` as the
     second key still gets useful range-pruning within that.
   - This is not free -- reclustering has ongoing credit cost, so apply it
     only after query-profile evidence (not by default) shows a clear
     scan-byte or latency benefit.
4. Add a narrow serving table or dynamic table for the hot access pattern
   (a recent-window aggregate feeding morning dashboards specifically),
   rather than serving that traffic off the full fact table.
5. Warehouse sizing/concurrency: right-size the virtual warehouse for the
   morning burst, with auto-suspend/auto-resume so idle time isn't billed.
6. Observe and tune from evidence, not assumptions: track query-profile
   partition-pruning percentage and bytes scanned; adjust clustering key
   order based on observed predicate selectivity.

Why this works:
- It aligns physical layout with the dominant filters (one squad + a 28-day
  range), minimizing scanned bytes and giving consistent latency.
- It complements observability-first operations, where run metadata and
  quality issue metrics are queryable by run, file, and issue type -- see
  the "At-Scale Validation and Observability" example schema in the README
  (`run_id`, `pipeline_name`, per-file `rows_received`/`rows_rejected`,
  `error_message`). **Not yet implemented in this repo as a general
  ingestion-level table** -- what does exist today is narrower:
  `OBSERVABILITY.DBT_TEST_RUNS` (via `src/run_dbt_pipeline.py`) persists
  per-run dbt *test* pass/fail history, and `GOLD.VAL_SUMMARY` gives a
  query-time row-count snapshot. Neither captures file-level `COPY INTO`
  load statistics (rows parsed/loaded/rejected per file) -- that data is
  fetched by Snowflake on every load but currently discarded, not persisted.


### 10) What I would do with another week

- Add Unit tests for athletes data
- Add persistent run-level observability tables (`run_id`, file-level counts, status, error metadata). Although row-level lineage is captured (`_source_file`, `_source_row_number`, `_loaded_at`), run-level metadata (`run_id`, file-level status/error tracking) is not yet persisted as first-class observability tables.
- Introduce richer domain rules (for example stricter optional-field expectations and domain-specific anomaly thresholds).
- Expand quarantine taxonomy and add remediation workflows.
- Add CI orchestration (GitHub Actions) running pipeline tests automatically.
- Add export automation for final mart CSV and evidence pack generation.
- Would add these parameters to clean_sessions `session_date`, `recorded_at`, `_source_file`, `_source_row_number` to make a Row identifier: `session_pk`, a deterministic MD5 hash over business + lineage fields:
 `session_id`, `athlete_id`, `session_date`, `recorded_at`, `_source_file`, `_source_row_number`
- Explore visualizations of the data using streamlit UI in Snowflake



### 11) Known limitations / uncertainties


- Business definitions for some thresholds/rules should still be signed off by domain stakeholders.
- Clustering/search optimization in serving layers is future-state and should be enabled only after workload-based measurement.
