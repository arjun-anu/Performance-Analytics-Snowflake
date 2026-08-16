"""
step1_load.py
----------------
Landing script (bronze ingestion) that:
 - Connects to Snowflake using `get_snowflake_connection()`.
 - Creates an internal stage for CSV uploads.
 - PUTs local CSVs into the stage and COPYs them into RAW tables.

Notes:
 - Raw tables use VARCHAR for all source columns to preserve original payloads.
 - Meta-columns `_source_file_name`, `_source_file_row`, and `_loaded_at` provide traceability.
 - COPY uses `ON_ERROR='CONTINUE'` to avoid failing the entire load; quarantined rows are handled downstream in dbt.
"""

import os
from pathlib import Path
from snowflake_conn import get_snowflake_connection


def run_query(cursor, query):
    """Execute a SQL statement with minimal logging.

    Parameters
    - cursor: Snowflake cursor
    - query: SQL string to execute
    """
    # Print the first token for lightweight logging (CREATE/COPY/PUT)
    print(f"Executing: {query.strip().split(maxsplit=1)[0]}...")
    cursor.execute(query)


def main():
    """Primary entrypoint for the load script.

    Responsibilities:
    - Establish a secure Snowflake connection via `src/snowflake_conn.py`.
    - Create staging resources and load raw CSVs into traceable RAW tables.
    - Leave data cleansing and quarantining to dbt staging models.
    """
    # 1. Connect to Snowflake using centralized connection helper
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Ensure context (warehouse / database / schema) for load operations
    cursor.execute("USE WAREHOUSE elt_pipeline_wh;")
    cursor.execute("USE DATABASE performance_analytics;")
    # Load into the BRONZE schema as the canonical landing layer
    cursor.execute("CREATE SCHEMA IF NOT EXISTS BRONZE;")
    cursor.execute("USE SCHEMA BRONZE;")

    try:
        # 2. Create an Internal Stage to hold the files
        run_query(cursor, "CREATE OR REPLACE STAGE bronze_landing_stage FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);")

        # 3. Upload (PUT) the sessions CSV file from the local data/ folder
        # Note: path is absolute here for reliability in CI; make this configurable if needed
        run_query(cursor, "PUT 'file:///Users/arjun/Desktop/AthleteSessionAssessment/data/sessions.csv' @bronze_landing_stage auto_compress=true;")
        
        # 4. Upload (PUT) the athletes CSV file
        run_query(cursor, "PUT 'file:///Users/arjun/Desktop/AthleteSessionAssessment/data/athletes.csv' @bronze_landing_stage auto_compress=true;")

        # 5. Create RAW_SESSIONS Table
        create_sessions_table = """
        CREATE OR REPLACE TABLE RAW_SESSIONS (
            session_id VARCHAR,
            athlete_id VARCHAR,
            session_date VARCHAR,
            session_type VARCHAR,
            duration_min VARCHAR,
            distance VARCHAR,
            distance_unit VARCHAR,
            max_velocity VARCHAR,
            avg_heart_rate VARCHAR,
            rpe VARCHAR,
            recorded_at VARCHAR,
            
            -- Meta-columns for traceability
            _source_file_name VARCHAR,
            _source_file_row NUMBER,
            _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        run_query(cursor, create_sessions_table)

        # 6. COPY INTO RAW_SESSIONS
        # We intentionally map file columns to raw VARCHAR columns and preserve file-level metadata.
        copy_sessions = """
        COPY INTO RAW_SESSIONS (
            session_id, athlete_id, session_date, session_type, duration_min, 
            distance, distance_unit, max_velocity, avg_heart_rate, rpe, recorded_at,
            _source_file_name, _source_file_row
        )
        FROM (
            SELECT 
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                METADATA$FILENAME, 
                METADATA$FILE_ROW_NUMBER
            FROM @bronze_landing_stage/sessions.csv.gz
        )
        ON_ERROR = 'CONTINUE'; 
        """
        run_query(cursor, copy_sessions)

        # 7. Create RAW_ATHLETES Table (6 source columns)
        create_athletes_table = """
        CREATE OR REPLACE TABLE RAW_ATHLETES (
            athlete_id VARCHAR,
            full_name VARCHAR,
            squad VARCHAR,
            position VARCHAR,
            date_of_birth VARCHAR,
            height VARCHAR,
            
            -- Meta-columns for traceability
            _source_file_name VARCHAR,
            _source_file_row NUMBER,
            _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        run_query(cursor, create_athletes_table)

        # 8. COPY INTO RAW_ATHLETES
        copy_athletes = """
        COPY INTO RAW_ATHLETES (
            athlete_id, full_name, squad, position, date_of_birth, height,
            _source_file_name, _source_file_row
        )
        FROM (
            SELECT 
                $1, $2, $3, $4, $5, $6,
                METADATA$FILENAME, 
                METADATA$FILE_ROW_NUMBER
            FROM @bronze_landing_stage/athletes.csv.gz
        )
        ON_ERROR = 'CONTINUE'; 
        """
        run_query(cursor, copy_athletes)

        print("Step 1 Complete: All raw files landed securely with full traceability.")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()