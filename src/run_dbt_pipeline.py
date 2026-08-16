import subprocess
import json
import os
from pathlib import Path
from snowflake_conn import get_snowflake_connection
#import shutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DBT_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR   

def run_dbt_and_summarize():
    print("=== Executing dbt Staging Models & Quarantine ===")
    dbt_command = str(DBT_EXECUTABLE) if DBT_EXECUTABLE.exists() else "dbt"

    # Ensure SNOWFLAKE_PRIVATE_KEY_PATH is expanded for subprocesses
    sk_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if sk_path:
        expanded = str(Path(sk_path).expanduser().resolve())
        if not Path(expanded).exists():
            print(f"[ERROR] Private key not found at {expanded}")
            print("Set SNOWFLAKE_PRIVATE_KEY_PATH to the correct path and retry.")
            return
        os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = expanded

    # Run 'dbt build' inside the dbt_project directory
    command = [
        dbt_command,
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ]
    
    # 1. Run dbt models
    run_process = subprocess.run(command, capture_output=True, text=True, env=os.environ)
    print(run_process.stdout)
    if run_process.returncode != 0:
        print(run_process.stderr)

    # 2. Run dbt tests
    #test_process = subprocess.run([command, "test"], capture_output=True, text=True)
    #print(test_process.stdout)

    # 3. Read dbt run_results.json for Programmatic Summary
    # dbt writes artifacts under the project `target/` directory (e.g. dbt_project/target/run_results.json)
    candidate_paths = [
        DBT_PROJECT_DIR / "target" / "run_results.json",
        PROJECT_ROOT / "target" / "run_results.json",
        Path("target") / "run_results.json",
    ]

    run_results_path = None
    for p in candidate_paths:
        if p.exists():
            run_results_path = p
            break

    if run_results_path:
        with open(run_results_path, "r") as f:
            results = json.load(f)
        
        print("\n" + "="*50)
        print("         DBT PROGRAMMATIC VALIDATION SUMMARY       ")
        print("="*50)
        
        passed_tests = 0
        failed_tests = 0
        
        for res in results.get("results", []):
            status = res.get("status")
            node_name = res.get("unique_id")
            # dbt unit tests use a "unit_test." unique_id prefix (not "test."), so this
            # substring check catches both schema/singular tests and unit tests.
            if "test" in node_name:
                if status == "pass":
                    passed_tests += 1
                else:
                    failed_tests += 1
                    print(f"❌ TEST FAILED: {node_name}")

        print(f"Total dbt Validation Tests Executed: {passed_tests + failed_tests}")
        print(f"Tests Passed:                      {passed_tests}")
        print(f"Tests Failed:                      {failed_tests}")
        print("="*50 + "\n")

        # 4. Persist every test result (pass/fail/error/skip) to Snowflake so there's
        # a full history across runs to query -- not just this run's console output,
        # and not just the failing rows that dbt's store_failures config captures.
        persist_test_results(results)
    else:
        checked = ", ".join([str(p) for p in candidate_paths])
        print(f"Warning: run_results.json not found. Checked: {checked}")


def persist_test_results(results):
    """Write every dbt test outcome from a run_results.json payload to Snowflake.

    Creates PERFORMANCE_ANALYTICS.OBSERVABILITY.DBT_TEST_RUNS if it doesn't
    already exist and appends one row per test node for this invocation, so
    results accumulate into a queryable history across runs (both passes and
    failures -- dbt's own `store_failures` config only captures failing rows).
    """
    metadata = results.get("metadata", {})
    invocation_id = metadata.get("invocation_id")
    generated_at = metadata.get("generated_at")

    test_rows = [
        (
            generated_at,
            invocation_id,
            res.get("unique_id"),
            res.get("status"),
            res.get("execution_time"),
            (res.get("message") or "")[:2000],
        )
        for res in results.get("results", [])
        # Covers both schema/singular tests ("test." prefix) and dbt unit tests
        # ("unit_test." prefix).
        if "test" in str(res.get("unique_id", ""))
    ]

    if not test_rows:
        print("No test results found in run_results.json; nothing to persist.")
        return

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("USE WAREHOUSE elt_pipeline_wh;")
        cursor.execute("USE DATABASE performance_analytics;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS OBSERVABILITY;")
        cursor.execute("USE SCHEMA OBSERVABILITY;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DBT_TEST_RUNS (
                run_generated_at TIMESTAMP_NTZ,
                invocation_id VARCHAR,
                test_unique_id VARCHAR,
                status VARCHAR,
                execution_time FLOAT,
                message VARCHAR,
                _logged_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """)
        cursor.executemany(
            """
            INSERT INTO DBT_TEST_RUNS
                (run_generated_at, invocation_id, test_unique_id, status, execution_time, message)
            VALUES (TRY_TO_TIMESTAMP_NTZ(%s), %s, %s, %s, %s, %s)
            """,
            test_rows,
        )
        print(f"Persisted {len(test_rows)} test result(s) to OBSERVABILITY.DBT_TEST_RUNS.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_dbt_and_summarize()