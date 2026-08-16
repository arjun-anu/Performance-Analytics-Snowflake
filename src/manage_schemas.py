"""
manage_schemas.py

Safe helper to list and optionally drop schemas in the configured Snowflake database.
Run with `python src/manage_schemas.py --list` to list schemas.
Run with `python src/manage_schemas.py --drop RAW_GOLD RAW_SILVER raw_QUARANTINE` to drop specific schemas (will prompt).

This script uses the existing `src/snowflake_conn.py` connection helper.
"""
import argparse
from snowflake_conn import get_snowflake_connection


def list_schemas(cursor):
    cursor.execute("SHOW SCHEMAS IN DATABASE PERFORMANCE_ANALYTICS;")
    return cursor.fetchall()


def drop_schema(cursor, schema_name):
    # Use fully-qualified schema name to avoid "no current database" errors
    cursor.execute(f"DROP SCHEMA IF EXISTS PERFORMANCE_ANALYTICS.{schema_name} CASCADE;")


def main():
    parser = argparse.ArgumentParser(description='List or drop schemas in PERFORMANCE_ANALYTICS')
    parser.add_argument('--list', action='store_true', help='List schemas')
    parser.add_argument('--drop', nargs='+', help='Schema names to drop (confirmation required)')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt (use with caution)')
    args = parser.parse_args()

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        if args.list:
            rows = list_schemas(cur)
            print('Schemas in PERFORMANCE_ANALYTICS:')
            for r in rows:
                print(' -', r[1])
            return

        if args.drop:
            print('Requested drops:', args.drop)
            if not args.yes:
                confirm = input('Type DROP to confirm permanent deletion of these schemas: ')
                if confirm != 'DROP':
                    print('Aborted by user.')
                    return
            for s in args.drop:
                print('Dropping', s)
                drop_schema(cur, s)
            print('Done.')
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
