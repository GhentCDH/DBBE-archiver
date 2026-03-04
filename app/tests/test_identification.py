from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("SELECT DISTINCT system_name FROM data.identifier WHERE system_name IS NOT NULL")
    pg_set = set(str(row[0]) for row in pg_cursor.fetchall())

    sqlite_cursor.execute("SELECT DISTINCT type FROM identification WHERE type IS NOT NULL")
    sqlite_set = set(str(row[0]) for row in sqlite_cursor.fetchall())

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "identification type coverage",
        sqlite_set,
        pg_set,
        label="system_name / type"
    )