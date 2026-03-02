from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # -------------------------
    # PostgreSQL: library identities
    # -------------------------
    pg_cursor.execute("SELECT identity FROM data.library")
    pg_set = set()
    for row in pg_cursor.fetchall():
        if row[0] is not None:
            pg_set.add(str(int(row[0])))

    # -------------------------
    # SQLite: library IDs
    # -------------------------
    sqlite_cursor.execute("SELECT id FROM library")
    sqlite_set = set()
    for row in sqlite_cursor.fetchall():
        if row[0] is not None:
            sqlite_set.add(str(int(row[0])))

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "library table coverage",
        sqlite_set,
        pg_set,
        label="Library ID"
    )