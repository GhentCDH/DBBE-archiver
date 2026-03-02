from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # -------------------------
    # PostgreSQL: poem identities
    # -------------------------
    pg_cursor.execute("""
        SELECT identity
        FROM data.poem
    """)
    pg_set = {
        str(row[0])
        for row in pg_cursor.fetchall()
    }

    # -------------------------
    # SQLite: type IDs
    # -------------------------
    sqlite_cursor.execute("SELECT id FROM type")
    type_set = {
        str(row[0])
        for row in sqlite_cursor.fetchall()
    }

    # -------------------------
    # SQLite: occurrence IDs
    # -------------------------
    sqlite_cursor.execute("SELECT id FROM occurrence")
    occurrence_set = {
        str(row[0])
        for row in sqlite_cursor.fetchall()
    }

    sqlite_set = type_set | occurrence_set

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "poem identity coverage (type ∪ occurrence)",
        sqlite_set,
        pg_set,
        label="Poem Identity"
    )