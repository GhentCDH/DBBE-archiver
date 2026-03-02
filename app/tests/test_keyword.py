from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_id, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # SQLite: id + name
    sqlite_cursor.execute("SELECT id, name FROM keyword")
    sqlite_rows = {
        (
            normalize_id(row[0]),
            normalize_id(row[1])
        )
        for row in sqlite_cursor.fetchall()
        if row[0] is not None
    }

    # Postgres: identity + keyword
    pg_cursor.execute('SELECT identity, keyword FROM data.keyword')
    pg_rows = {
        (
            normalize_id(row[0]),
            normalize_id(row[1])
        )
        for row in pg_cursor.fetchall()
        if row[0] is not None
    }

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "keywords",
        sqlite_rows,
        pg_rows,
        label="(ID, Name)"
    )