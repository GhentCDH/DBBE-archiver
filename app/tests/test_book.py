from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, year, publisher, editor, idcluster, idseries, forthcoming
        FROM data.book
    """)
    pg_set = {
        (
            str(row[0]),
            row[1],
            normalize_string(row[2]) if row[2] else None,
            normalize_string(row[3]) if row[3] else None,
            str(row[4]) if row[4] else None,
            str(row[5]) if row[5] else None,
            bool(row[6])
        )
        for row in pg_cursor.fetchall()
    }

    sqlite_cursor.execute("""
        SELECT id, year, publisher, editor, idcluster, idseries, forthcoming
        FROM book
    """)
    sqlite_set = {
        (
            str(row[0]),
            row[1],
            normalize_string(row[2]) if row[2] else None,
            normalize_string(row[3]) if row[3] else None,
            str(row[4]) if row[4] else None,
            str(row[5]) if row[5] else None,
            bool(row[6])
        )
        for row in sqlite_cursor.fetchall()
    }

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "book table coverage (shared columns)",
        sqlite_set,
        pg_set,
        label="ID + Year + Publisher + Editor + idcluster + idseries + Forthcoming"
    )