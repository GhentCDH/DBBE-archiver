from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    sqlite_cursor.execute("SELECT id, lemma FROM type")
    sqlite_set = {
        (str(row[0]), normalize_string(row[1]))
        for row in sqlite_cursor.fetchall()
        if row[1] is not None
    }

    pg_cursor.execute("""
        SELECT id_reconstructed_poem, lemma
        FROM data.reconstructed_poem_lemma
    """)
    pg_set = {
        (str(row[0]), normalize_string(row[1]))
        for row in pg_cursor.fetchall()
        if row[1] is not None
    }

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "type.lemma",
        sqlite_set,
        pg_set,
        label="ID + Lemma"
    )