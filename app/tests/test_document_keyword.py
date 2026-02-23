from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT iddocument, idkeyword
        FROM data.document_keyword
    """)
    pg_rows = pg_cursor.fetchall()
    pg_set = {(iddocument, idkeyword) for iddocument, idkeyword in pg_rows}

    sqlite_set = set()

    sqlite_cursor.execute("SELECT keyword_id, occurrence_id FROM occurrence_keyword")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    sqlite_cursor.execute("SELECT keyword_id, type_id FROM type_keyword")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    pg_cursor.close()
    pg_conn.close()
    sqlite_cursor.close()
    sqlite_conn.close()

    return compare_sets(
        "document_keyword",
        sqlite_set,
        pg_set,
        label="(document_id, keyword_id)"
    )