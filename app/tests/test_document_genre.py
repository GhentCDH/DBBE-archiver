from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # -------------------------
    # PostgreSQL source set
    # -------------------------
    pg_cursor.execute("""
        SELECT iddocument, idgenre
        FROM data.document_genre
    """)
    pg_set = {
        (str(row[0]), str(row[1]))
        for row in pg_cursor.fetchall()
    }

    # -------------------------
    # SQLite: type_genre
    # -------------------------
    sqlite_cursor.execute("""
        SELECT type_id, genre_id
        FROM type_genre
    """)
    type_set = {
        (str(row[0]), str(row[1]))
        for row in sqlite_cursor.fetchall()
    }

    # -------------------------
    # SQLite: occurrence_genre
    # -------------------------
    sqlite_cursor.execute("""
        SELECT occurrence_id, genre_id
        FROM occurrence_genre
    """)
    occurrence_set = {
        (str(row[0]), str(row[1]))
        for row in sqlite_cursor.fetchall()
    }

    # -------------------------
    # SQLite: manuscript_content
    # manuscript_id == iddocument
    # content_id == idgenre
    # -------------------------
    sqlite_cursor.execute("""
        SELECT manuscript_id, content_id
        FROM manuscript_content
    """)
    manuscript_set = {
        (str(row[0]), str(row[1]))
        for row in sqlite_cursor.fetchall()
    }

    # Union of all SQLite coverage tables
    sqlite_set = type_set | occurrence_set | manuscript_set

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "document_genre coverage (type ∪ occurrence ∪ manuscript_content)",
        sqlite_set,
        pg_set,
        label="Document ID + Genre ID"
    )