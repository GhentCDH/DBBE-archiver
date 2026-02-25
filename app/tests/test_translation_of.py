from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT iddocument, idtranslation
        FROM data.translation_of
    """)
    pg_set = {(str(iddocument), str(idtranslation)) for iddocument, idtranslation in pg_cursor.fetchall()}

    sqlite_cursor.execute("""
        SELECT type_id, id
        FROM type_translation
    """)
    sqlite_set = {(str(type_id), str(translation_id)) for type_id, translation_id in sqlite_cursor.fetchall()}


    pg_conn.close()
    sqlite_conn.close()

    return compare_sets(
        "translation_of",
        pg_set,
        sqlite_set,
        label="type_id + translation_id"
    )