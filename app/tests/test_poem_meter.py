from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT pm.idpoem, pm.idmeter
        FROM data.poem_meter pm
    """)
    pg_set = {(str(idpoem), str(idmeter)) for idpoem, idmeter in pg_cursor.fetchall()}

    sqlite_set = set()

    sqlite_cursor.execute("SELECT type_id, metre_id FROM type_metre")
    sqlite_set.update((str(type_id), str(metre_id)) for type_id, metre_id in sqlite_cursor.fetchall())

    sqlite_cursor.execute("SELECT occurrence_id, metre_id FROM occurrence_metre")
    sqlite_set.update((str(occurrence_id), str(metre_id)) for occurrence_id, metre_id in sqlite_cursor.fetchall())

    pg_conn.close()
    sqlite_conn.close()

    return compare_sets(
        "poem_meter",
        sqlite_set,
        pg_set,
        label="poem_id + metre_id"
    )