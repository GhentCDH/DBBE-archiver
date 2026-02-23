from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    # Connect to DBs
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    # --- Fetch Postgres rows ---
    pg_cursor.execute("""
        SELECT iddocument, idacknowledgement
        FROM data.document_acknowledgement
    """)
    pg_rows = pg_cursor.fetchall()

    # Build Postgres set: just tuples of (iddocument, ack_id)
    pg_set = {(iddocument, idack) for iddocument, idack in pg_rows}

    # --- Fetch SQLite join tables ---
    sqlite_set = set()

    sqlite_cursor.execute("SELECT acknowledgement_id, manuscript_id FROM manuscript_acknowledgement")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    sqlite_cursor.execute("SELECT acknowledgement_id, occurrence_id FROM occurrence_acknowledgement")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    sqlite_cursor.execute("SELECT acknowledgement_id, person_id FROM person_acknowledgement")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    sqlite_cursor.execute("SELECT acknowledgement_id, type_id FROM type_acknowledgement")
    sqlite_set.update((row[1], row[0]) for row in sqlite_cursor.fetchall() if row[1] is not None)

    # Close connections
    pg_cursor.close()
    pg_conn.close()
    sqlite_cursor.close()
    sqlite_conn.close()

    return compare_sets(
        "document_acknowledgement",
        sqlite_set,
        pg_set,
        label="ID + ack_id"
    )