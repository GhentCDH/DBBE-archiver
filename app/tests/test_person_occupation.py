from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT idperson, idoccupation
        FROM data.person_occupation
    """)
    pg_set = {(str(idperson), str(idoccupation)) for idperson, idoccupation in pg_cursor.fetchall()}

    sqlite_cursor.execute("""
        SELECT person_id, office_id
        FROM person_office
    """)
    sqlite_set = {(str(person_id), str(office_id)) for person_id, office_id in sqlite_cursor.fetchall()}

    pg_conn.close()
    sqlite_conn.close()

    return compare_sets(
        "person_occupation",
        sqlite_set,
        pg_set,
        label="person_id + office_id"
    )