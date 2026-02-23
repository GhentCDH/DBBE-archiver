from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT idperson, idself_designation
        FROM data.person_self_designation
    """)
    pg_rows = pg_cursor.fetchall()
    pg_data = {(str(idperson), str(idself_designation)) for idperson, idself_designation in pg_rows}

    sqlite_cursor.execute("""
        SELECT person_id, self_designation_id
        FROM person_self_designation
    """)
    sqlite_rows = sqlite_cursor.fetchall()
    sqlite_data = {(str(person_id), str(self_designation_id)) for person_id, self_designation_id in sqlite_rows}

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "person_self_designation",
        sqlite_data,
        pg_data,
        label="(Person ID, Self Designation ID)"
    )