from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    sqlite_cursor.execute("SELECT name FROM acknowledgement")
    sqlite_set = {normalize_string(row[0]) for row in sqlite_cursor.fetchall()}

    pg_cursor.execute('SELECT "acknowledgement" FROM data.acknowledgement')
    pg_set = {normalize_string(row[0]) for row in pg_cursor.fetchall()}

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets("acknowledgements", sqlite_set, pg_set, label="Name")