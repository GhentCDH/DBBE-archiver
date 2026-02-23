from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # SQLite
    sqlite_cursor.execute("SELECT id, name FROM self_designation")
    sqlite_set = {
        (row[0], normalize_string(row[1]))
        for row in sqlite_cursor.fetchall()
    }

    pg_cursor.execute('SELECT id, name FROM data.self_designation')
    pg_set = {
        (row[0], normalize_string(row[1]))
        for row in pg_cursor.fetchall()
    }

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets("self_designatioin", sqlite_set, pg_set, label="ID + Name")