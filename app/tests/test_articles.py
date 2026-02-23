from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_id, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    sqlite_cursor.execute("SELECT id FROM article")
    sqlite_set = {normalize_id(row[0]) for row in sqlite_cursor.fetchall()}

    pg_cursor.execute('SELECT "identity" FROM data.article')
    pg_set = {normalize_id(row[0]) for row in pg_cursor.fetchall()}

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets("articles", sqlite_set, pg_set, label="Article ID")