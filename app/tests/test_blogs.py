# tests/test_blogs.py
from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_id, normalize_string, compare_sets

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # ----------------------------
    # SQLITE SIDE
    # ----------------------------
    sqlite_cursor.execute("SELECT id, url FROM blog")
    sqlite_set = {(normalize_id(row[0]), normalize_string(row[1])) for row in sqlite_cursor.fetchall()}

    # ----------------------------
    # POSTGRES SIDE
    # ----------------------------
    pg_cursor.execute('SELECT identity, url FROM data.blog')
    pg_set = {(normalize_id(row[0]), normalize_string(row[1])) for row in pg_cursor.fetchall()}

    # ----------------------------
    # Close connections
    # ----------------------------
    sqlite_conn.close()
    pg_conn.close()

    # ----------------------------
    # Compare
    # ----------------------------
    return compare_sets("blogs", sqlite_set, pg_set, label="Blog ID / URL")