from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT DISTINCT di.system_name
        FROM data.identifier di
        WHERE di.system_name IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM data.global_id gi
              WHERE gi.idauthority = ANY(di.ids)
          )
    """)
    pg_set = set(str(row[0]) for row in pg_cursor.fetchall())

    sqlite_cursor.execute("""
        SELECT DISTINCT catalogue
        FROM identification
        WHERE catalogue IS NOT NULL
    """)
    sqlite_set = set(str(row[0]) for row in sqlite_cursor.fetchall())

    sqlite_conn.close()
    pg_conn.close()

    return compare_sets(
        "identification catalogue coverage",
        sqlite_set,
        pg_set,
        label="system_name / catalogue"
    )