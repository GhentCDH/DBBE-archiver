from db import get_sqlite_connection, get_postgres_connection
from tabulate import tabulate

def normalize_datetime(value):
    if value is None:
        return None
    return str(value).replace("+00:00", "").strip()

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, url, last_accessed
        FROM data.online_source
    """)
    pg_rows = pg_cursor.fetchall()

    pg_data = {
        str(identity): {
            "url": url,
            "last_accessed": normalize_datetime(last_accessed)
        }
        for identity, url, last_accessed in pg_rows
    }

    sqlite_cursor.execute("""
        SELECT id, url, last_accessed
        FROM online_source
    """)
    sqlite_rows = sqlite_cursor.fetchall()

    sqlite_data = {
        str(identity): {
            "url": url,
            "last_accessed": normalize_datetime(last_accessed)
        }
        for identity, url, last_accessed in sqlite_rows
    }

    sqlite_conn.close()
    pg_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg):
        differences.append([identity, "Missing in SQLite", "", "", ""])

    for identity in sorted(only_in_sqlite):
        differences.append([identity, "", "Missing in Postgres", "", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys())):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            pg_row["url"] != sqlite_row["url"] or
            pg_row["last_accessed"] != sqlite_row["last_accessed"]
        ):
            differences.append([
                identity,
                pg_row["url"],
                sqlite_row["url"],
                pg_row["last_accessed"],
                sqlite_row["last_accessed"]
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG URL",
            "SQLite URL",
            "PG Last Accessed",
            "SQLite Last Accessed"
        ],
        tablefmt="grid"
    )

    return False, table