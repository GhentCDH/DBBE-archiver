from db import get_sqlite_connection, get_postgres_connection
from tabulate import tabulate

import unicodedata

def normalize_text(value):
    if value is None:
        return None
    return unicodedata.normalize('NFC', value.replace('\r\n', '\n').replace('\r', '\n').strip())
def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, critical_apparatus
        FROM data.reconstructed_poem
    """)
    pg_data = {
        str(identity): normalize_text(critical_apparatus)
        for identity, critical_apparatus in pg_cursor.fetchall()
    }
    sqlite_cursor.execute("""
        SELECT id, critical_apparatus
        FROM type
    """)
    sqlite_data = {
        str(row_id): normalize_text(critical_apparatus)
        for row_id, critical_apparatus in sqlite_cursor.fetchall()
    }
    sqlite_conn.close()
    pg_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg, key=int):
        differences.append([identity, "Missing in SQLite", ""])

    for identity in sorted(only_in_sqlite, key=int):
        differences.append([identity, "", "Missing in Postgres"])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys()), key=int):
        if pg_data[identity] != sqlite_data[identity]:
            differences.append([
                identity,
                pg_data[identity],
                sqlite_data[identity]
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=["ID", "PG Critical Apparatus", "SQLite Critical Apparatus"],
        tablefmt="grid"
    )

    return False, table