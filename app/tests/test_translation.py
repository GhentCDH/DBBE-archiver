from db import get_sqlite_connection, get_postgres_connection
from tabulate import tabulate


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT identity, idlanguage
        FROM data.translation
    """)
    pg_data = {
        str(identity): str(idlanguage) if idlanguage else None
        for identity, idlanguage in pg_cursor.fetchall()
    }

    sqlite_cursor.execute("""
        SELECT id, language_id
        FROM type_translation
    """)
    sqlite_data = {
        str(translation_id): str(language_id) if language_id else None
        for translation_id, language_id in sqlite_cursor.fetchall()
    }

    pg_conn.close()
    sqlite_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg, key=int):
        differences.append([identity, "Missing in SQLite", "", ""])

    for identity in sorted(only_in_sqlite, key=int):
        differences.append([identity, "", "Missing in Postgres", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys()), key=int):
        if pg_data[identity] != sqlite_data[identity]:
            differences.append([identity, pg_data[identity], sqlite_data[identity], "Language mismatch"])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=["Translation ID", "PG Language ID", "SQLite Language ID", "Note"],
        tablefmt="grid"
    )

    return False, table