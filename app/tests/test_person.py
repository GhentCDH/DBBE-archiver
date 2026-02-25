from db import get_sqlite_connection, get_postgres_connection
from tabulate import tabulate


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM data.person
        WHERE email IS NOT NULL
    """)
    non_null_email_count = pg_cursor.fetchone()[0]
    if non_null_email_count > 0:
        print(f"WARNING: data.person.email is no longer always null — {non_null_email_count} non-null values found. Consider adding email column to SQLite.")

    pg_cursor.execute("""
        SELECT identity, is_historical, is_modern, is_dbbe
        FROM data.person
    """)
    pg_data = {
        str(identity): {
            "is_historical": is_historical,
            "is_modern": is_modern,
            "is_dbbe": is_dbbe,
        }
        for identity, is_historical, is_modern, is_dbbe in pg_cursor.fetchall()
    }

    sqlite_cursor.execute("""
        SELECT id, is_historical_person, is_modern_person, is_dbbe_person
        FROM person
    """)
    sqlite_data = {
        str(row_id): {
            "is_historical": is_historical,
            "is_modern": is_modern,
            "is_dbbe": is_dbbe,
        }
        for row_id, is_historical, is_modern, is_dbbe in sqlite_cursor.fetchall()
    }

    sqlite_conn.close()
    pg_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg, key=int):
        differences.append([identity, "Missing in SQLite", "", "", "", "", ""])

    for identity in sorted(only_in_sqlite, key=int):
        differences.append([identity, "", "Missing in Postgres", "", "", "", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys()), key=int):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            bool(pg_row["is_historical"]) != bool(sqlite_row["is_historical"]) or
            bool(pg_row["is_modern"]) != bool(sqlite_row["is_modern"]) or
            bool(pg_row["is_dbbe"]) != bool(sqlite_row["is_dbbe"])
        ):
            differences.append([
                identity,
                pg_row["is_historical"], sqlite_row["is_historical"],
                pg_row["is_modern"], sqlite_row["is_modern"],
                pg_row["is_dbbe"], sqlite_row["is_dbbe"],
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG is_historical", "SQLite is_historical",
            "PG is_modern", "SQLite is_modern",
            "PG is_dbbe", "SQLite is_dbbe",
        ],
        tablefmt="grid"
    )

    return False, table