from db import get_sqlite_connection, get_postgres_connection
from tabulate import tabulate


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT idlanguage, name, code, description
        FROM data.language
    """)
    pg_data = {
        str(idlanguage): {
            "name": name,
            "code": code,
            "description": description
        }
        for idlanguage, name, code, description in pg_cursor.fetchall()
    }

    sqlite_cursor.execute("""
        SELECT id, name, code, description
        FROM language
    """)
    sqlite_data = {
        str(row_id): {
            "name": name,
            "code": code,
            "description": description
        }
        for row_id, name, code, description in sqlite_cursor.fetchall()
    }

    pg_conn.close()
    sqlite_conn.close()

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
            pg_row["name"] != sqlite_row["name"] or
            pg_row["code"] != sqlite_row["code"] or
            pg_row["description"] != sqlite_row["description"]
        ):
            differences.append([
                identity,
                pg_row["name"], sqlite_row["name"],
                pg_row["code"], sqlite_row["code"],
                pg_row["description"], sqlite_row["description"],
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG Name", "SQLite Name",
            "PG Code", "SQLite Code",
            "PG Description", "SQLite Description"
        ],
        tablefmt="grid"
    )

    return False, table