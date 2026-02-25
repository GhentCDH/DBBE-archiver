from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string
from tabulate import tabulate


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT COUNT(*)
        FROM data.name
        WHERE self_designations IS NOT NULL
           OR idtransliterationsystem IS NOT NULL
           OR middle_name IS NOT NULL
           OR extra IS NOT NULL
    """)
    non_null_count = pg_cursor.fetchone()[0]
    if non_null_count > 0:
        print(f"WARNING: data.name has {non_null_count} rows with non-null values in previously empty columns (self_designations, idtransliterationsystem, middle_name, extra). Consider migrating these.")

    pg_cursor.execute("""
        SELECT idperson, first_name, last_name
        FROM data.name
    """)
    pg_data = {
        str(idperson): {
            "first_name": normalize_string(first_name),
            "last_name": normalize_string(last_name),
        }
        for idperson, first_name, last_name in pg_cursor.fetchall()
    }

    sqlite_cursor.execute("""
        SELECT id, first_name, last_name
        FROM person
    """)
    sqlite_data = {
        str(row_id): {
            "first_name": normalize_string(first_name),
            "last_name": normalize_string(last_name),
        }
        for row_id, first_name, last_name in sqlite_cursor.fetchall()
    }

    pg_conn.close()
    sqlite_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg, key=int):
        differences.append([identity, "Missing in SQLite", "", "", ""])

    for identity in sorted(only_in_sqlite, key=int):
        differences.append([identity, "", "Missing in Postgres", "", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys()), key=int):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            pg_row["first_name"] != sqlite_row["first_name"] or
            pg_row["last_name"] != sqlite_row["last_name"]
        ):
            differences.append([
                identity,
                pg_row["first_name"], sqlite_row["first_name"],
                pg_row["last_name"], sqlite_row["last_name"],
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG First Name", "SQLite First Name",
            "PG Last Name", "SQLite Last Name",
        ],
        tablefmt="grid"
    )

    return False, table