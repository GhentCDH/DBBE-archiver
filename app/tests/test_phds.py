from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string
from tabulate import tabulate


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, city, year, institution, volume, forthcoming
        FROM data.phd
    """)
    pg_rows = pg_cursor.fetchall()

    pg_data = {
        str(identity): {
            "city": normalize_string(city),
            "year": year,
            "institution": institution,
            "volume": volume,
            "forthcoming": forthcoming
        }
        for identity, city, year, institution, volume, forthcoming in pg_rows
    }

    sqlite_cursor.execute("""
        SELECT id, city, year, phd_institution, volume, forthcoming
        FROM phd
    """)
    sqlite_rows = sqlite_cursor.fetchall()

    sqlite_data = {}
    for identity, city_fk, year, institution, volume, forthcoming in sqlite_rows:
        identity_str = str(identity)

        sqlite_cursor.execute(
            "SELECT name FROM location WHERE id = ?",
            (city_fk,)
        )
        result = sqlite_cursor.fetchone()
        city_name = normalize_string(result[0]) if result else ''

        sqlite_data[identity_str] = {
            "city": city_name,
            "year": year,
            "phd_institution": institution,
            "volume": volume,
            "forthcoming": forthcoming
        }

    sqlite_conn.close()
    pg_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg):
        differences.append([identity, "Missing in SQLite", "", "", "", "", "", "", ""])

    for identity in sorted(only_in_sqlite):
        differences.append([identity, "", "Missing in Postgres", "", "", "", "", "", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys())):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            pg_row["city"] != sqlite_row["city"] or
            pg_row["year"] != sqlite_row["year"] or
            pg_row["institution"] != sqlite_row["phd_institution"] or
            pg_row["volume"] != sqlite_row["volume"] or
            pg_row["forthcoming"] != sqlite_row["forthcoming"]
        ):
            differences.append([
                identity,
                pg_row["city"],
                sqlite_row["city"],
                pg_row["year"],
                sqlite_row["year"],
                pg_row["institution"],
                sqlite_row["phd_institution"],
                pg_row["volume"],
                sqlite_row["volume"],
                pg_row["forthcoming"],
                sqlite_row["forthcoming"]
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG City",
            "SQLite City",
            "PG Year",
            "SQLite Year",
            "PG Institution",
            "SQLite Institution",
            "PG Volume",
            "SQLite Volume",
            "PG Forthcoming",
            "SQLite Forthcoming"
        ],
        tablefmt="grid"
    )

    return False, table