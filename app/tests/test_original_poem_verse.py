from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string
from tabulate import tabulate


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT id, idoriginal_poem, idgroup, verse, "order"
        FROM data.original_poem_verse
    """)
    pg_rows = pg_cursor.fetchall()

    pg_data = {
        str(row_id): {
            "idoriginal_poem": str(idoriginal_poem),
            "idgroup": str(idgroup) if idgroup is not None else None,
            "verse": normalize_string(verse),
            "order": order
        }
        for row_id, idoriginal_poem, idgroup, verse, order in pg_rows
    }

    sqlite_cursor.execute("""
        SELECT id, occurrence_id, verse_group_id, text, order_in_occurrence
        FROM verses
    """)
    sqlite_rows = sqlite_cursor.fetchall()

    sqlite_data = {
        str(row_id): {
            "idoriginal_poem": str(occurrence_id),
            "idgroup": str(verse_group_id) if verse_group_id is not None else None,
            "verse": normalize_string(text),
            "order": order_in_occurrence
        }
        for row_id, occurrence_id, verse_group_id, text, order_in_occurrence in sqlite_rows
    }

    sqlite_conn.close()
    pg_conn.close()

    only_in_pg = set(pg_data.keys()) - set(sqlite_data.keys())
    only_in_sqlite = set(sqlite_data.keys()) - set(pg_data.keys())

    differences = []

    for identity in sorted(only_in_pg, key=int):
        differences.append([identity, "Missing in SQLite", "", "", "", "", "", "", ""])

    for identity in sorted(only_in_sqlite, key=int):
        differences.append([identity, "", "Missing in Postgres", "", "", "", "", "", ""])

    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys()), key=int):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            pg_row["idoriginal_poem"] != sqlite_row["idoriginal_poem"] or
            pg_row["idgroup"] != sqlite_row["idgroup"] or
            pg_row["verse"] != sqlite_row["verse"] or
            pg_row["order"] != sqlite_row["order"]
        ):
            differences.append([
                identity,
                pg_row["idoriginal_poem"],
                sqlite_row["idoriginal_poem"],
                pg_row["idgroup"],
                sqlite_row["idgroup"],
                pg_row["verse"],
                sqlite_row["verse"],
                pg_row["order"],
                sqlite_row["order"]
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG Poem ID",
            "SQLite Poem ID",
            "PG Group ID",
            "SQLite Group ID",
            "PG Verse",
            "SQLite Verse",
            "PG Order",
            "SQLite Order"
        ],
        tablefmt="grid"
    )

    return False, table