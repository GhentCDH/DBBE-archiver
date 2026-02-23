from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets, normalize_string
from tabulate import tabulate


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, paleographical_info, transcription_reviewed
        FROM data.original_poem
    """)
    pg_rows = pg_cursor.fetchall()
    pg_data = {
        str(identity): {
            "paleographical_info": normalize_string(paleographical_info),
            "transcription_reviewed": transcription_reviewed
        }
        for identity, paleographical_info, transcription_reviewed in pg_rows
    }

    sqlite_cursor.execute("""
        SELECT id, palaeographical_info, transcription_reviewed
        FROM occurrence
    """)
    sqlite_rows = sqlite_cursor.fetchall()
    sqlite_data = {
        str(identity): {
            "palaeographical_info": normalize_string(palaeographical_info),
            "transcription_reviewed": transcription_reviewed
        }
        for identity, palaeographical_info, transcription_reviewed in sqlite_rows
    }

    sqlite_conn.close()
    pg_conn.close()

    ok, table = compare_sets(
        "original_poem",
        set(sqlite_data.keys()),
        set(pg_data.keys()),
        label="ID"
    )
    if not ok:
        return False, table

    differences = []
    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys())):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        if (
            pg_row["paleographical_info"] != sqlite_row["palaeographical_info"] or
            pg_row["transcription_reviewed"] != sqlite_row["transcription_reviewed"]
        ):
            differences.append([
                identity,
                pg_row["paleographical_info"],
                sqlite_row["palaeographical_info"],
                pg_row["transcription_reviewed"],
                sqlite_row["transcription_reviewed"]
            ])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=[
            "ID",
            "PG Paleographical Info", "SQLite Palaeographical Info",
            "PG Transcription Reviewed", "SQLite Transcription Reviewed"
        ],
        tablefmt="grid"
    )

    return False, table