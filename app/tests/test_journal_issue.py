from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets, normalize_string
from tabulate import tabulate


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, year, number, volume, forthcoming, series, idjournal,
               month, place, publisher, editor, title_abbreviated
        FROM data.journal_issue
    """)
    pg_rows = pg_cursor.fetchall()
    pg_data = {
        str(identity): {
            "year": year,
            "number": normalize_string(str(number)) if number is not None else None,
            "volume": normalize_string(str(volume)) if volume is not None else None,
            "forthcoming": forthcoming,
            "series": normalize_string(series),
            "journal_id": str(idjournal) if idjournal is not None else None,
            "month": month,
            "place": normalize_string(place),
            "publisher": normalize_string(publisher),
            "editor": normalize_string(editor),
            "title_abbreviated": normalize_string(title_abbreviated),
        }
        for identity, year, number, volume, forthcoming, series, idjournal,
            month, place, publisher, editor, title_abbreviated in pg_rows
    }

    sqlite_cursor.execute("""
        SELECT id, year, number, volume, forthcoming, series, journal_id
        FROM journal_issue
    """)
    sqlite_rows = sqlite_cursor.fetchall()
    sqlite_data = {
        str(identity): {
            "year": year,
            "number": normalize_string(str(number)) if number is not None else None,
            "volume": normalize_string(str(volume)) if volume is not None else None,
            "forthcoming": forthcoming,
            "series": normalize_string(series),
            "journal_id": str(journal_id) if journal_id is not None else None,
        }
        for identity, year, number, volume, forthcoming, series, journal_id in sqlite_rows
    }

    sqlite_conn.close()
    pg_conn.close()

    ok, table = compare_sets(
        "journal_issue",
        set(sqlite_data.keys()),
        set(pg_data.keys()),
        label="ID"
    )
    if not ok:
        return False, table

    # Check expected-empty columns in Postgres are indeed always empty
    non_empty_violations = []
    for identity, pg_row in sorted(pg_data.items()):
        for col in ("month", "place", "publisher", "editor", "title_abbreviated"):
            if pg_row[col] not in (None, ""):
                non_empty_violations.append([identity, col, pg_row[col]])

    if non_empty_violations:
        table = tabulate(
            non_empty_violations,
            headers=["ID", "Column", "Unexpected Value"],
            tablefmt="grid"
        )
        return False, f"Expected-empty columns in Postgres contain data:\n{table}"

    # Check non-empty fields match between Postgres and SQLite
    differences = []
    for identity in sorted(set(pg_data.keys()) & set(sqlite_data.keys())):
        pg_row = pg_data[identity]
        sqlite_row = sqlite_data[identity]

        fields = ["year", "number", "volume", "forthcoming", "series", "journal_id"]
        row_diffs = []
        for field in fields:
            pg_val = pg_row[field]
            sqlite_val = sqlite_row[field]
            if pg_val != sqlite_val:
                row_diffs.append((field, pg_val, sqlite_val))

        for field, pg_val, sqlite_val in row_diffs:
            differences.append([identity, field, pg_val, sqlite_val])

    if not differences:
        return True, None

    table = tabulate(
        differences,
        headers=["ID", "Field", "PG Value", "SQLite Value"],
        tablefmt="grid"
    )
    return False, table