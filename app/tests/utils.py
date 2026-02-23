# tests/utils.py

import unicodedata
from tabulate import tabulate


def normalize_string(value):
    if value is None:
        return ''
    return unicodedata.normalize('NFC', value).strip().lower().replace('\u200b', '')


def normalize_id(value):
    return str(value).strip() if value is not None else ''


def compare_sets(name, sqlite_set, pg_set, label="Value"):
    only_in_sqlite = sqlite_set - pg_set
    only_in_pg = pg_set - sqlite_set

    if not only_in_sqlite and not only_in_pg:
        return True, None

    rows = []
    for value in sorted(only_in_sqlite | only_in_pg):
        rows.append([
            value,
            '✓' if value in sqlite_set else '',
            '✓' if value in pg_set else ''
        ])

    table = tabulate(
        rows,
        headers=[label, 'SQLite', 'Postgres'],
        tablefmt='grid'
    )

    return False, table