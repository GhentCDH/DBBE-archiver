# app/migrate_bibliographies/schema.py
from app.common import execute_with_normalization, get_db_connection, add_column_if_missing
from .biblio_type_enum import BiblioType

def create_schema():
    conn, cursor = get_db_connection()

    for bib_type in BiblioType:
        execute_with_normalization(cursor, f"""
            CREATE TABLE IF NOT EXISTS {bib_type.value} (
                id INTEGER PRIMARY KEY,
                title TEXT,
                title_sort_key TEXT
            )
        """)

    execute_with_normalization(cursor, """
        CREATE TABLE IF NOT EXISTS journal (
            id INTEGER PRIMARY KEY,
            title TEXT,
            title_sort_key TEXT
        )
    """)

    execute_with_normalization(cursor, """
        CREATE TABLE IF NOT EXISTS journal_issue (
            id INTEGER PRIMARY KEY,
            journal_id INTEGER,
            title TEXT,
            title_sort_key TEXT
        )
    """)

    add_column_if_missing(cursor, "book_chapter", "book_id", "INTEGER")

    conn.commit()
    conn.close()
