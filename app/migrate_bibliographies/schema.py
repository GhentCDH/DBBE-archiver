# app/migrate_bibliographies/schema.py
from ..common import get_db_connection, add_column_if_missing
from .biblio_type import BiblioType

def create_schema():
    conn, cursor = get_db_connection()

    for bib_type in BiblioType:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {bib_type.value} (
                id TEXT PRIMARY KEY,
                title TEXT,
                title_sort_key TEXT
            )
        """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal (
            id TEXT PRIMARY KEY,
            title TEXT,
            title_sort_key TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_issue (
            id TEXT PRIMARY KEY,
            journal_id TEXT,
            title TEXT,
            title_sort_key TEXT
        )
    """)

    # Add foreign key columns if missing
    add_column_if_missing(cursor, "book_chapter", "book_id", "TEXT")
    add_column_if_missing(cursor, "article", "journal_issue_id", "TEXT")

    conn.commit()
    conn.close()
