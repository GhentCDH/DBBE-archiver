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

    add_column_if_missing(cursor, "book_chapter", "book_id", "INTEGER")
    add_column_if_missing(cursor, "bib_varia", "year", "INTEGER")
    add_column_if_missing(cursor, "bib_varia", "city", "INTEGER")
    add_column_if_missing(cursor, "bib_varia", "bib_varia_institution", "TEXT")
    add_column_if_missing(cursor, "blog_post", "post_date", "TEXT")
    add_column_if_missing(cursor, "blog_post", "url", "TEXT")
    add_column_if_missing(cursor, "blog_post", "blog", "INTEGER")
    add_column_if_missing(cursor, "blog", "url", "INTEGER")
    add_column_if_missing(cursor, "book", "year", "INTEGER")
    add_column_if_missing(cursor, "book", "publisher", "TEXT")
    add_column_if_missing(cursor, "book", "editor", "TEXT")
    add_column_if_missing(cursor, "book", "forthcoming", "BOOLEAN")
    add_column_if_missing(cursor, "book", "idcluster", "INTEGER")
    add_column_if_missing(cursor, "book", "idseries", "INTEGER")
    add_column_if_missing(cursor, "phd", "year", "INTEGER")
    add_column_if_missing(cursor, "phd", "city", "INTEGER")
    add_column_if_missing(cursor, "phd", "phd_institution", "TEXT")
    add_column_if_missing(cursor, "phd", "volume", "TEXT")
    add_column_if_missing(cursor, "phd", "forthcoming", "BOOLEAN")
    add_column_if_missing(cursor, "online_source", "url", "TEXT")
    add_column_if_missing(cursor, "online_source", "last_accessed", "TEXT")


    conn.commit()
    conn.close()
