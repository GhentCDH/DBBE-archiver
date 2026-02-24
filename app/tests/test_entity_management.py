from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets

MANAGEMENT_TABLES = [
    ("article_management", "article_id"),
    ("bib_varia_management", "bib_varia_id"),
    ("blog_management", "blog_id"),
    ("blog_post_management", "blog_post_id"),
    ("book_chapter_management", "book_chapter_id"),
    ("book_management", "book_id"),
    ("manuscript_management", "manuscript_id"),
    ("occurrence_management", "occurrence_id"),
    ("online_source_management", "online_source_id"),
    ("person_management", "person_id"),
    ("phd_management", "phd_id"),
    ("type_management", "type_id"),
    ("journal_management", "journal_id"),

]

def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    pg_cursor.execute("""
        SELECT identity, idmanagement
        FROM data.entity_management
    """)
    pg_set = {(int(identity), int(idmanagement)) for identity, idmanagement in pg_cursor.fetchall()}

    sqlite_set = set()
    for table, entity_col in MANAGEMENT_TABLES:
        sqlite_cursor.execute(f"SELECT {entity_col}, management_id FROM {table}")
        for entity_id, management_id in sqlite_cursor.fetchall():
            if entity_id is not None and management_id is not None:
                sqlite_set.add((int(entity_id), int(management_id)))


    pg_cursor.close()
    pg_conn.close()
    sqlite_cursor.close()
    sqlite_conn.close()

    return compare_sets(
        "entity_management",
        sqlite_set,
        pg_set,
        label="entity_id + management_id"
    )