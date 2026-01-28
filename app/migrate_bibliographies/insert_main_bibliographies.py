# app/migrate_bibliographies/insert_main_bibliographies.py
from ..common import get_db_connection, get_postgres_connection
from .biblio_type_enum import BiblioType
from .biblio_entity_enum import BiblioEntity
import sqlite3

# Map Postgres entity type strings → BiblioEntity enum
POSTGRES_TYPE_TO_ENTITY = {
    "manuscript": BiblioEntity.MANUSCRIPT,
    "occurrence": BiblioEntity.OCCURRENCE,
    "type": BiblioEntity.TYPE,
    "person": BiblioEntity.PERSON,
}

def migrate_main_bibliographies():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT identity, 'article' AS bib_type FROM data.article
        UNION ALL
        SELECT identity, 'blog_post' FROM data.blog_post
        UNION ALL
        SELECT identity, 'book' FROM data.book
        UNION ALL
        SELECT identity, 'book_chapter' FROM data.bookchapter
        UNION ALL
        SELECT identity, 'online_source' FROM data.online_source
        UNION ALL
        SELECT identity, 'phd' FROM data.phd
        UNION ALL
        SELECT identity, 'bib_varia' FROM data.bib_varia
    """)


    for source_id, bib_type in pg_cursor.fetchall():
        bib_type_enum = next((bt for bt in BiblioType if bt.value == bib_type), None)
        if bib_type_enum:
            cursor.execute(
                f"INSERT OR IGNORE INTO {bib_type_enum.value} (id) VALUES (?)",
                (str(source_id),)
            )

    pg_cursor.execute("""
        SELECT
            r.idsource AS biblio_id,
            r.idtarget AS entity_id,
            COALESCE(
                m.type::text,
                o.type::text,
                t.type::text,
                p.type::text,
                tr.type::text
            ) AS entity_type,
            r.page_start,
            r.page_end
        FROM data.reference r
        LEFT JOIN (SELECT identity AS entity_id, 'manuscript' AS type FROM data.manuscript) m
            ON r.idtarget = m.entity_id
        LEFT JOIN (SELECT identity AS entity_id, 'occurrence' AS type FROM data.original_poem) o
            ON r.idtarget = o.entity_id
        LEFT JOIN (SELECT identity AS entity_id, 'type' AS type FROM data.reconstructed_poem) t
            ON r.idtarget = t.entity_id
        LEFT JOIN (SELECT identity AS entity_id, 'person' AS type FROM data.person) p
            ON r.idtarget = p.entity_id
        LEFT JOIN (SELECT identity AS entity_id, 'translation' AS type FROM data.translation) tr
            ON r.idtarget = tr.entity_id
    """)
    for biblio_id, entity_id, entity_type_str, page_start, page_end in pg_cursor.fetchall():
        if not entity_type_str:
            continue

        entity_enum = POSTGRES_TYPE_TO_ENTITY.get(entity_type_str)
        if not entity_enum:
            continue

        pg_cursor.execute("""
            SELECT
                CASE
                    WHEN EXISTS(SELECT 1 FROM data.article WHERE identity = %s) THEN 'article'
                    WHEN EXISTS(SELECT 1 FROM data.blog_post WHERE identity = %s) THEN 'blog_post'
                    WHEN EXISTS(SELECT 1 FROM data.book WHERE identity = %s) THEN 'book'
                    WHEN EXISTS(SELECT 1 FROM data.bookchapter WHERE identity = %s) THEN 'book_chapter'
                    WHEN EXISTS(SELECT 1 FROM data.online_source WHERE identity = %s) THEN 'online_source'
                    WHEN EXISTS(SELECT 1 FROM data.phd WHERE identity = %s) THEN 'phd'
                    WHEN EXISTS(SELECT 1 FROM data.bib_varia WHERE identity = %s) THEN 'bib_varia'
                END AS bib_type
        """, (biblio_id,) * 7)
        result = pg_cursor.fetchone()
        if not result or not result[0]:
            continue

        bib_type_enum = next((bt for bt in BiblioType if bt.value == result[0]), None)
        if not bib_type_enum:
            continue

        join_table = f"{entity_enum.name.lower()}_{bib_type_enum.value}"
        entity_col = f"{entity_enum.name.lower()}_id"
        bib_col = f"{bib_type_enum.value}_id"

        try:
            cursor.execute(
                f"INSERT OR IGNORE INTO {join_table} ({entity_col}, {bib_col}, page_start, page_end) VALUES (?, ?, ?, ?)",
                (str(entity_id), str(biblio_id), page_start, page_end)
            )
        except sqlite3.IntegrityError as e:
            # Log a warning but continue
            print(
                f"FK WARNING: Could not insert into {join_table}: "
                f"entity_id={entity_id}, biblio_id={biblio_id}. "
                f"The bibliography itself was inserted.",
                e
            )

    conn.commit()
    conn.close()
    pg_conn.close()


