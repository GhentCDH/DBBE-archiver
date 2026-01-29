from ..common import get_db_connection, get_postgres_connection
from .biblio_type_enum import BiblioType
from .biblio_entity_enum import BiblioEntity

POSTGRES_TYPE_TO_ENTITY = {
    "manuscript": BiblioEntity.MANUSCRIPT,
    "occurrence": BiblioEntity.OCCURRENCE,
    "type": BiblioEntity.TYPE,
    "person": BiblioEntity.PERSON,
}

def migrate_biblio_references():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

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
    rows = pg_cursor.fetchall()

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
    biblio_type_map = {str(r[0]): r[1] for r in pg_cursor.fetchall()}
    BIB_TYPE_ENUM_MAP = {bt.value: bt for bt in BiblioType}

    cursor.execute("BEGIN")
    for biblio_id, entity_id, entity_type_str, page_start, page_end in rows:
        if not entity_type_str:
            continue
        entity_enum = POSTGRES_TYPE_TO_ENTITY.get(entity_type_str)
        if not entity_enum:
            continue
        bib_type = biblio_type_map.get(str(biblio_id))
        if not bib_type:
            continue
        bib_type_enum = BIB_TYPE_ENUM_MAP.get(bib_type)
        if not bib_type_enum:
            continue

        join_table = f"{entity_enum.name.lower()}_{bib_type_enum.value}"
        entity_col = f"{entity_enum.name.lower()}_id"
        bib_col = f"{bib_type_enum.value}_id"

        cursor.execute(
            f"INSERT OR IGNORE INTO {join_table} ({entity_col}, {bib_col}, page_start, page_end) VALUES (?, ?, ?, ?)",
            (str(entity_id), str(biblio_id), page_start, page_end)
        )
    cursor.execute("COMMIT")
    conn.close()
    pg_conn.close()
