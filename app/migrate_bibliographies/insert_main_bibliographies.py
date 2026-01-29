from ..common import get_db_connection, get_postgres_connection, get_es_client
from .biblio_type_enum import BiblioType
from .biblio_entity_enum import BiblioEntity
from collections import defaultdict

POSTGRES_TYPE_TO_ENTITY = {
    "manuscript": BiblioEntity.MANUSCRIPT,
    "occurrence": BiblioEntity.OCCURRENCE,
    "type": BiblioEntity.TYPE,
    "person": BiblioEntity.PERSON,
}

def get_biblio_titles_from_es(biblio_ids, es):
    index = "dbbe_dev_bibliographies"
    titles = {}
    CHUNK = 500

    for i in range(0, len(biblio_ids), CHUNK):
        chunk = biblio_ids[i:i+CHUNK]
        res = es.mget(index=index, body={"ids": chunk})

        for doc in res["docs"]:
            if doc.get("found"):
                src = doc["_source"]
                titles[doc["_id"]] = {
                    "title": src.get("title", ""),
                    "title_sort_key": src.get("title_sort_key", "")
                }
            else:
                titles[doc["_id"]] = {"title": "", "title_sort_key": ""}

    return titles

def migrate_main_bibliographies():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    BIB_TYPE_ENUM_MAP = {bt.value: bt for bt in BiblioType}

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
    rows = pg_cursor.fetchall()

    biblio_type_map = {
        str(identity): bib_type
        for identity, bib_type in rows
    }
    biblio_ids = [str(row[0]) for row in rows]
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)
    bib_rows_by_type = defaultdict(list)
    for source_id, bib_type in rows:
        title_data = titles_cache.get(str(source_id), {})
        title = title_data.get("title", "")
        title_sort = title_data.get("title_sort_key", "")

        bib_rows_by_type[bib_type].append(
            (str(source_id), title, title_sort)
        )

    cursor.execute("BEGIN")
    for bib_type, insert_rows in bib_rows_by_type.items():
        bib_type_enum = next((bt for bt in BiblioType if bt.value == bib_type), None)
        if not bib_type_enum:
            continue
        cursor.executemany(
            f"""
            INSERT OR IGNORE INTO {bib_type_enum.value}
            (id, title, title_sort_key)
            VALUES (?, ?, ?)
            """,
            insert_rows
        )
    cursor.execute("COMMIT")

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
    cursor.execute("BEGIN")
    for biblio_id, entity_id, entity_type_str, page_start, page_end in pg_cursor.fetchall():
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


