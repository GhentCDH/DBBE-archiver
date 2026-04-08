import os
from app.common import (
    execute_with_normalization,
    get_db_connection,
    get_postgres_connection,
    get_es_client,
    get_public_release,
insert_entity_managements
)
from .biblio_type_enum import BiblioType
from collections import defaultdict


def get_biblio_titles_from_es(biblio_ids, es):
    prefix = os.getenv("ES_INDEX_PREFIX", "dbbe_dev")
    index = f"{prefix}_bibliographies"
    titles = {}
    CHUNK = 500

    for i in range(0, len(biblio_ids), CHUNK):
        chunk = biblio_ids[i:i + CHUNK]
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


BIBLIO_SOURCES = [
    ("article",      "data.article",       False),
    ("book_chapter", "data.bookchapter",   False),
    ("blog",         "data.blog",          True),
]


def insert_bibliographies():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()
    is_public_release = get_public_release()

    union_parts = " UNION ALL ".join(
        f"SELECT identity, '{bib_type}' AS bib_type FROM {table}"
        for bib_type, table, _ in BIBLIO_SOURCES
    )

    pg_cursor.execute(f"""
        SELECT biblio.identity,
               biblio.bib_type,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment,
               blog.url
        FROM ({union_parts}) AS biblio
        LEFT JOIN data.entity entity ON entity.identity = biblio.identity
        LEFT JOIN data.blog blog ON blog.identity = biblio.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)

    biblio_rows_by_type = defaultdict(list)
    for identity, bib_type, created, modified, public_comment, private_comment, url in rows:
        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})
        private_comment_val = None if is_public_release else private_comment

        base = (
            identity_str,
            title_data.get("title", ""),
            title_data.get("title_sort_key", ""),
            created,
            modified,
            public_comment,
            private_comment_val,
        )
        biblio_rows_by_type[bib_type].append(base + (url,) if bib_type == "blog" else base)

    execute_with_normalization(cursor, "BEGIN")

    for bib_type, insert_rows in biblio_rows_by_type.items():
        bib_type_enum = next((bt for bt in BiblioType if bt.value == bib_type), None)
        if not bib_type_enum:
            continue

        is_blog = bib_type == "blog"
        extra_col = ", url" if is_blog else ""
        extra_placeholder = ", ?" if is_blog else ""

        cursor.executemany(
            f"""INSERT OR IGNORE INTO {bib_type_enum.value}
                (id, title, title_sort_key, created, modified,
                 public_comment, private_comment{extra_col})
                VALUES (?, ?, ?, ?, ?, ?, ?{extra_placeholder})""",
            insert_rows
        )

    execute_with_normalization(cursor, "COMMIT")

    execute_with_normalization(cursor, "BEGIN")
    for identity, bib_type, *_ in rows:
        identity_str = str(identity)
        bib_type_enum = next((bt for bt in BiblioType if bt.value == bib_type), None)
        if not bib_type_enum:
            continue
        insert_entity_managements(cursor, pg_cursor, identity_str, f"{bib_type_enum.value}_management", f"{bib_type_enum.value}_id")
    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    pg_conn.close()