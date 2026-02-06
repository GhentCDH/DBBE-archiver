from ..common import execute_with_normalization, get_db_connection, get_postgres_connection, get_es_client, get_public_release
from .biblio_type_enum import BiblioType
from collections import defaultdict

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


def insert_bibliographies():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    pg_cursor.execute("""
        SELECT biblio.identity, biblio.bib_type,
               entity.created, entity.modified,
               entity.public_comment, entity.private_comment
        FROM (
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
        ) AS biblio
        LEFT JOIN data.entity entity
            ON entity.identity = biblio.identity
        
            """)
    rows = pg_cursor.fetchall()

    biblio_ids = [str(r[0]) for r in rows]

    titles_cache = get_biblio_titles_from_es(biblio_ids, es)

    is_public_release = get_public_release()
    biblio_rows_by_type = defaultdict(list)

    for identity, bib_type, created, modified, public_comment, private_comment in rows:
        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})

        private_comment_val = None
        if not is_public_release:
            private_comment_val = private_comment

        biblio_rows_by_type[bib_type].append(
            (
                identity_str,
                title_data.get("title", ""),
                title_data.get("title_sort_key", ""),
                created,
                modified,
                public_comment,
                private_comment_val
            )
        )

    execute_with_normalization(cursor, "BEGIN")
    for bib_type, insert_rows in biblio_rows_by_type.items():
        bib_type_enum = next((bt for bt in BiblioType if bt.value == bib_type), None)
        if bib_type_enum:
            cursor.executemany(
                f"""INSERT OR IGNORE INTO {bib_type_enum.value} 
                    (id, title, title_sort_key, created, modified, public_comment, private_comment)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                insert_rows
            )
    execute_with_normalization(cursor, "COMMIT")

    conn.close()
    pg_conn.close()