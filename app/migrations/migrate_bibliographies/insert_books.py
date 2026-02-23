from app.common import (
    execute_with_normalization,
    get_db_connection,
    get_postgres_connection,
    get_es_client,
    get_public_release
)
from .biblio_type_enum import BiblioType


def get_biblio_titles_from_es(biblio_ids, es):
    index = "dbbe_dev_bibliographies"
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


def insert_books():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    pg_cursor.execute("""
        SELECT biblio.identity,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment
        FROM data.book biblio
        LEFT JOIN data.entity entity
            ON entity.identity = biblio.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)
    is_public_release = get_public_release()

    insert_rows = []
    for identity, created, modified, public_comment, private_comment in rows:
        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})
        private_comment_val = None if is_public_release else private_comment

        insert_rows.append((
            identity_str,
            title_data.get("title", ""),
            title_data.get("title_sort_key", ""),
            created,
            modified,
            public_comment,
            private_comment_val
        ))

    execute_with_normalization(cursor, "BEGIN")

    cursor.executemany(
        """INSERT OR IGNORE INTO book
           (id, title, title_sort_key,
            created, modified,
            public_comment, private_comment)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        insert_rows
    )

    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    pg_conn.close()