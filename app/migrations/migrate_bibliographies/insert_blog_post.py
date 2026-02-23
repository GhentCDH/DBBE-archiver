from app.common import (
    execute_with_normalization,
    get_db_connection,
    get_postgres_connection,
    get_es_client,
    get_public_release
)

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


def insert_blog_posts():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    pg_cursor.execute("""
        SELECT bp.identity,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment,
               bp.post_date,
               bp.url
        FROM data.blog_post bp
        LEFT JOIN data.entity entity
            ON entity.identity = bp.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)
    is_public_release = get_public_release()

    pg_cursor.execute("""
        SELECT idcontent, idcontainer
        FROM data.document_contains
    """)
    blog_mapping = {str(row[0]): str(row[1]) for row in pg_cursor.fetchall()}

    insert_rows = []
    for identity, created, modified, public_comment, private_comment, post_date, url in rows:
        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})

        private_comment_val = None
        if not is_public_release:
            private_comment_val = private_comment

        blog_id = blog_mapping.get(identity_str)  # Lookup the blog ID from Postgres

        insert_rows.append(
            (
                identity_str,
                title_data.get("title", ""),
                title_data.get("title_sort_key", ""),
                created,
                modified,
                public_comment,
                private_comment_val,
                post_date,
                url,
                blog_id
            )
        )
    execute_with_normalization(cursor, "BEGIN")

    cursor.executemany(
        """
        INSERT OR IGNORE INTO blog_post
        (id, title, title_sort_key,
         created, modified,
         public_comment, private_comment,
         post_date, url, blog)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        insert_rows
    )
    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    pg_conn.close()