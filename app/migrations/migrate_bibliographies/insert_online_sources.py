from app.common import (
    execute_with_normalization,
    get_db_connection,
    get_postgres_connection,
    get_es_client,
    get_public_release
)
from .biblio_type_enum import BiblioType


def get_online_source_titles_from_es(biblio_ids, es):
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


def insert_online_sources():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()
    is_public_release = get_public_release()

    pg_cursor.execute("""
        SELECT os.identity,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment,
               os.url,
               os.last_accessed
        FROM data.online_source os
        LEFT JOIN data.entity entity ON entity.identity = os.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]
    titles_cache = get_online_source_titles_from_es(biblio_ids, es)

    bib_type_enum = next((bt for bt in BiblioType if bt.value == "online_source"), None)
    if not bib_type_enum:
        conn.close()
        pg_conn.close()
        return

    insert_rows = []
    for identity, created, modified, public_comment, private_comment, url, last_accessed in rows:
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
            private_comment_val,
            url,
            last_accessed,
        ))
    execute_with_normalization(cursor, "BEGIN")

    cursor.executemany(
        f"""INSERT OR IGNORE INTO {bib_type_enum.value}
                (id, title, title_sort_key, created, modified,
                 public_comment, private_comment, url, last_accessed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        insert_rows
    )
    execute_with_normalization(cursor, "COMMIT")
    execute_with_normalization(cursor, "BEGIN")

    for identity, *_ in rows:
        identity_str = str(identity)
        pg_cursor.execute("""
            SELECT em.idmanagement, m.name
            FROM data.entity_management em
            JOIN data.management m ON m.id = em.idmanagement
            WHERE em.identity = %s
        """, (identity_str,))
        for management_id, management_name in pg_cursor.fetchall():
            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO management (id, name) VALUES (?, ?)",
                (str(management_id), management_name))
            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO online_source_management (online_source_id, management_id) VALUES (?, ?)",
                (identity_str, str(management_id)))
    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    pg_conn.close()