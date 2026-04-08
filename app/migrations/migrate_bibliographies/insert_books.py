from app.common import (
    execute_with_normalization,
    get_db_connection,
    get_postgres_connection,
    get_es_client,
    get_public_release
)
from .biblio_type_enum import BiblioType
import os

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


def insert_books():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    pg_cursor.execute("""
        SELECT biblio.identity,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment,
               biblio.year,
               biblio.publisher,
               biblio.editor,
               biblio.forthcoming,
               biblio.idcluster,
               biblio.idseries
        FROM data.book biblio
        LEFT JOIN data.entity entity
            ON entity.identity = biblio.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)
    is_public_release = get_public_release()

    insert_rows = []
    cluster_ids = set()
    series_ids = set()

    for (
        identity,
        created,
        modified,
        public_comment,
        private_comment,
        year,
        publisher,
        editor,
        forthcoming,
        idcluster,
        idseries
    ) in rows:

        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})
        private_comment_val = None if is_public_release else private_comment

        if idcluster is not None:
            cluster_ids.add(idcluster)

        if idseries is not None:
            series_ids.add(idseries)

        insert_rows.append((
            identity_str,
            title_data.get("title", ""),
            title_data.get("title_sort_key", ""),
            created,
            modified,
            public_comment,
            private_comment_val,
            year,
            publisher,
            editor,
            forthcoming,
            idcluster,
            idseries
        ))

        pg_cursor.execute("""
            SELECT idauthority, idsubject, identifier, volume
            FROM data.global_id
            WHERE idsubject = %s
        """, (identity,))

        for idauthority, idsubject, identifier_id, volume in pg_cursor.fetchall():
            pg_cursor.execute("""
                SELECT ids, system_name
                FROM data.identifier
                WHERE %s = ANY(ids)  
            """, (idauthority,))

            identifier_row = pg_cursor.fetchone()
            if not identifier_row:
                continue

            ids_array, system_name = identifier_row

            catalogue = system_name
            if volume is not None:
                catalogue_id = f"{volume}.{identifier_id}"
            else:
                catalogue_id = str(identifier_id)

            execute_with_normalization(cursor, """
                INSERT OR IGNORE INTO identification (
                    catalogue,
                    catalogue_id,
                    entity_type,
                    entity_id
                )
                VALUES (?, ?, ?, ?)
            """, (catalogue, catalogue_id, "book", identity_str))

    execute_with_normalization(cursor, "BEGIN")

    series_rows = []
    if series_ids:
        pg_cursor.execute(
            """
            SELECT bs.identity,
                   dt.title
            FROM data.book_series bs
            LEFT JOIN data.document_title dt
                ON dt.iddocument = bs.identity
            WHERE bs.identity = ANY(%s)
            """,
            (list(series_ids),)
        )
        series_rows = pg_cursor.fetchall()

    cluster_rows = []
    if cluster_ids:
        pg_cursor.execute(
            """
            SELECT bc.identity,
                   dt.title
            FROM data.book_cluster bc
            LEFT JOIN data.document_title dt
                ON dt.iddocument = bc.identity
            WHERE bc.identity = ANY(%s)
            """,
            (list(cluster_ids),)
        )
        cluster_rows = pg_cursor.fetchall()

    # Insert clusters with title
    cursor.executemany(
        "INSERT OR IGNORE INTO book_cluster (id, title) VALUES (?, ?)",
        [
            (cid, title if title else "")
            for cid, title in cluster_rows
        ]
    )

    # Insert series with title
    cursor.executemany(
        "INSERT OR IGNORE INTO book_series (id, title) VALUES (?, ?)",
        [
            (sid, title if title else "")
            for sid, title in series_rows
        ]
    )

    # Insert books
    cursor.executemany(
        """INSERT OR IGNORE INTO book
           (id, title, title_sort_key,
            created, modified,
            public_comment, private_comment,
            year, publisher, editor, forthcoming,
            idcluster, idseries)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                "INSERT OR IGNORE INTO book_management (book_id, management_id) VALUES (?, ?)",
                (identity_str, str(management_id)))
    execute_with_normalization(cursor, "COMMIT")

    conn.close()
    pg_conn.close()