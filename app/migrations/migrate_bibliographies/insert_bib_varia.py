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


def preload_locations(cursor):
    cursor.execute("SELECT id, name FROM location")
    return {name: loc_id for loc_id, name in cursor.fetchall()}


def resolve_location(cursor, location_map, city):
    if not city:
        return None

    if city in location_map:
        return location_map[city]

    cursor.execute("INSERT INTO location (name) VALUES (?)", (city,))
    location_id = cursor.lastrowid
    location_map[city] = location_id
    return location_id


def insert_bib_varia():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    es = get_es_client()

    location_map = preload_locations(cursor)

    pg_cursor.execute("""
        SELECT bv.identity,
               entity.created,
               entity.modified,
               entity.public_comment,
               entity.private_comment,
               bv.year,
               bv.city,
               bv.institution
        FROM data.bib_varia bv
        LEFT JOIN data.entity entity
            ON entity.identity = bv.identity
    """)

    rows = pg_cursor.fetchall()
    biblio_ids = [str(r[0]) for r in rows]

    # Fetch titles from ES
    titles_cache = get_biblio_titles_from_es(biblio_ids, es)

    is_public_release = get_public_release()

    insert_rows = []

    for identity, created, modified, public_comment, private_comment, year, city, bib_varia_institution in rows:
        identity_str = str(identity)
        title_data = titles_cache.get(identity_str, {})

        private_comment_val = None
        if not is_public_release:
            private_comment_val = private_comment

        location_id = resolve_location(cursor, location_map, city)

        insert_rows.append(
            (
                identity_str,
                title_data.get("title", ""),
                title_data.get("title_sort_key", ""),
                created,
                modified,
                public_comment,
                private_comment_val,
                year,
                location_id,
                bib_varia_institution
            )
        )

    # Insert into SQLite
    execute_with_normalization(cursor, "BEGIN")

    cursor.executemany(
        """
        INSERT OR IGNORE INTO bib_varia
        (id, title, title_sort_key, created, modified,
         public_comment, private_comment, year, city, bib_varia_institution)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        insert_rows
    )

    execute_with_normalization(cursor, "COMMIT")

    conn.close()
    pg_conn.close()