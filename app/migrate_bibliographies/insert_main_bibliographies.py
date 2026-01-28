# app/migrate_bibliographies/insert_main_bibliographies.py
from ..common import get_db_connection, get_postgres_connection, get_es_client, scroll_all, get_dbbe_indices
from .biblio_type_enum import BiblioType


def migrate_references():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT
            r.idreference,
            r.idsource,
            r.page_start,
            r.page_end,
            CASE WHEN r.page_start IS NULL THEN r.temp_page_removeme ELSE NULL END AS raw_pages,
            r.url,
            r.source_remark,
            r.image,
            COALESCE(
                a.type::text, bp.type::text, b.type::text,
                bc.type::text, os.type::text, p.type::text, bv.type::text
            ) AS bib_type
        FROM data.reference r
        LEFT JOIN (SELECT identity,'article' type FROM data.article) a ON r.idsource=a.identity
        LEFT JOIN (SELECT identity,'blog_post' type FROM data.blog_post) bp ON r.idsource=bp.identity
        LEFT JOIN (SELECT identity,'book' type FROM data.book) b ON r.idsource=b.identity
        LEFT JOIN (SELECT identity,'book_chapter' type FROM data.bookchapter) bc ON r.idsource=bc.identity
        LEFT JOIN (SELECT identity,'online_source' type FROM data.online_source) os ON r.idsource=os.identity
        LEFT JOIN (SELECT identity,'phd' type FROM data.phd) p ON r.idsource=p.identity
        LEFT JOIN (SELECT identity,'bib_varia' type FROM data.bib_varia) bv ON r.idsource=bv.identity
    """)

    reference_rows = pg_cursor.fetchall()

    for row in reference_rows:
        _, source_id, _, _, _, _, _, _, bib_type = row
        if not bib_type:
            continue

        bib_type_enum = None
        for bt in BiblioType:
            if bt.value == bib_type.lower().replace(" ", "_"):
                bib_type_enum = bt
                break
        if not bib_type_enum:
            continue

        cursor.execute(
            f"INSERT OR IGNORE INTO {bib_type_enum.value} (id) VALUES (?)",
            (str(source_id),)
        )

    es = get_es_client()
    indices = get_dbbe_indices(es)
    bibliography_index = next((idx for idx in indices if idx.endswith("bibliographies")), None)
    if bibliography_index:
        hits = scroll_all(es, bibliography_index)
        for hit in hits:
            source = hit['_source']
            bib_id = str(source.get('id', hit['_id']))
            title = source.get('title')
            title_sort_key = source.get('title_sort_key')

            bib_type_name = source.get('type', {}).get('name')
            if not bib_type_name:
                continue

            bib_type_enum = None
            for bt in BiblioType:
                if bt.value == bib_type_name.lower().replace(" ", "_"):
                    bib_type_enum = bt
                    break
            if not bib_type_enum:
                continue

            cursor.execute(f"""
                INSERT INTO {bib_type_enum.value} (id, title, title_sort_key)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    title_sort_key = excluded.title_sort_key
            """, (bib_id, title, title_sort_key))

    conn.commit()
    conn.close()
    pg_conn.close()
