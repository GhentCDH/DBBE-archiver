from ..common import get_db_connection, get_postgres_connection
from .biblio_type_enum import BiblioType

from ..common import get_db_connection, get_postgres_connection
from .biblio_type_enum import BiblioType


def migrate_managements():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    sqlite_managements = {row[0] for row in cursor.execute("SELECT id FROM management").fetchall()}

    pg_cursor.execute("""
        SELECT
            em.identity,
            em.idmanagement,
            COALESCE(
                a.type::text, bp.type::text, b.type::text,
                bc.type::text, os.type::text, p.type::text, bv.type::text
            ) AS bib_type
        FROM data.entity_management em
        LEFT JOIN (SELECT identity,'article' type FROM data.article) a
            ON em.identity = a.identity
        LEFT JOIN (SELECT identity,'blog_post' type FROM data.blog_post) bp
            ON em.identity = bp.identity
        LEFT JOIN (SELECT identity,'book' type FROM data.book) b
            ON em.identity = b.identity
        LEFT JOIN (SELECT identity,'book_chapter' type FROM data.bookchapter) bc
            ON em.identity = bc.identity
        LEFT JOIN (SELECT identity,'online_source' type FROM data.online_source) os
            ON em.identity = os.identity
        LEFT JOIN (SELECT identity,'phd' type FROM data.phd) p
            ON em.identity = p.identity
        LEFT JOIN (SELECT identity,'bib_varia' type FROM data.bib_varia) bv
            ON em.identity = bv.identity
    """)
    rows = pg_cursor.fetchall()

    for doc_id, mgmt_id, bib_type in rows:
        if not bib_type:
            continue

        bib_type_enum = next(
            (bt for bt in BiblioType if bt.value == bib_type.lower().replace(" ", "_")),
            None
        )
        if not bib_type_enum:
            continue

        doc_id = str(doc_id)
        mgmt_id = str(mgmt_id)

        if mgmt_id not in sqlite_managements:
            pg_cursor.execute("SELECT id, name FROM data.management WHERE id = %s", (mgmt_id,))
            mgmt_row = pg_cursor.fetchone()
            if not mgmt_row:
                continue
            cursor.execute("INSERT INTO management (id, name) VALUES (?, ?)", (mgmt_row[0], mgmt_row[1]))
            sqlite_managements.add(mgmt_id)

        cursor.execute(
            f"""
            INSERT OR IGNORE INTO {bib_type_enum.value}_managements
                (bibliography_id, management_id)
            VALUES (?, ?)
            """,
            (doc_id, mgmt_id)
        )

    conn.commit()
    conn.close()
    pg_conn.close()
