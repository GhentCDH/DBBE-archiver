# app/migrate_bibliographies/link_persons_to_bibliographies.py
import sqlite3
from ..common import execute_with_normalization, get_db_connection, get_postgres_connection
from .biblio_type_enum import BiblioType

def exists(cursor, table, id_):
    execute_with_normalization(cursor,
        f"SELECT 1 FROM {table} WHERE id = ? LIMIT 1",
                               (id_,)
                               )
    return cursor.fetchone() is not None


def migrate_person_roles():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT
            br.iddocument,
            br.idperson,
            br.idrole,
            COALESCE(
                a.type::text, bp.type::text, b.type::text,
                bc.type::text, os.type::text, p.type::text, bv.type::text
            ) AS bib_type
        FROM data.bibrole br
        LEFT JOIN (SELECT identity,'article' type FROM data.article) a
            ON br.iddocument = a.identity
        LEFT JOIN (SELECT identity,'blog_post' type FROM data.blog_post) bp
            ON br.iddocument = bp.identity
        LEFT JOIN (SELECT identity,'book' type FROM data.book) b
            ON br.iddocument = b.identity
        LEFT JOIN (SELECT identity,'book_chapter' type FROM data.bookchapter) bc
            ON br.iddocument = bc.identity
        LEFT JOIN (SELECT identity,'online_source' type FROM data.online_source) os
            ON br.iddocument = os.identity
        LEFT JOIN (SELECT identity,'phd' type FROM data.phd) p
            ON br.iddocument = p.identity
        LEFT JOIN (SELECT identity,'bib_varia' type FROM data.bib_varia) bv
            ON br.iddocument = bv.identity
    """)

    rows = pg_cursor.fetchall()

    for doc_id, person_id, role_id, bib_type in rows:
        if not bib_type:
            continue

        bib_type_enum = next(
            (
                bt for bt in BiblioType
                if bt.value == bib_type.lower().replace(" ", "_")
            ),
            None
        )

        if not bib_type_enum:
            continue

        bib_id = str(doc_id)
        person_id = str(person_id)
        role_id = str(role_id)

        try:
            execute_with_normalization(cursor,
                f"""
                INSERT INTO {bib_type_enum.value}_person_roles
                    (bibliography_id, person_id, role_id)
                VALUES (?, ?, ?)
                """,
                                       (bib_id, person_id, role_id)
                                       )
        except sqlite3.IntegrityError as e:
            print(
                "FK ERROR:",
                bib_type_enum.value,
                bib_id,
                person_id,
                role_id,
                e
            )

        execute_with_normalization(cursor,
            f"""
            INSERT OR IGNORE INTO {bib_type_enum.value}_person_roles
                (bibliography_id, person_id, role_id)
            VALUES (?, ?, ?)
            """,
                                   (str(doc_id), str(person_id), str(role_id))
                                   )

    conn.commit()
    conn.close()
    pg_conn.close()
