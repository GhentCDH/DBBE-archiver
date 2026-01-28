# app/migrate_bibliographies/link_chapters_to_books.py
from ..common import get_db_connection, get_postgres_connection

def migrate_book_chapters():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT bc.identity, b.identity
        FROM data.bookchapter bc
        JOIN data.document_contains dc ON dc.idcontent = bc.identity
        JOIN data.book b ON dc.idcontainer = b.identity
    """)

    for chapter_id, book_id in pg_cursor.fetchall():
        cursor.execute("""
            UPDATE book_chapter
            SET book_id = ?
            WHERE id = ?
        """, (str(book_id), str(chapter_id)))

    conn.commit()
    conn.close()
    pg_conn.close()
