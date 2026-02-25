from app.common import execute_with_normalization, get_db_connection, get_postgres_connection


def migrate_languages():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()


    pg_cursor.execute("""
        SELECT idlanguage, name, code, description
        FROM data.language
    """)

    execute_with_normalization(cursor, "BEGIN")
    for idlanguage, name, code, description in pg_cursor.fetchall():
        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO language (id, name, code, description)
            VALUES (?, ?, ?, ?)
        """, (str(idlanguage), name, code, description))
    execute_with_normalization(cursor, "COMMIT")

    conn.close()
    pg_conn.close()
