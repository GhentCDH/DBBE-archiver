from app.common import execute_with_normalization, get_db_connection, get_postgres_connection

def migrate_journals():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    execute_with_normalization(cursor, """
        PRAGMA table_info(article)
    """)
    columns = [row[1] for row in cursor.fetchall()]  # column names
    if "journal_issue_id" not in columns:
        execute_with_normalization(cursor, """
            ALTER TABLE article
            ADD COLUMN journal_issue_id INTEGER REFERENCES journal_issue(id)
        """)

    pg_cursor.execute("""
        SELECT j.identity AS journal_id,
               dt.title AS journal_title
        FROM data.journal j
        JOIN data.document_title dt ON j.identity = dt.iddocument
    """)
    for journal_id, journal_title in pg_cursor.fetchall():
        execute_with_normalization(cursor, """
            INSERT INTO journal (id, title, title_sort_key)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                title_sort_key = excluded.title_sort_key
        """, (str(journal_id), journal_title, journal_title))

    pg_cursor.execute("""
        SELECT ji.identity AS issue_id,
               ji.idjournal AS journal_id,
               ji.year,
               ji.volume,
               ji.number,
               ji.series,
               ji.forthcoming
        FROM data.journal_issue ji
    """)

    for issue_id, journal_id, year, volume, number, series, forthcoming in pg_cursor.fetchall():
        title_parts = []
        if year: title_parts.append(str(year))
        if series: title_parts.append(series)
        if volume: title_parts.append(volume)
        if number: title_parts.append(number)
        if forthcoming: title_parts.append("(forthcoming)")
        issue_title = " ".join(title_parts) if title_parts else None

        execute_with_normalization(cursor, """
            INSERT INTO journal_issue (id, journal_id, title, title_sort_key)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                journal_id = excluded.journal_id,
                title = excluded.title,
                title_sort_key = excluded.title_sort_key
        """, (str(issue_id), str(journal_id), issue_title, issue_title))

    # --- Step 3: update article → journal_issue
    pg_cursor.execute("""
        SELECT a.identity AS article_id, dc.idcontainer AS issue_id
        FROM data.article a
        JOIN data.document_contains dc ON dc.idcontent = a.identity
        JOIN data.journal_issue ji ON dc.idcontainer = ji.identity
    """)
    for article_id, issue_id in pg_cursor.fetchall():
        execute_with_normalization(cursor, """
            UPDATE article
            SET journal_issue_id = ?
            WHERE id = ?
        """, (str(issue_id), str(article_id)))

    conn.commit()
    conn.close()
    pg_conn.close()
