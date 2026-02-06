from app.common import execute_with_normalization, get_db_connection, add_column_if_missing

def create_schema():
    conn, cursor = get_db_connection()

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_person_role (
        manuscript_id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, person_id, role_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_management (
        manuscript_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, management_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_acknowledgement (
        manuscript_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, acknowledgement_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgement(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_content (
        manuscript_id INTEGER NOT NULL,
        content_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, content_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_identification (
        manuscript_id INTEGER NOT NULL,
        identification_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, identification_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_location (
        manuscript_id INTEGER NOT NULL,
        origin_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, origin_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (origin_id) REFERENCES location(id)
    )
    """)