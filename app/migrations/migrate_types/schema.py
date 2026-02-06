from app.common import execute_with_normalization, get_db_connection, add_column_if_missing

def create_schema():
    conn, cursor = get_db_connection()

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_management (
        type_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, management_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_acknowledgement (
        type_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, acknowledgement_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgement(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_tag (
        type_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, tag_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (tag_id) REFERENCES tag(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_genre (
        type_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, genre_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (genre_id) REFERENCES genre(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_metre (
        type_id INTEGER NOT NULL,
        metre_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, metre_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (metre_id) REFERENCES metre(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_editorial_status (
        type_id INTEGER NOT NULL,
        editorial_status_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, editorial_status_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (editorial_status_id) REFERENCES editorial_status(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_text_statuses (
        type_id INTEGER NOT NULL,
        text_status_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, text_status_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (text_status_id) REFERENCES text_statuses(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_person_role (
        type_id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, person_id, role_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_occurrence (
        type_id INTEGER NOT NULL,
        occurrence_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, occurrence_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrence(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_related_type (
        type_id INTEGER NOT NULL,
        related_type_id INTEGER NOT NULL,
        relation_definition_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, related_type_id, relation_definition_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (related_type_id) REFERENCES type(id),
        FOREIGN KEY (related_type_id) REFERENCES type(id),
        FOREIGN KEY (relation_definition_id) REFERENCES type_relation_definition(id),
        CHECK (type_id < related_type_id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_relation_definition (
        id INTEGER PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_keyword (
        type_id INTEGER NOT NULL,
        keyword_id INTEGER NOT NULL,
        PRIMARY KEY (type_id, keyword_id),
        FOREIGN KEY (type_id) REFERENCES type(id),
        FOREIGN KEY (keyword_id) REFERENCES keyword(id)
    );
    """)