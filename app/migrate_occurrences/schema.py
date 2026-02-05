# app/migrate_bibliographies/schema.py
from ..common import execute_with_normalization, get_db_connection, add_column_if_missing

def create_schema():
    conn, cursor = get_db_connection()

    occurrence_columns = [
        ("created", "TEXT"),
        ("modified", "TEXT"),
        ("public_comment", "TEXT"),
        ("private_comment", "TEXT"),
        ("is_dbbe", "BOOLEAN"),
        ("incipit", "TEXT"),
        ("text_stemmer", "TEXT"),
        ("text_original", "TEXT"),
        ("location_in_ms", "TEXT"),
        ("date_floor_year", "TEXT"),
        ("date_ceiling_year", "TEXT"),
        ("palaeographical_info", "TEXT"),
        ("contextual_info", "TEXT"),
        ("manuscript_id", "TEXT"),
        ("title", "TEXT")
    ]

    for col, col_type in occurrence_columns:
        add_column_if_missing(cursor, "occurrences", col, col_type)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_person_role (
        occurrence_id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, person_id, role_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_genre (
        occurrence_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, genre_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (genre_id) REFERENCES genre(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_metre (
        occurrence_id INTEGER NOT NULL,
        metre_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, metre_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (metre_id) REFERENCES metre(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_management (
        occurrence_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, management_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_acknowledgement (
        occurrence_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, acknowledgement_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgement(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_text_statuses (
        occurrence_id INTEGER NOT NULL,
        text_status_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, text_status_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (text_status_id) REFERENCES text_statuses(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_related_occurrences (
        occurrence_id INTEGER NOT NULL,
        related_occurrence_id INTEGER NOT NULL,
        relation_definition_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, related_occurrence_id, relation_definition_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (related_occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (relation_definition_id) REFERENCES occurrence_relation_definition(id),
        CHECK (occurrence_id <> related_occurrence_id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_keyword (
        occurrence_id INTEGER NOT NULL,
        keyword_id INTEGER NOT NULL,
        PRIMARY KEY (occurrence_id, keyword_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (keyword_id) REFERENCES keyword(id)
    );
    """)

    execute_with_normalization(cursor, """
    INSERT OR IGNORE INTO occurrence_relation_definition (id, definition) VALUES
    ('0', 'verse_related'),
    ('1', 'type_related')
    """)

    conn.commit()
    conn.close()
