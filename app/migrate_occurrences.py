"""
Migrate occurrences data from Elasticsearch to SQLite.
"""
from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME
)


def create_occurrence_tables(cursor):
    """
    Create all occurrence-related tables.
    """
    # Add columns to occurrences table
    occurrence_columns = [
        ("created", "TEXT"),
        ("modified", "TEXT"),
        ("public_comment", "TEXT"),
        ("private_comment", "TEXT"),
        ("is_dbbe", "BOOLEAN"),
        ("incipit", "TEXT"),
        ("text_stemmer", "TEXT"),
        ("text_original", "TEXT"),
        ("location", "TEXT"),
        ("date_floor_year", "TEXT"),
        ("date_ceiling_year", "TEXT"),
        ("palaeographical_info", "TEXT"),
        ("contextual_info", "TEXT"),
        ("manuscript_id", "TEXT"),
        ("title", "TEXT")
    ]
    
    for col, col_type in occurrence_columns:
        add_column_if_missing(cursor, "occurrences", col, col_type)
    
    # Create junction tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_person_roles (
        occurrence_id TEXT NOT NULL,
        person_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, person_id, role_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_genres (
        occurrence_id TEXT NOT NULL,
        genre_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, genre_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (genre_id) REFERENCES genres(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_metres (
        occurrence_id TEXT NOT NULL,
        metre_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, metre_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (metre_id) REFERENCES metres(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_management (
        occurrence_id TEXT NOT NULL,
        management_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, management_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_acknowledgement (
        occurrence_id TEXT NOT NULL,
        acknowledgement_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, acknowledgement_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgements(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_text_statuses (
        occurrence_id TEXT NOT NULL,
        text_status_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, text_status_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (text_status_id) REFERENCES text_statuses(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_subject (
        occurrence_id TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, subject_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (subject_id) REFERENCES subjects(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_related_occurrences (
        occurrence_id TEXT NOT NULL,
        related_occurrence_id TEXT NOT NULL,
        relation_definition_id TEXT NOT NULL,
        PRIMARY KEY (occurrence_id, related_occurrence_id, relation_definition_id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (related_occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (relation_definition_id) REFERENCES occurrence_relation_definitions(id),
        CHECK (occurrence_id <> related_occurrence_id)
    )
    """)
    
    # Insert relation definitions
    cursor.execute("""
    INSERT OR IGNORE INTO occurrence_relation_definitions (id, definition) VALUES
    ('0', 'verse_related'),
    ('1', 'type_related')
    """)


def migrate_occurrences():
    """
    Migrate occurrences from Elasticsearch to SQLite.
    """
    es = get_es_client()
    conn, cursor = get_db_connection()
    
    # DISABLE FOREIGN KEY CONSTRAINTS FOR BULK IMPORT
    cursor.execute("PRAGMA foreign_keys = OFF")
    print("Foreign key constraints disabled for migration")

    # Create occurrence tables
    create_occurrence_tables(cursor)

    # Get indices
    indices = get_dbbe_indices(es)
    occ_index = next((idx for idx in indices if idx.endswith("occurrences")), None)

    if not occ_index:
        print("No occurrence index found")
        # Re-enable foreign keys before closing
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.close()
        return

    print(f"Migrating occurrences from index: {occ_index}")
    hits = scroll_all(es, occ_index, size=500)
    print(f"Total occurrences fetched: {len(hits)}")

    cursor.execute("BEGIN")
    batch_count = 0

    for hit in hits:
        source = hit['_source']
        occ_id = str(source.get('id', hit['_id']))
        manuscript_id = str(source.get('manuscript', {}).get('id', ''))

        # Update occurrence main fields
        cursor.execute("""
        UPDATE occurrences SET
            created=?, modified=?, public_comment=?, private_comment=?,
            is_dbbe=?, incipit=?, text_stemmer=?, text_original=?,
            location=?, date_floor_year=?, date_ceiling_year=?,
            palaeographical_info=?, contextual_info=?, manuscript_id=?, title=?
        WHERE id=?
        """, (
            source.get('created', ''),
            source.get('modified', ''),
            source.get('public_comment', ''),
            source.get('private_comment', ''),
            bool(source.get('is_dbbe', False)),
            source.get('incipit', ''),
            source.get('text_stemmer', ''),
            source.get('text_original', ''),
            source.get('location', ''),
            source.get('date_floor_year', ''),
            source.get('date_ceiling_year', ''),
            source.get('palaeographical_info', ''),
            source.get('contextual_info', ''),
            manuscript_id,
            source.get('title_original', ''),
            occ_id
        ))

        # Genres
        for genre in source.get('genre', []):
            genre_id = str(genre.get('id', ''))
            if genre_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO genres (id, name) VALUES (?, ?)",
                    (genre_id, genre.get('name', ''))
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO occurrence_genres (occurrence_id, genre_id) VALUES (?, ?)",
                    (occ_id, genre_id)
                )

        # Metres
        for metre in source.get('metre', []):
            metre_id = str(metre.get('id', ''))
            if metre_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO metres (id, name) VALUES (?, ?)",
                    (metre_id, metre.get('name', ''))
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO occurrence_metres (occurrence_id, metre_id) VALUES (?, ?)",
                    (occ_id, metre_id)
                )

        # Acknowledgements
        for ack in source.get('acknowledgement', []):
            ack_id = str(ack.get('id', ''))
            ack_name = ack.get('name', '')
            if ack_id and ack_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO acknowledgements (id, name) VALUES (?, ?)",
                    (ack_id, ack_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO occurrence_acknowledgement (occurrence_id, acknowledgement_id) VALUES (?, ?)",
                    (occ_id, ack_id)
                )

        # Subjects
        subjects = source.get('subject', [])
        seen_subject_ids = set()

        for subj in subjects:
            subj_id = str(subj.get('id', ''))
            subj_name = subj.get('name', '')
            if not subj_id or subj_id in seen_subject_ids:
                continue

            seen_subject_ids.add(subj_id)
            cursor.execute(
                "INSERT OR IGNORE INTO subjects (id, name) VALUES (?, ?)",
                (subj_id, subj_name)
            )
            cursor.execute(
                "INSERT OR IGNORE INTO occurrence_subject (occurrence_id, subject_id) VALUES (?, ?)",
                (occ_id, subj_id)
            )

        # Text status
        ts = source.get('text_status')
        if isinstance(ts, dict):
            ts_id = str(ts.get('id', ''))
            ts_name = ts.get('name', '')
            if ts_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO text_statuses (id, name) VALUES (?, ?)",
                    (ts_id, ts_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO occurrence_text_statuses (occurrence_id, text_status_id) VALUES (?, ?)",
                    (occ_id, ts_id)
                )

        # Management
        for mgmt in source.get('management', []):
            mgmt_id = str(mgmt.get('id', ''))
            mgmt_name = mgmt.get('name', '')
            if mgmt_id and mgmt_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO management (id, name) VALUES (?, ?)",
                    (mgmt_id, mgmt_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO occurrence_management (occurrence_id, management_id) VALUES (?, ?)",
                    (occ_id, mgmt_id)
                )

        # Person roles
        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_role_id(cursor, role_name_in_table)
            if not role_id:
                continue

            persons = source.get(role_field, [])
            if isinstance(persons, dict):
                persons = [persons]
            elif not isinstance(persons, list):
                persons = []

            for p in persons:
                person_id = str(p.get('id', ''))
                if person_id:
                    # With foreign keys disabled, we can insert the relationship
                    # even if the person doesn't exist yet (they'll be added later)
                    cursor.execute(
                        "INSERT OR IGNORE INTO occurrence_person_roles (occurrence_id, person_id, role_id) VALUES (?, ?, ?)",
                        (occ_id, person_id, role_id)
                    )


        batch_count += 1
        if batch_count % 1000 == 0:
            cursor.execute("COMMIT")
            cursor.execute("BEGIN")
            print(f"Processed {batch_count} occurrences...")

    cursor.execute("COMMIT")

    # RE-ENABLE FOREIGN KEY CONSTRAINTS
    cursor.execute("PRAGMA foreign_keys = ON")
    print("Foreign key constraints re-enabled")

    conn.close()

    print(f"Occurrences migration completed: {batch_count} occurrences updated")


if __name__ == "__main__":
    migrate_occurrences()