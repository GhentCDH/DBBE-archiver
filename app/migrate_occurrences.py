from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME,
    insert_many_to_many, get_postgres_connection
)

def get_related_occurrences(occ_id, pg_cursor):
    pg_cursor.execute("""
        SELECT b.idoriginal_poem
        FROM data.original_poem_verse a
        INNER JOIN data.original_poem_verse b ON a.idgroup = b.idgroup
        WHERE a.idoriginal_poem = %s AND b.idoriginal_poem <> a.idoriginal_poem
        GROUP BY b.idoriginal_poem

        UNION

        SELECT fb.subject_identity
        FROM data.factoid fa
        INNER JOIN data.factoid_type fta ON fa.idfactoid_type = fta.idfactoid_type
        INNER JOIN data.factoid fb ON fa.object_identity = fb.object_identity
        INNER JOIN data.factoid_type ftb ON fb.idfactoid_type = ftb.idfactoid_type
        WHERE fa.subject_identity = %s
        AND fta.type = 'reconstruction of'
        AND ftb.type = 'reconstruction of'
        AND fb.subject_identity <> fa.subject_identity
        AND fb.subject_identity NOT IN (
            SELECT b.idoriginal_poem
            FROM data.original_poem_verse a
            INNER JOIN data.original_poem_verse b ON a.idgroup = b.idgroup
            WHERE a.idoriginal_poem = %s
            AND b.idoriginal_poem <> a.idoriginal_poem
            GROUP BY b.idoriginal_poem
        )
    """, (occ_id, occ_id, occ_id))
    return [row[0] for row in pg_cursor.fetchall()]

def create_occurrence_tables(cursor):
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
    
    cursor.execute("""
    INSERT OR IGNORE INTO occurrence_relation_definitions (id, definition) VALUES
    ('0', 'verse_related'),
    ('1', 'type_related')
    """)


def migrate_occurrences():
    es = get_es_client()
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    cursor.execute("PRAGMA foreign_keys = OFF")
    print("Foreign key constraints disabled for migration")

    create_occurrence_tables(cursor)

    indices = get_dbbe_indices(es)
    occ_index = next((idx for idx in indices if idx.endswith("occurrences")), None)

    if not occ_index:
        print("No occurrence index found")
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

        cursor.execute("""
        UPDATE occurrences SET
            created=?, modified=?, public_comment=?, private_comment=?,
            is_dbbe=?, incipit=?, text_stemmer=?, text_original=?,
            location_in_ms=?, date_floor_year=?, date_ceiling_year=?,
            palaeographical_info=?, contextual_info=?, manuscript_id=?, title=?
        WHERE id=?
        """, (
            source.get('created', ''),
            source.get('modified', ''),
            source.get('public_comment', ''),
            source.get('private_comment', ''),
            bool(source.get('dbbe', False)),
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

        OCCURRENCE_M2M = [
            {
                "source_key": "genre",
                "entity_table": "genres",
                "join_table": "occurrence_genres",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "genre_id",
            },
            {
                "source_key": "metre",
                "entity_table": "metres",
                "join_table": "occurrence_metres",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "metre_id",
            },
            {
                "source_key": "acknowledgement",
                "entity_table": "acknowledgements",
                "join_table": "occurrence_acknowledgement",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "acknowledgement_id",
            },
            {
                "source_key": "management",
                "entity_table": "management",
                "join_table": "occurrence_management",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "management_id",
            },
            {
                "source_key": "subject",
                "entity_table": "subjects",
                "join_table": "occurrence_subject",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "subject_id",
            },
        ]

        for cfg in OCCURRENCE_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=occ_id,
                **cfg
            )

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
                    (occ_id, ts_id))

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
                    cursor.execute(
                        "INSERT OR IGNORE INTO occurrence_person_roles (occurrence_id, person_id, role_id) VALUES (?, ?, ?)",
                        (occ_id, person_id, role_id)
                    )


        related_ids = get_related_occurrences(occ_id, pg_cursor)
        if related_ids:
            related_rows = [(occ_id, rid, '0') for rid in related_ids]
            cursor.executemany("""
                INSERT OR IGNORE INTO occurrence_related_occurrences
                (occurrence_id, related_occurrence_id, relation_definition_id)
                VALUES (?, ?, ?)
            """, related_rows)

        batch_count += 1
        if batch_count % 1000 == 0:
            cursor.execute("COMMIT")
            cursor.execute("BEGIN")
            print(f"Processed {batch_count} occurrences...")

    cursor.execute("COMMIT")

    cursor.execute("PRAGMA foreign_keys = ON")
    print("Foreign key constraints re-enabled")

    conn.close()

    print(f"Occurrences migration completed: {batch_count} occurrences updated")

if __name__ == "__main__":
    migrate_occurrences()