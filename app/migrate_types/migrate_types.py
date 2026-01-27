from ..common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME, insert_many_to_many, get_postgres_connection
)

def fetch_type_relations(pg_conn):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT
            f.subject_identity,
            f.object_identity,
            ft.idfactoid_type,
            ft.type
        FROM data.factoid f
        JOIN data.factoid_type ft
            ON f.idfactoid_type = ft.idfactoid_type
        WHERE ft.group = 'reconstructed_poem_related_to_reconstructed_poem'
    """)
    relations = pg_cursor.fetchall()
    pg_cursor.close()
    return relations


def create_type_tables(cursor):
    type_columns = {
        "text_stemmer": "TEXT",
        "text_original": "TEXT",
        "lemma": "TEXT",
        "incipit": "TEXT",
        "created": "TEXT",
        "modified": "TEXT",
        "public_comment": "TEXT",
        "private_comment": "TEXT",
        "title": "TEXT",
        "number_of_verses": "INTEGER"
    }
    
    for col, col_type in type_columns.items():
        add_column_if_missing(cursor, "types", col, col_type)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_management (
        type_id TEXT NOT NULL,
        management_id TEXT NOT NULL,
        PRIMARY KEY (type_id, management_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_acknowledgement (
        type_id TEXT NOT NULL,
        acknowledgement_id TEXT NOT NULL,
        PRIMARY KEY (type_id, acknowledgement_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgements(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_tags (
        type_id TEXT NOT NULL,
        tag_id TEXT NOT NULL,
        PRIMARY KEY (type_id, tag_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_genre (
        type_id TEXT NOT NULL,
        genre_id TEXT NOT NULL,
        PRIMARY KEY (type_id, genre_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (genre_id) REFERENCES genres(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_metre (
        type_id TEXT NOT NULL,
        metre_id TEXT NOT NULL,
        PRIMARY KEY (type_id, metre_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (metre_id) REFERENCES metres(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_editorial_status (
        type_id TEXT NOT NULL,
        editorial_status_id TEXT NOT NULL,
        PRIMARY KEY (type_id, editorial_status_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (editorial_status_id) REFERENCES editorial_statuses(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_text_statuses (
        type_id TEXT NOT NULL,
        text_status_id TEXT NOT NULL,
        PRIMARY KEY (type_id, text_status_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (text_status_id) REFERENCES text_statuses(id)
    )
    """)

    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_person_roles (
        type_id TEXT NOT NULL,
        person_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        PRIMARY KEY (type_id, person_id, role_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_occurrences (
        type_id TEXT NOT NULL,
        occurrence_id TEXT NOT NULL,
        PRIMARY KEY (type_id, occurrence_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_related_types (
        type_id TEXT NOT NULL,
        related_type_id TEXT NOT NULL,
        relation_definition_id TEXT NOT NULL,
        PRIMARY KEY (type_id, related_type_id, relation_definition_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (related_type_id) REFERENCES types(id),
        FOREIGN KEY (related_type_id) REFERENCES types(id),
        FOREIGN KEY (relation_definition_id) REFERENCES type_relation_definitions(id),
        CHECK (type_id <> related_type_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_relation_definitions (
        id TEXT PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_related_types (
        type_id TEXT NOT NULL,
        related_type_id TEXT NOT NULL,
        relation_definition_id TEXT NOT NULL,
        PRIMARY KEY (type_id, related_type_id, relation_definition_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (related_type_id) REFERENCES types(id),
        FOREIGN KEY (relation_definition_id) REFERENCES type_relation_definitions(id),
        CHECK (type_id <> related_type_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_keyword (
        type_id TEXT NOT NULL,
        keyword_id TEXT NOT NULL,
        PRIMARY KEY (type_id, keyword_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (keyword_id) REFERENCES keyword(id)
    );
    """)


def get_subject_keyword(pg_cursor, subject_id):
    pg_cursor.execute("""
        SELECT identity, keyword
        FROM data.keyword
        WHERE identity = %s
          AND is_subject = true
    """, (subject_id,))
    return pg_cursor.fetchone()

def migrate_types():
    es = get_es_client()
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    create_type_tables(cursor)
    
    indices = get_dbbe_indices(es)
    type_index = next((idx for idx in indices if idx.endswith("types")), None)
    
    if not type_index:
        print("No type index found")
        conn.close()
        return
    
    print(f"Migrating types from index: {type_index}")
    hits = scroll_all(es, type_index)
    print(f"Total types fetched: {len(hits)}")
    
    cursor.execute("BEGIN TRANSACTION")
    batch_count = 0
    
    for hit in hits:
        source = hit['_source']
        type_id = str(source.get('id', hit['_id']))

        verses = []
        for occ in source.get('occurrence', []):
            if isinstance(occ, dict) and 'verse' in occ:
                verses.extend(occ.get('verse', []))
        
        number_of_verses = len(verses) if verses else None

        cursor.execute("""
        INSERT INTO types (
            id, text_stemmer, text_original, lemma, incipit,
            created, modified, public_comment, private_comment,
            title, number_of_verses
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            text_stemmer = excluded.text_stemmer,
            text_original = excluded.text_original,
            lemma = excluded.lemma,
            incipit = excluded.incipit,
            created = excluded.created,
            modified = excluded.modified,
            public_comment = excluded.public_comment,
            private_comment = excluded.private_comment,
            title = excluded.title,
            number_of_verses = excluded.number_of_verses
        """, (
            type_id,
            source.get('text_stemmer'),
            source.get('text_original'),
            source.get('lemma'),
            source.get('incipit'),
            source.get('created'),
            source.get('modified'),
            source.get('public_comment'),
            source.get('private_comment'),
            source.get('title_original'),
            number_of_verses
        ))

        for tag in source.get('tag', []):
            tag_id = str(tag.get('id', ''))
            tag_name = tag.get('name', '')
            if tag_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO tags (id, name) VALUES (?, ?)",
                    (tag_id, tag_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO type_tags (type_id, tag_id) VALUES (?, ?)",
                    (type_id, tag_id)
                )


        cs = source.get('critical_status')
        if isinstance(cs, dict):
            cs_id = str(cs.get('id', ''))
            cs_name = cs.get('name', '')
            if cs_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO editorial_statuses (id, name) VALUES (?, ?)",
                    (cs_id, cs_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO type_editorial_status (type_id, editorial_status_id) VALUES (?, ?)",
                    (type_id, cs_id)
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
                    "INSERT OR IGNORE INTO type_text_statuses (type_id, text_status_id) VALUES (?, ?)",
                    (type_id, ts_id)
                )

        subjects = source.get("subject", [])
        if isinstance(subjects, dict):
            subjects = [subjects]
        elif not isinstance(subjects, list):
            subjects = []
        for subj in subjects:
            subject_id = str(subj.get("id", ""))
            if not subject_id:
                continue
            pg_keyword = get_subject_keyword(pg_cursor, subject_id)
            if not pg_keyword:
                continue
            keyword_id, keyword_name = pg_keyword
            cursor.execute(
                "INSERT OR IGNORE INTO keyword (id, name) VALUES (?, ?)",
                (keyword_id, keyword_name)
            )

            cursor.execute(
                "INSERT OR IGNORE INTO type_keyword (type_id, keyword_id) VALUES (?, ?)",
                (type_id, keyword_id)
            )

        TYPES_M2M = [
            {
                "source_key": "genre",
                "entity_table": "genres",
                "join_table": "type_genre",
                "parent_id_col": "type_id",
                "entity_id_col": "genre_id",
            },
            {
                "source_key": "metre",
                "entity_table": "metres",
                "join_table": "type_metre",
                "parent_id_col": "type_id",
                "entity_id_col": "metre_id",
            },
            {
                "source_key": "acknowledgement",
                "entity_table": "acknowledgements",
                "join_table": "type_acknowledgement",
                "parent_id_col": "type_id",
                "entity_id_col": "acknowledgement_id",
            },
            {
                "source_key": "management",
                "entity_table": "management",
                "join_table": "type_management",
                "parent_id_col": "type_id",
                "entity_id_col": "management_id",
            },
        ]

        for cfg in TYPES_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=type_id,
                **cfg
            )

        for occ_id in source.get('occurrence_ids', []):
            occ_id = str(occ_id)

            cursor.execute(
                "SELECT 1 FROM occurrences WHERE id=?",
                (occ_id,)
            )
            if cursor.fetchone() is None:
                continue

            cursor.execute(
                "INSERT OR IGNORE INTO type_occurrences (type_id, occurrence_id) VALUES (?, ?)",
                (type_id, occ_id)
            )

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
                if not person_id:
                    continue
                
                cursor.execute("SELECT 1 FROM persons WHERE id=?", (person_id,))
                if cursor.fetchone() is None:
                    continue
                
                cursor.execute(
                    "INSERT OR IGNORE INTO type_person_roles (type_id, person_id, role_id) VALUES (?, ?, ?)",
                    (type_id, person_id, role_id)
                )
        
        batch_count += 1
        if batch_count % 1000 == 0:
            cursor.execute("COMMIT")
            cursor.execute("BEGIN TRANSACTION")
            print(f"Processed {batch_count} types...")
    
    cursor.execute("COMMIT")


    relations = fetch_type_relations(pg_conn)
    cursor.execute("BEGIN TRANSACTION")

    for _, _, rel_def_id, rel_code in relations:
        cursor.execute("""
            INSERT OR IGNORE INTO type_relation_definitions (id, definition)
            VALUES (?, ?)
        """, (str(rel_def_id), rel_code))

    for type_id, related_type_id, rel_def_id, _ in relations:
        cursor.execute("""
            INSERT OR IGNORE INTO type_related_types
            (type_id, related_type_id, relation_definition_id)
            VALUES (?, ?, ?)
        """, (
            str(type_id),
            str(related_type_id),
            str(rel_def_id)
        ))

    cursor.execute("COMMIT")
    pg_conn.close()
    conn.close()


    print(f"Types migration completed: {batch_count} types inserted")


if __name__ == "__main__":
    migrate_types()
