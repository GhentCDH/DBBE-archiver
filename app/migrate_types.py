"""
Migrate types data from Elasticsearch to SQLite.
"""
from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME
)


def create_type_tables(cursor):
    """
    Create all type-related tables.
    """
    # Add columns to types table
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
    
    # Create junction tables
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
    CREATE TABLE IF NOT EXISTS type_subject (
        type_id TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        PRIMARY KEY (type_id, subject_id),
        FOREIGN KEY (type_id) REFERENCES types(id),
        FOREIGN KEY (subject_id) REFERENCES subjects(id)
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
        FOREIGN KEY (relation_definition_id) REFERENCES type_relation_definitions(id),
        CHECK (type_id <> related_type_id)
    )
    """)


def migrate_types():
    """
    Migrate types from Elasticsearch to SQLite.
    """
    es = get_es_client()
    conn, cursor = get_db_connection()
    
    # Create type tables
    create_type_tables(cursor)
    
    # Get indices
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
        
        # Get verses from occurrences
        verses = []
        for occ in source.get('occurrence', []):
            if isinstance(occ, dict) and 'verse' in occ:
                verses.extend(occ.get('verse', []))
        
        number_of_verses = len(verses) if verses else None
        
        # Insert or update type
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
        
        # Tags
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
                    "INSERT OR IGNORE INTO type_management (type_id, management_id) VALUES (?, ?)",
                    (type_id, mgmt_id)
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
                    "INSERT OR IGNORE INTO type_acknowledgement (type_id, acknowledgement_id) VALUES (?, ?)",
                    (type_id, ack_id)
                )
        
        # Genres
        for genre in source.get('genre', []):
            genre_id = str(genre.get('id', ''))
            genre_name = genre.get('name', '')
            if genre_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO genres (id, name) VALUES (?, ?)",
                    (genre_id, genre_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO type_genre (type_id, genre_id) VALUES (?, ?)",
                    (type_id, genre_id)
                )
        
        # Metres
        for metre in source.get('metre', []):
            metre_id = str(metre.get('id', ''))
            metre_name = metre.get('name', '')
            if metre_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO metres (id, name) VALUES (?, ?)",
                    (metre_id, metre_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO type_metre (type_id, metre_id) VALUES (?, ?)",
                    (type_id, metre_id)
                )
        
        # Editorial/critical status
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
                    "INSERT OR IGNORE INTO type_text_statuses (type_id, text_status_id) VALUES (?, ?)",
                    (type_id, ts_id)
                )
        
        # Subjects
        for subj in source.get('subject', []):
            subj_id = str(subj.get('id', ''))
            subj_name = subj.get('name', '')
            if subj_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO subjects (id, name) VALUES (?, ?)",
                    (subj_id, subj_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO type_subject (type_id, subject_id) VALUES (?, ?)",
                    (type_id, subj_id)
                )
        
        # Occurrences
        for occ in source.get('occurrence', []):
            occ_id = str(occ.get('id', ''))
            if occ_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO type_occurrences (type_id, occurrence_id) VALUES (?, ?)",
                    (type_id, occ_id)
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
    conn.close()
    
    print(f"Types migration completed: {batch_count} types inserted")


if __name__ == "__main__":
    migrate_types()
