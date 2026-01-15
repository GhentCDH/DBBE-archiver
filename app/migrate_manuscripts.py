import uuid
from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME, insert_many_to_many, insert_many_to_one
)

def create_manuscript_tables(cursor):
    manuscript_columns = [
        ("name", "TEXT"),
        ("date_floor_year", "INTEGER"),
        ("date_ceiling_year", "INTEGER"),
        ("created", "TEXT"),
        ("modified", "TEXT"),
        ("number_of_occurrences", "INTEGER"),
        ("shelf", "TEXT"),
        ("city_id", "INTEGER"),
        ("library_id", "INTEGER"),
        ("collection_id", "INTEGER")
    ]
    
    for col, col_type in manuscript_columns:
        add_column_if_missing(cursor, "manuscripts", col, col_type)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_person_roles (
        manuscript_id TEXT NOT NULL,
        person_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, person_id, role_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_management (
        manuscript_id TEXT NOT NULL,
        management_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, management_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_acknowledgement (
        manuscript_id TEXT NOT NULL,
        acknowledgement_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, acknowledgement_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgements(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_content (
        manuscript_id TEXT NOT NULL,
        content_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, content_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_identification (
        manuscript_id TEXT NOT NULL,
        identification_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, identification_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manuscript_origin (
        manuscript_id TEXT NOT NULL,
        origin_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, origin_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (origin_id) REFERENCES origins(id)
    )
    """)


def migrate_manuscripts():
    es = get_es_client()
    conn, cursor = get_db_connection()

    create_manuscript_tables(cursor)

    indices = get_dbbe_indices(es)
    manuscript_index = next((idx for idx in indices if idx.endswith("manuscripts")), None)
    
    if not manuscript_index:
        print("No manuscript index found")
        conn.close()
        return
    
    print(f"Migrating manuscripts from index: {manuscript_index}")
    hits = scroll_all(es, manuscript_index)
    print(f"Total manuscripts fetched: {len(hits)}")
    
    cursor.execute("BEGIN")
    
    for hit in hits:
        source = hit['_source']
        manuscript_id = str(source.get('id', hit['_id']))

        cursor.execute("""
        INSERT INTO manuscripts (
            id, name, date_floor_year, date_ceiling_year,
            created, modified, number_of_occurrences, shelf
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            date_floor_year = excluded.date_floor_year,
            date_ceiling_year = excluded.date_ceiling_year,
            created = excluded.created,
            modified = excluded.modified,
            number_of_occurrences = excluded.number_of_occurrences,
            shelf = excluded.shelf
        """, (
            manuscript_id,
            source.get('name'),
            source.get('date_floor_year'),
            source.get('date_ceiling_year'),
            source.get('created'),
            source.get('modified'),
            source.get('number_of_occurrences'),
            source.get('shelf')
        ))

        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_role_id(cursor, role_name_in_table)
            if not role_id:
                continue
            
            persons = source.get(role_field, [])
            if isinstance(persons, list):
                for p in persons:
                    person_id = str(p.get('id', ''))
                    if not person_id:
                        continue
                    
                    cursor.execute("SELECT 1 FROM persons WHERE id=?", (person_id,))
                    person_exists = cursor.fetchone()
                    
                    if person_exists:
                        cursor.execute(
                            "INSERT OR IGNORE INTO manuscript_person_roles (manuscript_id, person_id, role_id) VALUES (?, ?, ?)",
                            (manuscript_id, person_id, role_id)
                        )

        MANUSCRIPT_M2M = [
            {
                "source_key": "management",
                "entity_table": "management",
                "join_table": "manuscript_management",
                "parent_id_col": "manuscript_id",
                "entity_id_col": "management_id",
            },
            {
                "source_key": "acknowledgement",
                "entity_table": "acknowledgements",
                "join_table": "manuscript_acknowledgement",
                "parent_id_col": "manuscript_id",
                "entity_id_col": "acknowledgement_id",
            },
            {
                "source_key": "content",
                "entity_table": "content",
                "join_table": "manuscript_content",
                "parent_id_col": "manuscript_id",
                "entity_id_col": "content_id",
            },
            {
                "source_key": "origin",
                "entity_table": "origins",
                "join_table": "manuscript_origin",
                "parent_id_col": "manuscript_id",
                "entity_id_col": "origin_id",
            },
        ]

        for cfg in MANUSCRIPT_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=manuscript_id,
                **cfg,
            )

        insert_many_to_one(cursor, "city", "cities", manuscript_id, source.get("city"))
        insert_many_to_one(cursor, "library", "libraries", manuscript_id, source.get("library"))
        insert_many_to_one(cursor, "collection", "collections", manuscript_id, source.get("collection"))

        MANUSCRIPT_IDENT_TYPE_MAP = {
            "diktyon": "diktyon"
        }
        
        for es_field, ident_type in MANUSCRIPT_IDENT_TYPE_MAP.items():
            for identifier in source.get(es_field, []):
                if not identifier:
                    continue
                ident_id = str(uuid.uuid4())
                cursor.execute(
                    "INSERT OR IGNORE INTO identifications (id, type, identifier_value) VALUES (?, ?, ?)",
                    (ident_id, ident_type, identifier)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO manuscript_identification (manuscript_id, identification_id) VALUES (?, ?)",
                    (manuscript_id, ident_id)
                )
    
    cursor.execute("COMMIT")
    conn.close()
    
    print(f"Manuscripts migration completed: {len(hits)} manuscripts inserted")


if __name__ == "__main__":
    migrate_manuscripts()
