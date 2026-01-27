
import uuid
from ..common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing
)


def create_person_tables(cursor):
    person_columns = {
        "full_name": "TEXT",
        "born_date_floor_year": "TEXT",
        "born_date_ceiling_year": "TEXT",
        "death_date_floor_year": "TEXT",
        "death_date_ceiling_year": "TEXT",
        "is_dbbe_person": "BOOLEAN",
        "is_modern_person": "BOOLEAN",
        "is_historical_person": "BOOLEAN",
        "modified": "TEXT",
        "created": "TEXT",
        "public_comment": "TEXT",
        "private_comment": "TEXT"
    }
    
    for col, col_type in person_columns.items():
        add_column_if_missing(cursor, "persons", col, col_type)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_management (
        person_id TEXT NOT NULL,
        management_id TEXT NOT NULL,
        PRIMARY KEY (person_id, management_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_acknowledgement (
        person_id TEXT NOT NULL,
        acknowledgement_id TEXT NOT NULL,
        PRIMARY KEY (person_id, acknowledgement_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgements(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_self_designations (
        person_id TEXT NOT NULL,
        self_designation_id TEXT NOT NULL,
        PRIMARY KEY (person_id, self_designation_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (self_designation_id) REFERENCES self_designations(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_offices (
        person_id TEXT NOT NULL,
        office_id TEXT NOT NULL,
        PRIMARY KEY (person_id, office_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (office_id) REFERENCES offices(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_identification (
        person_id TEXT NOT NULL,
        identification_id TEXT NOT NULL,
        PRIMARY KEY (person_id, identification_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)


def migrate_persons():
    """
    Migrate persons from Elasticsearch to SQLite.
    """
    es = get_es_client()
    conn, cursor = get_db_connection()
    
    # Create person tables
    create_person_tables(cursor)
    
    # Get indices
    indices = get_dbbe_indices(es)
    person_index = next((idx for idx in indices if idx.endswith("persons")), None)
    
    if not person_index:
        print("No person index found")
        conn.close()
        return
    
    print(f"Migrating persons from index: {person_index}")
    hits = scroll_all(es, person_index)
    print(f"Total persons fetched: {len(hits)}")
    
    cursor.execute("BEGIN")
    
    for hit in hits:
        source = hit['_source']
        person_id = str(source.get('id', hit['_id']))
        
        # Insert or update person
        cursor.execute("""
        INSERT INTO persons (
            id, full_name, born_date_floor_year, born_date_ceiling_year,
            death_date_floor_year, death_date_ceiling_year,
            is_dbbe_person, is_modern_person, is_historical_person,
            modified, created, public_comment, private_comment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            full_name = excluded.full_name,
            born_date_floor_year = excluded.born_date_floor_year,
            born_date_ceiling_year = excluded.born_date_ceiling_year,
            death_date_floor_year = excluded.death_date_floor_year,
            death_date_ceiling_year = excluded.death_date_ceiling_year,
            is_dbbe_person = excluded.is_dbbe_person,
            is_modern_person = excluded.is_modern_person,
            is_historical_person = excluded.is_historical_person,
            modified = excluded.modified,
            created = excluded.created,
            public_comment = excluded.public_comment,
            private_comment = excluded.private_comment
        """, (
            person_id,
            source.get('name'),
            source.get('born_date_floor_year'),
            source.get('born_date_ceiling_year'),
            source.get('death_date_floor_year'),
            source.get('death_date_ceiling_year'),
            bool(source.get('is_dbbe_person', False)),
            bool(source.get('is_modern_person', False)),
            bool(source.get('is_historical_person', False)),
            source.get('modified'),
            source.get('created'),
            source.get('public_comment'),
            source.get('private_comment')
        ))
        
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
                    "INSERT OR IGNORE INTO person_management (person_id, management_id) VALUES (?, ?)",
                    (person_id, mgmt_id)
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
                    "INSERT OR IGNORE INTO person_acknowledgement (person_id, acknowledgement_id) VALUES (?, ?)",
                    (person_id, ack_id)
                )
        
        # Self designations
        for sd in source.get('self_designation', []):
            sd_id = str(sd.get('id', ''))
            sd_name = sd.get('name', '')
            if sd_id and sd_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO self_designations (id, name) VALUES (?, ?)",
                    (sd_id, sd_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO person_self_designations (person_id, self_designation_id) VALUES (?, ?)",
                    (person_id, sd_id)
                )
        
        # Offices
        for office in source.get('office', []):
            office_id = str(office.get('id', ''))
            office_name = office.get('name', '')
            if office_id and office_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO offices (id, name) VALUES (?, ?)",
                    (office_id, office_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO person_offices (person_id, office_id) VALUES (?, ?)",
                    (person_id, office_id)
                )
        
        # Identifications
        PERSON_IDENT_TYPE_MAP = {
            "viaf": "viaf",
            "plre": "plre",
            "pbw": "pbw"
        }
        
        for es_field, ident_type in PERSON_IDENT_TYPE_MAP.items():
            for identifier in source.get(es_field, []):
                if not identifier:
                    continue
                
                ident_id = str(uuid.uuid4())
                cursor.execute(
                    "INSERT OR IGNORE INTO identifications (id, type, identifier_value) VALUES (?, ?, ?)",
                    (ident_id, ident_type, identifier)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO person_identification (person_id, identification_id) VALUES (?, ?)",
                    (person_id, ident_id)
                )
    
    cursor.execute("COMMIT")
    conn.close()
    
    print(f"Persons migration completed: {len(hits)} persons inserted")


if __name__ == "__main__":
    migrate_persons()
