# app/migrate_bibliographies/schema.py
from ..common import get_db_connection, add_column_if_missing

def create_schema():
    conn, cursor = get_db_connection()

    person_columns = {
        "first_name": "TEXT",
        "last_name": "TEXT",
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
        "private_comment": "TEXT",
        "location_id": "TEXT",
    }

    for col, col_type in person_columns.items():
        add_column_if_missing(cursor, "persons", col, col_type)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_management (
        person_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, management_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_acknowledgement (
        person_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, acknowledgement_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgements(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_self_designations (
        person_id INTEGER NOT NULL,
        self_designation_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, self_designation_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (self_designation_id) REFERENCES self_designations(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_offices (
        person_id INTEGER NOT NULL,
        office_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, office_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (office_id) REFERENCES offices(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS person_identification (
        person_id INTEGER NOT NULL,
        identification_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, identification_id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)
    conn.commit()
    conn.close()