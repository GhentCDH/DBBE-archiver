# app/migrate_bibliographies/schema.py
from ..common import execute_with_normalization, get_db_connection, add_column_if_missing

def create_schema():
    conn, cursor = get_db_connection()

    person_columns = {
        "first_name": "TEXT",
        "last_name": "TEXT",
        "born_date_floor": "TEXT",
        "born_date_ceiling": "TEXT",
        "death_date_floor": "TEXT",
        "death_date_ceiling": "TEXT",
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
        add_column_if_missing(cursor, "person", col, col_type)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS person_management (
        person_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, management_id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS person_acknowledgement (
        person_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, acknowledgement_id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgement(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS person_self_designation (
        person_id INTEGER NOT NULL,
        self_designation_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, self_designation_id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (self_designation_id) REFERENCES self_designation(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS person_office (
        person_id INTEGER NOT NULL,
        office_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, office_id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (office_id) REFERENCES office(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS person_identification (
        person_id INTEGER NOT NULL,
        identification_id INTEGER NOT NULL,
        PRIMARY KEY (person_id, identification_id),
        FOREIGN KEY (person_id) REFERENCES person(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)
    conn.commit()
    conn.close()