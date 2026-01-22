import uuid
from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME, insert_many_to_many, insert_many_to_one,
    get_postgres_connection
)

def get_library_for_manuscript(pg_cursor, manuscript_id):
    pg_cursor.execute("""
        SELECT DISTINCT
            i.identity AS library_id,
            i.name AS library_name,
            i.idregion AS location_id
        FROM data.located_at la
        JOIN data.location l ON la.idlocation = l.idlocation
        JOIN data.fund f ON l.idfund = f.idfund
        JOIN data.institution i ON f.idlibrary = i.identity
        WHERE la.iddocument = %s
    """, (manuscript_id,))
    return pg_cursor.fetchone()

def insert_library(cursor, library_id, name, location_id):
    cursor.execute("""
        INSERT OR IGNORE INTO libraries (id, name, location_id)
        VALUES (?, ?, ?)
    """, (str(library_id), name, str(location_id) if location_id else None))

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
        ("library_id", "TEXT"),
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
    CREATE TABLE IF NOT EXISTS manuscript_location (
        manuscript_id TEXT NOT NULL,
        location_id TEXT NOT NULL,
        PRIMARY KEY (manuscript_id, location_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id),
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
    """)

def get_region_hierarchy(pg_cursor, location_id):
    hierarchy = []
    current_id = location_id
    while current_id:
        pg_cursor.execute("""
            SELECT identity, name, historical_name, parent_idregion
            FROM data.region
            WHERE identity = %s
        """, (current_id,))
        row = pg_cursor.fetchone()
        if not row:
            break
        identity, name, historical_name, parent_id = row
        hierarchy.append((str(identity), name, historical_name, str(parent_id) if parent_id else None))
        current_id = parent_id
    return hierarchy[::-1]

def insert_location_hierarchy(cursor, hierarchy):
    leaf_id = None
    for location_id, name, historical_name, parent_id in hierarchy:
        cursor.execute("""
            INSERT OR IGNORE INTO locations (id, name, historical_name, parent_id)
            VALUES (?, ?, ?, ?)
        """, (location_id, name, historical_name, parent_id))
        leaf_id = location_id
    return leaf_id

def link_manuscript_to_locations(cursor, manuscript_id, pg_cursor):
    pg_cursor.execute("""
        SELECT f.idlocation
        FROM data.factoid f
        JOIN data.factoid_type ft
            ON f.idfactoid_type = ft.idfactoid_type
        WHERE f.subject_identity = %s
          AND ft.type = 'written'
    """, (manuscript_id,))
    loc_rows = pg_cursor.fetchall()

    for (idlocation,) in loc_rows:
        pg_cursor.execute("""
            SELECT idregion
            FROM data.location
            WHERE idlocation = %s
        """, (idlocation,))
        row = pg_cursor.fetchone()
        if not row:
            continue
        idregion = row[0]

        hierarchy = get_region_hierarchy(pg_cursor, idregion)

        leaf_id = insert_location_hierarchy(cursor, hierarchy)

        if leaf_id:
            cursor.execute("""
                INSERT OR IGNORE INTO manuscript_location(manuscript_id, location_id)
                VALUES (?, ?)
            """, (manuscript_id, leaf_id))


def migrate_manuscript_content():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    cursor.execute("PRAGMA foreign_keys = OFF")
    print("Foreign key constraints disabled for content migration")

    # Step 1: fetch only content nodes
    pg_cursor.execute("""
        SELECT idgenre, idparentgenre, genre
        FROM data.genre
        WHERE is_content = TRUE
    """)

    rows = pg_cursor.fetchall()
    print(f"Fetched {len(rows)} content nodes from Postgres (is_content=True)")

    cursor.execute("BEGIN")

    # Step 2: insert all nodes first
    for idgenre, _, genre in rows:
        cursor.execute("""
            INSERT OR IGNORE INTO content (id, name)
            VALUES (?, ?)
        """, (str(idgenre), genre))

    # Step 3: wire up parent relationships
    for idgenre, idparentgenre, _ in rows:
        if idparentgenre is not None:
            cursor.execute("""
                UPDATE content
                SET parent_id = ?
                WHERE id = ?
            """, (str(idparentgenre), str(idgenre)))

    cursor.execute("COMMIT")
    cursor.execute("PRAGMA foreign_keys = ON")
    print("Foreign key constraints re-enabled")

    conn.close()
    pg_conn.close()
    print("Content migration completed")



def get_deepest_leaf_from_postgres(pg_cursor, content_ids):
    if not content_ids:
        return []

    content_ids = [int(cid) for cid in content_ids]

    pg_cursor.execute(f"""
        SELECT idgenre, idparentgenre
        FROM data.genre
        WHERE idgenre IN %s
          AND is_content = TRUE
    """, (tuple(content_ids),))
    rows = pg_cursor.fetchall()

    parents_in_list = set(row[1] for row in rows if row[1] is not None)

    leaf_ids = [str(cid) for cid in content_ids if cid not in parents_in_list]

    return leaf_ids

def migrate_manuscripts():
    migrate_manuscript_content()
    es = get_es_client()
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()


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

        link_manuscript_to_locations(cursor, manuscript_id, pg_cursor)

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
        ]

        for cfg in MANUSCRIPT_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=manuscript_id,
                **cfg,
            )

        insert_many_to_one(cursor, "collection", "collections", manuscript_id, source.get("collection"))

        lib = get_library_for_manuscript(pg_cursor, manuscript_id)

        if lib:
            library_id, library_name, location_id = lib

            if location_id:
                hierarchy = get_region_hierarchy(pg_cursor, location_id)
                insert_location_hierarchy(cursor, hierarchy)

            insert_library(cursor, library_id, library_name, location_id)
            cursor.execute("""
                UPDATE manuscripts
                SET library_id = ?
                WHERE id = ?
            """, (str(library_id), manuscript_id))



        MANUSCRIPT_IDENT_TYPE_MAP = {
            "diktyon": "diktyon"
        }

        content_list = source.get("content", [])
        content_ids = [c.get("id") for c in content_list if c.get("id")]

        leaf_ids = get_deepest_leaf_from_postgres(pg_cursor, content_ids)

        for leaf_id in leaf_ids:
            cursor.execute("""
                INSERT OR IGNORE INTO manuscript_content (manuscript_id, content_id)
                VALUES (?, ?)
            """, (manuscript_id, leaf_id))

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
