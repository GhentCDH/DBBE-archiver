import uuid

from ..common import (execute_with_normalization,
                      get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
                      add_column_if_missing, get_or_create_role, ROLE_FIELD_TO_ROLE_NAME, insert_many_to_many, insert_many_to_one,
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
    execute_with_normalization(cursor, """
        INSERT OR IGNORE INTO library (id, name, location_id)
        VALUES (?, ?, ?)
    """, (int(library_id), name, int(location_id) if location_id else None))

def create_manuscript_tables(cursor):
    manuscript_columns = [
        ("name", "TEXT"),
        ("date_floor", "INTEGER"),
        ("date_ceiling", "INTEGER"),
        ("created", "TEXT"),
        ("modified", "TEXT"),
        ("number_of_occurrences", "INTEGER"),
        ("shelf", "TEXT"),
        ("library_id", "INTEGER"),
        ("collection_id", "INTEGER")
    ]
    
    for col, col_type in manuscript_columns:
        add_column_if_missing(cursor, "manuscript", col, col_type)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_person_role (
        manuscript_id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, person_id, role_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_management (
        manuscript_id INTEGER NOT NULL,
        management_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, management_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_acknowledgement (
        manuscript_id INTEGER NOT NULL,
        acknowledgement_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, acknowledgement_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (acknowledgement_id) REFERENCES acknowledgement(id)
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_content (
        manuscript_id INTEGER NOT NULL,
        content_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, content_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (content_id) REFERENCES content(id)
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_identification (
        manuscript_id INTEGER NOT NULL,
        identification_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, identification_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (identification_id) REFERENCES identifications(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript_location (
        manuscript_id INTEGER NOT NULL,
        origin_id INTEGER NOT NULL,
        PRIMARY KEY (manuscript_id, origin_id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscript(id),
        FOREIGN KEY (origin_id) REFERENCES location(id)
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
        hierarchy.append((int(identity), name, historical_name, int(parent_id) if parent_id else None))
        current_id = parent_id
    return hierarchy[::-1]

def insert_location_hierarchy(cursor, hierarchy):
    leaf_id = None
    for location_id, name, historical_name, parent_id in hierarchy:
        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO location (id, name, historical_name, parent_id)
            VALUES (?, ?, ?, ?)
        """, (location_id, name, historical_name, parent_id))
        leaf_id = location_id
    return leaf_id

def link_manuscript_to_location(cursor, manuscript_id, pg_cursor):
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
            execute_with_normalization(cursor, """
                INSERT OR IGNORE INTO manuscript_location(manuscript_id, origin_id)
                VALUES (?, ?)
            """, (manuscript_id, leaf_id))


def migrate_manuscript_content():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    execute_with_normalization(cursor, "PRAGMA foreign_keys = OFF")
    print("Foreign key constraints disabled for content migration")

    pg_cursor.execute("""
        SELECT idgenre, idparentgenre, genre
        FROM data.genre
        WHERE is_content = TRUE
    """)

    rows = pg_cursor.fetchall()
    print(f"Fetched {len(rows)} content nodes from Postgres (is_content=True)")

    execute_with_normalization(cursor, "BEGIN")

    for idgenre, _, genre in rows:
        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO content (id, name)
            VALUES (?, ?)
        """, (int(idgenre), genre))

    for idgenre, idparentgenre, _ in rows:
        if idparentgenre is not None:
            execute_with_normalization(cursor, """
                UPDATE content
                SET parent_id = ?
                WHERE id = ?
            """, (int(idparentgenre), int(idgenre)))

    execute_with_normalization(cursor, "COMMIT")
    execute_with_normalization(cursor, "PRAGMA foreign_keys = ON")
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

    leaf_ids = [int(cid) for cid in content_ids if cid not in parents_in_list]

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
    
    execute_with_normalization(cursor, "BEGIN")
    
    for hit in hits:
        source = hit['_source']
        manuscript_id = int(source.get('id', hit['_id']))

        execute_with_normalization(cursor, """
        INSERT INTO manuscript (
            id, name, completion_date_floor, completion_date_ceiling,
            created, modified, number_of_occurrences, shelf
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            completion_date_floor = excluded.completion_date_floor,
            completion_date_ceiling = excluded.completion_date_ceiling,
            created = excluded.created,
            modified = excluded.modified,
            number_of_occurrences = excluded.number_of_occurrences,
            shelf = excluded.shelf
        """, (
            manuscript_id,
            source.get('name'),
            source.get('completion_floor'),
            source.get('completion_ceiling'),
            source.get('created'),
            source.get('modified'),
            source.get('number_of_occurrences'),
            source.get('shelf')
        ))

        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_or_create_role(cursor, role_name_in_table)
            if not role_id:
                continue
            
            persons = source.get(role_field, [])
            if isinstance(persons, dict):
                persons = [persons]
            elif not isinstance(persons, list):
                persons = []

            for p in persons:
                person_id = int(p.get('id', ''))
                if person_id:
                    execute_with_normalization(cursor,
                        "INSERT INTO manuscript_person_role (manuscript_id, person_id, role_id) VALUES (?, ?, ?)",
                                               (manuscript_id, person_id, role_id)
                                               )

        link_manuscript_to_location(cursor, manuscript_id, pg_cursor)

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
                "entity_table": "acknowledgement",
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

        insert_many_to_one(cursor, "collection", "collection", manuscript_id, source.get("collection"))

        lib = get_library_for_manuscript(pg_cursor, manuscript_id)

        if lib:
            library_id, library_name, location_id = lib

            if location_id:
                hierarchy = get_region_hierarchy(pg_cursor, location_id)
                insert_location_hierarchy(cursor, hierarchy)

            insert_library(cursor, library_id, library_name, location_id)
            execute_with_normalization(cursor, """
                UPDATE manuscript
                SET library_id = ?
                WHERE id = ?
            """, (int(library_id), manuscript_id))



        MANUSCRIPT_IDENT_TYPE_MAP = {
            "diktyon": "diktyon"
        }

        content_list = source.get("content", [])
        content_ids = [c.get("id") for c in content_list if c.get("id")]

        leaf_ids = get_deepest_leaf_from_postgres(pg_cursor, content_ids)

        for leaf_id in leaf_ids:
            execute_with_normalization(cursor, """
                INSERT OR IGNORE INTO manuscript_content (manuscript_id, content_id)
                VALUES (?, ?)
            """, (manuscript_id, leaf_id))

        for es_field, ident_type in MANUSCRIPT_IDENT_TYPE_MAP.items():
            for identifier in source.get(es_field, []):
                if not identifier:
                    continue
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO identifications (type, identifier_value) VALUES (?, ?)",
                                           (ident_type, identifier)
                                           )
                ident_id = cursor.lastrowid
                if ident_id == 0:
                    execute_with_normalization(cursor,
                        "SELECT id FROM identifications WHERE type = ? AND identifier_value = ?",
                                               (ident_type, identifier)
                                               )
                    ident_id = cursor.fetchone()[0]
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO manuscript_identification (manuscript_id, identification_id) VALUES (?, ?)",
                                           (manuscript_id, ident_id)
                                           )

    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    
    print(f"Manuscripts migration completed: {len(hits)} manuscripts inserted")


if __name__ == "__main__":
    migrate_manuscripts()
