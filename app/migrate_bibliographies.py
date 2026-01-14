from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
    add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME, get_or_create_role
)


def create_bibliography_tables(cursor):
    add_column_if_missing(cursor, "bibliographies", "category_id",
                         "TEXT REFERENCES biblio_category(id)")
    add_column_if_missing(cursor, "bibliographies", "title", "TEXT")
    add_column_if_missing(cursor, "bibliographies", "title_sort_key", "TEXT")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bibliography_person_roles (
        bibliography_id TEXT NOT NULL,
        person_id TEXT NOT NULL,
        role_id TEXT NOT NULL,
        PRIMARY KEY (bibliography_id, person_id, role_id),
        FOREIGN KEY (bibliography_id) REFERENCES bibliographies(id),
        FOREIGN KEY (person_id) REFERENCES persons(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bibliography_management (
        bibliography_id TEXT NOT NULL,
        management_id TEXT NOT NULL,
        PRIMARY KEY (bibliography_id, management_id),
        FOREIGN KEY (bibliography_id) REFERENCES bibliographies(id),
        FOREIGN KEY (management_id) REFERENCES management(id)
    )
    """)


def migrate_bibliographies():
    es = get_es_client()
    conn, cursor = get_db_connection()

    create_bibliography_tables(cursor)

    indices = get_dbbe_indices(es)
    bibliography_index = next((idx for idx in indices if idx.endswith("bibliographies")), None)
    
    if not bibliography_index:
        print("No bibliography index found")
        conn.close()
        return
    
    print(f"Migrating bibliographies from index: {bibliography_index}")
    hits = scroll_all(es, bibliography_index)
    print(f"Total bibliographies fetched: {len(hits)}")
    
    cursor.execute("BEGIN")
    
    for hit in hits:
        source = hit['_source']
        bib_id = str(source.get('id', hit['_id']))

        category_id = None
        bib_type = source.get('type', {})
        if isinstance(bib_type, dict):
            category_id = str(bib_type.get('id', ''))
            category_name = bib_type.get('name', '')
            if category_id and category_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO biblio_category (id, name) VALUES (?, ?)",
                    (category_id, category_name)
                )

        cursor.execute("""
        INSERT INTO bibliographies (id, category_id, title, title_sort_key)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            category_id = excluded.category_id,
            title = excluded.title,
            title_sort_key = excluded.title_sort_key
        """, (
            bib_id,
            category_id,
            source.get('title'),
            source.get('title_sort_key')
        ))

        for mgmt in source.get('management', []):
            mgmt_id = str(mgmt.get('id', ''))
            mgmt_name = mgmt.get('name', '')
            if mgmt_id and mgmt_name:
                cursor.execute(
                    "INSERT OR IGNORE INTO management (id, name) VALUES (?, ?)",
                    (mgmt_id, mgmt_name)
                )
                cursor.execute(
                    "INSERT OR IGNORE INTO bibliography_management (bibliography_id, management_id) VALUES (?, ?)",
                    (bib_id, mgmt_id)
                )

        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_or_create_role(cursor, role_name_in_table)
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
                    cursor.execute("SELECT 1 FROM bibliographies WHERE id=?", (bib_id,))
                    bibliography_exists = cursor.fetchone()
                    
                    if person_exists and bibliography_exists:
                        cursor.execute(
                            "INSERT OR IGNORE INTO bibliography_person_roles (bibliography_id, person_id, role_id) VALUES (?, ?, ?)",
                            (bib_id, person_id, role_id)
                        )
    
    cursor.execute("COMMIT")
    conn.close()
    
    print(f"Bibliographies migration completed: {len(hits)} bibliographies inserted")


if __name__ == "__main__":
    migrate_bibliographies()
