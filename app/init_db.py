from .common import MAIN_DB_PATH, get_db_connection

BIBLIO_TYPES = {
    "article",
    "blog_post",
    "book",
    "book_chapter",
    "online_source",
    "phd",
    "bib_varia",
}

BIBLIO_ENTITY_TYPES = {
    "manuscript": "manuscripts",
    "person": "persons",
    "occurrence": "occurrences",
    "type": "types",
}

def create_base_tables():
    conn, cursor = get_db_connection()

    cursor.execute("CREATE TABLE IF NOT EXISTS persons (id TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS occurrences (id TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS manuscripts (id TEXT PRIMARY KEY)")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS verses (
        id TEXT PRIMARY KEY,
        occurrence_id TEXT,
        verse_group_id TEXT,
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id)
    )
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS types (id TEXT PRIMARY KEY)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS roles (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS management (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS acknowledgements (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS metres (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        id TEXT PRIMARY KEY,
        name TEXT,
        historical_name TEXT,
        parent_id TEXT,
        FOREIGN KEY(parent_id) REFERENCES locations(id)
    )
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS text_statuses (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS keyword (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS editorial_statuses (
        id TEXT PRIMARY KEY,
        name TEXT
    )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS content (
            id TEXT PRIMARY KEY,
            parent_id TEXT REFERENCES content(id) ON DELETE CASCADE,
            name TEXT
        );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS identifications (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        identifier_value TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS self_designations (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS offices (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS libraries (
            id TEXT PRIMARY KEY,
            name TEXT,
            location_id TEXT,
            FOREIGN KEY (location_id) REFERENCES locations(id)
        );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collections (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS occurrence_relation_definitions (
        id TEXT PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS type_relation_definitions (
        id TEXT PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)
    for bib_type in BIBLIO_TYPES:
        # Type-specific bibliography table
        cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {bib_type} (
                   id TEXT PRIMARY KEY,
                   title TEXT,
                   title_sort_key TEXT
               )
           """)

        # Type-specific roles table
        cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_person_roles (
                   bibliography_id TEXT NOT NULL,
                   person_id TEXT NOT NULL,
                   role_id TEXT NOT NULL,
                   PRIMARY KEY (bibliography_id, person_id, role_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (person_id) REFERENCES persons(id),
                   FOREIGN KEY (role_id) REFERENCES roles(id)
               )
           """)

        # Type-specific management table
        cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_managements (
                   bibliography_id TEXT NOT NULL,
                   management_id TEXT NOT NULL,
                   PRIMARY KEY (bibliography_id, management_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (management_id) REFERENCES management(id)
               )
           """)

        cursor.execute(f"""
              CREATE TABLE IF NOT EXISTS journal (
                id TEXT PRIMARY KEY,
                title TEXT,
                title_sort_key TEXT
            );
           """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS journal_issue (
                id TEXT PRIMARY KEY,
                journal_id TEXT NOT NULL,
                title TEXT,
                title_sort_key TEXT,
                FOREIGN KEY (journal_id) REFERENCES journal(id)
            );
           """)

        cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_managements (
                   bibliography_id TEXT NOT NULL,
                   management_id TEXT NOT NULL,
                   PRIMARY KEY (bibliography_id, management_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (management_id) REFERENCES management(id)
               )
           """)

        # Type-specific entity ↔ bibliography join tables
        for entity, sqlite_table in BIBLIO_ENTITY_TYPES.items():
            cursor.execute(f"""
                   CREATE TABLE IF NOT EXISTS {entity}_{bib_type} (
                       {entity}_id TEXT NOT NULL,
                       {bib_type}_id TEXT NOT NULL,
                       page_start INTEGER,
                       page_end INTEGER,
                       raw_pages TEXT,
                       rel_url TEXT,
                       source_remark TEXT,
                       image TEXT,
                       PRIMARY KEY ({entity}_id, {bib_type}_id),
                       FOREIGN KEY ({entity}_id) REFERENCES {sqlite_table}(id),
                       FOREIGN KEY ({bib_type}_id) REFERENCES {bib_type}(id)
                   )
               """)

    conn.commit()
    conn.close()
    print(f"Base tables created in '{MAIN_DB_PATH}'")

if __name__ == "__main__":
    create_base_tables()
