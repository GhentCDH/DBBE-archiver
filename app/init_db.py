from .common import MAIN_DB_PATH, get_db_connection, execute_with_normalization

BIBLIO_type = {
    "article",
    "blog_post",
    "book",
    "book_chapter",
    "online_source",
    "phd",
    "bib_varia",
}

BIBLIO_ENTITY_type = {
    "manuscript": "manuscript",
    "person": "person",
    "occurrence": "occurrence",
    "type": "type",
}

def create_base_tables():
    conn, cursor = get_db_connection()

    execute_with_normalization(cursor, "CREATE TABLE IF NOT EXISTS person (id INTEGER PRIMARY KEY)")
    execute_with_normalization(cursor, "CREATE TABLE IF NOT EXISTS occurrence (id INTEGER PRIMARY KEY)")
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS manuscript (
        id INTEGER PRIMARY KEY,
        name TEXT,
        completion_date_floor TEXT,
        completion_date_ceiling TEXT,
        created TEXT,
        modified TEXT,
        public_comment TEXT,
        private_comment TEXT,
        number_of_occurrences INTEGER,
        shelf TEXT,
        library_id INTEGER,
        collection_id INTEGER,
        FOREIGN KEY (library_id) REFERENCES library(id),
        FOREIGN KEY (collection_id) REFERENCES collection(id)
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS verses (
        id INTEGER PRIMARY KEY,
        occurrence_id INTEGER,
        verse_group_id INTEGER,
        FOREIGN KEY (occurrence_id) REFERENCES occurrence(id)
    )
    """)
    execute_with_normalization(cursor, "CREATE TABLE IF NOT EXISTS type (id INTEGER PRIMARY KEY)")

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS management (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS acknowledgement (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS genre (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS metre (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY,
        name TEXT,
        historical_name TEXT,
        parent_id INTEGER,
        FOREIGN KEY(parent_id) REFERENCES location(id)
    )
    """)


    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS text_statuses (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS keyword (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS tag (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS editorial_status (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    
    execute_with_normalization(cursor, """
        CREATE TABLE IF NOT EXISTS content (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES content(id) ON DELETE CASCADE,
            name TEXT
        );
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS identifications (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        identifier_value TEXT NOT NULL
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS self_designation (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS office (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    execute_with_normalization(cursor, """
        CREATE TABLE IF NOT EXISTS library (
            id INTEGER PRIMARY KEY,
            name TEXT,
            location_id INTEGER,
            FOREIGN KEY (location_id) REFERENCES location(id)
        );
    """)
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS collection (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS occurrence_relation_definition (
        id INTEGER PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)
    
    execute_with_normalization(cursor, """
    CREATE TABLE IF NOT EXISTS type_relation_definition (
        id INTEGER PRIMARY KEY,
        definition TEXT NOT NULL UNIQUE
    )
    """)
    for bib_type in BIBLIO_type:
        # Type-specific bibliography table
        execute_with_normalization(cursor, f"""
               CREATE TABLE IF NOT EXISTS {bib_type} (
                   id INTEGER PRIMARY KEY,
                   title TEXT,
                   title_sort_key TEXT
               )
           """)

        # Type-specific roles table
        execute_with_normalization(cursor, f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_person_role (
                   bibliography_id INTEGER NOT NULL,
                   person_id INTEGER NOT NULL,
                   role_id INTEGER NOT NULL,
                   PRIMARY KEY (bibliography_id, person_id, role_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (person_id) REFERENCES person(id),
                   FOREIGN KEY (role_id) REFERENCES roles(id)
               )
           """)

        execute_with_normalization(cursor, f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_management (
                   bibliography_id INTEGER NOT NULL,
                   management_id INTEGER NOT NULL,
                   PRIMARY KEY (bibliography_id, management_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (management_id) REFERENCES management(id)
               )
           """)

        execute_with_normalization(cursor, f"""
              CREATE TABLE IF NOT EXISTS journal (
                id INTEGER PRIMARY KEY,
                title TEXT,
                title_sort_key TEXT
            );
           """)

        execute_with_normalization(cursor, f"""
            CREATE TABLE IF NOT EXISTS journal_issue (
                id INTEGER PRIMARY KEY,
                journal_id INTEGER NOT NULL,
                title TEXT,
                title_sort_key TEXT,
                FOREIGN KEY (journal_id) REFERENCES journal(id)
            );
           """)

        execute_with_normalization(cursor, f"""
               CREATE TABLE IF NOT EXISTS {bib_type}_management (
                   bibliography_id INTEGER NOT NULL,
                   management_id INTEGER NOT NULL,
                   PRIMARY KEY (bibliography_id, management_id),
                   FOREIGN KEY (bibliography_id) REFERENCES {bib_type}(id),
                   FOREIGN KEY (management_id) REFERENCES management(id)
               )
           """)

        for entity, sqlite_table in BIBLIO_ENTITY_type.items():
            execute_with_normalization(cursor, f"""
                   CREATE TABLE IF NOT EXISTS {entity}_{bib_type} (
                       {entity}_id INTEGER NOT NULL,
                       {bib_type}_id INTEGER NOT NULL,
                       page_start INTEGER,
                       page_end INTEGER,
                       url TEXT,
                       image TEXT,
                       private_comment TEXT,
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
