import sqlite3
from common import MAIN_DB_PATH, get_db_connection

def create_base_tables():
    conn, cursor = get_db_connection()

    cursor.execute("CREATE TABLE IF NOT EXISTS persons (id TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS occurrences (id TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS manuscripts (id TEXT PRIMARY KEY)")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS verses (
        id TEXT PRIMARY KEY,
        occurrence_id TEXT,
        manuscript_id TEXT,
        FOREIGN KEY (occurrence_id) REFERENCES occurrences(id),
        FOREIGN KEY (manuscript_id) REFERENCES manuscripts(id)
    )
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS bibliographies (id TEXT PRIMARY KEY)")
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
    CREATE TABLE IF NOT EXISTS cities (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS libraries (
            id TEXT PRIMARY KEY,
            name TEXT,
            region_id TEXT,
            FOREIGN KEY (region_id) REFERENCES locations(id)
        );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collections (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS biblio_category (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS verse_groups (
        id TEXT PRIMARY KEY
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



    conn.commit()
    conn.close()
    print(f"Base tables created in '{MAIN_DB_PATH}'")

if __name__ == "__main__":
    create_base_tables()
