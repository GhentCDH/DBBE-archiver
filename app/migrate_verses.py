from common import (
    get_db_connection, get_es_client, scroll_all, get_dbbe_indices, add_column_if_missing
)

def create_verse_tables(cursor):


    verse_columns = {
        "text": "TEXT",
        "occurrence_id": "TEXT",
        "manuscript_id": "TEXT",
        "order_in_occurrence": "INTEGER",
        "verse_group_id": "TEXT"
    }
    for col, col_type in verse_columns.items():
        add_column_if_missing(cursor, "verses", col, col_type)

def migrate_verses():
    es = get_es_client()
    conn, cursor = get_db_connection()
    create_verse_tables(cursor)

    cursor.execute("PRAGMA foreign_keys = ON;")

    indices = get_dbbe_indices(es)
    verse_index = next((idx for idx in indices if idx.endswith("verses")), None)

    if not verse_index:
        print("No verse index found")
        conn.close()
        return

    print(f"Migrating verses from index: {verse_index}")
    hits = scroll_all(es, verse_index)
    print(f"Total verses fetched: {len(hits)}")

    cursor.execute("BEGIN TRANSACTION")
    batch_count = 0

    for hit in hits:
        source = hit["_source"]
        verse_id = str(source.get("id", hit["_id"]))
        text = source.get("verse", "")
        order_in_occurrence = source.get("order", 0)

        occurrence_id = str(source.get("occurrence", {}).get("id", ""))
        manuscript_id = str(source.get("manuscript", {}).get("id", ""))

        if manuscript_id:
            cursor.execute(
                "INSERT OR IGNORE INTO manuscripts (id) VALUES (?)",
                (manuscript_id,)
            )
        if occurrence_id:
            cursor.execute(
                "INSERT OR IGNORE INTO occurrences (id) VALUES (?)",
                (occurrence_id,)
            )

        verse_group_id = source.get("group_id")
        if verse_group_id is not None:
            verse_group_id = str(verse_group_id)
        cursor.execute("""
            INSERT OR IGNORE INTO verses (
                id, text, occurrence_id, manuscript_id, order_in_occurrence, verse_group_id
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (verse_id, text, occurrence_id, manuscript_id, order_in_occurrence, verse_group_id))

        batch_count += 1
        if batch_count % 1000 == 0:
            cursor.execute("COMMIT")
            cursor.execute("BEGIN TRANSACTION")

    cursor.execute("COMMIT")
    conn.close()
    print(f"Verse migration completed: {batch_count} verses inserted")

if __name__ == "__main__":
    migrate_verses()
