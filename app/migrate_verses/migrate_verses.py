from ..common import (execute_with_normalization,
                      get_db_connection, get_es_client, scroll_all, get_dbbe_indices, add_column_if_missing, execute_with_normalization
                      )

def create_verse_tables(cursor):

    verse_columns = {
        "text": "TEXT",
        "occurrence_id": "INTEGER",
        "order_in_occurrence": "INTEGER",
        "verse_group_id": "INTEGER"
    }
    for col, col_type in verse_columns.items():
        add_column_if_missing(cursor, "verses", col, col_type)

def migrate_verses():
    es = get_es_client()
    conn, cursor = get_db_connection()
    create_verse_tables(cursor)

    execute_with_normalization(cursor, "PRAGMA foreign_keys = ON;")

    indices = get_dbbe_indices(es)
    verse_index = next((idx for idx in indices if idx.endswith("verses")), None)

    if not verse_index:
        print("No verse index found")
        conn.close()
        return

    print(f"Migrating verses from index: {verse_index}")
    hits = scroll_all(es, verse_index)
    print(f"Total verses fetched: {len(hits)}")

    execute_with_normalization(cursor, "BEGIN TRANSACTION")
    batch_count = 0

    for hit in hits:
        source = hit["_source"]
        try:
            verse_id = int(source.get("id", hit["_id"]))
        except (TypeError, ValueError):
            continue

        text = source.get("verse", "")
        order_in_occurrence = source.get("order", 0)

        occurrence_id = source.get("occurrence", {}).get("id")
        if occurrence_id is not None:
            try:
                occurrence_id = int(occurrence_id)
            except (TypeError, ValueError):
                occurrence_id = None

        if occurrence_id is not None:
            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO occurrences (id) VALUES (?)",
                                       (occurrence_id,)
                                       )

        verse_group_id = source.get("group_id")
        if verse_group_id is not None:
            try:
                verse_group_id = int(verse_group_id)
            except (TypeError, ValueError):
                verse_group_id = None

        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO verses (
                id, text, occurrence_id, order_in_occurrence, verse_group_id
            ) VALUES (?, ?, ?, ?, ?)
        """, (verse_id, text, occurrence_id, order_in_occurrence, verse_group_id))

        batch_count += 1
        if batch_count % 1000 == 0:
            execute_with_normalization(cursor, "COMMIT")
            execute_with_normalization(cursor, "BEGIN TRANSACTION")

    execute_with_normalization(cursor, "COMMIT")
    conn.close()
    print(f"Verse migration completed: {batch_count} verses inserted")

if __name__ == "__main__":
    migrate_verses()
