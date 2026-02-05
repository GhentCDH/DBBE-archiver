from ..common import (execute_with_normalization,
                      get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
                      get_role_id, ROLE_FIELD_TO_ROLE_NAME,
                      insert_many_to_many, get_postgres_connection
                      )


def preload_related_occurrence(pg_cursor):
    pg_cursor.execute("""
        WITH verse_links AS (
            SELECT
                a.idoriginal_poem AS src,
                b.idoriginal_poem AS dst
            FROM data.original_poem_verse a
            JOIN data.original_poem_verse b ON a.idgroup = b.idgroup
            WHERE a.idoriginal_poem < b.idoriginal_poem
        ),
        factoid_links AS (
            SELECT
                fa.subject_identity AS src,
                fb.subject_identity AS dst
            FROM data.factoid fa
            JOIN data.factoid_type fta ON fa.idfactoid_type = fta.idfactoid_type
            JOIN data.factoid fb ON fa.object_identity = fb.object_identity
            JOIN data.factoid_type ftb ON fb.idfactoid_type = ftb.idfactoid_type
            WHERE fta.type = 'reconstruction of'
              AND ftb.type = 'reconstruction of'
              AND fa.subject_identity <> fb.subject_identity
        )
        SELECT src, dst FROM verse_links
        UNION
        SELECT src, dst FROM factoid_links
    """)

    rel = {}
    for src, dst in pg_cursor.fetchall():
        rel.setdefault(str(src), []).append(str(dst))
    return rel


def get_subject_keyword(pg_cursor, subject_id):
    pg_cursor.execute("""
        SELECT identity, keyword
        FROM data.keyword
        WHERE identity = %s
          AND is_subject = true
    """, (subject_id,))
    return pg_cursor.fetchone()


def run_occurrence_migration():
    es = get_es_client()
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    related_occurrence_map = preload_related_occurrence(pg_cursor)

    execute_with_normalization(cursor, "PRAGMA foreign_keys = OFF")
    print("Foreign key constraints disabled for migration")

    indices = get_dbbe_indices(es)
    occ_index = next((idx for idx in indices if idx.endswith("occurrences")), None)

    if not occ_index:
        print("No occurrence index found")
        execute_with_normalization(cursor, "PRAGMA foreign_keys = ON")
        conn.close()
        return

    print(f"Migrating occurrence from index: {occ_index}")
    hits = scroll_all(es, occ_index, size=500)
    print(f"Total occurrence fetched: {len(hits)}")

    execute_with_normalization(cursor, "BEGIN")
    batch_count = 0

    pg_cursor.execute("""
        SELECT identity, keyword
        FROM data.keyword
        WHERE is_subject = true
    """)
    keyword_cache = {str(row[0]): row[1] for row in pg_cursor.fetchall()}

    for hit in hits:
        source = hit['_source']
        occ_id = str(source.get('id', hit['_id']))
        manuscript_id = str(source.get('manuscript', {}).get('id', ''))

        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO occurrence (id)
            VALUES (?)
        """, (occ_id,))

        execute_with_normalization(cursor, """
        UPDATE occurrence SET
            created=?, modified=?, public_comment=?, private_comment=?,
            is_dbbe=?, incipit=?, text_stemmer=?, text_original=?,
            location_in_ms=?, completion_date_floor=?, completion_date_ceiling=?,
            palaeographical_info=?, contextual_info=?, manuscript_id=?, title=?
        WHERE id=?
        """, (
            source.get('created', ''),
            source.get('modified', ''),
            source.get('public_comment', ''),
            source.get('private_comment', ''),
            bool(source.get('dbbe', False)),
            source.get('incipit', ''),
            source.get('text_stemmer', ''),
            source.get('text_original', ''),
            source.get('location', ''),
            source.get('completion_floor', ''),
            source.get('completion_ceiling', ''),
            source.get('palaeographical_info', ''),
            source.get('contextual_info', ''),
            manuscript_id,
            source.get('title_original', ''),
            occ_id
        ))

        subjects = source.get("subject", [])
        if isinstance(subjects, dict):
            subjects = [subjects]
        elif not isinstance(subjects, list):
            subjects = []

        occ_keyword_rows = []
        for subj in subjects:
            subject_id = str(subj.get("id", ""))
            if not subject_id:
                continue

            keyword_name = keyword_cache.get(subject_id)
            if not keyword_name:
                continue
            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO keyword (id, name) VALUES (?, ?)",
                                       (subject_id, keyword_name)
                                       )
            occ_keyword_rows.append((occ_id, subject_id))

        if occ_keyword_rows:
            cursor.executemany(
                "INSERT OR IGNORE INTO occurrence_keyword (occurrence_id, keyword_id) VALUES (?, ?)",
                occ_keyword_rows
            )

        OCCURRENCE_M2M = [
            {
                "source_key": "genre",
                "entity_table": "genre",
                "join_table": "occurrence_genre",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "genre_id",
            },
            {
                "source_key": "metre",
                "entity_table": "metre",
                "join_table": "occurrence_metre",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "metre_id",
            },
            {
                "source_key": "acknowledgement",
                "entity_table": "acknowledgement",
                "join_table": "occurrence_acknowledgement",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "acknowledgement_id",
            },
            {
                "source_key": "management",
                "entity_table": "management",
                "join_table": "occurrence_management",
                "parent_id_col": "occurrence_id",
                "entity_id_col": "management_id",
            },
        ]

        for cfg in OCCURRENCE_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=occ_id,
                **cfg
            )

        ts = source.get('text_status')
        if isinstance(ts, dict):
            ts_id = str(ts.get('id', ''))
            ts_name = ts.get('name', '')
            if ts_id:
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO text_statuses (id, name) VALUES (?, ?)",
                                           (ts_id, ts_name)
                                           )
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO occurrence_text_statuses (occurrence_id, text_status_id) VALUES (?, ?)",
                                           (occ_id, ts_id))

        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_role_id(cursor, role_name_in_table)
            if not role_id:
                continue

            persons = source.get(role_field, [])
            if isinstance(persons, dict):
                persons = [persons]
            elif not isinstance(persons, list):
                persons = []

            for p in persons:
                person_id = str(p.get('id', ''))
                if person_id:
                    execute_with_normalization(cursor,
                        "INSERT OR IGNORE INTO occurrence_person_role (occurrence_id, person_id, role_id) VALUES (?, ?, ?)",
                                               (occ_id, person_id, role_id)
                                               )


        related_ids = related_occurrence_map.get(occ_id, [])
        if related_ids:
            related_rows = [(occ_id, rid, '0') for rid in related_ids]
            cursor.executemany("""
                INSERT OR IGNORE INTO occurrence_related_occurrence
                (occurrence_id, related_occurrence_id, relation_definition_id)
                VALUES (?, ?, ?)
            """, related_rows)

        batch_count += 1
        if batch_count % 1000 == 0:
            execute_with_normalization(cursor, "COMMIT")
            execute_with_normalization(cursor, "BEGIN")
            print(f"Processed {batch_count} occurrences...")

    execute_with_normalization(cursor, "COMMIT")

    execute_with_normalization(cursor, "PRAGMA foreign_keys = ON")
    print("Foreign key constraints re-enabled")

    conn.close()

    print(f"Occurrences migration completed: {batch_count} occurrences updated")
