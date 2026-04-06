from app.common import (execute_with_normalization, get_db_connection, get_es_client, scroll_all, get_dbbe_indices,
                        add_column_if_missing, get_role_id, ROLE_FIELD_TO_ROLE_NAME, insert_many_to_many, get_postgres_connection, get_public_release
                        )

def get_critical_apparatus(pg_cursor, type_id: str):
    pg_cursor.execute("""
        SELECT critical_apparatus
        FROM data.reconstructed_poem
        WHERE identity = %s
    """, (type_id,))
    row = pg_cursor.fetchone()
    return row[0] if row else None

def fetch_type_relations(pg_conn):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT
            f.subject_identity,
            f.object_identity,
            ft.idfactoid_type,
            ft.type
        FROM data.factoid f
        JOIN data.factoid_type ft
            ON f.idfactoid_type = ft.idfactoid_type
        WHERE ft.group = 'reconstructed_poem_related_to_reconstructed_poem'
    """)
    relations = pg_cursor.fetchall()
    pg_cursor.close()
    return relations


def create_type_tables(cursor):
    type_columns = {
        "text_stemmer": "TEXT",
        "text_original": "TEXT",
        "lemma": "TEXT",
        "incipit": "TEXT",
        "created": "TEXT",
        "modified": "TEXT",
        "public_comment": "TEXT",
        "private_comment": "TEXT",
        "title": "TEXT",
        "number_of_verses": "INTEGER"
    }
    
    for col, col_type in type_columns.items():
        add_column_if_missing(cursor, "type", col, col_type)


def get_subject_keyword(pg_cursor, subject_id):
    pg_cursor.execute("""
        SELECT identity, keyword
        FROM data.keyword
        WHERE identity = %s
          AND is_subject = true
    """, (subject_id,))
    return pg_cursor.fetchone()

def get_number_of_verses(pg_cursor, type_id: str):
    pg_cursor.execute("""
        SELECT verses
        FROM data.poem
        WHERE identity = %s
    """, (type_id,))
    row = pg_cursor.fetchone()
    return row[0] if row else None

def fetch_reconstructed_poem_lemmas(pg_conn):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("""
        SELECT id_reconstructed_poem, lemma
        FROM data.reconstructed_poem_lemma
    """)
    rows = pg_cursor.fetchall()
    pg_cursor.close()
    return {str(row[0]): row[1] for row in rows}

def get_translations(pg_cursor, type_id: str):
    pg_cursor.execute("""
        SELECT t.idtranslation, d.text_content, tr.idlanguage
        FROM data.translation_of t
        JOIN data.document d ON d.identity = t.idtranslation
        LEFT JOIN data.translation tr ON tr.identity = t.idtranslation
        WHERE t.iddocument = %s
          AND d.text_content IS NOT NULL
    """, (type_id,))
    return [(str(row[0]), row[1], str(row[2]) if row[2] else None) for row in pg_cursor.fetchall()]

def run_type_migration():
    es = get_es_client()
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()
    lemma_map = fetch_reconstructed_poem_lemmas(pg_conn)

    create_type_tables(cursor)
    
    indices = get_dbbe_indices(es)
    type_index = next((idx for idx in indices if idx.endswith("types")), None)
    
    if not type_index:
        print("No type index found")
        conn.close()
        return
    
    print(f"Migrating types from index: {type_index}")
    hits = scroll_all(es, type_index)
    print(f"Total types fetched: {len(hits)}")
    
    execute_with_normalization(cursor, "BEGIN TRANSACTION")
    batch_count = 0
    
    for hit in hits:
        source = hit['_source']
        type_id = str(source.get('id', hit['_id']))
        is_public_type = bool(source.get('public', False))
        is_public_release = get_public_release()

        if not is_public_type and is_public_release:
            print(f"Skipping type {type_id} because public=False during public release")
            continue

        number_of_verses = get_number_of_verses(pg_cursor, type_id)
        critical_apparatus = get_critical_apparatus(pg_cursor, type_id)

        private_comment_val = None
        if not is_public_release:
            private_comment_val = source.get('private_comment')



        execute_with_normalization(cursor, """
        INSERT INTO type (
            id, text_stemmer, text_original, lemma, incipit,
            created, modified, public_comment, private_comment,
            title, number_of_verses, critical_apparatus
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            text_stemmer = excluded.text_stemmer,
            text_original = excluded.text_original,
            lemma = excluded.lemma,
            incipit = excluded.incipit,
            created = excluded.created,
            modified = excluded.modified,
            public_comment = excluded.public_comment,
            private_comment = excluded.private_comment,
            title = excluded.title,
            number_of_verses = excluded.number_of_verses,
            critical_apparatus = excluded.critical_apparatus
        """, (
            type_id,
            source.get('text_stemmer'),
            source.get('text_original'),
            lemma_map.get(type_id),
            source.get('incipit'),
            source.get('created'),
            source.get('modified'),
            source.get('public_comment'),
            private_comment_val,
            source.get('title_original'),
            number_of_verses,
            critical_apparatus
        ))

        pg_cursor.execute("""
            SELECT idauthority, idsubject, identifier, volume
            FROM data.global_id
            WHERE idsubject = %s
        """, (type_id,))

        for idauthority, idsubject, identifier_id, volume in pg_cursor.fetchall():
            pg_cursor.execute("""
                SELECT system_name
                FROM data.identifier
                WHERE %s = ANY(ids)
            """, (idauthority,))

            row = pg_cursor.fetchone()
            if not row:
                continue

            catalogue = row[0]
            if volume is not None:
                catalogue_id = f"{volume}.{identifier_id}"
            else:
                catalogue_id = str(identifier_id)

            execute_with_normalization(cursor, """
                INSERT OR IGNORE INTO identification (
                    catalogue,
                    catalogue_id,
                    entity_type,
                    entity_id
                )
                VALUES (?, ?, ?, ?)
            """, (catalogue, catalogue_id, "type", type_id))
        for translation_id, translation_text, language_id in get_translations(pg_cursor, type_id):
            execute_with_normalization(cursor,
                                       "INSERT OR IGNORE INTO type_translation (id, type_id, translation, language_id) VALUES (?, ?, ?, ?)",
                                       (translation_id, type_id, translation_text, language_id)
                                       )

        for tag in source.get('tag', []):
            tag_id = str(tag.get('id', ''))
            tag_name = tag.get('name', '')
            if tag_id:
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO tag (id, name) VALUES (?, ?)",
                                           (tag_id, tag_name)
                                           )
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO type_tag (type_id, tag_id) VALUES (?, ?)",
                                           (type_id, tag_id)
                                           )


        cs = source.get('critical_status')
        if isinstance(cs, dict):
            cs_id = str(cs.get('id', ''))
            cs_name = cs.get('name', '')
            if cs_id:
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO editorial_status (id, name) VALUES (?, ?)",
                                           (cs_id, cs_name)
                                           )
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO type_editorial_status (type_id, editorial_status_id) VALUES (?, ?)",
                                           (type_id, cs_id)
                                           )

        ts = source.get('text_status')
        if isinstance(ts, dict):
            ts_id = str(ts.get('id', ''))
            ts_name = ts.get('name', '')
            if ts_id:
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO text_status (id, name) VALUES (?, ?)",
                                           (ts_id, ts_name)
                                           )
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO type_text_status (type_id, text_status_id) VALUES (?, ?)",
                                           (type_id, ts_id)
                                           )

        subjects = source.get("subject", [])
        if isinstance(subjects, dict):
            subjects = [subjects]
        elif not isinstance(subjects, list):
            subjects = []
        for subj in subjects:
            subject_id = str(subj.get("id", ""))
            if not subject_id:
                continue
            pg_keyword = get_subject_keyword(pg_cursor, subject_id)
            if not pg_keyword:
                continue
            keyword_id, keyword_name = pg_keyword
            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO keyword (id, name) VALUES (?, ?)",
                                       (keyword_id, keyword_name)
                                       )

            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO type_keyword (type_id, keyword_id) VALUES (?, ?)",
                                       (type_id, keyword_id)
                                       )

        type_M2M = [
            {
                "source_key": "genre",
                "entity_table": "genre",
                "join_table": "type_genre",
                "parent_id_col": "type_id",
                "entity_id_col": "genre_id",
            },
            {
                "source_key": "metre",
                "entity_table": "metre",
                "join_table": "type_metre",
                "parent_id_col": "type_id",
                "entity_id_col": "metre_id",
            },
            {
                "source_key": "acknowledgement",
                "entity_table": "acknowledgement",
                "join_table": "type_acknowledgement",
                "parent_id_col": "type_id",
                "entity_id_col": "acknowledgement_id",
            },
            {
                "source_key": "management",
                "entity_table": "management",
                "join_table": "type_management",
                "parent_id_col": "type_id",
                "entity_id_col": "management_id",
            },
        ]

        for cfg in type_M2M:
            insert_many_to_many(
                cursor=cursor,
                source=source,
                parent_id=type_id,
                **cfg
            )

        for occ_id in source.get('occurrence_ids', []):
            occ_id = str(occ_id)

            execute_with_normalization(cursor,
                "SELECT 1 FROM occurrence WHERE id=?",
                                       (occ_id,)
                                       )
            if cursor.fetchone() is None:
                continue

            execute_with_normalization(cursor,
                "INSERT OR IGNORE INTO type_occurrence (type_id, occurrence_id) VALUES (?, ?)",
                                       (type_id, occ_id)
                                       )

        for role_field, role_name_in_table in ROLE_FIELD_TO_ROLE_NAME.items():
            role_id = get_role_id(cursor, role_name_in_table)
            if not role_id:
                continue
            
            person = source.get(role_field, [])
            if isinstance(person, dict):
                person = [person]
            elif not isinstance(person, list):
                person = []
            
            for p in person:
                person_id = str(p.get('id', ''))
                if not person_id:
                    continue
                
                execute_with_normalization(cursor, "SELECT 1 FROM person WHERE id=?", (person_id,))
                if cursor.fetchone() is None:
                    continue
                
                execute_with_normalization(cursor,
                    "INSERT OR IGNORE INTO type_person_role (type_id, person_id, role_id) VALUES (?, ?, ?)",
                                           (type_id, person_id, role_id)
                                           )
        
        batch_count += 1
        if batch_count % 1000 == 0:
            execute_with_normalization(cursor, "COMMIT")
            execute_with_normalization(cursor, "BEGIN TRANSACTION")
            print(f"Processed {batch_count} types...")
    
    execute_with_normalization(cursor, "COMMIT")


    relations = fetch_type_relations(pg_conn)
    execute_with_normalization(cursor, "BEGIN TRANSACTION")



    for _, _, rel_def_id, rel_code in relations:
        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO type_relation_definition (id, definition)
            VALUES (?, ?)
        """, (str(rel_def_id), rel_code))


    for type_id, related_type_id, rel_def_id, _ in relations:
        a = int(type_id)
        b = int(related_type_id)
        type_id_norm, related_type_id_norm = sorted((a, b))

        execute_with_normalization(cursor, """
            INSERT OR IGNORE INTO type_related_type
            (type_id, related_type_id, relation_definition_id)
            SELECT ?, ?, ?
            WHERE EXISTS (SELECT 1 FROM "type" WHERE id = ?)
              AND EXISTS (SELECT 1 FROM "type" WHERE id = ?)
        """, (
            type_id_norm,
            related_type_id_norm,
            str(rel_def_id),
            type_id_norm,
            related_type_id_norm
        ))

    execute_with_normalization(cursor, "COMMIT")
    pg_conn.close()
    conn.close()

    print(f"Types migration completed: {batch_count} types inserted")
