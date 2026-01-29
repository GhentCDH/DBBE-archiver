# app/migrate_bibliographies/cleanup.py
from ..common import get_db_connection
from .biblio_type_enum import BiblioType  # assuming enums are in the same folder
from .biblio_entity_enum import BiblioEntity

OPTIONAL_COLUMNS = ["page_start", "page_end", "url", "private_comment", "image"]


def drop_all_null_columns(cursor):
    for entity in BiblioEntity:
        for bib_type in BiblioType:
            table = f"{entity.name.lower()}_{bib_type.value}"

            cursor.execute(f"PRAGMA table_info({table})")
            columns_in_table = [row[1] for row in cursor.fetchall()]
            cols_to_check = [col for col in OPTIONAL_COLUMNS if col in columns_in_table]

            for col in cols_to_check:
                cursor.execute(f"SELECT COUNT(1) FROM {table} WHERE {col} IS NOT NULL")
                non_null_count = cursor.fetchone()[0]

                if non_null_count == 0:
                    print(f"Dropping empty column {col} from table {table}")

                    remaining_cols = [c for c in columns_in_table if c != col]
                    col_defs = ", ".join(
                        [f"{c} INTEGER" if c in ["page_start", "page_end"] else f"{c} TEXT" for c in remaining_cols]
                    )
                    tmp_table = f"{table}_tmp"

                    cursor.execute(f"CREATE TABLE {tmp_table} ({col_defs}, PRIMARY KEY ({entity.name.lower()}_id, {bib_type.value}_id))")
                    cols_csv = ", ".join(remaining_cols)
                    cursor.execute(f"INSERT INTO {tmp_table} ({cols_csv}) SELECT {cols_csv} FROM {table}")
                    cursor.execute(f"DROP TABLE {table}")
                    cursor.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")

                    columns_in_table = remaining_cols


def cleanup_bibliographies():
    conn, cursor = get_db_connection()
    drop_all_null_columns(cursor)
    conn.commit()
    conn.close()
