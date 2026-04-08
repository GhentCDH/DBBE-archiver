import os
import psycopg2
import sqlite3
from elasticsearch import Elasticsearch
import os
from pathlib import Path
BASE_DIR = Path(__file__).parent
MAIN_DB_PATH = BASE_DIR / "data" / "dbbe_archive.sqlite"
MAIN_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

ROLE_FIELD_TO_ROLE_NAME = {
    "person_subject": "Subject",
    "owner": "Owner",
    "poet": "Poet",
    "patron": "Patron",
    "related": "Related",
    "scribe": "Scribe",
    "person_content": "Content",
    "author": "Author",
    "supervisor": "Supervisor",
    "editor": "Editor",
    "contributor": "Contributor",
    "translator": "Translator",
    "transcriber": "Transcriber",
    "creator": "Creator",
    "illuminator": "Illuminator"
}

import unicodedata
import unicodedata

NORMALIZATION_STATS = {
    "changed": 0,
    "unchanged": 0,
    "samples": []  # store up to 20 examples
}
MAX_SAMPLES = 20

def normalize_value(value):
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value)
        if normalized != value:
            NORMALIZATION_STATS["changed"] += 1
            if len(NORMALIZATION_STATS["samples"]) < MAX_SAMPLES:
                NORMALIZATION_STATS["samples"].append((value, normalized))
        else:
            NORMALIZATION_STATS["unchanged"] += 1
        return normalized

    if isinstance(value, (list, tuple)):
        return type(value)(normalize_value(v) for v in value)

    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}

    return value


def execute_with_normalization(cursor, query, params=None):
    if params is None:
        cursor.execute(query)
    else:
        cursor.execute(query, normalize_value(params))
    return cursor


def get_db_connection(db_path=MAIN_DB_PATH):
    conn = sqlite3.connect(db_path, timeout=60, isolation_level=None)
    cursor = conn.cursor()
    execute_with_normalization(cursor, "PRAGMA journal_mode = WAL;")
    execute_with_normalization(cursor, "PRAGMA busy_timeout = 60000;")
    execute_with_normalization(cursor, "PRAGMA foreign_keys = ON;")
    return conn, cursor

def get_postgres_connection():
    pg_connection_string = os.getenv("PG_CONNECTION_STRING")

    if pg_connection_string:
        ### For some reason this is the only way we can connect via Nomad. Pg_user and password combination gives 'incorrect password' like errors.
        pg_conn = psycopg2.connect(pg_connection_string)
    else:
        pg_host = os.getenv("PG_HOST", "localhost")
        pg_port = os.getenv("PG_PORT", 15432)
        pg_user = os.getenv("PG_USER", "db_dbbe_dev")
        pg_password = os.getenv("PG_PASSWORD", "db_dbbe_dev")
        pg_db = os.getenv("PG_DB", "db_dbbe_dev")
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_password
        )
    pg_cursor = pg_conn.cursor()
    return pg_conn, pg_cursor


def get_es_client():
    es_host = os.getenv("ES_HOST", "http://localhost:19200")
    es_user = os.getenv("ES_USERNAME", "")
    es_pass = os.getenv("ES_PASSWORD", "")

    if es_user and es_pass:
        print("Connecting to host:", es_host)
        return Elasticsearch(
            es_host,
            basic_auth=(es_user, es_pass),
        )

    return Elasticsearch(es_host)

def scroll_all(es, index, query=None, size=1000):
    if query is None:
        query = {"query": {"match_all": {}}, "size": size}

    resp = es.search(index=index, body=query, scroll='2m')
    scroll_id = resp['_scroll_id']
    hits = resp['hits']['hits']
    all_hits = hits[:]

    while len(hits):
        resp = es.scroll(scroll_id=scroll_id, scroll='2m')
        hits = resp['hits']['hits']
        all_hits.extend(hits)


    return all_hits


def add_column_if_missing(cursor, table_name, column_name, column_type):
    execute_with_normalization(cursor, f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    if column_name not in columns:
        execute_with_normalization(cursor, f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def get_role_id(cursor, role_name):
    execute_with_normalization(cursor, "SELECT id FROM roles WHERE LOWER(name)=LOWER(?)", (role_name,))
    row = cursor.fetchone()
    return row[0] if row else None

from datetime import datetime

from datetime import datetime

LOCAL_ROLE_START_ID = 100

def get_or_create_role(cursor, role_name):
    role_name = role_name.strip()

    # 1️⃣ Check SQLite first
    execute_with_normalization(
        cursor,
        "SELECT id FROM roles WHERE LOWER(name) = LOWER(?) LIMIT 1",
        (role_name,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    # 2️⃣ Check Postgres (preferred source)
    pg_conn, pg_cursor = get_postgres_connection()
    try:
        pg_cursor.execute(
            """
            SELECT idrole, name, created, modified
            FROM data.role
            WHERE LOWER(system_name) = LOWER(%s)
            LIMIT 1
            """,
            (role_name,)
        )
        pg_row = pg_cursor.fetchone()

        if pg_row:
            role_id, pg_name, created, modified = pg_row

            execute_with_normalization(
                cursor,
                "INSERT INTO roles (id, name, created, modified) VALUES (?, ?, ?, ?)",
                (role_id, pg_name, created, modified)
            )
            return role_id

    finally:
        pg_cursor.close()
        pg_conn.close()

    # 3️⃣ Not in Postgres → create locally starting from 100+

    execute_with_normalization(
        cursor,
        "SELECT MAX(CAST(id AS INTEGER)) FROM roles WHERE CAST(id AS INTEGER) >= ?",
        (LOCAL_ROLE_START_ID,)
    )
    max_local_id = cursor.fetchone()[0]

    if max_local_id is None:
        role_id = LOCAL_ROLE_START_ID
    else:
        role_id = max_local_id + 1

    now = datetime.utcnow().isoformat()

    execute_with_normalization(
        cursor,
        "INSERT INTO roles (id, name, created, modified) VALUES (?, ?, ?, ?)",
        (role_id, role_name, now, now)
    )

    return role_id


def get_dbbe_indices(es):
    prefix = os.getenv("ES_INDEX_PREFIX", "dbbe_dev")
    indices = es.cat.indices(format="json")
    return [idx['index'] for idx in indices if idx['index'].startswith(prefix)]

def insert_many_to_many(
    cursor,
    source: dict,
    source_key: str,
    entity_table: str,
    join_table: str,
    parent_id_col: str,
    entity_id_col: str,
    parent_id: str,
):
    for item in source.get(source_key, []):
        item_id = item.get("id", "")
        item_name = item.get("name", "")
        if not item_id or not item_name:
            continue

        execute_with_normalization(cursor,
            f"INSERT OR IGNORE INTO {entity_table} (id, name) VALUES (?, ?)",
                                   (item_id, item_name),
                                   )

        execute_with_normalization(cursor,
            f"""
            INSERT OR IGNORE INTO {join_table}
            ({parent_id_col}, {entity_id_col})
            VALUES (?, ?)
            """,
                                   (parent_id, item_id),
                                   )

def insert_many_to_one(cursor, entity_name, table_name, manuscript_id, entity_data):
    if not entity_data:
        return

    entity_id = entity_data.get("id", "")
    entity_name_val = entity_data.get("name", "")

    if entity_id and entity_name_val:
        execute_with_normalization(cursor,
            f"INSERT OR IGNORE INTO {table_name} (id, name) VALUES (?, ?)",
                                   (entity_id, entity_name_val)
                                   )
        execute_with_normalization(cursor,
            f"UPDATE manuscript SET {entity_name}_id = ? WHERE id = ?",
                                   (entity_id, manuscript_id)
                                   )

def get_public_release() -> bool:
    public_release = os.getenv("PUBLIC_RELEASE", "true")
    return public_release.lower() in {"1", "true", "yes", "on"}


def insert_entity_managements(cursor, pg_cursor, entity_id: str, join_table: str, entity_col: str):
    pg_cursor.execute("""
        SELECT em.idmanagement, m.name
        FROM data.entity_management em
        JOIN data.management m ON m.id = em.idmanagement
        WHERE em.identity = %s
    """, (entity_id,))
    for management_id, management_name in pg_cursor.fetchall():
        execute_with_normalization(cursor,
            "INSERT OR IGNORE INTO management (id, name) VALUES (?, ?)",
            (str(management_id), management_name))
        execute_with_normalization(cursor,
            f"INSERT OR IGNORE INTO {join_table} ({entity_col}, management_id) VALUES (?, ?)",
            (entity_id, str(management_id)))