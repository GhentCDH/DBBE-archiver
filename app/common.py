import os
import psycopg2
import sqlite3
from elasticsearch import Elasticsearch
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).parent
MAIN_DB_PATH = BASE_DIR / "data" / "export_data.sqlite"
MAIN_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
print(MAIN_DB_PATH)

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


def get_db_connection(db_path=MAIN_DB_PATH):
    conn = sqlite3.connect(db_path, timeout=60, isolation_level=None)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA busy_timeout = 60000;")
    cursor.execute("PRAGMA foreign_keys = ON;")
    return conn, cursor

def get_postgres_connection():
    pg_connection_string = os.getenv("PG_CONNECTION_STRING")

    if pg_connection_string:
        ### For some reason this is the only way we can connect via Nomad. Pg_user and password combination gives 'incorrect password' like errors.
        print('Trying connection string')
        pg_conn = psycopg2.connect(pg_connection_string)
    else:
        print('Username password combo')
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
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def get_role_id(cursor, role_name):
    cursor.execute("SELECT id FROM roles WHERE LOWER(name)=LOWER(?)", (role_name,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_or_create_role(cursor, role_name):
    role_id = get_role_id(cursor, role_name)
    if role_id:
        return role_id
    role_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO roles (id, name) VALUES (?, ?)", (role_id, role_name))
    return role_id


def get_dbbe_indices(es):
    indices = es.cat.indices(format="json")
    return [idx['index'] for idx in indices if idx['index'].startswith("dbbe_dev")]

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
        item_id = str(item.get("id", ""))
        item_name = item.get("name", "")
        if not item_id or not item_name:
            continue

        cursor.execute(
            f"INSERT OR IGNORE INTO {entity_table} (id, name) VALUES (?, ?)",
            (item_id, item_name),
        )

        cursor.execute(
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

    entity_id = str(entity_data.get("id", ""))
    entity_name_val = entity_data.get("name", "")

    if entity_id and entity_name_val:
        cursor.execute(
            f"INSERT OR IGNORE INTO {table_name} (id, name) VALUES (?, ?)",
            (entity_id, entity_name_val)
        )
        cursor.execute(
            f"UPDATE manuscripts SET {entity_name}_id = ? WHERE id = ?",
            (entity_id, manuscript_id)
        )
