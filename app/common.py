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
    "content": "Content",
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
