# app/migrate_bibliographies/__init__.py

from .schema import create_schema
from .insert_persons import run_person_migration

def migrate_persons():
    create_schema()
    run_person_migration()

