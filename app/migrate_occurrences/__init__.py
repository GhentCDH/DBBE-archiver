# app/migrate_bibliographies/__init__.py

from .schema import create_schema
from .insert_occurrences import run_occurrence_migration

def migrate_occurrences():
    create_schema()
    run_occurrence_migration()

