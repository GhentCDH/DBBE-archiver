from .schema import create_schema
from .insert_manuscripts import run_manuscript_migration

def migrate_manuscripts():
    create_schema()
    run_manuscript_migration()

