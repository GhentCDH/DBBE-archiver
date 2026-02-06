from .schema import create_schema
from .insert_types import run_type_migration

def migrate_types():
    create_schema()
    run_type_migration()

