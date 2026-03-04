from .schema import create_schema
from .insert_types import run_type_migration
from .migrate_language import migrate_languages

def migrate_types():
    create_schema()
    migrate_languages()
    run_type_migration()

