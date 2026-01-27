# app/migrate_bibliographies/__init__.py

from .schema import create_schema
from .references import migrate_references
from .book_chapters import migrate_book_chapters
from .journals import migrate_journals
from .cleanup import cleanup_bibliographies

def migrate_bibliographies():
    create_schema()
    migrate_references()
    migrate_book_chapters()
    migrate_journals()
    cleanup_bibliographies()
