# app/migrate_bibliographies/__init__.py

from .schema import create_schema
from .insert_main_bibliographies import migrate_references
from .link_chapters_to_books import migrate_book_chapters
from .link_articles_to_journals import migrate_journals
from .cleanup import cleanup_bibliographies

def migrate_bibliographies():
    create_schema()
    migrate_references()
    migrate_book_chapters()
    migrate_journals()
    cleanup_bibliographies()
