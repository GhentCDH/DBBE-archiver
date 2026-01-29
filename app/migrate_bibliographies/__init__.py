# app/migrate_bibliographies/__init__.py

from .schema import create_schema
# from .insert_main_bibliographies import migrate_main_bibliographies
from .link_chapters_to_books import migrate_book_chapters
from .link_managements_to_bibliographies import migrate_managements
from .link_articles_to_journals import migrate_journals
from .cleanup import cleanup_bibliographies
from .link_persons_to_bibliographies import migrate_person_roles
from .insert_bibliographies import insert_bibliographies
from .insert_bibliography_references import migrate_biblio_references
def migrate_bibliographies():
    create_schema()
    # migrate_main_bibliographies()
    insert_bibliographies()
    migrate_biblio_references()
    migrate_book_chapters()
    migrate_journals()
    migrate_person_roles()
    migrate_managements()
    cleanup_bibliographies()
