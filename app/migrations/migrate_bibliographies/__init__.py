# app/migrate_bibliographies/__init__.py
from . import insert_blog_post
from .schema import create_schema
from .link_chapters_to_books import migrate_book_chapters
from .link_managements_to_bibliographies import migrate_managements
from .link_articles_to_journals import migrate_journals
from .cleanup import cleanup_bibliographies
from .link_persons_to_bibliographies import migrate_person_role
from .insert_bibliographies import insert_bibliographies
from .insert_bib_varia import insert_bib_varia
from .insert_blog_post import insert_blog_posts
from .insert_books import insert_books
from .insert_phds import insert_phds
from .insert_online_sources import insert_online_sources
from .link_bibliographies_to_bibsubjects import link_bibliographies_to_bibsubjects

import logging
import traceback

logger = logging.getLogger("bibliography_migration")

def run_step(name, fn):
    logger.info(f"▶ Starting step: {name}")
    try:
        fn()
        logger.info(f"✓ Completed step: {name}")
    except Exception as e:
        logger.error(f"✗ Failed step: {name}")
        logger.error(str(e))
        logger.debug(traceback.format_exc())
        raise

def migrate_bibliographies():
    run_step("create_schema", create_schema)
    run_step("insert_bibliographies", insert_bibliographies)
    run_step("insert_bib_varia", insert_bib_varia)
    run_step("insert_blog_post", insert_blog_posts)
    run_step("insert_books", insert_books)
    run_step("insert_phds", insert_phds)
    run_step("insert_online_sources", insert_online_sources)

    run_step("link_bibliographies_to_bibsubjects", link_bibliographies_to_bibsubjects)
    run_step("migrate_book_chapters", migrate_book_chapters)
    run_step("migrate_journals", migrate_journals)
    run_step("migrate_person_role", migrate_person_role)
    run_step("migrate_managements", migrate_managements)
    run_step("cleanup_bibliographies", cleanup_bibliographies)

