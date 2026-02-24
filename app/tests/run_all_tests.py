from test_acknowledgements import run_test as test_acknowledgements
from test_articles import run_test as test_articles
from test_journal import run_test as test_journals
from test_bib_varia import run_test as test_bib_varia
from test_bibrole import run_test as test_bibrole
from test_blog_post import run_test as test_blog_post
from test_blogs import run_test as test_blog
from test_manuscript import run_test as test_manuscripts
from test_management import run_test as test_managements
from test_self_designation import run_test as test_self_designations
from test_book_series import run_test as test_book_series
from test_book_clusters import run_test as test_book_clusters
from test_book_chapter import run_test as test_book_chapters
from test_metre import run_test as test_metres
from test_document_acknowledgement import run_test as test_document_acknowledgements
from test_phds import run_test as test_phds
from test_person_self_designations import run_test as test_person_self_designations
from test_original_poem import run_test as test_original_poem
from test_document_keyword import run_test as test_document_keywords
from test_online_source import run_test as test_online_source
from test_original_poem_verse import run_test as test_original_poem_verse
from test_reconstructed_poem import run_test as test_reconstructed_poems
from test_entity_management import run_test as test_document_management

def print_result(name, success, table):
    if success:
        print(f"{name}: ✓")
    else:
        print(f"{name}: ✗")
        print(table)
        print()


def main():
    tests = [
        ("acknowledgements", test_acknowledgements),
        ("articles", test_articles),
        ("bib_varia", test_bib_varia),
        # ("bibrole", test_bibrole),
        ("blog_post", test_blog_post),
        ("blog", test_blog),
        ("book_chapter", test_book_chapters),
        ("book_clusters", test_book_clusters),
        ("book_series", test_book_series),
        ("document_acknowledgement", test_document_acknowledgements),
        ("document_management", test_document_management),
        # ("document_keyword", test_document_keywords),
        ("journal", test_journals),
        ("management", test_managements),
        ("manuscript", test_manuscripts),
        ("metre", test_metres),
        ("original_poem", test_original_poem),
        ("original_poem_verse", test_original_poem_verse),
        ("person_self_designation", test_person_self_designations),
        ("phd", test_phds),
        ("reconstructed_poem", test_reconstructed_poems),
        ("self_designation", test_self_designations),
        ("online_source", test_online_source),

        # bibrole
        # book
        # document
        # document_contains
        # document_genre
        # document_group
        # document_image
        # document_keyword
        # document_status
        # document_title
        # entity
        # entity_management
        # entity_url
        # evidence
        # evidence_factoid
        # factoid
        # factoid_backup_26082025
        # factoid_type
        # fund
        # genre
        # global_id
        # identifier
        # image
        # institution
        # journal_issue
        # keyword
        # language
        # lemma_cache
        # library
        # located_at
        # location
        # monastery
        # name
        # node
        # occupation
        # person
        # person_acknowledgement
        # person_email
        # person_occupation
        # poem
        # poem_meter
        # reconstructed_poem_lemma
        # reference
        # reference_type
        # region
        # role
        # status
        # translation
        # translation_of
        # transliterationsystem
    ]

    for name, test_func in tests:
        success, table = test_func()
        print_result(name, success, table)


if __name__ == "__main__":
    main()