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
from test_person import run_test as test_person
from test_person_email import run_test as test_person_email
from test_person_acknowledgement import run_test as test_person_acknowledgement
from test_node import run_test as test_node
from test_translation import run_test as test_translation
from test_translation_of import run_test as test_translation_of
from test_language import run_test as test_language
from test_evidence import run_test as test_evidence
from test_evidence_factoid import run_test as test_evidence_factoid
from test_transliteration_system import run_test as test_transliteration_system
from test_name import run_test as test_name
from test_person_occupation import run_test as test_person_occupation
from test_poem_meter import run_test as test_poem_meter
from test_role import run_test as test_role
# from test_keyword import run_test as test_keyword
from test_journal_issue import run_test as test_journal_issue
from test_reconstructed_poem_lemma import run_test as test_reconstructed_poem_lemma
from test_document_genre import run_test as test_document_genre
from test_poem import run_test as test_poem
from test_genre import run_test as test_genre
from test_book import run_test as test_book
from test_library import run_test as test_library
from test_fund import run_test as test_fund

from test_identification import run_test as test_identification

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
        ("book", test_book),
        ("book_chapter", test_book_chapters),
        ("book_clusters", test_book_clusters),
        ("book_series", test_book_series),
        ("document_acknowledgement", test_document_acknowledgements),
        ("document_genre", test_document_genre),
        ("document_management", test_document_management),
        # ("document_keyword", test_document_keywords),
        ("evidence", test_evidence),
        ("evidence_factoid", test_evidence_factoid),
        ("fund", test_fund),
        ("genre", test_genre),
        ("identification", test_identification),
        ("journal", test_journals),
        ("journal_issue", test_journal_issue),
        # ("keyword", test_keyword),
        ("language", test_language),
        # ("library", test_library),
        ("management", test_managements),
        ("manuscript", test_manuscripts),
        ("metre", test_metres),
        ("name", test_name),
        ("node", test_node),
        ("original_poem", test_original_poem),
        ("original_poem_verse", test_original_poem_verse),
        ("online_source", test_online_source),
        ("person", test_person),
        ("person_acknowledgement", test_person_acknowledgement),
        ("person_self_designation", test_person_self_designations),
        ("person_email", test_person_email),
        ("person_occupation", test_person_occupation),
        # ("poem_meter", test_poem_meter),
        ("phd", test_phds),
        ("poem", test_poem),
        ("reconstructed_poem", test_reconstructed_poems),
        ("reconstructed_poem_lemma", test_reconstructed_poem_lemma),
        ("roles", test_role),
        ("self_designation", test_self_designations),
        ("translation", test_translation),
        ("translation_of", test_translation_of),
        ("transliteration_system", test_transliteration_system),

        # document
        # document_contains
        # document_group
        # document_image
        # document_status
        # document_title
        # entity
        # entity_url
        # factoid
        # factoid_type

        # global_id
        # image
        # institution
        # located_at
        # location
        # occupation
        # reference
        # reference_type
        # region
        # status
    ]

    for name, test_func in tests:
        success, table = test_func()
        print_result(name, success, table)


if __name__ == "__main__":
    main()