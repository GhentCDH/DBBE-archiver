from enum import StrEnum  # Python 3.11+, fallback to Enum for older versions

class BiblioType(StrEnum):
    ARTICLE = "article"
    BLOG_POST = "blog_post"
    BOOK = "book"
    BOOK_CHAPTER = "book_chapter"
    ONLINE_SOURCE = "online_source"
    PHD = "phd"
    BIB_VARIA = "bib_varia"
