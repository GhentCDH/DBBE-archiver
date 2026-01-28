from enum import Enum

class BiblioEntity(Enum):
    MANUSCRIPT = ("manuscripts", "id")
    PERSON = ("persons", "id")
    OCCURRENCE = ("occurrences", "id")
    TYPE = ("types", "id")

    def __init__(self, sqlite_table: str, id_column: str):
        self.sqlite_table = sqlite_table
        self.id_column = id_column
