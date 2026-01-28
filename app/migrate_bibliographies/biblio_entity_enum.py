from enum import Enum

class BiblioEntity(Enum):
    MANUSCRIPT = ("manuscript", "manuscript_id")
    PERSON = ("person", "person_id")
    OCCURRENCE = ("occurrence", "occurrence_id")
    TYPE = ("type", "type_id")

    def __init__(self, sqlite_table: str, id_column: str):
        self.sqlite_table = sqlite_table
        self.id_column = id_column
