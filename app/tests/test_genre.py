from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets


def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()


    pg_cursor.execute("""
        SELECT idgenre, genre, description, is_content, idparentgenre
        FROM data.genre
    """)
    pg_rows = pg_cursor.fetchall()

    pg_genre_set = set()
    pg_content_set = set()
    for row in pg_rows:
        idgenre, genre_name, description, is_content, parent_id = row
        idgenre_str = str(idgenre)
        genre_name_norm = normalize_string(genre_name)
        description_norm = normalize_string(description) if description else None

        if is_content:
            pg_content_set.add((idgenre_str, genre_name_norm, str(parent_id) if parent_id else None))
        else:
            pg_genre_set.add((idgenre_str, genre_name_norm, description_norm))


    sqlite_cursor.execute("SELECT id, name, description FROM genre")
    sqlite_genre_set = {
        (str(row[0]), normalize_string(row[1]), normalize_string(row[2]) if row[2] else None)
        for row in sqlite_cursor.fetchall()
    }


    sqlite_cursor.execute("SELECT id, name, parent_id FROM content")
    sqlite_content_set = {
        (str(row[0]), normalize_string(row[1]), str(row[2]) if row[2] else None)
        for row in sqlite_cursor.fetchall()
    }

    sqlite_conn.close()
    pg_conn.close()

    genre_result = compare_sets(
        "genre table coverage",
        sqlite_genre_set,
        pg_genre_set,
        label="ID + Name + Description"
    )

    content_result = compare_sets(
        "content table coverage",
        sqlite_content_set,
        pg_content_set,
        label="ID + Name + Parent ID"
    )

    return genre_result, content_result