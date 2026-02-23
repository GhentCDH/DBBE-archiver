from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_id, normalize_string, compare_sets

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # ----------------------------
    # Fetch from SQLite
    # ----------------------------
    sqlite_cursor.execute("SELECT id, url, post_date FROM blog_post")
    sqlite_set = {
        (
            normalize_id(row[0]),
            normalize_string(row[1]),
            str(row[2])
        )
        for row in sqlite_cursor.fetchall()
    }

    # ----------------------------
    # Fetch from Postgres
    # ----------------------------
    pg_cursor.execute("SELECT identity, url, post_date FROM data.blog_post")
    pg_set = {
        (
            normalize_id(row[0]),
            normalize_string(row[1]),
            str(row[2])
        )
        for row in pg_cursor.fetchall()
    }


    sqlite_conn.close()
    pg_conn.close()


    return compare_sets(
        "blog_posts",
        sqlite_set,
        pg_set,
        label="(Identity, URL, Post Date)"
    )

if __name__ == "__main__":
    success, table = run_test()
    if success:
        print("Blog posts: ✓ All rows match")
    else:
        print("Blog posts differences:\n", table)