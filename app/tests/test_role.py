from db import get_sqlite_connection, get_postgres_connection
from utils import compare_sets


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()
    sqlite_conn, sqlite_cursor = get_sqlite_connection()

    # Fetch roles from Postgres
    pg_cursor.execute("""
        SELECT idrole, name, system_name, created, modified
        FROM data.role
    """)
    pg_roles = pg_cursor.fetchall()

    # Fetch roles from SQLite
    sqlite_cursor.execute("""
        SELECT id, name, created, modified
        FROM roles
    """)
    sqlite_roles = sqlite_cursor.fetchall()

    pg_set = set()
    sqlite_set = set()
    mismatch_name_capitalization = []

    for idrole, name, system_name, created, modified in pg_roles:
        # Build a normalized key for Postgres
        pg_key = (str(idrole), name, str(created), str(modified))
        pg_set.add(pg_key)

    for id_, name, created, modified in sqlite_roles:
        sqlite_key = (str(id_), name, str(created), str(modified))
        sqlite_set.add(sqlite_key)

    # Check for Postgres names that differ more than capitalization
    for idrole, name, system_name, created, modified in pg_roles:
        # Find matching SQLite role by id
        match = next((r for r in sqlite_roles if str(r[0]) == str(idrole)), None)
        if match:
            sqlite_name = match[1]
            if name.lower() != sqlite_name.lower():
                mismatch_name_capitalization.append(
                    (idrole, name, sqlite_name)
                )

    pg_conn.close()
    sqlite_conn.close()

    result = compare_sets(
        "role",
        sqlite_set,
        pg_set,
        label="id + name + created + modified"
    )

    if mismatch_name_capitalization:
        print("Roles with name differences beyond capitalization:")
        for idrole, pg_name, sqlite_name in mismatch_name_capitalization:
            print(f"idrole={idrole}, postgres='{pg_name}', sqlite='{sqlite_name}'")

    return result