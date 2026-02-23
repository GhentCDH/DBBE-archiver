

from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string
from tabulate import tabulate

# All person_role tables in SQLite
SQLITE_ROLE_TABLES = [
    "article_person_role",
    "bib_varia_person_role",
    "blog_post_person_role",
    "book_person_role",
    "book_chapter_person_role",
    "blog_post_person_role",
    "manuscript_person_role",
    "occurrence_person_role",
    "online_source_person_role",
    "phd_person_role",
    "type_person_role",
]

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # ----------------------------
    # POSTGRES SIDE
    # ----------------------------
    pg_cursor.execute("""
        SELECT br.iddocument,
               br.idperson,
               r.name
        FROM data.bibrole br
        JOIN data.role r
          ON r.idrole = br.idrole
    """)
    pg_rows = pg_cursor.fetchall()

    # Split normal roles vs Content roles
    pg_roles_normal = []
    pg_roles_content = []

    for iddocument, idperson, role_name in pg_rows:
        role_name_norm = normalize_string(role_name)
        if role_name_norm == "content":
            pg_roles_content.append((iddocument, idperson))
        else:
            pg_roles_normal.append((iddocument, idperson, role_name_norm))

    # ----------------------------
    # SQLITE SIDE - NORMAL ROLES
    # ----------------------------

    # preload roles table
    sqlite_cursor.execute("SELECT id, name FROM roles")
    sqlite_roles = {role_id: normalize_string(name) for role_id, name in sqlite_cursor.fetchall()}

    sqlite_set = set()

    for table in SQLITE_ROLE_TABLES:
        # derive document_id column
        document_prefix = table.replace("_person_role", "")
        document_id_column = f"{document_prefix}_id"

        sqlite_cursor.execute(
            f"SELECT {document_id_column}, person_id, role_id FROM {table}"
        )
        rows = sqlite_cursor.fetchall()

        for document_id, person_id, role_id in rows:
            role_name = sqlite_roles.get(role_id, "")
            sqlite_set.add(
                (
                    str(document_id),
                    str(person_id),
                    role_name
                )
            )

    # Build Postgres normal role set
    pg_set = set((str(did), str(pid), rname) for did, pid, rname in pg_roles_normal)

    # ----------------------------
    # Compare normal roles
    # ----------------------------
    only_in_pg = pg_set - sqlite_set
    only_in_sqlite = sqlite_set - pg_set

    normal_diffs = []
    for row in sorted(only_in_pg):
        normal_diffs.append([*row, "✓", ""])
    for row in sorted(only_in_sqlite):
        normal_diffs.append([*row, "", "✓"])

    # ----------------------------
    # SQLITE SIDE - CONTENT ROLES
    # ----------------------------
    # Preload content table
    sqlite_cursor.execute("SELECT id, name FROM content")
    sqlite_content = {normalize_string(name): cid for cid, name in sqlite_cursor.fetchall()}

    # Preload manuscript_content table
    sqlite_cursor.execute("SELECT manuscript_id, content_id FROM manuscript_content")
    manuscript_content_rows = sqlite_cursor.fetchall()
    sqlite_content_set = set(manuscript_content_rows)

    # Check content roles
    content_diffs = []
    content_id = sqlite_content.get("content")
    for manuscript_id, idperson in pg_roles_content:
        if content_id is None or (manuscript_id, content_id) not in sqlite_content_set:
            content_diffs.append([manuscript_id, idperson, "Missing Content"])

    # ----------------------------
    # Close connections
    # ----------------------------
    sqlite_conn.close()
    pg_conn.close()

    # ----------------------------
    # Prepare final table
    # ----------------------------
    if not normal_diffs and not content_diffs:
        return True, None

    rows = []

    if normal_diffs:
        rows.append(["--- NORMAL ROLES ---", "", "", "", ""])
        rows.extend(normal_diffs)
    if content_diffs:
        rows.append(["--- CONTENT ROLES ---", "", "", ""])
        rows.extend(content_diffs)

    # Determine headers dynamically
    headers = ["Document ID", "Person ID", "Role Name", "Only in Postgres", "Only in SQLite"]
    if content_diffs:
        headers = ["Document ID", "Person ID", "Note"]

    table = tabulate(rows, headers=headers, tablefmt="grid")

    return False, table