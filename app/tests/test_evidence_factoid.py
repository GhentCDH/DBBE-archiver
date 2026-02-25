from db import get_postgres_connection


def run_test():
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("SELECT COUNT(*) FROM data.evidence_factoid")
    count = pg_cursor.fetchone()[0]

    pg_conn.close()

    if count > 0:
        return False, f"data.evidence_factoid is no longer empty — {count} rows found. Migration may be needed."

    return True, None