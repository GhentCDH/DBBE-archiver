from db import get_sqlite_connection, get_postgres_connection
from utils import normalize_string, compare_sets

def run_test():
    sqlite_conn, sqlite_cursor = get_sqlite_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    # 1. Get fund names in Postgres that are linked to at least one manuscript
    pg_cursor.execute("""
        SELECT DISTINCT f.name
        FROM data.fund f
        INNER JOIN data.location l ON l.idfund = f.idfund
        INNER JOIN data.located_at la ON la.idlocation = l.idlocation
        INNER JOIN data.manuscript m ON m.identity = la.iddocument
        LEFT JOIN data.factoid fac ON fac.idlocation = l.idlocation
                                AND fac.subject_identity = m.identity
        WHERE m.identity IS NOT NULL OR fac.subject_identity IS NOT NULL
    """)
    pg_fund_names = {normalize_string(row[0]) for row in pg_cursor.fetchall() if row[0]}

    # 2. Get all collection values from SQLite manuscripts
    sqlite_cursor.execute("SELECT collection FROM manuscript")
    sqlite_collections = {normalize_string(row[0]) for row in sqlite_cursor.fetchall() if row[0]}

    # Close connections
    sqlite_conn.close()
    pg_conn.close()

    # 3. Use compare_sets to report differences
    return compare_sets(
        "fund names in Postgres vs manuscript.collection in SQLite",
        sqlite_collections,
        pg_fund_names,
        label="Normalized Fund Name / Collection"
    )