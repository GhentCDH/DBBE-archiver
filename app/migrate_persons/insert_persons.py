
import uuid
from ..common import (
    get_db_connection, get_postgres_connection
)


def get_location_hierarchy_and_leaf(cursor, pg_cursor, location_id):
    hierarchy = []
    current_id = location_id
    while current_id:
        pg_cursor.execute("""
            SELECT identity, name, historical_name, parent_idregion
            FROM data.region
            WHERE identity = %s
        """, (current_id,))
        row = pg_cursor.fetchone()
        if not row:
            break
        identity, name, historical_name, parent_id = row
        hierarchy.append((str(identity), name, historical_name, str(parent_id) if parent_id else None))
        current_id = parent_id

    leaf_id = None
    for loc_id, name, hist_name, parent_id in reversed(hierarchy):
        cursor.execute("""
            INSERT OR IGNORE INTO locations (id, name, historical_name, parent_id)
            VALUES (?, ?, ?, ?)
        """, (loc_id, name, hist_name, parent_id))
        leaf_id = loc_id
    return leaf_id

def run_person_migration():
    conn, cursor = get_db_connection()
    pg_conn, pg_cursor = get_postgres_connection()

    pg_cursor.execute("""
        SELECT
            person.identity,
            name.first_name,
            name.last_name,
            factoid_orig.subject_identity,
            factoid_orig.idlocation,
            person.is_historical,
            person.is_modern,
            person.is_dbbe,
            factoid_born.born_date,
            factoid_died.death_date
        FROM data.person person
        INNER JOIN data.name name ON name.idperson = person.identity
        LEFT JOIN (
            SELECT subject_identity, date AS born_date
            FROM data.factoid f
            JOIN data.factoid_type ft ON f.idfactoid_type = ft.idfactoid_type
            WHERE ft.type = 'born'
        ) AS factoid_born ON person.identity = factoid_born.subject_identity
        LEFT JOIN (
            SELECT subject_identity, date AS death_date
            FROM data.factoid f
            JOIN data.factoid_type ft ON f.idfactoid_type = ft.idfactoid_type
            WHERE ft.type = 'died'
        ) AS factoid_died ON person.identity = factoid_died.subject_identity
        LEFT JOIN (
            SELECT subject_identity, idlocation
            FROM data.factoid f
            JOIN data.factoid_type ft ON f.idfactoid_type = ft.idfactoid_type
            WHERE ft.type = 'origination'
        ) AS factoid_orig ON person.identity = factoid_orig.subject_identity
    """)

    rows = pg_cursor.fetchall()

    person_locations = {}
    for row in rows:
        pid = str(row[0])
        loc_id = row[4]
        if loc_id:
            if pid in person_locations:
                raise ValueError(f"Person {pid} has multiple origination locations in Postgres!")
            person_locations[pid] = loc_id

    cursor.execute("BEGIN")
    for row in rows:
        (
            person_id,
            first_name,
            last_name,
            _,
            orig_location_id,
            is_historical,
            is_modern,
            is_dbbe,
            born_date,
            death_date
        ) = row

        person_id = str(person_id)

        cursor.execute("""
        INSERT INTO persons (
            id, first_name, last_name,
            born_date_floor_year, born_date_ceiling_year,
            death_date_floor_year, death_date_ceiling_year,
            is_dbbe_person, is_modern_person, is_historical_person
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            born_date_floor_year = excluded.born_date_floor_year,
            born_date_ceiling_year = excluded.born_date_ceiling_year,
            death_date_floor_year = excluded.death_date_floor_year,
            death_date_ceiling_year = excluded.death_date_ceiling_year,
            is_dbbe_person = excluded.is_dbbe_person,
            is_modern_person = excluded.is_modern_person,
            is_historical_person = excluded.is_historical_person
        """, (
            person_id,
            first_name,
            last_name,
            born_date,
            born_date,
            death_date,
            death_date,
            bool(is_dbbe),
            bool(is_modern),
            bool(is_historical)
        ))

        if orig_location_id:
            pg_cursor.execute("""
                SELECT idregion
                FROM data.location
                WHERE idlocation = %s
            """, (orig_location_id,))
            row_region = pg_cursor.fetchone()
            if row_region and row_region[0]:
                region_id = row_region[0]
                leaf_id = get_location_hierarchy_and_leaf(cursor, pg_cursor, region_id)
                cursor.execute("""
                    UPDATE persons
                    SET location_id = ?
                    WHERE id = ?
                """, (leaf_id, person_id))

    pg_cursor.execute("SELECT id, name FROM data.self_designation")
    for sd_id, sd_name in pg_cursor.fetchall():
        cursor.execute("""
            INSERT OR IGNORE INTO self_designations (id, name)
            VALUES (?, ?)
        """, (str(sd_id), sd_name))

    pg_cursor.execute("SELECT idperson, idself_designation FROM data.person_self_designation")
    for person_id, sd_id in pg_cursor.fetchall():
        cursor.execute("""
            INSERT OR IGNORE INTO person_self_designations (person_id, self_designation_id)
            VALUES (?, ?)
        """, (str(person_id), str(sd_id)))

    pg_cursor.execute("SELECT idoccupation, occupation FROM data.occupation")
    for office_id, office_name in pg_cursor.fetchall():
        cursor.execute("""
            INSERT OR IGNORE INTO offices (id, name)
            VALUES (?, ?)
        """, (str(office_id), office_name))

    pg_cursor.execute("SELECT idperson, idoccupation FROM data.person_occupation")
    for person_id, office_id in pg_cursor.fetchall():
        cursor.execute("""
            INSERT OR IGNORE INTO person_offices (person_id, office_id)
            VALUES (?, ?)
        """, (str(person_id), str(office_id)))

    cursor.execute("COMMIT")
    conn.close()
    pg_conn.close()
