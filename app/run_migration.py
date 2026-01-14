import sys
from init_db import create_base_tables
from migrate_verses import migrate_verses
from migrate_persons import migrate_persons
from migrate_types import migrate_types
from migrate_occurrences import migrate_occurrences
from migrate_manuscripts import migrate_manuscripts
from migrate_bibliographies import migrate_bibliographies

def run_migration():

    steps = [
        ("Initializing database", create_base_tables),
        ("Migrating verses", migrate_verses),
        ("Migrating persons", migrate_persons),
        ("Migrating manuscripts", migrate_manuscripts),
        ("Migrating bibliographies", migrate_bibliographies),
        ("Migrating types", migrate_types),
        ("Migrating occurrences", migrate_occurrences),
    ]
    
    for i, (step_name, step_func) in enumerate(steps, 1):
        try:
            step_func()
            print(f"{step_name}")
        except Exception as e:
            print(f"Error in {step_name}: {str(e)}")
            print(f"Migration failed at step {i}")
            sys.exit(1)

if __name__ == "__main__":
    run_migration()
