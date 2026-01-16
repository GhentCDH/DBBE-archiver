import sys
from init_db import create_base_tables
from migrate_verses import migrate_verses
from migrate_persons import migrate_persons
from migrate_types import migrate_types
from migrate_occurrences import migrate_occurrences
from migrate_manuscripts import migrate_manuscripts
from migrate_bibliographies import migrate_bibliographies
from zenodo_upload import upload_sqlite_files_to_zenodo
import os

def str_to_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}

def run_migration():

    steps = [
        ("Initializing database", create_base_tables),
        ("Migrating verses", migrate_verses),
        ("Migrating persons", migrate_persons),
        ("Migrating manuscripts", migrate_manuscripts),
        ("Migrating bibliographies", migrate_bibliographies),
        ("Migrating occurrences", migrate_occurrences),
        ("Migrating types", migrate_types)
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

    enable_zenodo_upload = str_to_bool(
        os.getenv("ENABLE_ZENODO_UPLOAD", "true")
    )

    if enable_zenodo_upload:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        data_folder = os.path.join(BASE_DIR, 'data')
        upload_sqlite_files_to_zenodo(data_folder)
    else:
        print("Zenodo upload not enabled")
