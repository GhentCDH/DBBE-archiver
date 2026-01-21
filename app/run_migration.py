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
import argparse
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
    parser = argparse.ArgumentParser(description='Run migration and optionally upload to Zenodo')
    parser.add_argument(
        '--zenodo-mode',
        choices=['draft', 'publish', 'update'],
        default=os.getenv("ZENODO_MODE"),
        help='Zenodo upload mode (can also use ZENODO_MODE env var)'
    )
    parser.add_argument(
        '--zenodo-id',
        type=str,
        default=os.getenv("ZENODO_ID"),
        help='Existing Zenodo deposition ID (can also use ZENODO_ID env var)'
    )

    args = parser.parse_args()

    # run_migration()

    if args.zenodo_mode:
        if args.zenodo_mode == "update" and not args.zenodo_id:
            print("--zenodo-id required when mode is 'update'")
            sys.exit(1)

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        data_folder = os.path.join(BASE_DIR, 'data')

        upload_sqlite_files_to_zenodo(
            data_folder,
            mode=args.zenodo_mode,
            deposition_id=args.zenodo_id
        )
    else:
        print("Zenodo upload not enabled. Use --zenodo-mode to enable.")
