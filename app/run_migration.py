import sys
from .init_db import create_base_tables
from .migrate_verses.migrate_verses import migrate_verses
from .migrate_persons import migrate_persons
from .migrate_types import migrate_types
from .migrate_occurrences import migrate_occurrences
from .migrate_manuscripts import migrate_manuscripts
from .migrate_bibliographies import migrate_bibliographies
from .zenodo_upload import upload_sqlite_files_to_zenodo
from .common import NORMALIZATION_STATS
import os

def str_to_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def run_migration():
    steps = [
        ("Initializing database", create_base_tables),
        ("Migrating verses", migrate_verses),
        ("Migrating persons", migrate_persons),
        ("Migrating manuscripts", migrate_manuscripts),
        ("Migrating occurrence", migrate_occurrences),
        ("Migrating type", migrate_types),
        ("Migrating bibliographies", migrate_bibliographies),

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
    print(
        f"Normalization stats: {NORMALIZATION_STATS['changed']} changed, {NORMALIZATION_STATS['unchanged']} unchanged")
    if NORMALIZATION_STATS["samples"]:
        print("Some sample changes:")
        for original, normalized in NORMALIZATION_STATS["samples"]:
            print(f"'{original}' → '{normalized}'")

    enable_zenodo_upload = str_to_bool(
        os.getenv("ENABLE_ZENODO_UPLOAD", "false")
    )

    if enable_zenodo_upload:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        publish_draft=str_to_bool(os.getenv("PUBLISH_DRAFT", "false"))
        deposition_id=os.getenv("DEPOSITION_ID", None)
        data_folder = os.path.join(BASE_DIR, 'data')
        upload_sqlite_files_to_zenodo(data_folder,publish_draft,deposition_id)
    else:
        print("Zenodo upload not enabled")