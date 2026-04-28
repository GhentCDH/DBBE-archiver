import unicodedata
import os
import requests
from dotenv import load_dotenv
from datetime import date
import re
import subprocess
import markdown
import unicodedata


SOURCE_RECORD_ID=os.getenv("DEPOSITION_ID", "")
load_dotenv()
ZENODO_TOKEN = os.getenv("ZENODO_TOKEN", "")
ZENODO_API_URL = os.getenv("ZENODO_API_URL", "https://sandbox.zenodo.org/api/deposit/depositions")
DEPOSITION_TITLE = os.getenv("DEPOSITION_TITLE", "Dataset of Byzantine Book Epigrams")
ZENODO_BASE = os.getenv("ZENODO_BASE", "https://sandbox.zenodo.org/")

NEW_CREATORS = [
    {"name": "Kyriaki Giannikou", "orcid": "0000-0002-5865-0810", "affiliation":"Ghent University"},
    {"name": "Eleonora Lauro", "orcid": "0009-0008-1228-617X", "affiliation":"Ghent University"},
    {"name": "Juan Bautista Juan López", "orcid": "0000-0002-7092-5338", "affiliation":"Ghent University"},
    {"name": "Francesca Samorì", "orcid": "0000-0003-1093-6980", "affiliation":"Ghent University"},
    {"name": "Paulien Lemay", "orcid": "0009-0004-2388-9233", "affiliation":"Ghent University"},
    {"name": "Joren Six", "orcid": "0000-0001-7671-1907", "affiliation":"Ghent University"},
    {"name": "Frederic Lamsens", "orcid": "0000-0002-1527-5723", "affiliation":"Ghent University"},
    {"name": "Maxime Deforche", "orcid": "0000-0002-2132-0439", "affiliation":"Ghent University"},
    {"name": "Ricarda Schier", "orcid": "0000-0002-3751-7535", "affiliation":"Ghent University"},
    {"name": "Grigory Vorobyev", "orcid": "0009-0006-3691-4746", "affiliation":"Ghent University"},
    {"name": "PIRIL US MACLENNAN", "orcid": "0000-0003-1344-1633", "affiliation":"Ghent University"},
    {"name": "Floris Bernard", "orcid":"0000-0003-3041-2762", "affiliation":"Ghent University"},
    {"name": "Marthe Nemegeer", "orcid": "0009-0001-9901-0916"},
    {"name": "Raf Praet", "orcid": "0000-0003-4793-5308"},
    {"name": "Lev Shadrin", "orcid": "0009-0000-9743-9981"},
    {"name":"Sofia Belioti","orcid":"0000-0002-4760-2637"},
    {"name": "Anna Gregoriani", "orcid":"0009-0006-0589-1588"},
    {"name":"Evelyne Diels", "orcid":"0009-0001-8176-9019"},
    {"name":"Quinten Goethals", "orcid":"0009-0005-5835-3110"}

]
CONTRIBUTORS = [
    {
        "name": "Lemay, Paulien",
        "affiliation": "Ghent CDH",
        "orcid": "0009-0004-2388-9233",
        "type": "ContactPerson"
    },
    {
        "name": "Demoen, Kristoffel",
        "orcid": "0000-0003-3831-6329",
        "type": "ContactPerson"
    }
]
TWO_WORD_FIRST_NAMES = {"Juan Bautista"}

AFFILIATION_ALIASES = [
    (["universiteit gent", "university ghent", "ugent", "ghent university", "ghent univ"], "Ghent University"),
    (["ku leuven", "katholieke universiteit leuven", "university of leuven"], "KU Leuven"),
    (["vrije universiteit brussel", "vub"], "Vrije Universiteit Brussel"),
    (["université libre de bruxelles", "ulb"], "Université libre de Bruxelles"),
]

######################
import sqlite3
import tempfile

def get_latest_sqlite_url(deposition_id: str, headers: dict) -> str | None:
    r = requests.get(f"{ZENODO_BASE}api/records/{deposition_id}/versions/latest", headers=headers)
    r.raise_for_status()
    files = r.json().get("files", [])
    for f in files:
        if f.get("key", "").endswith(".sqlite"):
            return f["links"]["self"]
    return None


def get_table_row_counts(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM \"{table}\"")
        counts[table] = cursor.fetchone()[0]
    conn.close()
    return counts


def has_changes(new_sqlite_path: str, deposition_id: str, headers: dict) -> bool:
    print("Checking for changes against latest published version...")
    url = get_latest_sqlite_url(deposition_id, headers)
    if not url:
        print("  No previous SQLite found — treating as first upload.")
        return True

    print(f"  Downloading previous SQLite for comparison...")
    r = requests.get(url, headers=headers, stream=True)
    r.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    for chunk in r.iter_content(chunk_size=8192):
        tmp.write(chunk)
    tmp.close()

    try:
        old_counts = get_table_row_counts(tmp.name)
        new_counts = get_table_row_counts(new_sqlite_path)
    finally:
        os.unlink(tmp.name)

    if old_counts == new_counts:
        print("  No changes detected — skipping deployment.")
        return False

    # Log what changed
    all_tables = sorted(set(old_counts) | set(new_counts))
    for table in all_tables:
        old_n = old_counts.get(table)
        new_n = new_counts.get(table)
        if old_n != new_n:
            if old_n is None:
                print(f"  + new table: {table} ({new_n} rows)")
            elif new_n is None:
                print(f"  - removed table: {table}")
            else:
                sign = "+" if new_n > old_n else ""
                print(f"  ~ {table}: {sign}{new_n - old_n} rows ({old_n} → {new_n})")

    return True

#######################

def normalize_affiliation(affiliation: str) -> str:
    """Map known affiliation variants to a canonical name."""
    lower = affiliation.strip().lower()
    for variants, canonical in AFFILIATION_ALIASES:
        if any(variant in lower for variant in variants):
            return canonical
    return affiliation  # return as-is if no match found

def get_affiliation_from_orcid(orcid: str) -> str | None:
    url = f"https://pub.orcid.org/v3.0/{orcid}/employments"
    headers = {"Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        r.raise_for_status()
        data = r.json()

        groups = data.get("affiliation-group", [])
        if not groups:
            return None

        for group in groups:
            for summary in group.get("summaries", []):
                emp = summary.get("employment-summary", {})
                if emp.get("end-date") is None:  # currently active
                    name = emp.get("organization", {}).get("name")
                    if name:
                        return normalize_affiliation(name)
        return None
    except Exception as e:
        print(f"Could not fetch affiliation for ORCID {orcid}: {e}")
        return None

def enrich_with_orcid(creator: dict) -> dict:
    orcid = creator.get("orcid")
    if not orcid:
        return creator

    orcid_affiliation = get_affiliation_from_orcid(orcid)
    if not orcid_affiliation:
        return creator

    existing_affiliation = creator.get("affiliation")
    # Also normalize the existing affiliation for comparison, so that
    # e.g. "Ghent CDH" vs "Ghent University" is caught correctly
    normalized_existing = normalize_affiliation(existing_affiliation) if existing_affiliation else None

    if not existing_affiliation:
        print(f"  {creator['name']}: adding affiliation from ORCID → {orcid_affiliation}")
        return {**creator, "affiliation": orcid_affiliation}
    elif normalized_existing != orcid_affiliation:
        print(f"  {creator['name']}: updating affiliation {existing_affiliation!r} → {orcid_affiliation!r}")
        return {**creator, "affiliation": orcid_affiliation}
    else:
        # Affiliation matches after normalization — but update the stored string
        # to the canonical form if it wasn't already
        if existing_affiliation != normalized_existing:
            return {**creator, "affiliation": normalized_existing}
        return creator
def normalize(s):
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii").lower()

def sort_key(creator):
    name = creator.get("name", "").strip()
    parts = name.split()

    for first in TWO_WORD_FIRST_NAMES:
        if name.startswith(first):
            return normalize(parts[2])

    return normalize(parts[1]) if len(parts) > 1 else normalize(name)

def get_creators_from_record(record_id: str, headers: dict) -> list:
    r = requests.get(f"{ZENODO_BASE}api/records/{record_id}", headers=headers)
    r.raise_for_status()
    creators = r.json().get("metadata", {}).get("creators", [])
    print(f"Found {len(creators)} creators from record {record_id}")
    return creators


def get_merged_creators(record_id: str, headers: dict, new_creators: list) -> list:
    r = requests.get(f"{ZENODO_BASE}api/records/{record_id}", headers=headers)
    r.raise_for_status()
    existing_creators = r.json().get("metadata", {}).get("creators", [])
    print(f"Found {len(existing_creators)} creators from record {record_id}")

    new_orcids = {c.get("orcid") for c in new_creators if c.get("orcid")}
    new_names = {normalize(c.get("name", "")) for c in new_creators}

    filtered_existing = [
        c for c in existing_creators
        if c.get("orcid") not in new_orcids
        and normalize(c.get("name", "")) not in new_names
    ]
    overridden = len(existing_creators) - len(filtered_existing)
    print(f"Overriding {overridden} existing creators with updated entries from NEW_CREATORS")

    # Merge: NEW_CREATORS takes priority over existing for overlapping people,
    # then enrich everyone with live ORCID data (only adds/updates, never removes)
    merged = [enrich_with_orcid(c) for c in filtered_existing + new_creators]

    pinned = [c for c in merged if c.get("name") == "Kristoffel Demoen"]
    rest = sorted(
        [c for c in merged if c.get("name") != "Kristoffel Demoen"],
        key=sort_key
    )

    return pinned + rest

def markdown_to_html(md_text):
    return markdown.markdown(
        md_text,
        extensions=[
            "extra",        # tables, fenced code, etc.
            "sane_lists",
            "toc"
        ]
    )

def extract_db_schema_from_readme(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        content = f.read()

    match = re.search(
        r"<!-- BEGIN DB_SCHEMA -->(.*?)<!-- END DB_SCHEMA -->",
        content,
        re.DOTALL
    )

    if not match:
        raise RuntimeError("DB_SCHEMA section not found in README.md")

    return match.group(1).strip()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /app/app
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))  # /app
README_PATH = os.path.join(PROJECT_ROOT, "README.md")
html_template_path = os.path.join(BASE_DIR, "DB_DESCRIPTION.html")

with open(html_template_path, "r", encoding="utf-8") as f:
    html_template = f.read()

schema_md = extract_db_schema_from_readme(README_PATH)
schema_html = markdown_to_html(schema_md)


description_text = html_template.replace(
    "<!-- DB_SCHEMA_PLACEHOLDER -->",
    schema_html
)

def upload_sqlite_files_to_zenodo(folder_path, publish, deposition_id):
    headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"}
    today_str = date.today().isoformat()

    # ── Find new sqlite ───────────────────────────────────────────────────────
    sqlite_files = [f for f in os.listdir(folder_path) if f.endswith(".sqlite")]
    if not sqlite_files:
        print("No SQLite files found — aborting.")
        return
    new_sqlite_path = os.path.join(folder_path, sqlite_files[0])

    # ── Bail out if nothing changed ───────────────────────────────────────────
    if deposition_id is not None and not has_changes(new_sqlite_path, deposition_id, headers):
        print("Nothing to deploy.")
        return
    #########################

    creators = get_merged_creators(SOURCE_RECORD_ID, headers, NEW_CREATORS)

    deposition_data = {
        "metadata": {
            "title": DEPOSITION_TITLE,
            "upload_type": "dataset",
            "description": description_text,
            "creators": creators or [{"name": "Paulien Lemay", "affiliation": "Ghent CDH"}],
            "access_right": "open",
            "contributors": CONTRIBUTORS,
            "publication_date": today_str,
            "keywords": ["Byzantine studies","Manuscript studies","Byzantium", "Digital humanities"],
            "related_identifiers": [{'relation': 'isSupplementTo',
                                     'identifier':'10.5281/zenodo.7682522',
                                     "resource_type":"dataset",
                                     "scheme":"doi"}],
            "subjects":[{"term": "Digital humanities",
                         "identifier": "https://publications.europa.eu/resource/authority/8mn/euroscivoc/0627c833-88fb-4bbd-86b4-1eb20529fb17",
                         "scheme": "url"}],
            "custom": {
                "code:codeRepository": "https://github.com/GhentCDH/DBBE-archiver",
                "code:programmingLanguage": [
                    {
                        "id": "python",
                        "title": {
                            "en": "Python"
                        }
                    }
                ],
                "code:developmentStatus": {
                    "id": "active",
                    "title": {
                        "en": "Active"
                    }
                }
            },
            "language":"eng"
        }
    }

    if deposition_id is None:
        print("Creating new deposition...")
        r = requests.post(ZENODO_API_URL, params={}, json=deposition_data, headers=headers)
        r.raise_for_status()
        deposition = r.json()
        new_deposition_id = deposition["id"]
        deposition_id = new_deposition_id
        print(f"Created new deposition ID: {new_deposition_id}")

    else:
        print("Creating new version...")
        r = requests.post(f"{ZENODO_API_URL}/{deposition_id}/actions/newversion", headers=headers)
        r.raise_for_status()
        response = r.json()

        latest_draft_url = response['links']['latest_draft']
        new_deposition_id = latest_draft_url.rstrip("/").split("/")[-1]
        deposition_id = new_deposition_id
        print(f"Created new version with draft ID: {new_deposition_id}")

        r_update = requests.put(f"{ZENODO_API_URL}/{deposition_id}", headers=headers, json=deposition_data)
        r_update.raise_for_status()
        print(f"Updated publication date to {today_str}")

        r_files = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
        r_files.raise_for_status()
        deposition_details = r_files.json()

        filename_to_find = "dbbe_archive.sqlite"
        file_id = None
        for f in deposition_details.get("files", []):
            if f["filename"] == filename_to_find:
                file_id = f["id"]
                break

        if file_id:
            try:
                r = requests.delete(f"{ZENODO_API_URL}/{deposition_id}/files/{file_id}", headers=headers)
                r.raise_for_status()
                print(f"✓ Deleted old {filename_to_find}")
            except Exception as e:
                print(f'Failed removing old SQL file: {e}')

    # Upload SQLite files
    sqlite_files = [f for f in os.listdir(folder_path) if f.endswith(".sqlite")]
    print(f"Found {len(sqlite_files)} SQLite files to upload")

    for file_name in sqlite_files:
        file_path = os.path.join(folder_path, file_name)
        print(f"Uploading {file_name}...")
        upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"
        with open(file_path, 'rb') as f:
            files = {'file': f}
            data = {'name': file_name}
            resp = requests.post(upload_url, headers=headers, files=files, data=data)
            if not resp.ok:
                print(f"Upload failed: {resp.status_code}")
                print(resp.json())
            resp.raise_for_status()
        print(f"✓ Uploaded {file_name}")

    if publish:
        print(f"Publishing deposition {deposition_id}...")
        publish_url = f"{ZENODO_API_URL}/{deposition_id}/actions/publish"
        pub_resp = requests.post(publish_url, headers=headers)
        if pub_resp.status_code == 202:
            print(f"✓ Deposition {deposition_id} published successfully.")
        else:
            print(f"Deposition {deposition_id} created but not published. Status: {pub_resp.status_code}")
            print(pub_resp.text)
    else:
        print(f"Deposition {deposition_id} saved as draft (not published)")

    print(f"\nAll files processed for deposition {deposition_id}.")