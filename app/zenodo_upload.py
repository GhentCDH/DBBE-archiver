import os
import requests
from dotenv import load_dotenv
from datetime import date
import re
import subprocess
import markdown
SOURCE_RECORD_ID=7682523
load_dotenv()
ZENODO_TOKEN = os.getenv("ZENODO_TOKEN", "")
ZENODO_API_URL = os.getenv("ZENODO_API_URL", "https://sandbox.zenodo.org/api/deposit/depositions")
DEPOSITION_TITLE = os.getenv("DEPOSITION_TITLE", "Database of Byzantine Book Epigrams - Archive")
NEW_CREATORS = [
    {"name": "Kyriaki Giannikou", "orcid": "0000-0002-5865-0810"},
    {"name": "Eleonora Lauro", "orcid": "0009-0008-1228-617X"},
    {"name": "Juan Bautista Juan López", "orcid": "0000-0002-7092-5338"},
    {"name": "Francesca Samori", "orcid": "0000-0003-1093-6980"},
    {"name": "Joren Six", "orcid": "0000-0001-7671-1907"},
    {"name": "Frederic Lamsens", "orcid": "0000-0002-1527-5723"},
    {"name": "Maxime Deforche", "orcid": "0000-0002-2132-0439"},
    {"name": "Ricarda Schier", "orcid": "0000-0002-3751-7535"},
    {"name": "Grigory Vorobyev", "orcid": "0009-0006-3691-4746"},
    {"name": "PIRIL US MACLENNAN", "orcid": "0000-0003-1344-1633"},

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


def sort_key(creator):
    name = creator.get("name", "").strip()
    parts = name.split()

    for first in TWO_WORD_FIRST_NAMES:
        if name.startswith(first):
            return parts[2].lower()

    return parts[1].lower() if len(parts) > 1 else name.lower()

def get_creators_from_record(record_id: str, headers: dict) -> list:
    r = requests.get(f"https://zenodo.org/api/records/{record_id}", headers=headers)
    r.raise_for_status()
    creators = r.json().get("metadata", {}).get("creators", [])
    print(f"Found {len(creators)} creators from record {record_id}")
    return creators

def get_merged_creators(record_id: str, headers: dict, new_creators: list) -> list:
    r = requests.get(f"https://zenodo.org/api/records/{record_id}", headers=headers)
    r.raise_for_status()
    existing_creators = r.json().get("metadata", {}).get("creators", [])
    print(f"Found {len(existing_creators)} creators from record {record_id}")

    existing_orcids = {c.get("orcid") for c in existing_creators if c.get("orcid")}
    added = []
    for creator in new_creators:
        if creator.get("orcid") not in existing_orcids:
            existing_creators.append(creator)
            added.append(creator["name"])

    print(f"Added {len(added)} new creators: {', '.join(added)}")

    pinned = [c for c in existing_creators if c.get("name") == "Kristoffel Demoen"]
    rest = sorted(
        [c for c in existing_creators if c.get("name") != "Kristoffel Demoen"],
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
            "keywords": ["Byzantine studies","Manuscript studies","Byzantium", "Digital Humanities"],
        }
    }
    print(deposition_data)
    if deposition_id is None:
        print("Creating new deposition...")
        r = requests.post(ZENODO_API_URL, params={}, json=deposition_data, headers=headers)
        r.raise_for_status()
        deposition = r.json()
        new_deposition_id = deposition["id"]
        deposition_id = new_deposition_id
        print(f"Created new deposition ID: {new_deposition_id}")

    else:
        # r = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
        # r.raise_for_status()
        # details = r.json()
        #
        # latest_draft_url = details['links'].get('latest_draft')
        # if latest_draft_url:
        #     new_deposition_id = latest_draft_url.rstrip("/").split("/")[-1]
        #     print(f"Using existing draft: {new_deposition_id}")
        # else:
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