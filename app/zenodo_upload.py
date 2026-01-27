import os
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()
ZENODO_TOKEN = os.getenv("ZENODO_TOKEN", "")
ZENODO_API_URL = os.getenv("ZENODO_API_URL", "https://sandbox.zenodo.org/api/deposit/depositions")
DEPOSITION_TITLE = os.getenv("DEPOSITION_TITLE", "Database of Byzantine Book Epigrams - Archive")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
description_file_path = os.path.join(BASE_DIR, "DB_DESCRIPTION.html")

with open(description_file_path, "r", encoding="utf-8") as f:
    description_text = f.read()


def upload_sqlite_files_to_zenodo(folder_path, publish, deposition_id):
    headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"}
    today_str = date.today().isoformat()
    deposition_data = {
        "metadata": {
            "title": DEPOSITION_TITLE,
            "upload_type": "dataset",
            "description": description_text,
            "creators": [{"name": "Paulien Lemay", "affiliation": "Ghent CDH"}],
            "access_right": "restricted",
            "publication_date": today_str
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

        # Step 3: Update the metadata (including updated publication date)
        r_update = requests.put(f"{ZENODO_API_URL}/{deposition_id}", headers=headers, json=deposition_data)
        r_update.raise_for_status()
        print(f"Updated publication date to {today_str}")

        r_files = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
        r_files.raise_for_status()
        deposition_details = r_files.json()

        filename_to_find = "export_data.sqlite"
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