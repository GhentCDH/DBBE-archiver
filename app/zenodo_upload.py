import os
import requests

ZENODO_TOKEN = "tRT9Go9VCZKhwLsMzWkqNhquRTle30esbmXRNZc89YVJGzVdVqRiGXpYGfcb"
ZENODO_API_URL = "https://sandbox.zenodo.org/api/deposit/depositions"
DEPOSITION_TITLE = "DBBE SQLite Backup"
deposition_data = {
    "metadata": {
        "title": "DBBE SQLite Backup",
        "upload_type": "dataset",
        "description": "Automated SQLite backup of DBBE corpus",
        "creators": [{"name": "Paulien Lemay", "affiliation": "Ghent CDH"}],
        "access_right": "private"  # can be 'open', 'restricted', or 'embargoed'
    }
}

DATA_FOLDER = "data"

def upload_sqlite_files_to_zenodo(folder_path, publish=False):
    headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"}

    # Step 1: Check for existing depositions
    r = requests.get(ZENODO_API_URL, headers=headers)
    r.raise_for_status()
    existing = r.json()

    deposition_id = None
    for dep in existing:
        if dep['metadata']['title'] == DEPOSITION_TITLE:
            deposition_id = dep['id']
            print(f"Found existing deposition ID: {deposition_id}")
            break

    # Step 2: Create deposition if it doesn't exist
    if deposition_id is None:
        deposition_data = {
            "metadata": {
                "title": DEPOSITION_TITLE,
                "upload_type": "dataset",
                "description": "Automated SQLite backup of DBBE corpus",
                "creators": [{"name": "Your Name", "affiliation": "Ghent CDH"}],
                "access_right": "restricted"  # can be 'open', 'restricted', or 'embargoed'
            }
        }
        r = requests.post(ZENODO_API_URL, params={}, json=deposition_data, headers=headers)
        r.raise_for_status()
        deposition = r.json()
        deposition_id = deposition["id"]
        print(f"Created new deposition ID: {deposition_id}")

    # Step 3: Upload files
    # First, get existing files in the deposition to avoid duplicates
    r = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
    r.raise_for_status()
    existing_files = {f['filename']: f['id'] for f in r.json().get('files', [])}

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".sqlite"):
            file_path = os.path.join(folder_path, file_name)
            files = {'file': open(file_path, 'rb')}
            data = {'name': file_name}
            upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"

            # Delete existing file if it exists
            if file_name in existing_files:
                file_id = existing_files[file_name]
                del_url = f"{upload_url}/{file_id}"
                del_resp = requests.delete(del_url, headers=headers)
                del_resp.raise_for_status()
                print(f"Replaced existing file: {file_name}")

            resp = requests.post(upload_url, headers=headers, files=files, data=data)
            resp.raise_for_status()
            print(f"Uploaded {file_name} to deposition {deposition_id}")

    # Step 4: Optionally publish
    if publish:
        publish_url = f"{ZENODO_API_URL}/{deposition_id}/actions/publish"
        pub_resp = requests.post(publish_url, headers=headers)
        if pub_resp.status_code == 202:
            print(f"Deposition {deposition_id} published successfully.")
        else:
            print(f"Deposition {deposition_id} created but not published. Status: {pub_resp.status_code}")

    print(f"All files processed for deposition {deposition_id}.")
