import os
import requests

ZENODO_TOKEN = "tRT9Go9VCZKhwLsMzWkqNhquRTle30esbmXRNZc89YVJGzVdVqRiGXpYGfcb"
ZENODO_API_URL = "https://sandbox.zenodo.org/api/deposit/depositions"
DEPOSITION_TITLE = "DBBE SQLite Backup"

DATA_FOLDER = "data"


def upload_sqlite_files_to_zenodo(folder_path, publish=False):
    headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"}

    # Step 1: Check for existing depositions
    print("Checking for existing depositions...")
    r = requests.get(ZENODO_API_URL, headers=headers)
    r.raise_for_status()
    existing = r.json()

    deposition_id = None
    for dep in existing:
        if dep['metadata']['title'] == DEPOSITION_TITLE:
            deposition_id = dep['id']
            print(f"Found existing deposition ID: {deposition_id}")

            # Check if it's published
            if dep.get('submitted', False):
                print(f"Deposition {deposition_id} is published. Creating new version...")
                # Create a new version
                new_version_url = f"{ZENODO_API_URL}/{deposition_id}/actions/newversion"
                nv_resp = requests.post(new_version_url, headers=headers)
                nv_resp.raise_for_status()
                new_version_data = nv_resp.json()
                # Get the new draft deposition ID
                draft_url = new_version_data['links']['latest_draft']
                deposition_id = draft_url.split('/')[-1]
                print(f"Created new version with deposition ID: {deposition_id}")
            break

    # Step 2: Create deposition if it doesn't exist
    if deposition_id is None:
        print("Creating new deposition...")
        deposition_data = {
            "metadata": {
                "title": DEPOSITION_TITLE,
                "upload_type": "dataset",
                "description": "Automated SQLite backup of DBBE corpus",
                "creators": [{"name": "Paulien Lemay", "affiliation": "Ghent CDH"}],
                "access_right": "restricted"
            }
        }
        r = requests.post(ZENODO_API_URL, params={}, json=deposition_data, headers=headers)
        r.raise_for_status()
        deposition = r.json()
        deposition_id = deposition["id"]
        print(f"Created new deposition ID: {deposition_id}")

    # Step 3: Get current deposition details
    print(f"Fetching deposition {deposition_id} details...")
    r = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
    r.raise_for_status()
    deposition_details = r.json()
    existing_files = {f['filename']: f['id'] for f in deposition_details.get('files', [])}
    print(f"Found {len(existing_files)} existing files in deposition")

    # Step 4: Upload files
    sqlite_files = [f for f in os.listdir(folder_path) if f.endswith(".sqlite")]
    print(f"Found {len(sqlite_files)} SQLite files to upload")

    for file_name in sqlite_files:
        file_path = os.path.join(folder_path, file_name)

        # Delete existing file if it exists
        if file_name in existing_files:
            file_id = existing_files[file_name]
            upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"
            del_url = f"{upload_url}/{file_id}"
            print(f"Deleting existing file: {file_name}")
            del_resp = requests.delete(del_url, headers=headers)
            del_resp.raise_for_status()
            print(f"Deleted existing file: {file_name}")

        # Upload file with proper context manager
        print(f"Uploading {file_name}...")
        upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"
        with open(file_path, 'rb') as f:
            files = {'file': f}
            data = {'name': file_name}
            resp = requests.post(upload_url, headers=headers, files=files, data=data)
            resp.raise_for_status()
        print(f"✓ Uploaded {file_name}")

    # Step 5: Optionally publish
    if publish:
        print(f"Publishing deposition {deposition_id}...")
        publish_url = f"{ZENODO_API_URL}/{deposition_id}/actions/publish"
        pub_resp = requests.post(publish_url, headers=headers)
        if pub_resp.status_code == 202:
            print(f"✓ Deposition {deposition_id} published successfully.")
        else:
            print(f"⚠ Deposition {deposition_id} created but not published. Status: {pub_resp.status_code}")
    else:
        print(f"Deposition {deposition_id} saved as draft (not published)")

    print(f"\n✓ All files processed for deposition {deposition_id}.")