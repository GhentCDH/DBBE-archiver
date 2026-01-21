import os
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()
ZENODO_TOKEN = os.getenv("ZENODO_TOKEN", "")
ZENODO_API_URL = os.getenv("ZENODO_API_URL", "https://sandbox.zenodo.org/api/deposit/depositions")
DEPOSITION_TITLE = os.getenv("DEPOSITION_TITLE", "DBBE SQLite Backup")

with open("../DB_DESCRIPTION.html", "r", encoding="utf-8") as f:
    description_text = f.read()

def upload_sqlite_files_to_zenodo(folder_path, mode="draft", deposition_id=None):
    if mode not in ["draft", "publish", "update"]:
        raise ValueError(f"Invalid mode: {mode}. Must be 'draft', 'publish', or 'update'")

    if mode == "update" and not deposition_id:
        raise ValueError("deposition_id is required when mode is 'update'")

    headers = {"Authorization": f"Bearer {ZENODO_TOKEN}"}

    if mode == "update":
        print(f"Updating existing deposition {deposition_id}...")

        r = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
        r.raise_for_status()
        dep_data = r.json()

        if dep_data.get('submitted', False):
            print(f"Deposition {deposition_id} is published. Creating new version...")
            new_version_url = f"{ZENODO_API_URL}/{deposition_id}/actions/newversion"
            nv_resp = requests.post(new_version_url, headers=headers)
            nv_resp.raise_for_status()
            new_version_data = nv_resp.json()
            draft_url = new_version_data['links']['latest_draft']
            deposition_id = draft_url.split('/')[-1]
            metadata_patch = { "metadata": { "publication_date": date.today().isoformat() }
            }

            patch_resp = requests.put(
                f"{ZENODO_API_URL}/{deposition_id}",
                headers=headers,
                json=metadata_patch
            )
            patch_resp.raise_for_status()
            print(f"Created new version with deposition ID: {deposition_id}")

    else:
        print("Checking for existing depositions...")
        r = requests.get(ZENODO_API_URL, headers=headers)
        r.raise_for_status()
        existing = r.json()

        deposition_id = None
        for dep in existing:
            if dep['metadata']['title'] == DEPOSITION_TITLE:
                deposition_id = dep['id']
                print(f"Found existing deposition with title '{DEPOSITION_TITLE}': ID {deposition_id}")

                if dep.get('submitted', False):
                    print(f"Deposition {deposition_id} is published. Creating new version...")
                    new_version_url = f"{ZENODO_API_URL}/{deposition_id}/actions/newversion"
                    nv_resp = requests.post(new_version_url, headers=headers)
                    nv_resp.raise_for_status()
                    new_version_data = nv_resp.json()
                    draft_url = new_version_data['links']['latest_draft']
                    deposition_id = draft_url.split('/')[-1]
                    metadata_patch = {
                        "metadata": {
                            "publication_date": date.today().isoformat()
                        }
                    }

                    patch_resp = requests.put(
                        f"{ZENODO_API_URL}/{deposition_id}",
                        headers=headers,
                        json=metadata_patch
                    )
                    patch_resp.raise_for_status()
                    print(f"Created new version with deposition ID: {deposition_id}")
                else:
                    print(f"Using existing draft deposition ID: {deposition_id}")
                break

        if deposition_id is None:
            print("Creating new deposition...")
            deposition_data = {
                "metadata": {
                    "title": DEPOSITION_TITLE,
                    "upload_type": "dataset",
                    "description": description_text,
                    "creators": [{"name": "Paulien Lemay", "affiliation": "Ghent CDH"}],
                    "access_right": "restricted"
                }
            }
            r = requests.post(ZENODO_API_URL, params={}, json=deposition_data, headers=headers)
            r.raise_for_status()
            deposition = r.json()
            deposition_id = deposition["id"]
            print(f"Created new deposition ID: {deposition_id}")

    print(f"Fetching deposition {deposition_id} details...")
    r = requests.get(f"{ZENODO_API_URL}/{deposition_id}", headers=headers)
    r.raise_for_status()
    deposition_details = r.json()
    existing_files = {f['filename']: f['id'] for f in deposition_details.get('files', [])}
    print(f"Found {len(existing_files)} existing files in deposition")

    sqlite_files = [f for f in os.listdir(folder_path) if f.endswith(".sqlite")]
    print(f"Found {len(sqlite_files)} SQLite files to upload")

    for file_name in sqlite_files:
        file_path = os.path.join(folder_path, file_name)

        if file_name in existing_files:
            file_id = existing_files[file_name]
            upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"
            del_url = f"{upload_url}/{file_id}"
            print(f"Deleting existing file: {file_name}")
            del_resp = requests.delete(del_url, headers=headers)
            del_resp.raise_for_status()
            print(f"Deleted existing file: {file_name}")

        print(f"Uploading {file_name}...")
        upload_url = f"{ZENODO_API_URL}/{deposition_id}/files"
        with open(file_path, 'rb') as f:
            files = {'file': f}
            data = {'name': file_name}
            resp = requests.post(upload_url, headers=headers, files=files, data=data)
            resp.raise_for_status()
        print(f"✓ Uploaded {file_name}")

    if mode == "publish":
        print(f"Publishing deposition {deposition_id}...")
        publish_url = f"{ZENODO_API_URL}/{deposition_id}/actions/publish"
        pub_resp = requests.post(publish_url, headers=headers)
        if pub_resp.status_code == 202:
            print(f"✓ Deposition {deposition_id} published successfully.")
        else:
            print(f"Deposition {deposition_id} created but publish failed. Status: {pub_resp.status_code}")
    else:
        print(f"Deposition {deposition_id} saved as draft (not published)")

    print(f"\nAll files processed for deposition {deposition_id}.")
    return deposition_id
