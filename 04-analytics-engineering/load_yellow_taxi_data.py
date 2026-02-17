import os
import sys
import urllib.request
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import product

from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden


# =========================
# CONFIG
# =========================
BUCKET_NAME = ""

CREDENTIALS_FILE = ""
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_"
YEARS = [2019]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

DOWNLOAD_DIR = "./data"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
MAX_WORKERS = 4

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


# =========================
# BUCKET
# =========================
def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except NotFound:
        try:
            client.create_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'.")
        except Forbidden:
            print(
                f"Bucket '{bucket_name}' exists but you don't have access. "
                f"Choose another name."
            )
            sys.exit(1)


# =========================
# DOWNLOAD
# =========================
def download_file(year, month):
    url = f"{BASE_URL}{year}-{month}.csv.gz"
    filename = f"fhv_tripdata__{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(file_path):
        print(f"Already downloaded: {filename}")
        return file_path

    try:
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, file_path)
        return file_path
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return None


# =========================
# UPLOAD
# =========================
def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {blob_name} (Attempt {attempt + 1})")
            blob.upload_from_filename(file_path)

            if blob.exists(client):
                print(f"✅ Uploaded: gs://{BUCKET_NAME}/{blob_name}")
                return
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            time.sleep(5)

    print(f"❌ Giving up on {blob_name}")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    tasks = list(product(YEARS, MONTHS))

    # 1️⃣ Download all .csv.gz
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        gz_files = list(executor.map(lambda x: download_file(*x), tasks))

    gz_files = [f for f in gz_files if f is not None]

    # 2️⃣ Upload to GCS (still .csv.gz)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(upload_to_gcs, gz_files)

    print("🚀 All files downloaded and uploaded as .csv.gz")
