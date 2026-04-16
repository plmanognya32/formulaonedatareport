import requests
import json
from datetime import datetime
from google.cloud import storage
from prefect import flow, task

BUCKET_NAME = "formulaonedatareportmanu"  # 👈 change this to your actual bucket name

@task
def fetch_sessions(year: int = 2024):
    url = f"https://api.openf1.org/v1/sessions?year={year}&session_type=Race"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} sessions for {year}")
    return data

@task
def fetch_laps(session_key: int):
    url = f"https://api.openf1.org/v1/laps?session_key={session_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} laps for session {session_key}")
    return data

@task
def fetch_drivers(session_key: int):
    url = f"https://api.openf1.org/v1/drivers?session_key={session_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@task
def upload_to_gcs(data: list, gcs_path: str):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"Uploaded {len(data)} records to gs://{BUCKET_NAME}/{gcs_path}")

@flow(name="f1-bronze-ingestion", log_prints=True)
def bronze_ingestion(year: int = 2024):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    sessions = fetch_sessions(year)
    upload_to_gcs(sessions, f"bronze/sessions/{year}/sessions_{timestamp}.json")

    for session in sessions[:3]:
        session_key = session["session_key"]
        circuit = session.get("circuit_short_name", "unknown")

        laps = fetch_laps(session_key)
        upload_to_gcs(laps, f"bronze/laps/{year}/{circuit}_{session_key}_{timestamp}.json")

        drivers = fetch_drivers(session_key)
        upload_to_gcs(drivers, f"bronze/drivers/{year}/{circuit}_{session_key}_{timestamp}.json")

    print("Bronze ingestion complete!")

if __name__ == "__main__":
    bronze_ingestion(year=2024)
