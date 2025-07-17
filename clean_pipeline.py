import apache_beam as beam
import csv
from io import StringIO
import requests
import base64
import json
import os

# =========================
# CONFIGURATION
# =========================

GITHUB_TOKEN = "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"  # 🔐 Replace with your GitHub token
REPO_NAME = "Reinaldo000/equifax"
BRANCH = "main"

# =========================
# UTILITY FUNCTIONS
# =========================

def read_csv_from_github(url):
    url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    response = requests.get(url)
    response.raise_for_status()
    return response.text.splitlines()

def extract_number(line):
    if not line.strip() or line.lower().startswith("numero"):
        return None
    reader = csv.reader(StringIO(line))
    row = next(reader)
    return row[0] if row and row[0] != "None" else None

def upload_to_github(token, repo, file_path, local_file, commit_message):
    url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    headers = {"Authorization": f"token {token}"}

    # Get current SHA for update
    get_resp = requests.get(url, headers=headers)
    sha = get_resp.json().get("sha")

    with open(local_file, 'rb') as f:
        content_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "message": commit_message,
        "content": content_b64,
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha

    put_resp = requests.put(url, headers=headers, data=json.dumps(payload))
    if put_resp.status_code in [200, 201]:
        print(f"✅ File updated: {file_path}")
    else:
        print(f"❌ Error uploading {file_path}: {put_resp.status_code}")
        print(put_resp.json())

# =========================
# URLs and paths
# =========================

source_url = 'https://github.com/Reinaldo000/equifax/blob/main/data/source.csv'
file1_path = 'data/destinations/2024-07-15/file1.csv'
file2_path = 'data/destinations/2024-07-16/file2.csv'

file1_url = f'https://github.com/Reinaldo000/equifax/blob/main/{file1_path}'
file2_url = f'https://github.com/Reinaldo000/equifax/blob/main/{file2_path}'

# =========================
# Read data
# =========================

source_lines = read_csv_from_github(source_url)
file1_lines = read_csv_from_github(file1_url)
file2_lines = read_csv_from_github(file2_url)

# =========================
# Beam pipeline for cleaning
# =========================

with beam.Pipeline() as p:
    source_pcoll = (
        p
        | 'Read source' >> beam.Create(source_lines)
        | 'Extract source numbers' >> beam.Map(extract_number)
        | 'Filter empty source' >> beam.Filter(lambda x: x is not None)
    )

    # Clean file1: remove numbers that exist in source
    (
        p
        | 'Read file1' >> beam.Create(file1_lines)
        | 'Extract file1 numbers' >> beam.Map(extract_number)
        | 'Filter empty file1' >> beam.Filter(lambda x: x is not None)
        | 'Filter file1 not matching source' >> beam.Filter(lambda x, src: x not in src, beam.pvalue.AsList(source_pcoll))
        | 'Format CSV file1' >> beam.Map(lambda x: f"{x}")
        | 'Write cleaned file1' >> beam.io.WriteToText('file1_cleaned', file_name_suffix='.csv', shard_name_template='')
    )

    # Clean file2: remove numbers that exist in source
    (
        p
        | 'Read file2' >> beam.Create(file2_lines)
        | 'Extract file2 numbers' >> beam.Map(extract_number)
        | 'Filter empty file2' >> beam.Filter(lambda x: x is not None)
        | 'Filter file2 not matching source' >> beam.Filter(lambda x, src: x not in src, beam.pvalue.AsList(source_pcoll))
        | 'Format CSV file2' >> beam.Map(lambda x: f"{x}")
        | 'Write cleaned file2' >> beam.io.WriteToText('file2_cleaned', file_name_suffix='.csv', shard_name_template='')
    )

# =========================
# Upload cleaned files to GitHub (overwrite existing)
# =========================

upload_to_github(
    token=GITHUB_TOKEN,
    repo=REPO_NAME,
    file_path=file1_path,
    local_file='file1_cleaned.csv',
    commit_message="Remove source matches from file1.csv"
)

upload_to_github(
    token=GITHUB_TOKEN,
    repo=REPO_NAME,
    file_path=file2_path,
    local_file='file2_cleaned.csv',
    commit_message="Remove source matches from file2.csv"
)
