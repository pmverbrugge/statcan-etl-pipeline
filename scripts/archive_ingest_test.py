import json
import psycopg2
from lib.archive_ingester import ingest_file_from_bytes, get_file_from_archive
from lib.config import DB_CONFIG


# Step 1: Create two fake JSON files in memory
json1 = {"cube": "A", "desc": "First test"}
json2 = {"cube": "B", "desc": "Second test"}

bytes1 = json.dumps(json1, indent=2).encode("utf-8")
bytes2 = json.dumps(json2, indent=2).encode("utf-8")

file1_name = "test_file_1.json"
file2_name = "test_file_2.json"
source_url = "https://statcan.gc.ca/mock/"

conn = psycopg2.connect(**DB_CONFIG)

# Wipe db 
with conn.cursor() as cur:
    cur.execute("TRUNCATE archive.raw_file RESTART IDENTITY;")
    conn.commit()

# Step 2: Ingest file1
print("Ingesting file 1 (first time)...")
assert ingest_file_from_bytes(bytes1, file1_name, conn, source_url + file1_name) is True

# Step 3: Ingest file1 again (should fail)
print("Ingesting file 1 (duplicate)...")
assert ingest_file_from_bytes(bytes1, file1_name, conn, source_url + file1_name) is False

# Step 4: Ingest file2
print("Ingesting file 2 (first time)...")
assert ingest_file_from_bytes(bytes2, file2_name, conn, source_url + file2_name) is True

# Step 5: Retrieve file1 and compare content
print("Retrieving file 1 and comparing...")
f1 = get_file_from_archive(conn, file_name=file1_name)
assert f1 is not None
assert json.loads(f1["content"].decode("utf-8")) == json1

# Step 6: Retrieve file2 and compare content
print("Retrieving file 2 and comparing...")
f2 = get_file_from_archive(conn, file_name=file2_name)
assert f2 is not None
assert json.loads(f2["content"].decode("utf-8")) == json2

# Step 7: Attempt to ingest file2 again (should fail)
print("Ingesting file 2 (duplicate)...")
assert ingest_file_from_bytes(bytes2, file2_name, conn, source_url + file2_name) is False

conn.close()
print("✅ All archive ingestion tests passed.")

