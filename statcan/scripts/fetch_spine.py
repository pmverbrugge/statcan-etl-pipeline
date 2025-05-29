# statcan/scripts/fetch_spine.py

import os
import json
import hashlib
import requests
import psycopg2
from datetime import datetime
from loguru import logger
from pathlib import Path
from statcan.tools.config import DB_CONFIG

# Add file logging
logger.add("/app/logs/fetch_spine.log", rotation="10 MB", retention="7 days")

SPINE_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesListLite"
ARCHIVE_DIR = Path("/app/raw/metadata")


def fetch_json():
    logger.info(f"Requesting cube metadata from: {SPINE_URL}")
    resp = requests.get(SPINE_URL)
    resp.raise_for_status()
    return resp.json()


def hash_json(data: dict) -> str:
    b = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def save_file(data: dict, file_hash: str) -> str:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"spine_{file_hash[:16]}.json"
    file_path = ARCHIVE_DIR / filename
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return str(file_path)


def file_exists(cur, file_hash: str) -> bool:
    cur.execute("SELECT 1 FROM raw_files.manage_spine_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur):
    cur.execute("UPDATE raw_files.manage_spine_raw_files SET active = FALSE WHERE active = TRUE")


def insert_record(cur, file_hash: str, file_path: str):
    deactivate_existing(cur)
    cur.execute("""
        INSERT INTO raw_files.manage_spine_raw_files (
            file_hash, date_download, active, storage_location
        ) VALUES (%s, now(), TRUE, %s)
    """, (file_hash, file_path))


def main():
    logger.info("Starting spine archive fetch...")

    try:
        data = fetch_json()
        file_hash = hash_json(data)
        file_path = f"/app/raw/statcan/metadata/spine_{file_hash[:16]}.json"

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                if file_exists(cur, file_hash):
                    logger.warning("Duplicate spine file – already archived.")
                    return

                save_file(data, file_hash)
                insert_record(cur, file_hash, file_path)
                conn.commit()
                logger.success(f"Archived new spine file: {file_path}")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()

