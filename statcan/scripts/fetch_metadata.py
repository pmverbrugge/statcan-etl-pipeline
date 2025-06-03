"""
Download StatCan metadata for cubes.
Uses getCubeMetadata endpoint to fetch bilingual JSON metadata.
Updates raw_files.manage_metadata_raw_files and metadata_status.
"""

import os
import requests
import hashlib
import psycopg2
from pathlib import Path
from loguru import logger
from datetime import datetime
from statcan.tools.config import DB_CONFIG
import time

WDS_URL_TEMPLATE = "https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata/{}"
DOWNLOAD_DIR = Path("/app/raw/metadata")
MAX_CUBES = None

logger.add("/app/logs/fetch_metadata.log", rotation="10 MB", retention="7 days")


def get_pending_metadata(limit=MAX_CUBES):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid FROM raw_files.metadata_status
                WHERE download_pending = TRUE
                ORDER BY productid
                LIMIT %s;
            """, (limit,))
            return [row[0] for row in cur.fetchall()]


def get_metadata_json(productid: int) -> bytes:
    url = "https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata"
    resp = requests.post(
        url,
        json=[{"productId": productid}],
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content


def hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def save_file(productid: int, file_hash: str, content: bytes) -> str:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{productid}_{file_hash[:16]}.json"
    file_path = DOWNLOAD_DIR / filename
    with open(file_path, "wb") as f:
        f.write(content)
    return str(file_path)


def file_exists(cur, file_hash: str) -> bool:
    cur.execute("SELECT 1 FROM raw_files.manage_metadata_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur, productid: int):
    cur.execute("""
        UPDATE raw_files.manage_metadata_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))


def insert_log(cur, productid: int, file_hash: str, file_path: str):
    deactivate_existing(cur, productid)
    cur.execute("""
        INSERT INTO raw_files.manage_metadata_raw_files (
            productid, file_hash, date_download, active, storage_location
        ) VALUES (%s, %s, now(), TRUE, %s)
    """, (productid, file_hash, file_path))


def update_status(cur, productid: int):
    cur.execute("""
        UPDATE raw_files.metadata_status
        SET download_pending = FALSE, last_download = now(), last_file_hash = (
            SELECT file_hash FROM raw_files.manage_metadata_raw_files
            WHERE productid = %s AND active = TRUE
        )
        WHERE productid = %s;
    """, (productid, productid))


def download_and_log(productid: int):
    logger.info(f"🔽 Downloading metadata for {productid}...")
    json_bytes = get_metadata_json(productid)
    file_hash = hash_bytes(json_bytes)

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            if file_exists(cur, file_hash):
                logger.warning(f"⚠️  Duplicate metadata for {productid}; skipping.")
                update_status(cur, productid)
                conn.commit()
                return

            file_path = save_file(productid, file_hash, json_bytes)
            insert_log(cur, productid, file_hash, file_path)
            update_status(cur, productid)
            conn.commit()
            logger.success(f"✅ Metadata saved for {productid} at {file_path}")


def main():
    logger.info("🚀 Starting metadata fetch script...")
    try:
        product_ids = get_pending_metadata()
        if not product_ids:
            logger.info("🎉 No metadata pending download.")
            return
        for pid in product_ids:
            try:
                download_and_log(pid)
                time.sleep(1)  # polite delay
            except Exception as e:
                logger.error(f"❌ Failed to process metadata for {pid}: {e}")
        logger.info("✅ Metadata batch complete.")
    except Exception as e:
        logger.exception(f"❌ Metadata pipeline failed: {e}")


if __name__ == "__main__":
    main()

