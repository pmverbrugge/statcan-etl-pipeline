"""
Download StatCan data cubes (limited test run).
Uses getFullTableDownloadSDMX endpoint to download cube zip files.
Updates raw_files.manage_cube_raw_files and cube_status.
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


WDS_URL_TEMPLATE = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadSDMX/{}"
DOWNLOAD_DIR = Path("/app/raw/cubes")
MAX_CUBES = 128

logger.add("/app/logs/fetch_cubes.log", rotation="10 MB", retention="7 days")


def get_pending_cubes(limit=MAX_CUBES):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid FROM raw_files.cube_status
                WHERE download_pending = TRUE
                ORDER BY productid
                LIMIT %s;
            """, (limit,))
            return [row[0] for row in cur.fetchall()]


def get_download_url(productid: int) -> str:
    url = WDS_URL_TEMPLATE.format(productid)
    resp = requests.get(url)
    resp.raise_for_status()
    result = resp.json()
    return result["object"]


def hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def save_file(productid: int, file_hash: str, content: bytes) -> str:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{productid}_{file_hash[:16]}.zip"
    file_path = DOWNLOAD_DIR / filename
    with open(file_path, "wb") as f:
        f.write(content)
    return str(file_path)


def file_exists(cur, file_hash: str) -> bool:
    cur.execute("SELECT 1 FROM raw_files.manage_cube_raw_files WHERE file_hash = %s", (file_hash,))
    return cur.fetchone() is not None


def deactivate_existing(cur, productid: int):
    cur.execute("""
        UPDATE raw_files.manage_cube_raw_files
        SET active = FALSE
        WHERE productid = %s AND active = TRUE
    """, (productid,))


def insert_log(cur, productid: int, file_hash: str, file_path: str):
    deactivate_existing(cur, productid)
    cur.execute("""
        INSERT INTO raw_files.manage_cube_raw_files (
            productid, file_hash, date_download, active, storage_location
        ) VALUES (%s, %s, now(), TRUE, %s)
    """, (productid, file_hash, file_path))


def update_status(cur, productid: int):
    cur.execute("""
        UPDATE raw_files.cube_status
        SET download_pending = FALSE, last_download = now(), last_file_hash = (
            SELECT file_hash FROM raw_files.manage_cube_raw_files
            WHERE productid = %s AND active = TRUE
        )
        WHERE productid = %s;
    """, (productid, productid))


def download_and_log(productid: int):
    logger.info(f"🔽 Downloading cube {productid}...")
    url = get_download_url(productid)
    resp = requests.get(url)
    resp.raise_for_status()
    file_bytes = resp.content
    file_hash = hash_bytes(file_bytes)

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            if file_exists(cur, file_hash):
                logger.warning(f"⚠️  Duplicate file for {productid}; skipping.")
                update_status(cur, productid)
                conn.commit()
                return

            file_path = save_file(productid, file_hash, file_bytes)
            insert_log(cur, productid, file_hash, file_path)
            update_status(cur, productid)
            conn.commit()
            logger.success(f"✅ Downloaded and logged {productid} to {file_path}")


def main():
    logger.info("🚀 Starting cube fetch script...")
    try:
        product_ids = get_pending_cubes()
        if not product_ids:
            logger.info("🎉 No cubes pending download.")
            return
        for pid in product_ids:
            try:
                download_and_log(pid)
                time.sleep(2) # polite pause
            except Exception as e:
                logger.error(f"❌ Failed to process cube {pid}: {e}")
        logger.info("✅ Batch download complete.")
    except Exception as e:
        logger.exception(f"❌ Download pipeline failed: {e}")


if __name__ == "__main__":
    main()

