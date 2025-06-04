"""
Verify raw cube files against database log and reset download flags if missing or corrupted.
"""

import os
import hashlib
import psycopg2
from pathlib import Path
from statcan.tools.config import DB_CONFIG
from loguru import logger

DOWNLOAD_DIR = Path("/app/raw/cubes")
logger.add("/app/logs/verify_raw_files.log", rotation="10 MB", retention="7 days")

def hash_file(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def verify_files():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT productid, file_hash, storage_location
                FROM raw_files.manage_cube_raw_files
                WHERE active = TRUE
            """)
            rows = cur.fetchall()

            for productid, file_hash, file_path in rows:
                p = Path(file_path)
                if not p.exists():
                    logger.warning(f"❌ File missing for {productid}: {file_path}")
                    cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", (productid, file_hash))
                    cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", (productid,))
                    conn.commit()
                    continue

                actual_hash = hash_file(p)
                if actual_hash != file_hash:
                    logger.error(f"⚠️ Hash mismatch for {productid}: expected {file_hash}, got {actual_hash}")
                    try:
                        os.remove(p)
                        logger.warning(f"🗑️ Corrupted file deleted: {file_path}")
                    except Exception as e:
                        logger.exception(f"💥 Failed to delete corrupted file: {file_path}")
                    cur.execute("DELETE FROM raw_files.manage_cube_raw_files WHERE productid = %s AND file_hash = %s", (productid, file_hash))
                    cur.execute("UPDATE raw_files.cube_status SET download_pending = TRUE WHERE productid = %s", (productid,))
                    conn.commit()
                else:
                    logger.info(f"✅ Verified {productid}: {p.name}")

def main():
    logger.info("🔍 Starting raw file verification...")
    try:
        verify_files()
        logger.success("🎯 Verification complete.")
    except Exception as e:
        logger.exception(f"💥 Verification failed: {e}")

if __name__ == "__main__":
    main()

